from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_OPTIONAL_JOB_ENV_VARS = (
    "PIPELINE_ENABLE_LLM_ENRICHMENT",
    "PIPELINE_LLM_OPENAI_MODEL",
    "PIPELINE_LLM_ANTHROPIC_MODEL",
    "PIPELINE_LLM_TIMEOUT_SECONDS",
    "PIPELINE_LLM_MAX_RETRIES",
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "PIPELINE_ENABLE_LANGFUSE",
    "PIPELINE_LANGFUSE_ALLOW_LOCAL_PROMPT_FALLBACK",
    "PIPELINE_LANGFUSE_PROMPT_NAME",
    "PIPELINE_LANGFUSE_PROMPT_LABEL",
    "PIPELINE_LANGFUSE_TRACE_NAME",
    "LANGFUSE_BASE_URL",
    "LANGFUSE_PUBLIC_KEY",
    "LANGFUSE_SECRET_KEY",
)

_INPUT_UPLOAD_MAX_ATTEMPTS = 3
_INPUT_UPLOAD_RETRY_SECONDS = 5


@dataclass(frozen=True)
class DatabricksDeploymentConfig:
    host: str
    job_name: str
    job_compute_mode: str
    workspace_root: str
    workspace_script_path: str
    catalog_name: str
    schema_name: str
    input_volume_name: str
    output_volume_name: str
    input_file_local: Path
    input_file_cli_path: str
    data_dir: str
    reports_dir: str
    state_dir: str
    runtime_dir: str
    config_dir: str
    workspace_project_path: str
    pipeline_input_file: str
    workspace_requirements_path: str
    serverless_environment_version: str
    spark_version: str
    node_type_id: str
    num_workers: int
    poll_seconds: int
    optional_job_env: dict[str, str]

    @property
    def input_volume_path(self) -> str:
        return f"/Volumes/{self.catalog_name}/{self.schema_name}/{self.input_volume_name}"

    @property
    def output_volume_path(self) -> str:
        return f"/Volumes/{self.catalog_name}/{self.schema_name}/{self.output_volume_name}"

    @property
    def input_file_volume_path(self) -> str:
        return self.pipeline_input_file

    @property
    def environment(self) -> dict[str, str]:
        environment = {
            "PIPELINE_INPUT_FILE": self.pipeline_input_file,
            "PIPELINE_DATA_DIR": self.data_dir,
            "PIPELINE_REPORTS_DIR": self.reports_dir,
            "PIPELINE_STATE_DIR": self.state_dir,
            "PIPELINE_RUNTIME_DIR": self.runtime_dir,
            "PIPELINE_CONFIG_DIR": self.config_dir,
        }
        environment.update(self.optional_job_env)
        return environment


def _required_env(env: dict[str, str], name: str) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _optional_env(env: dict[str, str], name: str, default: str) -> str:
    value = env.get(name, "").strip()
    return value or default


def _optional_int_env(env: dict[str, str], name: str, default: int) -> int:
    value = env.get(name, "").strip()
    return int(value) if value else default


def _normalized_job_compute_mode(env: dict[str, str]) -> str:
    mode = _optional_env(env, "DATABRICKS_JOB_COMPUTE_MODE", "serverless").lower()
    if mode not in {"serverless", "classic"}:
        raise ValueError("DATABRICKS_JOB_COMPUTE_MODE must be either 'serverless' or 'classic'")
    return mode


def build_databricks_deployment_config(
    repo_root: Path,
    env: dict[str, str] | None = None,
) -> DatabricksDeploymentConfig:
    runtime_env = dict(os.environ if env is None else env)
    host = _required_env(runtime_env, "DATABRICKS_HOST")
    workspace_root = _optional_env(
        runtime_env,
        "DATABRICKS_WORKSPACE_ROOT",
        "/Workspace/Shared/namastex-test",
    )
    catalog_name = _optional_env(runtime_env, "DATABRICKS_CATALOG", "main")
    schema_name = _optional_env(runtime_env, "DATABRICKS_SCHEMA", "ops")
    input_volume_name = _optional_env(runtime_env, "DATABRICKS_INPUT_VOLUME", "bronze_input")
    output_volume_name = _optional_env(
        runtime_env,
        "DATABRICKS_OUTPUT_VOLUME",
        "pipeline_output",
    )
    output_volume_path = f"/Volumes/{catalog_name}/{schema_name}/{output_volume_name}"
    input_volume_path = f"/Volumes/{catalog_name}/{schema_name}/{input_volume_name}"
    input_filename = _optional_env(
        runtime_env,
        "DATABRICKS_INPUT_FILENAME",
        "conversations_bronze.parquet",
    )

    return DatabricksDeploymentConfig(
        host=host,
        job_name=_optional_env(
            runtime_env,
            "DATABRICKS_JOB_NAME",
            "namastex-test-pipeline",
        ),
        job_compute_mode=_normalized_job_compute_mode(runtime_env),
        workspace_root=workspace_root.rstrip("/"),
        workspace_script_path=f"{workspace_root.rstrip('/')}/scripts/run_pipeline.py",
        catalog_name=catalog_name,
        schema_name=schema_name,
        input_volume_name=input_volume_name,
        output_volume_name=output_volume_name,
        input_file_local=repo_root / "docs" / "conversations_bronze.parquet",
        input_file_cli_path=f"dbfs:{input_volume_path}/{input_filename}",
        data_dir=f"{output_volume_path}/data",
        reports_dir=f"{output_volume_path}/reports",
        state_dir=f"{output_volume_path}/state",
        runtime_dir=f"{output_volume_path}/runtime",
        config_dir=f"{workspace_root.rstrip('/')}/config",
        workspace_project_path=workspace_root.rstrip("/"),
        pipeline_input_file=f"{input_volume_path}/{input_filename}",
        workspace_requirements_path=f"{workspace_root.rstrip('/')}/requirements.txt",
        serverless_environment_version=_optional_env(
            runtime_env,
            "DATABRICKS_SERVERLESS_ENVIRONMENT_VERSION",
            "2",
        ),
        spark_version=_optional_env(
            runtime_env,
            "DATABRICKS_SPARK_VERSION",
            "15.4.x-scala2.12",
        ),
        node_type_id=_optional_env(
            runtime_env,
            "DATABRICKS_NODE_TYPE_ID",
            "Standard_DS3_v2",
        ),
        num_workers=_optional_int_env(runtime_env, "DATABRICKS_NUM_WORKERS", 1),
        poll_seconds=_optional_int_env(runtime_env, "DATABRICKS_RUN_POLL_SECONDS", 10),
        optional_job_env={
            name: value.strip()
            for name in _OPTIONAL_JOB_ENV_VARS
            if (value := runtime_env.get(name, "").strip())
        },
    )


def _build_serverless_task_settings(config: DatabricksDeploymentConfig) -> dict[str, Any]:
    return {
        "environment_key": "default",
        "environments": [
            {
                "environment_key": "default",
                "spec": {
                    "environment_version": config.serverless_environment_version,
                    "dependencies": [
                        config.workspace_project_path,
                        f"-r {config.workspace_requirements_path}",
                    ],
                },
            }
        ],
    }


def _build_classic_task_settings(config: DatabricksDeploymentConfig) -> dict[str, Any]:
    return {
        "new_cluster": {
            "spark_version": config.spark_version,
            "node_type_id": config.node_type_id,
            "num_workers": config.num_workers,
        }
    }


def build_databricks_job_settings(config: DatabricksDeploymentConfig) -> dict[str, Any]:
    task_settings: dict[str, Any] = {
        "task_key": "run_pipeline",
        "spark_python_task": {
            "python_file": config.workspace_script_path,
            "parameters": [
                "--force",
                "--input-file",
                config.pipeline_input_file,
                "--data-dir",
                config.data_dir,
                "--reports-dir",
                config.reports_dir,
                "--state-dir",
                config.state_dir,
                "--runtime-dir",
                config.runtime_dir,
                "--config-dir",
                config.config_dir,
            ],
        },
        "environment_variables": config.environment,
    }
    if config.job_compute_mode == "serverless":
        serverless_settings = _build_serverless_task_settings(config)
        task_settings["environment_key"] = serverless_settings["environment_key"]
        environments = serverless_settings["environments"]
    else:
        task_settings.update(_build_classic_task_settings(config))
        environments = []

    return {
        "name": config.job_name,
        "tasks": [task_settings],
        **({"environments": environments} if environments else {}),
    }


def _run_command(args: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, check=True, text=True, capture_output=True, env=env)


def _command_failure_details(exc: subprocess.CalledProcessError) -> str:
    stdout = (exc.stdout or "").strip()
    stderr = (exc.stderr or "").strip()
    details = []
    if stderr:
        details.append(f"stderr={stderr}")
    if stdout:
        details.append(f"stdout={stdout}")
    return "; ".join(details) if details else "no Databricks CLI output captured"


def _databricks_env(config: DatabricksDeploymentConfig) -> dict[str, str]:
    databricks_env = dict(os.environ)
    databricks_env["DATABRICKS_HOST"] = config.host
    return databricks_env


def _api_call(
    config: DatabricksDeploymentConfig,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    args = ["databricks", "api", method, path]
    if payload is not None:
        args.extend(["--json", json.dumps(payload)])
    try:
        completed = _run_command(args, env=_databricks_env(config))
    except subprocess.CalledProcessError as exc:
        stdout = (exc.stdout or "").strip()
        stderr = (exc.stderr or "").strip()
        details = stderr or stdout or "no Databricks CLI output captured"
        raise RuntimeError(f"Databricks API {method.upper()} {path} failed: {details}") from exc
    stdout = completed.stdout.strip()
    return json.loads(stdout) if stdout else {}


def _looks_like_not_found(message: str, markers: tuple[str, ...]) -> bool:
    normalized = message.upper()
    return any(marker in normalized for marker in markers)


def _resource_exists(
    config: DatabricksDeploymentConfig,
    path: str,
    not_found_markers: tuple[str, ...] = (
        "RESOURCE_DOES_NOT_EXIST",
        "NOT_FOUND",
        "DOES NOT EXIST",
        "404",
    ),
) -> bool:
    try:
        _api_call(config, "get", path)
        return True
    except RuntimeError as exc:
        if _looks_like_not_found(str(exc), not_found_markers):
            return False
        raise


def reconcile_catalog(config: DatabricksDeploymentConfig) -> None:
    path = f"/api/2.1/unity-catalog/catalogs/{config.catalog_name}"
    if _resource_exists(config, path):
        return
    _api_call(
        config,
        "post",
        "/api/2.1/unity-catalog/catalogs",
        {"name": config.catalog_name},
    )


def reconcile_schema(config: DatabricksDeploymentConfig) -> None:
    path = f"/api/2.1/unity-catalog/schemas/{config.catalog_name}.{config.schema_name}"
    if _resource_exists(config, path):
        return
    _api_call(
        config,
        "post",
        "/api/2.1/unity-catalog/schemas",
        {"name": config.schema_name, "catalog_name": config.catalog_name},
    )


def reconcile_volume(config: DatabricksDeploymentConfig, volume_name: str) -> None:
    path = (
        f"/api/2.1/unity-catalog/volumes/{config.catalog_name}.{config.schema_name}.{volume_name}"
    )
    if _resource_exists(config, path):
        return
    _api_call(
        config,
        "post",
        "/api/2.1/unity-catalog/volumes",
        {
            "name": volume_name,
            "catalog_name": config.catalog_name,
            "schema_name": config.schema_name,
            "volume_type": "MANAGED",
        },
    )


def sync_workspace_files(config: DatabricksDeploymentConfig, repo_root: Path) -> None:
    env = _databricks_env(config)
    _run_command(["databricks", "workspace", "mkdirs", config.workspace_root], env=env)
    _run_command(
        [
            "databricks",
            "workspace",
            "import-dir",
            str(repo_root),
            config.workspace_root,
            "--overwrite",
        ],
        env=env,
    )


def upload_input_file(config: DatabricksDeploymentConfig) -> None:
    env = _databricks_env(config)
    args = [
        "databricks",
        "fs",
        "cp",
        str(config.input_file_local),
        config.input_file_cli_path,
        "--overwrite",
    ]
    last_error: subprocess.CalledProcessError | None = None
    for attempt in range(1, _INPUT_UPLOAD_MAX_ATTEMPTS + 1):
        try:
            _run_command(args, env=env)
            return
        except subprocess.CalledProcessError as exc:
            last_error = exc
            if attempt == _INPUT_UPLOAD_MAX_ATTEMPTS:
                break
            time.sleep(_INPUT_UPLOAD_RETRY_SECONDS)
    assert last_error is not None
    raise RuntimeError(
        "Databricks input upload failed after "
        f"{_INPUT_UPLOAD_MAX_ATTEMPTS} attempts for {config.input_file_cli_path}: "
        f"{_command_failure_details(last_error)}"
    ) from last_error


def _find_job_id(config: DatabricksDeploymentConfig) -> int | None:
    response = _api_call(config, "get", "/api/2.1/jobs/list")
    for job in response.get("jobs", []):
        settings = job.get("settings", {})
        if settings.get("name") == config.job_name:
            return int(job["job_id"])
    return None


def reconcile_job(config: DatabricksDeploymentConfig) -> int:
    job_settings = build_databricks_job_settings(config)
    job_id = _find_job_id(config)
    if job_id is None:
        response = _api_call(config, "post", "/api/2.1/jobs/create", job_settings)
        return int(response["job_id"])
    _api_call(
        config,
        "post",
        "/api/2.1/jobs/reset",
        {"job_id": job_id, "new_settings": job_settings},
    )
    return job_id


def _run_output_summary(config: DatabricksDeploymentConfig, status: dict[str, Any]) -> str:
    tasks = status.get("tasks", [])
    details: list[str] = []
    for task in tasks:
        run_id = task.get("run_id")
        task_key = task.get("task_key", "unknown")
        if run_id is None:
            continue
        try:
            output = _api_call(config, "get", f"/api/2.1/jobs/runs/get-output?run_id={run_id}")
        except RuntimeError as exc:
            details.append(f"task {task_key} output unavailable: {exc}")
            continue
        metadata = output.get("metadata", {})
        state = metadata.get("state", {})
        state_message = state.get("state_message", "").strip()
        error = str(output.get("error", "")).strip()
        if state_message:
            details.append(f"task {task_key} state_message={state_message}")
        if error:
            details.append(f"task {task_key} error={error}")
    return "; ".join(details)


def _run_failure_details(
    config: DatabricksDeploymentConfig, run_id: int, status: dict[str, Any]
) -> str:
    state = status.get("state", {})
    life_cycle_state = state.get("life_cycle_state", "") or "UNKNOWN"
    result_state = state.get("result_state", "") or "UNKNOWN"
    state_message = str(state.get("state_message", "")).strip()
    details = [
        f"life_cycle_state={life_cycle_state}",
        f"result_state={result_state}",
    ]
    if state_message:
        details.append(f"state_message={state_message}")
    output_summary = _run_output_summary(config, status)
    if output_summary:
        details.append(output_summary)
    return f"Databricks run {run_id} failed: " + "; ".join(details)


def run_job_and_wait(config: DatabricksDeploymentConfig, job_id: int) -> dict[str, Any]:
    run_response = _api_call(
        config,
        "post",
        "/api/2.1/jobs/run-now",
        {"job_id": job_id},
    )
    run_id = int(run_response["run_id"])
    while True:
        status = _api_call(config, "get", f"/api/2.1/jobs/runs/get?run_id={run_id}")
        state = status.get("state", {})
        life_cycle_state = state.get("life_cycle_state", "")
        result_state = state.get("result_state", "")
        if life_cycle_state == "TERMINATED":
            if result_state != "SUCCESS":
                raise RuntimeError(_run_failure_details(config, run_id, status))
            return status
        if life_cycle_state in {"SKIPPED", "INTERNAL_ERROR"}:
            raise RuntimeError(_run_failure_details(config, run_id, status))
        time.sleep(max(config.poll_seconds, 1))


def deploy_and_run_databricks(repo_root: Path, env: dict[str, str] | None = None) -> dict[str, Any]:
    config = build_databricks_deployment_config(repo_root=repo_root, env=env)
    if not config.input_file_local.exists():
        raise FileNotFoundError(f"Input file not found: {config.input_file_local}")

    reconcile_catalog(config)
    reconcile_schema(config)
    reconcile_volume(config, config.input_volume_name)
    reconcile_volume(config, config.output_volume_name)
    sync_workspace_files(config, repo_root)
    upload_input_file(config)
    job_id = reconcile_job(config)
    run_status = run_job_and_wait(config, job_id)
    return {
        "job_id": job_id,
        "run_id": run_status.get("run_id"),
        "workspace_root": config.workspace_root,
        "input_file_volume_path": config.input_file_volume_path,
        "output_volume_path": config.output_volume_path,
        "job_name": config.job_name,
    }
