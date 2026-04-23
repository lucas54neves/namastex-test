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
    "PIPELINE_LLM_TIMEOUT_SECONDS",
    "PIPELINE_LLM_MAX_RETRIES",
    "OPENAI_API_KEY",
    "PIPELINE_ENABLE_LANGFUSE",
    "PIPELINE_LANGFUSE_ALLOW_LOCAL_PROMPT_FALLBACK",
    "PIPELINE_LANGFUSE_PROMPT_NAME",
    "PIPELINE_LANGFUSE_PROMPT_LABEL",
    "PIPELINE_LANGFUSE_TRACE_NAME",
    "LANGFUSE_BASE_URL",
    "LANGFUSE_PUBLIC_KEY",
    "LANGFUSE_SECRET_KEY",
)


@dataclass(frozen=True)
class DatabricksDeploymentConfig:
    host: str
    job_name: str
    workspace_root: str
    workspace_script_path: str
    catalog_name: str
    schema_name: str
    input_volume_name: str
    output_volume_name: str
    input_file_local: Path
    input_file_volume_path: str
    data_dir: str
    reports_dir: str
    state_dir: str
    runtime_dir: str
    config_dir: str
    pipeline_input_file: str
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
        workspace_root=workspace_root.rstrip("/"),
        workspace_script_path=f"{workspace_root.rstrip('/')}/scripts/run_pipeline.py",
        catalog_name=catalog_name,
        schema_name=schema_name,
        input_volume_name=input_volume_name,
        output_volume_name=output_volume_name,
        input_file_local=repo_root / "docs" / "conversations_bronze.parquet",
        input_file_volume_path=f"{input_volume_path}/{input_filename}",
        data_dir=f"{output_volume_path}/data",
        reports_dir=f"{output_volume_path}/reports",
        state_dir=f"{output_volume_path}/state",
        runtime_dir=f"{output_volume_path}/runtime",
        config_dir=f"{workspace_root.rstrip('/')}/config",
        pipeline_input_file=f"{input_volume_path}/{input_filename}",
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


def build_databricks_job_settings(config: DatabricksDeploymentConfig) -> dict[str, Any]:
    return {
        "name": config.job_name,
        "tasks": [
            {
                "task_key": "run_pipeline",
                "spark_python_task": {
                    "python_file": config.workspace_script_path,
                    "parameters": ["--force"],
                },
                "new_cluster": {
                    "spark_version": config.spark_version,
                    "node_type_id": config.node_type_id,
                    "num_workers": config.num_workers,
                },
                "environment_variables": config.environment,
            }
        ],
    }


def _run_command(args: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, check=True, text=True, capture_output=True, env=env)


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
    completed = _run_command(args, env=_databricks_env(config))
    stdout = completed.stdout.strip()
    return json.loads(stdout) if stdout else {}


def _resource_exists(
    config: DatabricksDeploymentConfig,
    path: str,
    not_found_markers: tuple[str, ...] = ("RESOURCE_DOES_NOT_EXIST", "NOT_FOUND"),
) -> bool:
    try:
        _api_call(config, "get", path)
        return True
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr or ""
        if any(marker in stderr for marker in not_found_markers):
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
    _run_command(["databricks", "fs", "mkdir", config.input_volume_path], env=env)
    _run_command(
        [
            "databricks",
            "fs",
            "cp",
            str(config.input_file_local),
            config.input_file_volume_path,
            "--overwrite",
        ],
        env=env,
    )


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
                raise RuntimeError(
                    f"Databricks run {run_id} failed with result_state={result_state or 'UNKNOWN'}"
                )
            return status
        if life_cycle_state in {"SKIPPED", "INTERNAL_ERROR"}:
            raise RuntimeError(
                "Databricks run "
                f"{run_id} ended unexpectedly with life_cycle_state={life_cycle_state}"
            )
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
