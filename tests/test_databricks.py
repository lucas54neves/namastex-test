from __future__ import annotations

import subprocess
from pathlib import Path

from pipeline.config import build_paths
from pipeline.infrastructure.databricks import (
    _api_call,
    _resource_exists,
    build_databricks_deployment_config,
    build_databricks_job_settings,
    run_job_and_wait,
    upload_input_file,
)


def test_build_paths_keeps_local_defaults_without_overrides(tmp_path: Path) -> None:
    paths = build_paths(tmp_path, env={})

    assert paths.root == tmp_path.resolve()
    assert paths.raw_bronze_source == (tmp_path / "docs" / "conversations_bronze.parquet").resolve()
    assert paths.data == (tmp_path / "data").resolve()
    assert paths.reports == (tmp_path / "reports").resolve()
    assert paths.state == (tmp_path / "state").resolve()
    assert paths.candidates == (tmp_path / "runtime" / "candidates").resolve()
    assert paths.pipeline_spec == (tmp_path / "config" / "pipeline_spec.json").resolve()


def test_build_paths_applies_runtime_overrides(tmp_path: Path) -> None:
    env = {
        "PIPELINE_INPUT_FILE": str(tmp_path / "volumes" / "input" / "conversations.parquet"),
        "PIPELINE_DATA_DIR": str(tmp_path / "volumes" / "output" / "data"),
        "PIPELINE_REPORTS_DIR": str(tmp_path / "volumes" / "output" / "reports"),
        "PIPELINE_STATE_DIR": str(tmp_path / "volumes" / "output" / "state"),
        "PIPELINE_RUNTIME_DIR": str(tmp_path / "volumes" / "output" / "runtime"),
        "PIPELINE_CONFIG_DIR": str(tmp_path / "workspace" / "config"),
    }

    paths = build_paths(tmp_path, env=env)

    assert paths.raw_bronze_source == Path(env["PIPELINE_INPUT_FILE"]).resolve()
    assert paths.bronze == Path(env["PIPELINE_DATA_DIR"]).resolve() / "bronze"
    assert paths.gold == Path(env["PIPELINE_DATA_DIR"]).resolve() / "gold"
    assert paths.monitoring == Path(env["PIPELINE_REPORTS_DIR"]).resolve() / "monitoring"
    assert paths.approval_state == Path(env["PIPELINE_STATE_DIR"]).resolve() / "approval_state.json"
    assert paths.candidates == Path(env["PIPELINE_RUNTIME_DIR"]).resolve() / "candidates"
    assert paths.autonomy_policy == (
        Path(env["PIPELINE_CONFIG_DIR"]).resolve() / "agent_autonomy_policy.json"
    )


def test_build_databricks_job_settings_uses_serverless_defaults(tmp_path: Path) -> None:
    input_file = tmp_path / "docs" / "conversations_bronze.parquet"
    input_file.parent.mkdir(parents=True)
    input_file.write_bytes(b"PAR1")

    config = build_databricks_deployment_config(
        tmp_path,
        env={
            "DATABRICKS_HOST": "https://dbc.example.com",
            "DATABRICKS_WORKSPACE_ROOT": "/Workspace/Shared/namastex-test",
            "DATABRICKS_CATALOG": "main",
            "DATABRICKS_SCHEMA": "ops",
            "DATABRICKS_INPUT_VOLUME": "bronze_input",
            "DATABRICKS_OUTPUT_VOLUME": "pipeline_output",
            "DATABRICKS_JOB_NAME": "namastex-test-pipeline",
            "DATABRICKS_SERVERLESS_ENVIRONMENT_VERSION": "2",
            "PIPELINE_ENABLE_LLM_ENRICHMENT": "1",
            "PIPELINE_LLM_OPENAI_MODEL": "gpt-5-mini",
            "PIPELINE_LLM_ANTHROPIC_MODEL": "claude-sonnet",
            "OPENAI_API_KEY": "sk-test",
            "ANTHROPIC_API_KEY": "anthropic-test",
            "PIPELINE_ENABLE_LANGFUSE": "1",
            "LANGFUSE_BASE_URL": "https://langfuse.example.com",
            "LANGFUSE_PUBLIC_KEY": "lf_pk_test",
            "LANGFUSE_SECRET_KEY": "lf_sk_test",
        },
    )

    settings = build_databricks_job_settings(config)
    task = settings["tasks"][0]

    assert settings["name"] == "namastex-test-pipeline"
    assert settings["environments"] == [
        {
            "environment_key": "default",
            "spec": {
                "environment_version": "2",
                "dependencies": [
                    "/Workspace/Shared/namastex-test",
                    "-r /Workspace/Shared/namastex-test/requirements.txt",
                ],
            },
        }
    ]
    assert task["spark_python_task"]["python_file"] == (
        "/Workspace/Shared/namastex-test/scripts/run_pipeline.py"
    )
    assert task["spark_python_task"]["parameters"] == [
        "--force",
        "--input-file",
        "/Volumes/main/ops/bronze_input/conversations_bronze.parquet",
        "--data-dir",
        "/Volumes/main/ops/pipeline_output/data",
        "--reports-dir",
        "/Volumes/main/ops/pipeline_output/reports",
        "--state-dir",
        "/Volumes/main/ops/pipeline_output/state",
        "--runtime-dir",
        "/Volumes/main/ops/pipeline_output/runtime",
        "--config-dir",
        "/Workspace/Shared/namastex-test/config",
    ]
    assert task["environment_key"] == "default"
    assert "new_cluster" not in task
    assert task["environment_variables"]["PIPELINE_INPUT_FILE"] == (
        "/Volumes/main/ops/bronze_input/conversations_bronze.parquet"
    )
    assert config.input_file_cli_path == (
        "dbfs:/Volumes/main/ops/bronze_input/conversations_bronze.parquet"
    )
    assert task["environment_variables"]["PIPELINE_DATA_DIR"] == (
        "/Volumes/main/ops/pipeline_output/data"
    )
    assert task["environment_variables"]["PIPELINE_REPORTS_DIR"] == (
        "/Volumes/main/ops/pipeline_output/reports"
    )
    assert task["environment_variables"]["PIPELINE_CONFIG_DIR"] == (
        "/Workspace/Shared/namastex-test/config"
    )
    assert task["environment_variables"]["PIPELINE_ENABLE_LLM_ENRICHMENT"] == "1"
    assert task["environment_variables"]["PIPELINE_LLM_OPENAI_MODEL"] == "gpt-5-mini"
    assert task["environment_variables"]["PIPELINE_LLM_ANTHROPIC_MODEL"] == "claude-sonnet"
    assert task["environment_variables"]["OPENAI_API_KEY"] == "sk-test"
    assert task["environment_variables"]["ANTHROPIC_API_KEY"] == "anthropic-test"
    assert task["environment_variables"]["PIPELINE_ENABLE_LANGFUSE"] == "1"
    assert task["environment_variables"]["LANGFUSE_BASE_URL"] == "https://langfuse.example.com"
    assert task["environment_variables"]["LANGFUSE_PUBLIC_KEY"] == "lf_pk_test"
    assert task["environment_variables"]["LANGFUSE_SECRET_KEY"] == "lf_sk_test"
    assert settings["environments"][0]["spec"]["environment_version"] == "2"
    assert settings["environments"][0]["spec"]["dependencies"] == [
        "/Workspace/Shared/namastex-test",
        "-r /Workspace/Shared/namastex-test/requirements.txt",
    ]


def test_build_databricks_job_settings_supports_classic_compute(tmp_path: Path) -> None:
    input_file = tmp_path / "docs" / "conversations_bronze.parquet"
    input_file.parent.mkdir(parents=True)
    input_file.write_bytes(b"PAR1")

    config = build_databricks_deployment_config(
        tmp_path,
        env={
            "DATABRICKS_HOST": "https://dbc.example.com",
            "DATABRICKS_JOB_COMPUTE_MODE": "classic",
            "DATABRICKS_SPARK_VERSION": "15.4.x-scala2.12",
            "DATABRICKS_NODE_TYPE_ID": "Standard_DS3_v2",
            "DATABRICKS_NUM_WORKERS": "2",
        },
    )

    settings = build_databricks_job_settings(config)
    task = settings["tasks"][0]

    assert "environments" not in settings
    assert "environment_key" not in task
    assert task["new_cluster"] == {
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "Standard_DS3_v2",
        "num_workers": 2,
    }


def test_build_databricks_config_uses_defaults_when_workflow_vars_are_empty(
    tmp_path: Path,
) -> None:
    input_file = tmp_path / "docs" / "conversations_bronze.parquet"
    input_file.parent.mkdir(parents=True)
    input_file.write_bytes(b"PAR1")

    config = build_databricks_deployment_config(
        tmp_path,
        env={
            "DATABRICKS_HOST": "https://dbc.example.com",
            "DATABRICKS_WORKSPACE_ROOT": "",
            "DATABRICKS_CATALOG": "",
            "DATABRICKS_SCHEMA": "",
            "DATABRICKS_INPUT_VOLUME": "",
            "DATABRICKS_OUTPUT_VOLUME": "",
            "DATABRICKS_JOB_NAME": "",
            "DATABRICKS_SPARK_VERSION": "",
            "DATABRICKS_NODE_TYPE_ID": "",
            "DATABRICKS_NUM_WORKERS": "",
            "DATABRICKS_RUN_POLL_SECONDS": "",
        },
    )

    assert config.workspace_root == "/Workspace/Shared/namastex-test"
    assert config.catalog_name == "main"
    assert config.schema_name == "ops"
    assert config.input_volume_name == "bronze_input"
    assert config.output_volume_name == "pipeline_output"
    assert config.job_name == "namastex-test-pipeline"
    assert config.job_compute_mode == "serverless"
    assert config.serverless_environment_version == "2"
    assert config.spark_version == "15.4.x-scala2.12"
    assert config.node_type_id == "Standard_DS3_v2"
    assert config.num_workers == 1
    assert config.poll_seconds == 10


def test_build_databricks_config_rejects_invalid_compute_mode(tmp_path: Path) -> None:
    input_file = tmp_path / "docs" / "conversations_bronze.parquet"
    input_file.parent.mkdir(parents=True)
    input_file.write_bytes(b"PAR1")

    try:
        build_databricks_deployment_config(
            tmp_path,
            env={
                "DATABRICKS_HOST": "https://dbc.example.com",
                "DATABRICKS_JOB_COMPUTE_MODE": "gpu-dragon",
            },
        )
    except ValueError as exc:
        assert "DATABRICKS_JOB_COMPUTE_MODE" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_resource_exists_returns_false_for_databricks_not_found_errors(
    tmp_path: Path,
    monkeypatch,
) -> None:
    input_file = tmp_path / "docs" / "conversations_bronze.parquet"
    input_file.parent.mkdir(parents=True)
    input_file.write_bytes(b"PAR1")
    config = build_databricks_deployment_config(
        tmp_path,
        env={"DATABRICKS_HOST": "https://dbc.example.com"},
    )

    def raise_not_found(*_args, **_kwargs):
        raise subprocess.CalledProcessError(
            1,
            ["databricks", "api", "get", "/api/2.1/unity-catalog/schemas/main.ops"],
            output='{"error_code":"NOT_FOUND","message":"Schema main.ops does not exist"}',
            stderr="",
        )

    monkeypatch.setattr("pipeline.infrastructure.databricks._run_command", raise_not_found)

    assert _resource_exists(config, "/api/2.1/unity-catalog/schemas/main.ops") is False


def test_api_call_raises_runtime_error_with_databricks_output(tmp_path: Path, monkeypatch) -> None:
    input_file = tmp_path / "docs" / "conversations_bronze.parquet"
    input_file.parent.mkdir(parents=True)
    input_file.write_bytes(b"PAR1")
    config = build_databricks_deployment_config(
        tmp_path,
        env={"DATABRICKS_HOST": "https://dbc.example.com"},
    )

    def raise_forbidden(*_args, **_kwargs):
        raise subprocess.CalledProcessError(
            1,
            ["databricks", "api", "get", "/api/2.1/unity-catalog/schemas/main.ops"],
            output="",
            stderr='{"error_code":"PERMISSION_DENIED","message":"Access denied"}',
        )

    monkeypatch.setattr("pipeline.infrastructure.databricks._run_command", raise_forbidden)

    try:
        _api_call(config, "get", "/api/2.1/unity-catalog/schemas/main.ops")
    except RuntimeError as exc:
        assert "Databricks API GET /api/2.1/unity-catalog/schemas/main.ops failed" in str(exc)
        assert "PERMISSION_DENIED" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError")


def test_upload_input_file_copies_directly_to_existing_volume(tmp_path: Path, monkeypatch) -> None:
    input_file = tmp_path / "docs" / "conversations_bronze.parquet"
    input_file.parent.mkdir(parents=True)
    input_file.write_bytes(b"PAR1")
    config = build_databricks_deployment_config(
        tmp_path,
        env={"DATABRICKS_HOST": "https://dbc.example.com"},
    )
    calls: list[tuple[list[str], dict[str, str]]] = []

    def record_run_command(
        args: list[str], env: dict[str, str]
    ) -> subprocess.CompletedProcess[str]:
        calls.append((args, env))
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    monkeypatch.setattr("pipeline.infrastructure.databricks._run_command", record_run_command)

    upload_input_file(config)

    assert len(calls) == 1
    assert calls[0][0] == [
        "databricks",
        "fs",
        "cp",
        str(config.input_file_local),
        config.input_file_cli_path,
        "--overwrite",
    ]
    assert calls[0][1]["DATABRICKS_HOST"] == "https://dbc.example.com"


def test_upload_input_file_retries_until_success(tmp_path: Path, monkeypatch) -> None:
    input_file = tmp_path / "docs" / "conversations_bronze.parquet"
    input_file.parent.mkdir(parents=True)
    input_file.write_bytes(b"PAR1")
    config = build_databricks_deployment_config(
        tmp_path,
        env={"DATABRICKS_HOST": "https://dbc.example.com"},
    )
    calls: list[list[str]] = []
    sleeps: list[int] = []

    def flaky_run_command(args: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        if len(calls) < 3:
            raise subprocess.CalledProcessError(
                1,
                args,
                output="",
                stderr="volume path not ready",
            )
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    monkeypatch.setattr("pipeline.infrastructure.databricks._run_command", flaky_run_command)
    monkeypatch.setattr("pipeline.infrastructure.databricks.time.sleep", sleeps.append)

    upload_input_file(config)

    assert len(calls) == 3
    assert sleeps == [5, 5]


def test_upload_input_file_raises_runtime_error_with_cli_output(
    tmp_path: Path, monkeypatch
) -> None:
    input_file = tmp_path / "docs" / "conversations_bronze.parquet"
    input_file.parent.mkdir(parents=True)
    input_file.write_bytes(b"PAR1")
    config = build_databricks_deployment_config(
        tmp_path,
        env={"DATABRICKS_HOST": "https://dbc.example.com"},
    )
    sleeps: list[int] = []

    def always_fail(args: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        raise subprocess.CalledProcessError(
            1,
            args,
            output="cli stdout",
            stderr="permission denied",
        )

    monkeypatch.setattr("pipeline.infrastructure.databricks._run_command", always_fail)
    monkeypatch.setattr("pipeline.infrastructure.databricks.time.sleep", sleeps.append)

    try:
        upload_input_file(config)
    except RuntimeError as exc:
        message = str(exc)
        assert "Databricks input upload failed after 3 attempts" in message
        assert config.input_file_cli_path in message
        assert "stderr=permission denied" in message
        assert "stdout=cli stdout" in message
    else:
        raise AssertionError("Expected RuntimeError")

    assert sleeps == [5, 5]


def test_run_job_and_wait_raises_enriched_error_for_internal_error(
    tmp_path: Path, monkeypatch
) -> None:
    input_file = tmp_path / "docs" / "conversations_bronze.parquet"
    input_file.parent.mkdir(parents=True)
    input_file.write_bytes(b"PAR1")
    config = build_databricks_deployment_config(
        tmp_path,
        env={"DATABRICKS_HOST": "https://dbc.example.com"},
    )

    def fake_api_call(_config, method: str, path: str, payload=None):
        if method == "post" and path == "/api/2.1/jobs/run-now":
            return {"run_id": 123}
        if method == "get" and path == "/api/2.1/jobs/runs/get?run_id=123":
            return {
                "state": {
                    "life_cycle_state": "INTERNAL_ERROR",
                    "result_state": "FAILED",
                    "state_message": "Serverless environment setup failed",
                },
                "tasks": [{"task_key": "run_pipeline", "run_id": 456}],
            }
        if method == "get" and path == "/api/2.1/jobs/runs/get-output?run_id=456":
            return {
                "metadata": {
                    "state": {
                        "state_message": "Dependency installation failed",
                    }
                },
                "error": "pip could not resolve requirements",
            }
        raise AssertionError(f"Unexpected API call: {method} {path} payload={payload}")

    monkeypatch.setattr("pipeline.infrastructure.databricks._api_call", fake_api_call)

    try:
        run_job_and_wait(config, 999)
    except RuntimeError as exc:
        message = str(exc)
        assert "Databricks run 123 failed" in message
        assert "life_cycle_state=INTERNAL_ERROR" in message
        assert "result_state=FAILED" in message
        assert "state_message=Serverless environment setup failed" in message
        assert "task run_pipeline state_message=Dependency installation failed" in message
        assert "task run_pipeline error=pip could not resolve requirements" in message
    else:
        raise AssertionError("Expected RuntimeError")


def test_run_job_and_wait_raises_enriched_error_for_terminated_failure(
    tmp_path: Path, monkeypatch
) -> None:
    input_file = tmp_path / "docs" / "conversations_bronze.parquet"
    input_file.parent.mkdir(parents=True)
    input_file.write_bytes(b"PAR1")
    config = build_databricks_deployment_config(
        tmp_path,
        env={"DATABRICKS_HOST": "https://dbc.example.com"},
    )

    def fake_api_call(_config, method: str, path: str, payload=None):
        if method == "post" and path == "/api/2.1/jobs/run-now":
            return {"run_id": 321}
        if method == "get" and path == "/api/2.1/jobs/runs/get?run_id=321":
            return {
                "state": {
                    "life_cycle_state": "TERMINATED",
                    "result_state": "FAILED",
                    "state_message": "Task failed while executing script",
                },
                "tasks": [{"task_key": "run_pipeline", "run_id": 654}],
            }
        if method == "get" and path == "/api/2.1/jobs/runs/get-output?run_id=654":
            raise RuntimeError(
                "Databricks API GET /api/2.1/jobs/runs/get-output failed: not available"
            )
        raise AssertionError(f"Unexpected API call: {method} {path} payload={payload}")

    monkeypatch.setattr("pipeline.infrastructure.databricks._api_call", fake_api_call)

    try:
        run_job_and_wait(config, 999)
    except RuntimeError as exc:
        message = str(exc)
        assert "Databricks run 321 failed" in message
        assert "life_cycle_state=TERMINATED" in message
        assert "result_state=FAILED" in message
        assert "state_message=Task failed while executing script" in message
        assert "task run_pipeline output unavailable" in message
    else:
        raise AssertionError("Expected RuntimeError")


def test_workflow_exists_with_required_triggers_and_databricks_contract() -> None:
    workflow = Path(".github/workflows/databricks-deploy-run.yml").read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "push:" in workflow
    assert "- main" in workflow
    assert "DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}" in workflow
    assert "DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}" in workflow
    assert "OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}" in workflow
    assert "PIPELINE_ENABLE_LLM_ENRICHMENT: ${{ vars.PIPELINE_ENABLE_LLM_ENRICHMENT }}" in workflow
    assert "PIPELINE_LLM_OPENAI_MODEL: ${{ vars.PIPELINE_LLM_OPENAI_MODEL }}" in workflow
    assert "LANGFUSE_PUBLIC_KEY: ${{ secrets.LANGFUSE_PUBLIC_KEY }}" in workflow
    assert "LANGFUSE_SECRET_KEY: ${{ secrets.LANGFUSE_SECRET_KEY }}" in workflow
    assert "uses: databricks/setup-cli@main" in workflow
    assert "DATABRICKS_JOB_COMPUTE_MODE: ${{ vars.DATABRICKS_JOB_COMPUTE_MODE }}" in workflow
    assert (
        "DATABRICKS_SERVERLESS_ENVIRONMENT_VERSION: "
        "${{ vars.DATABRICKS_SERVERLESS_ENVIRONMENT_VERSION }}" in workflow
    )
    assert "python scripts/databricks_deploy_run.py" in workflow
