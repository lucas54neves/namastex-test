from __future__ import annotations

from pathlib import Path

from pipeline.config import build_paths
from pipeline.infrastructure.databricks import (
    build_databricks_deployment_config,
    build_databricks_job_settings,
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


def test_build_databricks_job_settings_uses_volume_and_workspace_paths(tmp_path: Path) -> None:
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
            "DATABRICKS_SPARK_VERSION": "15.4.x-scala2.12",
            "DATABRICKS_NODE_TYPE_ID": "Standard_DS3_v2",
            "DATABRICKS_NUM_WORKERS": "2",
            "PIPELINE_ENABLE_LLM_ENRICHMENT": "1",
            "PIPELINE_LLM_OPENAI_MODEL": "gpt-5-mini",
            "OPENAI_API_KEY": "sk-test",
            "PIPELINE_ENABLE_LANGFUSE": "1",
            "LANGFUSE_BASE_URL": "https://langfuse.example.com",
            "LANGFUSE_PUBLIC_KEY": "lf_pk_test",
            "LANGFUSE_SECRET_KEY": "lf_sk_test",
        },
    )

    settings = build_databricks_job_settings(config)
    task = settings["tasks"][0]

    assert settings["name"] == "namastex-test-pipeline"
    assert task["spark_python_task"]["python_file"] == (
        "/Workspace/Shared/namastex-test/scripts/run_pipeline.py"
    )
    assert task["spark_python_task"]["parameters"] == ["--force"]
    assert task["environment_variables"]["PIPELINE_INPUT_FILE"] == (
        "/Volumes/main/ops/bronze_input/conversations_bronze.parquet"
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
    assert task["environment_variables"]["OPENAI_API_KEY"] == "sk-test"
    assert task["environment_variables"]["PIPELINE_ENABLE_LANGFUSE"] == "1"
    assert task["environment_variables"]["LANGFUSE_BASE_URL"] == "https://langfuse.example.com"
    assert task["environment_variables"]["LANGFUSE_PUBLIC_KEY"] == "lf_pk_test"
    assert task["environment_variables"]["LANGFUSE_SECRET_KEY"] == "lf_sk_test"
    assert task["new_cluster"]["num_workers"] == 2


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
    assert config.spark_version == "15.4.x-scala2.12"
    assert config.node_type_id == "Standard_DS3_v2"
    assert config.num_workers == 1
    assert config.poll_seconds == 10


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
    assert "python scripts/databricks_deploy_run.py" in workflow
