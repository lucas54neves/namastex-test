from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pipeline.config import PipelinePaths


@dataclass(frozen=True)
class PipelineArtifacts:
    bronze_path: str
    silver_path: str
    silver_messages_path: str
    silver_conversations_llm_path: str
    gold_path: str
    gold_macro_path: str
    state_path: str
    validation_report_path: str
    agent_report_path: str
    alert_report_path: str
    executed: bool
    status: str


def state_file(paths: PipelinePaths) -> Path:
    return paths.state / "pipeline_state.json"


def validation_report_file(paths: PipelinePaths) -> Path:
    return paths.monitoring / "latest_run_report.json"


def agent_report_file(paths: PipelinePaths) -> Path:
    return paths.monitoring / "latest_agent_report.json"


def alert_report_file(paths: PipelinePaths) -> Path:
    return paths.monitoring / "latest_alert_report.json"


def plan_report_file(paths: PipelinePaths) -> Path:
    return paths.monitoring / "latest_plan_report.json"


def schema_drift_report_file(paths: PipelinePaths, run_id: str | None = None) -> Path:
    name = "latest_schema_drift_report.json"
    if run_id:
        name = f"schema_drift_{run_id}.json"
    return paths.monitoring / name


def _skip_artifacts(paths: PipelinePaths) -> PipelineArtifacts:
    return PipelineArtifacts(
        bronze_path=str(paths.bronze / "conversations.parquet"),
        silver_path=str(paths.silver / "silver_leads.parquet"),
        silver_messages_path=str(paths.silver / "silver_messages.parquet"),
        silver_conversations_llm_path=str(paths.silver / "silver_conversations_llm.parquet"),
        gold_path=str(paths.gold / "conversations_gold.parquet"),
        gold_macro_path=str(paths.gold / "conversations_gold_macro.parquet"),
        state_path=str(state_file(paths)),
        validation_report_path=str(validation_report_file(paths)),
        agent_report_path=str(agent_report_file(paths)),
        alert_report_path=str(alert_report_file(paths)),
        executed=False,
        status="skipped_no_source_change",
    )


def _success_artifacts(paths: PipelinePaths, status: str) -> PipelineArtifacts:
    return PipelineArtifacts(
        bronze_path=str(paths.bronze / "conversations.parquet"),
        silver_path=str(paths.silver / "silver_leads.parquet"),
        silver_messages_path=str(paths.silver / "silver_messages.parquet"),
        silver_conversations_llm_path=str(paths.silver / "silver_conversations_llm.parquet"),
        gold_path=str(paths.gold / "conversations_gold.parquet"),
        gold_macro_path=str(paths.gold / "conversations_gold_macro.parquet"),
        state_path=str(state_file(paths)),
        validation_report_path=str(validation_report_file(paths)),
        agent_report_path=str(agent_report_file(paths)),
        alert_report_path=str(alert_report_file(paths)),
        executed=True,
        status=status,
    )
