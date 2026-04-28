from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline.agent.alerts import handle_alerting
from pipeline.config import PipelinePaths
from pipeline.io.parquet_io import write_json
from pipeline.orchestration.operator_artifacts import plan_report_file


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _planner_report_summary(paths: PipelinePaths, planner_report: dict[str, Any]) -> dict[str, Any]:
    return {
        "report_path": str(plan_report_file(paths)),
        "proposal_id": planner_report.get("proposal_id"),
        "proposal_count": len(planner_report.get("proposals", [])),
        "requires_approval": planner_report.get("requires_approval", False),
        "approved": planner_report.get("approved", False),
        "applied": planner_report.get("applied", False),
        "promoted_proposal_ids": planner_report.get("promoted_proposal_ids", []),
        "applied_proposal_ids": planner_report.get("applied_proposal_ids", []),
        "applied_proposal_types": planner_report.get("applied_proposal_types", []),
        "autonomy_policy_path": planner_report.get("autonomy_policy_path"),
        "autonomy_metrics_path": planner_report.get("autonomy_metrics_path"),
    }


def _run_record(
    source_fingerprint: dict[str, Any],
    executed: bool,
    status: str,
    validation_summary: dict[str, Any] | None = None,
    details: dict[str, Any] | None = None,
    agent_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "run_at_utc": _utc_now_iso(),
        "executed": executed,
        "status": status,
        "source_fingerprint": source_fingerprint,
        "validation_summary": validation_summary or {},
        "agent_summary": agent_summary or {},
        "details": details or {},
    }


def _build_agent_report(
    incident_id: str,
    status: str,
    diagnoses: list[dict[str, Any]],
    auto_remediation: dict[str, Any] | None = None,
    fallback: dict[str, Any] | None = None,
    exception: str | None = None,
    decisions: list[dict[str, Any]] | None = None,
    planner_report: dict[str, Any] | None = None,
    quarantine_report: dict[str, Any] | None = None,
    llm_diagnoses: list[dict[str, Any]] | None = None,
    execution_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "generated_at_utc": _utc_now_iso(),
        "incident_id": incident_id,
        "status": status,
        "diagnoses": diagnoses,
        "auto_remediation": auto_remediation or {},
        "fallback": fallback or {},
        "decisions": decisions or [],
        "planner_report": planner_report or {},
        "quarantine_report": quarantine_report or {},
        "exception": exception,
        "llm_diagnoses": llm_diagnoses or [],
        "execution_plan": execution_plan or {},
    }


def _build_alert_report(
    paths: PipelinePaths,
    run_record: dict[str, Any],
    agent_report: dict[str, Any],
    validation_summary: dict[str, Any],
) -> dict[str, Any]:
    return handle_alerting(
        alerts_dir=paths.alerts,
        run_record=run_record,
        agent_report=agent_report,
        validation_report=validation_summary,
    )


def _write_reports(
    validation_summary: dict[str, Any],
    agent_report: dict[str, Any],
    alert_report: dict[str, Any],
    validation_report_path: Path,
    agent_report_path: Path,
    alert_report_path: Path,
    decisions_dir: Path,
) -> None:
    write_json(validation_summary, validation_report_path)
    write_json(agent_report, agent_report_path)
    write_json(alert_report, alert_report_path)
    decision_path = decisions_dir / "latest_agent_decision.json"
    write_json(
        {
            "generated_at_utc": agent_report["generated_at_utc"],
            "status": agent_report["status"],
            "decisions": agent_report.get("decisions", []),
        },
        decision_path,
    )


def _extract_llm_diagnoses(agent_diagnoses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for d in agent_diagnoses:
        source = d.get("source", {})
        if isinstance(source, dict) and "llm_diagnosis" in source:
            result.append(source["llm_diagnosis"])
    return result
