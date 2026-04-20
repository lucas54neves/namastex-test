from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from pipeline.agent import (
    attempt_auto_remediation,
    diagnose_exception,
    diagnose_validation_failures,
)
from pipeline.alerts import handle_alerting
from pipeline.config import PipelinePaths, ensure_directories
from pipeline.io import read_json, write_json, write_parquet
from pipeline.quality import (
    summarize_validation_results,
    validate_bronze,
    validate_gold,
    validate_silver,
)
from pipeline.state import (
    build_source_fingerprint,
    has_source_changed,
    load_pipeline_state,
    save_pipeline_state,
)
from pipeline.transforms import build_gold, build_silver, load_bronze_frame


@dataclass(frozen=True)
class PipelineArtifacts:
    bronze_path: str
    silver_path: str
    gold_path: str
    state_path: str
    validation_report_path: str
    agent_report_path: str
    alert_report_path: str
    executed: bool
    status: str


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _state_file(paths: PipelinePaths) -> Path:
    return paths.state / "pipeline_state.json"


def _validation_report_file(paths: PipelinePaths) -> Path:
    return paths.monitoring / "latest_run_report.json"


def _agent_report_file(paths: PipelinePaths) -> Path:
    return paths.monitoring / "latest_agent_report.json"


def _alert_report_file(paths: PipelinePaths) -> Path:
    return paths.monitoring / "latest_alert_report.json"


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


def _skip_artifacts(paths: PipelinePaths) -> PipelineArtifacts:
    return PipelineArtifacts(
        bronze_path=str(paths.bronze / "conversations.parquet"),
        silver_path=str(paths.silver / "conversations_silver.parquet"),
        gold_path=str(paths.gold / "conversations_gold.parquet"),
        state_path=str(_state_file(paths)),
        validation_report_path=str(_validation_report_file(paths)),
        agent_report_path=str(_agent_report_file(paths)),
        alert_report_path=str(_alert_report_file(paths)),
        executed=False,
        status="skipped_no_source_change",
    )


def _build_agent_report(
    status: str,
    diagnoses: list[dict[str, Any]],
    auto_remediation: dict[str, Any] | None = None,
    fallback: dict[str, Any] | None = None,
    exception: str | None = None,
) -> dict[str, Any]:
    return {
        "generated_at_utc": _utc_now_iso(),
        "status": status,
        "diagnoses": diagnoses,
        "auto_remediation": auto_remediation or {},
        "fallback": fallback or {},
        "exception": exception,
    }


def _write_reports(
    validation_summary: dict[str, Any],
    agent_report: dict[str, Any],
    alert_report: dict[str, Any],
    validation_report_path: Path,
    agent_report_path: Path,
    alert_report_path: Path,
) -> None:
    write_json(validation_summary, validation_report_path)
    write_json(agent_report, agent_report_path)
    write_json(alert_report, alert_report_path)


def _success_artifacts(paths: PipelinePaths, status: str) -> PipelineArtifacts:
    return PipelineArtifacts(
        bronze_path=str(paths.bronze / "conversations.parquet"),
        silver_path=str(paths.silver / "conversations_silver.parquet"),
        gold_path=str(paths.gold / "conversations_gold.parquet"),
        state_path=str(_state_file(paths)),
        validation_report_path=str(_validation_report_file(paths)),
        agent_report_path=str(_agent_report_file(paths)),
        alert_report_path=str(_alert_report_file(paths)),
        executed=True,
        status=status,
    )


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


def run_pipeline(paths: PipelinePaths, force: bool = False) -> PipelineArtifacts:
    ensure_directories(paths)

    state_path = _state_file(paths)
    report_path = _validation_report_file(paths)
    agent_report_path = _agent_report_file(paths)
    alert_report_path = _alert_report_file(paths)
    current_fingerprint_obj = build_source_fingerprint(paths.raw_bronze_source)
    current_fingerprint = current_fingerprint_obj.as_dict()
    state = load_pipeline_state(state_path)
    previous_fingerprint = state.get("last_source_fingerprint")

    if not force and not has_source_changed(current_fingerprint_obj, previous_fingerprint):
        agent_report = _build_agent_report(
            status="idle_no_source_change",
            diagnoses=[],
            fallback={"applied": False},
        )
        run_record = _run_record(
            source_fingerprint=current_fingerprint,
            executed=False,
            status="skipped_no_source_change",
            details={"reason": "source fingerprint unchanged"},
            agent_summary={"status": agent_report["status"], "diagnosis_count": 0},
        )
        validation_summary: dict[str, Any] = {
            "status": "passed",
            "checks": [],
            "failed_checks": [],
            "executed_at_utc": _utc_now_iso(),
            "source_fingerprint": current_fingerprint,
        }
        alert_report = _build_alert_report(paths, run_record, agent_report, validation_summary)
        _write_reports(
            validation_summary,
            agent_report,
            alert_report,
            report_path,
            agent_report_path,
            alert_report_path,
        )
        state.setdefault("runs", []).append(run_record)
        state["last_seen_at_utc"] = _utc_now_iso()
        save_pipeline_state(state_path, state)
        return _skip_artifacts(paths)

    bronze_path = paths.bronze / "conversations.parquet"
    silver_path = paths.silver / "conversations_silver.parquet"
    gold_path = paths.gold / "conversations_gold.parquet"

    try:
        bronze_df = load_bronze_frame(str(paths.raw_bronze_source))
        write_parquet(bronze_df, bronze_path)

        silver_df = build_silver(bronze_df)
        write_parquet(silver_df, silver_path)

        gold_df = build_gold(silver_df)
        write_parquet(gold_df, gold_path)

        validation_results = (
            validate_bronze(bronze_df) + validate_silver(silver_df) + validate_gold(gold_df)
        )
        validation_summary = summarize_validation_results(validation_results)
        validation_summary["executed_at_utc"] = _utc_now_iso()
        validation_summary["row_counts"] = {
            "bronze": int(len(bronze_df)),
            "silver": int(len(silver_df)),
            "gold": int(len(gold_df)),
        }
        validation_summary["source_fingerprint"] = current_fingerprint

        failed_checks = cast(list[dict[str, Any]], validation_summary["failed_checks"])
        row_counts = cast(dict[str, Any], validation_summary["row_counts"])

        agent_status = "healthy"
        agent_report = _build_agent_report(status=agent_status, diagnoses=[])
        run_status = "success"

        if validation_summary["status"] != "passed":
            diagnoses = [
                diagnosis.as_dict() for diagnosis in diagnose_validation_failures(failed_checks)
            ]
            remediation = attempt_auto_remediation(
                bronze_df=bronze_df,
                silver_df=silver_df,
                gold_df=gold_df,
                failed_checks=failed_checks,
            )
            if remediation["resolved"]:
                silver_df = remediation["silver_df"]
                gold_df = remediation["gold_df"]
                write_parquet(silver_df, silver_path)
                write_parquet(gold_df, gold_path)
                validation_results = (
                    validate_bronze(bronze_df) + validate_silver(silver_df) + validate_gold(gold_df)
                )
                validation_summary = summarize_validation_results(validation_results)
                validation_summary["executed_at_utc"] = _utc_now_iso()
                validation_summary["row_counts"] = {
                    "bronze": int(len(bronze_df)),
                    "silver": int(len(silver_df)),
                    "gold": int(len(gold_df)),
                }
                validation_summary["source_fingerprint"] = current_fingerprint
                failed_checks = cast(list[dict[str, Any]], validation_summary["failed_checks"])
                row_counts = cast(dict[str, Any], validation_summary["row_counts"])
                agent_status = "auto_remediated"
                run_status = "success_after_auto_remediation"
            else:
                agent_status = "degraded_validation_failed"
                run_status = "validation_failed"

            agent_report = _build_agent_report(
                status=agent_status,
                diagnoses=diagnoses,
                auto_remediation={
                    "applied": bool(remediation["actions"]),
                    "actions": remediation["actions"],
                    "resolved": remediation["resolved"],
                },
                fallback={"applied": False},
            )

        run_record = _run_record(
            source_fingerprint=current_fingerprint,
            executed=True,
            status=run_status,
            validation_summary={
                "status": validation_summary["status"],
                "failed_checks": failed_checks,
            },
            details=row_counts,
            agent_summary={
                "status": agent_report["status"],
                "diagnosis_count": len(agent_report["diagnoses"]),
                "auto_remediation_applied": agent_report["auto_remediation"].get(
                    "applied",
                    False,
                ),
            },
        )
        alert_report = _build_alert_report(paths, run_record, agent_report, validation_summary)
        _write_reports(
            validation_summary,
            agent_report,
            alert_report,
            report_path,
            agent_report_path,
            alert_report_path,
        )
        state.setdefault("runs", []).append(run_record)
        state["last_source_fingerprint"] = current_fingerprint
        if run_status in {"success", "success_after_auto_remediation"}:
            state["last_successful_run_at_utc"] = _utc_now_iso()
            state["last_successful_artifacts"] = {
                "bronze_path": str(bronze_path),
                "silver_path": str(silver_path),
                "gold_path": str(gold_path),
                "validation_report_path": str(report_path),
                "agent_report_path": str(agent_report_path),
                "alert_report_path": str(alert_report_path),
            }
        state["last_seen_at_utc"] = _utc_now_iso()
        save_pipeline_state(state_path, state)
        return _success_artifacts(paths, run_status)

    except Exception as exc:
        diagnosis = diagnose_exception(exc).as_dict()
        last_successful_artifacts = state.get("last_successful_artifacts", {})
        fallback_applied = bool(last_successful_artifacts)
        run_status = "fallback_to_last_successful" if fallback_applied else "runtime_failed"
        validation_summary = {
            "status": "failed",
            "checks": [],
            "failed_checks": [
                {
                    "layer": "runtime",
                    "check": "unexpected_exception",
                    "status": "failed",
                    "detail": {"exception": f"{type(exc).__name__}: {exc}"},
                }
            ],
            "executed_at_utc": _utc_now_iso(),
            "source_fingerprint": current_fingerprint,
        }
        agent_report = _build_agent_report(
            status="fallback_applied" if fallback_applied else "manual_intervention_required",
            diagnoses=[diagnosis],
            fallback={
                "applied": fallback_applied,
                "artifacts": last_successful_artifacts,
            },
            exception=f"{type(exc).__name__}: {exc}",
        )
        run_record = _run_record(
            source_fingerprint=current_fingerprint,
            executed=True,
            status=run_status,
            validation_summary={
                "status": validation_summary["status"],
                "failed_checks": cast(list[dict[str, Any]], validation_summary["failed_checks"]),
            },
            details={"exception": f"{type(exc).__name__}: {exc}"},
            agent_summary={
                "status": agent_report["status"],
                "diagnosis_count": 1,
                "fallback_applied": fallback_applied,
            },
        )
        alert_report = _build_alert_report(paths, run_record, agent_report, validation_summary)
        _write_reports(
            validation_summary,
            agent_report,
            alert_report,
            report_path,
            agent_report_path,
            alert_report_path,
        )
        state.setdefault("runs", []).append(run_record)
        state["last_seen_at_utc"] = _utc_now_iso()
        save_pipeline_state(state_path, state)
        return _success_artifacts(paths, run_status)


def build_monitor_snapshot(paths: PipelinePaths) -> dict[str, Any]:
    state_path = _state_file(paths)
    report_path = _validation_report_file(paths)
    agent_report_path = _agent_report_file(paths)
    alert_report_path = _alert_report_file(paths)
    state = load_pipeline_state(state_path)
    latest_report = read_json(report_path, default={})
    latest_agent_report = read_json(agent_report_path, default={})
    latest_alert_report = read_json(alert_report_path, default={})
    last_run = state.get("runs", [])[-1] if state.get("runs") else {}
    return {
        "state_path": str(state_path),
        "validation_report_path": str(report_path),
        "agent_report_path": str(agent_report_path),
        "alert_report_path": str(alert_report_path),
        "last_run": last_run,
        "run_count": len(state.get("runs", [])),
        "last_source_fingerprint": state.get("last_source_fingerprint"),
        "latest_validation_status": latest_report.get("status"),
        "latest_failed_checks": latest_report.get("failed_checks", []),
        "latest_agent_status": latest_agent_report.get("status"),
        "latest_agent_diagnoses": latest_agent_report.get("diagnoses", []),
        "latest_agent_fallback": latest_agent_report.get("fallback", {}),
        "latest_alert_severity": latest_alert_report.get("event", {}).get("severity"),
        "latest_alert_should_alert": latest_alert_report.get("event", {}).get("should_alert"),
        "latest_alert_delivery": latest_alert_report.get("delivery", {}),
    }


def artifacts_as_dict(artifacts: PipelineArtifacts) -> dict[str, Any]:
    return asdict(artifacts)
