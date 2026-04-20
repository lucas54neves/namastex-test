from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

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
    executed: bool
    status: str


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _state_file(paths: PipelinePaths) -> Path:
    return paths.state / "pipeline_state.json"


def _validation_report_file(paths: PipelinePaths) -> Path:
    return paths.monitoring / "latest_run_report.json"


def _run_record(
    source_fingerprint: dict[str, Any],
    executed: bool,
    status: str,
    validation_summary: dict[str, Any] | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "run_at_utc": _utc_now_iso(),
        "executed": executed,
        "status": status,
        "source_fingerprint": source_fingerprint,
        "validation_summary": validation_summary or {},
        "details": details or {},
    }


def _skip_artifacts(paths: PipelinePaths) -> PipelineArtifacts:
    return PipelineArtifacts(
        bronze_path=str(paths.bronze / "conversations.parquet"),
        silver_path=str(paths.silver / "conversations_silver.parquet"),
        gold_path=str(paths.gold / "conversations_gold.parquet"),
        state_path=str(_state_file(paths)),
        validation_report_path=str(_validation_report_file(paths)),
        executed=False,
        status="skipped_no_source_change",
    )


def run_pipeline(paths: PipelinePaths, force: bool = False) -> PipelineArtifacts:
    ensure_directories(paths)

    state_path = _state_file(paths)
    report_path = _validation_report_file(paths)
    current_fingerprint = build_source_fingerprint(paths.raw_bronze_source).as_dict()
    state = load_pipeline_state(state_path)
    previous_fingerprint = state.get("last_source_fingerprint")

    if not force and not has_source_changed(
        build_source_fingerprint(paths.raw_bronze_source), previous_fingerprint
    ):
        state.setdefault("runs", []).append(
            _run_record(
                source_fingerprint=current_fingerprint,
                executed=False,
                status="skipped_no_source_change",
                details={"reason": "source fingerprint unchanged"},
            )
        )
        state["last_seen_at_utc"] = _utc_now_iso()
        save_pipeline_state(state_path, state)
        return _skip_artifacts(paths)

    bronze_df = load_bronze_frame(str(paths.raw_bronze_source))
    bronze_path = paths.bronze / "conversations.parquet"
    silver_path = paths.silver / "conversations_silver.parquet"
    gold_path = paths.gold / "conversations_gold.parquet"

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
    write_json(validation_summary, report_path)

    run_status = "success" if validation_summary["status"] == "passed" else "validation_failed"
    state.setdefault("runs", []).append(
        _run_record(
            source_fingerprint=current_fingerprint,
            executed=True,
            status=run_status,
            validation_summary={
                "status": validation_summary["status"],
                "failed_checks": validation_summary["failed_checks"],
            },
            details=validation_summary["row_counts"],
        )
    )
    state["last_source_fingerprint"] = current_fingerprint
    state["last_successful_run_at_utc"] = (
        _utc_now_iso() if run_status == "success" else state.get("last_successful_run_at_utc")
    )
    state["last_seen_at_utc"] = _utc_now_iso()
    save_pipeline_state(state_path, state)

    return PipelineArtifacts(
        bronze_path=str(bronze_path),
        silver_path=str(silver_path),
        gold_path=str(gold_path),
        state_path=str(state_path),
        validation_report_path=str(report_path),
        executed=True,
        status=run_status,
    )


def build_monitor_snapshot(paths: PipelinePaths) -> dict[str, Any]:
    state_path = _state_file(paths)
    report_path = _validation_report_file(paths)
    state = load_pipeline_state(state_path)
    latest_report = read_json(report_path, default={})
    last_run = state.get("runs", [])[-1] if state.get("runs") else {}
    return {
        "state_path": str(state_path),
        "validation_report_path": str(report_path),
        "last_run": last_run,
        "run_count": len(state.get("runs", [])),
        "last_source_fingerprint": state.get("last_source_fingerprint"),
        "latest_validation_status": latest_report.get("status"),
        "latest_failed_checks": latest_report.get("failed_checks", []),
    }


def artifacts_as_dict(artifacts: PipelineArtifacts) -> dict[str, Any]:
    return asdict(artifacts)
