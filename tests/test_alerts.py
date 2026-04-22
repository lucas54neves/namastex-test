from __future__ import annotations

import json
from pathlib import Path

from pipeline.agent.alerts import build_alert_event, handle_alerting


def _run_record(
    status: str = "validation_failed",
    run_at_utc: str = "2026-04-20T22:00:00+00:00",
) -> dict:
    return {
        "run_at_utc": run_at_utc,
        "executed": True,
        "status": status,
        "source_fingerprint": {
            "path": "docs/conversations_bronze.parquet",
            "size_bytes": 100,
            "modified_time": 1.0,
        },
    }


def test_build_alert_event_marks_degraded_status_for_alerting() -> None:
    event = build_alert_event(
        run_record=_run_record(),
        agent_report={
            "status": "degraded_validation_failed",
            "diagnoses": [{"kind": "silver_deduplication_failure"}],
        },
        validation_report={"status": "failed"},
    )

    assert event.should_alert is True
    assert event.severity == "high"
    assert "silver_deduplication_failure" in event.dedupe_key


def test_build_alert_event_keeps_healthy_status_as_info() -> None:
    event = build_alert_event(
        run_record=_run_record(status="success"),
        agent_report={"status": "healthy", "diagnoses": []},
        validation_report={"status": "passed"},
    )

    assert event.should_alert is False
    assert event.severity == "info"


def test_handle_alerting_persists_incident_and_history(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("PIPELINE_ALERT_WEBHOOK_URL", raising=False)
    result = handle_alerting(
        alerts_dir=tmp_path,
        run_record=_run_record(),
        agent_report={
            "status": "manual_intervention_required",
            "diagnoses": [{"kind": "unexpected_runtime_error"}],
        },
        validation_report={"status": "failed"},
    )

    incident_path = Path(result["incident_path"])
    history_path = Path(result["history_path"])
    incident = json.loads(incident_path.read_text(encoding="utf-8"))
    history = json.loads(history_path.read_text(encoding="utf-8"))

    assert incident["should_alert"] is True
    assert incident["severity"] == "critical"
    assert result["delivery"]["attempted"] is False
    assert result["suppression"]["suppressed"] is False
    assert history["events"][-1]["incident_id"] == incident["incident_id"]


def test_handle_alerting_suppresses_duplicate_alert_within_window(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("PIPELINE_ALERT_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("PIPELINE_ALERT_SUPPRESSION_MINUTES", "60")

    first = handle_alerting(
        alerts_dir=tmp_path,
        run_record=_run_record(run_at_utc="2026-04-20T22:00:00+00:00"),
        agent_report={
            "status": "degraded_validation_failed",
            "diagnoses": [{"kind": "silver_deduplication_failure"}],
        },
        validation_report={"status": "failed"},
    )
    second = handle_alerting(
        alerts_dir=tmp_path,
        run_record=_run_record(run_at_utc="2026-04-20T22:10:00+00:00"),
        agent_report={
            "status": "degraded_validation_failed",
            "diagnoses": [{"kind": "silver_deduplication_failure"}],
        },
        validation_report={"status": "failed"},
    )

    history = json.loads((tmp_path / "alert_history.json").read_text(encoding="utf-8"))

    assert first["suppression"]["suppressed"] is False
    assert second["suppression"]["suppressed"] is True
    assert second["delivery"]["suppressed"] is True
    assert second["suppression"]["matched_incident_id"] == first["event"]["incident_id"]
    assert len(history["events"]) == 2


def test_handle_alerting_does_not_suppress_after_window_expires(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("PIPELINE_ALERT_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("PIPELINE_ALERT_SUPPRESSION_MINUTES", "5")

    handle_alerting(
        alerts_dir=tmp_path,
        run_record=_run_record(run_at_utc="2026-04-20T22:00:00+00:00"),
        agent_report={
            "status": "degraded_validation_failed",
            "diagnoses": [{"kind": "silver_deduplication_failure"}],
        },
        validation_report={"status": "failed"},
    )
    second = handle_alerting(
        alerts_dir=tmp_path,
        run_record=_run_record(run_at_utc="2026-04-20T22:10:00+00:00"),
        agent_report={
            "status": "degraded_validation_failed",
            "diagnoses": [{"kind": "silver_deduplication_failure"}],
        },
        validation_report={"status": "failed"},
    )

    assert second["suppression"]["suppressed"] is False
