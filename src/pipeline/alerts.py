from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import error, request

from pipeline.io import read_json, write_json

ALERTABLE_AGENT_STATUSES = {
    "degraded_validation_failed",
    "fallback_applied",
    "manual_intervention_required",
}

SEVERITY_BY_AGENT_STATUS = {
    "healthy": "info",
    "idle_no_source_change": "info",
    "auto_remediated": "warning",
    "degraded_validation_failed": "high",
    "fallback_applied": "high",
    "manual_intervention_required": "critical",
}


@dataclass(frozen=True)
class AlertEvent:
    incident_id: str
    severity: str
    title: str
    summary: str
    should_alert: bool
    payload: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "severity": self.severity,
            "title": self.title,
            "summary": self.summary,
            "should_alert": self.should_alert,
            "payload": self.payload,
        }


def _incident_id(run_at_utc: str, agent_status: str) -> str:
    normalized_run_at = run_at_utc.replace(":", "").replace("-", "")
    return f"incident_{normalized_run_at}_{agent_status}"


def build_alert_event(
    run_record: dict[str, Any],
    agent_report: dict[str, Any],
    validation_report: dict[str, Any],
) -> AlertEvent:
    agent_status = str(agent_report.get("status", "unknown"))
    run_at_utc = str(run_record.get("run_at_utc", "unknown"))
    severity = SEVERITY_BY_AGENT_STATUS.get(agent_status, "high")
    diagnoses = agent_report.get("diagnoses", [])
    should_alert = agent_status in ALERTABLE_AGENT_STATUSES
    title = f"Pipeline agent status: {agent_status}"
    summary = (
        f"Run {run_record.get('status')} with agent status {agent_status}. "
        f"Diagnoses: {len(diagnoses)}. Validation: {validation_report.get('status')}."
    )
    payload = {
        "run_record": run_record,
        "agent_report": agent_report,
        "validation_report": validation_report,
    }
    return AlertEvent(
        incident_id=_incident_id(run_at_utc, agent_status),
        severity=severity,
        title=title,
        summary=summary,
        should_alert=should_alert,
        payload=payload,
    )


def persist_incident(alerts_dir: Path, event: AlertEvent) -> Path:
    incident_path = alerts_dir / f"{event.incident_id}.json"
    write_json(event.as_dict(), incident_path)
    return incident_path


def append_alert_history(alerts_dir: Path, event: AlertEvent, incident_path: Path) -> Path:
    history_path = alerts_dir / "alert_history.json"
    history = read_json(history_path, default={"events": []})
    history.setdefault("events", []).append(
        {
            "incident_id": event.incident_id,
            "severity": event.severity,
            "title": event.title,
            "summary": event.summary,
            "should_alert": event.should_alert,
            "incident_path": str(incident_path),
        }
    )
    write_json(history, history_path)
    return history_path


def deliver_webhook(event: AlertEvent) -> dict[str, Any]:
    webhook_url = os.getenv("PIPELINE_ALERT_WEBHOOK_URL", "").strip()
    if not webhook_url:
        return {"attempted": False, "delivered": False, "reason": "webhook_not_configured"}

    body = json.dumps(event.as_dict()).encode("utf-8")
    req = request.Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=10) as response:  # noqa: S310
            return {
                "attempted": True,
                "delivered": True,
                "status_code": response.status,
            }
    except error.URLError as exc:
        return {
            "attempted": True,
            "delivered": False,
            "reason": str(exc),
        }


def handle_alerting(
    alerts_dir: Path,
    run_record: dict[str, Any],
    agent_report: dict[str, Any],
    validation_report: dict[str, Any],
) -> dict[str, Any]:
    event = build_alert_event(run_record, agent_report, validation_report)
    incident_path = persist_incident(alerts_dir, event)
    history_path = append_alert_history(alerts_dir, event, incident_path)
    delivery = {"attempted": False, "delivered": False}
    if event.should_alert:
        delivery = deliver_webhook(event)
    return {
        "event": event.as_dict(),
        "incident_path": str(incident_path),
        "history_path": str(history_path),
        "delivery": delivery,
    }
