from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

logger = logging.getLogger(__name__)

DeliveryStatus = Literal["delivered", "failed", "skipped"]

_SEVERITY_ORDER: dict[str, int] = {"low": 0, "medium": 1, "high": 2, "critical": 3}
_SEVERITY_NORMALIZE: dict[str, str] = {
    "info": "low",
    "warning": "medium",
    "low": "low",
    "medium": "medium",
    "high": "high",
    "critical": "critical",
}


@dataclass(frozen=True)
class DeliveryResult:
    channel: str
    status: DeliveryStatus
    attempts: int
    http_status_code: int | None
    error: str | None

    def as_dict(self) -> dict:
        return {
            "channel": self.channel,
            "status": self.status,
            "attempts": self.attempts,
            "http_status_code": self.http_status_code,
            "error": self.error,
        }


def _clamp(value: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, value))


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _min_severity() -> str:
    raw = os.getenv("PIPELINE_ALERT_MIN_SEVERITY", "medium").strip().lower()
    return raw if raw in _SEVERITY_ORDER else "medium"


def _normalize_severity(severity: str) -> str:
    return _SEVERITY_NORMALIZE.get(severity.lower(), "low")


def _is_test_url(url: str) -> bool:
    try:
        parsed = urllib.parse.urlparse(url)
        return parsed.scheme == "http" and parsed.hostname in ("localhost", "127.0.0.1", "::1")
    except Exception:
        return False


def _build_payload(event: dict) -> dict:
    inner = event.get("payload", {})
    agent_report = inner.get("agent_report", {})
    validation_report = inner.get("validation_report", {})
    run_record = inner.get("run_record", {})

    severity = _normalize_severity(str(event.get("severity", "low")))

    diagnoses = agent_report.get("diagnoses", [])
    failed_checks = (
        [str(d.get("kind", "unknown")) for d in diagnoses if isinstance(d, dict)]
        if isinstance(diagnoses, list)
        else []
    )

    run_at = run_record.get("run_at_utc")
    try:
        timestamp_utc = datetime.fromisoformat(str(run_at)).astimezone(UTC).isoformat()
    except (ValueError, TypeError):
        timestamp_utc = datetime.now(UTC).isoformat()

    details_url = os.getenv("PIPELINE_ALERT_DETAILS_URL", "").strip() or None

    return {
        "source": "namastex-pipeline-agent",
        "incident_id": str(event.get("incident_id", "")),
        "severity": severity,
        "status": str(agent_report.get("status", "unknown")),
        "summary": str(event.get("summary", "")),
        "pipeline_status": str(validation_report.get("status", "unknown")),
        "failed_checks": failed_checks,
        "auto_remediation_applied": bool(agent_report.get("auto_remediation_applied", False)),
        "timestamp_utc": timestamp_utc,
        "details_url": details_url,
    }


def deliver_alert(event: dict) -> DeliveryResult:
    """
    Delivers alert event to configured external HTTP webhook.
    Returns DeliveryResult regardless of success or failure.
    Never raises exceptions.
    """
    webhook_url = os.getenv("PIPELINE_ALERT_WEBHOOK_URL", "").strip()
    if not webhook_url:
        return DeliveryResult(
            channel="none", status="skipped", attempts=0, http_status_code=None, error=None
        )

    try:
        parsed = urllib.parse.urlparse(webhook_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("missing scheme or netloc")
    except Exception as exc:
        logger.warning("Invalid PIPELINE_ALERT_WEBHOOK_URL: %s", exc)
        return DeliveryResult(
            channel="webhook",
            status="skipped",
            attempts=0,
            http_status_code=None,
            error="invalid_url",
        )

    parsed = urllib.parse.urlparse(webhook_url)
    if parsed.scheme != "https" and not _is_test_url(webhook_url):
        logger.warning("PIPELINE_ALERT_WEBHOOK_URL must use HTTPS; skipping delivery")
        return DeliveryResult(
            channel="webhook",
            status="skipped",
            attempts=0,
            http_status_code=None,
            error="non_https_url",
        )

    event_severity = _normalize_severity(str(event.get("severity", "low")))
    min_severity = _min_severity()
    if _SEVERITY_ORDER.get(event_severity, 0) < _SEVERITY_ORDER.get(min_severity, 1):
        return DeliveryResult(
            channel="webhook", status="skipped", attempts=0, http_status_code=None, error=None
        )

    timeout = _clamp(_env_int("PIPELINE_ALERT_WEBHOOK_TIMEOUT_SECONDS", 5), 1, 30)
    max_retries = _clamp(_env_int("PIPELINE_ALERT_WEBHOOK_MAX_RETRIES", 2), 1, 5)
    token = os.getenv("PIPELINE_ALERT_WEBHOOK_TOKEN", "").strip()

    payload_dict = _build_payload(event)
    body = json.dumps(payload_dict, ensure_ascii=False).encode("utf-8")
    headers: dict[str, str] = {"Content-Type": "application/json; charset=utf-8"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    last_status: int | None = None
    last_error: str | None = None

    for attempt in range(1, max_retries + 1):
        try:
            req = urllib.request.Request(webhook_url, data=body, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
                status_code: int = resp.status
                if 200 <= status_code < 300:
                    return DeliveryResult(
                        channel="webhook",
                        status="delivered",
                        attempts=attempt,
                        http_status_code=status_code,
                        error=None,
                    )
                last_status = status_code
                last_error = f"http_{status_code}"
        except urllib.error.HTTPError as exc:
            last_status = exc.code
            last_error = f"http_{exc.code}"
            logger.error(
                "Webhook delivery attempt %d/%d failed: HTTP %d", attempt, max_retries, exc.code
            )
        except Exception as exc:
            last_status = None
            last_error = str(exc)
            logger.error("Webhook delivery attempt %d/%d failed: %s", attempt, max_retries, exc)

        if attempt < max_retries:
            time.sleep(1)

    return DeliveryResult(
        channel="webhook",
        status="failed",
        attempts=max_retries,
        http_status_code=last_status,
        error=last_error,
    )
