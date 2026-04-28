from __future__ import annotations

import io
import urllib.error
from unittest.mock import MagicMock, patch

from pipeline.agent.alert_channels import (
    DeliveryResult,
    _build_payload,
    _normalize_severity,
    deliver_alert,
)


def _event(severity: str = "high", should_alert: bool = True) -> dict:
    return {
        "incident_id": "incident_20260428T143022_manual_intervention_required",
        "dedupe_key": "high:manual_intervention_required:check_failed",
        "severity": severity,
        "title": "Pipeline agent status: manual_intervention_required",
        "summary": "Run validation_failed with agent status manual_intervention_required.",
        "should_alert": should_alert,
        "payload": {
            "run_record": {
                "run_at_utc": "2026-04-28T14:30:22+00:00",
                "status": "validation_failed",
            },
            "agent_report": {
                "status": "manual_intervention_required",
                "diagnoses": [{"kind": "silver_masked_text_fields_not_leaking"}],
                "auto_remediation_applied": False,
            },
            "validation_report": {"status": "failed"},
        },
    }


def _mock_response(status: int) -> MagicMock:
    resp = MagicMock()
    resp.status = status
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


# --- Unit: severity normalization ---


def test_normalize_severity_maps_info_to_low() -> None:
    assert _normalize_severity("info") == "low"


def test_normalize_severity_maps_warning_to_medium() -> None:
    assert _normalize_severity("warning") == "medium"


def test_normalize_severity_passes_through_high_and_critical() -> None:
    assert _normalize_severity("high") == "high"
    assert _normalize_severity("critical") == "critical"


# --- Unit: payload building ---


def test_build_payload_structure(monkeypatch) -> None:
    monkeypatch.delenv("PIPELINE_ALERT_DETAILS_URL", raising=False)
    payload = _build_payload(_event(severity="high"))

    assert payload["source"] == "namastex-pipeline-agent"
    assert payload["incident_id"] == "incident_20260428T143022_manual_intervention_required"
    assert payload["severity"] == "high"
    assert payload["status"] == "manual_intervention_required"
    assert payload["pipeline_status"] == "failed"
    assert "silver_masked_text_fields_not_leaking" in payload["failed_checks"]
    assert payload["auto_remediation_applied"] is False
    assert payload["details_url"] is None
    assert "timestamp_utc" in payload


def test_build_payload_includes_details_url(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_ALERT_DETAILS_URL", "https://example.com/incidents/123")
    payload = _build_payload(_event())
    assert payload["details_url"] == "https://example.com/incidents/123"


def test_build_payload_excludes_lead_key_and_schema_fields() -> None:
    payload = _build_payload(_event())
    forbidden = {"lead_key", "masked_text", "conversations", "bronze", "silver", "gold"}
    assert not forbidden.intersection(payload.keys())


# --- deliver_alert: no URL configured ---


def test_deliver_alert_skips_when_no_url(monkeypatch) -> None:
    monkeypatch.delenv("PIPELINE_ALERT_WEBHOOK_URL", raising=False)
    result = deliver_alert(_event())
    assert result.status == "skipped"
    assert result.channel == "none"
    assert result.attempts == 0
    assert result.http_status_code is None


# --- deliver_alert: HTTPS enforcement ---


def test_deliver_alert_skips_http_non_localhost(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_ALERT_WEBHOOK_URL", "http://example.com/hooks/abc")
    result = deliver_alert(_event())
    assert result.status == "skipped"
    assert result.error == "non_https_url"


def test_deliver_alert_allows_http_localhost(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_ALERT_WEBHOOK_URL", "http://localhost:9999/hook")
    with patch("urllib.request.urlopen", return_value=_mock_response(200)):
        result = deliver_alert(_event())
    assert result.status == "delivered"


# --- deliver_alert: severity filter ---


def test_deliver_alert_skips_when_severity_below_min(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_ALERT_WEBHOOK_URL", "https://hooks.example.com/abc")
    monkeypatch.setenv("PIPELINE_ALERT_MIN_SEVERITY", "high")
    result = deliver_alert(_event(severity="warning"))
    assert result.status == "skipped"
    assert result.attempts == 0


def test_deliver_alert_passes_when_severity_meets_min(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_ALERT_WEBHOOK_URL", "https://hooks.example.com/abc")
    monkeypatch.setenv("PIPELINE_ALERT_MIN_SEVERITY", "high")
    with patch("urllib.request.urlopen", return_value=_mock_response(200)):
        result = deliver_alert(_event(severity="critical"))
    assert result.status == "delivered"


# --- deliver_alert: successful delivery ---


def test_deliver_alert_delivers_on_200(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_ALERT_WEBHOOK_URL", "https://hooks.example.com/abc")
    monkeypatch.delenv("PIPELINE_ALERT_WEBHOOK_TOKEN", raising=False)
    with patch("urllib.request.urlopen", return_value=_mock_response(200)):
        result = deliver_alert(_event())
    assert result.status == "delivered"
    assert result.channel == "webhook"
    assert result.attempts == 1
    assert result.http_status_code == 200
    assert result.error is None


def test_deliver_alert_delivers_on_201(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_ALERT_WEBHOOK_URL", "https://hooks.example.com/abc")
    with patch("urllib.request.urlopen", return_value=_mock_response(201)):
        result = deliver_alert(_event())
    assert result.status == "delivered"
    assert result.http_status_code == 201


# --- deliver_alert: retry on failure ---


def test_deliver_alert_retries_on_500_and_fails(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_ALERT_WEBHOOK_URL", "https://hooks.example.com/abc")
    monkeypatch.setenv("PIPELINE_ALERT_WEBHOOK_MAX_RETRIES", "2")

    http_error = urllib.error.HTTPError(
        url="https://hooks.example.com/abc",
        code=500,
        msg="Internal Server Error",
        hdrs=None,  # type: ignore[arg-type]
        fp=io.BytesIO(b""),
    )

    with patch("urllib.request.urlopen", side_effect=http_error):
        with patch("time.sleep"):
            result = deliver_alert(_event())

    assert result.status == "failed"
    assert result.attempts == 2
    assert result.http_status_code == 500


def test_deliver_alert_succeeds_on_second_attempt(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_ALERT_WEBHOOK_URL", "https://hooks.example.com/abc")
    monkeypatch.setenv("PIPELINE_ALERT_WEBHOOK_MAX_RETRIES", "3")

    http_error = urllib.error.HTTPError(
        url="https://hooks.example.com/abc",
        code=503,
        msg="Service Unavailable",
        hdrs=None,  # type: ignore[arg-type]
        fp=io.BytesIO(b""),
    )
    responses = [http_error, _mock_response(200)]

    with patch("urllib.request.urlopen", side_effect=responses):
        with patch("time.sleep"):
            result = deliver_alert(_event())

    assert result.status == "delivered"
    assert result.attempts == 2


# --- deliver_alert: timeout ---


def test_deliver_alert_handles_timeout_without_blocking(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_ALERT_WEBHOOK_URL", "https://hooks.example.com/abc")
    monkeypatch.setenv("PIPELINE_ALERT_WEBHOOK_MAX_RETRIES", "1")
    monkeypatch.setenv("PIPELINE_ALERT_WEBHOOK_TIMEOUT_SECONDS", "1")

    with patch("urllib.request.urlopen", side_effect=TimeoutError("timed out")):
        result = deliver_alert(_event())

    assert result.status == "failed"
    assert result.attempts == 1
    assert result.http_status_code is None


# --- deliver_alert: Bearer token ---


def test_deliver_alert_sends_bearer_token_in_header(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_ALERT_WEBHOOK_URL", "https://hooks.example.com/abc")
    monkeypatch.setenv("PIPELINE_ALERT_WEBHOOK_TOKEN", "supersecret")

    captured_headers: dict = {}

    def fake_urlopen(req, timeout):
        captured_headers.update(req.headers)
        return _mock_response(200)

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        result = deliver_alert(_event())

    assert result.status == "delivered"
    auth = captured_headers.get("Authorization") or captured_headers.get("authorization")
    assert auth == "Bearer supersecret"


def test_deliver_alert_token_not_in_logs(monkeypatch, caplog) -> None:
    monkeypatch.setenv("PIPELINE_ALERT_WEBHOOK_URL", "https://hooks.example.com/abc")
    monkeypatch.setenv("PIPELINE_ALERT_WEBHOOK_TOKEN", "topsecrettoken")
    monkeypatch.setenv("PIPELINE_ALERT_WEBHOOK_MAX_RETRIES", "1")

    import urllib.error as ue

    http_error = ue.HTTPError(
        url="https://hooks.example.com/abc",
        code=500,
        msg="Error",
        hdrs=None,  # type: ignore[arg-type]
        fp=io.BytesIO(b""),
    )

    import logging

    with caplog.at_level(logging.ERROR):
        with patch("urllib.request.urlopen", side_effect=http_error):
            deliver_alert(_event())

    for record in caplog.records:
        assert "topsecrettoken" not in record.getMessage()


# --- deliver_alert: never raises ---


def test_deliver_alert_never_raises_on_unexpected_exception(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_ALERT_WEBHOOK_URL", "https://hooks.example.com/abc")

    with patch("urllib.request.urlopen", side_effect=RuntimeError("unexpected")):
        result = deliver_alert(_event())

    assert result.status == "failed"
    assert result.error is not None


# --- DeliveryResult.as_dict ---


def test_delivery_result_as_dict_shape() -> None:
    r = DeliveryResult(
        channel="webhook", status="delivered", attempts=1, http_status_code=200, error=None
    )
    d = r.as_dict()
    assert set(d.keys()) == {"channel", "status", "attempts", "http_status_code", "error"}
    assert d["status"] == "delivered"
    assert d["http_status_code"] == 200
