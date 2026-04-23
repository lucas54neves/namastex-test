from __future__ import annotations

import base64
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, cast
from urllib import error, parse, request

from pipeline.runtime.env import env_flag

DEFAULT_READY_TIMEOUT_SECONDS = 180
DEFAULT_READY_POLL_INTERVAL_SECONDS = 2

_LANGFUSE_PROMPT_TEMPLATE = (
    "Return valid JSON only with these fields: "
    "sentiment_label, sentiment_confidence_band, intent_stage, persona_profile, "
    "audience_segment, price_objection_intensity, competitor_pressure_level, "
    "commercial_urgency_signal, recommended_next_action, explanation_short. "
    "For every categorical field, you must choose exactly one value from this allowed set and "
    "must not invent synonyms, English labels, or intermediate stages: "
    "{{allowed_values_json}}. "
    "Keep explanation_short privacy-safe and under 280 characters. "
    "Payload: {{payload_json}}"
)


class LangfuseBootstrapError(RuntimeError):
    """Raised when Langfuse bootstrap fails."""


@dataclass(frozen=True)
class LangfuseBootstrapConfig:
    enabled: bool
    base_url: str
    public_key: str
    secret_key: str
    prompt_name: str
    prompt_label: str
    ready_timeout_seconds: int
    ready_poll_interval_seconds: int


def runtime_langfuse_prompt_template() -> str:
    return _LANGFUSE_PROMPT_TEMPLATE


def build_bootstrap_config_from_env() -> LangfuseBootstrapConfig:
    return LangfuseBootstrapConfig(
        enabled=env_flag("PIPELINE_LANGFUSE_BOOTSTRAP_ENABLED", True),
        base_url=os.getenv("LANGFUSE_BASE_URL", "").strip(),
        public_key=os.getenv("LANGFUSE_PUBLIC_KEY", "").strip(),
        secret_key=os.getenv("LANGFUSE_SECRET_KEY", "").strip(),
        prompt_name=os.getenv("PIPELINE_LANGFUSE_PROMPT_NAME", "").strip(),
        prompt_label=os.getenv("PIPELINE_LANGFUSE_PROMPT_LABEL", "").strip() or "production",
        ready_timeout_seconds=int(
            os.getenv(
                "PIPELINE_LANGFUSE_BOOTSTRAP_READY_TIMEOUT_SECONDS",
                str(DEFAULT_READY_TIMEOUT_SECONDS),
            ).strip()
            or DEFAULT_READY_TIMEOUT_SECONDS
        ),
        ready_poll_interval_seconds=max(
            1,
            int(
                os.getenv(
                    "PIPELINE_LANGFUSE_BOOTSTRAP_READY_POLL_INTERVAL_SECONDS",
                    str(DEFAULT_READY_POLL_INTERVAL_SECONDS),
                ).strip()
                or DEFAULT_READY_POLL_INTERVAL_SECONDS
            ),
        ),
    )


def _basic_auth_header(public_key: str, secret_key: str) -> str:
    token = base64.b64encode(f"{public_key}:{secret_key}".encode()).decode("ascii")
    return f"Basic {token}"


def _request_json(
    method: str,
    url: str,
    public_key: str,
    secret_key: str,
    payload: dict[str, Any] | None = None,
    timeout_seconds: int = 10,
) -> tuple[int, dict[str, Any] | None]:
    headers = {
        "Authorization": _basic_auth_header(public_key, secret_key),
        "Accept": "application/json",
    }
    data: bytes | None = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload, ensure_ascii=True).encode("utf-8")
    req = request.Request(url=url, method=method.upper(), headers=headers, data=data)
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            raw = response.read().decode("utf-8").strip()
            return response.status, json.loads(raw) if raw else None
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8").strip()
        try:
            body = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            body = {"raw": raw} if raw else None
        return exc.code, body


def wait_for_langfuse_ready(
    base_url: str,
    timeout_seconds: int,
    poll_interval_seconds: int,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    ready_url = f"{base_url.rstrip('/')}/api/public/ready"
    health_url = f"{base_url.rstrip('/')}/api/public/health?failIfDatabaseUnavailable=true"
    last_status: str | None = None
    while time.monotonic() < deadline:
        try:
            with request.urlopen(ready_url, timeout=5) as response:
                if response.status == 200:
                    with request.urlopen(health_url, timeout=5) as health_response:
                        if health_response.status == 200:
                            return
                        last_status = f"health_status={health_response.status}"
                else:
                    last_status = f"ready_status={response.status}"
        except Exception as exc:
            last_status = str(exc)
        time.sleep(poll_interval_seconds)
    raise LangfuseBootstrapError(
        f"langfuse_not_ready_after_{timeout_seconds}_seconds:{last_status or 'unknown'}"
    )


def _extract_prompt_record(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not payload:
        return None
    data = payload.get("data")
    if isinstance(data, dict):
        return cast(dict[str, Any], data)
    return payload


def _extract_prompt_body(prompt_record: dict[str, Any] | None) -> str | None:
    if not prompt_record:
        return None
    prompt = prompt_record.get("prompt")
    if isinstance(prompt, str):
        return prompt
    if isinstance(prompt_record.get("data"), dict):
        nested_prompt = prompt_record["data"].get("prompt")
        if isinstance(nested_prompt, str):
            return nested_prompt
    return None


def _extract_prompt_type(prompt_record: dict[str, Any] | None) -> str | None:
    if not prompt_record:
        return None
    for key in ("type", "promptType"):
        value = prompt_record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _extract_prompt_version(prompt_record: dict[str, Any] | None) -> int | str | None:
    if not prompt_record:
        return None
    for key in ("version", "versionId", "id"):
        value = prompt_record.get(key)
        if isinstance(value, int | str) and str(value).strip():
            return value
    return None


def fetch_prompt_by_label(
    base_url: str,
    public_key: str,
    secret_key: str,
    prompt_name: str,
    prompt_label: str,
) -> dict[str, Any] | None:
    encoded_name = parse.quote(prompt_name, safe="")
    encoded_label = parse.quote(prompt_label, safe="")
    status, body = _request_json(
        "GET",
        f"{base_url.rstrip('/')}/api/public/v2/prompts/{encoded_name}?label={encoded_label}",
        public_key,
        secret_key,
    )
    if status == 404:
        return None
    if status >= 400:
        raise LangfuseBootstrapError(f"prompt_lookup_failed:status={status}")
    return _extract_prompt_record(body)


def create_prompt_version(
    base_url: str,
    public_key: str,
    secret_key: str,
    prompt_name: str,
    prompt_label: str,
    prompt_template: str,
) -> dict[str, Any] | None:
    status, body = _request_json(
        "POST",
        f"{base_url.rstrip('/')}/api/public/v2/prompts",
        public_key,
        secret_key,
        payload={
            "name": prompt_name,
            "type": "text",
            "prompt": prompt_template,
            "labels": [prompt_label],
        },
    )
    if status >= 400:
        detail = None
        if isinstance(body, dict):
            detail = body.get("message") or body.get("error")
        raise LangfuseBootstrapError(
            f"prompt_create_failed:status={status}:{str(detail or 'unknown').strip()}"
        )
    return _extract_prompt_record(body)


def ensure_langfuse_prompt(
    base_url: str,
    public_key: str,
    secret_key: str,
    prompt_name: str,
    prompt_label: str,
    prompt_template: str,
) -> dict[str, object]:
    existing = fetch_prompt_by_label(
        base_url=base_url,
        public_key=public_key,
        secret_key=secret_key,
        prompt_name=prompt_name,
        prompt_label=prompt_label,
    )
    if (
        existing is not None
        and _extract_prompt_type(existing) == "text"
        and _extract_prompt_body(existing) == prompt_template
    ):
        return {
            "status": "unchanged",
            "prompt_name": prompt_name,
            "prompt_label": prompt_label,
            "prompt_version": _extract_prompt_version(existing),
            "detail": "existing_prompt_already_matches_repository_template",
        }

    created = create_prompt_version(
        base_url=base_url,
        public_key=public_key,
        secret_key=secret_key,
        prompt_name=prompt_name,
        prompt_label=prompt_label,
        prompt_template=prompt_template,
    )
    return {
        "status": "created" if existing is None else "updated",
        "prompt_name": prompt_name,
        "prompt_label": prompt_label,
        "prompt_version": _extract_prompt_version(created),
        "detail": "prompt_version_upserted",
    }


def run_langfuse_prompt_bootstrap(
    config: LangfuseBootstrapConfig | None = None,
) -> dict[str, object]:
    resolved = config or build_bootstrap_config_from_env()
    if not resolved.enabled:
        return {"status": "skipped", "detail": "bootstrap_disabled"}

    missing = [
        name
        for name, value in (
            ("LANGFUSE_BASE_URL", resolved.base_url),
            ("LANGFUSE_PUBLIC_KEY", resolved.public_key),
            ("LANGFUSE_SECRET_KEY", resolved.secret_key),
            ("PIPELINE_LANGFUSE_PROMPT_NAME", resolved.prompt_name),
        )
        if not value
    ]
    if missing:
        return {
            "status": "skipped",
            "detail": f"missing_required_env:{','.join(missing)}",
        }

    wait_for_langfuse_ready(
        base_url=resolved.base_url,
        timeout_seconds=resolved.ready_timeout_seconds,
        poll_interval_seconds=resolved.ready_poll_interval_seconds,
    )
    return ensure_langfuse_prompt(
        base_url=resolved.base_url,
        public_key=resolved.public_key,
        secret_key=resolved.secret_key,
        prompt_name=resolved.prompt_name,
        prompt_label=resolved.prompt_label,
        prompt_template=runtime_langfuse_prompt_template(),
    )


def main() -> int:
    try:
        result = run_langfuse_prompt_bootstrap()
        print(json.dumps(result, ensure_ascii=True))
        return 0 if result["status"] in {"skipped", "unchanged", "created", "updated"} else 1
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "detail": str(exc).strip() or exc.__class__.__name__,
                },
                ensure_ascii=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
