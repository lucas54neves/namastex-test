from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture(autouse=True)
def _isolate_pipeline_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "PIPELINE_ENABLE_LLM_ENRICHMENT",
        "PIPELINE_ENABLE_LANGFUSE",
        "PIPELINE_ENABLE_LLM_ADVISOR",
        "PIPELINE_LLM_TIMEOUT_SECONDS",
        "PIPELINE_LLM_MAX_RETRIES",
        "PIPELINE_LLM_OPENAI_MODEL",
        "PIPELINE_LLM_ANTHROPIC_MODEL",
        "PIPELINE_LANGFUSE_PROMPT_NAME",
        "PIPELINE_LANGFUSE_PROMPT_LABEL",
        "PIPELINE_LANGFUSE_TRACE_NAME",
        "PIPELINE_LANGFUSE_ALLOW_LOCAL_PROMPT_FALLBACK",
        "PIPELINE_LANGFUSE_BOOTSTRAP_ENABLED",
        "PIPELINE_LANGFUSE_BOOTSTRAP_READY_TIMEOUT_SECONDS",
        "PIPELINE_LANGFUSE_BOOTSTRAP_READY_POLL_INTERVAL_SECONDS",
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "LANGFUSE_BASE_URL",
        "LANGFUSE_PUBLIC_KEY",
        "LANGFUSE_SECRET_KEY",
        "PIPELINE_ALERT_WEBHOOK_URL",
        "PIPELINE_ALERT_SUPPRESSION_MINUTES",
        "PIPELINE_POLL_INTERVAL_SECONDS",
    ):
        monkeypatch.delenv(name, raising=False)
