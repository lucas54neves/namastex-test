from __future__ import annotations

from typing import Any

from pipeline.compiler import get_default_compiled_plan
from pipeline.conversation_enrichment import _validate_llm_response
from pipeline.llm_runtime import resolve_runtime_config, run_conversation_enrichment_graph


def _payload() -> dict[str, Any]:
    return {
        "conversation_id": "conv_1",
        "lead_key": "lead_1",
        "prompt_version": "v1",
        "conversation_statistics": {"message_count": 1},
        "messages": [
            {
                "timestamp": "2026-02-01T10:00:00+00:00",
                "direction": "inbound",
                "message_type": "text",
                "text": "quero cotacao",
            }
        ],
    }


def _compiled_plan() -> dict[str, Any]:
    return {
        **get_default_compiled_plan(),
        "llm": {
            "enabled": True,
            "prompt_version": "v1",
            "timeout_seconds": 20,
            "max_retries": 1,
            "providers": {
                "openai": {"enabled": True, "model": "gpt-5-mini"},
                "anthropic": {"enabled": True, "model": "claude-sonnet"},
            },
        },
    }


def _valid_output() -> dict[str, Any]:
    return {
        "sentiment_label": "neutro",
        "sentiment_confidence_band": "moderado",
        "intent_stage": "cotacao_ativa",
        "persona_profile": "cotador_comparador",
        "audience_segment": "oferta_competitiva",
        "price_objection_intensity": "forte",
        "competitor_pressure_level": "alta",
        "commercial_urgency_signal": "moderada",
        "recommended_next_action": "reforcar_diferenciais_e_retirar_objecao_preco",
        "explanation_short": "Lead compara concorrente e segue em cotacao ativa.",
    }


def test_resolve_runtime_config_supports_provider_structure(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_ENABLE_LLM_ENRICHMENT", "1")
    config = resolve_runtime_config(_compiled_plan())

    assert config["enabled"] is True
    assert config["providers"]["openai"]["model"] == "gpt-5-mini"
    assert config["providers"]["anthropic"]["model"] == "claude-sonnet"


def test_run_conversation_enrichment_graph_falls_back_to_anthropic(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_ENABLE_LLM_ENRICHMENT", "1")
    monkeypatch.setenv("OPENAI_API_KEY", "openai-key")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "anthropic-key")

    import pipeline.llm_runtime as llm_runtime

    def fake_openai(payload, config):
        del payload
        del config
        return {"intent_stage": ""}

    def fake_anthropic(payload, config):
        del payload
        del config
        return _valid_output()

    monkeypatch.setattr(llm_runtime, "_invoke_openai", fake_openai)
    monkeypatch.setattr(llm_runtime, "_invoke_anthropic", fake_anthropic)

    result = run_conversation_enrichment_graph(
        _payload(),
        _compiled_plan(),
        validator=_validate_llm_response,
    )

    assert result["status"] == "success"
    assert result["provider_name"] == "anthropic"
    assert result["attempted_providers"] == ["openai", "anthropic"]
    assert result["provider_errors"] == [
        {
            "provider": "openai",
            "kind": "invalid_output",
            "detail": (
                "missing_fields:sentiment_label,sentiment_confidence_band,"
                "persona_profile,audience_segment,price_objection_intensity,"
                "competitor_pressure_level,commercial_urgency_signal,"
                "recommended_next_action"
            ),
        }
    ]


def test_run_conversation_enrichment_graph_returns_fallback_when_credentials_missing(
    monkeypatch,
) -> None:
    monkeypatch.setenv("PIPELINE_ENABLE_LLM_ENRICHMENT", "1")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    result = run_conversation_enrichment_graph(
        _payload(),
        _compiled_plan(),
        validator=_validate_llm_response,
    )

    assert result["status"] == "fallback"
    assert result["provider_name"] is None
    assert result["attempted_providers"] == ["openai", "anthropic"]
