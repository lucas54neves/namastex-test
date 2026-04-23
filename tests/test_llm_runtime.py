from __future__ import annotations

import json
from typing import Any

from pipeline.orchestration.compiler import get_default_compiled_plan
from pipeline.runtime.llm_runtime import (
    _build_prompt,
    resolve_runtime_config,
    resolve_runtime_prompt,
    run_conversation_enrichment_graph,
)
from pipeline.transforms.conversation_enrichment import (
    _validate_llm_response,
    normalize_llm_output,
)


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
            "langfuse": {
                "enabled": False,
                "prompt_name": "conversation-enrichment-v1",
                "prompt_label": "production",
                "trace_name": "conversation-enrichment",
                "allow_local_prompt_fallback": True,
            },
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
    assert config["langfuse"]["trace_name"] == "conversation-enrichment"


def test_normalize_llm_output_maps_supported_synonyms() -> None:
    normalized, decisions = normalize_llm_output(
        {
            **_valid_output(),
            "sentiment_label": " Negative ",
            "sentiment_confidence_band": "HIGH",
        },
        _compiled_plan(),
    )

    assert normalized["sentiment_label"] == "negativo"
    assert normalized["sentiment_confidence_band"] == "forte"
    assert decisions == [
        {"field": "sentiment_label", "from": "Negative", "to": "negativo"},
        {"field": "sentiment_confidence_band", "from": "HIGH", "to": "forte"},
    ]


def test_normalize_llm_output_corrects_close_typo_against_allowed_vocab() -> None:
    normalized, decisions = normalize_llm_output(
        {
            **_valid_output(),
            "audience_segment": "oferta_competititiva",
        },
        _compiled_plan(),
    )

    assert normalized["audience_segment"] == "oferta_competitiva"
    assert decisions == [
        {
            "field": "audience_segment",
            "from": "oferta_competititiva",
            "to": "oferta_competitiva",
        }
    ]


def test_validate_llm_response_keeps_ambiguous_values_invalid() -> None:
    payload = {**_valid_output(), "sentiment_label": "skeptical"}

    assert _validate_llm_response(payload, _compiled_plan()) == "invalid_sentiment_label:skeptical"


def test_build_prompt_includes_allowed_vocabularies() -> None:
    prompt = _build_prompt(_payload(), _compiled_plan())
    expected_intent_vocab = (
        '"intent_stage": ["cotacao_ativa", "descoberta_inicial", '
        '"pesquisa_mercado", "pos_sinistro"]'
    )

    assert expected_intent_vocab in prompt
    assert '"sentiment_confidence_band": ["forte", "fraco", "moderado", "sem_evidencia"]' in prompt
    assert "must not invent synonyms" in prompt


def test_resolve_runtime_prompt_uses_local_prompt_when_langfuse_disabled() -> None:
    resolution = resolve_runtime_prompt(_payload(), _compiled_plan())

    assert resolution["source"] == "local"
    assert resolution["prompt_version"] == "v1"
    assert resolution["prompt_name"] is None
    assert "Payload:" in resolution["prompt_text"]


def test_resolve_runtime_prompt_fetches_langfuse_managed_prompt(monkeypatch) -> None:
    import pipeline.runtime.llm_runtime as llm_runtime

    class FakePrompt:
        version = "lf-v7"

        def compile(self, **kwargs):
            return f"Allowed={kwargs['allowed_values_json']} Payload={kwargs['payload_json']}"

    class FakeClient:
        def get_prompt(self, name, label=None):
            assert name == "conversation-enrichment-v1"
            assert label == "production"
            return FakePrompt()

    monkeypatch.setenv("PIPELINE_ENABLE_LANGFUSE", "1")
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk")
    monkeypatch.setattr(llm_runtime, "_get_langfuse_client", lambda config: FakeClient())

    resolution = resolve_runtime_prompt(_payload(), _compiled_plan())

    assert resolution["source"] == "langfuse"
    assert resolution["prompt_version"] == "lf-v7"
    assert resolution["prompt_name"] == "conversation-enrichment-v1"
    assert "Payload=" in resolution["prompt_text"]


def test_resolve_runtime_prompt_does_not_require_langchain_callback(monkeypatch) -> None:
    import pipeline.runtime.llm_runtime as llm_runtime

    class FakePrompt:
        version = "lf-v8"

        def compile(self, **kwargs):
            del kwargs
            return "managed prompt"

    class FakeClient:
        def get_prompt(self, name, label=None):
            del name
            del label
            return FakePrompt()

    monkeypatch.setenv("PIPELINE_ENABLE_LANGFUSE", "1")
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk")
    monkeypatch.setattr(llm_runtime, "LangfuseCallbackHandler", None)
    monkeypatch.setattr(llm_runtime, "_get_langfuse_client", lambda config: FakeClient())

    resolution = resolve_runtime_prompt(_payload(), _compiled_plan())

    assert resolution["source"] == "langfuse"
    assert resolution["prompt_version"] == "lf-v8"


def test_resolve_runtime_prompt_falls_back_locally_on_langfuse_failure(monkeypatch) -> None:
    import pipeline.runtime.llm_runtime as llm_runtime

    class FakeClient:
        def get_prompt(self, name, label=None):
            del name
            del label
            raise RuntimeError("prompt_lookup_failed")

    monkeypatch.setenv("PIPELINE_ENABLE_LANGFUSE", "1")
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk")
    monkeypatch.setattr(llm_runtime, "_get_langfuse_client", lambda config: FakeClient())

    resolution = resolve_runtime_prompt(_payload(), _compiled_plan())

    assert resolution["source"] == "local_fallback"
    assert resolution["prompt_version"] == "v1"
    assert resolution["resolution_error"] == "prompt_lookup_failed"


def test_run_conversation_enrichment_graph_falls_back_to_anthropic(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_ENABLE_LLM_ENRICHMENT", "1")
    monkeypatch.setenv("OPENAI_API_KEY", "openai-key")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "anthropic-key")

    import pipeline.runtime.llm_runtime as llm_runtime

    def fake_openai(prompt_text, config, langfuse_context):
        del prompt_text
        del config
        del langfuse_context
        return {"intent_stage": ""}

    def fake_anthropic(prompt_text, config, langfuse_context):
        del prompt_text
        del config
        del langfuse_context
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
    assert result["prompt_source"] == "local"
    assert result["trace_id"] is None


def test_run_conversation_enrichment_graph_accepts_normalized_openai_output(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_ENABLE_LLM_ENRICHMENT", "1")
    monkeypatch.setenv("OPENAI_API_KEY", "openai-key")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "anthropic-key")

    import pipeline.runtime.llm_runtime as llm_runtime

    def fake_openai(prompt_text, config, langfuse_context):
        del prompt_text
        del config
        del langfuse_context
        return {
            **_valid_output(),
            "sentiment_label": "negative",
            "sentiment_confidence_band": "high",
            "intent_stage": "quote_request",
        }

    def fake_anthropic(prompt_text, config, langfuse_context):
        del prompt_text
        del config
        del langfuse_context
        raise AssertionError("anthropic should not be called when normalization succeeds")

    monkeypatch.setattr(llm_runtime, "_invoke_openai", fake_openai)
    monkeypatch.setattr(llm_runtime, "_invoke_anthropic", fake_anthropic)

    result = run_conversation_enrichment_graph(
        _payload(),
        _compiled_plan(),
        validator=_validate_llm_response,
    )

    assert result["status"] == "success"
    assert result["provider_name"] == "openai"
    assert result["attempted_providers"] == ["openai"]
    assert result["provider_errors"] == []
    assert result["prompt_version"] == "v1"


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


def test_run_conversation_enrichment_graph_attaches_langfuse_callbacks(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_ENABLE_LLM_ENRICHMENT", "1")
    monkeypatch.setenv("PIPELINE_ENABLE_LANGFUSE", "1")
    monkeypatch.setenv("OPENAI_API_KEY", "openai-key")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "anthropic-key")
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk")

    import pipeline.runtime.llm_runtime as llm_runtime

    class FakeHandler:
        last_trace_id = "trace-from-handler"

    class FakeContextManager:
        def __enter__(self):
            class RootObservation:
                def update(self, **kwargs):
                    self.payload = kwargs

            return RootObservation()

        def __exit__(self, exc_type, exc, tb):
            del exc_type
            del exc
            del tb

    class FakeClient:
        def start_as_current_observation(self, **kwargs):
            assert kwargs["name"] == "conversation-enrichment"
            return FakeContextManager()

    captured: dict[str, Any] = {}

    class FakeOpenAI:
        def __init__(self, **kwargs):
            captured["init"] = kwargs

        def invoke(self, prompt_text, config=None):
            captured["prompt_text"] = prompt_text
            captured["config"] = config
            return json.dumps(_valid_output())

    monkeypatch.setattr(llm_runtime, "_get_langfuse_client", lambda config: FakeClient())
    monkeypatch.setattr(llm_runtime, "LangfuseCallbackHandler", FakeHandler)
    monkeypatch.setattr(llm_runtime, "ChatOpenAI", FakeOpenAI)
    monkeypatch.setattr(llm_runtime, "ChatAnthropic", object())

    result = run_conversation_enrichment_graph(
        {**_payload(), "llm_input_hash": "hash_123"},
        _compiled_plan(),
        validator=_validate_llm_response,
    )

    assert result["status"] == "success"
    assert result["trace_id"] == "trace-from-handler"
    assert captured["config"]["callbacks"][0].__class__ is FakeHandler
    assert captured["config"]["metadata"]["llm_input_hash"] == "hash_123"
    assert captured["config"]["metadata"]["provider_name"] == "openai"


def test_run_conversation_enrichment_graph_starts_langfuse_root_span_without_callback(
    monkeypatch,
) -> None:
    monkeypatch.setenv("PIPELINE_ENABLE_LLM_ENRICHMENT", "1")
    monkeypatch.setenv("PIPELINE_ENABLE_LANGFUSE", "1")
    monkeypatch.setenv("OPENAI_API_KEY", "openai-key")
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk")

    import pipeline.runtime.llm_runtime as llm_runtime

    updates: list[dict[str, Any]] = []

    class FakeContextManager:
        def __enter__(self):
            class RootObservation:
                def update(self, **kwargs):
                    updates.append(kwargs)

            return RootObservation()

        def __exit__(self, exc_type, exc, tb):
            del exc_type
            del exc
            del tb

    class FakeClient:
        def start_as_current_observation(self, **kwargs):
            assert kwargs["name"] == "conversation-enrichment"
            assert kwargs["trace_context"]["trace_id"]
            return FakeContextManager()

    class FakeOpenAI:
        def __init__(self, **kwargs):
            del kwargs

        def invoke(self, prompt_text, config=None):
            del prompt_text
            assert config is None
            return json.dumps(_valid_output())

    monkeypatch.setattr(llm_runtime, "_get_langfuse_client", lambda config: FakeClient())
    monkeypatch.setattr(llm_runtime, "LangfuseCallbackHandler", None)
    monkeypatch.setattr(llm_runtime, "ChatOpenAI", FakeOpenAI)

    result = run_conversation_enrichment_graph(
        {**_payload(), "llm_input_hash": "hash_123"},
        _compiled_plan(),
        validator=_validate_llm_response,
    )

    assert result["status"] == "success"
    assert result["trace_id"]
    assert updates[0]["metadata"]["llm_input_hash"] == "hash_123"
    assert updates[-1]["output"]["status"] == "success"
