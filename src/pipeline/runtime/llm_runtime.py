from __future__ import annotations

import json
import os
from collections.abc import Callable
from typing import Any, TypedDict, cast

from pipeline.runtime.env import env_flag

try:
    from langgraph.graph import END, START, StateGraph
except Exception:  # pragma: no cover
    END = "__end__"
    START = "__start__"
    StateGraph = None

try:
    from langchain_anthropic import ChatAnthropic
except Exception:  # pragma: no cover
    ChatAnthropic = None

try:
    from langchain_openai import ChatOpenAI
except Exception:  # pragma: no cover
    ChatOpenAI = None


class ProviderRuntimeConfig(TypedDict):
    enabled: bool
    model: str
    timeout_seconds: int
    max_retries: int
    api_key_env: str


class RuntimeConfig(TypedDict):
    enabled: bool
    prompt_version: str
    timeout_seconds: int
    max_retries: int
    providers: dict[str, ProviderRuntimeConfig]


class GraphState(TypedDict):
    conversation_id: str
    lead_key: str
    payload: dict[str, Any]
    attempted_providers: list[str]
    provider_result: dict[str, Any] | None
    provider_errors: list[dict[str, str]]
    selected_provider: str | None
    selected_model: str | None
    validation_error: str | None
    final_status: str | None
    current_provider: str | None
    pending_provider: str | None


def _safe_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def resolve_runtime_config(compiled_plan: dict[str, Any]) -> RuntimeConfig:
    llm_cfg = cast(dict[str, Any], compiled_plan.get("llm") or {})
    providers_cfg = cast(dict[str, Any], llm_cfg.get("providers") or {})
    timeout_seconds = int(
        _safe_string(os.getenv("PIPELINE_LLM_TIMEOUT_SECONDS")).strip()
        or llm_cfg.get("timeout_seconds")
        or 20
    )
    max_retries = int(
        _safe_string(os.getenv("PIPELINE_LLM_MAX_RETRIES")).strip()
        or llm_cfg.get("max_retries")
        or 1
    )
    openai_cfg = cast(dict[str, Any], providers_cfg.get("openai") or {})
    anthropic_cfg = cast(dict[str, Any], providers_cfg.get("anthropic") or {})
    return {
        "enabled": env_flag("PIPELINE_ENABLE_LLM_ENRICHMENT", bool(llm_cfg.get("enabled"))),
        "prompt_version": _safe_string(llm_cfg.get("prompt_version") or "v1"),
        "timeout_seconds": timeout_seconds,
        "max_retries": max_retries,
        "providers": {
            "openai": {
                "enabled": bool(openai_cfg.get("enabled", True)),
                "model": _safe_string(os.getenv("PIPELINE_LLM_OPENAI_MODEL")).strip()
                or _safe_string(openai_cfg.get("model") or llm_cfg.get("model") or "gpt-5-mini"),
                "timeout_seconds": timeout_seconds,
                "max_retries": max_retries,
                "api_key_env": "OPENAI_API_KEY",
            },
            "anthropic": {
                "enabled": bool(anthropic_cfg.get("enabled", True)),
                "model": _safe_string(os.getenv("PIPELINE_LLM_ANTHROPIC_MODEL")).strip()
                or _safe_string(anthropic_cfg.get("model") or "claude-sonnet"),
                "timeout_seconds": timeout_seconds,
                "max_retries": max_retries,
                "api_key_env": "ANTHROPIC_API_KEY",
            },
        },
    }


def runtime_model_identity(compiled_plan: dict[str, Any]) -> str:
    config = resolve_runtime_config(compiled_plan)
    active = [
        f"{provider}:{cfg['model']}"
        for provider, cfg in config["providers"].items()
        if cfg["enabled"]
    ]
    return "|".join(active) if active else "deterministic_fallback"


def _provider_error(provider: str, kind: str, detail: str) -> dict[str, str]:
    return {"provider": provider, "kind": kind, "detail": detail}


def _provider_ready(config: RuntimeConfig, provider: str) -> str | None:
    provider_cfg = config["providers"][provider]
    if not provider_cfg["enabled"]:
        return "provider_disabled"
    if not os.getenv(provider_cfg["api_key_env"]):
        return "missing_credentials"
    return None


def _allowed_values_text(compiled_plan: dict[str, Any]) -> str:
    allowed_values = {
        "sentiment_label": sorted(compiled_plan["gold_valid_conversation_sentiment_labels"]),
        "sentiment_confidence_band": sorted(
            compiled_plan["gold_valid_conversation_sentiment_supports"]
        ),
        "intent_stage": sorted(compiled_plan["gold_valid_intent_stages"]),
        "persona_profile": sorted(compiled_plan["gold_valid_personas"]),
        "audience_segment": sorted(compiled_plan["gold_valid_audiences"]),
        "price_objection_intensity": sorted(
            compiled_plan["gold_valid_price_objection_intensities"]
        ),
        "competitor_pressure_level": sorted(compiled_plan["gold_valid_competitor_pressure_levels"]),
        "commercial_urgency_signal": sorted(compiled_plan["gold_valid_commercial_urgency_signals"]),
        "recommended_next_action": sorted(
            [
                "acionar_fluxo_pos_sinistro",
                "avancar_coleta_de_contexto",
                "enviar_cotacao_objetiva",
                "priorizar_contato_imediato",
                "reforcar_diferenciais_e_retirar_objecao_preco",
                "seguir_nutricao_basica",
            ]
        ),
    }
    return json.dumps(allowed_values, ensure_ascii=True, sort_keys=True)


def _build_prompt(payload: dict[str, Any], compiled_plan: dict[str, Any]) -> str:
    return (
        "Return valid JSON only with these fields: "
        "sentiment_label, sentiment_confidence_band, intent_stage, persona_profile, "
        "audience_segment, price_objection_intensity, competitor_pressure_level, "
        "commercial_urgency_signal, recommended_next_action, explanation_short. "
        "For every categorical field, you must choose exactly one value from this allowed set and "
        "must not invent synonyms, English labels, or intermediate stages: "
        f"{_allowed_values_text(compiled_plan)}. "
        "Keep explanation_short privacy-safe and under 280 characters. "
        f"Payload: {json.dumps(payload, ensure_ascii=True, sort_keys=True)}"
    )


def _response_text(response: Any) -> str:
    if isinstance(response, dict):
        return json.dumps(response, ensure_ascii=True)
    content = getattr(response, "content", response)
    if isinstance(content, list):
        return "".join(_safe_string(getattr(item, "text", item)) for item in content)
    return _safe_string(content)


def _invoke_openai(
    payload: dict[str, Any],
    config: RuntimeConfig,
    compiled_plan: dict[str, Any],
) -> dict[str, Any]:
    if ChatOpenAI is None:
        raise RuntimeError("langchain_openai_unavailable")
    provider_cfg = config["providers"]["openai"]
    response = ChatOpenAI(
        model=provider_cfg["model"],
        timeout=provider_cfg["timeout_seconds"],
        max_retries=provider_cfg["max_retries"],
    ).invoke(_build_prompt(payload, compiled_plan))
    return cast(dict[str, Any], json.loads(_response_text(response)))


def _invoke_anthropic(
    payload: dict[str, Any],
    config: RuntimeConfig,
    compiled_plan: dict[str, Any],
) -> dict[str, Any]:
    if ChatAnthropic is None:
        raise RuntimeError("langchain_anthropic_unavailable")
    provider_cfg = config["providers"]["anthropic"]
    response = ChatAnthropic(
        model=provider_cfg["model"],
        timeout=provider_cfg["timeout_seconds"],
        max_retries=provider_cfg["max_retries"],
    ).invoke(_build_prompt(payload, compiled_plan))
    return cast(dict[str, Any], json.loads(_response_text(response)))


def _call_provider(
    provider: str,
    state: GraphState,
    config: RuntimeConfig,
    compiled_plan: dict[str, Any],
) -> GraphState:
    readiness_error = _provider_ready(config, provider)
    if readiness_error is not None:
        return {
            **state,
            "attempted_providers": state["attempted_providers"] + [provider],
            "provider_errors": state["provider_errors"]
            + [_provider_error(provider, "provider_error", readiness_error)],
            "current_provider": None,
            "pending_provider": "anthropic" if provider == "openai" else None,
        }

    invoke = _invoke_openai if provider == "openai" else _invoke_anthropic
    try:
        provider_result = invoke(state["payload"], config, compiled_plan)
    except Exception as exc:
        return {
            **state,
            "attempted_providers": state["attempted_providers"] + [provider],
            "provider_errors": state["provider_errors"]
            + [_provider_error(provider, "provider_error", _safe_string(exc))],
            "current_provider": None,
            "pending_provider": "anthropic" if provider == "openai" else None,
        }

    return {
        **state,
        "attempted_providers": state["attempted_providers"] + [provider],
        "provider_result": provider_result,
        "current_provider": provider,
        "pending_provider": None,
    }


def _validate_provider_output(
    state: GraphState,
    config: RuntimeConfig,
    validator: Callable[[dict[str, Any], dict[str, Any]], str | None] | None,
    compiled_plan: dict[str, Any],
) -> GraphState:
    provider = _safe_string(state["current_provider"]).strip()
    if not provider or state["provider_result"] is None:
        if state["pending_provider"] is not None:
            return state
        return {**state, "final_status": "fallback"}

    validation_error = (
        validator(state["provider_result"], compiled_plan) if validator is not None else None
    )
    if validation_error is None:
        return {
            **state,
            "selected_provider": provider,
            "selected_model": config["providers"][provider]["model"],
            "final_status": "success",
            "validation_error": None,
        }

    next_provider = "anthropic" if provider == "openai" else None
    return {
        **state,
        "provider_result": None,
        "provider_errors": state["provider_errors"]
        + [_provider_error(provider, "invalid_output", validation_error)],
        "validation_error": validation_error,
        "current_provider": None,
        "pending_provider": next_provider,
        "final_status": None if next_provider is not None else "fallback",
    }


def _finalize_result(state: GraphState) -> dict[str, Any]:
    status = _safe_string(state["final_status"]).strip() or "fallback"
    return {
        "status": status,
        "provider_name": state["selected_provider"],
        "model_name": state["selected_model"],
        "output": state["provider_result"] if status == "success" else None,
        "validation_error": state["validation_error"],
        "provider_errors": state["provider_errors"],
        "attempted_providers": state["attempted_providers"],
    }


def _run_linear_graph(
    initial_state: GraphState,
    config: RuntimeConfig,
    validator: Callable[[dict[str, Any], dict[str, Any]], str | None] | None,
    compiled_plan: dict[str, Any],
) -> dict[str, Any]:
    state = _call_provider("openai", initial_state, config, compiled_plan)
    state = _validate_provider_output(state, config, validator, compiled_plan)
    if state["final_status"] != "success" and state["pending_provider"] == "anthropic":
        state = _call_provider("anthropic", state, config, compiled_plan)
        state = _validate_provider_output(state, config, validator, compiled_plan)
    if state["final_status"] is None:
        state["final_status"] = "fallback"
    return _finalize_result(state)


def _run_langgraph(
    initial_state: GraphState,
    config: RuntimeConfig,
    validator: Callable[[dict[str, Any], dict[str, Any]], str | None] | None,
    compiled_plan: dict[str, Any],
) -> dict[str, Any]:
    if StateGraph is None:  # pragma: no cover
        return _run_linear_graph(initial_state, config, validator, compiled_plan)

    def call_openai(state: GraphState) -> GraphState:
        return _call_provider("openai", state, config, compiled_plan)

    def validate_openai(state: GraphState) -> GraphState:
        return _validate_provider_output(state, config, validator, compiled_plan)

    def call_anthropic(state: GraphState) -> GraphState:
        return _call_provider("anthropic", state, config, compiled_plan)

    def validate_anthropic(state: GraphState) -> GraphState:
        return _validate_provider_output(state, config, validator, compiled_plan)

    def route_after_openai(state: GraphState) -> str:
        if state["final_status"] == "success":
            return "end"
        if state["pending_provider"] == "anthropic":
            return "call_anthropic"
        return "end"

    builder = StateGraph(GraphState)
    builder.add_node("call_openai", call_openai)
    builder.add_node("validate_openai", validate_openai)
    builder.add_node("call_anthropic", call_anthropic)
    builder.add_node("validate_anthropic", validate_anthropic)
    builder.add_edge(START, "call_openai")
    builder.add_edge("call_openai", "validate_openai")
    builder.add_conditional_edges(
        "validate_openai",
        route_after_openai,
        {
            "call_anthropic": "call_anthropic",
            "end": END,
        },
    )
    builder.add_edge("call_anthropic", "validate_anthropic")
    builder.add_edge("validate_anthropic", END)
    graph = builder.compile()
    final_state = cast(GraphState, graph.invoke(initial_state))
    if final_state.get("final_status") is None:
        final_state["final_status"] = "fallback"
    return _finalize_result(final_state)


def run_conversation_enrichment_graph(
    payload: dict[str, Any],
    compiled_plan: dict[str, Any],
    validator: Callable[[dict[str, Any], dict[str, Any]], str | None] | None = None,
) -> dict[str, Any]:
    config = resolve_runtime_config(compiled_plan)
    initial_state: GraphState = {
        "conversation_id": _safe_string(payload.get("conversation_id")),
        "lead_key": _safe_string(payload.get("lead_key")),
        "payload": payload,
        "attempted_providers": [],
        "provider_result": None,
        "provider_errors": [],
        "selected_provider": None,
        "selected_model": None,
        "validation_error": None,
        "final_status": None,
        "current_provider": None,
        "pending_provider": None,
    }
    if not config["enabled"]:
        return _finalize_result({**initial_state, "final_status": "fallback"})
    return _run_langgraph(initial_state, config, validator, compiled_plan)
