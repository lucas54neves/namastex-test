from __future__ import annotations

import hashlib
import importlib
import json
import os
from collections.abc import Callable
from contextlib import nullcontext
from typing import Any, Protocol, TypedDict, cast

from pipeline.runtime.env import env_flag
from pipeline.runtime.langfuse_bootstrap import runtime_langfuse_prompt_template

Langfuse: Any = None
get_client: Any = None
LangfuseCallbackHandler: Any = None

try:
    _langgraph_graph = importlib.import_module("langgraph.graph")
    END: Any = _langgraph_graph.END
    START: Any = _langgraph_graph.START
    StateGraph: Any = _langgraph_graph.StateGraph
except Exception:  # pragma: no cover
    END = "__end__"
    START = "__start__"
    StateGraph = None

try:
    ChatAnthropic: Any = importlib.import_module("langchain_anthropic").ChatAnthropic
except Exception:  # pragma: no cover
    ChatAnthropic = None

try:
    ChatOpenAI: Any = importlib.import_module("langchain_openai").ChatOpenAI
except Exception:  # pragma: no cover
    ChatOpenAI = None

try:
    from langfuse import Langfuse, get_client
except Exception:  # pragma: no cover
    pass

try:
    from langfuse.langchain import CallbackHandler as LangfuseCallbackHandler
except Exception:  # pragma: no cover
    pass


class _PromptLike(Protocol):
    version: Any

    def compile(self, **kwargs: Any) -> Any: ...


class ProviderRuntimeConfig(TypedDict):
    enabled: bool
    model: str
    timeout_seconds: int
    max_retries: int
    api_key_env: str


class LangfuseRuntimeConfig(TypedDict):
    enabled: bool
    prompt_name: str | None
    prompt_label: str | None
    trace_name: str
    base_url: str | None
    allow_local_prompt_fallback: bool


class RuntimeConfig(TypedDict):
    enabled: bool
    prompt_version: str
    timeout_seconds: int
    max_retries: int
    providers: dict[str, ProviderRuntimeConfig]
    langfuse: LangfuseRuntimeConfig


class PromptResolution(TypedDict):
    source: str
    prompt_text: str
    prompt_name: str | None
    prompt_label: str | None
    prompt_version: str
    langfuse_prompt_ref: Any | None
    resolution_error: str | None


class LangfuseRunContext(TypedDict):
    enabled: bool
    trace_id: str | None
    handler: Any | None
    metadata: dict[str, Any]
    root_observation: Any | None
    root_context: Any | None


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
    prompt_resolution: PromptResolution
    langfuse_context: LangfuseRunContext


def _safe_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def resolve_runtime_config(compiled_plan: dict[str, Any]) -> RuntimeConfig:
    llm_cfg = cast(dict[str, Any], compiled_plan.get("llm") or {})
    providers_cfg = cast(dict[str, Any], llm_cfg.get("providers") or {})
    langfuse_cfg = cast(dict[str, Any], llm_cfg.get("langfuse") or {})
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
        "langfuse": {
            "enabled": env_flag(
                "PIPELINE_ENABLE_LANGFUSE",
                bool(langfuse_cfg.get("enabled")),
            ),
            "prompt_name": _safe_string(
                os.getenv("PIPELINE_LANGFUSE_PROMPT_NAME") or langfuse_cfg.get("prompt_name")
            ).strip()
            or None,
            "prompt_label": _safe_string(
                os.getenv("PIPELINE_LANGFUSE_PROMPT_LABEL")
                or langfuse_cfg.get("prompt_label")
                or "production"
            ).strip()
            or None,
            "trace_name": _safe_string(
                os.getenv("PIPELINE_LANGFUSE_TRACE_NAME")
                or langfuse_cfg.get("trace_name")
                or "conversation-enrichment"
            ).strip()
            or "conversation-enrichment",
            "base_url": _safe_string(
                os.getenv("LANGFUSE_BASE_URL") or langfuse_cfg.get("base_url")
            ).strip()
            or None,
            "allow_local_prompt_fallback": env_flag(
                "PIPELINE_LANGFUSE_ALLOW_LOCAL_PROMPT_FALLBACK",
                bool(langfuse_cfg.get("allow_local_prompt_fallback", True)),
            ),
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


def _payload_for_prompt(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in payload.items()
        if key not in {"llm_input_hash", "trace_id", "prompt_source"}
    }


def _build_prompt(payload: dict[str, Any], compiled_plan: dict[str, Any]) -> str:
    prompt_text = runtime_langfuse_prompt_template()
    prompt_text = prompt_text.replace(
        "{{allowed_values_json}}", _allowed_values_text(compiled_plan)
    )
    prompt_text = prompt_text.replace(
        "{{payload_json}}",
        json.dumps(_payload_for_prompt(payload), ensure_ascii=True, sort_keys=True),
    )
    return prompt_text


def _response_text(response: Any) -> str:
    if isinstance(response, dict):
        return json.dumps(response, ensure_ascii=True)
    content = getattr(response, "content", response)
    if isinstance(content, list):
        return "".join(_safe_string(getattr(item, "text", item)) for item in content)
    return _safe_string(content)


def _langfuse_credentials_present() -> bool:
    return bool(os.getenv("LANGFUSE_PUBLIC_KEY") and os.getenv("LANGFUSE_SECRET_KEY"))


def _langfuse_available() -> bool:
    return get_client is not None


def _get_langfuse_client(config: RuntimeConfig) -> Any | None:
    if not _langfuse_available():
        return None
    base_url = config["langfuse"]["base_url"]
    if base_url and not os.getenv("LANGFUSE_BASE_URL"):
        os.environ["LANGFUSE_BASE_URL"] = base_url
    try:
        return get_client()
    except Exception:
        return None


def _langfuse_prompt_version(prompt: Any, fallback_version: str) -> str:
    for candidate in (
        getattr(prompt, "version", None),
        getattr(prompt, "version_id", None),
        getattr(prompt, "id", None),
    ):
        normalized = _safe_string(candidate).strip()
        if normalized:
            return normalized
    if isinstance(prompt, dict):
        for key in ("version", "version_id", "id"):
            normalized = _safe_string(prompt.get(key)).strip()
            if normalized:
                return normalized
    return fallback_version


def resolve_runtime_prompt(
    payload: dict[str, Any],
    compiled_plan: dict[str, Any],
    config: RuntimeConfig | None = None,
) -> PromptResolution:
    runtime_config = config or resolve_runtime_config(compiled_plan)
    local_prompt = _build_prompt(payload, compiled_plan)
    local_resolution: PromptResolution = {
        "source": "local",
        "prompt_text": local_prompt,
        "prompt_name": None,
        "prompt_label": None,
        "prompt_version": runtime_config["prompt_version"],
        "langfuse_prompt_ref": None,
        "resolution_error": None,
    }
    langfuse_cfg = runtime_config["langfuse"]
    prompt_name = _safe_string(langfuse_cfg.get("prompt_name")).strip()
    if not prompt_name or not langfuse_cfg["enabled"]:
        return local_resolution
    if not _langfuse_credentials_present():
        return {
            **local_resolution,
            "source": "local_fallback",
            "prompt_name": prompt_name,
            "prompt_label": langfuse_cfg["prompt_label"],
            "resolution_error": "missing_langfuse_credentials",
        }
    client = _get_langfuse_client(runtime_config)
    if client is None:
        return {
            **local_resolution,
            "source": "local_fallback",
            "prompt_name": prompt_name,
            "prompt_label": langfuse_cfg["prompt_label"],
            "resolution_error": "langfuse_unavailable",
        }
    try:
        prompt_ref = client.get_prompt(prompt_name, label=langfuse_cfg["prompt_label"])
        compiled_prompt = prompt_ref.compile(
            allowed_values_json=_allowed_values_text(compiled_plan),
            payload_json=json.dumps(
                _payload_for_prompt(payload), ensure_ascii=True, sort_keys=True
            ),
        )
        prompt_text = _safe_string(compiled_prompt).strip()
        if not prompt_text:
            raise RuntimeError("empty_langfuse_prompt")
        return {
            "source": "langfuse",
            "prompt_text": prompt_text,
            "prompt_name": prompt_name,
            "prompt_label": langfuse_cfg["prompt_label"],
            "prompt_version": _langfuse_prompt_version(
                prompt_ref, runtime_config["prompt_version"]
            ),
            "langfuse_prompt_ref": prompt_ref,
            "resolution_error": None,
        }
    except Exception as exc:
        if not langfuse_cfg["allow_local_prompt_fallback"]:
            raise
        return {
            **local_resolution,
            "source": "local_fallback",
            "prompt_name": prompt_name,
            "prompt_label": langfuse_cfg["prompt_label"],
            "resolution_error": _safe_string(exc) or "langfuse_prompt_resolution_failed",
        }


def _deterministic_trace_id(
    payload: dict[str, Any], prompt_resolution: PromptResolution
) -> str | None:
    conversation_id = _safe_string(payload.get("conversation_id")).strip()
    lead_key = _safe_string(payload.get("lead_key")).strip()
    llm_input_hash = _safe_string(payload.get("llm_input_hash")).strip()
    if not (conversation_id or lead_key or llm_input_hash):
        return None
    seed = "|".join(
        part
        for part in (
            conversation_id,
            lead_key,
            llm_input_hash,
            _safe_string(prompt_resolution.get("prompt_version")).strip(),
        )
        if part
    )
    if not seed:
        return None
    if Langfuse is not None and hasattr(Langfuse, "create_trace_id"):
        try:
            return _safe_string(Langfuse.create_trace_id(seed=seed)).strip() or None
        except Exception:
            pass
    return hashlib.md5(seed.encode("utf-8")).hexdigest()


def _langfuse_trace_metadata(
    payload: dict[str, Any],
    prompt_resolution: PromptResolution,
    config: RuntimeConfig,
) -> dict[str, Any]:
    return {
        "conversation_id": _safe_string(payload.get("conversation_id")),
        "lead_key": _safe_string(payload.get("lead_key")),
        "llm_input_hash": _safe_string(payload.get("llm_input_hash")) or None,
        "prompt_version": prompt_resolution["prompt_version"],
        "prompt_source": prompt_resolution["source"],
        "prompt_name": prompt_resolution["prompt_name"],
        "prompt_label": prompt_resolution["prompt_label"],
        "trace_name": config["langfuse"]["trace_name"],
    }


def _enter_if_possible(context_manager: Any) -> Any | None:
    enter = getattr(context_manager, "__enter__", None)
    if enter is None:
        return None
    return enter()


def _exit_if_possible(context_manager: Any, exc_type: Any, exc: Any, tb: Any) -> None:
    exit_fn = getattr(context_manager, "__exit__", None)
    if exit_fn is not None:
        exit_fn(exc_type, exc, tb)


def start_langfuse_run(
    payload: dict[str, Any],
    config: RuntimeConfig,
    prompt_resolution: PromptResolution,
) -> LangfuseRunContext:
    metadata = _langfuse_trace_metadata(payload, prompt_resolution, config)
    disabled_context: LangfuseRunContext = {
        "enabled": False,
        "trace_id": None,
        "handler": None,
        "metadata": metadata,
        "root_observation": None,
        "root_context": None,
    }
    if not config["langfuse"]["enabled"] or not _langfuse_credentials_present():
        return disabled_context
    client = _get_langfuse_client(config)
    if client is None:
        return disabled_context
    try:
        trace_id = _deterministic_trace_id(payload, prompt_resolution)
        handler = LangfuseCallbackHandler() if LangfuseCallbackHandler is not None else None
        root_context = (
            client.start_as_current_observation(
                as_type="span",
                name=config["langfuse"]["trace_name"],
                trace_context={"trace_id": trace_id} if trace_id else None,
            )
            if hasattr(client, "start_as_current_observation")
            else nullcontext()
        )
        root_observation = _enter_if_possible(root_context)
        if root_observation is not None and hasattr(root_observation, "update"):
            root_observation.update(metadata=metadata)
        return {
            "enabled": True,
            "trace_id": trace_id,
            "handler": handler,
            "metadata": metadata,
            "root_observation": root_observation,
            "root_context": root_context,
        }
    except Exception:
        return disabled_context


def finish_langfuse_run(context: LangfuseRunContext, result: dict[str, Any]) -> None:
    root_observation = context["root_observation"]
    if root_observation is not None and hasattr(root_observation, "update"):
        try:
            root_observation.update(
                output={
                    "status": result.get("status"),
                    "provider_name": result.get("provider_name"),
                    "model_name": result.get("model_name"),
                    "validation_error": result.get("validation_error"),
                    "provider_errors": result.get("provider_errors"),
                }
            )
        except Exception:
            pass
    handler_trace_id = _safe_string(getattr(context["handler"], "last_trace_id", "")).strip()
    if handler_trace_id:
        context["trace_id"] = handler_trace_id


def close_langfuse_run(context: LangfuseRunContext) -> None:
    _exit_if_possible(context["root_context"], None, None, None)


def shutdown_langfuse_client(flush: bool = False) -> None:
    client = _get_langfuse_client(resolve_runtime_config({"llm": {"langfuse": {}}}))
    if client is None:
        return
    try:
        if flush and hasattr(client, "flush"):
            client.flush()
    except Exception:
        pass
    try:
        if hasattr(client, "shutdown"):
            client.shutdown()
    except Exception:
        pass


def _invoke_model(
    model_factory: Any,
    prompt_text: str,
    provider_cfg: ProviderRuntimeConfig,
    langfuse_context: LangfuseRunContext,
    provider: str,
) -> dict[str, Any]:
    invoke_config: dict[str, Any] | None = None
    if langfuse_context["enabled"] and langfuse_context["handler"] is not None:
        invoke_config = {
            "callbacks": [langfuse_context["handler"]],
            "metadata": {
                **langfuse_context["metadata"],
                "provider_name": provider,
                "model_name": provider_cfg["model"],
                "trace_id": langfuse_context["trace_id"],
            },
        }
    response = model_factory(
        model=provider_cfg["model"],
        timeout=provider_cfg["timeout_seconds"],
        max_retries=provider_cfg["max_retries"],
    ).invoke(prompt_text, config=invoke_config)
    return cast(dict[str, Any], json.loads(_response_text(response)))


def _invoke_openai(
    prompt_text: str,
    config: RuntimeConfig,
    langfuse_context: LangfuseRunContext,
) -> dict[str, Any]:
    if ChatOpenAI is None:
        raise RuntimeError("langchain_openai_unavailable")
    provider_cfg = config["providers"]["openai"]
    return _invoke_model(ChatOpenAI, prompt_text, provider_cfg, langfuse_context, "openai")


def _invoke_anthropic(
    prompt_text: str,
    config: RuntimeConfig,
    langfuse_context: LangfuseRunContext,
) -> dict[str, Any]:
    if ChatAnthropic is None:
        raise RuntimeError("langchain_anthropic_unavailable")
    provider_cfg = config["providers"]["anthropic"]
    return _invoke_model(ChatAnthropic, prompt_text, provider_cfg, langfuse_context, "anthropic")


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
        del compiled_plan
        provider_result = invoke(
            state["prompt_resolution"]["prompt_text"],
            config,
            state["langfuse_context"],
        )
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
    prompt_resolution = state["prompt_resolution"]
    langfuse_context = state["langfuse_context"]
    return {
        "status": status,
        "provider_name": state["selected_provider"],
        "model_name": state["selected_model"],
        "output": state["provider_result"] if status == "success" else None,
        "validation_error": state["validation_error"],
        "provider_errors": state["provider_errors"],
        "attempted_providers": state["attempted_providers"],
        "prompt_source": prompt_resolution["source"],
        "prompt_name": prompt_resolution["prompt_name"],
        "prompt_label": prompt_resolution["prompt_label"],
        "prompt_version": prompt_resolution["prompt_version"],
        "trace_id": langfuse_context["trace_id"],
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
    prompt_resolution: PromptResolution | None = None,
) -> dict[str, Any]:
    config = resolve_runtime_config(compiled_plan)
    resolved_prompt = prompt_resolution or resolve_runtime_prompt(
        payload, compiled_plan, config=config
    )
    langfuse_context = start_langfuse_run(payload, config, resolved_prompt)
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
        "prompt_resolution": resolved_prompt,
        "langfuse_context": langfuse_context,
    }
    if not config["enabled"]:
        result = _finalize_result({**initial_state, "final_status": "fallback"})
        finish_langfuse_run(langfuse_context, result)
        result["trace_id"] = langfuse_context["trace_id"]
        close_langfuse_run(langfuse_context)
        return result
    try:
        result = _run_langgraph(initial_state, config, validator, compiled_plan)
        finish_langfuse_run(langfuse_context, result)
        result["trace_id"] = langfuse_context["trace_id"]
        return result
    finally:
        close_langfuse_run(langfuse_context)
