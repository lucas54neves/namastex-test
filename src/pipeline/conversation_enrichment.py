from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from typing import Any, cast

import pandas as pd

from pipeline.transforms import (
    NEGATIVE_TONE_PATTERNS,
    POSITIVE_TONE_PATTERNS,
    _commercial_urgency_signal,
    _competitor_pressure_level,
    _count_tone_hits,
    _price_objection_intensity,
    _response_latency_band,
    _safe_float,
    _safe_string,
    derive_conversation_sentiment_label,
    derive_conversation_sentiment_support,
    detect_unmasked_sensitive_classes,
)

LLM_OUTPUT_REQUIRED_FIELDS = (
    "sentiment_label",
    "sentiment_confidence_band",
    "intent_stage",
    "persona_profile",
    "audience_segment",
    "price_objection_intensity",
    "competitor_pressure_level",
    "commercial_urgency_signal",
    "recommended_next_action",
)
LLM_INFERENCE_STATUSES = frozenset(
    {
        "success",
        "fallback",
        "invalid_output",
        "provider_error",
        "disabled",
        "skipped_cache_hit",
    }
)
RECOMMENDED_NEXT_ACTIONS = frozenset(
    {
        "avancar_coleta_de_contexto",
        "enviar_cotacao_objetiva",
        "reforcar_diferenciais_e_retirar_objecao_preco",
        "priorizar_contato_imediato",
        "acionar_fluxo_pos_sinistro",
        "seguir_nutricao_basica",
    }
)
SEVERITY_ORDER = {
    "nenhuma": 0,
    "baixa": 0,
    "leve": 1,
    "fraco": 1,
    "moderada": 2,
    "moderado": 2,
    "forte": 3,
    "alta": 3,
}


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _stable_input_hash(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _llm_enabled(compiled_plan: dict[str, Any]) -> bool:
    llm_cfg = cast(dict[str, Any], compiled_plan["llm"])
    return bool(llm_cfg.get("enabled")) and os.getenv("PIPELINE_ENABLE_LLM_ENRICHMENT") == "1"


def _prompt_version(compiled_plan: dict[str, Any]) -> str:
    llm_cfg = cast(dict[str, Any], compiled_plan["llm"])
    return str(llm_cfg.get("prompt_version") or "v1")


def _model_name(compiled_plan: dict[str, Any]) -> str:
    llm_cfg = cast(dict[str, Any], compiled_plan["llm"])
    provider = str(llm_cfg.get("provider") or "deterministic_fallback")
    model = str(llm_cfg.get("model") or "").strip()
    return model or provider


def _max_by_severity(values: pd.Series) -> str:
    normalized = [_safe_string(value).strip() for value in values if _safe_string(value).strip()]
    if not normalized:
        return ""
    return max(normalized, key=lambda value: (SEVERITY_ORDER.get(value, -1), value))


def _most_frequent_with_recency(
    frame: pd.DataFrame,
    value_column: str,
    recency_column: str,
) -> str:
    non_null = frame.loc[frame[value_column].notna()].copy()
    if non_null.empty:
        return ""
    counts = (
        non_null.groupby(value_column, dropna=False)
        .agg(
            item_count=(value_column, "size"),
            latest_seen=(recency_column, "max"),
        )
        .reset_index()
        .sort_values(["item_count", "latest_seen", value_column], ascending=[False, False, True])
    )
    return _safe_string(counts.iloc[0][value_column])


def _latest_non_null(frame: pd.DataFrame, value_column: str, recency_column: str) -> str:
    non_null = frame.loc[frame[value_column].notna()].sort_values(recency_column, ascending=False)
    if non_null.empty:
        return ""
    return _safe_string(non_null.iloc[0][value_column])


def _build_llm_request_payload(
    conversation_messages: pd.DataFrame,
    compiled_plan: dict[str, Any],
) -> dict[str, Any]:
    ordered = conversation_messages.sort_values(["timestamp", "message_id"]).reset_index(drop=True)
    prompt_version = _prompt_version(compiled_plan)
    return {
        "conversation_id": _safe_string(ordered.iloc[0]["conversation_id"]),
        "lead_key": _safe_string(ordered.iloc[0]["lead_key"]),
        "prompt_version": prompt_version,
        "conversation_statistics": {
            "message_count": int(len(ordered)),
            "inbound_messages": int(ordered["direction"].eq("inbound").sum()),
            "outbound_messages": int(ordered["direction"].eq("outbound").sum()),
            "has_vehicle_signal": bool(ordered["mentions_vehicle"].astype(bool).any()),
            "has_competitor_signal": bool(ordered["mentions_competitor"].astype(bool).any()),
            "has_sinistro_signal": bool(ordered["mentions_sinistro"].astype(bool).any()),
            "latest_quoted_price": _safe_float(ordered["quoted_price"].dropna().iloc[-1])
            if ordered["quoted_price"].notna().any()
            else None,
            "avg_response_time_sec": float(ordered["metadata_response_time_sec"].dropna().mean())
            if "metadata_response_time_sec" in ordered.columns
            and ordered["metadata_response_time_sec"].notna().any()
            else None,
            "response_latency_band": _response_latency_band(
                ordered["metadata_response_time_sec"].dropna().mean()
                if "metadata_response_time_sec" in ordered.columns
                and ordered["metadata_response_time_sec"].notna().any()
                else None
            ),
        },
        "messages": [
            {
                "timestamp": ordered_message["timestamp"].isoformat(),
                "direction": _safe_string(ordered_message["direction"]),
                "message_type": _safe_string(ordered_message["message_type"]),
                "text": _safe_string(ordered_message["message_body_masked"]),
            }
            for ordered_message in ordered.to_dict(orient="records")
        ],
    }


def _persona_from_context(
    has_sinistro_signal: bool,
    has_competitor_signal: bool,
    data_shared_score: int,
    message_count: int,
) -> str:
    if has_sinistro_signal:
        return "cliente_pos_sinistro"
    if has_competitor_signal:
        return "cotador_comparador"
    if data_shared_score >= 2 and message_count >= 3:
        return "lead_engajado_com_dados"
    return "lead_frio"


def _audience_from_persona(persona_profile: str) -> str:
    if persona_profile == "cliente_pos_sinistro":
        return "retencao_pos_sinistro"
    if persona_profile == "cotador_comparador":
        return "oferta_competitiva"
    if persona_profile == "lead_engajado_com_dados":
        return "close_comercial"
    return "nutricao_basica"


def _recommended_next_action(
    intent_stage: str,
    urgency_signal: str,
    price_objection_intensity: str,
) -> str:
    if intent_stage == "pos_sinistro":
        return "acionar_fluxo_pos_sinistro"
    if urgency_signal == "alta":
        return "priorizar_contato_imediato"
    if price_objection_intensity == "forte":
        return "reforcar_diferenciais_e_retirar_objecao_preco"
    if intent_stage == "cotacao_ativa":
        return "enviar_cotacao_objetiva"
    if intent_stage == "descoberta_inicial":
        return "avancar_coleta_de_contexto"
    return "seguir_nutricao_basica"


def build_deterministic_conversation_fallback(
    conversation_messages: pd.DataFrame,
) -> dict[str, Any]:
    ordered = conversation_messages.sort_values(["timestamp", "message_id"]).reset_index(drop=True)
    inbound = ordered.loc[ordered["direction"].eq("inbound")].copy()
    inbound_text = inbound["message_body_masked"].fillna("")
    positive_tone_hits = sum(
        int(_count_tone_hits(value, POSITIVE_TONE_PATTERNS)) for value in inbound_text.tolist()
    )
    negative_tone_hits = sum(
        int(_count_tone_hits(value, NEGATIVE_TONE_PATTERNS)) for value in inbound_text.tolist()
    )
    quoted_price_mentions = int(ordered["quoted_price"].notna().sum())
    competitor_mentions_count = int(ordered["mentions_competitor"].astype(bool).sum())
    competitor_comparison_hits = int(ordered["competitor_comparison_signal"].astype(bool).sum())
    price_objection_hits = int(ordered["price_objection_signal"].astype(bool).sum())
    urgency_hits = int(ordered["urgency_strength"].fillna(0).gt(0).sum())
    urgency_strength_max = int(ordered["urgency_strength"].fillna(0).max())
    contains_fields = [
        "contains_email",
        "contains_phone",
        "contains_cpf",
        "contains_cep",
        "contains_plate",
    ]
    data_shared_score = int(ordered[contains_fields].fillna(False).astype(bool).any(axis=0).sum())
    has_sinistro_signal = bool(ordered["mentions_sinistro"].astype(bool).any())
    has_competitor_signal = bool(ordered["mentions_competitor"].astype(bool).any())
    sentiment_label = derive_conversation_sentiment_label(positive_tone_hits, negative_tone_hits)
    sentiment_support = derive_conversation_sentiment_support(
        positive_tone_hits, negative_tone_hits
    )

    if has_sinistro_signal:
        intent_stage = "pos_sinistro"
    elif quoted_price_mentions > 0:
        intent_stage = "cotacao_ativa"
    elif has_competitor_signal:
        intent_stage = "pesquisa_mercado"
    else:
        intent_stage = "descoberta_inicial"

    persona_profile = _persona_from_context(
        has_sinistro_signal=has_sinistro_signal,
        has_competitor_signal=has_competitor_signal,
        data_shared_score=data_shared_score,
        message_count=int(len(ordered)),
    )
    audience_segment = _audience_from_persona(persona_profile)
    price_objection_intensity = _price_objection_intensity(
        price_objection_hits,
        competitor_mentions_count,
        quoted_price_mentions,
    )
    competitor_pressure_level = _competitor_pressure_level(
        ordered["competitor_mentioned"].dropna().iloc[-1]
        if ordered["competitor_mentioned"].notna().any()
        else None,
        competitor_mentions_count,
        competitor_comparison_hits,
    )
    commercial_urgency_signal = _commercial_urgency_signal(
        urgency_strength_max,
        urgency_hits,
        (ordered["timestamp"].max() - ordered["timestamp"].min()).total_seconds() / 3600.0,
    )
    recommended_next_action = _recommended_next_action(
        intent_stage=intent_stage,
        urgency_signal=commercial_urgency_signal,
        price_objection_intensity=price_objection_intensity,
    )

    explanation_short = (
        "Fallback deterministico aplicado para classificacao semantica por conversa."
    )
    return {
        "sentiment_label": sentiment_label,
        "sentiment_confidence_band": sentiment_support,
        "intent_stage": intent_stage,
        "persona_profile": persona_profile,
        "audience_segment": audience_segment,
        "price_objection_intensity": price_objection_intensity,
        "competitor_pressure_level": competitor_pressure_level,
        "commercial_urgency_signal": commercial_urgency_signal,
        "recommended_next_action": recommended_next_action,
        "explanation_short": explanation_short,
    }


def infer_conversation_semantics(
    payload: dict[str, Any],
    compiled_plan: dict[str, Any],
) -> dict[str, Any]:
    del payload
    del compiled_plan
    raise RuntimeError("No LLM provider configured for conversation enrichment")


def _validate_llm_response(
    response: dict[str, Any],
    compiled_plan: dict[str, Any],
) -> str | None:
    gold_cfg = compiled_plan
    missing_fields = [field for field in LLM_OUTPUT_REQUIRED_FIELDS if field not in response]
    if missing_fields:
        return f"missing_fields:{','.join(missing_fields)}"

    validations = {
        "sentiment_label": set(gold_cfg["gold_valid_conversation_sentiment_labels"]),
        "sentiment_confidence_band": set(gold_cfg["gold_valid_conversation_sentiment_supports"]),
        "intent_stage": set(gold_cfg["gold_valid_intent_stages"]),
        "persona_profile": set(gold_cfg["gold_valid_personas"]),
        "audience_segment": set(gold_cfg["gold_valid_audiences"]),
        "price_objection_intensity": set(gold_cfg["gold_valid_price_objection_intensities"]),
        "competitor_pressure_level": set(gold_cfg["gold_valid_competitor_pressure_levels"]),
        "commercial_urgency_signal": set(gold_cfg["gold_valid_commercial_urgency_signals"]),
        "recommended_next_action": set(RECOMMENDED_NEXT_ACTIONS),
    }
    for field, allowed in validations.items():
        value = _safe_string(response.get(field)).strip()
        if value not in allowed:
            return f"invalid_{field}:{value or '<empty>'}"

    explanation = _safe_string(response.get("explanation_short")).strip()
    if len(explanation) > 280:
        return "explanation_too_long"
    if detect_unmasked_sensitive_classes(explanation):
        return "explanation_privacy_leak"
    return None


def build_conversation_enrichment(
    silver_messages: pd.DataFrame,
    compiled_plan: dict[str, Any],
    existing_enrichment: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if silver_messages.empty:
        return pd.DataFrame(
            columns=[
                "conversation_id",
                "lead_key",
                "conversation_started_at",
                "conversation_last_message_at",
                "llm_input_hash",
                "prompt_version",
                "llm_model",
                "inference_status",
                "processed_at_utc",
                "sentiment_label",
                "sentiment_confidence_band",
                "intent_stage",
                "persona_profile",
                "audience_segment",
                "price_objection_intensity",
                "competitor_pressure_level",
                "commercial_urgency_signal",
                "recommended_next_action",
                "explanation_short",
                "fallback_reason",
                "validation_error",
            ]
        )

    ordered_messages = silver_messages.sort_values(["conversation_id", "timestamp", "message_id"])
    enabled = _llm_enabled(compiled_plan)
    prompt_version = _prompt_version(compiled_plan)
    model_name = _model_name(compiled_plan)
    cache_df = existing_enrichment.copy() if existing_enrichment is not None else pd.DataFrame()
    cache_lookup: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    if not cache_df.empty:
        for cached_row in cache_df.to_dict(orient="records"):
            key = (
                _safe_string(cached_row.get("conversation_id")),
                _safe_string(cached_row.get("llm_input_hash")),
                _safe_string(cached_row.get("prompt_version")),
                _safe_string(cached_row.get("llm_model")),
            )
            cache_lookup[key] = cached_row

    rows: list[dict[str, Any]] = []
    for _, conversation_messages in ordered_messages.groupby("conversation_id", dropna=False):
        payload = _build_llm_request_payload(conversation_messages, compiled_plan)
        llm_input_hash = _stable_input_hash(payload)
        cache_key = (
            _safe_string(payload["conversation_id"]),
            llm_input_hash,
            prompt_version,
            model_name,
        )
        fallback = build_deterministic_conversation_fallback(conversation_messages)
        base_row = {
            "conversation_id": _safe_string(payload["conversation_id"]),
            "lead_key": _safe_string(payload["lead_key"]),
            "conversation_started_at": conversation_messages["timestamp"].min(),
            "conversation_last_message_at": conversation_messages["timestamp"].max(),
            "llm_input_hash": llm_input_hash,
            "prompt_version": prompt_version,
            "llm_model": model_name,
            "processed_at_utc": _utc_now_iso(),
            "fallback_reason": None,
            "validation_error": None,
        }
        if cache_key in cache_lookup:
            cached_row = cache_lookup[cache_key]
            rows.append(
                {
                    **base_row,
                    **{
                        field: cached_row.get(field)
                        for field in (
                            "sentiment_label",
                            "sentiment_confidence_band",
                            "intent_stage",
                            "persona_profile",
                            "audience_segment",
                            "price_objection_intensity",
                            "competitor_pressure_level",
                            "commercial_urgency_signal",
                            "recommended_next_action",
                            "explanation_short",
                            "fallback_reason",
                            "validation_error",
                        )
                    },
                    "inference_status": "skipped_cache_hit",
                }
            )
            continue

        if not enabled:
            rows.append(
                {
                    **base_row,
                    **fallback,
                    "inference_status": "disabled",
                    "fallback_reason": "llm_enrichment_disabled",
                }
            )
            continue

        try:
            llm_response = infer_conversation_semantics(payload, compiled_plan)
        except Exception as exc:
            rows.append(
                {
                    **base_row,
                    **fallback,
                    "inference_status": "provider_error",
                    "fallback_reason": str(exc),
                }
            )
            continue

        validation_error = _validate_llm_response(llm_response, compiled_plan)
        if validation_error is not None:
            rows.append(
                {
                    **base_row,
                    **fallback,
                    "inference_status": "invalid_output",
                    "fallback_reason": "invalid_llm_response",
                    "validation_error": validation_error,
                }
            )
            continue

        rows.append(
            {
                **base_row,
                "inference_status": "success",
                "sentiment_label": _safe_string(llm_response.get("sentiment_label")),
                "sentiment_confidence_band": _safe_string(
                    llm_response.get("sentiment_confidence_band")
                ),
                "intent_stage": _safe_string(llm_response.get("intent_stage")),
                "persona_profile": _safe_string(llm_response.get("persona_profile")),
                "audience_segment": _safe_string(llm_response.get("audience_segment")),
                "price_objection_intensity": _safe_string(
                    llm_response.get("price_objection_intensity")
                ),
                "competitor_pressure_level": _safe_string(
                    llm_response.get("competitor_pressure_level")
                ),
                "commercial_urgency_signal": _safe_string(
                    llm_response.get("commercial_urgency_signal")
                ),
                "recommended_next_action": _safe_string(
                    llm_response.get("recommended_next_action")
                ),
                "explanation_short": _safe_string(llm_response.get("explanation_short")),
            }
        )

    enrichment = pd.DataFrame(rows)
    return enrichment.sort_values(
        ["lead_key", "conversation_last_message_at", "conversation_id"]
    ).reset_index(drop=True)


def consolidate_gold_semantics(conversation_enrichment: pd.DataFrame) -> pd.DataFrame:
    if conversation_enrichment.empty:
        return pd.DataFrame(
            columns=[
                "lead_key",
                "intent_stage",
                "persona_profile",
                "audience_segment",
                "price_objection_intensity",
                "competitor_pressure_level",
                "commercial_urgency_signal",
                "conversation_sentiment_label",
                "conversation_sentiment_support",
            ]
        )

    rows: list[dict[str, Any]] = []
    for lead_key, lead_conversations in conversation_enrichment.groupby("lead_key", dropna=False):
        ordered = lead_conversations.sort_values(
            ["conversation_last_message_at", "conversation_id"],
            ascending=[True, True],
        ).reset_index(drop=True)
        dominant_persona = _most_frequent_with_recency(
            ordered, "persona_profile", "conversation_last_message_at"
        )
        dominant_sentiment = _most_frequent_with_recency(
            ordered, "sentiment_label", "conversation_last_message_at"
        )
        support_for_dominant = _max_by_severity(
            ordered.loc[
                ordered["sentiment_label"].eq(dominant_sentiment),
                "sentiment_confidence_band",
            ]
        )
        audience_segment = _audience_from_persona(dominant_persona)
        if (
            audience_segment == "nutricao_basica"
            and _max_by_severity(ordered["competitor_pressure_level"]) == "alta"
        ):
            audience_segment = "oferta_competitiva"
        rows.append(
            {
                "lead_key": _safe_string(lead_key),
                "intent_stage": _latest_non_null(
                    ordered, "intent_stage", "conversation_last_message_at"
                ),
                "persona_profile": dominant_persona,
                "audience_segment": audience_segment,
                "price_objection_intensity": _max_by_severity(ordered["price_objection_intensity"]),
                "competitor_pressure_level": _max_by_severity(ordered["competitor_pressure_level"]),
                "commercial_urgency_signal": _latest_non_null(
                    ordered, "commercial_urgency_signal", "conversation_last_message_at"
                )
                or _max_by_severity(ordered["commercial_urgency_signal"]),
                "conversation_sentiment_label": dominant_sentiment,
                "conversation_sentiment_support": support_for_dominant,
            }
        )
    return pd.DataFrame(rows).sort_values("lead_key").reset_index(drop=True)


def validate_conversation_enrichment_frame(
    enrichment_df: pd.DataFrame,
    compiled_plan: dict[str, Any],
) -> list[str]:
    required_columns = {
        "conversation_id",
        "lead_key",
        "llm_input_hash",
        "prompt_version",
        "llm_model",
        "inference_status",
        "processed_at_utc",
        "sentiment_label",
        "sentiment_confidence_band",
        "intent_stage",
        "persona_profile",
        "audience_segment",
        "price_objection_intensity",
        "competitor_pressure_level",
        "commercial_urgency_signal",
        "recommended_next_action",
    }
    missing = sorted(required_columns - set(enrichment_df.columns))
    if missing:
        return [f"missing_columns:{','.join(missing)}"]

    errors: list[str] = []
    if not enrichment_df["conversation_id"].is_unique:
        errors.append("conversation_id_not_unique")
    invalid_statuses = sorted(set(enrichment_df["inference_status"]) - LLM_INFERENCE_STATUSES)
    if invalid_statuses:
        errors.append(f"invalid_statuses:{','.join(map(str, invalid_statuses))}")

    for _, row in enrichment_df.iterrows():
        row_payload = {
            "sentiment_label": row["sentiment_label"],
            "sentiment_confidence_band": row["sentiment_confidence_band"],
            "intent_stage": row["intent_stage"],
            "persona_profile": row["persona_profile"],
            "audience_segment": row["audience_segment"],
            "price_objection_intensity": row["price_objection_intensity"],
            "competitor_pressure_level": row["competitor_pressure_level"],
            "commercial_urgency_signal": row["commercial_urgency_signal"],
            "recommended_next_action": row["recommended_next_action"],
            "explanation_short": row.get("explanation_short"),
        }
        error = _validate_llm_response(row_payload, compiled_plan)
        if error is not None:
            errors.append(f"conversation:{_safe_string(row['conversation_id'])}:{error}")
    return errors
