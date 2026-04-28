# GUD-001 EXCEPTION: This file exceeds 400 lines. Justification: REQ-106 explicitly defers
# migration of build_gold and its Gold-segmentation helpers to a future spec; those functions
# (~400 lines combined) must remain here until gold.py becomes the canonical owner. All other
# symbols have been extracted to silver_patterns, silver_masking, silver_signals, silver_context,
# and silver_dedup; this module now serves exclusively as the retrocompatible entry point.
from __future__ import annotations

import json
from typing import Any, cast

import pandas as pd

from pipeline.orchestration.compiler import get_default_compiled_plan

# Reexports — retrocompatibility (noqa: F401 suppresses unused-import warnings)
from pipeline.transforms.silver_context import (  # noqa: F401
    _stable_hash_token,
    add_conversation_context,
    add_lead_context,
)
from pipeline.transforms.silver_dedup import deduplicate_events  # noqa: F401
from pipeline.transforms.silver_masking import (  # noqa: F401
    _is_masked_email,
    _is_masked_plate,
    _mask_alpha_numeric,
    _mask_digits,
    _mask_email,
    _mask_known_names,
    _mask_name_token,
    detect_sensitive_classes,
    detect_unmasked_sensitive_classes,
    mask_message_body,
    mask_sender_name,
)
from pipeline.transforms.silver_patterns import (  # noqa: F401
    CEP_PATTERN,
    COMPETITOR_COMPARISON_PATTERN,
    COMPETITOR_PATTERNS,
    CONVERSATION_SENTIMENT_LABELS,
    CONVERSATION_SENTIMENT_SUPPORT_LEVELS,
    CPF_PATTERN,
    EMAIL_PATTERN,
    EMAIL_PROVIDER_DOMAIN_PATTERN,
    NEGATIVE_TONE_PATTERNS,
    PHONE_PATTERN,
    PLATE_PATTERN,
    POSITIVE_TONE_PATTERNS,
    PRICE_OBJECTION_PATTERN,
    PRICE_PATTERN,
    SENSITIVE_PATTERNS,
    SINISTRO_PATTERNS,
    STATUS_PRIORITY,
    URGENCY_MODERATE_PATTERN,
    URGENCY_STRONG_PATTERN,
    VEHICLE_MAKES,
    VEHICLE_MODELS,
    YEAR_PATTERN,
    _normalize_ascii,
    _normalize_for_match,
    _safe_float,
    _safe_int,
    _safe_string,
)
from pipeline.transforms.silver_signals import (  # noqa: F401
    _count_tone_hits,
    _directional_bool_signal,
    _directional_int_signal,
    _directional_value_signal,
    _extract_competitor,
    _extract_competitor_comparison_signal,
    _extract_email_provider,
    _extract_first,
    _extract_price,
    _extract_price_objection_signal,
    _extract_sinistro_type,
    _extract_urgency_strength,
    _extract_vehicle_make,
    _extract_vehicle_model,
    _extract_vehicle_year,
    add_message_signals,
    derive_conversation_sentiment_label,
    derive_conversation_sentiment_support,
)

__all__ = [
    # patterns
    "EMAIL_PATTERN",
    "PHONE_PATTERN",
    "CPF_PATTERN",
    "CEP_PATTERN",
    "PLATE_PATTERN",
    "YEAR_PATTERN",
    "PRICE_PATTERN",
    "PRICE_OBJECTION_PATTERN",
    "URGENCY_STRONG_PATTERN",
    "URGENCY_MODERATE_PATTERN",
    "COMPETITOR_COMPARISON_PATTERN",
    "EMAIL_PROVIDER_DOMAIN_PATTERN",
    "COMPETITOR_PATTERNS",
    "SINISTRO_PATTERNS",
    "VEHICLE_MAKES",
    "VEHICLE_MODELS",
    "STATUS_PRIORITY",
    "SENSITIVE_PATTERNS",
    "POSITIVE_TONE_PATTERNS",
    "NEGATIVE_TONE_PATTERNS",
    "CONVERSATION_SENTIMENT_LABELS",
    "CONVERSATION_SENTIMENT_SUPPORT_LEVELS",
    # utility functions (patterns module)
    "_normalize_ascii",
    "_normalize_for_match",
    "_safe_string",
    "_safe_float",
    "_safe_int",
    # masking
    "_mask_digits",
    "_mask_email",
    "_mask_alpha_numeric",
    "_mask_name_token",
    "_mask_known_names",
    "_is_masked_email",
    "_is_masked_plate",
    "mask_sender_name",
    "mask_message_body",
    "detect_sensitive_classes",
    "detect_unmasked_sensitive_classes",
    # signals
    "_extract_first",
    "_extract_competitor",
    "_extract_sinistro_type",
    "_extract_vehicle_make",
    "_extract_vehicle_model",
    "_extract_vehicle_year",
    "_extract_price",
    "_extract_email_provider",
    "_extract_price_objection_signal",
    "_extract_urgency_strength",
    "_extract_competitor_comparison_signal",
    "_count_tone_hits",
    "derive_conversation_sentiment_label",
    "derive_conversation_sentiment_support",
    "add_message_signals",
    "_directional_bool_signal",
    "_directional_int_signal",
    "_directional_value_signal",
    # context
    "_stable_hash_token",
    "add_conversation_context",
    "add_lead_context",
    # dedup
    "deduplicate_events",
    # silver entry point
    "load_bronze_frame",
    "parse_metadata",
    "build_silver",
    "build_silver_leads",
    "add_gold_segments",
    "build_gold",
    # aggregation helpers
    "_first_non_empty",
    "_json_sorted_unique",
    "_first_non_null",
    "_last_non_null",
    "_max_or_false",
    "_bool_series_or_default",
    "_int_series_or_default",
    "_value_series_or_default",
    # gold helpers
    "_canonical_audience_for_persona",
    "_parse_json_list",
    "_normalize_outcome_group",
    "_response_latency_band",
    "_price_objection_intensity",
    "_commercial_urgency_signal",
    "_competitor_pressure_level",
]


def load_bronze_frame(source_path: str) -> pd.DataFrame:
    df = pd.read_parquet(source_path).copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df


def parse_metadata(df: pd.DataFrame) -> pd.DataFrame:
    metadata = df["metadata"].map(json.loads)
    metadata_df = pd.json_normalize(metadata)
    metadata_df.columns = [f"metadata_{column}" for column in metadata_df.columns]
    return pd.concat([df.drop(columns=["metadata"]), metadata_df], axis=1)


def _first_non_empty(values: pd.Series) -> str:
    for value in values:
        text = _safe_string(value).strip()
        if text:
            return text
    return ""


def _json_sorted_unique(values: pd.Series) -> str:
    collected = sorted(
        {_safe_string(value).strip() for value in values if _safe_string(value).strip()}
    )
    return json.dumps(collected, ensure_ascii=True)


def _first_non_null(values: pd.Series) -> object:
    for value in values:
        if pd.notna(value):
            return value
    return None


def _last_non_null(values: pd.Series) -> object:
    for value in values.iloc[::-1]:
        if pd.notna(value):
            return value
    return None


def _max_or_false(values: pd.Series) -> bool:
    return bool(values.fillna(False).astype(bool).max())


def _bool_series_or_default(
    frame: pd.DataFrame,
    column: str,
    fallback_column: str | None = None,
) -> pd.Series:
    if column in frame.columns:
        return frame[column].fillna(False).astype(bool)
    if fallback_column and fallback_column in frame.columns:
        fallback = frame[fallback_column].fillna(False).astype(bool)
        if "direction" not in frame.columns:
            return fallback
        return fallback & frame["direction"].eq("inbound")
    return pd.Series(False, index=frame.index, dtype=bool)


def _int_series_or_default(
    frame: pd.DataFrame,
    column: str,
    fallback_column: str | None = None,
) -> pd.Series:
    if column in frame.columns:
        return pd.to_numeric(frame[column], errors="coerce").fillna(0).astype(int)
    if fallback_column and fallback_column in frame.columns:
        fallback = pd.to_numeric(frame[fallback_column], errors="coerce").fillna(0).astype(int)
        if "direction" not in frame.columns:
            return fallback
        return fallback.where(frame["direction"].eq("inbound"), 0)
    return pd.Series(0, index=frame.index, dtype=int)


def _value_series_or_default(
    frame: pd.DataFrame,
    column: str,
    fallback_column: str | None = None,
) -> pd.Series:
    if column in frame.columns:
        return frame[column]
    if fallback_column and fallback_column in frame.columns:
        if "direction" not in frame.columns:
            return frame[fallback_column]
        return frame[fallback_column].where(frame["direction"].eq("inbound"))
    return pd.Series([None] * len(frame), index=frame.index, dtype=object)


def _canonical_audience_for_persona(persona_profile: object) -> str:
    persona = _safe_string(persona_profile).strip()
    if persona == "cliente_pos_sinistro":
        return "retencao_pos_sinistro"
    if persona == "cotador_comparador":
        return "oferta_competitiva"
    if persona == "lead_engajado_com_dados":
        return "close_comercial"
    if persona == "lead_frio":
        return "nutricao_basica"
    return ""


def _parse_json_list(raw: object) -> list[str]:
    text = _safe_string(raw).strip()
    if not text:
        return []
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _normalize_outcome_group(observed_outcomes: object) -> str:
    closed_outcomes = {
        "fechado_ganho",
        "fechado",
        "apolice_emitida",
        "venda_realizada",
        "contrato_assinado",
    }
    lost_outcomes = {
        "fechado_perdido",
        "perdido",
        "sem_interesse",
        "cancelado",
        "nao_convertido",
    }
    normalized = {
        _normalize_for_match(item).replace(" ", "_") for item in _parse_json_list(observed_outcomes)
    }
    if normalized & closed_outcomes:
        return "fechado"
    if normalized & lost_outcomes:
        return "perdido"
    return "aberto"


def _response_latency_band(avg_response_time_sec: object) -> str:
    value = _safe_float(avg_response_time_sec)
    if value is None:
        return "sem_evidencia"
    if value <= 300:
        return "rapida"
    if value <= 1800:
        return "moderada"
    return "lenta"


def _price_objection_intensity(
    price_objection_hits: object,
    competitor_mentions: object,
    quoted_price_mentions: object,
) -> str:
    hits = _safe_int(price_objection_hits)
    competitor_count = _safe_int(competitor_mentions)
    quote_count = _safe_int(quoted_price_mentions)
    if hits >= 2 or (hits >= 1 and competitor_count >= 1) or quote_count >= 2:
        return "forte"
    if hits >= 1 or quote_count >= 1 or competitor_count >= 1:
        return "leve"
    return "nenhuma"


def _commercial_urgency_signal(
    urgency_strength_max: object,
    urgency_hits: object,
    lead_lifecycle_hours: object,
) -> str:
    max_strength = _safe_int(urgency_strength_max)
    hit_count = _safe_int(urgency_hits)
    if max_strength >= 2 or hit_count >= 2:
        return "alta"
    if max_strength >= 1:
        return "moderada"
    return "nenhuma"


def _competitor_pressure_level(
    primary_competitor: object,
    competitor_mentions: object,
    competitor_comparison_hits: object,
) -> str:
    has_primary = bool(_safe_string(primary_competitor).strip())
    mentions = _safe_int(competitor_mentions)
    comparisons = _safe_int(competitor_comparison_hits)
    if not has_primary and mentions == 0 and comparisons == 0:
        return "nenhuma"
    if comparisons >= 1 or mentions >= 2:
        return "alta"
    return "leve"


def build_silver(df: pd.DataFrame, compiled_plan: dict[str, object] | None = None) -> pd.DataFrame:
    silver = parse_metadata(df)
    silver = add_conversation_context(silver)
    silver = deduplicate_events(silver, compiled_plan=compiled_plan)
    silver = add_message_signals(silver)
    silver = add_lead_context(silver)
    silver["is_inbound"] = silver["direction"].eq("inbound")
    silver["is_outbound"] = silver["direction"].eq("outbound")
    return silver.sort_values(["conversation_id", "timestamp", "message_id"]).reset_index(drop=True)


def build_silver_leads(silver_messages: pd.DataFrame) -> pd.DataFrame:
    lead_frame = silver_messages.copy()
    optional_defaults: dict[str, object] = {
        "metadata_city": "",
        "metadata_state": "",
        "metadata_lead_source": "",
        "metadata_response_time_sec": pd.NA,
        "metadata_is_business_hours": pd.NA,
    }
    for column, default in optional_defaults.items():
        if column not in lead_frame.columns:
            lead_frame[column] = default

    grouped = lead_frame.groupby("lead_key", dropna=False)
    leads = grouped.agg(
        canonical_lead_name_masked=(
            "lead_name_raw",
            lambda values: mask_sender_name(_first_non_empty(values)),
        ),
        lead_contact_ref=("lead_phone_masked", lambda values: _first_non_empty(values) or ""),
        city=("metadata_city", lambda values: _first_non_empty(values) or ""),
        state=("metadata_state", lambda values: _first_non_empty(values) or ""),
        first_seen_at=("timestamp", "min"),
        last_seen_at=("timestamp", "max"),
        conversation_count=("conversation_id", "nunique"),
        message_count=("message_id", "nunique"),
        observed_campaign_ids=("campaign_id", _json_sorted_unique),
        observed_lead_sources=("metadata_lead_source", _json_sorted_unique),
        observed_outcomes=("conversation_outcome", _json_sorted_unique),
        has_vehicle_signal=("mentions_vehicle", _max_or_false),
        has_competitor_signal=("mentions_competitor", _max_or_false),
        has_sinistro_signal=("mentions_sinistro", _max_or_false),
        has_email_signal=("contains_email", _max_or_false),
        has_phone_signal=("contains_phone", _max_or_false),
        has_cpf_signal=("contains_cpf", _max_or_false),
        has_cep_signal=("contains_cep", _max_or_false),
        has_plate_signal=("contains_plate", _max_or_false),
        primary_campaign_id=("campaign_id", lambda values: _first_non_empty(values) or ""),
        latest_outcome=(
            "conversation_outcome",
            lambda values: _first_non_empty(values.iloc[::-1]) or "",
        ),
        vehicle_make_observed=(
            "vehicle_make",
            lambda values: _safe_string(_first_non_null(values)),
        ),
        vehicle_model_observed=(
            "vehicle_model",
            lambda values: _safe_string(_first_non_null(values)),
        ),
        vehicle_year_observed=(
            "vehicle_year",
            lambda values: _safe_string(_first_non_null(values)),
        ),
        primary_competitor_observed=(
            "competitor_mentioned",
            lambda values: _safe_string(_first_non_null(values)),
        ),
        quoted_price_min=("quoted_price", "min"),
        quoted_price_max=("quoted_price", "max"),
        quoted_price_last=("quoted_price", lambda values: _first_non_null(values.iloc[::-1])),
        avg_response_time_sec=("metadata_response_time_sec", "mean"),
        business_hours_ratio=("metadata_is_business_hours", "mean"),
    ).reset_index()
    leads["lead_contact_ref"] = leads["lead_contact_ref"].where(
        leads["lead_contact_ref"].astype(str).str.strip().ne(""),
        leads["lead_key"],
    )
    return leads.sort_values("lead_key").reset_index(drop=True)


def add_gold_segments(
    gold: pd.DataFrame, compiled_plan: dict[str, object] | None = None
) -> pd.DataFrame:
    plan = compiled_plan or get_default_compiled_plan()
    segmentation = cast(dict[str, Any], plan["gold_segmentation"])
    temperature_cfg = cast(dict[str, Any], segmentation["lead_temperature"])
    price_cfg = cast(dict[str, Any], segmentation["price_sensitivity"])
    readiness_cfg = cast(dict[str, Any], segmentation["contact_readiness"])
    intent_cfg = cast(dict[str, Any], segmentation["intent_stage"])
    risk_cfg = cast(dict[str, Any], segmentation["risk_signal"])
    persona_cfg = cast(dict[str, Any], segmentation["personas"])
    audience_cfg = cast(dict[str, Any], segmentation["audiences"])

    segmented = gold.copy()
    segmented["lead_temperature"] = str(temperature_cfg["default"])
    segmented.loc[
        segmented["engagement_bucket"].eq(str(temperature_cfg["cold_bucket"]))
        & segmented["data_shared_score"].eq(int(temperature_cfg["cold_data_shared_score"])),
        "lead_temperature",
    ] = str(temperature_cfg["cold_label"])
    segmented.loc[
        segmented["engagement_bucket"].isin(list(temperature_cfg["hot_buckets"]))
        | segmented["data_shared_score"].ge(int(temperature_cfg["hot_min_data_shared_score"])),
        "lead_temperature",
    ] = str(temperature_cfg["hot_label"])

    segmented["price_sensitivity"] = str(price_cfg["default"])
    segmented.loc[
        segmented["mentioned_competitor"] | segmented["avg_quoted_price"].notna(),
        "price_sensitivity",
    ] = str(price_cfg["high_label"])

    segmented["contact_readiness"] = str(readiness_cfg["default"])
    segmented.loc[
        segmented["data_shared_score"].eq(int(readiness_cfg["medium_score"])),
        "contact_readiness",
    ] = str(readiness_cfg["medium_label"])
    segmented.loc[
        segmented["data_shared_score"].ge(int(readiness_cfg["high_min_score"])),
        "contact_readiness",
    ] = str(readiness_cfg["high_label"])

    segmented["intent_stage"] = str(intent_cfg["default"])
    negotiation_label = intent_cfg.get("negotiation_label")
    if negotiation_label is not None and "latest_outcome" in segmented.columns:
        normalized_outcome = segmented["latest_outcome"].fillna("").astype(str).str.lower()
        segmented.loc[normalized_outcome.str.contains("negoci"), "intent_stage"] = str(
            negotiation_label
        )
    segmented.loc[segmented["mentioned_sinistro"], "intent_stage"] = str(
        intent_cfg["sinistro_label"]
    )
    segmented.loc[
        segmented["mentioned_competitor"] & ~segmented["mentioned_sinistro"],
        "intent_stage",
    ] = str(intent_cfg["competitor_label"])
    segmented.loc[
        segmented["avg_quoted_price"].notna() & ~segmented["mentioned_sinistro"],
        "intent_stage",
    ] = str(intent_cfg["quoted_price_label"])

    segmented["risk_signal"] = str(risk_cfg["default"])
    segmented.loc[
        segmented["mentioned_sinistro"] | segmented["duplicate_events_removed"].gt(0),
        "risk_signal",
    ] = str(risk_cfg["medium_label"])
    segmented.loc[
        segmented["mentioned_sinistro"] & segmented["contains_cpf"],
        "risk_signal",
    ] = str(risk_cfg["high_label"])

    segmented["persona_profile"] = str(persona_cfg["default"])
    segmented.loc[
        segmented["mentioned_sinistro"],
        "persona_profile",
    ] = str(persona_cfg["sinistro"])
    segmented.loc[
        segmented["mentioned_competitor"] & segmented["avg_quoted_price"].notna(),
        "persona_profile",
    ] = str(persona_cfg["comparator"])
    segmented.loc[
        segmented["lead_temperature"].eq(str(temperature_cfg["hot_label"]))
        & segmented["contact_readiness"].eq(str(readiness_cfg["high_label"])),
        "persona_profile",
    ] = str(persona_cfg["engaged"])

    segmented["audience_segment"] = str(audience_cfg["default"])
    segmented.loc[
        segmented["persona_profile"].eq(str(persona_cfg["sinistro"])),
        "audience_segment",
    ] = str(audience_cfg["sinistro"])
    segmented.loc[
        segmented["persona_profile"].eq(str(persona_cfg["comparator"])),
        "audience_segment",
    ] = str(audience_cfg["comparator"])
    segmented.loc[
        segmented["persona_profile"].eq(str(persona_cfg["engaged"])),
        "audience_segment",
    ] = str(audience_cfg["engaged"])

    return segmented


def build_gold(
    silver_leads: pd.DataFrame,
    silver_messages: pd.DataFrame,
    silver_conversations_llm: pd.DataFrame | None = None,
    compiled_plan: dict[str, object] | None = None,
    gold_column_plan: object | None = None,
) -> pd.DataFrame:
    if silver_conversations_llm is not None:
        from pipeline.transforms.conversation_enrichment import consolidate_gold_semantics

    ordered_messages = silver_messages.sort_values(
        ["lead_key", "timestamp", "conversation_id", "message_id"]
    ).reset_index(drop=True)
    inbound_message_body = ordered_messages["message_body"].where(
        ordered_messages["is_inbound"], ""
    )
    ordered_messages = ordered_messages.assign(
        positive_tone_hits_message=inbound_message_body.map(
            lambda value: _count_tone_hits(value, POSITIVE_TONE_PATTERNS)
        ),
        negative_tone_hits_message=inbound_message_body.map(
            lambda value: _count_tone_hits(value, NEGATIVE_TONE_PATTERNS)
        ),
    )
    grouped = ordered_messages.groupby("lead_key", dropna=False)
    inbound_price_objection = _bool_series_or_default(
        ordered_messages,
        "price_objection_signal_inbound",
        "price_objection_signal",
    )
    inbound_urgency = _int_series_or_default(
        ordered_messages,
        "urgency_strength_inbound",
        "urgency_strength",
    )
    inbound_competitor_comparison = _bool_series_or_default(
        ordered_messages,
        "competitor_comparison_signal_inbound",
        "competitor_comparison_signal",
    )
    inbound_competitor_mentioned = _value_series_or_default(
        ordered_messages,
        "competitor_mentioned_inbound",
        "competitor_mentioned",
    )
    inbound_competitor_mentions = inbound_competitor_mentioned.notna()
    ordered_messages = ordered_messages.assign(
        price_objection_signal_inbound=inbound_price_objection,
        urgency_strength_inbound=inbound_urgency,
        competitor_comparison_signal_inbound=inbound_competitor_comparison,
        competitor_mentioned_inbound=inbound_competitor_mentioned,
        mentions_competitor_inbound=inbound_competitor_mentions,
    )

    message_aggregates = grouped.agg(
        total_messages=("message_id", "count"),
        inbound_messages=("is_inbound", "sum"),
        outbound_messages=("is_outbound", "sum"),
        duplicate_events_removed=("dropped_duplicate_events", "sum"),
        contains_email=("contains_email", _max_or_false),
        contains_phone=("contains_phone", _max_or_false),
        contains_cpf=("contains_cpf", _max_or_false),
        contains_cep=("contains_cep", _max_or_false),
        contains_plate=("contains_plate", _max_or_false),
        mentioned_vehicle=("mentions_vehicle", _max_or_false),
        mentioned_competitor=("mentions_competitor", _max_or_false),
        mentioned_sinistro=("mentions_sinistro", _max_or_false),
        avg_quoted_price=("quoted_price", "mean"),
        primary_competitor=("competitor_mentioned_inbound", _last_non_null),
        dominant_email_provider=("email_provider", _last_non_null),
        quoted_price_mentions=("quoted_price", lambda values: int(values.notna().sum())),
        price_objection_hits=("price_objection_signal_inbound", "sum"),
        urgency_hits=("urgency_strength_inbound", lambda values: int(values.gt(0).sum())),
        urgency_strength_max=("urgency_strength_inbound", "max"),
        competitor_mentions_count=("mentions_competitor_inbound", "sum"),
        competitor_comparison_hits=("competitor_comparison_signal_inbound", "sum"),
        positive_tone_hits=("positive_tone_hits_message", "sum"),
        negative_tone_hits=("negative_tone_hits_message", "sum"),
    ).reset_index()

    vehicle_context = (
        ordered_messages.loc[
            ordered_messages["mentions_vehicle"],
            ["lead_key", "vehicle_make", "vehicle_model", "vehicle_year"],
        ]
        .groupby("lead_key", dropna=False)
        .agg(
            vehicle_make=("vehicle_make", _last_non_null),
            vehicle_model=("vehicle_model", _last_non_null),
            vehicle_year=("vehicle_year", _last_non_null),
        )
        .reset_index()
    )
    sinistro_context = (
        ordered_messages.loc[
            ordered_messages["mentions_sinistro"],
            ["lead_key", "sinistro_type"],
        ]
        .groupby("lead_key", dropna=False)
        .agg(primary_sinistro_type=("sinistro_type", _last_non_null))
        .reset_index()
    )

    gold = silver_leads.merge(message_aggregates, on="lead_key", how="left")
    gold = gold.merge(vehicle_context, on="lead_key", how="left")
    gold = gold.merge(sinistro_context, on="lead_key", how="left")
    gold["engagement_bucket"] = pd.cut(
        gold["total_messages"],
        bins=[0, 4, 10, 20, float("inf")],
        labels=["lead_frio", "curta", "media", "longa"],
        right=True,
    ).astype("string")
    gold["data_shared_score"] = (
        gold[
            [
                "contains_email",
                "contains_phone",
                "contains_cpf",
                "contains_cep",
                "contains_plate",
            ]
        ]
        .astype(int)
        .sum(axis=1)
    )
    gold["lead_lifecycle_hours"] = (
        gold["last_seen_at"] - gold["first_seen_at"]
    ).dt.total_seconds() / 3600.0
    gold["business_hours_message_ratio"] = gold["business_hours_ratio"]
    gold["response_latency_band"] = gold["avg_response_time_sec"].map(_response_latency_band)
    gold["closure_outcome_group"] = gold["observed_outcomes"].map(_normalize_outcome_group)
    gold["has_closed_outcome"] = gold["closure_outcome_group"].eq("fechado")
    deterministic_semantics = pd.DataFrame(
        {
            "lead_key": gold["lead_key"],
            "price_objection_intensity": [
                _price_objection_intensity(price_hits, competitor_mentions, quote_mentions)
                for price_hits, competitor_mentions, quote_mentions in zip(
                    gold["price_objection_hits"],
                    gold["competitor_mentions_count"],
                    gold["quoted_price_mentions"],
                    strict=False,
                )
            ],
            "commercial_urgency_signal": [
                _commercial_urgency_signal(max_strength, hit_count, lifecycle_hours)
                for max_strength, hit_count, lifecycle_hours in zip(
                    gold["urgency_strength_max"],
                    gold["urgency_hits"],
                    gold["lead_lifecycle_hours"],
                    strict=False,
                )
            ],
            "competitor_pressure_level": [
                _competitor_pressure_level(primary_competitor, mentions, comparison_hits)
                for primary_competitor, mentions, comparison_hits in zip(
                    gold["primary_competitor"],
                    gold["competitor_mentions_count"],
                    gold["competitor_comparison_hits"],
                    strict=False,
                )
            ],
            "conversation_sentiment_label": [
                derive_conversation_sentiment_label(positive_hits, negative_hits)
                for positive_hits, negative_hits in zip(
                    gold["positive_tone_hits"],
                    gold["negative_tone_hits"],
                    strict=False,
                )
            ],
            "conversation_sentiment_support": [
                derive_conversation_sentiment_support(positive_hits, negative_hits)
                for positive_hits, negative_hits in zip(
                    gold["positive_tone_hits"],
                    gold["negative_tone_hits"],
                    strict=False,
                )
            ],
        }
    )
    segmented_baseline = add_gold_segments(gold, compiled_plan=compiled_plan)
    deterministic_semantics["intent_stage"] = segmented_baseline["intent_stage"]
    deterministic_semantics["persona_profile"] = segmented_baseline["persona_profile"]
    deterministic_semantics["audience_segment"] = segmented_baseline["audience_segment"]
    gold = segmented_baseline
    semantic_columns = [
        "intent_stage",
        "persona_profile",
        "audience_segment",
        "conversation_sentiment_label",
        "conversation_sentiment_support",
    ]
    provenance_columns = [
        "intent_stage_source_family",
        "persona_profile_source_family",
        "audience_segment_source_family",
        "conversation_sentiment_source_family",
    ]
    gold = gold.drop(columns=semantic_columns, errors="ignore")
    gold = gold.drop(columns=provenance_columns, errors="ignore")
    if silver_conversations_llm is None:
        semantic_gold = deterministic_semantics[["lead_key", *semantic_columns]].copy()
        semantic_gold = semantic_gold.assign(
            intent_stage_source_family="deterministic_fallback",
            persona_profile_source_family="deterministic_fallback",
            audience_segment_source_family="deterministic_fallback",
            conversation_sentiment_source_family="deterministic_fallback",
        )
    else:
        fallback_semantics = deterministic_semantics.rename(
            columns={column: f"{column}_fallback" for column in semantic_columns}
        )
        semantic_gold = fallback_semantics.merge(
            consolidate_gold_semantics(silver_conversations_llm),
            on="lead_key",
            how="left",
        )
        raw_persona = semantic_gold.get(
            "persona_profile", pd.Series("", index=semantic_gold.index)
        ).astype("string")
        raw_audience = semantic_gold.get(
            "audience_segment", pd.Series("", index=semantic_gold.index)
        ).astype("string")
        pair_invalid = (
            raw_persona.str.strip().eq("")
            | raw_audience.str.strip().eq("")
            | (
                raw_persona.map(lambda value: _canonical_audience_for_persona(value))
                != raw_audience
            )
        )
        for column in semantic_columns:
            fallback_column = f"{column}_fallback"
            if fallback_column not in semantic_gold.columns:
                semantic_gold[fallback_column] = None
        for column in semantic_columns:
            fallback_column = f"{column}_fallback"
            if fallback_column not in semantic_gold.columns:
                continue
            semantic_gold[column] = semantic_gold[column].where(
                semantic_gold[column].notna()
                & semantic_gold[column].astype("string").str.strip().ne(""),
                semantic_gold[fallback_column],
            )
        for column in provenance_columns:
            if column not in semantic_gold.columns:
                semantic_gold[column] = "deterministic_fallback"
            semantic_gold[column] = semantic_gold[column].where(
                semantic_gold[column].notna()
                & semantic_gold[column].astype("string").str.strip().ne(""),
                "deterministic_fallback",
            )
        semantic_gold.loc[pair_invalid, "persona_profile"] = semantic_gold.loc[
            pair_invalid, "persona_profile_fallback"
        ]
        semantic_gold.loc[pair_invalid, "audience_segment"] = semantic_gold.loc[
            pair_invalid, "audience_segment_fallback"
        ]
        semantic_gold.loc[pair_invalid, "persona_profile_source_family"] = "deterministic_fallback"
        semantic_gold.loc[pair_invalid, "audience_segment_source_family"] = "deterministic_fallback"
        semantic_gold = semantic_gold[["lead_key", *semantic_columns, *provenance_columns]]
    gold = gold.merge(semantic_gold, on="lead_key", how="left")
    gold["price_objection_intensity"] = deterministic_semantics["price_objection_intensity"]
    gold["commercial_urgency_signal"] = deterministic_semantics["commercial_urgency_signal"]
    gold["competitor_pressure_level"] = deterministic_semantics["competitor_pressure_level"]
    gold["dominant_email_provider"] = gold["dominant_email_provider"].where(
        gold["contains_email"],
        None,
    )
    base_gold = (
        gold.drop(
            columns=[
                "quoted_price_mentions",
                "price_objection_hits",
                "urgency_hits",
                "urgency_strength_max",
                "competitor_mentions_count",
                "competitor_comparison_hits",
                "lead_lifecycle_hours",
            ]
        )
        .sort_values("lead_key")
        .reset_index(drop=True)
    )
    if gold_column_plan is not None:
        from pipeline.agent.gold_designer import GoldColumnPlan, apply_gold_column_plan

        if isinstance(gold_column_plan, GoldColumnPlan) and gold_column_plan.source == "llm":
            base_gold = apply_gold_column_plan(base_gold, gold_column_plan)
    return base_gold
