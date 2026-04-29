from __future__ import annotations

import json

import pandas as pd

# Reexports — retrocompatibility (noqa: F401 suppresses unused-import warnings)
from pipeline.transforms.gold import (  # noqa: F401
    _canonical_audience_for_persona,
    _commercial_urgency_signal,
    _competitor_pressure_level,
    _normalize_outcome_group,
    _parse_json_list,
    _price_objection_intensity,
    _response_latency_band,
    add_gold_segments,
    build_gold,
)
from pipeline.transforms.silver_aggregators import (  # noqa: F401
    _bool_series_or_default,
    _first_non_empty,
    _first_non_null,
    _int_series_or_default,
    _json_sorted_unique,
    _last_non_null,
    _max_or_false,
    _value_series_or_default,
)
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
    # gold entry point (reexported from gold.py)
    "add_gold_segments",
    "build_gold",
    # aggregation helpers (reexported from silver_aggregators.py)
    "_first_non_empty",
    "_json_sorted_unique",
    "_first_non_null",
    "_last_non_null",
    "_max_or_false",
    "_bool_series_or_default",
    "_int_series_or_default",
    "_value_series_or_default",
    # gold helpers (reexported from gold.py / gold_segments.py)
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
