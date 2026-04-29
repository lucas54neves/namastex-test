from __future__ import annotations

import json

import pandas as pd

from pipeline.transforms.gold_macro import build_gold_macro  # noqa: F401
from pipeline.transforms.gold_segments import (  # noqa: F401
    _canonical_audience_for_persona,
    _commercial_urgency_signal,
    _competitor_pressure_level,
    _price_objection_intensity,
    _response_latency_band,
    add_gold_segments,
)
from pipeline.transforms.silver_aggregators import (
    _bool_series_or_default,
    _int_series_or_default,
    _last_non_null,
    _max_or_false,
    _value_series_or_default,
)
from pipeline.transforms.silver_patterns import (
    NEGATIVE_TONE_PATTERNS,
    POSITIVE_TONE_PATTERNS,
    _normalize_for_match,
    _safe_string,
)
from pipeline.transforms.silver_signals import (
    _count_tone_hits,
    derive_conversation_sentiment_label,
    derive_conversation_sentiment_support,
)

__all__ = [
    "build_gold",
    "add_gold_segments",
    "build_gold_macro",
    "_canonical_audience_for_persona",
    "_commercial_urgency_signal",
    "_competitor_pressure_level",
    "_price_objection_intensity",
    "_response_latency_band",
    "_parse_json_list",
    "_normalize_outcome_group",
]


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
