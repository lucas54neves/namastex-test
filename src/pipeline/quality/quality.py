from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import pandas as pd
from pandas.api.types import is_object_dtype, is_string_dtype

from pipeline.orchestration.compiler import get_default_compiled_plan
from pipeline.quality.publication import (
    forbidden_columns_present,
    missing_required_safe_columns,
    resolve_publish_safe_keys,
)
from pipeline.transforms.conversation_enrichment import validate_conversation_enrichment_frame
from pipeline.transforms.silver import (
    _commercial_urgency_signal,
    _competitor_pressure_level,
    _price_objection_intensity,
    detect_unmasked_sensitive_classes,
)

SILVER_TEXT_COLUMNS = ("canonical_lead_name_masked", "lead_contact_ref")
SILVER_MESSAGES_TEXT_COLUMNS = ("message_body_masked", "sender_name_masked", "sender_phone_masked")
SILVER_CONVERSATIONS_LLM_TEXT_COLUMNS = ("explanation_short",)
GOLD_TEXT_SCAN_EXCLUDED_COLUMNS = frozenset(
    {
        "conversation_id",
        "campaign_id",
        "agent_id",
        "engagement_bucket",
        "persona_profile",
        "audience_segment",
        "lead_temperature",
        "price_sensitivity",
        "intent_stage",
        "contact_readiness",
        "risk_signal",
        "primary_competitor",
        "city",
        "state",
        "lead_source",
        "vehicle_make",
        "vehicle_model",
        "primary_sinistro_type",
        "conversation_outcome",
    }
)
PERSONA_AUDIENCE_COMPATIBILITY = {
    "lead_frio": "nutricao_basica",
    "cliente_pos_sinistro": "retencao_pos_sinistro",
    "cotador_comparador": "oferta_competitiva",
    "lead_engajado_com_dados": "close_comercial",
}


@dataclass(frozen=True)
class ValidationResult:
    layer: str
    check: str
    status: str
    detail: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "layer": self.layer,
            "check": self.check,
            "status": self.status,
            "detail": self.detail,
        }


def _result(layer: str, check: str, passed: bool, **detail: Any) -> ValidationResult:
    return ValidationResult(
        layer=layer,
        check=check,
        status="passed" if passed else "failed",
        detail=detail,
    )


def _column_set_check(
    df: pd.DataFrame, required_columns: list[str], layer: str
) -> ValidationResult:
    missing = sorted(set(required_columns) - set(df.columns))
    return _result(layer, "required_columns", not missing, missing=missing)


def _published_text_columns(df: pd.DataFrame, layer: str) -> list[str]:
    if layer == "silver":
        return [column for column in SILVER_TEXT_COLUMNS if column in df.columns]
    if layer == "silver_messages":
        return [column for column in SILVER_MESSAGES_TEXT_COLUMNS if column in df.columns]
    if layer == "silver_conversations_llm":
        return [column for column in SILVER_CONVERSATIONS_LLM_TEXT_COLUMNS if column in df.columns]
    if layer == "gold":
        return [
            column
            for column in df.columns
            if column not in GOLD_TEXT_SCAN_EXCLUDED_COLUMNS
            and (is_string_dtype(df[column]) or is_object_dtype(df[column]))
        ]
    raise ValueError(f"Unsupported validation layer: {layer}")


def _sensitive_leak_detail(
    df: pd.DataFrame, columns: list[str], classes: list[str]
) -> dict[str, Any]:
    leaking_rows: set[int] = set()
    matched_patterns: set[str] = set()
    leaking_columns: set[str] = set()
    for column in columns:
        for row_position, value in enumerate(df[column].tolist()):
            matches = detect_unmasked_sensitive_classes(value, classes=classes)
            if not matches:
                continue
            leaking_rows.add(row_position)
            leaking_columns.add(column)
            matched_patterns.update(matches)
    return {
        "checked_columns": columns,
        "leaking_rows": len(leaking_rows),
        "matched_patterns": sorted(matched_patterns),
        "leaking_columns": sorted(leaking_columns),
        "matched_row_indices": sorted(leaking_rows)[:10],
    }


def _sensitive_class_check(df: pd.DataFrame, layer: str, sensitive_class: str) -> ValidationResult:
    checked_columns = _published_text_columns(df, layer)
    detail = _sensitive_leak_detail(df, checked_columns, [sensitive_class])
    return _result(
        layer,
        f"masked_{sensitive_class}_not_leaking",
        detail["leaking_rows"] == 0,
        **detail,
    )


def _masked_text_fields_not_leaking_check(df: pd.DataFrame, layer: str) -> ValidationResult:
    checked_columns = _published_text_columns(df, layer)
    detail = _sensitive_leak_detail(
        df,
        checked_columns,
        ["email", "phone", "cpf", "cep", "plate"],
    )
    return _result(
        layer,
        "masked_text_fields_not_leaking",
        detail["leaking_rows"] == 0,
        **detail,
    )


def _missing_columns(df: pd.DataFrame, required_columns: list[str]) -> list[str]:
    return sorted(column for column in required_columns if column not in df.columns)


def _bounded_sample(values: list[object], limit: int = 5) -> list[str]:
    sample: list[str] = []
    for value in values[:limit]:
        if pd.isna(value):
            sample.append("<null>")
        else:
            sample.append(str(value))
    return sample


def _failed_due_to_missing_columns(
    layer: str, check: str, df: pd.DataFrame, required_columns: list[str]
) -> ValidationResult | None:
    missing = _missing_columns(df, required_columns)
    if not missing:
        return None
    return _result(layer, check, False, missing_columns=missing)


def _first_message_outbound_check(df: pd.DataFrame) -> ValidationResult:
    missing_columns_result = _failed_due_to_missing_columns(
        "bronze",
        "first_message_outbound",
        df,
        ["conversation_id", "timestamp", "direction", "message_id"],
    )
    if missing_columns_result is not None:
        return missing_columns_result

    ordered = df.sort_values(["conversation_id", "timestamp", "message_id"])
    first_messages = ordered.groupby("conversation_id", dropna=False).first().reset_index()
    violating = first_messages.loc[~first_messages["direction"].eq("outbound"), "conversation_id"]
    checked_conversations = int(len(first_messages))
    violating_ids = violating.tolist()
    outbound_ratio = (
        float(first_messages["direction"].eq("outbound").mean()) if checked_conversations else 1.0
    )
    return _result(
        "bronze",
        "first_message_outbound",
        True,
        checked_conversations=checked_conversations,
        violating_conversations=len(violating_ids),
        sample_conversation_ids=_bounded_sample(violating_ids),
        outbound_ratio=outbound_ratio,
        enforcement_mode="diagnostic",
        expectation="soft_source_expectation",
    )


def _silver_message_aggregates(silver_messages_df: pd.DataFrame) -> pd.DataFrame:
    grouped = silver_messages_df.groupby("lead_key", dropna=False)
    return grouped.agg(
        first_seen_at=("timestamp", "min"),
        last_seen_at=("timestamp", "max"),
        conversation_count=("conversation_id", "nunique"),
        message_count=("lead_key", "size"),
        has_vehicle_signal=("mentions_vehicle", "max"),
        has_competitor_signal=("mentions_competitor", "max"),
        has_sinistro_signal=("mentions_sinistro", "max"),
        has_email_signal=("contains_email", "max"),
        has_phone_signal=("contains_phone", "max"),
        has_cpf_signal=("contains_cpf", "max"),
        has_cep_signal=("contains_cep", "max"),
        has_plate_signal=("contains_plate", "max"),
    )


def _gold_message_aggregates(silver_messages_df: pd.DataFrame) -> pd.DataFrame:
    enriched = silver_messages_df.copy()
    if "price_objection_signal_inbound" not in enriched.columns:
        enriched["price_objection_signal_inbound"] = enriched["price_objection_signal"].fillna(
            False
        ).astype(bool) & enriched["direction"].eq("inbound")
    if "urgency_strength_inbound" not in enriched.columns:
        enriched["urgency_strength_inbound"] = (
            pd.to_numeric(enriched["urgency_strength"], errors="coerce").fillna(0).astype(int)
        ).where(enriched["direction"].eq("inbound"), 0)
    if "competitor_comparison_signal_inbound" not in enriched.columns:
        enriched["competitor_comparison_signal_inbound"] = enriched[
            "competitor_comparison_signal"
        ].fillna(False).astype(bool) & enriched["direction"].eq("inbound")
    if "mentions_competitor_inbound" not in enriched.columns:
        enriched["mentions_competitor_inbound"] = enriched["mentions_competitor"].fillna(
            False
        ).astype(bool) & enriched["direction"].eq("inbound")
    grouped = enriched.groupby("lead_key", dropna=False)
    return grouped.agg(
        first_seen_at=("timestamp", "min"),
        last_seen_at=("timestamp", "max"),
        conversation_count=("conversation_id", "nunique"),
        total_messages=("lead_key", "size"),
        inbound_messages=("direction", lambda values: int(values.eq("inbound").sum())),
        outbound_messages=("direction", lambda values: int(values.eq("outbound").sum())),
        duplicate_events_removed=("dropped_duplicate_events", "sum"),
        contains_email=("contains_email", "max"),
        contains_phone=("contains_phone", "max"),
        contains_cpf=("contains_cpf", "max"),
        contains_cep=("contains_cep", "max"),
        contains_plate=("contains_plate", "max"),
        mentioned_vehicle=("mentions_vehicle", "max"),
        mentioned_competitor=("mentions_competitor", "max"),
        mentioned_sinistro=("mentions_sinistro", "max"),
        quoted_price_mentions=("quoted_price", lambda values: int(values.notna().sum())),
        price_objection_hits=("price_objection_signal_inbound", "sum"),
        urgency_hits=("urgency_strength_inbound", lambda values: int(values.gt(0).sum())),
        urgency_strength_max=("urgency_strength_inbound", "max"),
        competitor_mentions_count=("mentions_competitor_inbound", "sum"),
        competitor_comparison_hits=("competitor_comparison_signal_inbound", "sum"),
    )


def _series_equal(left: pd.Series, right: pd.Series) -> pd.Series:
    return left.eq(right) | (left.isna() & right.isna())


def _expected_gold_classifications(
    gold_df: pd.DataFrame, compiled_plan: dict[str, object] | None = None
) -> pd.DataFrame:
    plan = compiled_plan or get_default_compiled_plan()
    segmentation = cast(dict[str, Any], plan["gold_segmentation"])
    temperature_cfg = cast(dict[str, Any], segmentation["lead_temperature"])
    readiness_cfg = cast(dict[str, Any], segmentation["contact_readiness"])
    risk_cfg = cast(dict[str, Any], segmentation["risk_signal"])

    expected = pd.DataFrame(index=gold_df.index)

    expected["engagement_bucket"] = pd.cut(
        gold_df["total_messages"],
        bins=[0, 4, 10, 20, float("inf")],
        labels=["lead_frio", "curta", "media", "longa"],
        right=True,
    ).astype("string")

    expected["lead_temperature"] = str(temperature_cfg["default"])
    expected.loc[
        expected["engagement_bucket"].eq(str(temperature_cfg["cold_bucket"]))
        & gold_df["data_shared_score"].eq(int(temperature_cfg["cold_data_shared_score"])),
        "lead_temperature",
    ] = str(temperature_cfg["cold_label"])
    expected.loc[
        expected["engagement_bucket"].isin(list(temperature_cfg["hot_buckets"]))
        | gold_df["data_shared_score"].ge(int(temperature_cfg["hot_min_data_shared_score"])),
        "lead_temperature",
    ] = str(temperature_cfg["hot_label"])

    expected["contact_readiness"] = str(readiness_cfg["default"])
    expected.loc[
        gold_df["data_shared_score"].eq(int(readiness_cfg["medium_score"])),
        "contact_readiness",
    ] = str(readiness_cfg["medium_label"])
    expected.loc[
        gold_df["data_shared_score"].ge(int(readiness_cfg["high_min_score"])),
        "contact_readiness",
    ] = str(readiness_cfg["high_label"])

    expected["risk_signal"] = str(risk_cfg["default"])
    expected.loc[
        gold_df["mentioned_sinistro"] | gold_df["duplicate_events_removed"].gt(0),
        "risk_signal",
    ] = str(risk_cfg["medium_label"])
    expected.loc[
        gold_df["mentioned_sinistro"] & gold_df["contains_cpf"],
        "risk_signal",
    ] = str(risk_cfg["high_label"])

    return expected


def _semantic_contract(compiled_plan: dict[str, object] | None = None) -> dict[str, Any]:
    plan = compiled_plan or get_default_compiled_plan()
    return cast(dict[str, Any], plan["gold_semantic_contracts"])


def _interpretive_provenance_columns(
    compiled_plan: dict[str, object] | None = None,
) -> dict[str, str]:
    contract = _semantic_contract(compiled_plan)
    return cast(dict[str, str], contract["interpretive_provenance_columns"])


def validate_silver_consistency(
    silver_df: pd.DataFrame,
    silver_messages_df: pd.DataFrame,
) -> list[ValidationResult]:
    required_message_columns = [
        "lead_key",
        "timestamp",
        "conversation_id",
        "mentions_vehicle",
        "mentions_competitor",
        "mentions_sinistro",
        "contains_email",
        "contains_phone",
        "contains_cpf",
        "contains_cep",
        "contains_plate",
    ]
    required_silver_columns = [
        "lead_key",
        "first_seen_at",
        "last_seen_at",
        "conversation_count",
        "message_count",
        "has_vehicle_signal",
        "has_competitor_signal",
        "has_sinistro_signal",
        "has_email_signal",
        "has_phone_signal",
        "has_cpf_signal",
        "has_cep_signal",
        "has_plate_signal",
    ]
    results: list[ValidationResult] = []

    missing_result = _failed_due_to_missing_columns(
        "silver", "silver_lead_keys_have_messages", silver_df, required_silver_columns
    )
    if missing_result is not None:
        return [missing_result]
    missing_result = _failed_due_to_missing_columns(
        "silver_messages",
        "silver_lead_keys_have_messages",
        silver_messages_df,
        required_message_columns,
    )
    if missing_result is not None:
        return [missing_result]

    message_aggregates = _silver_message_aggregates(silver_messages_df)
    silver = silver_df.set_index("lead_key", drop=False)

    missing_lead_keys = silver.index.difference(message_aggregates.index).tolist()
    results.append(
        _result(
            "silver",
            "silver_lead_keys_have_messages",
            len(missing_lead_keys) == 0,
            violating_leads=len(missing_lead_keys),
            sample_lead_keys=_bounded_sample(missing_lead_keys),
        )
    )

    comparable = silver.index.intersection(message_aggregates.index)
    silver_common = silver.loc[comparable]
    message_common = message_aggregates.loc[comparable]

    timestamp_mask = _series_equal(silver_common["first_seen_at"], message_common["first_seen_at"])
    timestamp_mask &= _series_equal(silver_common["last_seen_at"], message_common["last_seen_at"])
    violating_timestamp = silver_common.index[~timestamp_mask].tolist()
    results.append(
        _result(
            "silver",
            "silver_timestamps_match_messages",
            len(violating_timestamp) == 0,
            checked_leads=int(len(comparable)),
            violating_leads=len(violating_timestamp),
            sample_lead_keys=_bounded_sample(violating_timestamp),
        )
    )

    conversation_mask = silver_common["conversation_count"].eq(message_common["conversation_count"])
    violating_conversation = silver_common.index[~conversation_mask].tolist()
    results.append(
        _result(
            "silver",
            "silver_conversation_count_matches_messages",
            len(violating_conversation) == 0,
            checked_leads=int(len(comparable)),
            violating_leads=len(violating_conversation),
            sample_lead_keys=_bounded_sample(violating_conversation),
        )
    )

    message_mask = silver_common["message_count"].eq(message_common["message_count"])
    violating_message = silver_common.index[~message_mask].tolist()
    results.append(
        _result(
            "silver",
            "silver_message_count_matches_messages",
            len(violating_message) == 0,
            checked_leads=int(len(comparable)),
            violating_leads=len(violating_message),
            sample_lead_keys=_bounded_sample(violating_message),
        )
    )

    signal_columns = [
        "has_vehicle_signal",
        "has_competitor_signal",
        "has_sinistro_signal",
        "has_email_signal",
        "has_phone_signal",
        "has_cpf_signal",
        "has_cep_signal",
        "has_plate_signal",
    ]
    signal_mask = pd.Series(True, index=comparable)
    for column in signal_columns:
        signal_mask &= silver_common[column].astype(bool).eq(message_common[column].astype(bool))
    violating_signal = silver_common.index[~signal_mask].tolist()
    results.append(
        _result(
            "silver",
            "silver_signal_flags_match_messages",
            len(violating_signal) == 0,
            checked_leads=int(len(comparable)),
            violating_leads=len(violating_signal),
            sample_lead_keys=_bounded_sample(violating_signal),
        )
    )
    return results


def validate_silver_conversations_llm(
    enrichment_df: pd.DataFrame,
    compiled_plan: dict[str, object] | None = None,
) -> list[ValidationResult]:
    plan = compiled_plan or get_default_compiled_plan()
    validation_errors = validate_conversation_enrichment_frame(enrichment_df, plan)
    return [
        _result(
            "silver_conversations_llm",
            "conversation_enrichment_contract_valid",
            len(validation_errors) == 0,
            validation_errors=validation_errors[:10],
            validation_error_count=len(validation_errors),
        ),
        _result(
            "silver_conversations_llm",
            "forbidden_raw_columns_absent",
            len(forbidden_columns_present(enrichment_df, "silver_conversations_llm")) == 0,
            present=forbidden_columns_present(enrichment_df, "silver_conversations_llm"),
        ),
        _result(
            "silver_conversations_llm",
            "required_safe_columns_present",
            len(missing_required_safe_columns(enrichment_df, "silver_conversations_llm")) == 0,
            missing=missing_required_safe_columns(enrichment_df, "silver_conversations_llm"),
        ),
        _masked_text_fields_not_leaking_check(enrichment_df, "silver_conversations_llm"),
    ]


def validate_gold_consistency(
    gold_df: pd.DataFrame,
    silver_df: pd.DataFrame,
    silver_messages_df: pd.DataFrame,
    compiled_plan: dict[str, object] | None = None,
) -> list[ValidationResult]:
    required_gold_columns = [
        "lead_key",
        "first_seen_at",
        "last_seen_at",
        "conversation_count",
        "total_messages",
        "inbound_messages",
        "outbound_messages",
        "duplicate_events_removed",
        "contains_email",
        "contains_phone",
        "contains_cpf",
        "contains_cep",
        "contains_plate",
        "mentioned_vehicle",
        "mentioned_competitor",
        "mentioned_sinistro",
        "engagement_bucket",
        "data_shared_score",
        "lead_temperature",
        "contact_readiness",
        "risk_signal",
        "dominant_email_provider",
        "response_latency_band",
        "closure_outcome_group",
        "has_closed_outcome",
        "price_objection_intensity",
        "commercial_urgency_signal",
        "competitor_pressure_level",
        "conversation_sentiment_label",
        "conversation_sentiment_support",
        "intent_stage_source_family",
        "persona_profile_source_family",
        "audience_segment_source_family",
        "conversation_sentiment_source_family",
        "positive_tone_hits",
        "negative_tone_hits",
        "avg_response_time_sec",
        "avg_quoted_price",
        "primary_competitor",
    ]
    required_silver_columns = ["lead_key", "first_seen_at", "last_seen_at", "conversation_count"]
    required_message_columns = [
        "lead_key",
        "timestamp",
        "conversation_id",
        "direction",
        "dropped_duplicate_events",
        "contains_email",
        "contains_phone",
        "contains_cpf",
        "contains_cep",
        "contains_plate",
        "mentions_vehicle",
        "mentions_competitor",
        "mentions_sinistro",
        "quoted_price",
        "price_objection_signal",
        "urgency_strength",
        "competitor_comparison_signal",
    ]
    results: list[ValidationResult] = []

    missing_result = _failed_due_to_missing_columns(
        "gold", "gold_lead_keys_in_silver", gold_df, required_gold_columns
    )
    if missing_result is not None:
        return [missing_result]
    missing_result = _failed_due_to_missing_columns(
        "silver", "gold_lead_keys_in_silver", silver_df, required_silver_columns
    )
    if missing_result is not None:
        return [missing_result]
    missing_result = _failed_due_to_missing_columns(
        "silver_messages", "gold_lead_keys_in_silver", silver_messages_df, required_message_columns
    )
    if missing_result is not None:
        return [missing_result]

    gold = gold_df.set_index("lead_key", drop=False)
    silver = silver_df.set_index("lead_key", drop=False)
    message_aggregates = _gold_message_aggregates(silver_messages_df)

    missing_in_silver = gold.index.difference(silver.index).tolist()
    results.append(
        _result(
            "gold",
            "gold_lead_keys_in_silver",
            len(missing_in_silver) == 0,
            violating_leads=len(missing_in_silver),
            sample_lead_keys=_bounded_sample(missing_in_silver),
        )
    )

    comparable = gold.index.intersection(silver.index).intersection(message_aggregates.index)
    gold_common = gold.loc[comparable]
    silver_common = silver.loc[comparable]
    message_common = message_aggregates.loc[comparable]

    timestamp_mask = gold_common["first_seen_at"].ge(silver_common["first_seen_at"])
    timestamp_mask &= gold_common["last_seen_at"].ge(silver_common["last_seen_at"])
    timestamp_mask &= _series_equal(gold_common["first_seen_at"], message_common["first_seen_at"])
    timestamp_mask &= _series_equal(gold_common["last_seen_at"], message_common["last_seen_at"])
    violating_timestamp = gold_common.index[~timestamp_mask].tolist()
    results.append(
        _result(
            "gold",
            "gold_timestamps_match_published_history",
            len(violating_timestamp) == 0,
            checked_leads=int(len(comparable)),
            violating_leads=len(violating_timestamp),
            sample_lead_keys=_bounded_sample(violating_timestamp),
        )
    )

    conversation_mask = gold_common["conversation_count"].eq(message_common["conversation_count"])
    violating_conversation = gold_common.index[~conversation_mask].tolist()
    results.append(
        _result(
            "gold",
            "gold_conversation_count_matches_history",
            len(violating_conversation) == 0,
            checked_leads=int(len(comparable)),
            violating_leads=len(violating_conversation),
            sample_lead_keys=_bounded_sample(violating_conversation),
        )
    )

    message_mask = gold_common["total_messages"].eq(message_common["total_messages"])
    message_mask &= gold_common["inbound_messages"].eq(message_common["inbound_messages"])
    message_mask &= gold_common["outbound_messages"].eq(message_common["outbound_messages"])
    message_mask &= gold_common["duplicate_events_removed"].eq(
        message_common["duplicate_events_removed"]
    )
    message_mask &= (
        gold_common["inbound_messages"]
        .add(gold_common["outbound_messages"])
        .eq(gold_common["total_messages"])
    )
    violating_message = gold_common.index[~message_mask].tolist()
    results.append(
        _result(
            "gold",
            "gold_message_totals_match_history",
            len(violating_message) == 0,
            checked_leads=int(len(comparable)),
            violating_leads=len(violating_message),
            sample_lead_keys=_bounded_sample(violating_message),
        )
    )

    signal_columns = [
        "contains_email",
        "contains_phone",
        "contains_cpf",
        "contains_cep",
        "contains_plate",
        "mentioned_vehicle",
        "mentioned_competitor",
        "mentioned_sinistro",
    ]
    signal_mask = pd.Series(True, index=comparable)
    for column in signal_columns:
        signal_mask &= gold_common[column].astype(bool).eq(message_common[column].astype(bool))
    violating_signal = gold_common.index[~signal_mask].tolist()
    results.append(
        _result(
            "gold",
            "gold_signal_flags_match_history",
            len(violating_signal) == 0,
            checked_leads=int(len(comparable)),
            violating_leads=len(violating_signal),
            sample_lead_keys=_bounded_sample(violating_signal),
        )
    )

    expected = _expected_gold_classifications(gold_common, compiled_plan=compiled_plan)
    engagement_mask = _series_equal(
        gold_common["engagement_bucket"].astype("string"),
        expected["engagement_bucket"].astype("string"),
    )
    violating_engagement = gold_common.index[~engagement_mask].tolist()
    results.append(
        _result(
            "gold",
            "engagement_bucket_coherent",
            len(violating_engagement) == 0,
            checked_leads=int(len(comparable)),
            violating_leads=len(violating_engagement),
            sample_lead_keys=_bounded_sample(violating_engagement),
        )
    )

    temperature_mask = _series_equal(
        gold_common["lead_temperature"].astype("string"),
        expected["lead_temperature"].astype("string"),
    )
    violating_temperature = gold_common.index[~temperature_mask].tolist()
    results.append(
        _result(
            "gold",
            "lead_temperature_coherent",
            len(violating_temperature) == 0,
            checked_leads=int(len(comparable)),
            violating_leads=len(violating_temperature),
            sample_lead_keys=_bounded_sample(violating_temperature),
        )
    )

    readiness_mask = _series_equal(
        gold_common["contact_readiness"].astype("string"),
        expected["contact_readiness"].astype("string"),
    )
    violating_readiness = gold_common.index[~readiness_mask].tolist()
    results.append(
        _result(
            "gold",
            "contact_readiness_coherent",
            len(violating_readiness) == 0,
            checked_leads=int(len(comparable)),
            violating_leads=len(violating_readiness),
            sample_lead_keys=_bounded_sample(violating_readiness),
        )
    )

    risk_mask = _series_equal(
        gold_common["risk_signal"].astype("string"),
        expected["risk_signal"].astype("string"),
    )
    violating_risk = gold_common.index[~risk_mask].tolist()
    results.append(
        _result(
            "gold",
            "risk_signal_coherent",
            len(violating_risk) == 0,
            checked_leads=int(len(comparable)),
            violating_leads=len(violating_risk),
            sample_lead_keys=_bounded_sample(violating_risk),
        )
    )
    provider_mask = (
        gold_common["dominant_email_provider"]
        .notna()
        .eq(gold_common["contains_email"].astype(bool))
    )
    violating_provider = gold_common.index[~provider_mask].tolist()
    results.append(
        _result(
            "gold",
            "dominant_email_provider_coherent",
            len(violating_provider) == 0,
            checked_leads=int(len(comparable)),
            violating_leads=len(violating_provider),
            sample_lead_keys=_bounded_sample(violating_provider),
        )
    )

    expected_latency = gold_common["avg_response_time_sec"].map(
        lambda value: "sem_evidencia"
        if pd.isna(value)
        else "rapida"
        if float(value) <= 300
        else "moderada"
        if float(value) <= 1800
        else "lenta"
    )
    latency_mask = _series_equal(
        gold_common["response_latency_band"].astype("string"),
        expected_latency.astype("string"),
    )
    violating_latency = gold_common.index[~latency_mask].tolist()
    results.append(
        _result(
            "gold",
            "response_latency_band_coherent",
            len(violating_latency) == 0,
            checked_leads=int(len(comparable)),
            violating_leads=len(violating_latency),
            sample_lead_keys=_bounded_sample(violating_latency),
        )
    )

    closure_mask = (
        gold_common["has_closed_outcome"]
        .astype(bool)
        .eq(gold_common["closure_outcome_group"].eq("fechado"))
    )
    violating_closure = gold_common.index[~closure_mask].tolist()
    results.append(
        _result(
            "gold",
            "closure_outcome_coherent",
            len(violating_closure) == 0,
            checked_leads=int(len(comparable)),
            violating_leads=len(violating_closure),
            sample_lead_keys=_bounded_sample(violating_closure),
        )
    )

    expected_price_objection = pd.Series(
        [
            _price_objection_intensity(price_hits, competitor_mentions, quote_mentions)
            for price_hits, competitor_mentions, quote_mentions in zip(
                message_common["price_objection_hits"],
                message_common["competitor_mentions_count"],
                message_common["quoted_price_mentions"],
                strict=False,
            )
        ],
        index=gold_common.index,
        dtype="string",
    )
    price_mask = _series_equal(
        gold_common["price_objection_intensity"].astype("string"),
        expected_price_objection,
    )
    violating_price = gold_common.index[~price_mask].tolist()
    results.append(
        _result(
            "gold",
            "price_objection_intensity_coherent",
            len(violating_price) == 0,
            checked_leads=int(len(comparable)),
            violating_leads=len(violating_price),
            sample_lead_keys=_bounded_sample(violating_price),
        )
    )

    lifecycle_hours = (
        gold_common["last_seen_at"] - gold_common["first_seen_at"]
    ).dt.total_seconds() / 3600.0
    expected_urgency = pd.Series(
        [
            _commercial_urgency_signal(max_strength, hit_count, lead_lifecycle_hours)
            for max_strength, hit_count, lead_lifecycle_hours in zip(
                message_common["urgency_strength_max"],
                message_common["urgency_hits"],
                lifecycle_hours,
                strict=False,
            )
        ],
        index=gold_common.index,
        dtype="string",
    )
    urgency_mask = _series_equal(
        gold_common["commercial_urgency_signal"].astype("string"),
        expected_urgency,
    )
    violating_urgency = gold_common.index[~urgency_mask].tolist()
    results.append(
        _result(
            "gold",
            "commercial_urgency_signal_coherent",
            len(violating_urgency) == 0,
            checked_leads=int(len(comparable)),
            violating_leads=len(violating_urgency),
            sample_lead_keys=_bounded_sample(violating_urgency),
        )
    )

    expected_competitor_pressure = pd.Series(
        [
            _competitor_pressure_level(primary_competitor, mentions, comparison_hits)
            for primary_competitor, mentions, comparison_hits in zip(
                gold_common["primary_competitor"],
                message_common["competitor_mentions_count"],
                message_common["competitor_comparison_hits"],
                strict=False,
            )
        ],
        index=gold_common.index,
        dtype="string",
    )
    competitor_mask = _series_equal(
        gold_common["competitor_pressure_level"].astype("string"),
        expected_competitor_pressure,
    )
    violating_competitor = gold_common.index[~competitor_mask].tolist()
    results.append(
        _result(
            "gold",
            "competitor_pressure_level_coherent",
            len(violating_competitor) == 0,
            checked_leads=int(len(comparable)),
            violating_leads=len(violating_competitor),
            sample_lead_keys=_bounded_sample(violating_competitor),
        )
    )

    provenance_columns = list(_interpretive_provenance_columns(compiled_plan).values())
    valid_source_families = cast(
        set[str],
        (compiled_plan or get_default_compiled_plan())["gold_valid_semantic_source_families"],
    )
    provenance_mask = pd.Series(True, index=gold_common.index)
    for column in provenance_columns:
        provenance_mask &= gold_common[column].astype("string").isin(valid_source_families)
    violating_provenance = gold_common.index[~provenance_mask].tolist()
    results.append(
        _result(
            "gold",
            "semantic_source_family_valid",
            len(violating_provenance) == 0,
            checked_leads=int(len(comparable)),
            violating_leads=len(violating_provenance),
            sample_lead_keys=_bounded_sample(violating_provenance),
        )
    )
    return results


def validate_cross_layer_consistency(
    silver_df: pd.DataFrame,
    silver_messages_df: pd.DataFrame,
    gold_df: pd.DataFrame,
    compiled_plan: dict[str, object] | None = None,
) -> list[ValidationResult]:
    return validate_silver_consistency(silver_df, silver_messages_df) + validate_gold_consistency(
        gold_df,
        silver_df,
        silver_messages_df,
        compiled_plan=compiled_plan,
    )


def validate_bronze(
    df: pd.DataFrame, compiled_plan: dict[str, object] | None = None
) -> list[ValidationResult]:
    plan = compiled_plan or get_default_compiled_plan()
    bronze_required_columns = cast(list[str], plan["bronze_required_columns"])
    supported_channels = cast(set[str], plan["supported_channels"])
    results = [
        _column_set_check(df, bronze_required_columns, "bronze"),
        _result(
            "bronze",
            "message_id_unique",
            df["message_id"].nunique(dropna=False) == len(df),
            unique_ids=int(df["message_id"].nunique(dropna=False)),
            rows=int(len(df)),
        ),
        _result(
            "bronze",
            "channel_whatsapp_only",
            df["channel"].isin(supported_channels).all(),
            distinct_channels=sorted(df["channel"].dropna().unique().tolist()),
        ),
        _first_message_outbound_check(df),
    ]
    return results


def validate_silver(
    df: pd.DataFrame, compiled_plan: dict[str, object] | None = None
) -> list[ValidationResult]:
    plan = compiled_plan or get_default_compiled_plan()
    silver_required_columns = cast(list[str], plan["silver_lead_required_columns"])
    forbidden_columns = forbidden_columns_present(df, "silver")
    missing_safe_columns = missing_required_safe_columns(df, "silver")
    results = [
        _column_set_check(df, silver_required_columns, "silver"),
        _result(
            "silver",
            "forbidden_raw_columns_absent",
            not forbidden_columns,
            forbidden_columns=forbidden_columns,
        ),
        _result(
            "silver",
            "required_safe_columns_present",
            not missing_safe_columns,
            missing_safe_columns=missing_safe_columns,
        ),
        _result(
            "silver",
            "lead_key_unique",
            df["lead_key"].nunique(dropna=False) == len(df),
            unique_ids=int(df["lead_key"].nunique(dropna=False)),
            rows=int(len(df)),
        ),
        _result(
            "silver",
            "lead_timestamps_not_null",
            df["first_seen_at"].notna().all() and df["last_seen_at"].notna().all(),
            null_first_seen=int(df["first_seen_at"].isna().sum()),
            null_last_seen=int(df["last_seen_at"].isna().sum()),
        ),
        _result(
            "silver",
            "lead_counts_non_negative",
            bool((df["conversation_count"] >= 0).all() and (df["message_count"] >= 0).all()),
            min_conversation_count=int(df["conversation_count"].min()),
            min_message_count=int(df["message_count"].min()),
        ),
        _masked_text_fields_not_leaking_check(df, "silver"),
    ]
    return results


def validate_silver_messages(
    df: pd.DataFrame, compiled_plan: dict[str, object] | None = None
) -> list[ValidationResult]:
    plan = compiled_plan or get_default_compiled_plan()
    dedupe_keys = resolve_publish_safe_keys(cast(list[str], plan["dedupe_keys"]), df.columns)
    silver_required_columns = cast(list[str], plan["silver_message_required_columns"])
    duplicate_rows = int(df.duplicated(subset=dedupe_keys).sum()) if dedupe_keys else 0
    forbidden_columns = forbidden_columns_present(df, "silver_messages")
    missing_safe_columns = missing_required_safe_columns(df, "silver_messages")
    results = [
        _column_set_check(df, silver_required_columns, "silver_messages"),
        _result(
            "silver_messages",
            "forbidden_raw_columns_absent",
            not forbidden_columns,
            forbidden_columns=forbidden_columns,
        ),
        _result(
            "silver_messages",
            "required_safe_columns_present",
            not missing_safe_columns,
            missing_safe_columns=missing_safe_columns,
        ),
        _result(
            "silver_messages",
            "timestamp_not_null",
            df["timestamp"].notna().all(),
            null_timestamps=int(df["timestamp"].isna().sum()),
        ),
        _result(
            "silver_messages",
            "dedupe_keys_unique",
            duplicate_rows == 0,
            duplicate_rows=duplicate_rows,
            dedupe_keys=dedupe_keys,
        ),
        _sensitive_class_check(df, "silver_messages", "email"),
        _sensitive_class_check(df, "silver_messages", "phone"),
        _sensitive_class_check(df, "silver_messages", "cpf"),
        _sensitive_class_check(df, "silver_messages", "cep"),
        _sensitive_class_check(df, "silver_messages", "plate"),
        _result(
            "silver_messages",
            "vehicle_mentions_consistent",
            bool(
                (
                    df["mentions_vehicle"]
                    == (
                        df["vehicle_make"].notna()
                        | df["vehicle_model"].notna()
                        | df["vehicle_year"].notna()
                        | df["contains_plate"]
                    )
                ).all()
            ),
            vehicle_rows=int(df["mentions_vehicle"].sum()),
        ),
    ]
    return results


def validate_gold(
    df: pd.DataFrame, compiled_plan: dict[str, object] | None = None
) -> list[ValidationResult]:
    plan = compiled_plan or get_default_compiled_plan()
    valid_buckets = cast(set[str], plan["gold_valid_buckets"])
    valid_personas = cast(set[str], plan["gold_valid_personas"])
    valid_audiences = cast(set[str], plan["gold_valid_audiences"])
    valid_temperatures = cast(set[str], plan["gold_valid_temperatures"])
    valid_price_sensitivities = cast(set[str], plan["gold_valid_price_sensitivities"])
    valid_intent_stages = cast(set[str], plan["gold_valid_intent_stages"])
    valid_contact_readiness = cast(set[str], plan["gold_valid_contact_readiness"])
    valid_risk_signals = cast(set[str], plan["gold_valid_risk_signals"])
    valid_email_providers = cast(set[str], plan["gold_valid_email_providers"])
    valid_response_latency_bands = cast(set[str], plan["gold_valid_response_latency_bands"])
    valid_closure_outcome_groups = cast(set[str], plan["gold_valid_closure_outcome_groups"])
    valid_price_objection_intensities = cast(
        set[str], plan["gold_valid_price_objection_intensities"]
    )
    valid_commercial_urgency_signals = cast(set[str], plan["gold_valid_commercial_urgency_signals"])
    valid_competitor_pressure_levels = cast(set[str], plan["gold_valid_competitor_pressure_levels"])
    valid_conversation_sentiment_labels = cast(
        set[str], plan["gold_valid_conversation_sentiment_labels"]
    )
    valid_conversation_sentiment_supports = cast(
        set[str], plan["gold_valid_conversation_sentiment_supports"]
    )
    valid_semantic_source_families = cast(set[str], plan["gold_valid_semantic_source_families"])
    interpretive_provenance_columns = _interpretive_provenance_columns(plan)
    gold_required_columns = cast(list[str], plan["gold_required_columns"])
    forbidden_columns = forbidden_columns_present(df, "gold")
    results = [
        _column_set_check(df, gold_required_columns, "gold"),
        _result(
            "gold",
            "forbidden_raw_columns_absent",
            not forbidden_columns,
            forbidden_columns=forbidden_columns,
        ),
        _masked_text_fields_not_leaking_check(df, "gold"),
        _result(
            "gold",
            "lead_key_unique",
            df["lead_key"].nunique(dropna=False) == len(df),
            unique_ids=int(df["lead_key"].nunique(dropna=False)),
            rows=int(len(df)),
        ),
        _result(
            "gold",
            "conversation_count_non_negative",
            bool((df["conversation_count"] >= 0).all()),
            min_conversation_count=int(df["conversation_count"].min()),
        ),
        _result(
            "gold",
            "message_totals_non_negative",
            bool((df["total_messages"] >= 0).all()),
            min_total_messages=int(df["total_messages"].min()),
        ),
        _result(
            "gold",
            "engagement_bucket_valid",
            set(df["engagement_bucket"].dropna().astype(str).unique()).issubset(valid_buckets),
            distinct_buckets=sorted(df["engagement_bucket"].dropna().astype(str).unique().tolist()),
        ),
        _result(
            "gold",
            "duplicate_events_removed_non_negative",
            bool((df["duplicate_events_removed"] >= 0).all()),
            max_removed=int(df["duplicate_events_removed"].max()),
        ),
        _result(
            "gold",
            "persona_profile_valid",
            set(df["persona_profile"].dropna().astype(str).unique()).issubset(valid_personas),
            distinct_personas=sorted(df["persona_profile"].dropna().astype(str).unique().tolist()),
        ),
        _result(
            "gold",
            "audience_segment_valid",
            set(df["audience_segment"].dropna().astype(str).unique()).issubset(valid_audiences),
            distinct_audiences=sorted(
                df["audience_segment"].dropna().astype(str).unique().tolist()
            ),
        ),
        _result(
            "gold",
            "lead_temperature_valid",
            set(df["lead_temperature"].dropna().astype(str).unique()).issubset(valid_temperatures),
            distinct_temperatures=sorted(
                df["lead_temperature"].dropna().astype(str).unique().tolist()
            ),
        ),
        _result(
            "gold",
            "price_sensitivity_valid",
            set(df["price_sensitivity"].dropna().astype(str).unique()).issubset(
                valid_price_sensitivities
            ),
            distinct_price_sensitivities=sorted(
                df["price_sensitivity"].dropna().astype(str).unique().tolist()
            ),
        ),
        _result(
            "gold",
            "intent_stage_valid",
            set(df["intent_stage"].dropna().astype(str).unique()).issubset(valid_intent_stages),
            distinct_intent_stages=sorted(
                df["intent_stage"].dropna().astype(str).unique().tolist()
            ),
        ),
        _result(
            "gold",
            "contact_readiness_valid",
            set(df["contact_readiness"].dropna().astype(str).unique()).issubset(
                valid_contact_readiness
            ),
            distinct_contact_readiness=sorted(
                df["contact_readiness"].dropna().astype(str).unique().tolist()
            ),
        ),
        _result(
            "gold",
            "risk_signal_valid",
            set(df["risk_signal"].dropna().astype(str).unique()).issubset(valid_risk_signals),
            distinct_risk_signals=sorted(df["risk_signal"].dropna().astype(str).unique().tolist()),
        ),
        _result(
            "gold",
            "dominant_email_provider_valid",
            set(df["dominant_email_provider"].dropna().astype(str).unique()).issubset(
                valid_email_providers
            ),
            distinct_email_providers=sorted(
                df["dominant_email_provider"].dropna().astype(str).unique().tolist()
            ),
        ),
        _result(
            "gold",
            "response_latency_band_valid",
            set(df["response_latency_band"].dropna().astype(str).unique()).issubset(
                valid_response_latency_bands
            ),
            distinct_response_latency_bands=sorted(
                df["response_latency_band"].dropna().astype(str).unique().tolist()
            ),
        ),
        _result(
            "gold",
            "closure_outcome_group_valid",
            set(df["closure_outcome_group"].dropna().astype(str).unique()).issubset(
                valid_closure_outcome_groups
            ),
            distinct_closure_outcome_groups=sorted(
                df["closure_outcome_group"].dropna().astype(str).unique().tolist()
            ),
        ),
        _result(
            "gold",
            "price_objection_intensity_valid",
            set(df["price_objection_intensity"].dropna().astype(str).unique()).issubset(
                valid_price_objection_intensities
            ),
            distinct_price_objection_intensities=sorted(
                df["price_objection_intensity"].dropna().astype(str).unique().tolist()
            ),
        ),
        _result(
            "gold",
            "commercial_urgency_signal_valid",
            set(df["commercial_urgency_signal"].dropna().astype(str).unique()).issubset(
                valid_commercial_urgency_signals
            ),
            distinct_commercial_urgency_signals=sorted(
                df["commercial_urgency_signal"].dropna().astype(str).unique().tolist()
            ),
        ),
        _result(
            "gold",
            "competitor_pressure_level_valid",
            set(df["competitor_pressure_level"].dropna().astype(str).unique()).issubset(
                valid_competitor_pressure_levels
            ),
            distinct_competitor_pressure_levels=sorted(
                df["competitor_pressure_level"].dropna().astype(str).unique().tolist()
            ),
        ),
        _result(
            "gold",
            "conversation_sentiment_label_valid",
            set(df["conversation_sentiment_label"].dropna().astype(str).unique()).issubset(
                valid_conversation_sentiment_labels
            ),
            distinct_conversation_sentiment_labels=sorted(
                df["conversation_sentiment_label"].dropna().astype(str).unique().tolist()
            ),
        ),
        _result(
            "gold",
            "conversation_sentiment_support_valid",
            set(df["conversation_sentiment_support"].dropna().astype(str).unique()).issubset(
                valid_conversation_sentiment_supports
            ),
            distinct_conversation_sentiment_supports=sorted(
                df["conversation_sentiment_support"].dropna().astype(str).unique().tolist()
            ),
        ),
        _result(
            "gold",
            "persona_profile_not_null",
            df["persona_profile"].notna().all(),
            null_rows=int(df["persona_profile"].isna().sum()),
        ),
        _result(
            "gold",
            "semantic_source_family_valid",
            all(
                set(df[column].dropna().astype(str).unique()).issubset(
                    valid_semantic_source_families
                )
                for column in interpretive_provenance_columns.values()
            ),
            distinct_source_families=sorted(
                {
                    value
                    for column in interpretive_provenance_columns.values()
                    for value in df[column].dropna().astype(str).unique().tolist()
                }
            ),
        ),
        _result(
            "gold",
            "closure_outcome_flag_coherent",
            df["has_closed_outcome"]
            .astype(bool)
            .eq(df["closure_outcome_group"].eq("fechado"))
            .all(),
            violating_rows=int(
                (
                    df["has_closed_outcome"].astype(bool)
                    != df["closure_outcome_group"].eq("fechado")
                ).sum()
            ),
        ),
        _result(
            "gold",
            "dominant_email_provider_nullability_coherent",
            df["dominant_email_provider"].notna().eq(df["contains_email"].astype(bool)).all(),
            violating_rows=int(
                (df["dominant_email_provider"].notna() != df["contains_email"].astype(bool)).sum()
            ),
        ),
        _result(
            "gold",
            "persona_audience_semantic_alignment",
            (
                df["persona_profile"].astype("string").map(PERSONA_AUDIENCE_COMPATIBILITY)
                == df["audience_segment"].astype("string")
            ).all(),
            violating_rows=int(
                (
                    df["persona_profile"].astype("string").map(PERSONA_AUDIENCE_COMPATIBILITY)
                    != df["audience_segment"].astype("string")
                ).sum()
            ),
        ),
        _result(
            "gold",
            "conversation_sentiment_support_alignment",
            (
                (
                    df["conversation_sentiment_label"].eq("sem_evidencia")
                    & df["conversation_sentiment_support"].eq("sem_evidencia")
                )
                | (
                    ~df["conversation_sentiment_label"].eq("sem_evidencia")
                    & ~df["conversation_sentiment_support"].eq("sem_evidencia")
                )
            ).all(),
            violating_rows=int(
                (
                    ~(
                        (
                            df["conversation_sentiment_label"].eq("sem_evidencia")
                            & df["conversation_sentiment_support"].eq("sem_evidencia")
                        )
                        | (
                            ~df["conversation_sentiment_label"].eq("sem_evidencia")
                            & ~df["conversation_sentiment_support"].eq("sem_evidencia")
                        )
                    )
                ).sum()
            ),
        ),
        _result(
            "gold",
            "conversation_sentiment_evidence_compatibility",
            (
                ~(
                    df["conversation_sentiment_label"].eq("positivo")
                    & df["negative_tone_hits"].ge(2)
                    & df["positive_tone_hits"].eq(0)
                )
                & ~(
                    df["conversation_sentiment_label"].eq("negativo")
                    & df["positive_tone_hits"].ge(2)
                    & df["negative_tone_hits"].eq(0)
                )
                & ~(
                    df["conversation_sentiment_label"].eq("sem_evidencia")
                    & (df["positive_tone_hits"].gt(0) | df["negative_tone_hits"].gt(0))
                )
            ).all(),
            violating_rows=int(
                (
                    (
                        df["conversation_sentiment_label"].eq("positivo")
                        & df["negative_tone_hits"].ge(2)
                        & df["positive_tone_hits"].eq(0)
                    )
                    | (
                        df["conversation_sentiment_label"].eq("negativo")
                        & df["positive_tone_hits"].ge(2)
                        & df["negative_tone_hits"].eq(0)
                    )
                    | (
                        df["conversation_sentiment_label"].eq("sem_evidencia")
                        & (df["positive_tone_hits"].gt(0) | df["negative_tone_hits"].gt(0))
                    )
                ).sum()
            ),
        ),
    ]
    return results


def summarize_validation_results(results: list[ValidationResult]) -> dict[str, Any]:
    failures = [result.as_dict() for result in results if result.status == "failed"]
    return {
        "status": "passed" if not failures else "failed",
        "checks": [result.as_dict() for result in results],
        "failed_checks": failures,
    }
