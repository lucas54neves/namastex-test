from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import pandas as pd
from pandas.api.types import is_object_dtype, is_string_dtype

from pipeline.compiler import get_default_compiled_plan
from pipeline.publication import (
    forbidden_columns_present,
    missing_required_safe_columns,
    resolve_publish_safe_keys,
)
from pipeline.transforms import detect_unmasked_sensitive_classes

SILVER_TEXT_COLUMNS = ("canonical_lead_name_masked", "lead_contact_ref")
SILVER_MESSAGES_TEXT_COLUMNS = ("message_body_masked", "sender_name_masked", "sender_phone_masked")
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
        _result(
            "bronze",
            "first_message_outbound_ratio",
            True,
            outbound_ratio=float(
                df.sort_values(["conversation_id", "timestamp", "message_id"])
                .groupby("conversation_id", dropna=False)["direction"]
                .first()
                .eq("outbound")
                .mean()
            ),
        ),
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
            "persona_profile_not_null",
            df["persona_profile"].notna().all(),
            null_rows=int(df["persona_profile"].isna().sum()),
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
