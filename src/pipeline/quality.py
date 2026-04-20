from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


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


def validate_bronze(df: pd.DataFrame) -> list[ValidationResult]:
    results = [
        _column_set_check(
            df,
            [
                "message_id",
                "conversation_id",
                "timestamp",
                "direction",
                "sender_phone",
                "sender_name",
                "message_type",
                "message_body",
                "status",
                "channel",
                "campaign_id",
                "agent_id",
                "conversation_outcome",
                "metadata",
            ],
            "bronze",
        ),
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
            df["channel"].eq("whatsapp").all(),
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


def validate_silver(df: pd.DataFrame) -> list[ValidationResult]:
    dedupe_keys = [
        "conversation_id",
        "timestamp",
        "direction",
        "sender_phone",
        "message_type",
        "message_body",
    ]
    duplicate_rows = int(df.duplicated(subset=dedupe_keys).sum())
    pii_leak_count = int(
        (
            df["contains_cpf"]
            & df["message_body_masked"]
            .fillna("")
            .str.contains(r"\d{3}\.\d{3}\.\d{3}-\d{2}", regex=True)
        ).sum()
    )
    results = [
        _column_set_check(
            df,
            [
                "message_body_masked",
                "sender_name_masked",
                "sender_phone_masked",
                "dropped_duplicate_events",
                "mentions_vehicle",
                "mentions_competitor",
                "mentions_sinistro",
            ],
            "silver",
        ),
        _result(
            "silver",
            "timestamp_not_null",
            df["timestamp"].notna().all(),
            null_timestamps=int(df["timestamp"].isna().sum()),
        ),
        _result(
            "silver",
            "dedupe_keys_unique",
            duplicate_rows == 0,
            duplicate_rows=duplicate_rows,
        ),
        _result(
            "silver",
            "masked_cpf_not_leaking",
            pii_leak_count == 0,
            leaking_rows=pii_leak_count,
        ),
        _result(
            "silver",
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


def validate_gold(df: pd.DataFrame) -> list[ValidationResult]:
    valid_buckets = {"lead_frio", "curta", "media", "longa"}
    results = [
        _column_set_check(
            df,
            [
                "conversation_id",
                "total_messages",
                "duplicate_events_removed",
                "engagement_bucket",
                "data_shared_score",
            ],
            "gold",
        ),
        _result(
            "gold",
            "conversation_id_unique",
            df["conversation_id"].nunique(dropna=False) == len(df),
            unique_ids=int(df["conversation_id"].nunique(dropna=False)),
            rows=int(len(df)),
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
    ]
    return results


def summarize_validation_results(results: list[ValidationResult]) -> dict[str, Any]:
    failures = [result.as_dict() for result in results if result.status == "failed"]
    return {
        "status": "passed" if not failures else "failed",
        "checks": [result.as_dict() for result in results],
        "failed_checks": failures,
    }
