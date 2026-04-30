from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.quality.schema_drift import (
    DriftEvent,
    classify_bronze_columns,
    classify_category_drift,
    classify_metadata_keys,
)

__all__ = [
    "BronzeValidationReport",
    "BronzeResult",
    "BRONZE_CATEGORICAL_COLUMNS",
    "load_bronze_frame",
    "parse_metadata",
    "cast_bronze_dtypes",
    "validate_bronze_schema",
    "detect_bronze_schema_drift",
    "build_bronze",
]

BRONZE_CATEGORICAL_COLUMNS: dict[str, list[str]] = {
    "direction": ["outbound", "inbound"],
    "message_type": ["text", "image", "audio", "document", "sticker"],
    "status": ["sent", "delivered", "read", "failed"],
    "channel": ["whatsapp"],
    "conversation_outcome": [
        "venda_fechada",
        "perdido_preco",
        "perdido_concorrente",
        "ghosting",
        "desistencia_lead",
        "proposta_enviada",
        "em_negociacao",
    ],
}

_METADATA_STRING_COLS = [
    "metadata_device",
    "metadata_city",
    "metadata_state",
    "metadata_lead_source",
]
_REQUIRED_SOURCE_COLS = [
    "message_id",
    "conversation_id",
    "timestamp",
    "sender_phone",
    "sender_name",
    "message_body",
    "campaign_id",
    "agent_id",
    "direction",
]


@dataclass
class BronzeValidationReport:
    schema_ok: bool
    missing_columns: list[str]
    row_count: int
    unknown_columns: list[str] = field(default_factory=list)
    optional_known_columns_present: list[str] = field(default_factory=list)
    unknown_metadata_keys: list[str] = field(default_factory=list)
    type_mismatch_columns: list[dict[str, Any]] = field(default_factory=list)
    category_drift_columns: list[dict[str, Any]] = field(default_factory=list)
    applied_policies: dict[str, str] = field(default_factory=dict)
    contract_version: int = 0
    drift_events: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class BronzeResult:
    df: pd.DataFrame
    validation_report: BronzeValidationReport
    metadata_parsed_ok: bool
    drift_events: list[DriftEvent] = field(default_factory=list)


def load_bronze_frame(source_path: str) -> pd.DataFrame:
    df = pd.read_parquet(source_path).copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df


def parse_metadata(df: pd.DataFrame) -> pd.DataFrame:
    records: list[dict] = []
    invalid_positions: list[int] = []
    for i, raw in enumerate(df["metadata"]):
        if isinstance(raw, str):
            try:
                records.append(json.loads(raw))
            except (json.JSONDecodeError, ValueError):
                records.append({})
                invalid_positions.append(i)
        else:
            records.append({})
            invalid_positions.append(i)

    meta = pd.json_normalize(records)
    meta.columns = pd.Index([f"metadata_{c}" for c in meta.columns])
    meta.index = df.index

    result = pd.concat([df.drop(columns=["metadata"]), meta], axis=1)

    for col in _METADATA_STRING_COLS:
        if col not in result.columns:
            result[col] = ""
        else:
            result[col] = result[col].fillna("").astype(object)

    if invalid_positions:
        bad_labels = df.index[invalid_positions]
        result.loc[bad_labels, _METADATA_STRING_COLS] = None

    if "metadata_response_time_sec" not in result.columns:
        result["metadata_response_time_sec"] = pd.array([pd.NA] * len(result), dtype="Int64")
    else:
        result["metadata_response_time_sec"] = pd.to_numeric(
            result["metadata_response_time_sec"], errors="coerce"
        ).astype("Int64")

    if "metadata_is_business_hours" not in result.columns:
        result["metadata_is_business_hours"] = pd.array([pd.NA] * len(result), dtype="boolean")
    else:
        result["metadata_is_business_hours"] = result["metadata_is_business_hours"].astype(
            "boolean"
        )

    return result


def cast_bronze_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col, categories in BRONZE_CATEGORICAL_COLUMNS.items():
        if col in out.columns:
            out[col] = out[col].astype(pd.CategoricalDtype(categories=categories, ordered=False))
    return out


def validate_bronze_schema(df: pd.DataFrame) -> BronzeValidationReport:
    missing = [c for c in _REQUIRED_SOURCE_COLS if c not in df.columns]
    return BronzeValidationReport(
        schema_ok=len(missing) == 0,
        missing_columns=missing,
        row_count=len(df),
    )


def _observe_metadata_keys(raw_frame: pd.DataFrame) -> list[str]:
    if "metadata" not in raw_frame.columns:
        return []
    keys: set[str] = set()
    for raw in raw_frame["metadata"]:
        if not isinstance(raw, str):
            continue
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            continue
        if isinstance(payload, dict):
            keys.update(str(k) for k in payload.keys())
    return sorted(keys)


def detect_bronze_schema_drift(
    raw_frame: pd.DataFrame,
    contract: Mapping[str, Any] | None,
) -> list[DriftEvent]:
    """Detect every drift event for a raw Bronze frame.

    Operates on the raw frame (before ``parse_metadata``) so the original
    ``metadata`` column and any unknown top-level columns are still observable.
    """

    if not contract:
        return []
    events: list[DriftEvent] = []
    events.extend(classify_bronze_columns(list(raw_frame.columns), contract))

    metadata_keys = _observe_metadata_keys(raw_frame)
    if metadata_keys:
        events.extend(classify_metadata_keys(metadata_keys, contract))

    bronze_section = contract.get("bronze") or {}
    domains = (
        bronze_section.get("category_domains") if isinstance(bronze_section, Mapping) else None
    )
    if isinstance(domains, Mapping):
        for column in domains.keys():
            if column not in raw_frame.columns:
                continue
            observed_values = raw_frame[column].dropna().astype(str).unique().tolist()
            event = classify_category_drift(column, observed_values, contract)
            if event is not None:
                events.append(event)
    return events


def _build_validation_report_with_drift(
    df: pd.DataFrame,
    drift_events: list[DriftEvent],
    contract: Mapping[str, Any] | None,
) -> BronzeValidationReport:
    base = validate_bronze_schema(df)
    unknown_columns: list[str] = []
    optional_present: list[str] = []
    unknown_metadata: list[str] = []
    type_mismatch: list[dict[str, Any]] = []
    category_drift: list[dict[str, Any]] = []
    applied_policies: dict[str, str] = {}

    for event in drift_events:
        applied_policies[f"{event.scope}:{event.column}"] = event.applied_policy
        if event.drift_class == "unknown" and event.scope == "top_level":
            unknown_columns.append(event.column)
        elif event.drift_class == "optional_known":
            optional_present.append(event.column)
        elif event.drift_class == "unknown" and event.scope == "metadata_key":
            unknown_metadata.append(event.column)
        elif event.drift_class == "type_mismatch":
            type_mismatch.append({"column": event.column, **event.detail})
        elif event.drift_class == "category_drift":
            category_drift.append(
                {
                    "column": event.column,
                    "unexpected_values_masked": event.detail.get("unexpected_values_masked", []),
                }
            )

    has_block = any(event.applied_policy == "block" for event in drift_events)
    schema_ok = base.schema_ok and not has_block

    contract_version = 0
    if isinstance(contract, Mapping):
        try:
            contract_version = int(contract.get("schema_contract_version", 0) or 0)
        except (TypeError, ValueError):
            contract_version = 0

    return BronzeValidationReport(
        schema_ok=schema_ok,
        missing_columns=base.missing_columns,
        row_count=base.row_count,
        unknown_columns=unknown_columns,
        optional_known_columns_present=optional_present,
        unknown_metadata_keys=unknown_metadata,
        type_mismatch_columns=type_mismatch,
        category_drift_columns=category_drift,
        applied_policies=applied_policies,
        contract_version=contract_version,
        drift_events=[event.as_dict() for event in drift_events],
    )


def build_bronze(
    source_path: str | Path,
    compiled_plan: dict | None = None,
    *,
    contract: Mapping[str, Any] | None = None,
) -> BronzeResult:
    raw = load_bronze_frame(str(source_path))
    drift_events = detect_bronze_schema_drift(raw, contract)
    df = parse_metadata(raw)
    df = cast_bronze_dtypes(df)
    if contract is not None:
        report = _build_validation_report_with_drift(df, drift_events, contract)
    else:
        report = validate_bronze_schema(df)
    return BronzeResult(
        df=df,
        validation_report=report,
        metadata_parsed_ok=True,
        drift_events=drift_events,
    )
