from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

__all__ = [
    "BronzeValidationReport",
    "BronzeResult",
    "BRONZE_CATEGORICAL_COLUMNS",
    "load_bronze_frame",
    "parse_metadata",
    "cast_bronze_dtypes",
    "validate_bronze_schema",
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


@dataclass
class BronzeResult:
    df: pd.DataFrame
    validation_report: BronzeValidationReport
    metadata_parsed_ok: bool


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


def build_bronze(
    source_path: str | Path,
    compiled_plan: dict | None = None,
) -> BronzeResult:
    df = load_bronze_frame(str(source_path))
    df = parse_metadata(df)
    df = cast_bronze_dtypes(df)
    report = validate_bronze_schema(df)
    return BronzeResult(df=df, validation_report=report, metadata_parsed_ok=True)
