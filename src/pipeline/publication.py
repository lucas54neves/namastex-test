from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

SILVER_FORBIDDEN_COLUMNS = frozenset(
    {
        "sender_name",
        "sender_phone",
        "message_body",
        "conversation_lead_name",
        "conversation_agent_name",
        "sender_name_normalized",
    }
)
GOLD_FORBIDDEN_COLUMNS = frozenset(
    {
        "sender_name",
        "sender_phone",
        "message_body",
        "conversation_lead_name",
        "conversation_agent_name",
        "sender_name_normalized",
    }
)
SILVER_REQUIRED_SAFE_COLUMNS = frozenset(
    {
        "sender_name_masked",
        "sender_phone_masked",
        "message_body_masked",
    }
)
PUBLISH_SAFE_SILVER_KEY_MAP = {
    "sender_phone": "sender_phone_masked",
    "message_body": "message_body_masked",
}


def forbidden_columns_for_layer(layer: str) -> frozenset[str]:
    if layer == "silver":
        return SILVER_FORBIDDEN_COLUMNS
    if layer == "gold":
        return GOLD_FORBIDDEN_COLUMNS
    raise ValueError(f"Unsupported publication layer: {layer}")


def required_safe_columns_for_layer(layer: str) -> frozenset[str]:
    if layer == "silver":
        return SILVER_REQUIRED_SAFE_COLUMNS
    if layer == "gold":
        return frozenset()
    raise ValueError(f"Unsupported publication layer: {layer}")


def sanitize_for_publication(df: pd.DataFrame, layer: str) -> pd.DataFrame:
    forbidden = forbidden_columns_for_layer(layer)
    to_drop = [column for column in df.columns if column in forbidden]
    if not to_drop:
        return df.copy()
    return df.drop(columns=to_drop).copy()


def forbidden_columns_present(df: pd.DataFrame, layer: str) -> list[str]:
    forbidden = forbidden_columns_for_layer(layer)
    return sorted(column for column in df.columns if column in forbidden)


def missing_required_safe_columns(df: pd.DataFrame, layer: str) -> list[str]:
    required_columns = required_safe_columns_for_layer(layer)
    return sorted(required_columns - set(df.columns))


def resolve_publish_safe_keys(keys: Iterable[str], available_columns: Iterable[str]) -> list[str]:
    available = set(available_columns)
    resolved: list[str] = []
    for key in keys:
        candidate = PUBLISH_SAFE_SILVER_KEY_MAP.get(key, key)
        if candidate in available:
            resolved.append(candidate)
    return resolved
