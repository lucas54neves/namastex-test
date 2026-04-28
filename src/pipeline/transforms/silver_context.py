from __future__ import annotations

import hashlib

import pandas as pd

from pipeline.transforms.silver_masking import _mask_digits
from pipeline.transforms.silver_patterns import (
    _normalize_for_match,
    _safe_string,
)


def _stable_hash_token(*parts: object, prefix: str) -> str:
    normalized_parts = [_normalize_for_match(_safe_string(part)) for part in parts]
    joined = "|".join(part for part in normalized_parts if part)
    if not joined:
        joined = "unknown"
    digest = hashlib.sha256(joined.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def add_conversation_context(df: pd.DataFrame) -> pd.DataFrame:
    enriched = df.copy()
    lead_name = (
        enriched["sender_name"]
        .where(enriched["direction"].eq("inbound"))
        .groupby(enriched["conversation_id"])
        .transform("first")
        .fillna("")
    )
    agent_name = (
        enriched["sender_name"]
        .where(enriched["direction"].eq("outbound"))
        .groupby(enriched["conversation_id"])
        .transform("first")
        .fillna("")
    )
    sender_phone = (
        enriched["sender_phone"]
        if "sender_phone" in enriched.columns
        else pd.Series("", index=enriched.index, dtype="object")
    )
    lead_phone = (
        sender_phone.where(enriched["direction"].eq("inbound"))
        .groupby(enriched["conversation_id"])
        .transform("first")
        .fillna("")
    )
    enriched["conversation_lead_name"] = lead_name
    enriched["conversation_agent_name"] = agent_name
    enriched["conversation_lead_phone"] = lead_phone
    return enriched


def add_lead_context(df: pd.DataFrame) -> pd.DataFrame:
    enriched = df.copy()
    enriched["lead_phone_raw"] = enriched["conversation_lead_phone"].map(_safe_string)
    enriched["lead_phone_masked"] = enriched["lead_phone_raw"].map(_mask_digits)
    enriched["lead_name_raw"] = enriched["conversation_lead_name"].map(_safe_string)
    enriched["lead_key"] = [
        _stable_hash_token(
            phone if phone else name,
            city,
            state,
            prefix="lead",
        )
        for phone, name, city, state in zip(
            enriched["lead_phone_raw"],
            enriched["lead_name_raw"],
            enriched.get("metadata_city", pd.Series("", index=enriched.index)),
            enriched.get("metadata_state", pd.Series("", index=enriched.index)),
            strict=False,
        )
    ]
    return enriched
