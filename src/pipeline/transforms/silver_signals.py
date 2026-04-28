from __future__ import annotations

import re

import pandas as pd

from pipeline.transforms.silver_masking import _mask_digits, mask_message_body, mask_sender_name
from pipeline.transforms.silver_patterns import (
    CEP_PATTERN,
    COMPETITOR_COMPARISON_PATTERN,
    COMPETITOR_PATTERNS,
    CPF_PATTERN,
    EMAIL_PATTERN,
    EMAIL_PROVIDER_DOMAIN_PATTERN,
    PHONE_PATTERN,
    PLATE_PATTERN,
    PRICE_OBJECTION_PATTERN,
    PRICE_PATTERN,
    SINISTRO_PATTERNS,
    URGENCY_MODERATE_PATTERN,
    URGENCY_STRONG_PATTERN,
    VEHICLE_MAKES,
    VEHICLE_MODELS,
    YEAR_PATTERN,
    _normalize_for_match,
    _safe_int,
    _safe_string,
)


def _extract_first(pattern: re.Pattern[str], text: str) -> str | None:
    match = pattern.search(text)
    return match.group(0) if match else None


def _extract_competitor(text: str) -> str | None:
    for competitor, pattern in COMPETITOR_PATTERNS.items():
        if pattern.search(text):
            return competitor
    return None


def _extract_sinistro_type(text: str, direction: str) -> str | None:
    normalized = text.lower()
    if direction != "inbound":
        return None
    if "cobertura" in normalized and "tive" not in normalized and "bati" not in normalized:
        return None
    for label, pattern in SINISTRO_PATTERNS.items():
        if pattern.search(normalized):
            return label
    return None


def _extract_vehicle_make(text: str) -> str | None:
    normalized = _normalize_for_match(text)
    for make in VEHICLE_MAKES:
        if re.search(rf"\b{re.escape(make)}\b", normalized):
            return make
    return None


def _extract_vehicle_model(text: str) -> str | None:
    normalized = _normalize_for_match(text).replace("t cross", "t-cross")
    for model in VEHICLE_MODELS:
        normalized_model = model.replace(" ", "-")
        if normalized_model in normalized:
            return model
    return None


def _extract_vehicle_year(text: str) -> str | None:
    raw_text = _safe_string(text)
    normalized = _normalize_for_match(raw_text)
    contextual_patterns = [
        re.compile(r"\bano\s+(19\d{2}|20\d{2})\b", re.IGNORECASE),
        re.compile(r"\b(19\d{2}|20\d{2})/(19\d{2}|20\d{2})\b", re.IGNORECASE),
    ]
    for pattern in contextual_patterns:
        match = pattern.search(normalized)
        if match:
            return match.group(1)
    if (
        _extract_vehicle_make(raw_text)
        or _extract_vehicle_model(raw_text)
        or PLATE_PATTERN.search(raw_text)
    ):
        match = YEAR_PATTERN.search(normalized)
        if match:
            return match.group(1)
    return None


def _extract_price(text: str) -> float | None:
    match = PRICE_PATTERN.search(text)
    if not match:
        return None
    raw_value = match.group(1).replace(".", "").replace(",", ".")
    try:
        return float(raw_value)
    except ValueError:
        return None


def _extract_email_provider(text: str) -> str | None:
    match = EMAIL_PROVIDER_DOMAIN_PATTERN.search(text)
    if not match:
        return None
    domain = match.group(1).lower()
    if domain.endswith("gmail.com"):
        return "gmail"
    if domain.endswith(("hotmail.com", "outlook.com", "live.com", "msn.com")):
        return "outlook"
    if domain.endswith(("yahoo.com", "yahoo.com.br")):
        return "yahoo"
    if domain.endswith(("icloud.com", "me.com")):
        return "icloud"
    if domain.endswith("uol.com.br"):
        return "uol"
    if domain.endswith("bol.com.br"):
        return "bol"
    if domain.endswith("terra.com.br"):
        return "terra"
    return "other"


def _extract_price_objection_signal(text: str) -> bool:
    return bool(PRICE_OBJECTION_PATTERN.search(text))


def _extract_urgency_strength(text: str) -> int:
    if URGENCY_STRONG_PATTERN.search(text):
        return 2
    if URGENCY_MODERATE_PATTERN.search(text):
        return 1
    return 0


def _extract_competitor_comparison_signal(text: str) -> bool:
    return bool(COMPETITOR_COMPARISON_PATTERN.search(text))


def _count_tone_hits(text: object, patterns: tuple[re.Pattern[str], ...]) -> int:
    normalized = _normalize_for_match(_safe_string(text))
    if not normalized:
        return 0
    return sum(1 for pattern in patterns if pattern.search(normalized))


def derive_conversation_sentiment_label(
    positive_tone_hits: object, negative_tone_hits: object
) -> str:
    positive_hits = _safe_int(positive_tone_hits)
    negative_hits = _safe_int(negative_tone_hits)
    total_hits = positive_hits + negative_hits
    balance = positive_hits - negative_hits

    if total_hits == 0:
        return "sem_evidencia"
    if balance >= 2 or (positive_hits >= 2 and negative_hits == 0):
        return "positivo"
    if balance <= -2 or (negative_hits >= 2 and positive_hits == 0):
        return "negativo"
    return "neutro"


def derive_conversation_sentiment_support(
    positive_tone_hits: object, negative_tone_hits: object
) -> str:
    positive_hits = _safe_int(positive_tone_hits)
    negative_hits = _safe_int(negative_tone_hits)
    total_hits = positive_hits + negative_hits
    balance_magnitude = abs(positive_hits - negative_hits)

    if total_hits == 0:
        return "sem_evidencia"
    if max(positive_hits, negative_hits) >= 2 or balance_magnitude >= 3:
        return "forte"
    if total_hits >= 2 or balance_magnitude >= 1:
        return "moderado"
    return "fraco"


def _directional_bool_signal(signal: pd.Series, direction: pd.Series, expected: str) -> pd.Series:
    return signal.fillna(False).astype(bool) & direction.eq(expected)


def _directional_int_signal(signal: pd.Series, direction: pd.Series, expected: str) -> pd.Series:
    values = pd.to_numeric(signal, errors="coerce").fillna(0).astype(int)
    return values.where(direction.eq(expected), 0)


def _directional_value_signal(signal: pd.Series, direction: pd.Series, expected: str) -> pd.Series:
    return signal.where(direction.eq(expected))


def add_message_signals(df: pd.DataFrame) -> pd.DataFrame:
    message_body = df["message_body"].fillna("")
    normalized_name = df["sender_name"].fillna("").map(_normalize_for_match)
    direction = df["direction"].fillna("")

    enriched = df.copy()
    enriched["sender_name_normalized"] = normalized_name
    enriched["sender_name_masked"] = enriched["sender_name"].map(mask_sender_name)
    enriched["sender_phone_masked"] = enriched["sender_phone"].map(_mask_digits)
    enriched["message_body_masked"] = enriched.apply(mask_message_body, axis=1)
    enriched["message_length"] = message_body.str.len()
    enriched["word_count"] = message_body.str.split().str.len()
    enriched["is_empty_message"] = message_body.str.strip().eq("")
    enriched["contains_email"] = message_body.str.contains(EMAIL_PATTERN, na=False)
    enriched["contains_phone"] = message_body.str.contains(PHONE_PATTERN, na=False)
    enriched["contains_cpf"] = message_body.str.contains(CPF_PATTERN, na=False)
    enriched["contains_cep"] = message_body.str.contains(CEP_PATTERN, na=False)
    enriched["contains_plate"] = message_body.str.contains(PLATE_PATTERN, na=False)
    enriched["vehicle_year"] = message_body.map(_extract_vehicle_year)
    enriched["vehicle_make"] = message_body.map(_extract_vehicle_make)
    enriched["vehicle_model"] = message_body.map(_extract_vehicle_model)
    enriched["competitor_mentioned"] = message_body.map(_extract_competitor)
    enriched["email_provider"] = message_body.map(_extract_email_provider)
    enriched["quoted_price"] = message_body.map(_extract_price)
    enriched["price_objection_signal"] = message_body.map(_extract_price_objection_signal)
    enriched["urgency_strength"] = message_body.map(_extract_urgency_strength)
    enriched["competitor_comparison_signal"] = message_body.map(
        _extract_competitor_comparison_signal
    )
    enriched["sinistro_type"] = [
        _extract_sinistro_type(text, direction)
        for text, direction in zip(message_body, enriched["direction"], strict=False)
    ]
    enriched["mentions_vehicle"] = (
        enriched["vehicle_make"].notna()
        | enriched["vehicle_model"].notna()
        | enriched["vehicle_year"].notna()
        | enriched["contains_plate"]
    )
    enriched["mentions_competitor"] = enriched["competitor_mentioned"].notna()
    enriched["mentions_sinistro"] = enriched["sinistro_type"].notna()
    enriched["price_objection_signal_inbound"] = _directional_bool_signal(
        enriched["price_objection_signal"],
        direction,
        "inbound",
    )
    enriched["price_objection_signal_outbound"] = _directional_bool_signal(
        enriched["price_objection_signal"],
        direction,
        "outbound",
    )
    enriched["urgency_strength_inbound"] = _directional_int_signal(
        enriched["urgency_strength"],
        direction,
        "inbound",
    )
    enriched["urgency_strength_outbound"] = _directional_int_signal(
        enriched["urgency_strength"],
        direction,
        "outbound",
    )
    enriched["competitor_comparison_signal_inbound"] = _directional_bool_signal(
        enriched["competitor_comparison_signal"],
        direction,
        "inbound",
    )
    enriched["competitor_comparison_signal_outbound"] = _directional_bool_signal(
        enriched["competitor_comparison_signal"],
        direction,
        "outbound",
    )
    enriched["competitor_mentioned_inbound"] = _directional_value_signal(
        enriched["competitor_mentioned"],
        direction,
        "inbound",
    )
    enriched["competitor_mentioned_outbound"] = _directional_value_signal(
        enriched["competitor_mentioned"],
        direction,
        "outbound",
    )
    enriched["mentions_competitor_inbound"] = enriched["competitor_mentioned_inbound"].notna()
    enriched["mentions_competitor_outbound"] = enriched["competitor_mentioned_outbound"].notna()
    return enriched
