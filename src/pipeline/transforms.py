from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections.abc import Iterable
from typing import Any, cast

import pandas as pd

from pipeline.compiler import get_default_compiled_plan

EMAIL_PATTERN = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PHONE_PATTERN = re.compile(r"(?:\+55\s?)?(?:\(?\d{2}\)?\s?)?(?:9?\d{4})-?\d{4}")
CPF_PATTERN = re.compile(r"\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b")
CEP_PATTERN = re.compile(r"\b\d{5}-?\d{3}\b")
PLATE_PATTERN = re.compile(r"\b[A-Z]{3}[0-9][A-Z0-9][0-9]{2}\b", re.IGNORECASE)
YEAR_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\b")
PRICE_PATTERN = re.compile(r"r\$\s?(\d[\d.]*(?:,\d{2})?)", re.IGNORECASE)
COMPETITOR_PATTERNS = {
    "porto_seguro": re.compile(r"\bporto seguro\b", re.IGNORECASE),
    "azul_seguros": re.compile(r"\bazul(?: seguros)?\b", re.IGNORECASE),
    "bradesco_seguros": re.compile(r"\bbradesco seguros\b", re.IGNORECASE),
    "sulamerica": re.compile(r"\bsul\s?america\b", re.IGNORECASE),
    "liberty_seguros": re.compile(r"\bliberty(?: seguros)?\b", re.IGNORECASE),
    "allianz": re.compile(r"\ballianz\b", re.IGNORECASE),
    "hdi_seguros": re.compile(r"\bhdi(?: seguros)?\b", re.IGNORECASE),
}
SINISTRO_PATTERNS = {
    "enchente": re.compile(r"\benchente|alagamento\b", re.IGNORECASE),
    "colisao": re.compile(r"\bbati\b|\bbatida\b|\bcolis[aã]o\b", re.IGNORECASE),
    "roubo_furto": re.compile(r"\broubaram\b|\bfurto\b|\broubo\b", re.IGNORECASE),
    "perda_total": re.compile(r"\bperda total\b", re.IGNORECASE),
    "sinistro_generico": re.compile(r"\bsinistro\b", re.IGNORECASE),
}
VEHICLE_MAKES = (
    "chevrolet",
    "volkswagen",
    "fiat",
    "ford",
    "toyota",
    "honda",
    "hyundai",
    "jeep",
    "renault",
    "nissan",
    "peugeot",
    "citroen",
    "mitsubishi",
    "kia",
    "bmw",
    "mercedes",
    "audi",
    "volvo",
    "byd",
    "gwm",
)
VEHICLE_MODELS = (
    "onix",
    "gol",
    "civic",
    "hb20",
    "corolla",
    "compass",
    "hr-v",
    "hrv",
    "kwid",
    "208",
    "toro",
    "argo",
    "mobi",
    "creta",
    "t cross",
    "t-cross",
    "nivus",
    "tracker",
    "pulse",
    "kicks",
)
STATUS_PRIORITY = {"failed": 0, "sent": 1, "delivered": 2, "read": 3}
SENSITIVE_PATTERNS: dict[str, re.Pattern[str]] = {
    "email": EMAIL_PATTERN,
    "phone": PHONE_PATTERN,
    "cpf": CPF_PATTERN,
    "cep": CEP_PATTERN,
    "plate": PLATE_PATTERN,
}


def _normalize_ascii(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    return normalized.encode("ascii", errors="ignore").decode("ascii")


def _normalize_for_match(value: str) -> str:
    normalized = _normalize_ascii(value).lower()
    normalized = re.sub(r"[^a-z0-9 ]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _mask_digits(raw: str) -> str:
    return "".join("X" if char.isdigit() else char for char in raw)


def _mask_email(raw: str) -> str:
    return re.sub(r"[A-Za-z0-9]", "x", raw)


def _mask_alpha_numeric(raw: str) -> str:
    masked = []
    for char in raw:
        if char.isalpha():
            masked.append("X")
        elif char.isdigit():
            masked.append("9")
        else:
            masked.append(char)
    return "".join(masked)


def _mask_name_token(raw: str) -> str:
    return "".join("X" if char.isalpha() else char for char in raw)


def _safe_string(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value)


def _stable_hash_token(*parts: object, prefix: str) -> str:
    normalized_parts = [_normalize_for_match(_safe_string(part)) for part in parts]
    joined = "|".join(part for part in normalized_parts if part)
    if not joined:
        joined = "unknown"
    digest = hashlib.sha256(joined.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _first_non_empty(values: pd.Series) -> str:
    for value in values:
        text = _safe_string(value).strip()
        if text:
            return text
    return ""


def _json_sorted_unique(values: pd.Series) -> str:
    collected = sorted(
        {_safe_string(value).strip() for value in values if _safe_string(value).strip()}
    )
    return json.dumps(collected, ensure_ascii=True)


def _first_non_null(values: pd.Series) -> object:
    for value in values:
        if pd.notna(value):
            return value
    return None


def _max_or_false(values: pd.Series) -> bool:
    return bool(values.fillna(False).astype(bool).max())


def detect_sensitive_classes(value: object, classes: Iterable[str] | None = None) -> set[str]:
    text = _safe_string(value)
    requested = set(classes) if classes is not None else set(SENSITIVE_PATTERNS)
    return {
        name
        for name, pattern in SENSITIVE_PATTERNS.items()
        if name in requested and pattern.search(text)
    }


def _is_masked_email(value: str) -> bool:
    return bool(re.fullmatch(r"[xX._%+-]+@[xX.-]+\.[xX]{2,}", value))


def _is_masked_plate(value: str) -> bool:
    normalized = value.upper()
    return bool(normalized) and set(normalized) <= {"X", "9", "-"}


def detect_unmasked_sensitive_classes(
    value: object, classes: Iterable[str] | None = None
) -> set[str]:
    text = _safe_string(value)
    requested = set(classes) if classes is not None else set(SENSITIVE_PATTERNS)
    matches: set[str] = set()
    for name, pattern in SENSITIVE_PATTERNS.items():
        if name not in requested:
            continue
        for match in pattern.finditer(text):
            token = match.group(0)
            if name == "email" and _is_masked_email(token):
                continue
            if name == "plate" and _is_masked_plate(token):
                continue
            matches.add(name)
            break
    return matches


def load_bronze_frame(source_path: str) -> pd.DataFrame:
    df = pd.read_parquet(source_path).copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df


def parse_metadata(df: pd.DataFrame) -> pd.DataFrame:
    metadata = df["metadata"].map(json.loads)
    metadata_df = pd.json_normalize(metadata)
    metadata_df.columns = [f"metadata_{column}" for column in metadata_df.columns]
    return pd.concat([df.drop(columns=["metadata"]), metadata_df], axis=1)


def mask_sender_name(name: object) -> str:
    raw_name = _safe_string(name)
    return "".join("X" if char.isalpha() else char for char in raw_name)


def _mask_known_names(text: str, names: Iterable[str]) -> str:
    masked = text
    for name in names:
        normalized = _normalize_for_match(name)
        if not normalized or len(normalized) < 3:
            continue
        tokens = [token for token in normalized.split(" ") if len(token) >= 2]
        if not tokens:
            continue
        pattern = re.compile(r"\b" + r"\s+".join(map(re.escape, tokens)) + r"\b", re.IGNORECASE)
        masked = pattern.sub(lambda match: _mask_name_token(match.group(0)), masked)
    return masked


def mask_message_body(row: pd.Series) -> str:
    text = _safe_string(row["message_body"])
    masked = text
    masked = EMAIL_PATTERN.sub(lambda match: _mask_email(match.group(0)), masked)
    masked = CPF_PATTERN.sub(lambda match: _mask_digits(match.group(0)), masked)
    masked = CEP_PATTERN.sub(lambda match: _mask_digits(match.group(0)), masked)
    masked = PHONE_PATTERN.sub(lambda match: _mask_digits(match.group(0)), masked)
    masked = PLATE_PATTERN.sub(lambda match: _mask_alpha_numeric(match.group(0).upper()), masked)
    known_names = {
        row.get("sender_name", ""),
        row.get("conversation_lead_name", ""),
        row.get("conversation_agent_name", ""),
    }
    masked = _mask_known_names(masked, known_names)
    return masked


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


def deduplicate_events(
    df: pd.DataFrame, compiled_plan: dict[str, object] | None = None
) -> pd.DataFrame:
    plan = compiled_plan or get_default_compiled_plan()
    dedupe_keys = cast(list[str], plan["dedupe_keys"])
    ranked = df.copy()
    ranked["status_priority"] = ranked["status"].map(STATUS_PRIORITY).fillna(-1).astype(int)
    ranked["duplicate_event_group_size"] = ranked.groupby(dedupe_keys, dropna=False)[
        "message_id"
    ].transform("size")
    ranked["had_status_duplication"] = ranked["duplicate_event_group_size"].gt(1)
    ranked = ranked.sort_values(
        ["conversation_id", "timestamp", "status_priority", "message_id"],
        ascending=[True, True, False, True],
    )
    deduped = ranked.drop_duplicates(subset=dedupe_keys, keep="first").copy()
    deduped["dropped_duplicate_events"] = deduped["duplicate_event_group_size"] - 1
    return deduped.drop(columns=["status_priority"]).reset_index(drop=True)


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


def add_message_signals(df: pd.DataFrame) -> pd.DataFrame:
    message_body = df["message_body"].fillna("")
    normalized_name = df["sender_name"].fillna("").map(_normalize_for_match)

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
    enriched["quoted_price"] = message_body.map(_extract_price)
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
    return enriched


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
    grouped = silver_messages.groupby("lead_key", dropna=False)
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


def add_gold_segments(
    gold: pd.DataFrame, compiled_plan: dict[str, object] | None = None
) -> pd.DataFrame:
    plan = compiled_plan or get_default_compiled_plan()
    segmentation = cast(dict[str, Any], plan["gold_segmentation"])
    temperature_cfg = cast(dict[str, Any], segmentation["lead_temperature"])
    price_cfg = cast(dict[str, Any], segmentation["price_sensitivity"])
    readiness_cfg = cast(dict[str, Any], segmentation["contact_readiness"])
    intent_cfg = cast(dict[str, Any], segmentation["intent_stage"])
    risk_cfg = cast(dict[str, Any], segmentation["risk_signal"])
    persona_cfg = cast(dict[str, Any], segmentation["personas"])
    audience_cfg = cast(dict[str, Any], segmentation["audiences"])

    segmented = gold.copy()
    segmented["lead_temperature"] = str(temperature_cfg["default"])
    segmented.loc[
        segmented["engagement_bucket"].eq(str(temperature_cfg["cold_bucket"]))
        & segmented["data_shared_score"].eq(int(temperature_cfg["cold_data_shared_score"])),
        "lead_temperature",
    ] = str(temperature_cfg["cold_label"])
    segmented.loc[
        segmented["engagement_bucket"].isin(list(temperature_cfg["hot_buckets"]))
        | segmented["data_shared_score"].ge(int(temperature_cfg["hot_min_data_shared_score"])),
        "lead_temperature",
    ] = str(temperature_cfg["hot_label"])

    segmented["price_sensitivity"] = str(price_cfg["default"])
    segmented.loc[
        segmented["mentioned_competitor"] | segmented["avg_quoted_price"].notna(),
        "price_sensitivity",
    ] = str(price_cfg["high_label"])

    segmented["contact_readiness"] = str(readiness_cfg["default"])
    segmented.loc[
        segmented["data_shared_score"].eq(int(readiness_cfg["medium_score"])),
        "contact_readiness",
    ] = str(readiness_cfg["medium_label"])
    segmented.loc[
        segmented["data_shared_score"].ge(int(readiness_cfg["high_min_score"])),
        "contact_readiness",
    ] = str(readiness_cfg["high_label"])

    segmented["intent_stage"] = str(intent_cfg["default"])
    segmented.loc[segmented["mentioned_sinistro"], "intent_stage"] = str(
        intent_cfg["sinistro_label"]
    )
    segmented.loc[
        segmented["mentioned_competitor"] & ~segmented["mentioned_sinistro"],
        "intent_stage",
    ] = str(intent_cfg["competitor_label"])
    segmented.loc[
        segmented["avg_quoted_price"].notna() & ~segmented["mentioned_sinistro"],
        "intent_stage",
    ] = str(intent_cfg["quoted_price_label"])

    segmented["risk_signal"] = str(risk_cfg["default"])
    segmented.loc[
        segmented["mentioned_sinistro"] | segmented["duplicate_events_removed"].gt(0),
        "risk_signal",
    ] = str(risk_cfg["medium_label"])
    segmented.loc[
        segmented["mentioned_sinistro"] & segmented["contains_cpf"],
        "risk_signal",
    ] = str(risk_cfg["high_label"])

    segmented["persona_profile"] = str(persona_cfg["default"])
    segmented.loc[
        segmented["mentioned_sinistro"],
        "persona_profile",
    ] = str(persona_cfg["sinistro"])
    segmented.loc[
        segmented["mentioned_competitor"] & segmented["avg_quoted_price"].notna(),
        "persona_profile",
    ] = str(persona_cfg["comparator"])
    segmented.loc[
        segmented["lead_temperature"].eq(str(temperature_cfg["hot_label"]))
        & segmented["contact_readiness"].eq(str(readiness_cfg["high_label"])),
        "persona_profile",
    ] = str(persona_cfg["engaged"])

    segmented["audience_segment"] = str(audience_cfg["default"])
    segmented.loc[
        segmented["persona_profile"].eq(str(persona_cfg["sinistro"])),
        "audience_segment",
    ] = str(audience_cfg["sinistro"])
    segmented.loc[
        segmented["persona_profile"].eq(str(persona_cfg["comparator"])),
        "audience_segment",
    ] = str(audience_cfg["comparator"])
    segmented.loc[
        segmented["persona_profile"].eq(str(persona_cfg["engaged"])),
        "audience_segment",
    ] = str(audience_cfg["engaged"])

    return segmented


def build_gold(
    silver: pd.DataFrame, compiled_plan: dict[str, object] | None = None
) -> pd.DataFrame:
    grouped = silver.groupby("conversation_id", dropna=False)
    gold = grouped.agg(
        started_at=("timestamp", "min"),
        ended_at=("timestamp", "max"),
        campaign_id=("campaign_id", "first"),
        agent_id=("agent_id", "first"),
        conversation_outcome=("conversation_outcome", "last"),
        total_messages=("message_id", "count"),
        inbound_messages=("is_inbound", "sum"),
        outbound_messages=("is_outbound", "sum"),
        non_text_messages=("message_type", lambda values: int((values != "text").sum())),
        duplicate_events_removed=("dropped_duplicate_events", "sum"),
        contains_email=("contains_email", "max"),
        contains_phone=("contains_phone", "max"),
        contains_cpf=("contains_cpf", "max"),
        contains_cep=("contains_cep", "max"),
        contains_plate=("contains_plate", "max"),
        mentioned_vehicle=("mentions_vehicle", "max"),
        mentioned_competitor=("mentions_competitor", "max"),
        mentioned_sinistro=("mentions_sinistro", "max"),
        primary_competitor=("competitor_mentioned", "first"),
        avg_response_time_sec=("metadata_response_time_sec", "mean"),
        avg_quoted_price=("quoted_price", "mean"),
        city=("metadata_city", "first"),
        state=("metadata_state", "first"),
        lead_source=("metadata_lead_source", "first"),
        lead_name_masked=(
            "conversation_lead_name",
            lambda values: mask_sender_name(values.iloc[0]),
        ),
    ).reset_index()

    vehicle_context = (
        silver.loc[
            silver["mentions_vehicle"],
            ["conversation_id", "vehicle_make", "vehicle_model", "vehicle_year"],
        ]
        .groupby("conversation_id", dropna=False)
        .agg(
            vehicle_make=("vehicle_make", "first"),
            vehicle_model=("vehicle_model", "first"),
            vehicle_year=("vehicle_year", "first"),
        )
        .reset_index()
    )
    sinistro_context = (
        silver.loc[silver["mentions_sinistro"], ["conversation_id", "sinistro_type"]]
        .groupby("conversation_id", dropna=False)
        .agg(primary_sinistro_type=("sinistro_type", "first"))
        .reset_index()
    )

    gold = gold.merge(vehicle_context, on="conversation_id", how="left")
    gold = gold.merge(sinistro_context, on="conversation_id", how="left")
    gold["conversation_duration_min"] = (
        (gold["ended_at"] - gold["started_at"]).dt.total_seconds() / 60.0
    ).fillna(0.0)
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
    gold = add_gold_segments(gold, compiled_plan=compiled_plan)
    return gold.sort_values("conversation_id").reset_index(drop=True)
