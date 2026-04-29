from __future__ import annotations

from typing import Any, cast

import pandas as pd

from pipeline.orchestration.compiler import get_default_compiled_plan
from pipeline.transforms.silver_patterns import _safe_float, _safe_int, _safe_string

__all__ = [
    "_canonical_audience_for_persona",
    "_response_latency_band",
    "_price_objection_intensity",
    "_commercial_urgency_signal",
    "_competitor_pressure_level",
    "add_gold_segments",
]


def _canonical_audience_for_persona(persona_profile: object) -> str:
    persona = _safe_string(persona_profile).strip()
    if persona == "cliente_pos_sinistro":
        return "retencao_pos_sinistro"
    if persona == "cotador_comparador":
        return "oferta_competitiva"
    if persona == "lead_engajado_com_dados":
        return "close_comercial"
    if persona == "lead_frio":
        return "nutricao_basica"
    return ""


def _response_latency_band(avg_response_time_sec: object) -> str:
    value = _safe_float(avg_response_time_sec)
    if value is None:
        return "sem_evidencia"
    if value <= 300:
        return "rapida"
    if value <= 1800:
        return "moderada"
    return "lenta"


def _price_objection_intensity(
    price_objection_hits: object,
    competitor_mentions: object,
    quoted_price_mentions: object,
) -> str:
    hits = _safe_int(price_objection_hits)
    competitor_count = _safe_int(competitor_mentions)
    quote_count = _safe_int(quoted_price_mentions)
    if hits >= 2 or (hits >= 1 and competitor_count >= 1) or quote_count >= 2:
        return "forte"
    if hits >= 1 or quote_count >= 1 or competitor_count >= 1:
        return "leve"
    return "nenhuma"


def _commercial_urgency_signal(
    urgency_strength_max: object,
    urgency_hits: object,
    lead_lifecycle_hours: object,
) -> str:
    max_strength = _safe_int(urgency_strength_max)
    hit_count = _safe_int(urgency_hits)
    if max_strength >= 2 or hit_count >= 2:
        return "alta"
    if max_strength >= 1:
        return "moderada"
    return "nenhuma"


def _competitor_pressure_level(
    primary_competitor: object,
    competitor_mentions: object,
    competitor_comparison_hits: object,
) -> str:
    has_primary = bool(_safe_string(primary_competitor).strip())
    mentions = _safe_int(competitor_mentions)
    comparisons = _safe_int(competitor_comparison_hits)
    if not has_primary and mentions == 0 and comparisons == 0:
        return "nenhuma"
    if comparisons >= 1 or mentions >= 2:
        return "alta"
    return "leve"


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
    negotiation_label = intent_cfg.get("negotiation_label")
    if negotiation_label is not None and "latest_outcome" in segmented.columns:
        normalized_outcome = segmented["latest_outcome"].fillna("").astype(str).str.lower()
        segmented.loc[normalized_outcome.str.contains("negoci"), "intent_stage"] = str(
            negotiation_label
        )
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
