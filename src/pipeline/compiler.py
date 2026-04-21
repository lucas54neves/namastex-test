from __future__ import annotations

from functools import lru_cache
from typing import Any

from pipeline.spec import default_pipeline_spec, validate_pipeline_spec


def compile_pipeline_spec(spec: dict[str, Any]) -> dict[str, Any]:
    validate_pipeline_spec(spec)
    return {
        "bronze_required_columns": list(spec["bronze"]["required_columns"]),
        "supported_channels": set(spec["bronze"]["supported_channels"]),
        "bronze_quarantine": dict(spec["bronze"].get("quarantine", {})),
        "silver_lead_required_columns": list(spec["silver"]["lead_required_columns"]),
        "silver_message_required_columns": list(spec["silver"]["message_required_columns"]),
        "dedupe_keys": list(spec["silver"]["dedupe_keys"]),
        "metadata_fields": list(spec["silver"].get("metadata_fields", [])),
        "silver_derived_fields": list(spec["silver"].get("derived_fields", [])),
        "gold_required_columns": list(spec["gold"]["required_columns"]),
        "gold_valid_buckets": set(spec["gold"]["valid_buckets"]),
        "gold_valid_personas": set(spec["gold"]["valid_personas"]),
        "gold_valid_audiences": set(spec["gold"]["valid_audiences"]),
        "gold_valid_temperatures": set(spec["gold"]["valid_temperatures"]),
        "gold_valid_price_sensitivities": set(spec["gold"]["valid_price_sensitivities"]),
        "gold_valid_intent_stages": set(spec["gold"]["valid_intent_stages"]),
        "gold_valid_contact_readiness": set(spec["gold"]["valid_contact_readiness"]),
        "gold_valid_risk_signals": set(spec["gold"]["valid_risk_signals"]),
        "gold_valid_email_providers": set(spec["gold"]["valid_email_providers"]),
        "gold_valid_response_latency_bands": set(spec["gold"]["valid_response_latency_bands"]),
        "gold_valid_closure_outcome_groups": set(spec["gold"]["valid_closure_outcome_groups"]),
        "gold_valid_price_objection_intensities": set(
            spec["gold"]["valid_price_objection_intensities"]
        ),
        "gold_valid_commercial_urgency_signals": set(
            spec["gold"]["valid_commercial_urgency_signals"]
        ),
        "gold_valid_competitor_pressure_levels": set(
            spec["gold"]["valid_competitor_pressure_levels"]
        ),
        "gold_segmentation": dict(spec["gold"]["segmentation"]),
        "quality": dict(spec["quality"]),
        "quality_validation_rules": dict(spec["quality"].get("validation_rules", {})),
        "agent": dict(spec["agent"]),
        "agent_planner": dict(spec["agent"].get("planner", {})),
        "llm": dict(spec["llm"]),
    }


@lru_cache(maxsize=1)
def get_default_compiled_plan() -> dict[str, Any]:
    return compile_pipeline_spec(default_pipeline_spec())
