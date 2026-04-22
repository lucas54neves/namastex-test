from __future__ import annotations

import json
from pathlib import Path

from pipeline.runtime.spec import default_pipeline_spec, validate_pipeline_spec

ROOT = Path(__file__).resolve().parents[1]


def test_default_pipeline_spec_is_valid() -> None:
    spec = default_pipeline_spec()
    validate_pipeline_spec(spec)
    assert spec["gold"]["segmentation"]["personas"]["default"] == "lead_frio"
    assert "quoted_price" in spec["silver"]["derived_fields"]
    assert "conversation_sentiment_label" in spec["gold"]["required_columns"]
    assert "sem_evidencia" in spec["gold"]["valid_conversation_sentiment_labels"]
    assert "llm_provider" in spec["gold"]["valid_semantic_source_families"]
    assert (
        spec["gold"]["semantic_contracts"]["interpretive_provenance_columns"]["intent_stage"]
        == "intent_stage_source_family"
    )
    assert spec["agent"]["planner"]["proposal_defaults"]["default_status"] == "proposed"


def test_versioned_pipeline_spec_is_valid() -> None:
    spec = json.loads((ROOT / "config" / "pipeline_spec.json").read_text(encoding="utf-8"))
    validate_pipeline_spec(spec)


def test_versioned_pipeline_spec_matches_default_required_fields() -> None:
    default_spec = default_pipeline_spec()
    versioned_spec = json.loads(
        (ROOT / "config" / "pipeline_spec.json").read_text(encoding="utf-8")
    )

    required_list_paths = [
        ("silver", "derived_fields"),
        ("gold", "required_columns"),
        ("gold", "valid_email_providers"),
        ("gold", "valid_response_latency_bands"),
        ("gold", "valid_closure_outcome_groups"),
        ("gold", "valid_price_objection_intensities"),
        ("gold", "valid_commercial_urgency_signals"),
        ("gold", "valid_competitor_pressure_levels"),
        ("gold", "valid_conversation_sentiment_labels"),
        ("gold", "valid_conversation_sentiment_supports"),
        ("gold", "valid_semantic_source_families"),
        ("quality", "bronze_checks"),
        ("quality", "silver_checks"),
        ("quality", "gold_checks"),
    ]

    for section, key in required_list_paths:
        assert versioned_spec[section][key] == default_spec[section][key]

    assert (
        versioned_spec["quality"]["validation_rules"] == default_spec["quality"]["validation_rules"]
    )
    assert (
        versioned_spec["gold"]["semantic_contracts"] == default_spec["gold"]["semantic_contracts"]
    )
    assert versioned_spec["agent"]["planner"] == default_spec["agent"]["planner"]
