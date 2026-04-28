from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.agent.gold_designer import (
    GoldColumnDefinition,
    GoldColumnPlan,
    _fallback_gold_plan,
    _validate_plan,
    apply_gold_column_plan,
    design_gold_columns,
)


def _sample_leads() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "lead_key": ["lead_1", "lead_2"],
            "message_count": [5, 25],
            "conversation_count": [2, 10],
            "mentioned_competitor": [False, True],
            "mentioned_sinistro": [False, True],
            "contains_email": [True, False],
            "contains_cpf": [False, True],
            "data_shared_score": [1, 3],
            "engagement_bucket": ["curta", "longa"],
            "avg_quoted_price": [None, 1500.0],
        }
    )


def _sample_messages() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "lead_key": ["lead_1", "lead_2"],
            "message_body": ["oi tudo bem", "quero cotação sinistro"],
            "is_inbound": [True, True],
            "mentions_vehicle": [False, True],
        }
    )


def _make_paths(tmp_path: Path) -> Any:
    from pipeline.config import build_paths

    return build_paths(root=tmp_path)


def _make_spec() -> dict[str, Any]:
    return {
        "forbidden_columns": ["sender_phone", "sender_name", "message_body"],
        "silver": {"required_columns": []},
    }


# --- fallback plan tests ---


def test_fallback_plan_has_required_columns() -> None:
    plan = _fallback_gold_plan()
    assert plan.source == "deterministic_fallback"
    names = {c.name for c in plan.columns}
    assert "lead_temperature" in names
    assert "price_sensitivity" in names
    assert "contact_readiness" in names
    assert "risk_signal" in names
    assert "persona_profile" in names
    assert "audience_segment" in names


def test_fallback_plan_all_conditional_buckets_have_segment_values() -> None:
    plan = _fallback_gold_plan()
    for col in plan.columns:
        if col.derivation_logic.get("type") == "conditional_bucket":
            assert col.segment_values is not None and len(col.segment_values) > 0, (
                f"Column {col.name} missing segment_values"
            )


# --- validate_plan tests ---


def test_validate_plan_rejects_missing_segment_values() -> None:
    bad_col = GoldColumnDefinition(
        name="bad_col",
        data_type="string",
        derivation_logic={"type": "conditional_bucket", "conditions": []},
        rationale="test",
        segment_values=None,
    )
    plan = GoldColumnPlan(
        columns=[bad_col],
        source="llm",
        generated_at_utc="2026-04-27T00:00:00+00:00",
        llm_rationale=None,
    )
    valid_plan, violations = _validate_plan(plan, _make_spec(), set())
    assert any("missing_segment_values" in v for v in violations)
    assert len(valid_plan.columns) == 0


def test_validate_plan_removes_forbidden_columns() -> None:
    bad_col = GoldColumnDefinition(
        name="phone_signal",
        data_type="string",
        derivation_logic={"type": "aggregation", "agg_fn": "sum", "source_col": "sender_phone"},
        rationale="test",
        segment_values=None,
    )
    plan = GoldColumnPlan(
        columns=[bad_col],
        source="llm",
        generated_at_utc="2026-04-27T00:00:00+00:00",
        llm_rationale=None,
    )
    valid_plan, violations = _validate_plan(plan, _make_spec(), set())
    assert any("privacy_violation" in v for v in violations)
    assert len(valid_plan.columns) == 0


def test_validate_plan_rejects_duplicate_names() -> None:
    col = GoldColumnDefinition(
        name="dup_col",
        data_type="int64",
        derivation_logic={"type": "aggregation", "agg_fn": "sum", "source_col": "message_count"},
        rationale="test",
        segment_values=None,
    )
    plan = GoldColumnPlan(
        columns=[col, col],
        source="llm",
        generated_at_utc="2026-04-27T00:00:00+00:00",
        llm_rationale=None,
    )
    valid_plan, violations = _validate_plan(plan, _make_spec(), set())
    assert any("duplicate_column_name" in v for v in violations)
    assert len(valid_plan.columns) == 1


def test_validate_plan_accepts_valid_columns() -> None:
    good_col = GoldColumnDefinition(
        name="engagement_ratio",
        data_type="float64",
        derivation_logic={"type": "aggregation", "agg_fn": "mean", "source_col": "message_count"},
        rationale="ratio test",
        segment_values=None,
    )
    plan = GoldColumnPlan(
        columns=[good_col],
        source="llm",
        generated_at_utc="2026-04-27T00:00:00+00:00",
        llm_rationale=None,
    )
    valid_plan, violations = _validate_plan(plan, _make_spec(), set())
    assert violations == []
    assert len(valid_plan.columns) == 1


# --- design_gold_columns tests ---


def test_design_gold_columns_no_llm_returns_fallback(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    paths.agent_decisions.mkdir(parents=True, exist_ok=True)
    paths.docs.mkdir(parents=True, exist_ok=True)

    plan = design_gold_columns(
        silver_leads_df=_sample_leads(),
        silver_messages_df=_sample_messages(),
        spec=_make_spec(),
        paths=paths,
        llm_call=None,
    )
    assert plan.source == "deterministic_fallback"
    assert len(plan.columns) >= 5


def test_design_gold_columns_llm_success(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    paths.agent_decisions.mkdir(parents=True, exist_ok=True)
    paths.docs.mkdir(parents=True, exist_ok=True)

    llm_response = json.dumps(
        {
            "columns": [
                {
                    "name": "engagement_intensity",
                    "data_type": "string",
                    "derivation_logic": {
                        "type": "conditional_bucket",
                        "conditions": [
                            {"when": "message_count > 20", "then": "high"},
                            {"when": "message_count > 5", "then": "medium"},
                            {"else": "low"},
                        ],
                    },
                    "rationale": "Measures engagement depth",
                    "segment_values": ["low", "medium", "high"],
                },
                {
                    "name": "inbound_ratio",
                    "data_type": "float64",
                    "derivation_logic": {
                        "type": "aggregation",
                        "agg_fn": "mean",
                        "source_col": "data_shared_score",
                    },
                    "rationale": "Ratio of inbound messages",
                    "segment_values": None,
                },
                {
                    "name": "commercial_intent",
                    "data_type": "string",
                    "derivation_logic": {
                        "type": "conditional_bucket",
                        "conditions": [
                            {"when": "mentioned_competitor", "then": "comparador"},
                            {"else": "prospect"},
                        ],
                    },
                    "rationale": "Commercial intent signal",
                    "segment_values": ["prospect", "comparador"],
                },
                {
                    "name": "persona_type",
                    "data_type": "string",
                    "derivation_logic": {
                        "type": "conditional_bucket",
                        "conditions": [
                            {"when": "mentioned_sinistro", "then": "sinistrado"},
                            {"else": "padrao"},
                        ],
                    },
                    "rationale": "Persona classification",
                    "segment_values": ["padrao", "sinistrado"],
                },
                {
                    "name": "data_richness",
                    "data_type": "int64",
                    "derivation_logic": {
                        "type": "aggregation",
                        "agg_fn": "sum",
                        "source_col": "data_shared_score",
                    },
                    "rationale": "Amount of contextual data shared",
                    "segment_values": None,
                },
            ],
            "llm_rationale": "Novel segmentation dimensions for vehicle insurance CRM.",
        }
    )

    def mock_llm(prompt: str, compiled_plan: dict, timeout: float) -> str:
        return llm_response

    plan = design_gold_columns(
        silver_leads_df=_sample_leads(),
        silver_messages_df=_sample_messages(),
        spec=_make_spec(),
        paths=paths,
        llm_call=mock_llm,
    )
    assert plan.source == "llm"
    assert len(plan.columns) >= 5
    assert plan.llm_rationale is not None


def test_design_gold_columns_llm_timeout_uses_fallback(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    paths.agent_decisions.mkdir(parents=True, exist_ok=True)
    paths.docs.mkdir(parents=True, exist_ok=True)

    def mock_llm(prompt: str, compiled_plan: dict, timeout: float) -> str:
        raise TimeoutError("LLM timed out")

    plan = design_gold_columns(
        silver_leads_df=_sample_leads(),
        silver_messages_df=_sample_messages(),
        spec=_make_spec(),
        paths=paths,
        llm_call=mock_llm,
    )
    assert plan.source == "deterministic_fallback"


def test_design_gold_columns_llm_invalid_json_uses_fallback(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    paths.agent_decisions.mkdir(parents=True, exist_ok=True)
    paths.docs.mkdir(parents=True, exist_ok=True)

    def mock_llm(prompt: str, compiled_plan: dict, timeout: float) -> str:
        return "This is not JSON"

    plan = design_gold_columns(
        silver_leads_df=_sample_leads(),
        silver_messages_df=_sample_messages(),
        spec=_make_spec(),
        paths=paths,
        llm_call=mock_llm,
    )
    assert plan.source == "deterministic_fallback"


def test_design_gold_columns_persists_plan_json(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    paths.agent_decisions.mkdir(parents=True, exist_ok=True)
    paths.docs.mkdir(parents=True, exist_ok=True)

    design_gold_columns(
        silver_leads_df=_sample_leads(),
        silver_messages_df=_sample_messages(),
        spec=_make_spec(),
        paths=paths,
        llm_call=None,
    )
    plan_file = paths.agent_decisions / "latest_gold_column_plan.json"
    assert plan_file.exists()
    data = json.loads(plan_file.read_text())
    assert "columns" in data
    assert "source" in data


# --- apply_gold_column_plan tests ---


def test_apply_plan_adds_aggregation_column() -> None:
    df = pd.DataFrame({"lead_key": ["a", "b"], "data_shared_score": [1, 3]})
    col = GoldColumnDefinition(
        name="data_score_copy",
        data_type="int64",
        derivation_logic={
            "type": "aggregation",
            "agg_fn": "sum",
            "source_col": "data_shared_score",
        },
        rationale="test",
        segment_values=None,
    )
    plan = GoldColumnPlan(
        columns=[col],
        source="llm",
        generated_at_utc="2026-04-27T00:00:00+00:00",
        llm_rationale=None,
    )
    result = apply_gold_column_plan(df, plan)
    assert "data_score_copy" in result.columns


def test_apply_plan_missing_source_col_produces_null() -> None:
    df = pd.DataFrame({"lead_key": ["a"]})
    col = GoldColumnDefinition(
        name="phantom_col",
        data_type="int64",
        derivation_logic={"type": "aggregation", "agg_fn": "sum", "source_col": "nonexistent"},
        rationale="test",
        segment_values=None,
    )
    plan = GoldColumnPlan(
        columns=[col],
        source="llm",
        generated_at_utc="2026-04-27T00:00:00+00:00",
        llm_rationale=None,
    )
    result = apply_gold_column_plan(df, plan)
    assert "phantom_col" in result.columns
    assert result["phantom_col"].isna().all()


def test_apply_plan_conditional_bucket() -> None:
    df = pd.DataFrame({"lead_key": ["a", "b", "c"], "message_count": [2, 8, 25]})
    col = GoldColumnDefinition(
        name="engagement_tier",
        data_type="string",
        derivation_logic={
            "type": "conditional_bucket",
            "conditions": [
                {"when": "message_count > 20", "then": "high"},
                {"when": "message_count > 5", "then": "medium"},
                {"else": "low"},
            ],
        },
        rationale="test",
        segment_values=["low", "medium", "high"],
    )
    plan = GoldColumnPlan(
        columns=[col],
        source="llm",
        generated_at_utc="2026-04-27T00:00:00+00:00",
        llm_rationale=None,
    )
    result = apply_gold_column_plan(df, plan)
    assert "engagement_tier" in result.columns
    assert result.loc[result["message_count"] == 25, "engagement_tier"].iloc[0] == "high"
    assert result.loc[result["message_count"] == 8, "engagement_tier"].iloc[0] == "medium"
    assert result.loc[result["message_count"] == 2, "engagement_tier"].iloc[0] == "low"


def test_apply_plan_skips_existing_columns() -> None:
    df = pd.DataFrame({"lead_key": ["a"], "existing_col": [42]})
    col = GoldColumnDefinition(
        name="existing_col",
        data_type="int64",
        derivation_logic={"type": "aggregation", "agg_fn": "sum", "source_col": "lead_key"},
        rationale="test",
        segment_values=None,
    )
    plan = GoldColumnPlan(
        columns=[col],
        source="llm",
        generated_at_utc="2026-04-27T00:00:00+00:00",
        llm_rationale=None,
    )
    result = apply_gold_column_plan(df, plan)
    assert result["existing_col"].iloc[0] == 42


# --- GoldColumnPlan dataclass tests ---


def test_gold_column_plan_as_dict() -> None:
    plan = _fallback_gold_plan()
    d = plan.as_dict()
    assert "columns" in d
    assert isinstance(d["columns"], list)
    assert d["source"] == "deterministic_fallback"
    assert "generated_at_utc" in d
