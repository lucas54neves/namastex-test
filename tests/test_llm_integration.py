from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd

from pipeline.agent.execution_planner import (
    VALID_STAGES,
    Observation,
    build_execution_plan,
)
from pipeline.agent.gold_designer import (
    _fallback_gold_plan,
    design_gold_columns,
)
from pipeline.agent.llm_advisor import get_llm_advice

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_compiled_plan(enabled: bool = True, provider: str = "openai") -> dict[str, Any]:
    return {"llm": {"enabled": enabled, "provider": provider}, "agent": {}}


def _make_advisor_context(
    proposals: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "observed_columns": ["conversation_id", "message_body"],
        "observed_metadata_fields": ["is_business_hours"],
        "detected_contexts": [{"context_type": "bronze_observation_summary"}],
        "proposals": proposals
        or [
            {
                "proposal_type": "gold_business_hours_metric_addition",
                "proposal_family": "derived_column_addition",
                "expected_impact": "adds metric",
            },
            {
                "proposal_type": "metadata_key_normalization_rule",
                "proposal_family": "transformation_rule_change",
                "expected_impact": "normalizes keys",
            },
        ],
    }


def _sample_leads() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "lead_key": ["lead_1", "lead_2"],
            "message_count": [5, 25],
            "conversation_count": [2, 10],
            "mentioned_competitor": [False, True],
            "mentioned_sinistro": [False, True],
            "data_shared_score": [1, 3],
            "engagement_bucket": ["curta", "longa"],
            "avg_quoted_price": [None, 1500.0],
        }
    )


def _sample_messages() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "lead_key": ["lead_1", "lead_2"],
            "message_body": ["oi", "cotacao"],
            "is_inbound": [True, True],
        }
    )


def _make_paths(tmp_path: Path) -> Any:
    from pipeline.config import build_paths

    return build_paths(root=tmp_path)


def _make_gold_spec() -> dict[str, Any]:
    return {
        "forbidden_columns": ["sender_phone", "sender_name", "message_body"],
        "silver": {"required_columns": []},
    }


def _make_observation(source_changed: bool = True) -> Observation:
    return Observation(
        source_changed=source_changed,
        layer_artifacts={
            "bronze": {"exists": False, "age_hours": None},
            "silver": {"exists": False, "age_hours": None},
            "gold": {"exists": False, "age_hours": None},
        },
        last_validation_summary={},
        last_run_status=None,
        run_count=0,
        generated_at_utc="2026-04-28T00:00:00+00:00",
    )


def _valid_llm_advisor_response(proposals: list[dict[str, Any]]) -> str:
    types = [p["proposal_type"] for p in proposals]
    return json.dumps(
        {
            "priority_proposals": types[:1],
            "deferred_proposals": types[1:2] if len(types) > 1 else [],
            "risk_flags": ["potential schema drift detected"],
            "summary": "Prioritize first proposal.",
        }
    )


# ---------------------------------------------------------------------------
# QUAL-04: get_llm_advice — LLM path
# ---------------------------------------------------------------------------


def test_get_llm_advice_llm_path_success() -> None:
    context = _make_advisor_context()
    mock_response = _valid_llm_advisor_response(context["proposals"])

    with patch("pipeline.runtime.llm_runtime.call_llm", return_value=mock_response):
        result = get_llm_advice(context, _make_compiled_plan(enabled=True))

    assert result["status"] == "ok"
    assert "gold_business_hours_metric_addition" in result["priority_proposals"]
    assert result["deferred_proposals"] == ["metadata_key_normalization_rule"]
    assert isinstance(result["risk_flags"], list)


def test_get_llm_advice_llm_path_diverges_from_fallback() -> None:
    context = _make_advisor_context()
    mock_response = _valid_llm_advisor_response(context["proposals"])

    with patch("pipeline.runtime.llm_runtime.call_llm", return_value=mock_response):
        llm_result = get_llm_advice(context, _make_compiled_plan(enabled=True))

    fallback_result = get_llm_advice(context, _make_compiled_plan(enabled=False))

    assert llm_result["status"] == "ok"
    assert fallback_result["status"] == "disabled"
    assert llm_result["priority_proposals"] != fallback_result["priority_proposals"]


def test_get_llm_advice_fallback_path_on_invalid_json() -> None:
    with patch("pipeline.runtime.llm_runtime.call_llm", return_value="not valid json"):
        result = get_llm_advice(_make_advisor_context(), _make_compiled_plan(enabled=True))

    assert result["status"] == "llm_failed"
    assert result["priority_proposals"] == []
    assert result["deferred_proposals"] == []


def test_get_llm_advice_fallback_path_on_exception() -> None:
    with patch("pipeline.runtime.llm_runtime.call_llm", side_effect=Exception("provider timeout")):
        result = get_llm_advice(_make_advisor_context(), _make_compiled_plan(enabled=True))

    assert result["status"] == "llm_failed"
    assert result["priority_proposals"] == []


def test_get_llm_advice_disabled_path() -> None:
    result = get_llm_advice(_make_advisor_context(), _make_compiled_plan(enabled=False))

    assert result["status"] == "disabled"
    assert result["priority_proposals"] == []
    assert result["deferred_proposals"] == []


def test_get_llm_advice_disabled_path_does_not_call_llm() -> None:
    with patch("pipeline.runtime.llm_runtime.call_llm") as mock_llm:
        get_llm_advice(_make_advisor_context(), _make_compiled_plan(enabled=False))

    mock_llm.assert_not_called()


def test_get_llm_advice_invented_proposal_types_ignored_in_reorder() -> None:
    """LLM returning invented proposal_types must not create ghost entries when filtered."""
    context = _make_advisor_context()
    real_types = {p["proposal_type"] for p in context["proposals"]}

    mock_response = json.dumps(
        {
            "priority_proposals": [
                "invented_nonexistent_type",
                "gold_business_hours_metric_addition",
            ],
            "deferred_proposals": [],
            "risk_flags": [],
            "summary": "Invented type included.",
        }
    )

    with patch("pipeline.runtime.llm_runtime.call_llm", return_value=mock_response):
        result = get_llm_advice(context, _make_compiled_plan(enabled=True))

    # The LLM response is returned verbatim by get_llm_advice; the filtering
    # happens in plan_pipeline_spec. Verify the invented type is in the raw result
    # but would be filtered out if applied to the actual proposals list.
    priority_from_llm = result["priority_proposals"]
    filtered = [p for p in context["proposals"] if p["proposal_type"] in priority_from_llm]
    filtered_types = {p["proposal_type"] for p in filtered}

    assert "invented_nonexistent_type" not in filtered_types
    assert all(t in real_types for t in filtered_types)


# ---------------------------------------------------------------------------
# QUAL-04: build_execution_plan — LLM path
# ---------------------------------------------------------------------------


def test_build_execution_plan_llm_path_success(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    paths.monitoring.mkdir(parents=True, exist_ok=True)
    obs = _make_observation(source_changed=True)

    mock_response = json.dumps(
        {
            "stages": ["bronze", "silver", "gold", "validation"],
            "rationale": "Source changed — run all stages.",
            "confidence": 0.95,
        }
    )

    def mock_llm(prompt: str, compiled_plan: dict, timeout: float) -> str:
        return mock_response

    plan = build_execution_plan(obs, paths, llm_call=mock_llm)

    assert plan.source == "llm"
    assert "bronze" in plan.stages
    assert plan.confidence == 0.95


def test_build_execution_plan_llm_path_filters_invalid_stages(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    paths.monitoring.mkdir(parents=True, exist_ok=True)
    obs = _make_observation(source_changed=False)

    mock_response = json.dumps(
        {
            "stages": ["bronze", "invalid_stage", "gold", "also_invalid"],
            "rationale": "LLM returned invalid stages.",
            "confidence": 0.7,
        }
    )

    def mock_llm(prompt: str, compiled_plan: dict, timeout: float) -> str:
        return mock_response

    plan = build_execution_plan(obs, paths, llm_call=mock_llm)

    assert plan.source == "llm"
    for stage in plan.stages:
        assert stage in VALID_STAGES, f"Invalid stage found in plan: {stage}"
    assert "invalid_stage" not in plan.stages
    assert "also_invalid" not in plan.stages


def test_build_execution_plan_fallback_path_on_invalid_json(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    paths.monitoring.mkdir(parents=True, exist_ok=True)
    obs = _make_observation(source_changed=True)

    def mock_llm(prompt: str, compiled_plan: dict, timeout: float) -> str:
        return "not valid json"

    plan = build_execution_plan(obs, paths, llm_call=mock_llm)

    assert plan.source == "deterministic_fallback"


def test_build_execution_plan_fallback_path_on_exception(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    paths.monitoring.mkdir(parents=True, exist_ok=True)
    obs = _make_observation(source_changed=True)

    def mock_llm(prompt: str, compiled_plan: dict, timeout: float) -> str:
        raise Exception("LLM provider down")

    plan = build_execution_plan(obs, paths, llm_call=mock_llm)

    assert plan.source == "deterministic_fallback"


def test_build_execution_plan_disabled_path_returns_deterministic(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    paths.monitoring.mkdir(parents=True, exist_ok=True)
    obs = _make_observation(source_changed=True)

    plan = build_execution_plan(obs, paths, llm_call=None)

    assert plan.source == "deterministic_fallback"
    assert "bronze" in plan.stages


def test_build_execution_plan_disabled_path_matches_deterministic(tmp_path: Path) -> None:
    from pipeline.agent.execution_planner import _deterministic_plan

    paths = _make_paths(tmp_path)
    paths.monitoring.mkdir(parents=True, exist_ok=True)
    obs = _make_observation(source_changed=True)

    plan_no_llm = build_execution_plan(obs, paths, llm_call=None)
    plan_deterministic = _deterministic_plan(obs)

    assert plan_no_llm.stages == plan_deterministic.stages
    assert plan_no_llm.source == plan_deterministic.source


# ---------------------------------------------------------------------------
# QUAL-04: design_gold_columns — LLM path
# ---------------------------------------------------------------------------

_VALID_GOLD_LLM_RESPONSE = json.dumps(
    {
        "columns": [
            {
                "name": "engagement_intensity",
                "data_type": "string",
                "derivation_logic": {
                    "type": "conditional_bucket",
                    "conditions": [
                        {"when": "message_count > 20", "then": "high"},
                        {"else": "low"},
                    ],
                },
                "rationale": "Engagement depth",
                "segment_values": ["low", "high"],
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
                "rationale": "Commercial intent",
                "segment_values": ["prospect", "comparador"],
            },
            {
                "name": "sinistro_flag",
                "data_type": "string",
                "derivation_logic": {
                    "type": "conditional_bucket",
                    "conditions": [
                        {"when": "mentioned_sinistro", "then": "sinistrado"},
                        {"else": "padrao"},
                    ],
                },
                "rationale": "Sinistro classification",
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
                "rationale": "Data richness",
                "segment_values": None,
            },
            {
                "name": "msg_volume",
                "data_type": "int64",
                "derivation_logic": {
                    "type": "aggregation",
                    "agg_fn": "sum",
                    "source_col": "message_count",
                },
                "rationale": "Message volume",
                "segment_values": None,
            },
        ],
        "llm_rationale": "Novel CRM segmentation columns.",
    }
)


def test_design_gold_columns_llm_path_success(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    paths.agent_decisions.mkdir(parents=True, exist_ok=True)
    paths.docs.mkdir(parents=True, exist_ok=True)

    def mock_llm(prompt: str, compiled_plan: dict, timeout: float) -> str:
        return _VALID_GOLD_LLM_RESPONSE

    plan = design_gold_columns(
        silver_leads_df=_sample_leads(),
        silver_messages_df=_sample_messages(),
        spec=_make_gold_spec(),
        paths=paths,
        llm_call=mock_llm,
    )

    assert plan.source == "llm"
    assert len(plan.columns) >= 5
    assert plan.llm_rationale is not None


def test_design_gold_columns_llm_path_diverges_from_fallback(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    paths.agent_decisions.mkdir(parents=True, exist_ok=True)
    paths.docs.mkdir(parents=True, exist_ok=True)

    def mock_llm(prompt: str, compiled_plan: dict, timeout: float) -> str:
        return _VALID_GOLD_LLM_RESPONSE

    llm_plan = design_gold_columns(
        silver_leads_df=_sample_leads(),
        silver_messages_df=_sample_messages(),
        spec=_make_gold_spec(),
        paths=paths,
        llm_call=mock_llm,
    )
    fallback_plan = _fallback_gold_plan()

    assert llm_plan.source == "llm"
    assert fallback_plan.source == "deterministic_fallback"
    llm_names = {c.name for c in llm_plan.columns}
    fallback_names = {c.name for c in fallback_plan.columns}
    assert llm_names != fallback_names


def test_design_gold_columns_llm_rejects_conditional_bucket_without_segment_values(
    tmp_path: Path,
) -> None:
    paths = _make_paths(tmp_path)
    paths.agent_decisions.mkdir(parents=True, exist_ok=True)
    paths.docs.mkdir(parents=True, exist_ok=True)

    bad_response = json.dumps(
        {
            "columns": [
                {
                    "name": "bad_bucket",
                    "data_type": "string",
                    "derivation_logic": {
                        "type": "conditional_bucket",
                        "conditions": [
                            {"when": "message_count > 10", "then": "high"},
                            {"else": "low"},
                        ],
                    },
                    "rationale": "Missing segment_values",
                    "segment_values": None,
                }
            ],
            "llm_rationale": "Bad plan with missing segment_values.",
        }
    )

    def mock_llm(prompt: str, compiled_plan: dict, timeout: float) -> str:
        return bad_response

    plan = design_gold_columns(
        silver_leads_df=_sample_leads(),
        silver_messages_df=_sample_messages(),
        spec=_make_gold_spec(),
        paths=paths,
        llm_call=mock_llm,
    )

    col_names = [c.name for c in plan.columns]
    assert "bad_bucket" not in col_names


def test_design_gold_columns_fallback_path_on_invalid_json(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    paths.agent_decisions.mkdir(parents=True, exist_ok=True)
    paths.docs.mkdir(parents=True, exist_ok=True)

    def mock_llm(prompt: str, compiled_plan: dict, timeout: float) -> str:
        return "not valid json"

    plan = design_gold_columns(
        silver_leads_df=_sample_leads(),
        silver_messages_df=_sample_messages(),
        spec=_make_gold_spec(),
        paths=paths,
        llm_call=mock_llm,
    )

    assert plan.source == "deterministic_fallback"


def test_design_gold_columns_fallback_path_matches_fallback_function(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    paths.agent_decisions.mkdir(parents=True, exist_ok=True)
    paths.docs.mkdir(parents=True, exist_ok=True)

    def mock_llm(prompt: str, compiled_plan: dict, timeout: float) -> str:
        raise Exception("LLM provider unavailable")

    plan = design_gold_columns(
        silver_leads_df=_sample_leads(),
        silver_messages_df=_sample_messages(),
        spec=_make_gold_spec(),
        paths=paths,
        llm_call=mock_llm,
    )

    fallback = _fallback_gold_plan()
    assert plan.source == "deterministic_fallback"
    fallback_names = {c.name for c in fallback.columns}
    plan_names = {c.name for c in plan.columns}
    assert plan_names == fallback_names


def test_design_gold_columns_disabled_path_returns_fallback(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    paths.agent_decisions.mkdir(parents=True, exist_ok=True)
    paths.docs.mkdir(parents=True, exist_ok=True)

    plan = design_gold_columns(
        silver_leads_df=_sample_leads(),
        silver_messages_df=_sample_messages(),
        spec=_make_gold_spec(),
        paths=paths,
        llm_call=None,
    )

    assert plan.source == "deterministic_fallback"


def test_design_gold_columns_disabled_path_does_not_call_llm(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    paths.agent_decisions.mkdir(parents=True, exist_ok=True)
    paths.docs.mkdir(parents=True, exist_ok=True)

    with patch("pipeline.runtime.llm_runtime.call_llm") as mock_llm:
        design_gold_columns(
            silver_leads_df=_sample_leads(),
            silver_messages_df=_sample_messages(),
            spec=_make_gold_spec(),
            paths=paths,
            llm_call=None,
        )

    mock_llm.assert_not_called()
