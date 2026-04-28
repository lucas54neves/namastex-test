from __future__ import annotations

from pipeline.agent.llm_advisor import agent_self_review_proposal, get_llm_advice
from pipeline.orchestration.compiler import get_default_compiled_plan


def test_llm_advice_defaults_to_disabled() -> None:
    advice = get_llm_advice({"changes": []}, get_default_compiled_plan())
    assert advice["enabled"] is False
    assert advice["status"] == "disabled"


def test_agent_self_review_returns_safe_fallback_when_llm_disabled() -> None:
    result = agent_self_review_proposal(
        proposal={"proposal_type": "bronze_required_columns_addition"},
        gate_results={},
        diff={},
        compiled_plan=get_default_compiled_plan(),
    )
    assert result["should_approve"] is False
    assert result["confidence"] == 0.0
    assert "disabled" in result["rationale"]


def test_agent_self_review_returns_parsed_result_when_llm_enabled(monkeypatch) -> None:
    import json

    llm_response = json.dumps(
        {"should_approve": True, "confidence": 0.92, "rationale": "additive and backward-compat"}
    )
    compiled_plan = {**get_default_compiled_plan(), "llm": {"enabled": True, "provider": "test"}}
    monkeypatch.setattr(
        "pipeline.runtime.llm_runtime.call_llm",
        lambda *args, **kwargs: llm_response,
    )

    result = agent_self_review_proposal(
        proposal={
            "proposal_id": "proposal_abc",
            "proposal_type": "silver_metadata_fields_addition",
            "proposal_family": "schema_update",
            "title": "test",
            "expected_impact": "expands contract",
            "risk": "low",
            "impact_class": "high",
            "proposed_change": {},
            "rationale": "test",
            "items": ["score_band"],
            "affected_layers": ["silver"],
            "privacy_impact": "none",
        },
        gate_results={"contract_validation": {"passed": True}},
        diff={"schema_diff": {}},
        compiled_plan=compiled_plan,
    )

    assert result["should_approve"] is True
    assert result["confidence"] == 0.92
    assert result["rationale"] == "additive and backward-compat"


def test_agent_self_review_falls_back_on_llm_error(monkeypatch) -> None:
    compiled_plan = {**get_default_compiled_plan(), "llm": {"enabled": True, "provider": "test"}}
    monkeypatch.setattr(
        "pipeline.runtime.llm_runtime.call_llm",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("timeout")),
    )

    result = agent_self_review_proposal(
        proposal={"proposal_type": "schema_update"},
        gate_results={},
        diff={},
        compiled_plan=compiled_plan,
    )

    assert result["should_approve"] is False
    assert result["confidence"] == 0.0
    assert "llm_error" in result["rationale"]
