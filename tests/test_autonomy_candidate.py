from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from pipeline.agent.autonomy import (
    _run_targeted_tests_gate,
    apply_proposal_to_spec,
    build_candidate_actions,
    evaluate_candidate,
)


def _make_proposal(
    proposal_family: str = "derived_column_addition",
    impact_class: str = "medium",
) -> dict[str, Any]:
    return {
        "proposal_id": "proposal_test",
        "proposal_type": "gold_business_hours_metric_addition",
        "proposal_family": proposal_family,
        "impact_class": impact_class,
        "policy_snapshot": {"requires_privacy_scan": False},
        "privacy_impact": "none",
    }


def _make_compiled_plan(test_paths: list[str] | None = None, timeout: int = 120) -> dict[str, Any]:
    agent_cfg: dict[str, Any] = {}
    if test_paths is not None:
        agent_cfg["targeted_test_paths"] = test_paths
        agent_cfg["targeted_tests_timeout_sec"] = timeout
    return {"agent": agent_cfg}


def _make_diff_with_gold_col_change() -> dict[str, Any]:
    return {
        "schema_diff": {
            "bronze.required_columns": {"added": [], "removed": []},
            "silver.metadata_fields": {"added": [], "removed": []},
            "gold.required_columns": {"added": ["business_hours_message_ratio"], "removed": []},
            "gold.valid_intent_stages": {"added": [], "removed": []},
        },
    }


def _make_diff_no_change() -> dict[str, Any]:
    return {
        "schema_diff": {
            "bronze.required_columns": {"added": [], "removed": []},
            "silver.metadata_fields": {"added": [], "removed": []},
            "gold.required_columns": {"added": [], "removed": []},
            "gold.valid_intent_stages": {"added": [], "removed": []},
        },
    }


def _base_spec() -> dict[str, Any]:
    from pipeline.runtime.spec import default_pipeline_spec

    return default_pipeline_spec()


# --- QUAL-01: targeted_tests gate ---


def test_targeted_tests_no_paths_configured_returns_not_executed() -> None:
    gate = _run_targeted_tests_gate(
        _make_proposal(),
        _make_compiled_plan(test_paths=None),
        _make_diff_with_gold_col_change(),
        {},
        {},
    )
    assert gate["executed"] is False
    assert gate["passed"] is True
    assert gate["reason"] == "no_targeted_tests_configured"
    assert gate["returncode"] is None
    assert gate["test_paths_run"] == []


def test_targeted_tests_empty_paths_returns_not_executed() -> None:
    gate = _run_targeted_tests_gate(
        _make_proposal(),
        _make_compiled_plan(test_paths=[]),
        _make_diff_with_gold_col_change(),
        {},
        {},
    )
    assert gate["executed"] is False
    assert gate["passed"] is True
    assert gate["reason"] == "no_targeted_tests_configured"


def test_targeted_tests_low_impact_validation_enhancement_skipped() -> None:
    gate = _run_targeted_tests_gate(
        _make_proposal(proposal_family="validation_enhancement", impact_class="low"),
        _make_compiled_plan(test_paths=["tests/test_jobs.py"]),
        _make_diff_with_gold_col_change(),
        {},
        {},
    )
    assert gate["executed"] is False
    assert gate["passed"] is True
    assert gate["reason"] == "low_impact_validation_enhancement_skipped"
    assert gate["returncode"] is None


def test_targeted_tests_no_relevant_changes_skips_execution() -> None:
    gate = _run_targeted_tests_gate(
        _make_proposal(),
        _make_compiled_plan(test_paths=["tests/test_jobs.py"]),
        _make_diff_no_change(),
        {},
        {},
    )
    assert gate["executed"] is False
    assert gate["passed"] is True
    assert gate["reason"] == "no_relevant_changes_in_candidate"


def test_targeted_tests_subprocess_returncode_0_passes() -> None:
    mock_result = MagicMock()
    mock_result.returncode = 0

    with patch("subprocess.run", return_value=mock_result):
        gate = _run_targeted_tests_gate(
            _make_proposal(),
            _make_compiled_plan(test_paths=["tests/test_jobs.py"]),
            _make_diff_with_gold_col_change(),
            {},
            {},
        )

    assert gate["executed"] is True
    assert gate["passed"] is True
    assert gate["returncode"] == 0
    assert gate["reason"] == "tests_passed"
    assert gate["test_paths_run"] == ["tests/test_jobs.py"]


def test_targeted_tests_subprocess_returncode_1_fails() -> None:
    mock_result = MagicMock()
    mock_result.returncode = 1

    with patch("subprocess.run", return_value=mock_result):
        gate = _run_targeted_tests_gate(
            _make_proposal(),
            _make_compiled_plan(test_paths=["tests/test_jobs.py"]),
            _make_diff_with_gold_col_change(),
            {},
            {},
        )

    assert gate["executed"] is True
    assert gate["passed"] is False
    assert gate["returncode"] == 1
    assert gate["reason"] == "tests_failed"


def test_targeted_tests_timeout_fails_gate() -> None:
    with patch(
        "subprocess.run",
        side_effect=subprocess.TimeoutExpired(cmd="pytest", timeout=5),
    ):
        gate = _run_targeted_tests_gate(
            _make_proposal(),
            _make_compiled_plan(test_paths=["tests/test_jobs.py"], timeout=5),
            _make_diff_with_gold_col_change(),
            {},
            {},
        )

    assert gate["executed"] is True
    assert gate["passed"] is False
    assert gate["returncode"] is None
    assert gate["reason"] == "timeout"


def test_targeted_tests_validation_rules_change_triggers_execution() -> None:
    base = _base_spec()
    candidate = {
        **base,
        "quality": {
            **base["quality"],
            "validation_rules": {
                **base["quality"]["validation_rules"],
                "silver": list(base["quality"]["validation_rules"].get("silver", []))
                + ["metadata_boolean_normalized"],
            },
        },
    }
    mock_result = MagicMock()
    mock_result.returncode = 0

    with patch("subprocess.run", return_value=mock_result):
        gate = _run_targeted_tests_gate(
            _make_proposal(),
            _make_compiled_plan(test_paths=["tests/test_jobs.py"]),
            _make_diff_no_change(),
            base,
            candidate,
        )

    assert gate["executed"] is True
    assert gate["passed"] is True


def test_evaluate_candidate_targeted_tests_gate_included_in_results(tmp_path: Path) -> None:
    spec = _base_spec()
    proposal = _make_proposal()

    gate_results, diff = evaluate_candidate(proposal, spec, spec)

    assert "targeted_tests" in gate_results
    tg = gate_results["targeted_tests"]
    assert "executed" in tg
    assert "passed" in tg
    assert "reason" in tg
    assert "returncode" in tg
    assert "test_paths_run" in tg


def test_evaluate_candidate_no_test_paths_executed_false(tmp_path: Path) -> None:
    spec = _base_spec()
    # Default spec has no targeted_test_paths in agent section
    gate_results, _ = evaluate_candidate(_make_proposal(), spec, spec)
    assert gate_results["targeted_tests"]["executed"] is False
    assert gate_results["targeted_tests"]["passed"] is True
    assert gate_results["targeted_tests"]["reason"] == "no_targeted_tests_configured"


def test_evaluate_candidate_with_test_paths_and_passing_tests(tmp_path: Path) -> None:
    import copy

    spec = _base_spec()
    candidate_spec = copy.deepcopy(spec)
    candidate_spec["agent"]["targeted_test_paths"] = ["tests/test_jobs.py"]
    candidate_spec["gold"]["required_columns"] = list(
        candidate_spec["gold"]["required_columns"]
    ) + ["new_test_col"]

    mock_result = MagicMock()
    mock_result.returncode = 0

    with patch("subprocess.run", return_value=mock_result):
        gate_results, _ = evaluate_candidate(_make_proposal(), spec, candidate_spec)

    assert gate_results["targeted_tests"]["executed"] is True
    assert gate_results["targeted_tests"]["passed"] is True
    assert gate_results["targeted_tests"]["returncode"] == 0


def test_evaluate_candidate_failing_tests_blocks_promotion(tmp_path: Path) -> None:
    import copy

    spec = _base_spec()
    candidate_spec = copy.deepcopy(spec)
    candidate_spec["agent"]["targeted_test_paths"] = ["tests/test_jobs.py"]
    candidate_spec["gold"]["required_columns"] = list(
        candidate_spec["gold"]["required_columns"]
    ) + ["new_test_col"]

    mock_result = MagicMock()
    mock_result.returncode = 1

    with patch("subprocess.run", return_value=mock_result):
        gate_results, _ = evaluate_candidate(_make_proposal(), spec, candidate_spec)

    assert gate_results["targeted_tests"]["executed"] is True
    assert gate_results["targeted_tests"]["passed"] is False


# --- FIX-B: targeted_test_paths wired in pipeline_spec.json ---


def test_evaluate_candidate_config_targeted_test_paths_executes_on_schema_diff() -> None:
    import copy
    import json
    from pathlib import Path

    _REPO_ROOT = Path(__file__).resolve().parents[1]
    spec = json.loads((_REPO_ROOT / "config" / "pipeline_spec.json").read_text())
    candidate_spec = copy.deepcopy(spec)
    candidate_spec["gold"]["required_columns"] = list(
        candidate_spec["gold"]["required_columns"]
    ) + ["new_config_col"]

    mock_result = MagicMock()
    mock_result.returncode = 0

    with patch("subprocess.run", return_value=mock_result):
        gate_results, _ = evaluate_candidate(_make_proposal(), spec, candidate_spec)

    assert gate_results["targeted_tests"]["executed"] is True
    assert gate_results["targeted_tests"]["passed"] is True
    assert "tests/test_jobs.py" in gate_results["targeted_tests"]["test_paths_run"]


def test_apply_proposal_to_spec_handles_schema_promotion_with_companion_actions() -> None:
    spec = _base_spec()
    proposal = {
        "proposal_type": "gold_passthrough_columns_addition",
        "proposed_change": {
            "target_path": "gold.passthrough_columns",
            "operation": "add_items",
            "items": ["response_latency_raw"],
            "companion_actions": [
                {
                    "target_path": "gold.aggregation_rules",
                    "operation": "set_keys",
                    "keys": {"response_latency_raw": "mean"},
                },
                {
                    "target_path": "schema_contract_version",
                    "operation": "set_value",
                    "value": 3,
                },
            ],
        },
        "items": ["response_latency_raw"],
    }

    updated, changed = apply_proposal_to_spec(spec, proposal)
    actions = build_candidate_actions(proposal)

    assert changed is True
    assert "response_latency_raw" in updated["gold"]["passthrough_columns"]
    assert updated["gold"]["aggregation_rules"]["response_latency_raw"] == "mean"
    assert updated["schema_contract_version"] == 3
    assert len(actions) == 3
