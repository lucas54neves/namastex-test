from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pipeline.agent.autonomy import (
    apply_proposal_to_spec,
    classify_proposal,
    get_quality_drift_policy,
)
from pipeline.agent.planner import _quality_drift_proposals
from pipeline.runtime.state import compute_quality_snapshot, load_quality_baseline

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_paths(tmp_path: Path) -> Any:
    paths = MagicMock()
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    proposals_dir = tmp_path / "proposals"
    proposals_dir.mkdir(parents=True, exist_ok=True)
    approval_state = tmp_path / "approval_state.json"
    approval_state.write_text(
        json.dumps({"approved_proposals": {}, "proposal_decisions": {}, "rejection_cooloff": {}})
    )

    paths.state = state_dir
    paths.autonomy_policy = config_dir / "agent_autonomy_policy.json"
    paths.autonomy_proposals = proposals_dir
    paths.approval_state = approval_state
    return paths


def _default_policy() -> dict[str, Any]:
    return {
        "null_rate_columns": ["message_body", "conversation_outcome", "channel", "timestamp"],
        "distribution_columns": ["conversation_outcome", "channel"],
        "null_rate_threshold_pp": 10.0,
        "record_count_drop_threshold_pct": 20.0,
        "distribution_shift_threshold_pp": 15.0,
    }


def _simple_df(n: int = 100, null_frac: float = 0.0) -> pd.DataFrame:
    nulls = int(n * null_frac)
    bodies = [None] * nulls + ["hello"] * (n - nulls)
    return pd.DataFrame(
        {
            "message_body": bodies,
            "conversation_outcome": ["vendido"] * n,
            "channel": ["whatsapp"] * n,
            "timestamp": ["2026-01-01"] * n,
        }
    )


# ---------------------------------------------------------------------------
# _quality_drift_proposals — no baseline
# ---------------------------------------------------------------------------


def test_no_baseline_returns_empty(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    df = _simple_df()
    contexts, proposals = _quality_drift_proposals(paths, "run_1", df, None, _default_policy())
    assert contexts == []
    assert proposals == []


# ---------------------------------------------------------------------------
# Record count drop
# ---------------------------------------------------------------------------


def test_record_count_drop_above_threshold(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    baseline = {"record_count": 100, "null_rates": {}, "distribution": {}}
    df = _simple_df(n=79)
    _, proposals = _quality_drift_proposals(paths, "run_1", df, baseline, _default_policy())
    assert len(proposals) == 1
    assert "record_count_drop" in proposals[0]["items"]


def test_record_count_drop_below_threshold(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    baseline = {"record_count": 100, "null_rates": {}, "distribution": {}}
    df = _simple_df(n=85)
    _, proposals = _quality_drift_proposals(paths, "run_1", df, baseline, _default_policy())
    assert proposals == []


def test_record_count_drop_baseline_zero(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    baseline = {"record_count": 0, "null_rates": {}, "distribution": {}}
    df = _simple_df(n=50)
    _, proposals = _quality_drift_proposals(paths, "run_1", df, baseline, _default_policy())
    assert proposals == []


# ---------------------------------------------------------------------------
# Null rate increase
# ---------------------------------------------------------------------------


def test_null_rate_increase_above_threshold(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    baseline = {"record_count": 100, "null_rates": {"message_body": 0.02}, "distribution": {}}
    df = _simple_df(n=100, null_frac=0.13)
    _, proposals = _quality_drift_proposals(paths, "run_1", df, baseline, _default_policy())
    assert len(proposals) == 1
    assert "message_body_null_rate_increase" in proposals[0]["items"]


def test_null_rate_increase_below_threshold(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    baseline = {"record_count": 100, "null_rates": {"message_body": 0.02}, "distribution": {}}
    df = _simple_df(n=100, null_frac=0.10)  # drift = 8 pp < 10
    _, proposals = _quality_drift_proposals(paths, "run_1", df, baseline, _default_policy())
    assert proposals == []


def test_null_rate_decrease_not_triggered(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    baseline = {"record_count": 100, "null_rates": {"message_body": 0.50}, "distribution": {}}
    df = _simple_df(n=100, null_frac=0.01)
    _, proposals = _quality_drift_proposals(paths, "run_1", df, baseline, _default_policy())
    assert proposals == []


# ---------------------------------------------------------------------------
# Distribution shift
# ---------------------------------------------------------------------------


def _df_with_outcome_dist(n: int, dist: dict[str, float]) -> pd.DataFrame:
    outcomes = []
    for value, frac in dist.items():
        outcomes.extend([value] * round(n * frac))
    outcomes = outcomes[:n] + [outcomes[-1]] * max(0, n - len(outcomes))
    return pd.DataFrame(
        {
            "message_body": ["hello"] * n,
            "conversation_outcome": outcomes,
            "channel": ["whatsapp"] * n,
            "timestamp": ["2026-01-01"] * n,
        }
    )


def test_distribution_shift_above_threshold(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    baseline = {
        "record_count": 100,
        "null_rates": {},
        "distribution": {"conversation_outcome": {"vendido": 0.80, "sem_retorno": 0.20}},
    }
    df = _df_with_outcome_dist(100, {"vendido": 0.60, "sem_retorno": 0.40})
    _, proposals = _quality_drift_proposals(paths, "run_1", df, baseline, _default_policy())
    assert len(proposals) == 1
    assert "conversation_outcome_distribution_shift" in proposals[0]["items"]


def test_distribution_shift_below_threshold(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    baseline = {
        "record_count": 100,
        "null_rates": {},
        "distribution": {"conversation_outcome": {"vendido": 0.80, "sem_retorno": 0.20}},
    }
    df = _df_with_outcome_dist(100, {"vendido": 0.87, "sem_retorno": 0.13})  # ~7 pp < 15
    _, proposals = _quality_drift_proposals(paths, "run_1", df, baseline, _default_policy())
    assert proposals == []


def test_distribution_new_value_above_threshold(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    baseline = {
        "record_count": 100,
        "null_rates": {},
        "distribution": {"conversation_outcome": {"vendido": 0.80, "sem_retorno": 0.20}},
    }
    df = _df_with_outcome_dist(100, {"vendido": 0.60, "sem_retorno": 0.20, "sem_interesse": 0.20})
    _, proposals = _quality_drift_proposals(paths, "run_1", df, baseline, _default_policy())
    assert len(proposals) == 1
    assert "conversation_outcome_distribution_shift" in proposals[0]["items"]


def test_distribution_vanished_value_above_threshold(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    baseline = {
        "record_count": 100,
        "null_rates": {},
        "distribution": {
            "conversation_outcome": {"vendido": 0.60, "sem_retorno": 0.20, "sem_interesse": 0.20}
        },
    }
    df = _df_with_outcome_dist(100, {"vendido": 0.80, "sem_retorno": 0.20})
    _, proposals = _quality_drift_proposals(paths, "run_1", df, baseline, _default_policy())
    assert len(proposals) == 1
    assert "conversation_outcome_distribution_shift" in proposals[0]["items"]


# ---------------------------------------------------------------------------
# Missing columns
# ---------------------------------------------------------------------------


def test_missing_column_skipped(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    policy = _default_policy()
    policy["null_rate_columns"] = ["nonexistent_col"]
    policy["distribution_columns"] = ["nonexistent_col"]
    baseline = {"record_count": 100, "null_rates": {}, "distribution": {}}
    df = pd.DataFrame({"message_body": ["hello"] * 100})
    contexts, proposals = _quality_drift_proposals(paths, "run_1", df, baseline, policy)
    assert contexts == []
    assert proposals == []


# ---------------------------------------------------------------------------
# Multiple triggers — single proposal
# ---------------------------------------------------------------------------


def test_multiple_triggers_single_proposal(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    baseline = {
        "record_count": 100,
        "null_rates": {"message_body": 0.02},
        "distribution": {"conversation_outcome": {"vendido": 0.80, "sem_retorno": 0.20}},
    }
    df = _df_with_outcome_dist(79, {"vendido": 0.50, "sem_retorno": 0.50})
    df["message_body"] = [None] * 20 + ["hello"] * 59
    _, proposals = _quality_drift_proposals(paths, "run_1", df, baseline, _default_policy())
    assert len(proposals) == 1
    assert len(proposals[0]["items"]) >= 2


# ---------------------------------------------------------------------------
# proposed_change must not contain metric values
# ---------------------------------------------------------------------------


def test_proposed_change_has_no_metric_values(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    baseline = {"record_count": 100, "null_rates": {"message_body": 0.02}, "distribution": {}}
    df = _simple_df(n=100, null_frac=0.13)
    _, proposals = _quality_drift_proposals(paths, "run_1", df, baseline, _default_policy())
    assert len(proposals) == 1
    pc = proposals[0]["proposed_change"]
    assert set(pc.keys()) == {"target_path", "operation", "drift_triggers"}


# ---------------------------------------------------------------------------
# items is sorted
# ---------------------------------------------------------------------------


def test_items_is_sorted(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    baseline = {
        "record_count": 100,
        "null_rates": {"message_body": 0.02},
        "distribution": {"conversation_outcome": {"vendido": 0.80, "sem_retorno": 0.20}},
    }
    df = _df_with_outcome_dist(79, {"vendido": 0.50, "sem_retorno": 0.50})
    df["message_body"] = [None] * 20 + ["hello"] * 59
    _, proposals = _quality_drift_proposals(paths, "run_1", df, baseline, _default_policy())
    assert len(proposals) == 1
    items = proposals[0]["items"]
    assert items == sorted(items)


# ---------------------------------------------------------------------------
# Fingerprint stability
# ---------------------------------------------------------------------------


def test_proposal_fingerprint_stable_same_triggers(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    baseline = {"record_count": 100, "null_rates": {"message_body": 0.02}, "distribution": {}}
    df = _simple_df(n=100, null_frac=0.13)
    _, proposals1 = _quality_drift_proposals(paths, "run_1", df, baseline, _default_policy())
    _, proposals2 = _quality_drift_proposals(paths, "run_2", df, baseline, _default_policy())
    assert proposals1[0]["proposal_id"] == proposals2[0]["proposal_id"]


def test_proposal_fingerprint_differs_different_triggers(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    baseline = {"record_count": 100, "null_rates": {"message_body": 0.02}, "distribution": {}}
    df_drop = _simple_df(n=79)
    df_null = _simple_df(n=100, null_frac=0.13)
    _, proposals_drop = _quality_drift_proposals(
        paths, "run_1", df_drop, baseline, _default_policy()
    )
    _, proposals_null = _quality_drift_proposals(
        paths, "run_1", df_null, baseline, _default_policy()
    )
    assert proposals_drop[0]["proposal_id"] != proposals_null[0]["proposal_id"]


# ---------------------------------------------------------------------------
# compute_quality_snapshot
# ---------------------------------------------------------------------------


def test_compute_quality_snapshot_returns_null_rates() -> None:
    df = pd.DataFrame({"message_body": [None, None, "hi", "hi", "hi"]})
    snap = compute_quality_snapshot(df, ["message_body"], [])
    assert snap["null_rates"]["message_body"] == pytest.approx(0.4, abs=1e-5)


def test_compute_quality_snapshot_returns_distribution() -> None:
    df = pd.DataFrame({"conversation_outcome": ["vendido", "vendido", "sem_retorno"]})
    snap = compute_quality_snapshot(df, [], ["conversation_outcome"])
    dist = snap["distribution"]["conversation_outcome"]
    assert "vendido" in dist
    assert dist["vendido"] == pytest.approx(2 / 3, abs=1e-4)


def test_compute_quality_snapshot_skips_missing_columns() -> None:
    df = pd.DataFrame({"other_col": [1, 2, 3]})
    snap = compute_quality_snapshot(df, ["message_body"], ["conversation_outcome"])
    assert snap["null_rates"] == {}
    assert snap["distribution"] == {}


# ---------------------------------------------------------------------------
# load_quality_baseline
# ---------------------------------------------------------------------------


def test_load_quality_baseline_none_when_absent(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    result = load_quality_baseline(paths)
    assert result is None


def test_load_quality_baseline_returns_stored(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    stored = {"record_count": 42, "null_rates": {}, "distribution": {}}
    state_path = paths.state / "pipeline_state.json"
    state_path.write_text(json.dumps({"runs": [], "quality_baseline": stored}))
    result = load_quality_baseline(paths)
    assert result == stored


# ---------------------------------------------------------------------------
# Baseline written after plan_pipeline_spec
# ---------------------------------------------------------------------------


def _make_full_paths(tmp_path: Path) -> Any:
    from pipeline.config import build_paths

    (tmp_path / "docs").mkdir(parents=True, exist_ok=True)
    # bronze parquet will be written by the test
    return build_paths(tmp_path)


def _write_minimal_spec(paths: Any) -> None:
    from pipeline.runtime.spec import default_pipeline_spec

    spec = default_pipeline_spec()
    paths.pipeline_spec.write_text(json.dumps(spec))


def _full_bronze_df(n: int) -> pd.DataFrame:
    df = _simple_df(n=n)
    df["metadata"] = json.dumps({"device": "android"})
    return df


def test_baseline_written_after_plan(tmp_path: Path) -> None:
    from pipeline.agent.planner import plan_pipeline_spec

    paths = _make_full_paths(tmp_path)
    df = _full_bronze_df(n=50)
    df.to_parquet(paths.raw_bronze_source)

    with (
        patch(
            "pipeline.agent.planner.get_llm_advice",
            return_value={"priority_proposals": [], "deferred_proposals": []},
        ),
        patch(
            "pipeline.agent.planner.agent_self_review_proposal",
            return_value={"should_approve": False, "confidence": 0.0},
        ),
    ):
        plan_pipeline_spec(paths)

    state_path = paths.state / "pipeline_state.json"
    state = json.loads(state_path.read_text())
    assert "quality_baseline" in state
    assert state["quality_baseline"]["record_count"] == 50


def test_baseline_written_even_when_no_drift(tmp_path: Path) -> None:
    from pipeline.agent.planner import plan_pipeline_spec

    paths = _make_full_paths(tmp_path)
    df = _full_bronze_df(n=100)
    df.to_parquet(paths.raw_bronze_source)

    with (
        patch(
            "pipeline.agent.planner.get_llm_advice",
            return_value={"priority_proposals": [], "deferred_proposals": []},
        ),
        patch(
            "pipeline.agent.planner.agent_self_review_proposal",
            return_value={"should_approve": False, "confidence": 0.0},
        ),
    ):
        plan_pipeline_spec(paths)

    state_path = paths.state / "pipeline_state.json"
    state = json.loads(state_path.read_text())
    assert "quality_baseline" in state


# ---------------------------------------------------------------------------
# apply_proposal_to_spec
# ---------------------------------------------------------------------------


def test_apply_proposal_to_spec_adds_drift_log() -> None:
    spec = {
        "bronze": {"required_columns": []},
        "silver": {"metadata_fields": []},
        "gold": {"required_columns": [], "valid_intent_stages": []},
        "quality": {"validation_rules": {"silver": []}},
    }
    proposal = {
        "proposal_type": "data_quality_drift_detected",
        "items": ["record_count_drop"],
        "created_at_utc": "2026-04-29T00:00:00+00:00",
    }
    updated, changed = apply_proposal_to_spec(spec, proposal)
    assert changed is True
    assert len(updated["quality"]["drift_log"]) == 1
    assert updated["quality"]["drift_log"][0]["drift_triggers"] == ["record_count_drop"]


# ---------------------------------------------------------------------------
# get_quality_drift_policy
# ---------------------------------------------------------------------------


def test_get_quality_drift_policy_defaults(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    policy = get_quality_drift_policy(paths)
    assert policy["null_rate_threshold_pp"] == 10.0
    assert policy["record_count_drop_threshold_pct"] == 20.0
    assert policy["distribution_shift_threshold_pp"] == 15.0
    assert "message_body" in policy["null_rate_columns"]
    assert "conversation_outcome" in policy["distribution_columns"]


def test_get_quality_drift_policy_custom(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    custom = {
        "quality_drift_record_count_drop_threshold_pct": 10.0,
        "quality_drift_null_rate_threshold_pp": 5.0,
        "quality_drift_distribution_shift_threshold_pp": 10.0,
        "mutation_families": {},
    }
    paths.autonomy_policy.write_text(json.dumps(custom))
    policy = get_quality_drift_policy(paths)
    assert policy["record_count_drop_threshold_pct"] == 10.0
    assert policy["null_rate_threshold_pp"] == 5.0
    assert policy["distribution_shift_threshold_pp"] == 10.0


# ---------------------------------------------------------------------------
# Family classification
# ---------------------------------------------------------------------------


def test_data_quality_drift_family_classified_high(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    result = classify_proposal("data_quality_drift", paths)
    assert result["impact_class"] == "high"


def test_data_quality_drift_requires_approval(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    result = classify_proposal("data_quality_drift", paths)
    assert result["requires_approval"] is True


# ---------------------------------------------------------------------------
# AC-009: no triggers → empty lists
# ---------------------------------------------------------------------------


def test_no_triggers_returns_empty_lists(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    baseline = {
        "record_count": 100,
        "null_rates": {"message_body": 0.0},
        "distribution": {"conversation_outcome": {"vendido": 1.0}},
    }
    df = _simple_df(n=100, null_frac=0.0)
    contexts, proposals = _quality_drift_proposals(paths, "run_1", df, baseline, _default_policy())
    assert contexts == []
    assert proposals == []
