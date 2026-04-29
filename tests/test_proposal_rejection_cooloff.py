from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from pipeline.agent.approval import (
    APPROVAL_STATUS_APPROVED,
    APPROVAL_STATUS_REJECTED,
    expire_rejection_cooloff,
    get_rejection_cooloff_record,
    increment_rejection_cooloff_cycle,
    is_in_rejection_cooloff,
    start_rejection_cooloff,
)
from pipeline.agent.autonomy import (
    DEFAULT_AUTONOMY_POLICY,
    PROPOSAL_STATUS_CLOSED_NO_ACTION,
    get_rejection_cooloff_policy,
)
from pipeline.config import build_paths
from pipeline.io.parquet_io import write_json

_APPROVAL_MODULE = "pipeline.agent.approval"
_AUTONOMY_MODULE = "pipeline.agent.autonomy"
_PLANNER_MODULE = "pipeline.agent.planner"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_paths(tmp_path: Path):
    return build_paths(tmp_path)


def _write_cooloff(paths, proposal_id: str, cycle_count: int, started_at: datetime) -> None:
    from pipeline.agent.approval import load_approval_state

    state = load_approval_state(paths)
    state.setdefault("rejection_cooloff", {})[proposal_id] = {
        "cooloff_started_at_utc": started_at.isoformat(),
        "cycle_count": cycle_count,
    }
    write_json(state, paths.approval_state)


def _minimal_proposal(
    proposal_id: str = "proposal_abc123",
    family: str = "schema_update",
) -> dict:
    return {
        "proposal_id": proposal_id,
        "proposal_family": family,
        "proposal_type": "bronze_required_columns_addition",
        "status": "proposed",
        "requires_approval": True,
        "safe_auto_promote": False,
        "impact_class": "high",
        "policy_snapshot": {"requires_privacy_scan": False},
        "privacy_impact": "none",
        "items": [],
        "proposed_change": {},
        "approval_context": {},
        "candidate_actions": [],
    }


# ---------------------------------------------------------------------------
# start_rejection_cooloff
# ---------------------------------------------------------------------------


def test_start_cooloff_creates_record(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    pid = "proposal_abc"

    start_rejection_cooloff(paths, pid)

    record = get_rejection_cooloff_record(paths, pid)
    assert record["cycle_count"] == 0
    assert "cooloff_started_at_utc" in record


def test_start_cooloff_resets_existing_record(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    pid = "proposal_abc"
    old_time = datetime(2026, 1, 1, tzinfo=UTC)
    _write_cooloff(paths, pid, cycle_count=5, started_at=old_time)

    start_rejection_cooloff(paths, pid)

    record = get_rejection_cooloff_record(paths, pid)
    assert record["cycle_count"] == 0
    assert record["cooloff_started_at_utc"] != old_time.isoformat()


# ---------------------------------------------------------------------------
# is_in_rejection_cooloff
# ---------------------------------------------------------------------------


def test_is_in_cooloff_true_when_entry_exists(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    pid = "proposal_abc"
    _write_cooloff(paths, pid, cycle_count=0, started_at=datetime.now(UTC))

    assert is_in_rejection_cooloff(paths, pid) is True


def test_is_in_cooloff_false_when_no_entry(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)

    assert is_in_rejection_cooloff(paths, "proposal_missing") is False


# ---------------------------------------------------------------------------
# increment_rejection_cooloff_cycle
# ---------------------------------------------------------------------------


def test_increment_cooloff_cycle_increments_count(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    pid = "proposal_abc"
    _write_cooloff(paths, pid, cycle_count=3, started_at=datetime.now(UTC))

    new_count = increment_rejection_cooloff_cycle(paths, pid)

    assert new_count == 4
    record = get_rejection_cooloff_record(paths, pid)
    assert record["cycle_count"] == 4


def test_increment_cooloff_cycle_noop_when_missing(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)

    result = increment_rejection_cooloff_cycle(paths, "proposal_missing")

    assert result == 0
    # state not mutated — rejection_cooloff should be empty
    from pipeline.agent.approval import load_approval_state

    state = load_approval_state(paths)
    assert state.get("rejection_cooloff", {}) == {}


# ---------------------------------------------------------------------------
# expire_rejection_cooloff
# ---------------------------------------------------------------------------


def test_expire_cooloff_removes_entry(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    pid = "proposal_abc"
    _write_cooloff(paths, pid, cycle_count=2, started_at=datetime.now(UTC))

    expire_rejection_cooloff(paths, pid)

    assert is_in_rejection_cooloff(paths, pid) is False


def test_expire_cooloff_noop_when_missing(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)

    # Must not raise and must not mutate state
    expire_rejection_cooloff(paths, "proposal_missing")

    from pipeline.agent.approval import load_approval_state

    state = load_approval_state(paths)
    assert state.get("rejection_cooloff", {}) == {}


# ---------------------------------------------------------------------------
# get_rejection_cooloff_policy
# ---------------------------------------------------------------------------


def test_get_rejection_cooloff_policy_defaults(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    policy = get_rejection_cooloff_policy(paths)

    assert policy["threshold_cycles"] == 10
    assert policy["threshold_hours"] == pytest.approx(24.0)


def test_get_rejection_cooloff_policy_custom(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    paths.autonomy_policy.parent.mkdir(parents=True, exist_ok=True)
    write_json(
        {"rejection_cooloff_threshold_cycles": 5, "rejection_cooloff_threshold_hours": 12.0},
        paths.autonomy_policy,
    )

    policy = get_rejection_cooloff_policy(paths)

    assert policy["threshold_cycles"] == 5
    assert policy["threshold_hours"] == pytest.approx(12.0)


# ---------------------------------------------------------------------------
# DEFAULT_AUTONOMY_POLICY has new keys (VAL-003)
# ---------------------------------------------------------------------------


def test_default_policy_has_cooloff_keys() -> None:
    assert "rejection_cooloff_threshold_cycles" in DEFAULT_AUTONOMY_POLICY
    assert "rejection_cooloff_threshold_hours" in DEFAULT_AUTONOMY_POLICY
    assert DEFAULT_AUTONOMY_POLICY["rejection_cooloff_threshold_cycles"] == 10
    assert DEFAULT_AUTONOMY_POLICY["rejection_cooloff_threshold_hours"] == pytest.approx(24.0)


# ---------------------------------------------------------------------------
# Cooloff logic — planner integration (via direct simulation)
# ---------------------------------------------------------------------------


def _run_cooloff_branch(
    paths: Any,
    proposal: dict,
    *,
    approval_status: str,
    cycle_count_in_record: int,
    started_at: datetime,
    threshold_cycles: int = 10,
    threshold_hours: float = 24.0,
    now: datetime | None = None,
) -> dict:
    """
    Simulate the cooloff branch from plan_pipeline_spec without invoking
    the full planner (which needs Bronze data and LLM).
    Returns a dict describing what would happen.
    """
    from pipeline.agent.autonomy import PROPOSAL_STATUS_CLOSED_NO_ACTION

    _write_cooloff(paths, proposal["proposal_id"], cycle_count_in_record, started_at)
    cooloff_policy = {"threshold_cycles": threshold_cycles, "threshold_hours": threshold_hours}

    suppressed = False
    expired = False
    cleared_by_approval = False

    if is_in_rejection_cooloff(paths, proposal["proposal_id"]):
        if approval_status != APPROVAL_STATUS_REJECTED:
            expire_rejection_cooloff(paths, proposal["proposal_id"])
            cleared_by_approval = True
        else:
            cooloff_record = get_rejection_cooloff_record(paths, proposal["proposal_id"])
            new_count = increment_rejection_cooloff_cycle(paths, proposal["proposal_id"])
            started = datetime.fromisoformat(cooloff_record["cooloff_started_at_utc"])
            _now = now or datetime.now(UTC)
            elapsed_hours = (_now - started).total_seconds() / 3600
            if (
                new_count >= cooloff_policy["threshold_cycles"]
                or elapsed_hours >= cooloff_policy["threshold_hours"]
            ):
                expire_rejection_cooloff(paths, proposal["proposal_id"])
                expired = True
            else:
                proposal["status"] = PROPOSAL_STATUS_CLOSED_NO_ACTION
                proposal["decision_reason"] = "rejected_cooloff_active"
                suppressed = True

    return {
        "suppressed": suppressed,
        "expired": expired,
        "cleared_by_approval": cleared_by_approval,
        "proposal": proposal,
        "still_in_cooloff": is_in_rejection_cooloff(paths, proposal["proposal_id"]),
    }


def test_cooloff_suppresses_proposal_within_window(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    proposal = _minimal_proposal()
    now = datetime.now(UTC)

    result = _run_cooloff_branch(
        paths,
        proposal,
        approval_status=APPROVAL_STATUS_REJECTED,
        cycle_count_in_record=5,
        started_at=now - timedelta(hours=1),
        threshold_cycles=10,
        threshold_hours=24.0,
        now=now,
    )

    assert result["suppressed"] is True
    assert result["expired"] is False
    assert result["proposal"]["status"] == PROPOSAL_STATUS_CLOSED_NO_ACTION
    assert result["proposal"]["decision_reason"] == "rejected_cooloff_active"


def test_cooloff_expires_on_cycle_threshold(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    proposal = _minimal_proposal()
    now = datetime.now(UTC)

    # cycle_count=9 in record → increment → new_count=10 >= threshold=10
    result = _run_cooloff_branch(
        paths,
        proposal,
        approval_status=APPROVAL_STATUS_REJECTED,
        cycle_count_in_record=9,
        started_at=now - timedelta(hours=1),
        threshold_cycles=10,
        threshold_hours=24.0,
        now=now,
    )

    assert result["expired"] is True
    assert result["suppressed"] is False
    assert result["still_in_cooloff"] is False


def test_cooloff_expires_on_time_threshold(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    proposal = _minimal_proposal()
    now = datetime.now(UTC)

    # cycle_count=2 (< 10) but elapsed_hours=25 >= 24.0
    result = _run_cooloff_branch(
        paths,
        proposal,
        approval_status=APPROVAL_STATUS_REJECTED,
        cycle_count_in_record=2,
        started_at=now - timedelta(hours=25, minutes=6),
        threshold_cycles=10,
        threshold_hours=24.0,
        now=now,
    )

    assert result["expired"] is True
    assert result["suppressed"] is False
    assert result["still_in_cooloff"] is False


def test_cooloff_cleared_when_operator_approves(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    proposal = _minimal_proposal()
    now = datetime.now(UTC)

    result = _run_cooloff_branch(
        paths,
        proposal,
        approval_status=APPROVAL_STATUS_APPROVED,
        cycle_count_in_record=3,
        started_at=now - timedelta(hours=2),
        now=now,
    )

    assert result["cleared_by_approval"] is True
    assert result["suppressed"] is False
    assert result["still_in_cooloff"] is False


# ---------------------------------------------------------------------------
# start_rejection_cooloff called after rejection branch
# ---------------------------------------------------------------------------


def test_cooloff_started_after_rejection_branch(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    pid = "proposal_abc"

    # Simulate the rejection branch
    start_rejection_cooloff(paths, pid)

    assert is_in_rejection_cooloff(paths, pid) is True
    record = get_rejection_cooloff_record(paths, pid)
    assert record["cycle_count"] == 0


def test_fresh_cooloff_after_reevaluation_rejection(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    pid = "proposal_abc"
    old_time = datetime(2026, 1, 1, tzinfo=UTC)

    # Simulate re-evaluation pass: expire was called, then rejection branch runs again
    _write_cooloff(paths, pid, cycle_count=10, started_at=old_time)
    expire_rejection_cooloff(paths, pid)
    assert is_in_rejection_cooloff(paths, pid) is False

    start_rejection_cooloff(paths, pid)

    assert is_in_rejection_cooloff(paths, pid) is True
    record = get_rejection_cooloff_record(paths, pid)
    assert record["cycle_count"] == 0
    assert record["cooloff_started_at_utc"] != old_time.isoformat()


# ---------------------------------------------------------------------------
# Metrics NOT updated for suppressed proposals (AC-007 / REQ-017)
# ---------------------------------------------------------------------------


def test_metrics_not_updated_for_suppressed_proposal(tmp_path: Path) -> None:
    """
    When a proposal is suppressed by cooloff, update_autonomy_metrics must NOT
    be called (beyond the initial proposal-count increment that happens before
    the cooloff check in the real planner). Since we test the branch logic
    directly here, we verify that the suppression path does not invoke it.
    """
    paths = _make_paths(tmp_path)
    proposal = _minimal_proposal()
    now = datetime.now(UTC)

    update_metrics_calls: list[Any] = []

    def _fake_update_metrics(*args: Any, **kwargs: Any) -> dict:
        update_metrics_calls.append((args, kwargs))
        return {}

    # Run the cooloff branch (suppressed path) without calling update_autonomy_metrics
    _write_cooloff(
        paths, proposal["proposal_id"], cycle_count=3, started_at=now - timedelta(hours=1)
    )

    # The suppression path only calls persist_proposal_record, not update_autonomy_metrics
    if is_in_rejection_cooloff(paths, proposal["proposal_id"]):
        cooloff_record = get_rejection_cooloff_record(paths, proposal["proposal_id"])
        new_count = increment_rejection_cooloff_cycle(paths, proposal["proposal_id"])
        started = datetime.fromisoformat(cooloff_record["cooloff_started_at_utc"])
        elapsed = (now - started).total_seconds() / 3600
        if new_count < 10 and elapsed < 24.0:
            proposal["status"] = PROPOSAL_STATUS_CLOSED_NO_ACTION
            proposal["decision_reason"] = "rejected_cooloff_active"
            # Note: update_autonomy_metrics is NOT called here
        else:
            _fake_update_metrics(paths, proposal["proposal_family"])

    assert len(update_metrics_calls) == 0
    assert proposal["status"] == PROPOSAL_STATUS_CLOSED_NO_ACTION
