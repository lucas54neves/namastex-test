from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pipeline.config import PipelinePaths
from pipeline.io.parquet_io import read_json, write_json

APPROVAL_STATUS_APPROVED = "approved"
APPROVAL_STATUS_REJECTED = "rejected"
APPROVAL_STATUS_PENDING = "pending"


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def load_approval_state(paths: PipelinePaths) -> dict[str, Any]:
    return read_json(
        paths.approval_state,
        default={"approved_proposals": {}, "proposal_decisions": {}, "rejection_cooloff": {}},
    )


def get_proposal_approval_status(paths: PipelinePaths, proposal_id: str) -> str:
    state = load_approval_state(paths)
    decisions = state.get("proposal_decisions", {})
    decision = decisions.get(proposal_id)
    if not isinstance(decision, dict):
        approved = state.get("approved_proposals", {})
        decision = approved.get(proposal_id)
    if not isinstance(decision, dict):
        return APPROVAL_STATUS_PENDING
    if bool(decision.get("approved")):
        return APPROVAL_STATUS_APPROVED
    if decision.get("approved") is False:
        return APPROVAL_STATUS_REJECTED
    return APPROVAL_STATUS_PENDING


def is_proposal_approved(paths: PipelinePaths, proposal_id: str) -> bool:
    return get_proposal_approval_status(paths, proposal_id) == APPROVAL_STATUS_APPROVED


def get_proposal_approval_record(paths: PipelinePaths, proposal_id: str) -> dict[str, Any]:
    state = load_approval_state(paths)
    decisions = state.get("proposal_decisions", {})
    decision = decisions.get(proposal_id)
    if isinstance(decision, dict):
        return decision
    approved = state.get("approved_proposals", {})
    legacy = approved.get(proposal_id)
    if isinstance(legacy, dict):
        return legacy
    return {}


def approve_proposal(paths: PipelinePaths, proposal_id: str, approved_by: str) -> None:
    state = load_approval_state(paths)
    approved = state.setdefault("approved_proposals", {})
    approved[proposal_id] = {"approved": True, "approved_by": approved_by}
    decisions = state.setdefault("proposal_decisions", {})
    decisions[proposal_id] = {
        "approved": True,
        "approved_by": approved_by,
        "status": APPROVAL_STATUS_APPROVED,
        "decided_at_utc": _utc_now_iso(),
    }
    write_json(state, paths.approval_state)


def start_rejection_cooloff(paths: PipelinePaths, proposal_id: str) -> None:
    state = load_approval_state(paths)
    state.setdefault("rejection_cooloff", {})[proposal_id] = {
        "cooloff_started_at_utc": _utc_now_iso(),
        "cycle_count": 0,
    }
    write_json(state, paths.approval_state)


def get_rejection_cooloff_record(paths: PipelinePaths, proposal_id: str) -> dict[str, Any]:
    state = load_approval_state(paths)
    return dict(state.get("rejection_cooloff", {}).get(proposal_id, {}))


def is_in_rejection_cooloff(paths: PipelinePaths, proposal_id: str) -> bool:
    state = load_approval_state(paths)
    return proposal_id in state.get("rejection_cooloff", {})


def increment_rejection_cooloff_cycle(paths: PipelinePaths, proposal_id: str) -> int:
    state = load_approval_state(paths)
    cooloff = state.get("rejection_cooloff", {})
    if proposal_id not in cooloff:
        return 0
    cooloff[proposal_id]["cycle_count"] += 1
    write_json(state, paths.approval_state)
    return int(cooloff[proposal_id]["cycle_count"])


def expire_rejection_cooloff(paths: PipelinePaths, proposal_id: str) -> None:
    state = load_approval_state(paths)
    cooloff = state.get("rejection_cooloff", {})
    if proposal_id in cooloff:
        del cooloff[proposal_id]
        write_json(state, paths.approval_state)


def reject_proposal(paths: PipelinePaths, proposal_id: str, rejected_by: str) -> None:
    state = load_approval_state(paths)
    approved = state.setdefault("approved_proposals", {})
    approved[proposal_id] = {"approved": False, "approved_by": rejected_by}
    decisions = state.setdefault("proposal_decisions", {})
    decisions[proposal_id] = {
        "approved": False,
        "approved_by": rejected_by,
        "status": APPROVAL_STATUS_REJECTED,
        "decided_at_utc": _utc_now_iso(),
    }
    write_json(state, paths.approval_state)
