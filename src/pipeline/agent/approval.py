from __future__ import annotations

from typing import Any

from pipeline.config import PipelinePaths
from pipeline.io.parquet_io import read_json, write_json

APPROVAL_STATUS_APPROVED = "approved"
APPROVAL_STATUS_REJECTED = "rejected"
APPROVAL_STATUS_PENDING = "pending"


def load_approval_state(paths: PipelinePaths) -> dict[str, Any]:
    return read_json(paths.approval_state, default={"approved_proposals": {}})


def get_proposal_approval_status(paths: PipelinePaths, proposal_id: str) -> str:
    state = load_approval_state(paths)
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


def approve_proposal(paths: PipelinePaths, proposal_id: str, approved_by: str) -> None:
    state = load_approval_state(paths)
    approved = state.setdefault("approved_proposals", {})
    approved[proposal_id] = {"approved": True, "approved_by": approved_by}
    write_json(state, paths.approval_state)


def reject_proposal(paths: PipelinePaths, proposal_id: str, rejected_by: str) -> None:
    state = load_approval_state(paths)
    approved = state.setdefault("approved_proposals", {})
    approved[proposal_id] = {"approved": False, "approved_by": rejected_by}
    write_json(state, paths.approval_state)
