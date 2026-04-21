from __future__ import annotations

from typing import Any

from pipeline.config import PipelinePaths
from pipeline.io import read_json, write_json


def load_approval_state(paths: PipelinePaths) -> dict[str, Any]:
    return read_json(paths.approval_state, default={"approved_proposals": {}})


def is_proposal_approved(paths: PipelinePaths, proposal_id: str) -> bool:
    state = load_approval_state(paths)
    approved = state.get("approved_proposals", {})
    return bool(approved.get(proposal_id))


def approve_proposal(paths: PipelinePaths, proposal_id: str, approved_by: str) -> None:
    state = load_approval_state(paths)
    approved = state.setdefault("approved_proposals", {})
    approved[proposal_id] = {"approved": True, "approved_by": approved_by}
    write_json(state, paths.approval_state)
