from __future__ import annotations

from pipeline.approval import (
    APPROVAL_STATUS_REJECTED,
    approve_proposal,
    get_proposal_approval_status,
    is_proposal_approved,
    reject_proposal,
)
from pipeline.config import build_paths


def test_approve_proposal_persists_state(tmp_path) -> None:
    paths = build_paths(tmp_path)
    approve_proposal(paths, "proposal_x", "tester")

    assert is_proposal_approved(paths, "proposal_x") is True


def test_reject_proposal_persists_status(tmp_path) -> None:
    paths = build_paths(tmp_path)
    reject_proposal(paths, "proposal_x", "reviewer")

    assert is_proposal_approved(paths, "proposal_x") is False
    assert get_proposal_approval_status(paths, "proposal_x") == APPROVAL_STATUS_REJECTED
