from __future__ import annotations

from pipeline.approval import approve_proposal, is_proposal_approved
from pipeline.config import build_paths


def test_approve_proposal_persists_state(tmp_path) -> None:
    paths = build_paths(tmp_path)
    approve_proposal(paths, "proposal_x", "tester")

    assert is_proposal_approved(paths, "proposal_x") is True
