from __future__ import annotations

import json
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from pipeline.agent.approval import approve_proposal
from pipeline.agent.autonomy import get_agent_auto_approve_threshold
from pipeline.agent.planner import plan_pipeline_spec
from pipeline.config import build_paths
from pipeline.orchestration.operator import run_cycle


def _write_bronze(root: Path, rows: list[dict[str, object]]) -> None:
    (root / "docs").mkdir()
    pd.DataFrame(rows).to_parquet(root / "docs" / "conversations_bronze.parquet", index=False)


def _base_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "message_id": "m1",
        "conversation_id": "conv_1",
        "timestamp": "2026-02-01 10:00:00",
        "direction": "outbound",
        "sender_phone": "+5511991111111",
        "sender_name": "Diego Pereira",
        "message_type": "text",
        "message_body": "Oi, tenho interesse em cotacao de seguro",
        "status": "delivered",
        "channel": "whatsapp",
        "campaign_id": "camp_1",
        "agent_id": "agent_1",
        "conversation_outcome": "em_negociacao",
        "metadata": json.dumps(
            {
                "device": "android",
                "city": "Sao Paulo",
                "state": "SP",
                "response_time_sec": 10,
                "is_business_hours": True,
                "lead_source": "google_ads",
                "score_band": "alto",
            }
        ),
    }
    row.update(overrides)
    return row


def test_planner_emits_structured_schema_update_for_new_metadata_field(tmp_path: Path) -> None:
    root = tmp_path
    _write_bronze(
        root,
        [
            _base_row(
                metadata=json.dumps(
                    {
                        "device": "android",
                        "city": "Sao Paulo",
                        "state": "SP",
                        "response_time_sec": 10,
                        "lead_source": "google_ads",
                        "score_band": "alto",
                    }
                )
            )
        ],
    )

    paths = build_paths(root)
    report = plan_pipeline_spec(paths)

    proposal = next(
        item
        for item in report["proposals"]
        if item["proposal_type"] == "silver_metadata_fields_addition"
    )
    assert proposal["proposal_family"] == "schema_update"
    assert proposal["context_detected"]["evidence"]["items"] == ["score_band"]
    assert proposal["impact_class"] == "high"
    assert proposal["safe_auto_promote"] is False
    assert proposal["candidate_actions"]
    assert proposal["safe_auto_apply"] is False
    assert proposal["recommendation_only"] is True
    latest_report = json.loads(
        Path(paths.monitoring / "latest_plan_report.json").read_text(encoding="utf-8")
    )
    assert latest_report["proposal_id"] == report["proposal_id"]
    latest_structural = next(
        item
        for item in latest_report["proposals"]
        if item["proposal_type"] == "silver_metadata_fields_addition"
    )
    assert latest_structural["status"] == "awaiting_approval"


def test_planner_keeps_detected_contexts_when_no_structural_proposal_exists(tmp_path: Path) -> None:
    root = tmp_path
    _write_bronze(
        root,
        [
            _base_row(
                conversation_outcome="fechado",
                metadata=json.dumps(
                    {
                        "device": "android",
                        "city": "Sao Paulo",
                        "state": "SP",
                        "response_time_sec": 10,
                        "lead_source": "google_ads",
                    }
                ),
            )
        ],
    )

    paths = build_paths(root)
    report = plan_pipeline_spec(paths)

    assert report["proposals"] == []
    assert report["detected_contexts"]
    assert report["requires_approval"] is False
    assert report["applied"] is False


def test_planner_emits_new_structured_proposal_families(tmp_path: Path) -> None:
    root = tmp_path
    _write_bronze(
        root,
        [
            _base_row(
                metadata=json.dumps(
                    {
                        "device": "android",
                        "city": "Sao Paulo",
                        "state": "SP",
                        "response_time_sec": 10,
                        "is_business_hours": "true",
                        "leadSource": "google_ads",
                    }
                ),
            ),
            _base_row(
                message_id="m2",
                conversation_id="conv_2",
                metadata=json.dumps(
                    {
                        "device": "ios",
                        "city": "Campinas",
                        "state": "SP",
                        "response_time_sec": 22,
                        "is_business_hours": "false",
                        "leadSource": "referral",
                    }
                ),
            ),
        ],
    )

    paths = build_paths(root)
    report = plan_pipeline_spec(paths)

    families = {proposal["proposal_family"] for proposal in report["proposals"]}
    assert "validation_enhancement" in families
    assert "segmentation_adjustment" in families
    assert "transformation_rule_change" in families

    validation = next(
        item for item in report["proposals"] if item["proposal_family"] == "validation_enhancement"
    )
    assert validation["requires_approval"] is False
    assert validation["safe_auto_apply"] is True
    assert validation["status"] == "promoted"


def test_planner_does_not_apply_structural_changes_without_explicit_approval(
    tmp_path: Path,
) -> None:
    root = tmp_path
    _write_bronze(
        root,
        [
            _base_row(
                metadata=json.dumps(
                    {
                        "device": "android",
                        "city": "Sao Paulo",
                        "state": "SP",
                        "response_time_sec": 10,
                        "lead_source": "google_ads",
                        "score_band": "alto",
                    }
                )
            )
        ],
    )

    paths = build_paths(root)
    report = plan_pipeline_spec(paths)

    proposal = next(
        item
        for item in report["proposals"]
        if item["proposal_type"] == "silver_metadata_fields_addition"
    )

    assert report["applied"] is False
    assert proposal["status"] == "awaiting_approval"
    assert proposal["proposal_id"].startswith("proposal_")
    assert paths.pipeline_spec.exists() is False


def test_planner_applies_supported_structural_change_after_explicit_approval(
    tmp_path: Path,
) -> None:
    root = tmp_path
    _write_bronze(
        root,
        [
            _base_row(
                metadata=json.dumps(
                    {
                        "device": "android",
                        "city": "Sao Paulo",
                        "state": "SP",
                        "response_time_sec": 10,
                        "lead_source": "google_ads",
                        "score_band": "alto",
                    }
                )
            )
        ],
    )

    paths = build_paths(root)
    first_report = plan_pipeline_spec(paths)
    proposal = next(
        item
        for item in first_report["proposals"]
        if item["proposal_type"] == "silver_metadata_fields_addition"
    )
    approve_proposal(paths, proposal["proposal_id"], "tester")

    second_report = plan_pipeline_spec(paths)
    applied = next(
        item
        for item in second_report["proposals"]
        if item["proposal_type"] == "silver_metadata_fields_addition"
    )
    spec_after = json.loads(paths.pipeline_spec.read_text(encoding="utf-8"))

    assert second_report["applied"] is True
    assert proposal["proposal_id"] in second_report["approved_proposal_ids"]
    assert proposal["proposal_id"] in second_report["applied_proposal_ids"]
    assert applied["status"] == "promoted"
    assert "score_band" in spec_after["silver"]["metadata_fields"]


def test_planner_persists_candidate_artifacts_and_metrics_for_auto_promoted_changes(
    tmp_path: Path,
) -> None:
    root = tmp_path
    _write_bronze(
        root,
        [
            _base_row(
                metadata=json.dumps(
                    {
                        "device": "android",
                        "city": "Sao Paulo",
                        "state": "SP",
                        "response_time_sec": 10,
                        "is_business_hours": "true",
                        "lead_source": "google_ads",
                    }
                ),
            )
        ],
    )
    paths = build_paths(root)

    report = plan_pipeline_spec(paths)
    validation = next(
        item
        for item in report["proposals"]
        if item["proposal_type"] == "metadata_boolean_validation_addition"
    )
    metrics = json.loads(paths.autonomy_metrics.read_text(encoding="utf-8"))
    candidate_spec = json.loads(paths.pipeline_spec.read_text(encoding="utf-8"))

    assert validation["status"] == "promoted"
    assert "business_hours_message_ratio" in candidate_spec["gold"]["required_columns"]
    assert metrics["promotion_count_total"] >= 1
    assert metrics["proposal_count_total"] >= 1


def test_planner_auto_approves_structural_proposal_when_agent_self_review_confident(
    tmp_path: Path, monkeypatch
) -> None:
    root = tmp_path
    _write_bronze(
        root,
        [
            _base_row(
                metadata=json.dumps(
                    {
                        "device": "android",
                        "city": "Sao Paulo",
                        "state": "SP",
                        "response_time_sec": 10,
                        "lead_source": "google_ads",
                        "score_band": "alto",
                    }
                )
            )
        ],
    )
    paths = build_paths(root)

    monkeypatch.setattr(
        "pipeline.agent.planner.agent_self_review_proposal",
        lambda *args, **kwargs: {"should_approve": True, "confidence": 0.95, "rationale": "ok"},
    )

    report = plan_pipeline_spec(paths)
    proposal = next(
        item
        for item in report["proposals"]
        if item["proposal_type"] == "silver_metadata_fields_addition"
    )
    spec_after = json.loads(paths.pipeline_spec.read_text(encoding="utf-8"))

    assert proposal["status"] == "promoted"
    assert proposal["approval_context"]["approved_by"] == "agent"
    assert "score_band" in spec_after["silver"]["metadata_fields"]


def test_planner_does_not_auto_approve_when_agent_self_review_confidence_below_threshold(
    tmp_path: Path, monkeypatch
) -> None:
    root = tmp_path
    _write_bronze(
        root,
        [
            _base_row(
                metadata=json.dumps(
                    {
                        "device": "android",
                        "city": "Sao Paulo",
                        "state": "SP",
                        "response_time_sec": 10,
                        "lead_source": "google_ads",
                        "score_band": "alto",
                    }
                )
            )
        ],
    )
    paths = build_paths(root)

    monkeypatch.setattr(
        "pipeline.agent.planner.agent_self_review_proposal",
        lambda *args, **kwargs: {
            "should_approve": True,
            "confidence": 0.50,
            "rationale": "uncertain",
        },
    )

    report = plan_pipeline_spec(paths)
    proposal = next(
        item
        for item in report["proposals"]
        if item["proposal_type"] == "silver_metadata_fields_addition"
    )

    assert proposal["status"] == "awaiting_approval"


def test_planner_holds_high_impact_change_for_approval_with_candidate_artifacts(
    tmp_path: Path,
) -> None:
    root = tmp_path
    _write_bronze(root, [_base_row()])
    paths = build_paths(root)

    report = plan_pipeline_spec(paths)
    proposal = next(
        item
        for item in report["proposals"]
        if item["proposal_type"] == "silver_metadata_fields_addition"
    )
    decision = json.loads(
        (paths.autonomy_decisions / "latest_autonomy_decision.json").read_text(encoding="utf-8")
    )

    assert proposal["status"] == "awaiting_approval"
    assert (paths.autonomy_proposals / f"{proposal['proposal_id']}.json").exists()
    assert (paths.candidates / proposal["proposal_id"] / "candidate_diff.json").exists()
    assert decision["decision"] in {"hold_for_approval", "promote"}


# --- FIX-A: quality_baseline persisted after plan_pipeline_spec call ---

_OP = "pipeline.orchestration.operator"


class _CDCStateStub:
    digest = "stub"
    known_ids: frozenset = frozenset()
    row_count = 0

    def as_dict(self) -> dict:
        return {"digest": self.digest, "known_ids": [], "row_count": 0}


def test_run_cycle_preserves_quality_baseline_written_by_planner(tmp_path: Path) -> None:
    from pipeline.agent.execution_planner import ExecutionPlan

    paths = build_paths(tmp_path)

    stale_state: dict = {}
    fresh_state: dict = {
        "quality_baseline": {
            "recorded_at_utc": "2026-01-01T00:00:00+00:00",
            "record_count": 250,
            "null_rates": {},
            "distribution": {},
        }
    }
    saved_states: list[dict] = []

    with (
        patch(f"{_OP}.ensure_directories"),
        patch(f"{_OP}.ensure_pipeline_spec", return_value={}),
        patch(f"{_OP}.compile_pipeline_spec", return_value={}),
        patch(f"{_OP}.build_cdc_state", return_value=_CDCStateStub()),
        patch(f"{_OP}.load_pipeline_state", side_effect=[stale_state, fresh_state]),
        patch(f"{_OP}.has_source_changed_cdc", return_value=True),
        patch(f"{_OP}.plan_pipeline_spec", return_value={"proposals": [], "applied": False}),
        patch(f"{_OP}.build_observation", return_value=MagicMock()),
        patch(
            f"{_OP}.build_execution_plan",
            return_value=ExecutionPlan(
                stages=[],
                rationale="test",
                confidence=1.0,
                source="test",
                generated_at_utc="2026-01-01T00:00:00+00:00",
            ),
        ),
        patch(f"{_OP}.save_pipeline_state", side_effect=lambda _p, s: saved_states.append(dict(s))),
        patch(f"{_OP}._write_reports"),
        patch(f"{_OP}._build_alert_report", return_value={}),
        patch(f"{_OP}.log_event"),
    ):
        run_cycle(paths, force=False)

    assert saved_states, "save_pipeline_state was never called"
    assert "quality_baseline" in saved_states[-1], (
        "quality_baseline written by plan_pipeline_spec was overwritten by stale state"
    )


# --- FIX-C: agent_auto_approve_if_confidence_ge thresholds in autonomy policy ---

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _paths_with_real_policy(tmp_path: Path):
    paths = build_paths(tmp_path)
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    shutil.copy(_REPO_ROOT / "config" / "agent_autonomy_policy.json", paths.autonomy_policy)
    return paths


def test_get_agent_auto_approve_threshold_schema_update(tmp_path: Path) -> None:
    paths = _paths_with_real_policy(tmp_path)
    assert get_agent_auto_approve_threshold(paths, "schema_update") == 0.90


def test_get_agent_auto_approve_threshold_data_quality_drift(tmp_path: Path) -> None:
    paths = _paths_with_real_policy(tmp_path)
    assert get_agent_auto_approve_threshold(paths, "data_quality_drift") == 0.85


def test_get_agent_auto_approve_threshold_validation_enhancement_is_none(tmp_path: Path) -> None:
    paths = _paths_with_real_policy(tmp_path)
    assert get_agent_auto_approve_threshold(paths, "validation_enhancement") is None
