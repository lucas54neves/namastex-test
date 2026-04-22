from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pipeline.agent.approval import approve_proposal
from pipeline.agent.planner import plan_pipeline_spec
from pipeline.config import build_paths


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
    _write_bronze(root, [_base_row()])

    paths = build_paths(root)
    report = plan_pipeline_spec(paths)

    proposal = next(
        item
        for item in report["proposals"]
        if item["proposal_type"] == "silver_metadata_fields_addition"
    )
    assert proposal["proposal_family"] == "schema_update"
    assert proposal["context_detected"]["evidence"]["items"] == ["score_band"]
    assert proposal["safe_auto_apply"] is False
    assert proposal["recommendation_only"] is True
    latest_report = json.loads(
        Path(paths.monitoring / "latest_plan_report.json").read_text(encoding="utf-8")
    )
    assert latest_report["proposal_id"] == report["proposal_id"]
    assert latest_report["proposals"][0]["status"] == "proposed"


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
    assert "derived_column_addition" in families
    assert "segmentation_adjustment" in families
    assert "transformation_rule_change" in families

    validation = next(
        item for item in report["proposals"] if item["proposal_family"] == "validation_enhancement"
    )
    assert validation["requires_approval"] is True
    assert validation["safe_auto_apply"] is False


def test_planner_does_not_apply_structural_changes_without_explicit_approval(
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

    assert report["applied"] is False
    assert proposal["status"] == "proposed"
    assert proposal["proposal_id"].startswith("proposal_")
    assert paths.pipeline_spec.exists() is False


def test_planner_applies_supported_structural_change_after_explicit_approval(
    tmp_path: Path,
) -> None:
    root = tmp_path
    _write_bronze(root, [_base_row()])

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
    assert applied["status"] == "applied"
    assert "score_band" in spec_after["silver"]["metadata_fields"]
