from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pipeline.config import build_paths
from pipeline.planner import plan_pipeline_spec


def test_planner_detects_new_metadata_field(tmp_path: Path) -> None:
    root = tmp_path
    (root / "docs").mkdir()
    frame = pd.DataFrame(
        [
            {
                "message_id": "m1",
                "conversation_id": "conv_1",
                "timestamp": "2026-02-01 10:00:00",
                "direction": "outbound",
                "sender_phone": "+5511991111111",
                "sender_name": "Diego Pereira",
                "message_type": "text",
                "message_body": "Oi",
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
        ]
    )
    frame.to_parquet(root / "docs" / "conversations_bronze.parquet", index=False)

    paths = build_paths(root)
    report = plan_pipeline_spec(paths)

    assert any(change["type"] == "silver_metadata_fields_addition" for change in report["changes"])
    latest_report = json.loads(
        Path(paths.monitoring / "latest_plan_report.json").read_text(encoding="utf-8")
    )
    assert latest_report["proposal_id"] == report["proposal_id"]
