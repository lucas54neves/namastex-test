from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pipeline.config import build_paths
from pipeline.jobs import run_pipeline
from pipeline.quality import ValidationResult


def _sample_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "message_id": "m1",
                "conversation_id": "conv_1",
                "timestamp": "2026-02-01 10:00:00",
                "direction": "outbound",
                "sender_phone": "+5511991111111",
                "sender_name": "Diego Pereira",
                "message_type": "text",
                "message_body": "Oi Ana Paula, tudo bem?",
                "status": "delivered",
                "channel": "whatsapp",
                "campaign_id": "camp_1",
                "agent_id": "agent_diego_14",
                "conversation_outcome": "em_negociacao",
                "metadata": json.dumps(
                    {
                        "device": "android",
                        "city": "Sao Paulo",
                        "state": "SP",
                        "response_time_sec": None,
                        "is_business_hours": True,
                        "lead_source": "google_ads",
                    }
                ),
            },
            {
                "message_id": "m2",
                "conversation_id": "conv_1",
                "timestamp": "2026-02-01 10:01:00",
                "direction": "inbound",
                "sender_phone": "+5511982222222",
                "sender_name": "Ana Paula",
                "message_type": "text",
                "message_body": "Meu cpf eh 123.456.789-00 e meu carro eh Civic 2019 placa ABC1D23",
                "status": "read",
                "channel": "whatsapp",
                "campaign_id": "camp_1",
                "agent_id": "agent_diego_14",
                "conversation_outcome": "em_negociacao",
                "metadata": json.dumps(
                    {
                        "device": "iphone",
                        "city": "Sao Paulo",
                        "state": "SP",
                        "response_time_sec": 60,
                        "is_business_hours": True,
                        "lead_source": "google_ads",
                    }
                ),
            },
        ]
    )


def test_run_pipeline_skips_when_source_is_unchanged(tmp_path: Path) -> None:
    root = tmp_path
    (root / "docs").mkdir()
    frame = _sample_frame()
    frame.to_parquet(root / "docs" / "conversations_bronze.parquet", index=False)

    paths = build_paths(root)
    first = run_pipeline(paths, force=False)
    second = run_pipeline(paths, force=False)

    assert first.executed is True
    assert second.executed is False
    assert second.status == "skipped_no_source_change"


def test_run_pipeline_writes_validation_report(tmp_path: Path) -> None:
    root = tmp_path
    (root / "docs").mkdir()
    frame = _sample_frame()
    frame.to_parquet(root / "docs" / "conversations_bronze.parquet", index=False)

    paths = build_paths(root)
    result = run_pipeline(paths, force=True)

    report = json.loads(Path(result.validation_report_path).read_text(encoding="utf-8"))
    assert result.status == "success"
    assert report["status"] == "passed"
    assert report["row_counts"] == {"bronze": 2, "silver": 2, "gold": 1}


def test_run_pipeline_applies_agent_fallback_on_runtime_error(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path
    (root / "docs").mkdir()
    frame = _sample_frame()
    frame.to_parquet(root / "docs" / "conversations_bronze.parquet", index=False)

    paths = build_paths(root)
    first = run_pipeline(paths, force=True)
    assert first.status == "success"

    import pipeline.jobs as jobs_module

    def explode(_silver: pd.DataFrame) -> pd.DataFrame:
        raise RuntimeError("boom")

    monkeypatch.setattr(jobs_module, "build_gold", explode)
    second = run_pipeline(paths, force=True)

    agent_report = json.loads(Path(second.agent_report_path).read_text(encoding="utf-8"))
    assert second.status == "fallback_to_last_successful"
    assert agent_report["fallback"]["applied"] is True


def test_run_pipeline_marks_auto_remediation_when_validation_is_fixed(
    tmp_path: Path, monkeypatch
) -> None:
    root = tmp_path
    (root / "docs").mkdir()
    frame = _sample_frame()
    frame.to_parquet(root / "docs" / "conversations_bronze.parquet", index=False)

    paths = build_paths(root)

    import pipeline.jobs as jobs_module

    original_validate_gold = jobs_module.validate_gold
    calls = {"count": 0}

    def flaky_validate_gold(df: pd.DataFrame) -> list[ValidationResult]:
        calls["count"] += 1
        if calls["count"] == 1:
            return [
                ValidationResult(
                    layer="gold",
                    check="engagement_bucket_valid",
                    status="failed",
                    detail={"distinct_buckets": ["bad_bucket"]},
                )
            ]
        return original_validate_gold(df)

    monkeypatch.setattr(jobs_module, "validate_gold", flaky_validate_gold)
    result = run_pipeline(paths, force=True)

    agent_report = json.loads(Path(result.agent_report_path).read_text(encoding="utf-8"))
    assert result.status == "success_after_auto_remediation"
    assert agent_report["status"] == "auto_remediated"
    assert agent_report["auto_remediation"]["applied"] is True
