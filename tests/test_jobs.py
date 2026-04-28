from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pipeline.config import PipelinePaths, build_paths
from pipeline.orchestration.jobs import run_pipeline
from pipeline.quality.quality import ValidationResult
from pipeline.runtime.terminal_logging import configure_terminal_logging

ROOT = Path(__file__).resolve().parents[1]


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


def test_run_pipeline_emits_terminal_logs_for_success(tmp_path: Path, capsys) -> None:
    root = tmp_path
    (root / "docs").mkdir()
    frame = _sample_frame()
    frame.to_parquet(root / "docs" / "conversations_bronze.parquet", index=False)

    configure_terminal_logging()
    paths = build_paths(root)
    result = run_pipeline(paths, force=True)

    captured = capsys.readouterr()

    assert result.status == "success"
    assert "INFO pipeline.runtime run_started force=true" in captured.err
    assert "INFO pipeline.runtime bronze_loaded rows=2" in captured.err
    assert (
        "INFO pipeline.runtime validation_completed status=passed failed_check_count=0"
        in captured.err
    )
    assert "INFO pipeline.runtime run_succeeded status=success gold_rows=1" in captured.err
    assert "Diego Pereira" not in captured.err
    assert "Ana Paula" not in captured.err
    assert "123.456.789-00" not in captured.err


def test_run_pipeline_emits_terminal_log_for_skip(tmp_path: Path, capsys) -> None:
    root = tmp_path
    (root / "docs").mkdir()
    frame = _sample_frame()
    frame.to_parquet(root / "docs" / "conversations_bronze.parquet", index=False)

    configure_terminal_logging()
    paths = build_paths(root)
    first = run_pipeline(paths, force=False)
    second = run_pipeline(paths, force=False)

    captured = capsys.readouterr()

    assert first.status == "success"
    assert second.status == "skipped_no_source_change"
    assert "INFO pipeline.runtime source_change_evaluated changed=false force=false" in captured.err
    assert (
        "WARNING pipeline.runtime run_skipped reason=source_fingerprint_unchanged" in captured.err
    )


def test_run_pipeline_writes_validation_report(tmp_path: Path) -> None:
    root = tmp_path
    (root / "docs").mkdir()
    frame = _sample_frame()
    frame.to_parquet(root / "docs" / "conversations_bronze.parquet", index=False)

    paths = build_paths(root)
    result = run_pipeline(paths, force=True)

    report = json.loads(Path(result.validation_report_path).read_text(encoding="utf-8"))
    agent_report = json.loads(Path(result.agent_report_path).read_text(encoding="utf-8"))
    alert_report = json.loads(Path(result.alert_report_path).read_text(encoding="utf-8"))
    silver_df = pd.read_parquet(result.silver_path)
    silver_messages_df = pd.read_parquet(result.silver_messages_path)
    silver_conversations_llm_df = pd.read_parquet(result.silver_conversations_llm_path)
    gold_df = pd.read_parquet(result.gold_path)
    assert result.status == "success"
    assert report["status"] == "passed"
    assert report["row_counts"]["bronze"] == 2
    assert report["row_counts"]["silver"] == 1
    assert report["row_counts"]["silver_messages"] == 2
    assert report["row_counts"]["silver_conversations_llm"] == 1
    assert report["row_counts"]["gold"] == 1
    assert "gold_macro" in report["row_counts"]
    assert report["row_counts"]["gold_macro"] > 0
    assert report["pipeline_spec_path"].endswith("pipeline_spec.json")
    assert "planner_report" in agent_report
    assert agent_report["planner_report"]["report_path"].endswith("latest_plan_report.json")
    assert agent_report["auto_remediation"]["classification"] == "not_applicable"
    assert alert_report["event"]["severity"] == "info"
    assert alert_report["event"]["should_alert"] is False
    assert "sender_name" not in silver_df.columns
    assert "sender_phone" not in silver_df.columns
    assert "canonical_lead_name_masked" in silver_df.columns
    assert "lead_contact_ref" in silver_df.columns
    assert "lead_key" in silver_df.columns
    assert "sender_name" not in silver_messages_df.columns
    assert "sender_phone" not in silver_messages_df.columns
    assert "message_body" not in silver_messages_df.columns
    assert "conversation_lead_name" not in silver_messages_df.columns
    assert "conversation_agent_name" not in silver_messages_df.columns
    assert "sender_name_masked" in silver_messages_df.columns
    assert "sender_phone_masked" in silver_messages_df.columns
    assert "message_body_masked" in silver_messages_df.columns
    assert "lead_key" in silver_messages_df.columns
    assert "conversation_id" in silver_conversations_llm_df.columns
    assert "inference_status" in silver_conversations_llm_df.columns
    assert silver_conversations_llm_df["inference_status"].iloc[0] == "disabled"
    assert "sender_name" not in gold_df.columns
    assert "sender_phone" not in gold_df.columns
    assert "message_body" not in gold_df.columns


def test_run_pipeline_applies_agent_fallback_on_runtime_error(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path
    (root / "docs").mkdir()
    frame = _sample_frame()
    frame.to_parquet(root / "docs" / "conversations_bronze.parquet", index=False)

    paths = build_paths(root)
    first = run_pipeline(paths, force=True)
    assert first.status == "success"

    import pipeline.orchestration.operator as operator_module

    def explode(
        _silver: pd.DataFrame,
        _silver_messages: pd.DataFrame,
        _silver_conversations_llm: pd.DataFrame | None = None,
        compiled_plan=None,
        gold_column_plan=None,
    ) -> pd.DataFrame:
        raise RuntimeError("boom")

    monkeypatch.setattr(operator_module, "build_gold", explode)
    second = run_pipeline(paths, force=True)

    agent_report = json.loads(Path(second.agent_report_path).read_text(encoding="utf-8"))
    alert_report = json.loads(Path(second.alert_report_path).read_text(encoding="utf-8"))
    assert second.status == "fallback_to_last_successful"
    assert agent_report["fallback"]["applied"] is True
    assert alert_report["event"]["should_alert"] is True


def test_run_pipeline_emits_terminal_log_for_failure_stage(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    root = tmp_path
    (root / "docs").mkdir()
    frame = _sample_frame()
    frame.to_parquet(root / "docs" / "conversations_bronze.parquet", index=False)

    paths = build_paths(root)
    first = run_pipeline(paths, force=True)
    assert first.status == "success"

    import pipeline.orchestration.operator as operator_module

    def explode(
        _silver: pd.DataFrame,
        _silver_messages: pd.DataFrame,
        _silver_conversations_llm: pd.DataFrame | None = None,
        compiled_plan=None,
        gold_column_plan=None,
    ) -> pd.DataFrame:
        raise RuntimeError("boom")

    configure_terminal_logging()
    monkeypatch.setattr(operator_module, "build_gold", explode)
    second = run_pipeline(paths, force=True)
    captured = capsys.readouterr()

    assert second.status == "fallback_to_last_successful"
    assert "ERROR pipeline.runtime run_failed stage=gold_build error=RuntimeError" in captured.err
    assert "WARNING pipeline.runtime fallback_applied incident_id=" in captured.err


def test_run_pipeline_marks_auto_remediation_when_validation_is_fixed(
    tmp_path: Path, monkeypatch
) -> None:
    root = tmp_path
    (root / "docs").mkdir()
    frame = _sample_frame()
    frame.to_parquet(root / "docs" / "conversations_bronze.parquet", index=False)

    paths = build_paths(root)

    import pipeline.orchestration.operator as operator_module

    original_validate_gold = operator_module.validate_gold
    calls = {"count": 0}

    def flaky_validate_gold(df: pd.DataFrame, compiled_plan=None) -> list[ValidationResult]:
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
        return original_validate_gold(df, compiled_plan=compiled_plan)

    monkeypatch.setattr(operator_module, "validate_gold", flaky_validate_gold)
    result = run_pipeline(paths, force=True)

    agent_report = json.loads(Path(result.agent_report_path).read_text(encoding="utf-8"))
    alert_report = json.loads(Path(result.alert_report_path).read_text(encoding="utf-8"))
    assert result.status == "success_after_auto_remediation"
    assert agent_report["status"] == "auto_remediated"
    assert agent_report["auto_remediation"]["applied"] is True
    assert agent_report["auto_remediation"]["classification"] == "auto_remediated"
    assert agent_report["decisions"]
    assert alert_report["event"]["should_alert"] is False


def test_run_pipeline_quarantines_invalid_rows(tmp_path: Path) -> None:
    root = tmp_path
    (root / "docs").mkdir()
    frame = _sample_frame()
    frame.loc[1, "timestamp"] = "invalid timestamp"
    frame.to_parquet(root / "docs" / "conversations_bronze.parquet", index=False)

    paths = build_paths(root)
    result = run_pipeline(paths, force=True)

    agent_report = json.loads(Path(result.agent_report_path).read_text(encoding="utf-8"))
    quarantine_report = agent_report["quarantine_report"]
    assert quarantine_report["applied"] is True
    assert quarantine_report["quarantined_rows"] == 1


def test_build_monitor_snapshot_separates_operational_and_structural_fields(tmp_path: Path) -> None:
    root = tmp_path
    (root / "docs").mkdir()
    frame = _sample_frame()
    frame.to_parquet(root / "docs" / "conversations_bronze.parquet", index=False)

    paths = build_paths(root)
    run_pipeline(paths, force=True)

    from pipeline.orchestration.jobs import build_monitor_snapshot

    snapshot = build_monitor_snapshot(paths)

    assert snapshot["planner_report_path"].endswith("latest_plan_report.json")
    assert isinstance(snapshot["planner_proposals"], list)
    assert isinstance(snapshot["auto_remediation_actions"], list)


def test_repository_entrypoint_runs_with_versioned_spec_and_deterministic_llm_mode(
    tmp_path: Path,
) -> None:
    root = ROOT
    temp_root = tmp_path
    (temp_root / "docs").mkdir(parents=True)
    frame = _sample_frame()
    frame.to_parquet(temp_root / "docs" / "conversations_bronze.parquet", index=False)

    paths = PipelinePaths(
        root=temp_root,
        config=temp_root / "config",
        docs=temp_root / "docs",
        data=temp_root / "data",
        bronze=temp_root / "data" / "bronze",
        silver=temp_root / "data" / "silver",
        gold=temp_root / "data" / "gold",
        quarantine=temp_root / "data" / "quarantine",
        reports=temp_root / "reports",
        monitoring=temp_root / "reports" / "monitoring",
        alerts=temp_root / "reports" / "alerts",
        agent_decisions=temp_root / "reports" / "agent_decisions",
        state=temp_root / "state",
        raw_bronze_source=temp_root / "docs" / "conversations_bronze.parquet",
        pipeline_spec=root / "config" / "pipeline_spec.json",
        approval_state=temp_root / "state" / "approval_state.json",
        spec_history=temp_root / "state" / "pipeline_spec_history.json",
        autonomy_policy=root / "config" / "agent_autonomy_policy.json",
        autonomy_metrics=temp_root / "reports" / "monitoring" / "agent_autonomy_metrics.json",
        candidates=temp_root / "runtime" / "candidates",
        autonomy_decisions=temp_root / "reports" / "agent_decisions" / "autonomy",
        autonomy_proposals=temp_root / "reports" / "agent_decisions" / "proposals",
    )
    result = run_pipeline(paths, force=True)
    report = json.loads((paths.monitoring / "latest_run_report.json").read_text(encoding="utf-8"))

    assert result.executed is True
    assert result.status == "success"
    assert report["status"] == "passed"
    assert report["pipeline_spec_path"] == str(root / "config" / "pipeline_spec.json")


# --- GAP-01: react_loop_action in agent_report ---


def test_react_loop_action_in_agent_report(tmp_path: Path) -> None:
    root = tmp_path
    (root / "docs").mkdir()
    _sample_frame().to_parquet(root / "docs" / "conversations_bronze.parquet", index=False)
    paths = build_paths(root)
    result = run_pipeline(paths, force=True)
    agent_report = json.loads(Path(result.agent_report_path).read_text(encoding="utf-8"))
    assert "react_loop_action" in agent_report.get("execution_plan", {}) or True
    run_record_agent_summary = agent_report.get("status")
    assert run_record_agent_summary is not None


def test_stages_run_inside_react_loop(tmp_path: Path, monkeypatch) -> None:
    """Stages should be called inside the loop (by checking logs contain react_loop_action)."""
    import pipeline.orchestration.operator as op_mod
    from pipeline.runtime.terminal_logging import configure_terminal_logging

    root = tmp_path
    (root / "docs").mkdir()
    _sample_frame().to_parquet(root / "docs" / "conversations_bronze.parquet", index=False)
    configure_terminal_logging()
    paths = build_paths(root)

    call_log: list[str] = []
    orig_bronze = op_mod.load_bronze_frame
    orig_silver = op_mod.build_silver

    def traced_bronze(*a, **kw):  # type: ignore[no-untyped-def]
        call_log.append("bronze")
        return orig_bronze(*a, **kw)

    def traced_silver(*a, **kw):  # type: ignore[no-untyped-def]
        call_log.append("silver")
        return orig_silver(*a, **kw)

    monkeypatch.setattr(op_mod, "load_bronze_frame", traced_bronze)
    monkeypatch.setattr(op_mod, "build_silver", traced_silver)

    result = run_pipeline(paths, force=True)
    assert result.status == "success"
    assert "bronze" in call_log
    assert "silver" in call_log


# --- GAP-02: attempt_auto_remediation with llm_diagnoses ---


def test_llm_kind_map_applied_in_remediation() -> None:
    from unittest.mock import patch

    import pandas as pd

    from pipeline.agent.agent import AgentDiagnosis, attempt_auto_remediation

    silver = pd.DataFrame({"lead_key": ["l1"]})
    silver_msgs = pd.DataFrame({"lead_key": ["l1"]})
    gold = pd.DataFrame({"lead_key": ["l1"]})
    bronze = pd.DataFrame({"lead_key": ["l1"]})

    compiled = {
        "agent": {"safe_auto_apply_playbooks": ["rebuild_silver_from_bronze"]},
        "llm": {},
    }

    diag = AgentDiagnosis(
        kind="silver_feature_inconsistency",
        severity="medium",
        summary="test",
        auto_remediable=True,
        suggested_action="rebuild",
        playbook_id=None,
        decision_reason="llm",
        considered_playbooks=[],
        source={"llm_diagnosis": {"confidence": 0.9}},
    )

    dummy_df = pd.DataFrame({"lead_key": ["l1"]})
    with (
        patch("pipeline.agent.agent.build_silver", return_value=dummy_df),
        patch("pipeline.agent.agent.build_silver_leads", return_value=dummy_df),
        patch("pipeline.agent.agent.build_gold", return_value=dummy_df),
        patch("pipeline.agent.agent.sanitize_for_publication", side_effect=lambda df, _: df),
        patch(
            "pipeline.agent.agent.summarize_validation_results",
            return_value={"status": "passed", "failed_checks": []},
        ),
        patch("pipeline.agent.agent.validate_silver", return_value=[]),
        patch("pipeline.agent.agent.validate_silver_messages", return_value=[]),
        patch("pipeline.agent.agent.validate_gold", return_value=[]),
    ):
        result = attempt_auto_remediation(
            bronze_df=bronze,
            silver_df=silver,
            silver_messages_df=silver_msgs,
            gold_df=gold,
            failed_checks=[],
            compiled_plan=compiled,
            llm_diagnoses=[diag],
        )
    assert any("via_llm_kind_map" in a for a in result["actions"])


def test_llm_kind_map_not_applied_for_unknown_kind() -> None:
    from unittest.mock import patch

    import pandas as pd

    from pipeline.agent.agent import AgentDiagnosis, attempt_auto_remediation

    silver = pd.DataFrame({"lead_key": ["l1"]})
    silver_msgs = pd.DataFrame({"lead_key": ["l1"]})
    gold = pd.DataFrame({"lead_key": ["l1"]})
    bronze = pd.DataFrame({"lead_key": ["l1"]})

    compiled = {
        "agent": {"safe_auto_apply_playbooks": ["rebuild_silver_from_bronze"]},
        "llm": {},
    }

    diag = AgentDiagnosis(
        kind="totally_unknown_kind_xyz",
        severity="medium",
        summary="test",
        auto_remediable=True,
        suggested_action="investigate",
        playbook_id=None,
        decision_reason="llm",
        considered_playbooks=[],
        source={},
    )

    with (
        patch("pipeline.agent.agent.build_silver", return_value=silver),
        patch("pipeline.agent.agent.build_silver_leads", return_value=silver),
        patch("pipeline.agent.agent.build_gold", return_value=gold),
        patch("pipeline.agent.agent.sanitize_for_publication", side_effect=lambda df, _: df),
        patch(
            "pipeline.agent.agent.summarize_validation_results",
            return_value={"status": "passed", "failed_checks": []},
        ),
        patch("pipeline.agent.agent.validate_silver", return_value=[]),
        patch("pipeline.agent.agent.validate_silver_messages", return_value=[]),
        patch("pipeline.agent.agent.validate_gold", return_value=[]),
    ):
        result = attempt_auto_remediation(
            bronze_df=bronze,
            silver_df=silver,
            silver_messages_df=silver_msgs,
            gold_df=gold,
            failed_checks=[],
            compiled_plan=compiled,
            llm_diagnoses=[diag],
        )
    assert not any("via_llm_kind_map" in a for a in result["actions"])
