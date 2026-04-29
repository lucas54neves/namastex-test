from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pipeline.config import build_paths
from pipeline.orchestration.jobs import run_pipeline


def _bronze_rows() -> list[dict[str, object]]:
    return [
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


def _write_bronze(root: Path, rows: list[dict[str, object]]) -> None:
    docs_dir = root / "docs"
    docs_dir.mkdir(exist_ok=True)
    pd.DataFrame(rows).to_parquet(docs_dir / "conversations_bronze.parquet", index=False)


def _bronze_rows_with_growth() -> list[dict[str, object]]:
    rows = _bronze_rows()
    rows.append(
        {
            "message_id": "m3",
            "conversation_id": "conv_1",
            "timestamp": "2026-02-01 10:05:00",
            "direction": "outbound",
            "sender_phone": "+5511991111111",
            "sender_name": "Diego Pereira",
            "message_type": "text",
            "message_body": "Consigo te enviar uma proposta agora com cobertura melhor.",
            "status": "delivered",
            "channel": "whatsapp",
            "campaign_id": "camp_1",
            "agent_id": "agent_diego_14",
            "conversation_outcome": "proposta_enviada",
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
        }
    )
    return rows


def test_adherence_silver_principal_artifact_is_unique_by_lead(tmp_path: Path) -> None:
    _write_bronze(tmp_path, _bronze_rows())

    paths = build_paths(tmp_path)
    result = run_pipeline(paths, force=True)

    silver_df = pd.read_parquet(result.silver_path)
    silver_messages_df = pd.read_parquet(result.silver_messages_path)

    assert result.status == "success"
    assert set(silver_df["lead_key"]) == set(silver_messages_df["lead_key"].unique())
    assert silver_df["lead_key"].is_unique
    assert len(silver_df) == 1
    assert silver_df["conversation_count"].iloc[0] == 1
    assert silver_df["message_count"].iloc[0] == 2


def test_adherence_published_artifacts_do_not_expose_raw_pii(tmp_path: Path) -> None:
    _write_bronze(tmp_path, _bronze_rows())

    paths = build_paths(tmp_path)
    result = run_pipeline(paths, force=True)

    silver_df = pd.read_parquet(result.silver_path)
    silver_conversations_llm_df = pd.read_parquet(result.silver_conversations_llm_path)
    gold_df = pd.read_parquet(result.gold_path)
    forbidden_raw_columns = {
        "sender_name",
        "sender_phone",
        "message_body",
        "conversation_lead_name",
        "conversation_lead_phone",
        "conversation_agent_name",
        "sender_name_normalized",
        "lead_name_raw",
        "lead_phone_raw",
    }

    assert forbidden_raw_columns.isdisjoint(silver_df.columns)
    assert forbidden_raw_columns.isdisjoint(gold_df.columns)
    assert forbidden_raw_columns.isdisjoint(silver_conversations_llm_df.columns)
    assert "canonical_lead_name_masked" in silver_df.columns
    assert "lead_contact_ref" in silver_df.columns


def test_adherence_gold_refreshes_after_bronze_growth(tmp_path: Path) -> None:
    _write_bronze(tmp_path, _bronze_rows())

    paths = build_paths(tmp_path)
    first = run_pipeline(paths, force=False)
    first_gold_df = pd.read_parquet(first.gold_path)

    _write_bronze(tmp_path, _bronze_rows_with_growth())
    second = run_pipeline(paths, force=False)
    second_gold_df = pd.read_parquet(second.gold_path)

    assert first.status == "success"
    assert second.executed is True
    assert second.status == "success"
    assert len(first_gold_df) == 1
    assert len(second_gold_df) == 1
    assert int(first_gold_df["total_messages"].iloc[0]) == 2
    assert int(second_gold_df["total_messages"].iloc[0]) == 3
    assert str(second_gold_df["latest_outcome"].iloc[0]) == "proposta_enviada"


def test_adherence_pipeline_persists_operational_state(tmp_path: Path) -> None:
    _write_bronze(tmp_path, _bronze_rows())

    paths = build_paths(tmp_path)
    result = run_pipeline(paths, force=True)
    state = json.loads(Path(result.state_path).read_text(encoding="utf-8"))

    assert result.status == "success"
    assert state["last_source_fingerprint"]["path"].endswith("docs/conversations_bronze.parquet")
    assert state["last_successful_artifacts"]["silver_path"].endswith("silver_leads.parquet")
    assert state["last_successful_artifacts"]["silver_conversations_llm_path"].endswith(
        "silver_conversations_llm.parquet"
    )
    assert state["last_successful_artifacts"]["gold_path"].endswith("conversations_gold.parquet")
    assert state["runs"]
    assert state["runs"][-1]["status"] == "success"
    assert state["runs"][-1]["executed"] is True


def test_adherence_gold_macro_artifact_exists_after_run(tmp_path: Path) -> None:
    _write_bronze(tmp_path, _bronze_rows())

    paths = build_paths(tmp_path)
    result = run_pipeline(paths, force=True)

    assert result.status == "success"
    assert Path(result.gold_macro_path).exists()
    macro_df = pd.read_parquet(result.gold_macro_path)
    required_columns = {
        "dimension",
        "dimension_value",
        "lead_count",
        "lead_pct",
        "rank",
        "computed_at_utc",
    }
    assert required_columns.issubset(set(macro_df.columns))
    assert "numeric_snapshot" in macro_df["dimension"].unique()
    assert (
        macro_df[macro_df["dimension"] == "numeric_snapshot"]["dimension_value"]
        .str.contains("total_leads")
        .any()
    )


def test_adherence_simulated_failure_generates_alert_and_diagnosis(tmp_path: Path) -> None:
    from pipeline.orchestration.operator import run_cycle
    from pipeline.orchestration.operator_stages import StageDeps

    _write_bronze(tmp_path, _bronze_rows())

    paths = build_paths(tmp_path)
    first = run_pipeline(paths, force=True)
    assert first.status == "success"

    def explode(*a, **kw) -> None:
        raise RuntimeError("boom")

    deps = StageDeps(
        run_bronze=StageDeps.default().run_bronze,
        run_silver=StageDeps.default().run_silver,
        run_gold=explode,
        run_validation=StageDeps.default().run_validation,
    )
    second = run_cycle(paths, force=True, stage_deps=deps)

    agent_report = json.loads(Path(second.agent_report_path).read_text(encoding="utf-8"))
    alert_report = json.loads(Path(second.alert_report_path).read_text(encoding="utf-8"))

    assert second.status == "fallback_to_last_successful"
    assert agent_report["status"] == "fallback_applied"
    assert agent_report["diagnoses"][0]["kind"] == "unexpected_runtime_error"
    assert agent_report["fallback"]["applied"] is True
    assert alert_report["event"]["should_alert"] is True
    assert alert_report["event"]["severity"] == "high"
