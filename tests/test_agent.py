from __future__ import annotations

import pandas as pd

from pipeline.agent import (
    attempt_auto_remediation,
    diagnose_exception,
    diagnose_validation_failures,
)
from pipeline.compiler import get_default_compiled_plan
from pipeline.transforms import build_gold, build_silver, build_silver_leads


def _sample_bronze_frame() -> pd.DataFrame:
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
                "metadata": (
                    '{"device":"android","city":"Sao Paulo","state":"SP",'
                    '"response_time_sec":null,"is_business_hours":true,'
                    '"lead_source":"google_ads"}'
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
                "message_body": "meu cpf eh 123.456.789-00 e meu Civic 2019 placa ABC1D23",
                "status": "read",
                "channel": "whatsapp",
                "campaign_id": "camp_1",
                "agent_id": "agent_diego_14",
                "conversation_outcome": "em_negociacao",
                "metadata": (
                    '{"device":"iphone","city":"Sao Paulo","state":"SP",'
                    '"response_time_sec":60,"is_business_hours":true,'
                    '"lead_source":"google_ads"}'
                ),
            },
        ]
    )


def test_diagnose_validation_failures_maps_known_check() -> None:
    diagnoses = diagnose_validation_failures(
        [
            {
                "layer": "silver_messages",
                "check": "dedupe_keys_unique",
                "status": "failed",
                "detail": {"duplicate_rows": 2},
            }
        ],
        get_default_compiled_plan(),
    )

    assert len(diagnoses) == 1
    assert diagnoses[0].kind == "silver_deduplication_failure"
    assert diagnoses[0].auto_remediable is True
    assert diagnoses[0].playbook_id == "rebuild_silver_from_bronze"


def test_attempt_auto_remediation_rebuilds_gold_when_gold_check_fails() -> None:
    bronze = _sample_bronze_frame()
    bronze["timestamp"] = pd.to_datetime(bronze["timestamp"])
    silver_messages = build_silver(bronze)
    silver = build_silver_leads(silver_messages)
    gold = build_gold(silver, silver_messages)
    broken_gold = gold.copy()
    broken_gold["engagement_bucket"] = "quebrado"

    remediation = attempt_auto_remediation(
        bronze_df=bronze,
        silver_df=silver,
        silver_messages_df=silver_messages,
        gold_df=broken_gold,
        failed_checks=[
            {
                "layer": "gold",
                "check": "engagement_bucket_valid",
                "status": "failed",
                "detail": {"distinct_buckets": ["quebrado"]},
            }
        ],
        compiled_plan=get_default_compiled_plan(),
    )

    assert remediation["resolved"] is True
    assert "lead_key" in remediation["silver_messages_df"].columns
    assert "rebuild_gold_from_silver" in remediation["actions"]
    assert remediation["decisions"][0]["playbook"]["playbook_id"] == "rebuild_gold_from_silver"


def test_diagnose_exception_classifies_missing_source() -> None:
    diagnosis = diagnose_exception(FileNotFoundError("missing parquet"))
    assert diagnosis.kind == "source_not_found"
