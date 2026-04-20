from __future__ import annotations

import pandas as pd

from pipeline.quality import summarize_validation_results, validate_gold, validate_silver


def test_validate_silver_detects_duplicate_rows() -> None:
    df = pd.DataFrame(
        [
            {
                "conversation_id": "conv_1",
                "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
                "direction": "inbound",
                "sender_phone": "+5511999999999",
                "message_type": "text",
                "message_body": "oi",
                "message_body_masked": "oi",
                "sender_name_masked": "XX",
                "sender_phone_masked": "+XXXXXXXXXXXXX",
                "dropped_duplicate_events": 0,
                "mentions_vehicle": False,
                "mentions_competitor": False,
                "mentions_sinistro": False,
                "contains_cpf": False,
                "contains_plate": False,
                "vehicle_make": None,
                "vehicle_model": None,
                "vehicle_year": None,
                "timestamp_not_used": None,
            },
            {
                "conversation_id": "conv_1",
                "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
                "direction": "inbound",
                "sender_phone": "+5511999999999",
                "message_type": "text",
                "message_body": "oi",
                "message_body_masked": "oi",
                "sender_name_masked": "XX",
                "sender_phone_masked": "+XXXXXXXXXXXXX",
                "dropped_duplicate_events": 0,
                "mentions_vehicle": False,
                "mentions_competitor": False,
                "mentions_sinistro": False,
                "contains_cpf": False,
                "contains_plate": False,
                "vehicle_make": None,
                "vehicle_model": None,
                "vehicle_year": None,
                "timestamp_not_used": None,
            },
        ]
    )
    df["contains_phone"] = False
    df["contains_email"] = False
    df["contains_cep"] = False

    summary = summarize_validation_results(validate_silver(df))

    assert summary["status"] == "failed"
    assert any(item["check"] == "dedupe_keys_unique" for item in summary["failed_checks"])


def test_validate_gold_accepts_valid_bucket_set() -> None:
    df = pd.DataFrame(
        [
            {
                "conversation_id": "conv_1",
                "total_messages": 3,
                "duplicate_events_removed": 0,
                "engagement_bucket": "lead_frio",
                "data_shared_score": 1,
                "persona_profile": "lead_frio",
                "audience_segment": "nutricao_basica",
                "lead_temperature": "morno",
                "price_sensitivity": "baixa",
                "intent_stage": "descoberta_inicial",
                "contact_readiness": "media",
                "risk_signal": "baixo",
            }
        ]
    )

    summary = summarize_validation_results(validate_gold(df))

    assert summary["status"] == "passed"


def test_validate_gold_rejects_invalid_persona_profile() -> None:
    df = pd.DataFrame(
        [
            {
                "conversation_id": "conv_1",
                "total_messages": 3,
                "duplicate_events_removed": 0,
                "engagement_bucket": "lead_frio",
                "data_shared_score": 1,
                "persona_profile": "desconhecida",
                "audience_segment": "nutricao_basica",
                "lead_temperature": "morno",
                "price_sensitivity": "baixa",
                "intent_stage": "descoberta_inicial",
                "contact_readiness": "media",
                "risk_signal": "baixo",
            }
        ]
    )

    summary = summarize_validation_results(validate_gold(df))

    assert summary["status"] == "failed"
    assert any(item["check"] == "persona_profile_valid" for item in summary["failed_checks"])
