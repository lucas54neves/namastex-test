from __future__ import annotations

import pandas as pd

from pipeline.quality import (
    summarize_validation_results,
    validate_bronze,
    validate_cross_layer_consistency,
    validate_gold,
    validate_silver,
    validate_silver_messages,
)


def _base_bronze_row() -> dict[str, object]:
    return {
        "message_id": "m1",
        "conversation_id": "conv_1",
        "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
        "direction": "outbound",
        "sender_phone": "+5511999999999",
        "sender_name": "Ana Paula",
        "message_type": "text",
        "message_body": "oi",
        "status": "delivered",
        "channel": "whatsapp",
        "campaign_id": "camp_1",
        "agent_id": "agent_1",
        "conversation_outcome": "em_negociacao",
        "metadata": "{}",
    }


def _base_silver_row() -> dict[str, object]:
    return {
        "lead_key": "lead_123",
        "canonical_lead_name_masked": "XXX XXXXX",
        "lead_contact_ref": "+XXXXXXXXXXXXX",
        "first_seen_at": pd.Timestamp("2026-02-01 10:00:00"),
        "last_seen_at": pd.Timestamp("2026-02-01 10:01:00"),
        "conversation_count": 1,
        "message_count": 2,
        "observed_campaign_ids": '["camp_1"]',
        "observed_lead_sources": '["google_ads"]',
        "observed_outcomes": '["em_negociacao"]',
        "has_vehicle_signal": False,
        "has_competitor_signal": False,
        "has_sinistro_signal": False,
        "has_email_signal": False,
        "has_phone_signal": False,
        "has_cpf_signal": False,
        "has_cep_signal": False,
        "has_plate_signal": False,
    }


def _base_gold_row() -> dict[str, object]:
    return {
        "lead_key": "lead_123",
        "first_seen_at": pd.Timestamp("2026-02-01 10:00:00"),
        "last_seen_at": pd.Timestamp("2026-02-01 10:01:00"),
        "conversation_count": 1,
        "total_messages": 2,
        "inbound_messages": 1,
        "outbound_messages": 1,
        "duplicate_events_removed": 0,
        "contains_email": False,
        "contains_phone": False,
        "contains_cpf": False,
        "contains_cep": False,
        "contains_plate": False,
        "mentioned_vehicle": False,
        "mentioned_competitor": False,
        "mentioned_sinistro": False,
        "avg_response_time_sec": 60.0,
        "avg_quoted_price": None,
        "primary_competitor": None,
        "city": "Sao Paulo",
        "state": "SP",
        "observed_lead_sources": '["google_ads"]',
        "observed_campaign_ids": '["camp_1"]',
        "observed_outcomes": '["em_negociacao"]',
        "dominant_email_provider": None,
        "engagement_bucket": "lead_frio",
        "data_shared_score": 0,
        "persona_profile": "lead_frio",
        "audience_segment": "nutricao_basica",
        "lead_temperature": "frio",
        "price_sensitivity": "baixa",
        "intent_stage": "descoberta_inicial",
        "contact_readiness": "baixa",
        "risk_signal": "baixo",
        "response_latency_band": "rapida",
        "closure_outcome_group": "aberto",
        "has_closed_outcome": False,
        "price_objection_intensity": "nenhuma",
        "commercial_urgency_signal": "nenhuma",
        "competitor_pressure_level": "nenhuma",
        "conversation_sentiment_label": "sem_evidencia",
        "conversation_sentiment_support": "sem_evidencia",
        "positive_tone_hits": 0,
        "negative_tone_hits": 0,
    }


def _base_silver_message_row() -> dict[str, object]:
    return {
        "lead_key": "lead_123",
        "conversation_id": "conv_1",
        "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
        "direction": "inbound",
        "message_type": "text",
        "message_body_masked": "oi",
        "sender_name_masked": "XX",
        "sender_phone_masked": "+XXXXXXXXXXXXX",
        "dropped_duplicate_events": 0,
        "mentions_vehicle": False,
        "mentions_competitor": False,
        "mentions_sinistro": False,
        "contains_cpf": False,
        "contains_plate": False,
        "contains_phone": False,
        "contains_email": False,
        "contains_cep": False,
        "vehicle_make": None,
        "vehicle_model": None,
        "vehicle_year": None,
    }


def test_validate_bronze_rejects_conversation_starting_inbound() -> None:
    first = _base_bronze_row()
    first["direction"] = "inbound"
    second = dict(first)
    second["message_id"] = "m2"
    second["timestamp"] = pd.Timestamp("2026-02-01 10:01:00")
    second["direction"] = "outbound"
    df = pd.DataFrame([second, first])

    summary = summarize_validation_results(validate_bronze(df))

    assert summary["status"] == "failed"
    failure = next(
        item for item in summary["failed_checks"] if item["check"] == "first_message_outbound"
    )
    assert failure["detail"]["violating_conversations"] == 1
    assert failure["detail"]["sample_conversation_ids"] == ["conv_1"]


def test_validate_bronze_accepts_outbound_first_message() -> None:
    first = _base_bronze_row()
    second = dict(first)
    second["message_id"] = "m2"
    second["timestamp"] = pd.Timestamp("2026-02-01 10:01:00")
    second["direction"] = "inbound"
    df = pd.DataFrame([second, first])

    summary = summarize_validation_results(validate_bronze(df))

    assert summary["status"] == "passed"
    assert all(item["check"] != "first_message_outbound_ratio" for item in summary["checks"])


def test_validate_silver_detects_duplicate_rows() -> None:
    first = _base_silver_row()
    second = dict(first)
    second["lead_key"] = first["lead_key"]
    df = pd.DataFrame([first, second])

    summary = summarize_validation_results(validate_silver(df))

    assert summary["status"] == "failed"
    assert any(item["check"] == "lead_key_unique" for item in summary["failed_checks"])


def test_validate_gold_accepts_valid_bucket_set() -> None:
    df = pd.DataFrame([_base_gold_row()])

    summary = summarize_validation_results(validate_gold(df))

    assert summary["status"] == "passed"


def test_validate_gold_rejects_invalid_persona_profile() -> None:
    row = _base_gold_row()
    row["persona_profile"] = "desconhecida"
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_gold(df))

    assert summary["status"] == "failed"
    assert any(item["check"] == "persona_profile_valid" for item in summary["failed_checks"])


def test_validate_gold_rejects_invalid_price_sensitivity() -> None:
    row = _base_gold_row()
    row["price_sensitivity"] = "desconhecida"
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_gold(df))

    assert summary["status"] == "failed"
    assert any(item["check"] == "price_sensitivity_valid" for item in summary["failed_checks"])


def test_validate_gold_rejects_invalid_response_latency_band() -> None:
    row = _base_gold_row()
    row["response_latency_band"] = "instantanea"
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_gold(df))

    assert summary["status"] == "failed"
    assert any(item["check"] == "response_latency_band_valid" for item in summary["failed_checks"])


def test_validate_gold_rejects_contradictory_closure_flag() -> None:
    row = _base_gold_row()
    row["closure_outcome_group"] = "fechado"
    row["has_closed_outcome"] = False
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_gold(df))

    assert summary["status"] == "failed"
    assert any(
        item["check"] == "closure_outcome_flag_coherent" for item in summary["failed_checks"]
    )


def test_validate_gold_rejects_email_provider_without_email_signal() -> None:
    row = _base_gold_row()
    row["dominant_email_provider"] = "gmail"
    row["contains_email"] = False
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_gold(df))

    assert summary["status"] == "failed"
    assert any(
        item["check"] == "dominant_email_provider_nullability_coherent"
        for item in summary["failed_checks"]
    )


def test_validate_gold_rejects_invalid_conversation_sentiment_label() -> None:
    row = _base_gold_row()
    row["conversation_sentiment_label"] = "otimista"
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_gold(df))

    assert summary["status"] == "failed"
    assert any(
        item["check"] == "conversation_sentiment_label_valid" for item in summary["failed_checks"]
    )


def test_validate_gold_rejects_non_sem_evidencia_support_without_evidence() -> None:
    row = _base_gold_row()
    row["conversation_sentiment_support"] = "fraco"
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_gold(df))

    assert summary["status"] == "failed"
    assert any(
        item["check"] == "conversation_sentiment_support_coherent"
        for item in summary["failed_checks"]
    )


def test_validate_gold_rejects_forte_support_without_repeated_evidence() -> None:
    row = _base_gold_row()
    row["conversation_sentiment_label"] = "positivo"
    row["conversation_sentiment_support"] = "forte"
    row["positive_tone_hits"] = 1
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_gold(df))

    assert summary["status"] == "failed"
    assert any(
        item["check"] == "conversation_sentiment_support_strength_coherent"
        for item in summary["failed_checks"]
    )


def test_validate_silver_rejects_forbidden_raw_columns() -> None:
    row = _base_silver_row()
    row["sender_phone"] = "+5511999999999"
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_silver(df))

    assert summary["status"] == "failed"
    assert any(item["check"] == "forbidden_raw_columns_absent" for item in summary["failed_checks"])


def test_validate_gold_rejects_forbidden_raw_columns() -> None:
    row = _base_gold_row()
    row["message_body"] = "texto cru"
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_gold(df))

    assert summary["status"] == "failed"
    assert any(item["check"] == "forbidden_raw_columns_absent" for item in summary["failed_checks"])


def test_validate_silver_accepts_masked_text_without_leaks() -> None:
    df = pd.DataFrame([_base_silver_row()])

    summary = summarize_validation_results(validate_silver(df))

    assert summary["status"] == "passed"


def test_validate_silver_rejects_leaking_email_in_masked_text() -> None:
    row = _base_silver_row()
    row["canonical_lead_name_masked"] = "ana.paula@gmail.com"
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_silver(df))

    assert summary["status"] == "failed"
    assert any(
        item["check"] == "masked_text_fields_not_leaking" for item in summary["failed_checks"]
    )


def test_validate_silver_messages_rejects_leaking_phone_in_masked_text() -> None:
    row = _base_silver_message_row()
    row["message_body_masked"] = "telefone +55 11 99999-9999"
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_silver_messages(df))

    assert summary["status"] == "failed"
    assert any(item["check"] == "masked_phone_not_leaking" for item in summary["failed_checks"])


def test_validate_silver_messages_rejects_leaking_cpf_in_masked_text() -> None:
    row = _base_silver_message_row()
    row["message_body_masked"] = "cpf 123.456.789-00"
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_silver_messages(df))

    assert summary["status"] == "failed"
    assert any(item["check"] == "masked_cpf_not_leaking" for item in summary["failed_checks"])


def test_validate_silver_messages_rejects_leaking_cep_in_masked_text() -> None:
    row = _base_silver_message_row()
    row["message_body_masked"] = "cep 04567-123"
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_silver_messages(df))

    assert summary["status"] == "failed"
    assert any(item["check"] == "masked_cep_not_leaking" for item in summary["failed_checks"])


def test_validate_silver_messages_rejects_leaking_plate_in_masked_text() -> None:
    row = _base_silver_message_row()
    row["message_body_masked"] = "placa ABC1D23"
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_silver_messages(df))

    assert summary["status"] == "failed"
    assert any(item["check"] == "masked_plate_not_leaking" for item in summary["failed_checks"])


def test_validate_gold_rejects_leaking_text_in_published_text_field() -> None:
    row = _base_gold_row()
    row["summary_text"] = "email ana.paula@gmail.com"
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_gold(df))

    assert summary["status"] == "failed"
    assert any(
        item["check"] == "masked_text_fields_not_leaking" for item in summary["failed_checks"]
    )


def test_validate_silver_messages_detects_duplicate_rows() -> None:
    first = _base_silver_message_row()
    first["sender_phone"] = "+5511999999999"
    first["message_body"] = "oi"
    second = dict(first)
    df = pd.DataFrame([first, second])

    summary = summarize_validation_results(validate_silver_messages(df))

    assert summary["status"] == "failed"
    assert any(item["check"] == "dedupe_keys_unique" for item in summary["failed_checks"])


def test_validate_cross_layer_consistency_rejects_silver_mismatched_aggregates() -> None:
    silver_df = pd.DataFrame([_base_silver_row()])
    silver_messages_df = pd.DataFrame(
        [
            _base_silver_message_row(),
            {
                **_base_silver_message_row(),
                "message_body_masked": "resposta",
                "timestamp": pd.Timestamp("2026-02-01 10:01:00"),
                "direction": "outbound",
            },
        ]
    )
    gold_df = pd.DataFrame([_base_gold_row()])
    silver_df.loc[0, "message_count"] = 99

    summary = summarize_validation_results(
        validate_cross_layer_consistency(silver_df, silver_messages_df, gold_df)
    )

    assert summary["status"] == "failed"
    assert any(
        item["check"] == "silver_message_count_matches_messages"
        for item in summary["failed_checks"]
    )


def test_validate_cross_layer_consistency_rejects_gold_lead_missing_in_silver() -> None:
    silver_df = pd.DataFrame([_base_silver_row()])
    silver_messages_df = pd.DataFrame(
        [
            _base_silver_message_row(),
            {
                **_base_silver_message_row(),
                "message_body_masked": "resposta",
                "timestamp": pd.Timestamp("2026-02-01 10:01:00"),
                "direction": "outbound",
            },
        ]
    )
    gold_row = _base_gold_row()
    gold_row["lead_key"] = "lead_missing"
    gold_df = pd.DataFrame([gold_row])

    summary = summarize_validation_results(
        validate_cross_layer_consistency(silver_df, silver_messages_df, gold_df)
    )

    assert summary["status"] == "failed"
    assert any(item["check"] == "gold_lead_keys_in_silver" for item in summary["failed_checks"])


def test_validate_cross_layer_consistency_rejects_gold_semantic_incoherence() -> None:
    silver_df = pd.DataFrame([_base_silver_row()])
    silver_messages_df = pd.DataFrame(
        [
            _base_silver_message_row(),
            {
                **_base_silver_message_row(),
                "message_body_masked": "resposta",
                "timestamp": pd.Timestamp("2026-02-01 10:01:00"),
                "direction": "outbound",
            },
        ]
    )
    gold_row = _base_gold_row()
    gold_row["engagement_bucket"] = "longa"
    gold_df = pd.DataFrame([gold_row])

    summary = summarize_validation_results(
        validate_cross_layer_consistency(silver_df, silver_messages_df, gold_df)
    )

    assert summary["status"] == "failed"
    assert any(item["check"] == "engagement_bucket_coherent" for item in summary["failed_checks"])


def test_validate_cross_layer_consistency_accepts_consistent_published_artifacts() -> None:
    silver_df = pd.DataFrame([_base_silver_row()])
    silver_messages_df = pd.DataFrame(
        [
            _base_silver_message_row(),
            {
                **_base_silver_message_row(),
                "message_body_masked": "resposta",
                "timestamp": pd.Timestamp("2026-02-01 10:01:00"),
                "direction": "outbound",
            },
        ]
    )
    gold_df = pd.DataFrame([_base_gold_row()])

    summary = summarize_validation_results(
        validate_cross_layer_consistency(silver_df, silver_messages_df, gold_df)
    )

    assert summary["status"] == "passed"
