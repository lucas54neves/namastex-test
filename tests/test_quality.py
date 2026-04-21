from __future__ import annotations

import pandas as pd

from pipeline.quality import summarize_validation_results, validate_gold, validate_silver


def _base_silver_row() -> dict[str, object]:
    return {
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


def _base_gold_row() -> dict[str, object]:
    return {
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


def test_validate_silver_detects_duplicate_rows() -> None:
    first = _base_silver_row()
    first["sender_phone"] = "+5511999999999"
    first["message_body"] = "oi"
    second = dict(first)
    df = pd.DataFrame([first, second])

    summary = summarize_validation_results(validate_silver(df))

    assert summary["status"] == "failed"
    assert any(item["check"] == "dedupe_keys_unique" for item in summary["failed_checks"])


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


def test_validate_silver_rejects_forbidden_raw_columns() -> None:
    row = _base_silver_row()
    row["sender_phone"] = "+5511999999999"
    row["message_body"] = "oi"
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
    row["message_body_masked"] = "fale com ana.paula@gmail.com"
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_silver(df))

    assert summary["status"] == "failed"
    assert any(item["check"] == "masked_email_not_leaking" for item in summary["failed_checks"])


def test_validate_silver_rejects_leaking_phone_in_masked_text() -> None:
    row = _base_silver_row()
    row["message_body_masked"] = "telefone +55 11 99999-9999"
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_silver(df))

    assert summary["status"] == "failed"
    assert any(item["check"] == "masked_phone_not_leaking" for item in summary["failed_checks"])


def test_validate_silver_rejects_leaking_cpf_in_masked_text() -> None:
    row = _base_silver_row()
    row["message_body_masked"] = "cpf 123.456.789-00"
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_silver(df))

    assert summary["status"] == "failed"
    assert any(item["check"] == "masked_cpf_not_leaking" for item in summary["failed_checks"])


def test_validate_silver_rejects_leaking_cep_in_masked_text() -> None:
    row = _base_silver_row()
    row["message_body_masked"] = "cep 04567-123"
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_silver(df))

    assert summary["status"] == "failed"
    assert any(item["check"] == "masked_cep_not_leaking" for item in summary["failed_checks"])


def test_validate_silver_rejects_leaking_plate_in_masked_text() -> None:
    row = _base_silver_row()
    row["message_body_masked"] = "placa ABC1D23"
    df = pd.DataFrame([row])

    summary = summarize_validation_results(validate_silver(df))

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
