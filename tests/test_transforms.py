from __future__ import annotations

import pandas as pd

from pipeline.transforms import (
    add_conversation_context,
    add_gold_segments,
    build_gold,
    build_silver,
    build_silver_leads,
    deduplicate_events,
    mask_message_body,
)


def test_mask_message_body_preserves_shape_for_structured_pii() -> None:
    row = pd.Series(
        {
            "message_body": (
                "Ana Paula, meu cpf eh 123.456.789-00, cep 04567-123, "
                "email ana.paula@gmail.com, telefone +55 11 99999-9999 e placa ABC1D23"
            ),
            "sender_name": "Ana Paula",
            "conversation_lead_name": "Ana Paula",
            "conversation_agent_name": "Diego Pereira",
        }
    )

    masked = mask_message_body(row)

    assert "123.456.789-00" not in masked
    assert "04567-123" not in masked
    assert "ana.paula@gmail.com" not in masked
    assert "+55 11 99999-9999" not in masked
    assert "ABC1D23" not in masked
    assert "XXX.XXX.XXX-XX" in masked
    assert "XXXXX-XXX" in masked
    assert "@xxxxx." in masked
    assert "+XX XX XXXXX-XXXX" in masked
    assert "XXX9X99" in masked


def test_deduplicate_events_keeps_highest_status_priority() -> None:
    df = pd.DataFrame(
        [
            {
                "message_id": "m1",
                "conversation_id": "conv_1",
                "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
                "direction": "inbound",
                "sender_phone": "+5511999999999",
                "message_type": "text",
                "message_body": "oi",
                "status": "sent",
            },
            {
                "message_id": "m2",
                "conversation_id": "conv_1",
                "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
                "direction": "inbound",
                "sender_phone": "+5511999999999",
                "message_type": "text",
                "message_body": "oi",
                "status": "read",
            },
        ]
    )

    deduped = deduplicate_events(df)

    assert len(deduped) == 1
    assert deduped.iloc[0]["status"] == "read"
    assert deduped.iloc[0]["dropped_duplicate_events"] == 1
    assert bool(deduped.iloc[0]["had_status_duplication"]) is True


def test_conversation_context_uses_first_inbound_and_outbound_names() -> None:
    df = pd.DataFrame(
        [
            {"conversation_id": "conv_1", "direction": "outbound", "sender_name": "Diego"},
            {"conversation_id": "conv_1", "direction": "inbound", "sender_name": "Ana Paula"},
            {"conversation_id": "conv_1", "direction": "inbound", "sender_name": "Ana P."},
        ]
    )

    enriched = add_conversation_context(df)

    assert enriched["conversation_agent_name"].tolist() == ["Diego", "Diego", "Diego"]
    assert enriched["conversation_lead_name"].tolist() == ["Ana Paula", "Ana Paula", "Ana Paula"]


def test_build_silver_leads_consolidates_multiple_conversations_for_same_lead() -> None:
    bronze = pd.DataFrame(
        [
            {
                "message_id": "m1",
                "conversation_id": "conv_1",
                "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
                "direction": "inbound",
                "sender_phone": "+5511982222222",
                "sender_name": "Ana Paula",
                "message_type": "text",
                "message_body": "quero cotacao do Civic 2019",
                "status": "read",
                "channel": "whatsapp",
                "campaign_id": "camp_1",
                "agent_id": "agent_1",
                "conversation_outcome": "em_negociacao",
                "metadata": (
                    '{"device":"iphone","city":"Sao Paulo","state":"SP",'
                    '"response_time_sec":60,"is_business_hours":true,'
                    '"lead_source":"google_ads"}'
                ),
            },
            {
                "message_id": "m2",
                "conversation_id": "conv_2",
                "timestamp": pd.Timestamp("2026-02-03 11:00:00"),
                "direction": "inbound",
                "sender_phone": "+5511982222222",
                "sender_name": "Ana Paula",
                "message_type": "text",
                "message_body": "Porto Seguro me cobrou R$ 2.500,00",
                "status": "read",
                "channel": "whatsapp",
                "campaign_id": "camp_2",
                "agent_id": "agent_2",
                "conversation_outcome": "proposta_enviada",
                "metadata": (
                    '{"device":"iphone","city":"Sao Paulo","state":"SP",'
                    '"response_time_sec":120,"is_business_hours":false,'
                    '"lead_source":"referral"}'
                ),
            },
        ]
    )

    silver_messages = build_silver(bronze)
    silver_leads = build_silver_leads(silver_messages)

    assert len(silver_leads) == 1
    assert silver_leads.iloc[0]["conversation_count"] == 2
    assert silver_leads.iloc[0]["message_count"] == 2
    assert silver_leads.iloc[0]["canonical_lead_name_masked"] == "XXX XXXXX"
    assert silver_leads.iloc[0]["observed_campaign_ids"] == '["camp_1", "camp_2"]'
    assert silver_leads.iloc[0]["observed_lead_sources"] == '["google_ads", "referral"]'
    assert bool(silver_leads.iloc[0]["has_vehicle_signal"]) is True
    assert bool(silver_leads.iloc[0]["has_competitor_signal"]) is True


def test_add_gold_segments_assigns_persona_and_audience() -> None:
    gold = pd.DataFrame(
        [
            {
                "engagement_bucket": "media",
                "data_shared_score": 3,
                "mentioned_competitor": False,
                "avg_quoted_price": None,
                "mentioned_sinistro": False,
                "duplicate_events_removed": 0,
                "contains_cpf": False,
            },
            {
                "engagement_bucket": "curta",
                "data_shared_score": 1,
                "mentioned_competitor": True,
                "avg_quoted_price": 2500.0,
                "mentioned_sinistro": False,
                "duplicate_events_removed": 0,
                "contains_cpf": False,
            },
            {
                "engagement_bucket": "lead_frio",
                "data_shared_score": 0,
                "mentioned_competitor": False,
                "avg_quoted_price": None,
                "mentioned_sinistro": True,
                "duplicate_events_removed": 1,
                "contains_cpf": True,
            },
        ]
    )

    segmented = add_gold_segments(gold)

    assert segmented["persona_profile"].tolist() == [
        "lead_engajado_com_dados",
        "cotador_comparador",
        "cliente_pos_sinistro",
    ]
    assert segmented["audience_segment"].tolist() == [
        "close_comercial",
        "oferta_competitiva",
        "retencao_pos_sinistro",
    ]
    assert segmented["lead_temperature"].tolist() == ["quente", "morno", "frio"]


def test_build_gold_consolidates_multiple_conversations_per_lead() -> None:
    bronze = pd.DataFrame(
        [
            {
                "message_id": "m1",
                "conversation_id": "conv_1",
                "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
                "direction": "inbound",
                "sender_phone": "+5511982222222",
                "sender_name": "Ana Paula",
                "message_type": "text",
                "message_body": "quero cotacao do Civic 2019",
                "status": "read",
                "channel": "whatsapp",
                "campaign_id": "camp_1",
                "agent_id": "agent_1",
                "conversation_outcome": "em_negociacao",
                "metadata": (
                    '{"device":"iphone","city":"Sao Paulo","state":"SP",'
                    '"response_time_sec":60,"is_business_hours":true,'
                    '"lead_source":"google_ads"}'
                ),
            },
            {
                "message_id": "m2",
                "conversation_id": "conv_2",
                "timestamp": pd.Timestamp("2026-02-03 11:00:00"),
                "direction": "inbound",
                "sender_phone": "+5511982222222",
                "sender_name": "Ana Paula",
                "message_type": "text",
                "message_body": "Porto Seguro me cobrou R$ 2.500,00 e tive sinistro",
                "status": "read",
                "channel": "whatsapp",
                "campaign_id": "camp_2",
                "agent_id": "agent_2",
                "conversation_outcome": "proposta_enviada",
                "metadata": (
                    '{"device":"iphone","city":"Sao Paulo","state":"SP",'
                    '"response_time_sec":120,"is_business_hours":false,'
                    '"lead_source":"referral"}'
                ),
            },
        ]
    )

    silver_messages = build_silver(bronze)
    silver_leads = build_silver_leads(silver_messages)
    gold = build_gold(silver_leads, silver_messages)

    assert len(gold) == 1
    assert gold.iloc[0]["lead_key"] == silver_leads.iloc[0]["lead_key"]
    assert gold.iloc[0]["conversation_count"] == 2
    assert gold.iloc[0]["total_messages"] == 2
    assert bool(gold.iloc[0]["mentioned_competitor"]) is True
    assert bool(gold.iloc[0]["mentioned_sinistro"]) is True
    assert gold.iloc[0]["observed_campaign_ids"] == '["camp_1", "camp_2"]'
    assert gold.iloc[0]["lead_temperature"] == "frio"
    assert gold.iloc[0]["intent_stage"] == "pos_sinistro"
