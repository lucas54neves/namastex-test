from __future__ import annotations

import pandas as pd

import pipeline.conversation_enrichment as conversation_enrichment
from pipeline.compiler import get_default_compiled_plan
from pipeline.conversation_enrichment import (
    build_conversation_enrichment,
    consolidate_gold_semantics,
)
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
                "message_body": "quero cotacao do Civic 2019, meu email eh ana@gmail.com",
                "status": "read",
                "channel": "whatsapp",
                "campaign_id": "camp_1",
                "agent_id": "agent_1",
                "conversation_outcome": "fechado",
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
                "message_body": (
                    "Porto Seguro me cobrou R$ 2.500,00 e tive sinistro, preciso fechar hoje"
                ),
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
    silver_conversations_llm = build_conversation_enrichment(
        silver_messages, {"llm": {"enabled": False}}
    )
    gold = build_gold(silver_leads, silver_messages, silver_conversations_llm)

    assert len(gold) == 1
    assert gold.iloc[0]["lead_key"] == silver_leads.iloc[0]["lead_key"]
    assert gold.iloc[0]["conversation_count"] == 2
    assert gold.iloc[0]["total_messages"] == 2
    assert bool(gold.iloc[0]["mentioned_competitor"]) is True
    assert bool(gold.iloc[0]["mentioned_sinistro"]) is True
    assert gold.iloc[0]["observed_campaign_ids"] == '["camp_1", "camp_2"]'
    assert gold.iloc[0]["lead_temperature"] == "morno"
    assert gold.iloc[0]["intent_stage"] == "pos_sinistro"
    assert gold.iloc[0]["dominant_email_provider"] == "gmail"


def test_build_conversation_enrichment_uses_cache_and_marks_cache_hit() -> None:
    silver_messages = pd.DataFrame(
        [
            {
                "conversation_id": "conv_1",
                "lead_key": "lead_1",
                "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
                "message_id": "m1",
                "direction": "inbound",
                "message_type": "text",
                "message_body_masked": "quero cotacao",
                "mentions_vehicle": False,
                "mentions_competitor": False,
                "mentions_sinistro": False,
                "quoted_price": None,
                "metadata_response_time_sec": 60.0,
                "contains_email": False,
                "contains_phone": False,
                "contains_cpf": False,
                "contains_cep": False,
                "contains_plate": False,
                "price_objection_signal": False,
                "urgency_strength": 0,
                "competitor_comparison_signal": False,
                "competitor_mentioned": None,
            }
        ]
    )
    first = build_conversation_enrichment(silver_messages, {"llm": {"enabled": False}})
    second = build_conversation_enrichment(
        silver_messages,
        {"llm": {"enabled": False}},
        existing_enrichment=first,
    )

    assert first.iloc[0]["inference_status"] == "disabled"
    assert second.iloc[0]["inference_status"] == "skipped_cache_hit"
    assert "provider_name" in second.columns
    assert second.iloc[0]["provider_attempt_count"] == 0


def test_build_conversation_enrichment_accepts_mocked_llm_success(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_ENABLE_LLM_ENRICHMENT", "1")

    silver_messages = pd.DataFrame(
        [
            {
                "conversation_id": "conv_1",
                "lead_key": "lead_1",
                "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
                "message_id": "m1",
                "direction": "inbound",
                "message_type": "text",
                "message_body_masked": "Porto fez mais barato, quero comparar",
                "mentions_vehicle": False,
                "mentions_competitor": True,
                "mentions_sinistro": False,
                "quoted_price": 2500.0,
                "metadata_response_time_sec": 60.0,
                "contains_email": False,
                "contains_phone": False,
                "contains_cpf": False,
                "contains_cep": False,
                "contains_plate": False,
                "price_objection_signal": True,
                "urgency_strength": 1,
                "competitor_comparison_signal": True,
                "competitor_mentioned": "porto_seguro",
            }
        ]
    )

    def fake_infer(payload, compiled_plan):
        del payload
        del compiled_plan
        return {
            "status": "success",
            "provider_name": "openai",
            "model_name": "gpt-5-mini",
            "output": {
                "sentiment_label": "neutro",
                "sentiment_confidence_band": "moderado",
                "intent_stage": "pesquisa_mercado",
                "persona_profile": "cotador_comparador",
                "audience_segment": "oferta_competitiva",
                "price_objection_intensity": "forte",
                "competitor_pressure_level": "alta",
                "commercial_urgency_signal": "moderada",
                "recommended_next_action": "reforcar_diferenciais_e_retirar_objecao_preco",
                "explanation_short": "Lead compara proposta concorrente com interesse ativo.",
            },
            "validation_error": None,
            "provider_errors": [],
            "attempted_providers": ["openai"],
        }

    monkeypatch.setattr(conversation_enrichment, "infer_conversation_semantics", fake_infer)
    enrichment = build_conversation_enrichment(
        silver_messages,
        {
            **get_default_compiled_plan(),
            "llm": {
                "enabled": True,
                "prompt_version": "v1",
                "providers": {
                    "openai": {"enabled": True, "model": "gpt-5-mini"},
                    "anthropic": {"enabled": True, "model": "claude-sonnet"},
                },
            },
        },
    )

    assert enrichment.iloc[0]["inference_status"] == "success"
    assert enrichment.iloc[0]["persona_profile"] == "cotador_comparador"
    assert enrichment.iloc[0]["provider_name"] == "openai"
    assert enrichment.iloc[0]["provider_attempt_count"] == 1


def test_build_conversation_enrichment_records_provider_fallback_metadata(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_ENABLE_LLM_ENRICHMENT", "1")

    silver_messages = pd.DataFrame(
        [
            {
                "conversation_id": "conv_1",
                "lead_key": "lead_1",
                "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
                "message_id": "m1",
                "direction": "inbound",
                "message_type": "text",
                "message_body_masked": "Porto fez mais barato, quero comparar",
                "mentions_vehicle": False,
                "mentions_competitor": True,
                "mentions_sinistro": False,
                "quoted_price": 2500.0,
                "metadata_response_time_sec": 60.0,
                "contains_email": False,
                "contains_phone": False,
                "contains_cpf": False,
                "contains_cep": False,
                "contains_plate": False,
                "price_objection_signal": True,
                "urgency_strength": 1,
                "competitor_comparison_signal": True,
                "competitor_mentioned": "porto_seguro",
            }
        ]
    )

    def fake_infer(payload, compiled_plan):
        del payload
        del compiled_plan
        return {
            "status": "success",
            "provider_name": "anthropic",
            "model_name": "claude-sonnet",
            "output": {
                "sentiment_label": "neutro",
                "sentiment_confidence_band": "moderado",
                "intent_stage": "pesquisa_mercado",
                "persona_profile": "cotador_comparador",
                "audience_segment": "oferta_competitiva",
                "price_objection_intensity": "forte",
                "competitor_pressure_level": "alta",
                "commercial_urgency_signal": "moderada",
                "recommended_next_action": "reforcar_diferenciais_e_retirar_objecao_preco",
                "explanation_short": "Lead compara proposta concorrente com interesse ativo.",
            },
            "validation_error": None,
            "provider_errors": [
                {
                    "provider": "openai",
                    "kind": "invalid_output",
                    "detail": "invalid_intent_stage:<empty>",
                }
            ],
            "attempted_providers": ["openai", "anthropic"],
        }

    monkeypatch.setattr(conversation_enrichment, "infer_conversation_semantics", fake_infer)
    enrichment = build_conversation_enrichment(
        silver_messages,
        {
            **get_default_compiled_plan(),
            "llm": {
                "enabled": True,
                "prompt_version": "v1",
                "providers": {
                    "openai": {"enabled": True, "model": "gpt-5-mini"},
                    "anthropic": {"enabled": True, "model": "claude-sonnet"},
                },
            },
        },
    )

    assert enrichment.iloc[0]["inference_status"] == "success"
    assert enrichment.iloc[0]["provider_name"] == "anthropic"
    assert enrichment.iloc[0]["provider_attempt_count"] == 2
    assert (
        "openai:invalid_output:invalid_intent_stage:<empty>"
        in enrichment.iloc[0]["provider_error_summary"]
    )


def test_consolidate_gold_semantics_respects_dominance_and_recency() -> None:
    enrichment = pd.DataFrame(
        [
            {
                "conversation_id": "conv_1",
                "lead_key": "lead_1",
                "conversation_last_message_at": pd.Timestamp("2026-02-01 10:00:00"),
                "intent_stage": "descoberta_inicial",
                "persona_profile": "lead_frio",
                "audience_segment": "nutricao_basica",
                "price_objection_intensity": "nenhuma",
                "competitor_pressure_level": "nenhuma",
                "commercial_urgency_signal": "nenhuma",
                "sentiment_label": "neutro",
                "sentiment_confidence_band": "fraco",
            },
            {
                "conversation_id": "conv_2",
                "lead_key": "lead_1",
                "conversation_last_message_at": pd.Timestamp("2026-02-03 10:00:00"),
                "intent_stage": "pesquisa_mercado",
                "persona_profile": "cotador_comparador",
                "audience_segment": "oferta_competitiva",
                "price_objection_intensity": "forte",
                "competitor_pressure_level": "alta",
                "commercial_urgency_signal": "moderada",
                "sentiment_label": "neutro",
                "sentiment_confidence_band": "moderado",
            },
        ]
    )

    consolidated = consolidate_gold_semantics(enrichment)

    assert consolidated.iloc[0]["persona_profile"] == "cotador_comparador"
    assert consolidated.iloc[0]["audience_segment"] == "oferta_competitiva"
    assert consolidated.iloc[0]["price_objection_intensity"] == "forte"
    assert consolidated.iloc[0]["conversation_sentiment_label"] == "neutro"
    assert consolidated.iloc[0]["conversation_sentiment_support"] == "moderado"


def test_build_gold_derives_positive_sentiment_from_inbound_cues() -> None:
    bronze = pd.DataFrame(
        [
            {
                "message_id": "m1",
                "conversation_id": "conv_1",
                "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
                "direction": "outbound",
                "sender_phone": "+5511991111111",
                "sender_name": "Diego",
                "message_type": "text",
                "message_body": "Posso te enviar a proposta?",
                "status": "delivered",
                "channel": "whatsapp",
                "campaign_id": "camp_1",
                "agent_id": "agent_1",
                "conversation_outcome": "proposta_enviada",
                "metadata": '{"city":"Sao Paulo","state":"SP","response_time_sec":null}',
            },
            {
                "message_id": "m2",
                "conversation_id": "conv_1",
                "timestamp": pd.Timestamp("2026-02-01 10:02:00"),
                "direction": "inbound",
                "sender_phone": "+5511982222222",
                "sender_name": "Ana",
                "message_type": "text",
                "message_body": "Perfeito, obrigado, pode seguir com a proposta",
                "status": "read",
                "channel": "whatsapp",
                "campaign_id": "camp_1",
                "agent_id": "agent_1",
                "conversation_outcome": "proposta_enviada",
                "metadata": '{"city":"Sao Paulo","state":"SP","response_time_sec":60}',
            },
        ]
    )

    gold = build_gold(build_silver_leads(build_silver(bronze)), build_silver(bronze))

    assert gold.iloc[0]["conversation_sentiment_label"] == "positivo"
    assert gold.iloc[0]["conversation_sentiment_support"] == "forte"
    assert gold.iloc[0]["positive_tone_hits"] >= 3
    assert gold.iloc[0]["negative_tone_hits"] == 0


def test_build_gold_derives_negative_sentiment_from_inbound_cues() -> None:
    bronze = pd.DataFrame(
        [
            {
                "message_id": "m1",
                "conversation_id": "conv_1",
                "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
                "direction": "outbound",
                "sender_phone": "+5511991111111",
                "sender_name": "Diego",
                "message_type": "text",
                "message_body": "Posso te ajudar com a cotacao?",
                "status": "delivered",
                "channel": "whatsapp",
                "campaign_id": "camp_1",
                "agent_id": "agent_1",
                "conversation_outcome": "em_negociacao",
                "metadata": '{"city":"Sao Paulo","state":"SP","response_time_sec":null}',
            },
            {
                "message_id": "m2",
                "conversation_id": "conv_1",
                "timestamp": pd.Timestamp("2026-02-01 10:02:00"),
                "direction": "inbound",
                "sender_phone": "+5511982222222",
                "sender_name": "Ana",
                "message_type": "text",
                "message_body": "Muito caro, estou desconfiada e quero cancelar",
                "status": "read",
                "channel": "whatsapp",
                "campaign_id": "camp_1",
                "agent_id": "agent_1",
                "conversation_outcome": "cancelado",
                "metadata": '{"city":"Sao Paulo","state":"SP","response_time_sec":60}',
            },
        ]
    )

    gold = build_gold(build_silver_leads(build_silver(bronze)), build_silver(bronze))

    assert gold.iloc[0]["conversation_sentiment_label"] == "negativo"
    assert gold.iloc[0]["conversation_sentiment_support"] == "forte"
    assert gold.iloc[0]["negative_tone_hits"] >= 3
    assert gold.iloc[0]["positive_tone_hits"] == 0


def test_build_gold_derives_neutral_sentiment_from_mixed_inbound_cues() -> None:
    bronze = pd.DataFrame(
        [
            {
                "message_id": "m1",
                "conversation_id": "conv_1",
                "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
                "direction": "outbound",
                "sender_phone": "+5511991111111",
                "sender_name": "Diego",
                "message_type": "text",
                "message_body": "Posso te ajudar com a cotacao?",
                "status": "delivered",
                "channel": "whatsapp",
                "campaign_id": "camp_1",
                "agent_id": "agent_1",
                "conversation_outcome": "em_negociacao",
                "metadata": '{"city":"Sao Paulo","state":"SP","response_time_sec":null}',
            },
            {
                "message_id": "m2",
                "conversation_id": "conv_1",
                "timestamp": pd.Timestamp("2026-02-01 10:02:00"),
                "direction": "inbound",
                "sender_phone": "+5511982222222",
                "sender_name": "Ana",
                "message_type": "text",
                "message_body": "Obrigado, mas ainda estou desconfiada",
                "status": "read",
                "channel": "whatsapp",
                "campaign_id": "camp_1",
                "agent_id": "agent_1",
                "conversation_outcome": "em_negociacao",
                "metadata": '{"city":"Sao Paulo","state":"SP","response_time_sec":60}',
            },
        ]
    )

    gold = build_gold(build_silver_leads(build_silver(bronze)), build_silver(bronze))

    assert gold.iloc[0]["conversation_sentiment_label"] == "neutro"
    assert gold.iloc[0]["conversation_sentiment_support"] == "moderado"
    assert gold.iloc[0]["positive_tone_hits"] == 1
    assert gold.iloc[0]["negative_tone_hits"] == 1


def test_build_gold_keeps_provider_null_and_sem_evidencia_without_supporting_data() -> None:
    bronze = pd.DataFrame(
        [
            {
                "message_id": "m1",
                "conversation_id": "conv_1",
                "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
                "direction": "inbound",
                "sender_phone": "+5511981111111",
                "sender_name": "Carlos",
                "message_type": "text",
                "message_body": "quero saber mais",
                "status": "read",
                "channel": "whatsapp",
                "campaign_id": "camp_1",
                "agent_id": "agent_1",
                "conversation_outcome": "em_negociacao",
                "metadata": '{"device":"iphone","city":"Campinas","state":"SP"}',
            }
        ]
    )

    silver_messages = build_silver(bronze)
    silver_leads = build_silver_leads(silver_messages)
    gold = build_gold(silver_leads, silver_messages)

    assert pd.isna(gold.iloc[0]["avg_response_time_sec"])
    assert gold.iloc[0]["response_latency_band"] == "sem_evidencia"
    assert pd.isna(gold.iloc[0]["dominant_email_provider"])
    assert gold.iloc[0]["closure_outcome_group"] == "aberto"
    assert bool(gold.iloc[0]["has_closed_outcome"]) is False
    assert gold.iloc[0]["price_objection_intensity"] == "nenhuma"
    assert gold.iloc[0]["commercial_urgency_signal"] == "nenhuma"
    assert gold.iloc[0]["competitor_pressure_level"] == "nenhuma"
    assert gold.iloc[0]["conversation_sentiment_label"] == "sem_evidencia"
    assert gold.iloc[0]["conversation_sentiment_support"] == "sem_evidencia"
