from __future__ import annotations

import pandas as pd

import pipeline.transforms.conversation_enrichment as conversation_enrichment
from pipeline.orchestration.compiler import get_default_compiled_plan
from pipeline.transforms.bronze import (
    build_bronze,
)
from pipeline.transforms.conversation_enrichment import (
    build_conversation_enrichment,
    consolidate_gold_semantics,
)
from pipeline.transforms.gold import build_gold
from pipeline.transforms.gold_macro import build_gold_macro
from pipeline.transforms.silver import (
    add_conversation_context,
    add_gold_segments,
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

    def fake_infer(payload, compiled_plan, prompt_resolution=None):
        del payload
        del compiled_plan
        del prompt_resolution
        return {
            "status": "success",
            "provider_name": "openai",
            "model_name": "gpt-5-mini",
            "prompt_source": "local",
            "prompt_name": None,
            "prompt_label": None,
            "prompt_version": "v1",
            "trace_id": None,
            "output": {
                "sentiment_label": "neutral",
                "sentiment_confidence_band": "high",
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
    assert enrichment.iloc[0]["sentiment_label"] == "neutro"
    assert enrichment.iloc[0]["sentiment_confidence_band"] == "forte"
    assert enrichment.iloc[0]["persona_profile"] == "cotador_comparador"
    assert enrichment.iloc[0]["provider_name"] == "openai"
    assert enrichment.iloc[0]["provider_attempt_count"] == 1
    assert enrichment.iloc[0]["prompt_source"] == "local"


def test_build_conversation_enrichment_rejects_unmappable_llm_values(monkeypatch) -> None:
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

    def fake_infer(payload, compiled_plan, prompt_resolution=None):
        del payload
        del compiled_plan
        del prompt_resolution
        return {
            "status": "success",
            "provider_name": "openai",
            "model_name": "gpt-5-mini",
            "prompt_source": "local",
            "prompt_name": None,
            "prompt_label": None,
            "prompt_version": "v1",
            "trace_id": None,
            "output": {
                "sentiment_label": "skeptical",
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

    assert enrichment.iloc[0]["inference_status"] == "invalid_output"
    assert enrichment.iloc[0]["validation_error"] == "invalid_sentiment_label:skeptical"
    assert enrichment.iloc[0]["fallback_reason"] == "invalid_llm_response"
    assert enrichment.iloc[0]["sentiment_label"] != "skeptical"


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

    def fake_infer(payload, compiled_plan, prompt_resolution=None):
        del payload
        del compiled_plan
        del prompt_resolution
        return {
            "status": "success",
            "provider_name": "anthropic",
            "model_name": "claude-sonnet",
            "prompt_source": "local",
            "prompt_name": None,
            "prompt_label": None,
            "prompt_version": "v1",
            "trace_id": "trace_123",
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
    assert enrichment.iloc[0]["trace_id"] == "trace_123"


def test_build_conversation_enrichment_disables_managed_prompt_lookup_after_first_failure(
    monkeypatch,
) -> None:
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
            },
            {
                "conversation_id": "conv_2",
                "lead_key": "lead_2",
                "timestamp": pd.Timestamp("2026-02-01 10:01:00"),
                "message_id": "m2",
                "direction": "inbound",
                "message_type": "text",
                "message_body_masked": "quero cotacao tambem",
                "mentions_vehicle": False,
                "mentions_competitor": False,
                "mentions_sinistro": False,
                "quoted_price": None,
                "metadata_response_time_sec": 30.0,
                "contains_email": False,
                "contains_phone": False,
                "contains_cpf": False,
                "contains_cep": False,
                "contains_plate": False,
                "price_objection_signal": False,
                "urgency_strength": 0,
                "competitor_comparison_signal": False,
                "competitor_mentioned": None,
            },
        ]
    )
    observed_sources: list[str] = []

    def fake_resolve_runtime_prompt(payload, compiled_plan, config=None):
        del payload
        del compiled_plan
        assert config is not None
        if config["langfuse"]["enabled"]:
            observed_sources.append("managed")
            return {
                "source": "local_fallback",
                "prompt_text": "local fallback",
                "prompt_name": "conversation-enrichment-v1",
                "prompt_label": "production",
                "prompt_version": "v1",
                "langfuse_prompt_ref": None,
                "resolution_error": "prompt_lookup_failed",
            }
        observed_sources.append("local")
        return {
            "source": "local",
            "prompt_text": "local prompt",
            "prompt_name": None,
            "prompt_label": None,
            "prompt_version": "v1",
            "langfuse_prompt_ref": None,
            "resolution_error": None,
        }

    def fake_infer(payload, compiled_plan, prompt_resolution=None):
        del payload
        del compiled_plan
        assert prompt_resolution is not None
        return {
            "status": "provider_error",
            "provider_name": None,
            "model_name": "deterministic_fallback",
            "prompt_source": prompt_resolution["source"],
            "prompt_name": prompt_resolution["prompt_name"],
            "prompt_label": prompt_resolution["prompt_label"],
            "prompt_version": prompt_resolution["prompt_version"],
            "trace_id": None,
            "output": None,
            "validation_error": None,
            "provider_errors": [],
            "attempted_providers": [],
        }

    monkeypatch.setattr(
        conversation_enrichment,
        "resolve_runtime_prompt",
        fake_resolve_runtime_prompt,
    )
    monkeypatch.setattr(conversation_enrichment, "infer_conversation_semantics", fake_infer)

    enrichment = build_conversation_enrichment(
        silver_messages,
        {
            **get_default_compiled_plan(),
            "llm": {
                "enabled": True,
                "prompt_version": "v1",
                "langfuse": {
                    "enabled": True,
                    "prompt_name": "conversation-enrichment-v1",
                    "prompt_label": "production",
                    "allow_local_prompt_fallback": True,
                },
                "providers": {
                    "openai": {"enabled": True, "model": "gpt-5-mini"},
                    "anthropic": {"enabled": True, "model": "claude-sonnet"},
                },
            },
        },
    )

    assert observed_sources == ["managed", "local"]
    assert enrichment["prompt_source"].tolist() == ["local_fallback", "local"]


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
                "provider_name": None,
                "inference_status": "disabled",
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
                "provider_name": "openai",
                "inference_status": "success",
            },
        ]
    )

    consolidated = consolidate_gold_semantics(enrichment)

    assert consolidated.iloc[0]["persona_profile"] == "cotador_comparador"
    assert consolidated.iloc[0]["audience_segment"] == "oferta_competitiva"
    assert consolidated.iloc[0]["conversation_sentiment_label"] == "neutro"
    assert consolidated.iloc[0]["conversation_sentiment_support"] == "moderado"
    assert consolidated.iloc[0]["conversation_sentiment_source_family"] == "mixed"


def test_consolidate_gold_semantics_keeps_audience_under_high_competitor_pressure() -> None:
    enrichment = pd.DataFrame(
        [
            {
                "conversation_id": "conv_1",
                "lead_key": "lead_1",
                "conversation_last_message_at": pd.Timestamp("2026-02-03 10:00:00"),
                "intent_stage": "pesquisa_mercado",
                "persona_profile": "lead_frio",
                "audience_segment": "nutricao_basica",
                "price_objection_intensity": "forte",
                "competitor_pressure_level": "alta",
                "commercial_urgency_signal": "moderada",
                "sentiment_label": "neutro",
                "sentiment_confidence_band": "moderado",
                "provider_name": None,
                "inference_status": "disabled",
            }
        ]
    )

    consolidated = consolidate_gold_semantics(enrichment)

    assert consolidated.iloc[0]["persona_profile"] == "lead_frio"
    assert consolidated.iloc[0]["audience_segment"] == "nutricao_basica"


def test_build_gold_falls_back_persona_audience_pair_when_llm_pair_is_incomplete() -> None:
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
    silver_conversations_llm = pd.DataFrame(
        [
            {
                "conversation_id": "conv_1",
                "lead_key": silver_leads.iloc[0]["lead_key"],
                "conversation_last_message_at": pd.Timestamp("2026-02-01 10:00:00"),
                "intent_stage": "descoberta_inicial",
                "persona_profile": "lead_frio",
                "audience_segment": "",
                "price_objection_intensity": "nenhuma",
                "competitor_pressure_level": "alta",
                "commercial_urgency_signal": "nenhuma",
                "sentiment_label": "sem_evidencia",
                "sentiment_confidence_band": "sem_evidencia",
                "provider_name": "openai",
                "inference_status": "success",
            }
        ]
    )

    gold = build_gold(silver_leads, silver_messages, silver_conversations_llm)

    assert gold.iloc[0]["persona_profile"] == "lead_frio"
    assert gold.iloc[0]["audience_segment"] == "nutricao_basica"
    assert gold.iloc[0]["persona_profile_source_family"] == "deterministic_fallback"
    assert gold.iloc[0]["audience_segment_source_family"] == "deterministic_fallback"


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


def test_build_gold_ignores_outbound_only_price_objection_and_urgency_language() -> None:
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
                "message_body": "posso montar a cotacao e preciso fechar hoje",
                "status": "delivered",
                "channel": "whatsapp",
                "campaign_id": "camp_1",
                "agent_id": "agent_1",
                "conversation_outcome": "em_negociacao",
                "metadata": '{"city":"Sao Paulo","state":"SP"}',
            },
            {
                "message_id": "m2",
                "conversation_id": "conv_1",
                "timestamp": pd.Timestamp("2026-02-01 10:01:00"),
                "direction": "inbound",
                "sender_phone": "+5511982222222",
                "sender_name": "Ana",
                "message_type": "text",
                "message_body": "obrigado",
                "status": "read",
                "channel": "whatsapp",
                "campaign_id": "camp_1",
                "agent_id": "agent_1",
                "conversation_outcome": "em_negociacao",
                "metadata": '{"city":"Sao Paulo","state":"SP","response_time_sec":60}',
            },
        ]
    )

    silver_messages = build_silver(bronze)
    silver_leads = build_silver_leads(silver_messages)
    gold = build_gold(silver_leads, silver_messages)

    assert bool(silver_messages.iloc[0]["price_objection_signal_outbound"]) is True
    assert silver_messages["price_objection_signal_inbound"].sum() == 0
    assert silver_messages["urgency_strength_inbound"].max() == 0
    assert gold.iloc[0]["price_objection_intensity"] == "nenhuma"
    assert gold.iloc[0]["commercial_urgency_signal"] == "nenhuma"


def test_build_gold_keeps_commercial_severity_deterministic_when_llm_disagrees() -> None:
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
                "message_body": "podemos seguir com a cotacao?",
                "status": "delivered",
                "channel": "whatsapp",
                "campaign_id": "camp_1",
                "agent_id": "agent_1",
                "conversation_outcome": "em_negociacao",
                "metadata": '{"city":"Sao Paulo","state":"SP"}',
            },
            {
                "message_id": "m2",
                "conversation_id": "conv_1",
                "timestamp": pd.Timestamp("2026-02-01 10:05:00"),
                "direction": "inbound",
                "sender_phone": "+5511982222222",
                "sender_name": "Ana",
                "message_type": "text",
                "message_body": "quero analisar e te respondo depois",
                "status": "read",
                "channel": "whatsapp",
                "campaign_id": "camp_1",
                "agent_id": "agent_1",
                "conversation_outcome": "em_negociacao",
                "metadata": '{"city":"Sao Paulo","state":"SP","response_time_sec":300}',
            },
        ]
    )

    silver_messages = build_silver(bronze)
    silver_leads = build_silver_leads(silver_messages)
    silver_conversations_llm = pd.DataFrame(
        [
            {
                "conversation_id": "conv_1",
                "lead_key": silver_leads.iloc[0]["lead_key"],
                "conversation_last_message_at": pd.Timestamp("2026-02-01 10:05:00"),
                "intent_stage": "pesquisa_mercado",
                "persona_profile": "cotador_comparador",
                "audience_segment": "oferta_competitiva",
                "price_objection_intensity": "forte",
                "competitor_pressure_level": "alta",
                "commercial_urgency_signal": "alta",
                "sentiment_label": "neutro",
                "sentiment_confidence_band": "moderado",
                "provider_name": "openai",
                "inference_status": "success",
            }
        ]
    )

    gold = build_gold(silver_leads, silver_messages, silver_conversations_llm)

    assert gold.iloc[0]["intent_stage"] == "pesquisa_mercado"
    assert gold.iloc[0]["intent_stage_source_family"] == "llm_provider"
    assert gold.iloc[0]["price_objection_intensity"] == "nenhuma"
    assert gold.iloc[0]["commercial_urgency_signal"] == "nenhuma"
    assert gold.iloc[0]["competitor_pressure_level"] == "nenhuma"


def _make_gold_df(n: int = 4) -> pd.DataFrame:
    rows = []
    personas = ["lead_frio", "cotador_comparador", "lead_frio", "cliente_pos_sinistro"]
    audiences = [
        "nutricao_basica",
        "oferta_competitiva",
        "nutricao_basica",
        "retencao_pos_sinistro",
    ]
    temperatures = ["frio", "morno", "frio", "quente"]
    buckets = ["lead_frio", "curta", "lead_frio", "longa"]
    sentiments = ["sem_evidencia", "neutro", "positivo", "sem_evidencia"]
    closures = ["aberto", "fechado", "aberto", "aberto"]
    competitors = ["nenhuma", "leve", "nenhuma", "nenhuma"]
    prices = ["nenhuma", "forte", "nenhuma", "nenhuma"]
    urgencies = ["nenhuma", "moderada", "nenhuma", "nenhuma"]
    intents = ["descoberta_inicial", "pesquisa_mercado", "descoberta_inicial", "pos_sinistro"]
    email_providers = ["gmail", None, None, "outlook"]
    contains_email = [True, False, False, True]
    for i in range(n):
        rows.append(
            {
                "lead_key": f"lead_{i}",
                "persona_profile": personas[i],
                "audience_segment": audiences[i],
                "lead_temperature": temperatures[i],
                "engagement_bucket": buckets[i],
                "conversation_sentiment_label": sentiments[i],
                "closure_outcome_group": closures[i],
                "competitor_pressure_level": competitors[i],
                "price_objection_intensity": prices[i],
                "commercial_urgency_signal": urgencies[i],
                "intent_stage": intents[i],
                "dominant_email_provider": email_providers[i],
                "contains_email": contains_email[i],
                "total_messages": 2 + i,
                "conversation_count": 1,
                "data_shared_score": i,
                "mentioned_competitor": i == 1,
                "mentioned_sinistro": i == 3,
                "has_closed_outcome": i == 1,
            }
        )
    return pd.DataFrame(rows)


class TestBuildGoldMacro:
    def test_contains_all_required_dimensions(self) -> None:
        gold_df = _make_gold_df()
        macro = build_gold_macro(gold_df)

        expected_dimensions = {
            "persona_profile",
            "audience_segment",
            "dominant_email_provider",
            "lead_temperature",
            "engagement_bucket",
            "conversation_sentiment_label",
            "closure_outcome_group",
            "competitor_pressure_level",
            "price_objection_intensity",
            "commercial_urgency_signal",
            "intent_stage",
            "numeric_snapshot",
        }
        present = set(macro["dimension"].unique())
        assert expected_dimensions.issubset(present)

    def test_rank_1_has_highest_lead_count_per_dimension(self) -> None:
        gold_df = _make_gold_df()
        macro = build_gold_macro(gold_df)

        for dimension in macro["dimension"].unique():
            if dimension == "numeric_snapshot":
                continue
            dim_rows = macro[macro["dimension"] == dimension].sort_values("rank")
            top = dim_rows.iloc[0]
            rest = dim_rows.iloc[1:]
            assert all(top["lead_count"] >= r["lead_count"] for _, r in rest.iterrows())

    def test_lead_count_sum_equals_total_leads(self) -> None:
        gold_df = _make_gold_df()
        macro = build_gold_macro(gold_df)
        total = len(gold_df)

        for dimension in macro["dimension"].unique():
            if dimension in ("numeric_snapshot", "dominant_email_provider"):
                continue
            dim_sum = macro[macro["dimension"] == dimension]["lead_count"].sum()
            assert dim_sum == total

    def test_lead_pct_sums_approximately_to_one(self) -> None:
        gold_df = _make_gold_df()
        macro = build_gold_macro(gold_df)

        # dominant_email_provider is filtered by contains_email, so its pct sums to the email share
        skip = {"numeric_snapshot", "dominant_email_provider"}
        for dimension in macro["dimension"].unique():
            if dimension in skip:
                continue
            pct_sum = macro[macro["dimension"] == dimension]["lead_pct"].sum()
            assert 0.99 <= pct_sum <= 1.01

    def test_null_dimension_value_becomes_sem_informacao(self) -> None:
        gold_df = _make_gold_df().copy()
        gold_df.loc[0, "intent_stage"] = None
        macro = build_gold_macro(gold_df)

        intent_values = set(macro[macro["dimension"] == "intent_stage"]["dimension_value"])
        assert "sem_informacao" in intent_values
        assert macro[macro["dimension"] == "intent_stage"]["dimension_value"].isna().sum() == 0

    def test_no_pii_identifier_columns(self) -> None:
        gold_df = _make_gold_df()
        macro = build_gold_macro(gold_df)

        forbidden_substrings = ("lead_key", "contact_ref", "name_masked")
        for col in macro.columns:
            assert not any(sub in col for sub in forbidden_substrings)

    def test_numeric_snapshot_total_leads_correct(self) -> None:
        gold_df = _make_gold_df()
        macro = build_gold_macro(gold_df)

        snap = macro[macro["dimension"] == "numeric_snapshot"]
        total_row = snap[snap["dimension_value"] == "total_leads"]
        assert len(total_row) == 1
        assert float(total_row.iloc[0]["metric_value"]) == float(len(gold_df))

    def test_dominant_email_provider_filtered_by_contains_email(self) -> None:
        gold_df = _make_gold_df()
        macro = build_gold_macro(gold_df)

        email_rows = macro[macro["dimension"] == "dominant_email_provider"]
        email_leads = gold_df[gold_df["contains_email"].astype(bool)]
        assert email_rows["lead_count"].sum() == len(email_leads)

    def test_required_columns_present(self) -> None:
        gold_df = _make_gold_df()
        macro = build_gold_macro(gold_df)

        required = {
            "dimension",
            "dimension_value",
            "lead_count",
            "lead_pct",
            "rank",
            "computed_at_utc",
        }
        assert required.issubset(set(macro.columns))

    def test_sorted_by_dimension_and_rank(self) -> None:
        gold_df = _make_gold_df()
        macro = build_gold_macro(gold_df)

        categorical_rows = macro[macro["dimension"] != "numeric_snapshot"]
        assert list(categorical_rows["dimension"]) == sorted(categorical_rows["dimension"].tolist())
        for dimension in categorical_rows["dimension"].unique():
            ranks = macro[macro["dimension"] == dimension]["rank"].tolist()
            assert ranks == sorted(ranks)


# ---------------------------------------------------------------------------
# Bronze enrichment tests (spec-architecture-bronze-layer-enrichment.md)
# ---------------------------------------------------------------------------

_FIXTURE_PARQUET = "docs/conversations_bronze.parquet"


def _make_minimal_bronze_parquet(tmp_path, rows=None):
    if rows is None:
        rows = [
            {
                "message_id": "m1",
                "conversation_id": "conv_1",
                "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
                "sender_phone": "+5511999999999",
                "sender_name": "Ana",
                "message_body": "quero cotacao",
                "campaign_id": "camp_1",
                "agent_id": "agent_1",
                "direction": "inbound",
                "message_type": "text",
                "status": "read",
                "channel": "whatsapp",
                "conversation_outcome": "em_negociacao",
                "metadata": (
                    '{"device":"iphone","city":"SP","state":"SP",'
                    '"response_time_sec":60,"is_business_hours":true,"lead_source":"google"}'
                ),
            }
        ]
    df = pd.DataFrame(rows)
    path = tmp_path / "bronze.parquet"
    df.to_parquet(path, index=False)
    return str(path)


def test_build_bronze_casts_categoricals(tmp_path) -> None:
    path = _make_minimal_bronze_parquet(tmp_path)
    result = build_bronze(path)
    assert result.df["direction"].dtype.name == "category"
    cats = list(result.df["direction"].cat.categories)
    assert cats == ["outbound", "inbound"]


def test_build_bronze_expands_metadata(tmp_path) -> None:
    path = _make_minimal_bronze_parquet(tmp_path)
    result = build_bronze(path)
    assert "metadata_response_time_sec" in result.df.columns
    assert str(result.df["metadata_response_time_sec"].dtype) == "Int64"


def test_build_bronze_validation_report_ok(tmp_path) -> None:
    path = _make_minimal_bronze_parquet(tmp_path)
    result = build_bronze(path)
    assert result.validation_report.schema_ok is True
    assert result.validation_report.missing_columns == []


def test_build_bronze_malformed_metadata_no_exception(tmp_path) -> None:
    rows = [
        {
            "message_id": "m1",
            "conversation_id": "conv_1",
            "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
            "sender_phone": "+5511999999999",
            "sender_name": "Ana",
            "message_body": "ok",
            "campaign_id": "camp_1",
            "agent_id": "agent_1",
            "direction": "inbound",
            "message_type": "text",
            "status": "read",
            "channel": "whatsapp",
            "conversation_outcome": "em_negociacao",
            "metadata": '{"device":"iphone"}',
        },
        {
            "message_id": "m2",
            "conversation_id": "conv_2",
            "timestamp": pd.Timestamp("2026-02-01 10:01:00"),
            "sender_phone": "+5511888888888",
            "sender_name": "Bruno",
            "message_body": "ok",
            "campaign_id": "camp_1",
            "agent_id": "agent_1",
            "direction": "outbound",
            "message_type": "text",
            "status": "sent",
            "channel": "whatsapp",
            "conversation_outcome": "em_negociacao",
            "metadata": "not_valid_json",
        },
    ]
    path = _make_minimal_bronze_parquet(tmp_path, rows=rows)
    result = build_bronze(path)
    assert pd.isna(result.df["metadata_device"].iloc[1])


def test_build_bronze_missing_column_report(tmp_path) -> None:
    rows = [
        {
            "message_id": "m1",
            "conversation_id": "conv_1",
            "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
            "sender_phone": "+5511999999999",
            "sender_name": "Ana",
            "message_body": "ok",
            "campaign_id": "camp_1",
            "agent_id": "agent_1",
            "message_type": "text",
            "status": "read",
            "channel": "whatsapp",
            "conversation_outcome": "em_negociacao",
            "metadata": '{"device":"iphone"}',
        }
    ]
    path = _make_minimal_bronze_parquet(tmp_path, rows=rows)
    result = build_bronze(path)
    assert result.validation_report.schema_ok is False
    assert "direction" in result.validation_report.missing_columns


def test_build_bronze_drops_raw_metadata(tmp_path) -> None:
    path = _make_minimal_bronze_parquet(tmp_path)
    result = build_bronze(path)
    assert "metadata" not in result.df.columns


def test_silver_still_exports_load_bronze_frame() -> None:
    from pipeline.transforms.silver import load_bronze_frame, parse_metadata  # noqa: F401

    assert callable(load_bronze_frame)
    assert callable(parse_metadata)


def test_silver_module_has_facade_docstring() -> None:
    import pipeline.transforms.silver as silver_mod

    assert silver_mod.__doc__ is not None
    assert "facade" in silver_mod.__doc__.lower()
    assert "build_silver" in silver_mod.__doc__
