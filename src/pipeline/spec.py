from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from pipeline.io import write_json

DEFAULT_PIPELINE_SPEC: dict[str, Any] = {
    "version": 1,
    "bronze": {
        "required_columns": [
            "message_id",
            "conversation_id",
            "timestamp",
            "direction",
            "sender_phone",
            "sender_name",
            "message_type",
            "message_body",
            "status",
            "channel",
            "campaign_id",
            "agent_id",
            "conversation_outcome",
            "metadata",
        ],
        "supported_channels": ["whatsapp"],
        "fingerprint_strategy": "path_size_mtime",
        "quarantine": {
            "invalid_timestamp": True,
            "invalid_metadata": True,
        },
    },
    "silver": {
        "lead_required_columns": [
            "lead_key",
            "canonical_lead_name_masked",
            "lead_contact_ref",
            "first_seen_at",
            "last_seen_at",
            "conversation_count",
            "message_count",
            "observed_campaign_ids",
            "observed_lead_sources",
            "observed_outcomes",
            "has_vehicle_signal",
            "has_competitor_signal",
            "has_sinistro_signal",
            "has_email_signal",
            "has_phone_signal",
            "has_cpf_signal",
            "has_cep_signal",
            "has_plate_signal",
        ],
        "message_required_columns": [
            "lead_key",
            "message_body_masked",
            "sender_name_masked",
            "sender_phone_masked",
            "dropped_duplicate_events",
            "mentions_vehicle",
            "mentions_competitor",
            "mentions_sinistro",
        ],
        "dedupe_keys": [
            "conversation_id",
            "timestamp",
            "direction",
            "sender_phone",
            "message_type",
            "message_body",
        ],
        "metadata_fields": [
            "device",
            "city",
            "state",
            "response_time_sec",
            "is_business_hours",
            "lead_source",
        ],
    },
    "gold": {
        "required_columns": [
            "conversation_id",
            "total_messages",
            "duplicate_events_removed",
            "engagement_bucket",
            "data_shared_score",
            "persona_profile",
            "audience_segment",
            "lead_temperature",
            "price_sensitivity",
            "intent_stage",
            "contact_readiness",
            "risk_signal",
        ],
        "valid_buckets": ["lead_frio", "curta", "media", "longa"],
        "valid_personas": [
            "lead_frio",
            "cliente_pos_sinistro",
            "cotador_comparador",
            "lead_engajado_com_dados",
        ],
        "valid_audiences": [
            "nutricao_basica",
            "retencao_pos_sinistro",
            "oferta_competitiva",
            "close_comercial",
        ],
        "valid_temperatures": ["frio", "morno", "quente"],
        "segmentation": {
            "lead_temperature": {
                "default": "morno",
                "cold_label": "frio",
                "cold_bucket": "lead_frio",
                "cold_data_shared_score": 0,
                "hot_label": "quente",
                "hot_buckets": ["media", "longa"],
                "hot_min_data_shared_score": 2,
            },
            "price_sensitivity": {
                "default": "baixa",
                "high_label": "alta",
            },
            "contact_readiness": {
                "default": "baixa",
                "medium_label": "media",
                "medium_score": 1,
                "high_label": "alta",
                "high_min_score": 2,
            },
            "intent_stage": {
                "default": "descoberta_inicial",
                "sinistro_label": "pos_sinistro",
                "competitor_label": "pesquisa_mercado",
                "quoted_price_label": "cotacao_ativa",
            },
            "risk_signal": {
                "default": "baixo",
                "medium_label": "medio",
                "high_label": "alto",
            },
            "personas": {
                "default": "lead_frio",
                "sinistro": "cliente_pos_sinistro",
                "comparator": "cotador_comparador",
                "engaged": "lead_engajado_com_dados",
            },
            "audiences": {
                "default": "nutricao_basica",
                "sinistro": "retencao_pos_sinistro",
                "comparator": "oferta_competitiva",
                "engaged": "close_comercial",
            },
        },
    },
    "quality": {
        "bronze_checks": [
            "required_columns",
            "message_id_unique",
            "channel_whatsapp_only",
            "first_message_outbound_ratio",
        ],
        "silver_checks": [
            "required_columns",
            "forbidden_raw_columns_absent",
            "required_safe_columns_present",
            "timestamp_not_null",
            "dedupe_keys_unique",
            "masked_email_not_leaking",
            "masked_phone_not_leaking",
            "masked_cpf_not_leaking",
            "masked_cep_not_leaking",
            "masked_plate_not_leaking",
            "vehicle_mentions_consistent",
        ],
        "gold_checks": [
            "required_columns",
            "forbidden_raw_columns_absent",
            "masked_text_fields_not_leaking",
            "conversation_id_unique",
            "message_totals_non_negative",
            "engagement_bucket_valid",
            "duplicate_events_removed_non_negative",
            "persona_profile_valid",
            "audience_segment_valid",
            "lead_temperature_valid",
            "persona_profile_not_null",
        ],
    },
    "agent": {
        "safe_auto_apply_playbooks": [
            "rebuild_silver_from_bronze",
            "rebuild_gold_from_silver",
            "quarantine_invalid_records",
            "fallback_to_last_successful_artifacts",
        ],
        "planner": {
            "auto_apply_safe_updates": False,
        },
    },
    "llm": {
        "enabled": False,
        "provider": None,
    },
}


def default_pipeline_spec() -> dict[str, Any]:
    return copy.deepcopy(DEFAULT_PIPELINE_SPEC)


def validate_pipeline_spec(spec: dict[str, Any]) -> None:
    required_sections = {"bronze", "silver", "gold", "quality", "agent", "llm"}
    missing_sections = sorted(required_sections - set(spec))
    if missing_sections:
        raise ValueError(f"Missing required spec sections: {missing_sections}")

    list_paths = [
        ("bronze", "required_columns"),
        ("bronze", "supported_channels"),
        ("silver", "lead_required_columns"),
        ("silver", "message_required_columns"),
        ("silver", "dedupe_keys"),
        ("gold", "required_columns"),
        ("gold", "valid_buckets"),
        ("gold", "valid_personas"),
        ("gold", "valid_audiences"),
        ("gold", "valid_temperatures"),
    ]
    for section, key in list_paths:
        value = spec.get(section, {}).get(key)
        if not isinstance(value, list) or not value:
            raise ValueError(f"Spec field {section}.{key} must be a non-empty list")


def load_pipeline_spec(path: Path) -> dict[str, Any]:
    if not path.exists():
        return default_pipeline_spec()
    spec = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(spec, dict):
        raise ValueError(f"Expected JSON object in {path}")
    validate_pipeline_spec(spec)
    return spec


def ensure_pipeline_spec(path: Path) -> dict[str, Any]:
    spec = load_pipeline_spec(path)
    if not path.exists():
        write_json(spec, path)
    return spec


def save_pipeline_spec(spec: dict[str, Any], path: Path) -> None:
    validate_pipeline_spec(spec)
    write_json(spec, path)
