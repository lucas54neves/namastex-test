from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypedDict, cast

import pandas as pd

from pipeline.playbooks import get_playbook, safe_auto_apply_playbooks
from pipeline.publication import sanitize_for_publication
from pipeline.quality import (
    summarize_validation_results,
    validate_gold,
    validate_silver,
    validate_silver_messages,
)
from pipeline.transforms import build_gold, build_silver, build_silver_leads


@dataclass(frozen=True)
class AgentDiagnosis:
    kind: str
    severity: str
    summary: str
    auto_remediable: bool
    suggested_action: str
    playbook_id: str | None
    decision_reason: str
    considered_playbooks: list[str]
    source: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "severity": self.severity,
            "summary": self.summary,
            "auto_remediable": self.auto_remediable,
            "suggested_action": self.suggested_action,
            "playbook_id": self.playbook_id,
            "decision_reason": self.decision_reason,
            "considered_playbooks": self.considered_playbooks,
            "source": self.source,
        }


class ValidationCheckConfig(TypedDict):
    kind: str
    severity: str
    playbook_id: str | None
    suggested_action: str


VALIDATION_CHECK_MAP = {
    ("bronze", "required_columns"): {
        "kind": "source_schema_drift",
        "severity": "high",
        "playbook_id": "update_pipeline_spec",
        "suggested_action": "Verificar schema da Bronze e adaptar a spec ou bloquear a ingestão.",
    },
    ("bronze", "message_id_unique"): {
        "kind": "source_duplication",
        "severity": "high",
        "playbook_id": None,
        "suggested_action": "Investigar duplicidade na fonte e introduzir chave estável na Bronze.",
    },
    ("bronze", "channel_whatsapp_only"): {
        "kind": "unexpected_channel",
        "severity": "medium",
        "playbook_id": None,
        "suggested_action": (
            "Filtrar canais não suportados ou expandir o pipeline para múltiplos canais."
        ),
    },
    ("silver", "required_columns"): {
        "kind": "silver_schema_break",
        "severity": "high",
        "playbook_id": "rebuild_silver_from_bronze",
        "suggested_action": "Reconstruir a Silver a partir da Bronze com a spec atual.",
    },
    ("silver", "lead_key_unique"): {
        "kind": "silver_lead_identity_break",
        "severity": "high",
        "playbook_id": "rebuild_silver_from_bronze",
        "suggested_action": (
            "Reconstruir a Silver principal e recalcular a identidade estável do lead."
        ),
    },
    ("silver", "lead_timestamps_not_null"): {
        "kind": "silver_timestamp_parse_failure",
        "severity": "high",
        "playbook_id": "quarantine_invalid_records",
        "suggested_action": "Isolar registros inválidos e reconstruir Silver e Gold.",
    },
    ("silver", "lead_counts_non_negative"): {
        "kind": "silver_aggregate_corruption",
        "severity": "medium",
        "playbook_id": "rebuild_silver_from_bronze",
        "suggested_action": "Reagregar a Silver principal a partir da Silver de mensagens.",
    },
    ("silver", "masked_text_fields_not_leaking"): {
        "kind": "pii_masking_leak",
        "severity": "high",
        "playbook_id": "rebuild_silver_from_bronze",
        "suggested_action": "Reconstruir a Silver principal reaplicando contrato publish-safe.",
    },
    ("silver_messages", "required_columns"): {
        "kind": "silver_schema_break",
        "severity": "high",
        "playbook_id": "rebuild_silver_from_bronze",
        "suggested_action": "Reconstruir a Silver auxiliar a partir da Bronze com a spec atual.",
    },
    ("silver_messages", "timestamp_not_null"): {
        "kind": "silver_timestamp_parse_failure",
        "severity": "high",
        "playbook_id": "quarantine_invalid_records",
        "suggested_action": "Isolar registros inválidos e reconstruir Silver e Gold.",
    },
    ("silver_messages", "dedupe_keys_unique"): {
        "kind": "silver_deduplication_failure",
        "severity": "medium",
        "playbook_id": "rebuild_silver_from_bronze",
        "suggested_action": "Reaplicar deduplicação semântica na Silver e recalcular a Gold.",
    },
    ("silver_messages", "masked_email_not_leaking"): {
        "kind": "pii_masking_leak",
        "severity": "high",
        "playbook_id": "rebuild_silver_from_bronze",
        "suggested_action": "Reconstruir a Silver reaplicando mascaramento antes de publicar.",
    },
    ("silver_messages", "masked_phone_not_leaking"): {
        "kind": "pii_masking_leak",
        "severity": "high",
        "playbook_id": "rebuild_silver_from_bronze",
        "suggested_action": "Reconstruir a Silver reaplicando mascaramento antes de publicar.",
    },
    ("silver_messages", "masked_cpf_not_leaking"): {
        "kind": "pii_masking_leak",
        "severity": "high",
        "playbook_id": "rebuild_silver_from_bronze",
        "suggested_action": "Reconstruir a Silver reaplicando mascaramento antes de publicar.",
    },
    ("silver_messages", "masked_cep_not_leaking"): {
        "kind": "pii_masking_leak",
        "severity": "high",
        "playbook_id": "rebuild_silver_from_bronze",
        "suggested_action": "Reconstruir a Silver reaplicando mascaramento antes de publicar.",
    },
    ("silver_messages", "masked_plate_not_leaking"): {
        "kind": "pii_masking_leak",
        "severity": "high",
        "playbook_id": "rebuild_silver_from_bronze",
        "suggested_action": "Reconstruir a Silver reaplicando mascaramento antes de publicar.",
    },
    ("silver_messages", "forbidden_raw_columns_absent"): {
        "kind": "silver_publication_policy_violation",
        "severity": "high",
        "playbook_id": "rebuild_silver_from_bronze",
        "suggested_action": "Reconstruir a Silver e reaplicar a política de publicação segura.",
    },
    ("silver_messages", "required_safe_columns_present"): {
        "kind": "silver_publication_schema_break",
        "severity": "high",
        "playbook_id": "rebuild_silver_from_bronze",
        "suggested_action": "Reconstruir a Silver e restaurar as colunas mascaradas obrigatórias.",
    },
    ("silver_messages", "vehicle_mentions_consistent"): {
        "kind": "silver_feature_inconsistency",
        "severity": "medium",
        "playbook_id": "rebuild_silver_from_bronze",
        "suggested_action": "Recalcular features derivadas da Silver a partir da Bronze.",
    },
    ("gold", "required_columns"): {
        "kind": "gold_schema_break",
        "severity": "high",
        "playbook_id": "rebuild_gold_from_silver",
        "suggested_action": "Reconstruir a Gold a partir da Silver válida.",
    },
    ("gold", "lead_key_unique"): {
        "kind": "gold_aggregation_duplication",
        "severity": "high",
        "playbook_id": "rebuild_gold_from_silver",
        "suggested_action": "Reexecutar agregação da Gold a partir da Silver.",
    },
    ("gold", "conversation_count_non_negative"): {
        "kind": "gold_metric_corruption",
        "severity": "high",
        "playbook_id": "rebuild_gold_from_silver",
        "suggested_action": "Recalcular contagens consolidadas da Gold a partir da Silver.",
    },
    ("gold", "message_totals_non_negative"): {
        "kind": "gold_metric_corruption",
        "severity": "high",
        "playbook_id": "rebuild_gold_from_silver",
        "suggested_action": "Recalcular métricas agregadas da Gold a partir da Silver.",
    },
    ("gold", "forbidden_raw_columns_absent"): {
        "kind": "gold_publication_policy_violation",
        "severity": "high",
        "playbook_id": "rebuild_gold_from_silver",
        "suggested_action": "Reconstruir a Gold e reaplicar a política de publicação segura.",
    },
    ("gold", "masked_text_fields_not_leaking"): {
        "kind": "pii_masking_leak",
        "severity": "high",
        "playbook_id": "rebuild_gold_from_silver",
        "suggested_action": "Reconstruir a Gold e revisar qualquer campo textual publicado.",
    },
    ("gold", "engagement_bucket_valid"): {
        "kind": "gold_bucket_invalid",
        "severity": "medium",
        "playbook_id": "rebuild_gold_from_silver",
        "suggested_action": "Recalcular buckets analíticos da Gold a partir da Silver.",
    },
    ("gold", "duplicate_events_removed_non_negative"): {
        "kind": "gold_metric_corruption",
        "severity": "high",
        "playbook_id": "rebuild_gold_from_silver",
        "suggested_action": "Recalcular métricas agregadas da Gold a partir da Silver.",
    },
}


def diagnose_validation_failures(
    failed_checks: list[dict[str, Any]], compiled_plan: dict[str, Any]
) -> list[AgentDiagnosis]:
    diagnoses: list[AgentDiagnosis] = []
    safe_playbooks = safe_auto_apply_playbooks(compiled_plan)
    for failed in failed_checks:
        layer = str(failed.get("layer", "unknown"))
        check = str(failed.get("check", "unknown"))
        mapped = cast(
            ValidationCheckConfig,
            VALIDATION_CHECK_MAP.get(
                (layer, check),
                {
                    "kind": "unknown_validation_failure",
                    "severity": "high",
                    "playbook_id": None,
                    "suggested_action": (
                        "Inspecionar a falha manualmente e revisar o contrato da camada afetada."
                    ),
                },
            ),
        )
        playbook_id = mapped["playbook_id"]
        auto_remediable = bool(playbook_id and playbook_id in safe_playbooks)
        considered_playbooks = [playbook_id] if playbook_id else []
        decision_reason = (
            f"Falha {layer}.{check} mapeada para playbook {playbook_id}."
            if playbook_id
            else f"Falha {layer}.{check} sem playbook seguro configurado."
        )
        diagnoses.append(
            AgentDiagnosis(
                kind=str(mapped["kind"]),
                severity=str(mapped["severity"]),
                summary=f"Falha de validação em {layer}.{check}",
                auto_remediable=auto_remediable,
                suggested_action=str(mapped["suggested_action"]),
                playbook_id=str(playbook_id) if playbook_id else None,
                decision_reason=decision_reason,
                considered_playbooks=considered_playbooks,
                source=failed,
            )
        )
    return diagnoses


def diagnose_exception(exc: Exception) -> AgentDiagnosis:
    text = f"{type(exc).__name__}: {exc}"
    lowered = text.lower()
    if "no such file" in lowered or "filenotfounderror" in lowered:
        return AgentDiagnosis(
            kind="source_not_found",
            severity="high",
            summary="Arquivo de origem indisponível para leitura.",
            auto_remediable=False,
            suggested_action="Verificar presença do arquivo Bronze esperado antes da execução.",
            playbook_id=None,
            decision_reason="Erro indica ausência da fonte; não há remediação automática segura.",
            considered_playbooks=[],
            source={"exception": text},
        )
    if "json" in lowered:
        return AgentDiagnosis(
            kind="metadata_parse_failure",
            severity="high",
            summary="Falha ao parsear metadata JSON.",
            auto_remediable=False,
            suggested_action="Inspecionar registros com metadata inválido "
            "e adicionar tratamento defensivo.",
            playbook_id="quarantine_invalid_records",
            decision_reason=(
                "Erro sugere metadata inválido; a quarentena pode isolar registros ruins."
            ),
            considered_playbooks=["quarantine_invalid_records"],
            source={"exception": text},
        )
    return AgentDiagnosis(
        kind="unexpected_runtime_error",
        severity="high",
        summary="Erro inesperado durante a execução do pipeline.",
        auto_remediable=False,
        suggested_action="Inspecionar stacktrace e preservar último estado "
        "bem-sucedido até correção.",
        playbook_id="fallback_to_last_successful_artifacts",
        decision_reason="Erro inesperado em runtime; o fallback é a ação operacional mais segura.",
        considered_playbooks=["fallback_to_last_successful_artifacts"],
        source={"exception": text},
    )


def attempt_auto_remediation(
    bronze_df: pd.DataFrame,
    silver_df: pd.DataFrame,
    silver_messages_df: pd.DataFrame,
    gold_df: pd.DataFrame,
    failed_checks: list[dict[str, Any]],
    compiled_plan: dict[str, Any],
) -> dict[str, Any]:
    repaired_silver_runtime = silver_df
    repaired_silver_messages_runtime = silver_messages_df
    repaired_gold_runtime = gold_df
    actions: list[str] = []
    touched_silver = False
    touched_gold = False
    decisions: list[dict[str, Any]] = []
    safe_playbooks = safe_auto_apply_playbooks(compiled_plan)

    for failed in failed_checks:
        layer = failed.get("layer")
        check = failed.get("check")
        mapped = cast(
            ValidationCheckConfig | None, VALIDATION_CHECK_MAP.get((str(layer), str(check)))
        )
        playbook_id = mapped["playbook_id"] if mapped else None
        if not playbook_id:
            continue
        playbook = get_playbook(playbook_id)
        decisions.append(
            {
                "playbook": playbook.as_dict(),
                "failed_check": failed,
                "selected": playbook_id in safe_playbooks,
            }
        )
        if playbook_id not in safe_playbooks:
            continue
        if playbook_id == "rebuild_silver_from_bronze":
            repaired_silver_messages_runtime = build_silver(bronze_df, compiled_plan=compiled_plan)
            repaired_silver_runtime = build_silver_leads(repaired_silver_messages_runtime)
            actions.append(f"rebuild_silver_for_{check}")
            touched_silver = True
        if playbook_id == "quarantine_invalid_records":
            repaired_silver_messages_runtime = build_silver(bronze_df, compiled_plan=compiled_plan)
            repaired_silver_runtime = build_silver_leads(repaired_silver_messages_runtime)
            actions.append(f"rebuild_silver_after_quarantine_for_{check}")
            touched_silver = True
        if playbook_id == "rebuild_gold_from_silver":
            touched_gold = True

    if touched_gold and not touched_silver:
        repaired_silver_messages_runtime = build_silver(bronze_df, compiled_plan=compiled_plan)
        repaired_silver_runtime = build_silver_leads(repaired_silver_messages_runtime)

    if touched_silver or touched_gold:
        repaired_gold_runtime = build_gold(
            repaired_silver_runtime,
            repaired_silver_messages_runtime,
            compiled_plan=compiled_plan,
        )
        actions.append("rebuild_gold_from_silver")

    repaired_silver = sanitize_for_publication(repaired_silver_runtime, "silver")
    repaired_silver_messages = sanitize_for_publication(
        repaired_silver_messages_runtime,
        "silver_messages",
    )
    repaired_gold = sanitize_for_publication(repaired_gold_runtime, "gold")
    validation_summary = summarize_validation_results(
        validate_silver(repaired_silver, compiled_plan=compiled_plan)
        + validate_silver_messages(repaired_silver_messages, compiled_plan=compiled_plan)
        + validate_gold(repaired_gold, compiled_plan=compiled_plan)
    )
    return {
        "silver_df": repaired_silver,
        "silver_messages_df": repaired_silver_messages,
        "gold_df": repaired_gold,
        "actions": actions,
        "decisions": decisions,
        "validation_summary": validation_summary,
        "resolved": validation_summary["status"] == "passed",
    }
