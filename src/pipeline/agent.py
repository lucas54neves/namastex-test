from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from pipeline.quality import summarize_validation_results, validate_gold, validate_silver
from pipeline.transforms import build_gold, build_silver


@dataclass(frozen=True)
class AgentDiagnosis:
    kind: str
    severity: str
    summary: str
    auto_remediable: bool
    suggested_action: str
    source: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "severity": self.severity,
            "summary": self.summary,
            "auto_remediable": self.auto_remediable,
            "suggested_action": self.suggested_action,
            "source": self.source,
        }


VALIDATION_CHECK_MAP = {
    ("bronze", "required_columns"): (
        "source_schema_drift",
        "high",
        False,
        "Verificar schema da Bronze e adaptar o parser ou bloquear "
        "a ingestão até alinhar o contrato.",
    ),
    ("bronze", "message_id_unique"): (
        "source_duplication",
        "high",
        False,
        "Investigar duplicidade na fonte e introduzir chave de "
        "deduplicação estável antes da Bronze.",
    ),
    ("bronze", "channel_whatsapp_only"): (
        "unexpected_channel",
        "medium",
        False,
        "Filtrar canais não suportados ou expandir o pipeline para múltiplos canais.",
    ),
    ("silver", "required_columns"): (
        "silver_schema_break",
        "high",
        True,
        "Reconstruir a Silver a partir da Bronze com as transformações atuais.",
    ),
    ("silver", "timestamp_not_null"): (
        "silver_timestamp_parse_failure",
        "high",
        True,
        "Reconstruir a Silver e descartar ou isolar registros com timestamp inválido.",
    ),
    ("silver", "dedupe_keys_unique"): (
        "silver_deduplication_failure",
        "medium",
        True,
        "Reaplicar deduplicação semântica na Silver e recalcular a Gold.",
    ),
    ("silver", "masked_cpf_not_leaking"): (
        "pii_masking_leak",
        "high",
        True,
        "Reconstruir a Silver reaplicando mascaramento e bloquear "
        "publicação até remover vazamento.",
    ),
    ("silver", "vehicle_mentions_consistent"): (
        "silver_feature_inconsistency",
        "medium",
        True,
        "Recalcular features derivadas da Silver a partir da Bronze.",
    ),
    ("gold", "required_columns"): (
        "gold_schema_break",
        "high",
        True,
        "Reconstruir a Gold a partir da Silver válida.",
    ),
    ("gold", "conversation_id_unique"): (
        "gold_aggregation_duplication",
        "high",
        True,
        "Reexecutar agregação da Gold a partir da Silver.",
    ),
    ("gold", "message_totals_non_negative"): (
        "gold_metric_corruption",
        "high",
        True,
        "Recalcular métricas agregadas da Gold a partir da Silver.",
    ),
    ("gold", "engagement_bucket_valid"): (
        "gold_bucket_invalid",
        "medium",
        True,
        "Recalcular buckets analíticos da Gold a partir da Silver.",
    ),
    ("gold", "duplicate_events_removed_non_negative"): (
        "gold_metric_corruption",
        "high",
        True,
        "Recalcular métricas agregadas da Gold a partir da Silver.",
    ),
}


def diagnose_validation_failures(failed_checks: list[dict[str, Any]]) -> list[AgentDiagnosis]:
    diagnoses: list[AgentDiagnosis] = []
    for failed in failed_checks:
        layer = str(failed.get("layer", "unknown"))
        check = str(failed.get("check", "unknown"))
        kind, severity, auto_remediable, suggested_action = VALIDATION_CHECK_MAP.get(
            (layer, check),
            (
                "unknown_validation_failure",
                "high",
                False,
                "Inspecionar a falha manualmente e revisar o contrato da camada afetada.",
            ),
        )
        diagnoses.append(
            AgentDiagnosis(
                kind=kind,
                severity=severity,
                summary=f"Falha de validação em {layer}.{check}",
                auto_remediable=auto_remediable,
                suggested_action=suggested_action,
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
            source={"exception": text},
        )
    return AgentDiagnosis(
        kind="unexpected_runtime_error",
        severity="high",
        summary="Erro inesperado durante a execução do pipeline.",
        auto_remediable=False,
        suggested_action="Inspecionar stacktrace e preservar último estado "
        "bem-sucedido até correção.",
        source={"exception": text},
    )


def attempt_auto_remediation(
    bronze_df: pd.DataFrame,
    silver_df: pd.DataFrame,
    gold_df: pd.DataFrame,
    failed_checks: list[dict[str, Any]],
) -> dict[str, Any]:
    repaired_silver = silver_df
    repaired_gold = gold_df
    actions: list[str] = []
    touched_silver = False
    touched_gold = False

    for failed in failed_checks:
        layer = failed.get("layer")
        check = failed.get("check")
        if layer == "silver" and check in {
            "required_columns",
            "timestamp_not_null",
            "dedupe_keys_unique",
            "masked_cpf_not_leaking",
            "vehicle_mentions_consistent",
        }:
            repaired_silver = build_silver(bronze_df)
            actions.append(f"rebuild_silver_for_{check}")
            touched_silver = True
        if layer == "gold" and check in {
            "required_columns",
            "conversation_id_unique",
            "message_totals_non_negative",
            "engagement_bucket_valid",
            "duplicate_events_removed_non_negative",
        }:
            touched_gold = True

    if touched_silver or touched_gold:
        repaired_gold = build_gold(repaired_silver)
        actions.append("rebuild_gold_from_silver")

    validation_summary = summarize_validation_results(
        validate_silver(repaired_silver) + validate_gold(repaired_gold)
    )
    return {
        "silver_df": repaired_silver,
        "gold_df": repaired_gold,
        "actions": actions,
        "validation_summary": validation_summary,
        "resolved": validation_summary["status"] == "passed",
    }
