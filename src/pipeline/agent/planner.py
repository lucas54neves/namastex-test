from __future__ import annotations

import copy
import hashlib
import json
import re
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from pipeline.agent.approval import (
    APPROVAL_STATUS_APPROVED,
    APPROVAL_STATUS_PENDING,
    APPROVAL_STATUS_REJECTED,
    get_proposal_approval_status,
)
from pipeline.agent.llm_advisor import get_llm_advice
from pipeline.config import PipelinePaths
from pipeline.io.parquet_io import read_json, write_json
from pipeline.orchestration.compiler import compile_pipeline_spec
from pipeline.runtime.spec import load_pipeline_spec, save_pipeline_spec

PROPOSAL_FAMILY_SCHEMA_UPDATE = "schema_update"
PROPOSAL_FAMILY_VALIDATION = "validation_enhancement"
PROPOSAL_FAMILY_DERIVED = "derived_column_addition"
PROPOSAL_FAMILY_SEGMENTATION = "segmentation_adjustment"
PROPOSAL_FAMILY_TRANSFORMATION = "transformation_rule_change"

PROPOSAL_STATUS_PROPOSED = "proposed"
PROPOSAL_STATUS_APPROVED = "approved"
PROPOSAL_STATUS_APPLIED = "applied"
PROPOSAL_STATUS_REJECTED = "rejected"
PROPOSAL_STATUS_NOT_APPLICABLE = "not_applicable"

_CAMEL_CASE_PATTERN = re.compile(r"(?<!^)(?=[A-Z])")


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _parse_metadata_objects(bronze_df: pd.DataFrame) -> tuple[list[dict[str, Any]], int]:
    parsed_objects: list[dict[str, Any]] = []
    invalid_count = 0
    for value in bronze_df["metadata"].dropna().head(500):
        try:
            parsed = json.loads(str(value))
        except (TypeError, ValueError, json.JSONDecodeError):
            invalid_count += 1
            continue
        if isinstance(parsed, dict):
            parsed_objects.append(parsed)
    return parsed_objects, invalid_count


def _discover_metadata_fields(metadata_objects: list[dict[str, Any]]) -> list[str]:
    discovered: set[str] = set()
    for parsed in metadata_objects:
        discovered.update(str(key) for key in parsed.keys())
    return sorted(discovered)


def _normalize_key_name(value: str) -> str:
    normalized = _CAMEL_CASE_PATTERN.sub("_", value)
    normalized = re.sub(r"[\s\-]+", "_", normalized)
    normalized = re.sub(r"[^a-zA-Z0-9_]", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_").lower()


def _build_context(context_type: str, summary: str, evidence: dict[str, Any]) -> dict[str, Any]:
    return {
        "context_type": context_type,
        "summary": summary,
        "evidence": evidence,
    }


def _proposal_fingerprint(
    proposal_type: str,
    proposal_family: str,
    proposed_change: dict[str, Any],
    items: list[str],
) -> str:
    payload = {
        "proposal_type": proposal_type,
        "proposal_family": proposal_family,
        "proposed_change": proposed_change,
        "items": items,
    }
    return hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:12]


def _build_proposal(
    planning_run_id: str,
    proposal_type: str,
    proposal_family: str,
    title: str,
    context_detected: dict[str, Any],
    proposed_change: dict[str, Any],
    expected_impact: str,
    risk: str,
    impact_scope: str,
    requires_approval: bool,
    rationale: str,
    affected_layers: list[str],
    affected_artifacts: list[str],
    privacy_impact: str,
    items: list[str] | None = None,
) -> dict[str, Any]:
    proposal_items = items or []
    proposal_id = "proposal_" + _proposal_fingerprint(
        proposal_type,
        proposal_family,
        proposed_change,
        proposal_items,
    )
    return {
        "proposal_id": proposal_id,
        "planning_run_id": planning_run_id,
        "proposal_type": proposal_type,
        "proposal_family": proposal_family,
        "title": title,
        "context_detected": context_detected,
        "proposed_change": proposed_change,
        "expected_impact": expected_impact,
        "risk": risk,
        "impact_scope": impact_scope,
        "requires_approval": requires_approval,
        "safe_auto_apply": False,
        "recommendation_only": True,
        "rationale": rationale,
        "affected_layers": affected_layers,
        "affected_artifacts": affected_artifacts,
        "privacy_impact": privacy_impact,
        "status": PROPOSAL_STATUS_PROPOSED,
        "items": proposal_items,
    }


def _apply_supported_proposals(
    spec: dict[str, Any], proposals: list[dict[str, Any]]
) -> tuple[dict[str, Any], list[str], list[str]]:
    updated_spec = copy.deepcopy(spec)
    applied_types: list[str] = []
    applied_ids: list[str] = []

    for proposal in proposals:
        proposal_type = str(proposal["proposal_type"])
        proposal_id = str(proposal["proposal_id"])
        items = list(proposal.get("items", []))
        if proposal_type == "bronze_required_columns_addition" and items:
            updated_spec["bronze"]["required_columns"] = sorted(
                set(updated_spec["bronze"]["required_columns"]) | set(items)
            )
            applied_types.append(proposal_type)
            applied_ids.append(proposal_id)
        elif proposal_type == "silver_metadata_fields_addition" and items:
            updated_spec["silver"]["metadata_fields"] = sorted(
                set(updated_spec["silver"]["metadata_fields"]) | set(items)
            )
            applied_types.append(proposal_type)
            applied_ids.append(proposal_id)

    return updated_spec, applied_types, applied_ids


def _schema_proposals(
    planning_run_id: str,
    spec: dict[str, Any],
    observed_columns: list[str],
    observed_metadata_fields: list[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    contexts: list[dict[str, Any]] = []
    proposals: list[dict[str, Any]] = []

    missing_columns = sorted(set(observed_columns) - set(spec["bronze"]["required_columns"]))
    if missing_columns:
        context = _build_context(
            "bronze_schema_drift",
            "Novas colunas foram observadas na Bronze fora do contrato atual.",
            {"items": missing_columns, "observed_count": len(missing_columns)},
        )
        contexts.append(context)
        proposals.append(
            _build_proposal(
                planning_run_id=planning_run_id,
                proposal_type="bronze_required_columns_addition",
                proposal_family=PROPOSAL_FAMILY_SCHEMA_UPDATE,
                title="Declarar novas colunas obrigatorias na Bronze",
                context_detected=context,
                proposed_change={
                    "target_path": "bronze.required_columns",
                    "operation": "add_items",
                    "items": missing_columns,
                },
                expected_impact=(
                    "Alinha o contrato da Bronze ao schema observado "
                    "e melhora a auditoria de drift."
                ),
                risk="medium",
                impact_scope="bronze",
                requires_approval=True,
                rationale=(
                    "Novas colunas na Bronze mudam o contrato de ingestao e exigem revisao humana."
                ),
                affected_layers=["bronze"],
                affected_artifacts=[
                    "config/pipeline_spec.json",
                    "reports/monitoring/latest_plan_report.json",
                ],
                privacy_impact="sensitive_detection",
                items=missing_columns,
            )
        )

    missing_metadata_fields = sorted(
        set(observed_metadata_fields) - set(spec["silver"].get("metadata_fields", []))
    )
    if missing_metadata_fields:
        context = _build_context(
            "silver_metadata_field_drift",
            "Novos campos de metadata foram observados fora do contrato publicado da Silver.",
            {"items": missing_metadata_fields, "observed_count": len(missing_metadata_fields)},
        )
        contexts.append(context)
        proposals.append(
            _build_proposal(
                planning_run_id=planning_run_id,
                proposal_type="silver_metadata_fields_addition",
                proposal_family=PROPOSAL_FAMILY_SCHEMA_UPDATE,
                title="Declarar novos campos de metadata na Silver",
                context_detected=context,
                proposed_change={
                    "target_path": "silver.metadata_fields",
                    "operation": "add_items",
                    "items": missing_metadata_fields,
                },
                expected_impact=(
                    "Expande o contrato declarativo da Silver para cobrir "
                    "metadata observada no runtime."
                ),
                risk="low",
                impact_scope="silver",
                requires_approval=True,
                rationale=(
                    "Mesmo mudancas declarativas no contrato publicado da Silver "
                    "devem seguir aprovacao estrutural explicita."
                ),
                affected_layers=["silver"],
                affected_artifacts=[
                    "config/pipeline_spec.json",
                    "reports/monitoring/latest_plan_report.json",
                ],
                privacy_impact="none",
                items=missing_metadata_fields,
            )
        )

    return contexts, proposals


def _validation_proposals(
    planning_run_id: str,
    metadata_objects: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    boolean_string_fields: dict[str, int] = {}
    for metadata in metadata_objects:
        for key, value in metadata.items():
            if isinstance(value, str) and value.strip().lower() in {"true", "false", "sim", "nao"}:
                boolean_string_fields[str(key)] = boolean_string_fields.get(str(key), 0) + 1

    if not boolean_string_fields:
        return [], []

    items = sorted(boolean_string_fields)
    context = _build_context(
        "metadata_boolean_string_drift",
        "Campos booleanos em metadata estao chegando como texto e pedem validacao dedicada.",
        {"items": items, "string_value_count": sum(boolean_string_fields.values())},
    )
    proposal = _build_proposal(
        planning_run_id=planning_run_id,
        proposal_type="metadata_boolean_validation_addition",
        proposal_family=PROPOSAL_FAMILY_VALIDATION,
        title="Adicionar validacao para coercao de booleanos em metadata",
        context_detected=context,
        proposed_change={
            "target_path": "quality.validation_rules.silver",
            "operation": "register_rule",
            "rule_id": "metadata_boolean_normalized",
            "fields": items,
        },
        expected_impact=(
            "Reduz inconsistencias de tipagem antes da publicacao "
            "e evita regras derivadas ambiguas."
        ),
        risk="medium",
        impact_scope="silver",
        requires_approval=True,
        rationale=(
            "Campos booleanos serializados como texto indicam uma lacuna "
            "de qualidade que precisa ser explicitamente validada."
        ),
        affected_layers=["silver", "quality"],
        affected_artifacts=[
            "config/pipeline_spec.json",
            "reports/monitoring/latest_plan_report.json",
        ],
        privacy_impact="none",
        items=items,
    )
    return [context], [proposal]


def _derived_column_proposals(
    planning_run_id: str,
    spec: dict[str, Any],
    metadata_objects: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    has_business_hours_signal = any(
        "is_business_hours" in metadata for metadata in metadata_objects
    )
    gold_required_columns = set(spec["gold"]["required_columns"])
    if not has_business_hours_signal or "business_hours_message_ratio" in gold_required_columns:
        return [], []

    context = _build_context(
        "business_hours_signal_available",
        (
            "O runtime ja observa sinal de horario comercial em metadata, "
            "mas a Gold nao expõe agregacao equivalente."
        ),
        {"items": ["is_business_hours"], "observed_count": len(metadata_objects)},
    )
    proposal = _build_proposal(
        planning_run_id=planning_run_id,
        proposal_type="gold_business_hours_metric_addition",
        proposal_family=PROPOSAL_FAMILY_DERIVED,
        title="Adicionar metrica derivada de horario comercial na Gold",
        context_detected=context,
        proposed_change={
            "target_path": "gold.required_columns",
            "operation": "add_item",
            "item": "business_hours_message_ratio",
            "source_signal": "metadata.is_business_hours",
        },
        expected_impact=(
            "Permite analisar concentracao de interacoes em horario comercial "
            "com metrica deterministica."
        ),
        risk="medium",
        impact_scope="gold",
        requires_approval=True,
        rationale=(
            "Ja existe sinal suficiente na Bronze para derivar uma metrica "
            "adicional util sem depender de texto livre."
        ),
        affected_layers=["silver", "gold"],
        affected_artifacts=[
            "config/pipeline_spec.json",
            "reports/monitoring/latest_plan_report.json",
        ],
        privacy_impact="none",
        items=["business_hours_message_ratio"],
    )
    return [context], [proposal]


def _segmentation_proposals(
    planning_run_id: str,
    spec: dict[str, Any],
    bronze_df: pd.DataFrame,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    outcomes = bronze_df.get("conversation_outcome")
    if outcomes is None:
        return [], []

    normalized_outcomes = outcomes.dropna().astype(str).str.lower().str.strip()
    negotiation_count = int(normalized_outcomes.str.contains("negoci").sum())
    if negotiation_count == 0 or "negociacao_em_andamento" in set(
        spec["gold"]["valid_intent_stages"]
    ):
        return [], []

    context = _build_context(
        "negotiation_outcome_gap",
        (
            "Foi observado resultado de conversa ligado a negociacao "
            "sem classe explicita na segmentacao."
        ),
        {"items": ["em_negociacao"], "observed_count": negotiation_count},
    )
    proposal = _build_proposal(
        planning_run_id=planning_run_id,
        proposal_type="intent_stage_negotiation_extension",
        proposal_family=PROPOSAL_FAMILY_SEGMENTATION,
        title="Estender segmentacao de intent_stage para negociacao",
        context_detected=context,
        proposed_change={
            "target_path": "gold.segmentation.intent_stage",
            "operation": "extend_vocabulary",
            "new_label": "negociacao_em_andamento",
            "trigger": "conversation_outcome contains 'negoci'",
        },
        expected_impact=(
            "Separa leads em negociacao ativa de estagios mais iniciais "
            "e melhora leitura comercial da Gold."
        ),
        risk="high",
        impact_scope="gold",
        requires_approval=True,
        rationale=(
            "Alteracoes de segmentacao mudam interpretacao analitica "
            "e precisam de aprovacao explicita."
        ),
        affected_layers=["gold"],
        affected_artifacts=[
            "config/pipeline_spec.json",
            "reports/monitoring/latest_plan_report.json",
        ],
        privacy_impact="none",
        items=["negociacao_em_andamento"],
    )
    return [context], [proposal]


def _transformation_proposals(
    planning_run_id: str,
    metadata_objects: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    normalization_gaps: dict[str, str] = {}
    for metadata in metadata_objects:
        for key in metadata.keys():
            original = str(key)
            normalized = _normalize_key_name(original)
            if normalized and normalized != original:
                normalization_gaps[original] = normalized

    if not normalization_gaps:
        return [], []

    items = sorted(normalization_gaps)
    context = _build_context(
        "metadata_key_normalization_gap",
        (
            "Foram observadas chaves de metadata com variacoes de naming "
            "que deveriam seguir contrato deterministico."
        ),
        {"items": items, "normalized_mapping": normalization_gaps},
    )
    proposal = _build_proposal(
        planning_run_id=planning_run_id,
        proposal_type="metadata_key_normalization_rule",
        proposal_family=PROPOSAL_FAMILY_TRANSFORMATION,
        title="Explicitar regra de normalizacao de chaves de metadata",
        context_detected=context,
        proposed_change={
            "target_path": "silver.metadata_fields",
            "operation": "normalize_keys",
            "mapping": normalization_gaps,
        },
        expected_impact=(
            "Reduz duplicidade semantica e garante consistencia de nomenclatura "
            "nas colunas derivadas de metadata."
        ),
        risk="medium",
        impact_scope="cross_layer",
        requires_approval=True,
        rationale=(
            "Variacoes de naming geram inconsistencias de transformacao "
            "e devem virar regra explicita de normalizacao."
        ),
        affected_layers=["bronze", "silver"],
        affected_artifacts=[
            "config/pipeline_spec.json",
            "reports/monitoring/latest_plan_report.json",
        ],
        privacy_impact="none",
        items=items,
    )
    return [context], [proposal]


def plan_pipeline_spec(paths: PipelinePaths) -> dict[str, Any]:
    spec = load_pipeline_spec(paths.pipeline_spec)
    bronze_df = pd.read_parquet(paths.raw_bronze_source)
    metadata_objects, invalid_metadata_count = _parse_metadata_objects(bronze_df)
    observed_columns = sorted(bronze_df.columns.tolist())
    observed_metadata_fields = _discover_metadata_fields(metadata_objects)

    planning_run_id = f"planning_run_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}"
    detected_contexts = [
        _build_context(
            "bronze_observation_summary",
            "Resumo deterministico das observacoes de schema e metadata da Bronze.",
            {
                "observed_columns": observed_columns,
                "observed_metadata_fields": observed_metadata_fields,
                "record_count": int(len(bronze_df)),
                "invalid_metadata_count": invalid_metadata_count,
            },
        )
    ]
    proposals: list[dict[str, Any]] = []

    detector_results = [
        _schema_proposals(planning_run_id, spec, observed_columns, observed_metadata_fields),
        _validation_proposals(planning_run_id, metadata_objects),
        _derived_column_proposals(planning_run_id, spec, metadata_objects),
        _segmentation_proposals(planning_run_id, spec, bronze_df),
        _transformation_proposals(planning_run_id, metadata_objects),
    ]
    for contexts, generated_proposals in detector_results:
        detected_contexts.extend(contexts)
        proposals.extend(generated_proposals)

    approval_status_by_proposal = {
        str(proposal["proposal_id"]): get_proposal_approval_status(
            paths, str(proposal["proposal_id"])
        )
        for proposal in proposals
    }
    approved_proposal_ids = sorted(
        proposal_id
        for proposal_id, status in approval_status_by_proposal.items()
        if status == APPROVAL_STATUS_APPROVED
    )
    rejected_proposal_ids = sorted(
        proposal_id
        for proposal_id, status in approval_status_by_proposal.items()
        if status == APPROVAL_STATUS_REJECTED
    )
    requires_approval = any(bool(proposal["requires_approval"]) for proposal in proposals)
    approved = bool(proposals) and all(
        approval_status_by_proposal[str(proposal["proposal_id"])] == APPROVAL_STATUS_APPROVED
        for proposal in proposals
        if proposal["requires_approval"]
    )

    eligible_for_application = [
        proposal
        for proposal in proposals
        if approval_status_by_proposal[str(proposal["proposal_id"])] == APPROVAL_STATUS_APPROVED
    ]

    applied_proposal_types: list[str] = []
    applied_proposal_ids: list[str] = []
    active_spec = spec
    if eligible_for_application:
        updated_spec, applied_proposal_types, applied_proposal_ids = _apply_supported_proposals(
            spec, eligible_for_application
        )
        if applied_proposal_types:
            save_pipeline_spec(updated_spec, paths.pipeline_spec)
            history = read_json(paths.spec_history, default={"changes": []})
            history.setdefault("changes", []).append(
                {
                    "planning_run_id": planning_run_id,
                    "applied_at_utc": _utc_now_iso(),
                    "applied_proposal_ids": applied_proposal_ids,
                    "applied_proposal_types": applied_proposal_types,
                    "proposals": proposals,
                }
            )
            write_json(history, paths.spec_history)
            active_spec = updated_spec

    for proposal in proposals:
        proposal_id = str(proposal["proposal_id"])
        approval_status = approval_status_by_proposal[proposal_id]
        if proposal_id in applied_proposal_ids:
            proposal["status"] = PROPOSAL_STATUS_APPLIED
        elif approval_status == APPROVAL_STATUS_APPROVED:
            proposal["status"] = PROPOSAL_STATUS_APPROVED
        elif approval_status == APPROVAL_STATUS_REJECTED:
            proposal["status"] = PROPOSAL_STATUS_REJECTED
        elif approval_status == APPROVAL_STATUS_PENDING and proposal["requires_approval"]:
            proposal["status"] = PROPOSAL_STATUS_PROPOSED
        else:
            proposal["status"] = PROPOSAL_STATUS_NOT_APPLICABLE

    compiled_plan = compile_pipeline_spec(active_spec)
    llm_advice = get_llm_advice(
        {
            "observed_columns": observed_columns,
            "observed_metadata_fields": observed_metadata_fields,
            "detected_contexts": detected_contexts,
            "proposals": proposals,
        },
        compiled_plan,
    )

    report = {
        "generated_at_utc": _utc_now_iso(),
        "proposal_id": planning_run_id,
        "detected_contexts": detected_contexts,
        "observed_columns": observed_columns,
        "observed_metadata_fields": observed_metadata_fields,
        "proposals": proposals,
        "changes": proposals,
        "requires_approval": requires_approval,
        "approved": approved,
        "approved_proposal_ids": approved_proposal_ids,
        "rejected_proposal_ids": rejected_proposal_ids,
        "applied": bool(applied_proposal_ids),
        "applied_proposal_ids": applied_proposal_ids,
        "applied_proposal_types": applied_proposal_types,
        "llm_advice": llm_advice,
        "summary": {
            "proposal_count": len(proposals),
            "context_count": len(detected_contexts),
            "families": sorted({str(proposal["proposal_family"]) for proposal in proposals}),
            "status_counts": {
                PROPOSAL_STATUS_PROPOSED: sum(
                    1 for proposal in proposals if proposal["status"] == PROPOSAL_STATUS_PROPOSED
                ),
                PROPOSAL_STATUS_APPROVED: sum(
                    1 for proposal in proposals if proposal["status"] == PROPOSAL_STATUS_APPROVED
                ),
                PROPOSAL_STATUS_APPLIED: sum(
                    1 for proposal in proposals if proposal["status"] == PROPOSAL_STATUS_APPLIED
                ),
                PROPOSAL_STATUS_REJECTED: sum(
                    1 for proposal in proposals if proposal["status"] == PROPOSAL_STATUS_REJECTED
                ),
                PROPOSAL_STATUS_NOT_APPLICABLE: sum(
                    1
                    for proposal in proposals
                    if proposal["status"] == PROPOSAL_STATUS_NOT_APPLICABLE
                ),
            },
        },
    }
    write_json(report, paths.monitoring / "latest_plan_report.json")
    return report
