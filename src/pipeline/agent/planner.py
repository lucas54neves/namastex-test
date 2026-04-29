from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from pipeline.agent.approval import (
    APPROVAL_STATUS_APPROVED,
    APPROVAL_STATUS_REJECTED,
    approve_proposal,
    expire_rejection_cooloff,
    get_proposal_approval_record,
    get_proposal_approval_status,
    get_rejection_cooloff_record,
    increment_rejection_cooloff_cycle,
    is_in_rejection_cooloff,
    start_rejection_cooloff,
)
from pipeline.agent.autonomy import (
    DECISION_HOLD_FOR_APPROVAL,
    DECISION_PROMOTE,
    DECISION_REJECT,
    DECISION_STALE,
    PROPOSAL_STATUS_APPROVED,
    PROPOSAL_STATUS_AWAITING_APPROVAL,
    PROPOSAL_STATUS_CANDIDATE_MATERIALIZED,
    PROPOSAL_STATUS_CLOSED_NO_ACTION,
    PROPOSAL_STATUS_PROMOTED,
    PROPOSAL_STATUS_PROPOSED,
    PROPOSAL_STATUS_REJECTED,
    PROPOSAL_STATUS_STALE,
    PROPOSAL_STATUS_VALIDATION_FAILED,
    apply_proposal_to_spec,
    build_candidate_actions,
    classify_proposal,
    evaluate_candidate,
    get_agent_auto_approve_threshold,
    get_awaiting_approval_stale_policy,
    get_quality_drift_policy,
    get_rejection_cooloff_policy,
    load_proposal_record,
    persist_autonomy_decision,
    persist_candidate_artifacts,
    persist_proposal_record,
    promote_candidate_spec,
    update_autonomy_metrics,
)
from pipeline.agent.llm_advisor import agent_self_review_proposal, get_llm_advice
from pipeline.config import PipelinePaths
from pipeline.io.parquet_io import write_json
from pipeline.orchestration.compiler import compile_pipeline_spec
from pipeline.runtime.spec import load_pipeline_spec
from pipeline.runtime.state import (
    compute_quality_snapshot,
    load_pipeline_state,
    load_quality_baseline,
    save_pipeline_state,
)

PROPOSAL_FAMILY_SCHEMA_UPDATE = "schema_update"
PROPOSAL_FAMILY_VALIDATION = "validation_enhancement"
PROPOSAL_FAMILY_DERIVED = "derived_column_addition"
PROPOSAL_FAMILY_SEGMENTATION = "segmentation_adjustment"
PROPOSAL_FAMILY_TRANSFORMATION = "transformation_rule_change"
PROPOSAL_FAMILY_QUALITY_DRIFT = "data_quality_drift"

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
    paths: PipelinePaths,
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
    classification = classify_proposal(proposal_family, paths)
    requires_explicit_approval = classification["requires_approval"] or requires_approval
    return {
        "proposal_id": proposal_id,
        "planning_run_id": planning_run_id,
        "created_at_utc": _utc_now_iso(),
        "proposal_type": proposal_type,
        "proposal_family": proposal_family,
        "trigger_kind": str(context_detected.get("context_type", "runtime_observation")),
        "impact_class": classification["impact_class"],
        "safe_auto_promote": classification["safe_auto_promote"] and not requires_explicit_approval,
        "requires_approval": requires_explicit_approval,
        "status": PROPOSAL_STATUS_PROPOSED,
        "title": title,
        "context_detected": context_detected,
        "proposed_change": proposed_change,
        "expected_outcome": expected_impact,
        "expected_impact": expected_impact,
        "risk": risk,
        "impact_scope": impact_scope,
        "safe_auto_apply": classification["safe_auto_promote"] and not requires_explicit_approval,
        "recommendation_only": True,
        "rationale": rationale,
        "affected_layers": affected_layers,
        "affected_paths": affected_artifacts,
        "affected_artifacts": affected_artifacts,
        "evidence_bundle": {
            "context": context_detected,
            "items": proposal_items,
        },
        "candidate_actions": [],
        "approval_context": get_proposal_approval_record(paths, proposal_id),
        "policy_snapshot": classification["policy_snapshot"],
        "privacy_impact": privacy_impact,
        "items": proposal_items,
    }


def _schema_proposals(
    paths: PipelinePaths,
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
                paths=paths,
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
                paths=paths,
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
    paths: PipelinePaths,
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
        paths=paths,
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
        requires_approval=False,
        rationale=(
            "Campos booleanos serializados como texto indicam uma lacuna "
            "de qualidade que pode ser tratada de forma aditiva."
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
    paths: PipelinePaths,
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
        paths=paths,
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
        requires_approval=False,
        rationale=(
            "Ja existe sinal suficiente na Bronze para derivar uma metrica "
            "adicional util sem quebrar o contrato existente."
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
    paths: PipelinePaths,
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
        paths=paths,
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
    paths: PipelinePaths,
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
        paths=paths,
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


def _quality_drift_proposals(
    paths: PipelinePaths,
    planning_run_id: str,
    bronze_df: pd.DataFrame,
    baseline: dict[str, Any] | None,
    policy: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if baseline is None:
        return [], []

    drift_triggers: list[str] = []
    evidence: dict[str, Any] = {
        "current_record_count": int(len(bronze_df)),
        "baseline_record_count": int(baseline.get("record_count", 0)),
        "null_rate_drift_pp": {},
        "record_count_drop_pct": None,
        "distribution_diff": {},
    }

    current_null_rates = {
        col: round(float(bronze_df[col].isna().mean()), 6)
        for col in policy["null_rate_columns"]
        if col in bronze_df.columns
    }
    baseline_null_rates = dict(baseline.get("null_rates", {}))
    evidence["current_null_rates"] = current_null_rates
    evidence["baseline_null_rates"] = baseline_null_rates
    for col, current_rate in current_null_rates.items():
        if col not in baseline_null_rates:
            continue
        drift_pp = (current_rate - float(baseline_null_rates[col])) * 100
        if drift_pp >= policy["null_rate_threshold_pp"]:
            drift_triggers.append(f"{col}_null_rate_increase")
            evidence["null_rate_drift_pp"][col] = round(drift_pp, 2)

    baseline_count = int(baseline.get("record_count", 0))
    current_count = int(len(bronze_df))
    if baseline_count > 0:
        drop_pct = (baseline_count - current_count) / baseline_count * 100
        if drop_pct >= policy["record_count_drop_threshold_pct"]:
            drift_triggers.append("record_count_drop")
            evidence["record_count_drop_pct"] = round(drop_pct, 2)

    baseline_distribution = dict(baseline.get("distribution", {}))
    for col in policy["distribution_columns"]:
        if col not in bronze_df.columns or col not in baseline_distribution:
            continue
        current_freq: dict[str, float] = {
            k: round(float(v), 6)
            for k, v in bronze_df[col].dropna().astype(str).value_counts(normalize=True).items()
        }
        baseline_freq: dict[str, float] = {
            k: float(v) for k, v in baseline_distribution[col].items()
        }
        all_values = set(current_freq) | set(baseline_freq)
        max_shift_pp = (
            max(abs(current_freq.get(v, 0.0) - baseline_freq.get(v, 0.0)) * 100 for v in all_values)
            if all_values
            else 0.0
        )
        if max_shift_pp >= policy["distribution_shift_threshold_pp"]:
            drift_triggers.append(f"{col}_distribution_shift")
            evidence["distribution_diff"][col] = {
                v: round((current_freq.get(v, 0.0) - baseline_freq.get(v, 0.0)) * 100, 2)
                for v in all_values
            }

    if not drift_triggers:
        return [], []

    drift_triggers = sorted(drift_triggers)
    context = _build_context(
        "quality_drift_detected",
        "Desvio estatistico detectado no Bronze em relacao ao baseline registrado.",
        evidence | {"items": drift_triggers},
    )
    proposal = _build_proposal(
        paths=paths,
        planning_run_id=planning_run_id,
        proposal_type="data_quality_drift_detected",
        proposal_family=PROPOSAL_FAMILY_QUALITY_DRIFT,
        title="Desvio de qualidade detectado no Bronze",
        context_detected=context,
        proposed_change={
            "target_path": "quality.drift_log",
            "operation": "record_quality_drift_event",
            "drift_triggers": drift_triggers,
        },
        expected_impact=(
            "Registra evento de desvio de qualidade e sinaliza necessidade "
            "de rebuild da Silver com revisao de regras de quarentena."
        ),
        risk="high",
        impact_scope="cross_layer",
        requires_approval=True,
        rationale=(
            "Desvio estatistico no Bronze pode propagar silenciosamente "
            "para Silver e Gold sem acionar validacoes estruturais."
        ),
        affected_layers=["bronze", "silver", "gold"],
        affected_artifacts=[
            "config/pipeline_spec.json",
            "reports/monitoring/latest_plan_report.json",
        ],
        privacy_impact="none",
        items=drift_triggers,
    )
    return [context], [proposal]


def plan_pipeline_spec(paths: PipelinePaths) -> dict[str, Any]:
    spec = load_pipeline_spec(paths.pipeline_spec)
    bronze_df = pd.read_parquet(paths.raw_bronze_source)
    metadata_objects, invalid_metadata_count = _parse_metadata_objects(bronze_df)
    observed_columns = sorted(bronze_df.columns.tolist())
    observed_metadata_fields = _discover_metadata_fields(metadata_objects)

    planning_run_id = f"planning_run_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}"
    quality_drift_policy = get_quality_drift_policy(paths)
    quality_baseline = load_quality_baseline(paths)
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
        _schema_proposals(paths, planning_run_id, spec, observed_columns, observed_metadata_fields),
        _validation_proposals(paths, planning_run_id, metadata_objects),
        _derived_column_proposals(paths, planning_run_id, spec, metadata_objects),
        _segmentation_proposals(paths, planning_run_id, spec, bronze_df),
        _transformation_proposals(paths, planning_run_id, metadata_objects),
        _quality_drift_proposals(
            paths, planning_run_id, bronze_df, quality_baseline, quality_drift_policy
        ),
    ]
    for contexts, generated_proposals in detector_results:
        detected_contexts.extend(contexts)
        proposals.extend(generated_proposals)

    # GAP-04: call LLM Advisor before evaluation to reorder/defer proposals
    compiled_plan = compile_pipeline_spec(spec)
    llm_advice = get_llm_advice(
        {
            "observed_columns": observed_columns,
            "observed_metadata_fields": observed_metadata_fields,
            "detected_contexts": detected_contexts,
            "proposals": [
                {
                    "proposal_type": p["proposal_type"],
                    "proposal_family": p["proposal_family"],
                    "expected_impact": p["expected_impact"],
                }
                for p in proposals
            ],
        },
        compiled_plan,
    )

    priority_types: list[str] = llm_advice.get("priority_proposals", [])
    deferred_types: set[str] = set(llm_advice.get("deferred_proposals", []))

    # Partition: priority first (in LLM-supplied order), then non-priority, deferred last
    priority_order = {pt: i for i, pt in enumerate(priority_types)}
    priority_proposals_list = sorted(
        [p for p in proposals if p["proposal_type"] in priority_types],
        key=lambda p: priority_order.get(p["proposal_type"], 999),
    )
    normal_proposals_list = [
        p
        for p in proposals
        if p["proposal_type"] not in priority_types and p["proposal_type"] not in deferred_types
    ]
    deferred_proposals_list = [p for p in proposals if p["proposal_type"] in deferred_types]

    # Mark deferred proposals as closed (no evaluation)
    for proposal in deferred_proposals_list:
        proposal["status"] = PROPOSAL_STATUS_CLOSED_NO_ACTION
        proposal["decision_reason"] = "deferred_by_llm_advisor"
        persist_proposal_record(paths, proposal)

    proposals_to_evaluate = priority_proposals_list + normal_proposals_list

    cooloff_policy = get_rejection_cooloff_policy(paths)

    approval_status_by_proposal = {
        str(proposal["proposal_id"]): get_proposal_approval_status(
            paths, str(proposal["proposal_id"])
        )
        for proposal in proposals_to_evaluate
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
    requires_approval = any(
        bool(proposal["requires_approval"]) for proposal in proposals_to_evaluate
    )

    applied_proposal_types: list[str] = []
    applied_proposal_ids: list[str] = []
    promoted_proposal_ids: list[str] = []
    active_spec = spec
    promotion_enabled = paths.pipeline_spec.parent.resolve() == paths.config.resolve()

    for proposal in proposals_to_evaluate:
        proposal_id = str(proposal["proposal_id"])
        approval_status = approval_status_by_proposal[proposal_id]

        if is_in_rejection_cooloff(paths, proposal_id):
            if approval_status != APPROVAL_STATUS_REJECTED:
                expire_rejection_cooloff(paths, proposal_id)
                # fall through to normal evaluation
            else:
                cooloff_record = get_rejection_cooloff_record(paths, proposal_id)
                new_count = increment_rejection_cooloff_cycle(paths, proposal_id)
                cooloff_started_at = datetime.fromisoformat(
                    cooloff_record["cooloff_started_at_utc"]
                )
                elapsed_hours = (datetime.now(UTC) - cooloff_started_at).total_seconds() / 3600
                if (
                    new_count >= cooloff_policy["threshold_cycles"]
                    or elapsed_hours >= cooloff_policy["threshold_hours"]
                ):
                    expire_rejection_cooloff(paths, proposal_id)
                    # fall through to normal evaluation (re-evaluation pass)
                else:
                    proposal["status"] = PROPOSAL_STATUS_CLOSED_NO_ACTION
                    proposal["decision_reason"] = "rejected_cooloff_active"
                    persist_proposal_record(paths, proposal)
                    continue

        proposal["candidate_actions"] = build_candidate_actions(proposal)
        proposal["approval_context"] = get_proposal_approval_record(paths, proposal_id)
        update_autonomy_metrics(
            paths,
            str(proposal["proposal_family"]),
            counted_proposal=True,
            approval_required=bool(proposal["requires_approval"]),
        )

        candidate_spec, changed = apply_proposal_to_spec(active_spec, proposal)
        if not changed:
            proposal["status"] = PROPOSAL_STATUS_CLOSED_NO_ACTION
            persist_proposal_record(paths, proposal)
            continue

        started_at = time.perf_counter()
        try:
            gate_results, diff = evaluate_candidate(proposal, active_spec, candidate_spec)
            validation_status = "passed"
        except Exception as exc:
            gate_results = {
                "contract_validation": {
                    "passed": False,
                    "error": f"{type(exc).__name__}: {exc}",
                },
                "targeted_tests": {
                    "passed": False,
                    "executed": False,
                },
                "backward_compatibility": {
                    "passed": False,
                    "removed_fields": [],
                },
                "privacy": {
                    "passed": False,
                    "requires_privacy_scan": bool(
                        proposal["policy_snapshot"].get("requires_privacy_scan", False)
                    ),
                },
            }
            diff = {
                "schema_diff": {},
                "value_distribution_diff": {},
                "null_rate_diff": {},
                "privacy_diff": {},
                "taxonomy_diff": {},
                "quality_diff": {},
                "business_signal_diff": {},
            }
            validation_status = "failed"
        duration_sec = time.perf_counter() - started_at
        candidate_references = persist_candidate_artifacts(
            paths,
            proposal,
            candidate_spec,
            gate_results,
            diff,
            validation_status,
        )
        update_autonomy_metrics(
            paths,
            str(proposal["proposal_family"]),
            validation_duration_sec=duration_sec,
            privacy_blocked=not gate_results["privacy"]["passed"],
        )

        gate_passed = all(
            bool(gate_results[key]["passed"])
            for key in (
                "contract_validation",
                "targeted_tests",
                "backward_compatibility",
                "privacy",
            )
        )
        decision = DECISION_REJECT
        decision_reason = "Candidate gates failed."

        # Agent self-review: auto-approve high-confidence structural proposals
        if (
            gate_passed
            and proposal["requires_approval"]
            and approval_status != APPROVAL_STATUS_APPROVED
        ):
            threshold = get_agent_auto_approve_threshold(paths, str(proposal["proposal_family"]))
            if threshold is not None:
                review = agent_self_review_proposal(proposal, gate_results, diff, compiled_plan)
                if review["should_approve"] and float(review["confidence"]) >= threshold:
                    approve_proposal(paths, proposal_id, "agent")
                    approval_status = APPROVAL_STATUS_APPROVED
                    proposal["approval_context"] = get_proposal_approval_record(paths, proposal_id)

        if (
            gate_passed
            and proposal["requires_approval"]
            and approval_status != APPROVAL_STATUS_APPROVED
        ):
            existing_record = load_proposal_record(paths, proposal_id)
            cycle_count = int(existing_record.get("awaiting_approval_cycle_count", 0)) + 1
            proposal["awaiting_approval_cycle_count"] = cycle_count

            stale_policy = get_awaiting_approval_stale_policy(paths)
            stale_threshold = stale_policy["stale_threshold_cycles"]
            reduction = stale_policy["confidence_reduction_per_cycle"]
            floor = stale_policy["confidence_floor"]

            if cycle_count >= stale_threshold:
                base_threshold = get_agent_auto_approve_threshold(
                    paths, str(proposal["proposal_family"])
                )
                if base_threshold is None:
                    proposal["status"] = PROPOSAL_STATUS_STALE
                    decision = DECISION_STALE
                    decision_reason = "awaiting_approval_expired_no_auto_approve_threshold"
                    update_autonomy_metrics(
                        paths,
                        str(proposal["proposal_family"]),
                        unresolved_failure=True,
                    )
                else:
                    reduced = max(
                        base_threshold - reduction * (cycle_count - stale_threshold + 1), floor
                    )
                    review = agent_self_review_proposal(proposal, gate_results, diff, compiled_plan)
                    if review["should_approve"] and float(review["confidence"]) >= reduced:
                        approve_proposal(paths, proposal_id, "agent_secondary_review")
                        approval_status = APPROVAL_STATUS_APPROVED
                        proposal["approval_context"] = get_proposal_approval_record(
                            paths, proposal_id
                        )
                        # fall through to promotion path below
                    elif reduced <= floor:
                        proposal["status"] = PROPOSAL_STATUS_STALE
                        decision = DECISION_STALE
                        decision_reason = "awaiting_approval_expired_at_confidence_floor"
                        update_autonomy_metrics(
                            paths,
                            str(proposal["proposal_family"]),
                            unresolved_failure=True,
                        )
                    else:
                        proposal["status"] = PROPOSAL_STATUS_AWAITING_APPROVAL
                        decision = DECISION_HOLD_FOR_APPROVAL
                        decision_reason = "Impact-governed policy requires explicit approval."
            else:
                proposal["status"] = PROPOSAL_STATUS_AWAITING_APPROVAL
                decision = DECISION_HOLD_FOR_APPROVAL
                decision_reason = "Impact-governed policy requires explicit approval."
        elif approval_status == APPROVAL_STATUS_REJECTED:
            proposal["status"] = PROPOSAL_STATUS_REJECTED
            decision = DECISION_REJECT
            decision_reason = "Proposal was explicitly rejected."
            start_rejection_cooloff(paths, proposal_id)
        elif gate_passed and not promotion_enabled:
            proposal["status"] = PROPOSAL_STATUS_CANDIDATE_MATERIALIZED
            decision_reason = (
                "Candidate materialized, but the configured pipeline spec is read-only."
            )
        elif gate_passed and (
            proposal["safe_auto_promote"] or approval_status == APPROVAL_STATUS_APPROVED
        ):
            promote_candidate_spec(
                paths,
                candidate_spec,
                planning_run_id=planning_run_id,
                proposal=proposal,
                candidate_references=candidate_references,
            )
            active_spec = candidate_spec
            proposal["status"] = PROPOSAL_STATUS_PROMOTED
            decision = DECISION_PROMOTE
            decision_reason = "Candidate passed deterministic gates and promotion policy."
            applied_proposal_ids.append(proposal_id)
            applied_proposal_types.append(str(proposal["proposal_type"]))
            promoted_proposal_ids.append(proposal_id)
            update_autonomy_metrics(
                paths,
                str(proposal["proposal_family"]),
                promoted=True,
            )
        elif gate_passed and approval_status == APPROVAL_STATUS_APPROVED:
            proposal["status"] = PROPOSAL_STATUS_APPROVED
            decision_reason = "Proposal approved but not yet eligible for automatic promotion."
        elif gate_passed:
            proposal["status"] = PROPOSAL_STATUS_CANDIDATE_MATERIALIZED
            decision_reason = "Candidate materialized but policy does not allow promotion."
        else:
            proposal["status"] = PROPOSAL_STATUS_VALIDATION_FAILED
            update_autonomy_metrics(
                paths,
                str(proposal["proposal_family"]),
                unresolved_failure=True,
            )

        proposal["approval_context"] = get_proposal_approval_record(paths, proposal_id)
        persist_proposal_record(paths, proposal)
        persist_autonomy_decision(
            paths,
            {
                "decision_id": f"decision_{proposal_id}",
                "proposal_id": proposal_id,
                "decision_at_utc": _utc_now_iso(),
                "decision": decision,
                "decision_reason": decision_reason,
                "gate_results": gate_results,
                "baseline_reference": {
                    "pipeline_spec_path": str(paths.pipeline_spec),
                },
                "candidate_reference": candidate_references,
                "post_promotion_monitoring_required": decision == DECISION_PROMOTE,
            },
        )

    # All proposals (evaluated + deferred) for the report
    all_proposals = proposals_to_evaluate + deferred_proposals_list

    approved = bool(proposals_to_evaluate) and all(
        proposal["status"] in {PROPOSAL_STATUS_PROMOTED, PROPOSAL_STATUS_APPROVED}
        for proposal in proposals_to_evaluate
        if proposal["requires_approval"]
    )

    report = {
        "generated_at_utc": _utc_now_iso(),
        "proposal_id": planning_run_id,
        "detected_contexts": detected_contexts,
        "observed_columns": observed_columns,
        "observed_metadata_fields": observed_metadata_fields,
        "proposals": all_proposals,
        "changes": all_proposals,
        "requires_approval": requires_approval,
        "approved": approved,
        "approved_proposal_ids": approved_proposal_ids,
        "rejected_proposal_ids": rejected_proposal_ids,
        "applied": bool(applied_proposal_ids),
        "promoted_proposal_ids": promoted_proposal_ids,
        "applied_proposal_ids": applied_proposal_ids,
        "applied_proposal_types": applied_proposal_types,
        "autonomy_policy_path": str(paths.autonomy_policy),
        "autonomy_metrics_path": str(paths.autonomy_metrics),
        "llm_advice": llm_advice,
        "summary": {
            "proposal_count": len(all_proposals),
            "context_count": len(detected_contexts),
            "families": sorted({str(proposal["proposal_family"]) for proposal in all_proposals}),
            "status_counts": {
                PROPOSAL_STATUS_PROPOSED: sum(
                    1
                    for proposal in all_proposals
                    if proposal["status"] == PROPOSAL_STATUS_PROPOSED
                ),
                PROPOSAL_STATUS_CANDIDATE_MATERIALIZED: sum(
                    1
                    for proposal in all_proposals
                    if proposal["status"] == PROPOSAL_STATUS_CANDIDATE_MATERIALIZED
                ),
                PROPOSAL_STATUS_AWAITING_APPROVAL: sum(
                    1
                    for proposal in all_proposals
                    if proposal["status"] == PROPOSAL_STATUS_AWAITING_APPROVAL
                ),
                PROPOSAL_STATUS_APPROVED: sum(
                    1
                    for proposal in all_proposals
                    if proposal["status"] == PROPOSAL_STATUS_APPROVED
                ),
                PROPOSAL_STATUS_PROMOTED: sum(
                    1
                    for proposal in all_proposals
                    if proposal["status"] == PROPOSAL_STATUS_PROMOTED
                ),
                PROPOSAL_STATUS_REJECTED: sum(
                    1
                    for proposal in all_proposals
                    if proposal["status"] == PROPOSAL_STATUS_REJECTED
                ),
                PROPOSAL_STATUS_VALIDATION_FAILED: sum(
                    1
                    for proposal in all_proposals
                    if proposal["status"] == PROPOSAL_STATUS_VALIDATION_FAILED
                ),
                PROPOSAL_STATUS_CLOSED_NO_ACTION: sum(
                    1
                    for proposal in all_proposals
                    if proposal["status"] == PROPOSAL_STATUS_CLOSED_NO_ACTION
                ),
                PROPOSAL_STATUS_STALE: sum(
                    1 for proposal in all_proposals if proposal["status"] == PROPOSAL_STATUS_STALE
                ),
            },
        },
    }
    write_json(report, paths.monitoring / "latest_plan_report.json")

    quality_snapshot = compute_quality_snapshot(
        bronze_df,
        quality_drift_policy["null_rate_columns"],
        quality_drift_policy["distribution_columns"],
    )
    state_path = paths.state / "pipeline_state.json"
    pipeline_state = load_pipeline_state(state_path)
    pipeline_state["quality_baseline"] = quality_snapshot
    save_pipeline_state(state_path, pipeline_state)

    return report
