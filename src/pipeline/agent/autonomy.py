from __future__ import annotations

import copy
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline.config import PipelinePaths
from pipeline.io.parquet_io import read_json, write_json
from pipeline.orchestration.compiler import compile_pipeline_spec
from pipeline.runtime.spec import save_pipeline_spec, validate_pipeline_spec

IMPACT_LOW = "low"
IMPACT_MEDIUM = "medium"
IMPACT_HIGH = "high"

DECISION_PROMOTE = "promote"
DECISION_HOLD_FOR_APPROVAL = "hold_for_approval"
DECISION_REJECT = "reject"
DECISION_ROLLBACK = "rollback"

PROPOSAL_STATUS_PROPOSED = "proposed"
PROPOSAL_STATUS_CANDIDATE_MATERIALIZED = "candidate_materialized"
PROPOSAL_STATUS_VALIDATION_FAILED = "validation_failed"
PROPOSAL_STATUS_AWAITING_APPROVAL = "awaiting_approval"
PROPOSAL_STATUS_APPROVED = "approved"
PROPOSAL_STATUS_REJECTED = "rejected"
PROPOSAL_STATUS_PROMOTED = "promoted"
PROPOSAL_STATUS_ROLLED_BACK = "rolled_back"
PROPOSAL_STATUS_CLOSED_NO_ACTION = "closed_no_action"
PROPOSAL_STATUS_STALE = "stale"

DECISION_STALE = "stale"

DEFAULT_AUTONOMY_POLICY: dict[str, Any] = {
    "awaiting_approval_stale_threshold_cycles": 5,
    "awaiting_approval_confidence_reduction_per_cycle": 0.05,
    "awaiting_approval_confidence_floor": 0.70,
    "rejection_cooloff_threshold_cycles": 10,
    "rejection_cooloff_threshold_hours": 24.0,
    "quality_drift_null_rate_columns": [
        "message_body",
        "conversation_outcome",
        "channel",
        "timestamp",
    ],
    "quality_drift_distribution_columns": ["conversation_outcome", "channel"],
    "quality_drift_null_rate_threshold_pp": 10.0,
    "quality_drift_record_count_drop_threshold_pct": 20.0,
    "quality_drift_distribution_shift_threshold_pp": 15.0,
    "promotion_confidence_weights": {
        "stability": 0.4,
        "type_consistency": 0.2,
        "cardinality_fit": 0.2,
        "privacy_clean": 0.2,
    },
    "mutation_families": {
        "schema_update": {
            "default_impact_class": IMPACT_HIGH,
            "auto_promote": False,
            "requires_approval": True,
            "requires_backward_compatibility": True,
            "requires_privacy_scan": True,
            "agent_auto_approve_if_confidence_ge": 0.90,
        },
        "validation_enhancement": {
            "default_impact_class": IMPACT_LOW,
            "auto_promote": True,
            "requires_approval": False,
            "requires_backward_compatibility": True,
            "requires_privacy_scan": False,
        },
        "derived_column_addition": {
            "default_impact_class": IMPACT_MEDIUM,
            "auto_promote": True,
            "requires_approval": False,
            "requires_backward_compatibility": True,
            "requires_privacy_scan": True,
        },
        "segmentation_adjustment": {
            "default_impact_class": IMPACT_HIGH,
            "auto_promote": False,
            "requires_approval": True,
            "requires_backward_compatibility": True,
            "requires_privacy_scan": False,
            "agent_auto_approve_if_confidence_ge": 0.90,
        },
        "transformation_rule_change": {
            "default_impact_class": IMPACT_MEDIUM,
            "auto_promote": False,
            "requires_approval": True,
            "requires_backward_compatibility": True,
            "requires_privacy_scan": False,
            "agent_auto_approve_if_confidence_ge": 0.85,
        },
        "data_quality_drift": {
            "default_impact_class": IMPACT_HIGH,
            "auto_promote": False,
            "requires_approval": True,
            "requires_backward_compatibility": False,
            "requires_privacy_scan": False,
            "agent_auto_approve_if_confidence_ge": 0.85,
        },
        "schema_promotion_silver": {
            "default_impact_class": IMPACT_LOW,
            "auto_promote": True,
            "requires_approval": False,
            "requires_backward_compatibility": True,
            "requires_privacy_scan": True,
            "agent_auto_approve_if_confidence_ge": 0.85,
        },
        "schema_promotion_silver_rule": {
            "default_impact_class": IMPACT_MEDIUM,
            "auto_promote": False,
            "requires_approval": True,
            "requires_backward_compatibility": True,
            "requires_privacy_scan": False,
            "agent_auto_approve_if_confidence_ge": 0.90,
        },
        "schema_promotion_bronze_optional": {
            "default_impact_class": IMPACT_LOW,
            "auto_promote": True,
            "requires_approval": False,
            "requires_backward_compatibility": True,
            "requires_privacy_scan": True,
            "agent_auto_approve_if_confidence_ge": 0.85,
        },
        "schema_promotion_gold_optional": {
            "default_impact_class": IMPACT_MEDIUM,
            "auto_promote": False,
            "requires_approval": True,
            "requires_backward_compatibility": True,
            "requires_privacy_scan": True,
            "agent_auto_approve_if_confidence_ge": 0.90,
        },
        "schema_promotion_gold_passthrough": {
            "default_impact_class": IMPACT_MEDIUM,
            "auto_promote": False,
            "requires_approval": True,
            "requires_backward_compatibility": True,
            "requires_privacy_scan": True,
            "agent_auto_approve_if_confidence_ge": 0.90,
        },
        "schema_promotion_gold_macro_dimension": {
            "default_impact_class": IMPACT_HIGH,
            "auto_promote": False,
            "requires_approval": True,
            "requires_backward_compatibility": True,
            "requires_privacy_scan": True,
            "agent_auto_approve_if_confidence_ge": None,
        },
        "schema_contract_version_bump": {
            "default_impact_class": IMPACT_LOW,
            "auto_promote": True,
            "requires_approval": False,
            "requires_backward_compatibility": True,
            "requires_privacy_scan": False,
            "agent_auto_approve_if_confidence_ge": None,
        },
    },
}


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def load_autonomy_policy(paths: PipelinePaths) -> dict[str, Any]:
    if not paths.autonomy_policy.exists():
        write_json(copy.deepcopy(DEFAULT_AUTONOMY_POLICY), paths.autonomy_policy)
    return read_json(paths.autonomy_policy, default=copy.deepcopy(DEFAULT_AUTONOMY_POLICY))


def classify_proposal(proposal_family: str, paths: PipelinePaths) -> dict[str, Any]:
    policy = load_autonomy_policy(paths)
    family_policy = copy.deepcopy(policy.get("mutation_families", {}).get(proposal_family, {}))
    if not family_policy:
        family_policy = {
            "default_impact_class": IMPACT_HIGH,
            "auto_promote": False,
            "requires_approval": True,
            "requires_backward_compatibility": True,
            "requires_privacy_scan": True,
        }
    return {
        "impact_class": str(family_policy.get("default_impact_class", IMPACT_HIGH)),
        "safe_auto_promote": bool(family_policy.get("auto_promote", False)),
        "requires_approval": bool(family_policy.get("requires_approval", False)),
        "policy_snapshot": family_policy,
    }


def candidate_dir(paths: PipelinePaths, proposal_id: str) -> Path:
    return paths.candidates / proposal_id


def proposal_record_path(paths: PipelinePaths, proposal_id: str) -> Path:
    return paths.autonomy_proposals / f"{proposal_id}.json"


def autonomy_decision_path(paths: PipelinePaths) -> Path:
    return paths.autonomy_decisions / "latest_autonomy_decision.json"


def load_autonomy_metrics(paths: PipelinePaths) -> dict[str, Any]:
    return read_json(
        paths.autonomy_metrics,
        default={
            "proposal_count_total": 0,
            "promotion_count_total": 0,
            "rollback_count_total": 0,
            "approval_required_count_total": 0,
            "unresolved_failure_count_total": 0,
            "promotion_rate_by_family": {},
            "rollback_rate_by_family": {},
            "privacy_block_rate": 0.0,
            "median_candidate_validation_time_sec": 0.0,
            "post_promotion_regression_count": 0,
            "proposal_count_by_family": {},
            "privacy_block_count_total": 0,
            "candidate_validation_durations_sec": [],
        },
    )


def persist_autonomy_metrics(paths: PipelinePaths, metrics: dict[str, Any]) -> None:
    durations = list(metrics.get("candidate_validation_durations_sec", []))
    if durations:
        ordered = sorted(float(item) for item in durations)
        metrics["median_candidate_validation_time_sec"] = ordered[len(ordered) // 2]
    proposal_count_by_family = metrics.get("proposal_count_by_family", {})
    promotion_count_by_family = metrics.get("promotion_count_by_family", {})
    rollback_count_by_family = metrics.get("rollback_count_by_family", {})
    promotion_rate_by_family: dict[str, float] = {}
    rollback_rate_by_family: dict[str, float] = {}
    for family, total in proposal_count_by_family.items():
        denominator = int(total) or 1
        promotion_rate_by_family[str(family)] = round(
            int(promotion_count_by_family.get(family, 0)) / denominator,
            4,
        )
        promoted = int(promotion_count_by_family.get(family, 0)) or 1
        rollback_rate_by_family[str(family)] = round(
            int(rollback_count_by_family.get(family, 0)) / promoted,
            4,
        )
    metrics["promotion_rate_by_family"] = promotion_rate_by_family
    metrics["rollback_rate_by_family"] = rollback_rate_by_family
    total_proposals = int(metrics.get("proposal_count_total", 0)) or 1
    metrics["privacy_block_rate"] = round(
        int(metrics.get("privacy_block_count_total", 0)) / total_proposals,
        4,
    )
    write_json(metrics, paths.autonomy_metrics)


def update_autonomy_metrics(
    paths: PipelinePaths,
    proposal_family: str,
    *,
    counted_proposal: bool = False,
    approval_required: bool = False,
    promoted: bool = False,
    rolled_back: bool = False,
    unresolved_failure: bool = False,
    privacy_blocked: bool = False,
    validation_duration_sec: float | None = None,
) -> dict[str, Any]:
    metrics = load_autonomy_metrics(paths)
    if counted_proposal:
        metrics["proposal_count_total"] = int(metrics.get("proposal_count_total", 0)) + 1
        by_family = metrics.setdefault("proposal_count_by_family", {})
        by_family[proposal_family] = int(by_family.get(proposal_family, 0)) + 1
    if approval_required:
        metrics["approval_required_count_total"] = (
            int(metrics.get("approval_required_count_total", 0)) + 1
        )
    if promoted:
        metrics["promotion_count_total"] = int(metrics.get("promotion_count_total", 0)) + 1
        by_family = metrics.setdefault("promotion_count_by_family", {})
        by_family[proposal_family] = int(by_family.get(proposal_family, 0)) + 1
    if rolled_back:
        metrics["rollback_count_total"] = int(metrics.get("rollback_count_total", 0)) + 1
        by_family = metrics.setdefault("rollback_count_by_family", {})
        by_family[proposal_family] = int(by_family.get(proposal_family, 0)) + 1
    if unresolved_failure:
        metrics["unresolved_failure_count_total"] = (
            int(metrics.get("unresolved_failure_count_total", 0)) + 1
        )
    if privacy_blocked:
        metrics["privacy_block_count_total"] = int(metrics.get("privacy_block_count_total", 0)) + 1
    if validation_duration_sec is not None:
        metrics.setdefault("candidate_validation_durations_sec", []).append(
            round(validation_duration_sec, 6)
        )
    persist_autonomy_metrics(paths, metrics)
    return metrics


def build_candidate_actions(proposal: dict[str, Any]) -> list[dict[str, Any]]:
    proposal_type = str(proposal["proposal_type"])
    items = list(proposal.get("items", []))
    if proposal_type == "bronze_required_columns_addition":
        return [
            {
                "action_id": "action_01",
                "action_kind": "spec_patch",
                "target_path": "config/pipeline_spec.json",
                "target_selector": "bronze.required_columns",
                "operation": "add_items",
                "payload": {"items": items},
                "reversible": True,
                "validation_scope": ["spec_validation", "candidate_diff"],
            }
        ]
    if proposal_type == "silver_metadata_fields_addition":
        return [
            {
                "action_id": "action_01",
                "action_kind": "spec_patch",
                "target_path": "config/pipeline_spec.json",
                "target_selector": "silver.metadata_fields",
                "operation": "add_items",
                "payload": {"items": items},
                "reversible": True,
                "validation_scope": ["spec_validation", "candidate_diff"],
            }
        ]
    if proposal_type == "metadata_boolean_validation_addition":
        return [
            {
                "action_id": "action_01",
                "action_kind": "validator_rule_patch",
                "target_path": "config/pipeline_spec.json",
                "target_selector": "quality.validation_rules.silver",
                "operation": "add_item",
                "payload": {"item": "metadata_boolean_normalized"},
                "reversible": True,
                "validation_scope": ["spec_validation", "candidate_diff"],
            }
        ]
    if proposal_type == "gold_business_hours_metric_addition":
        return [
            {
                "action_id": "action_01",
                "action_kind": "gold_feature_addition",
                "target_path": "config/pipeline_spec.json",
                "target_selector": "gold.required_columns",
                "operation": "add_item",
                "payload": {"item": "business_hours_message_ratio"},
                "reversible": True,
                "validation_scope": ["spec_validation", "candidate_diff"],
            }
        ]
    if proposal_type == "intent_stage_negotiation_extension":
        return [
            {
                "action_id": "action_01",
                "action_kind": "taxonomy_addition",
                "target_path": "config/pipeline_spec.json",
                "target_selector": "gold.valid_intent_stages",
                "operation": "add_item",
                "payload": {"item": "negociacao_em_andamento"},
                "reversible": True,
                "validation_scope": ["spec_validation", "candidate_diff"],
            }
        ]
    if proposal_type == "metadata_key_normalization_rule":
        return [
            {
                "action_id": "action_01",
                "action_kind": "config_patch",
                "target_path": "config/pipeline_spec.json",
                "target_selector": "silver.metadata_key_normalization",
                "operation": "replace",
                "payload": proposal.get("proposed_change", {}),
                "reversible": True,
                "validation_scope": ["spec_validation"],
            }
        ]
    if proposal_type == "data_quality_drift_detected":
        return [
            {
                "action_id": "action_01",
                "action_kind": "drift_escalation",
                "target_path": "config/pipeline_spec.json",
                "target_selector": "quality.drift_log",
                "operation": "record_quality_drift_event",
                "payload": {"drift_triggers": proposal.get("items", [])},
                "reversible": True,
                "validation_scope": ["spec_validation"],
            }
        ]
    if proposal_type in {
        "bronze_optional_columns_addition",
        "silver_preserve_extra_columns_addition",
        "silver_extra_aggregation_rule_addition",
        "gold_optional_columns_addition",
        "gold_passthrough_columns_addition",
        "gold_aggregation_rule_addition",
        "gold_macro_dimension_addition",
        "schema_contract_version_bump",
    }:
        actions: list[dict[str, Any]] = []
        changes = [proposal.get("proposed_change", {})] + list(
            proposal.get("proposed_change", {}).get("companion_actions", [])
        )
        for index, change in enumerate(changes, start=1):
            if not isinstance(change, dict) or not change:
                continue
            actions.append(
                {
                    "action_id": f"action_{index:02d}",
                    "action_kind": "spec_patch",
                    "target_path": "config/pipeline_spec.json",
                    "target_selector": change.get("target_path"),
                    "operation": change.get("operation"),
                    "payload": {
                        "items": list(change.get("items", [])),
                        "keys": dict(change.get("keys", {})),
                        "value": change.get("value"),
                    },
                    "reversible": True,
                    "validation_scope": ["spec_validation", "candidate_diff"],
                }
            )
        return actions
    return []


def _sorted_unique(existing: list[Any], extra: list[Any]) -> list[Any]:
    return sorted({*existing, *extra})


def _dict_diff(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    before_keys = set(before)
    after_keys = set(after)
    return {
        "added_keys": sorted(after_keys - before_keys),
        "removed_keys": sorted(before_keys - after_keys),
        "changed_keys": {
            key: {"before": before.get(key), "after": after.get(key)}
            for key in sorted(before_keys & after_keys)
            if before.get(key) != after.get(key)
        },
    }


def _resolve_path_container(spec: dict[str, Any], dotted_path: str) -> tuple[dict[str, Any], str]:
    segments = dotted_path.split(".")
    container: dict[str, Any] = spec
    for segment in segments[:-1]:
        next_value = container.setdefault(segment, {})
        if not isinstance(next_value, dict):
            raise ValueError(f"Spec path {dotted_path} is not addressable")
        container = next_value
    return container, segments[-1]


def _apply_structural_change(spec: dict[str, Any], change: dict[str, Any]) -> bool:
    target_path = str(change.get("target_path", "")).strip()
    operation = str(change.get("operation", "")).strip()
    if not target_path or not operation:
        return False
    container, key = _resolve_path_container(spec, target_path)
    if operation == "add_items":
        current = list(container.get(key, []))
        updated = _sorted_unique(current, list(change.get("items", [])))
        if updated == current:
            return False
        container[key] = updated
        return True
    if operation == "set_keys":
        current_map = dict(container.get(key, {}))
        updated_map = dict(current_map)
        updated_map.update(dict(change.get("keys", {})))
        if updated_map == current_map:
            return False
        container[key] = updated_map
        return True
    if operation == "set_value":
        value = change.get("value")
        if container.get(key) == value:
            return False
        container[key] = value
        return True
    return False


def apply_proposal_to_spec(
    spec: dict[str, Any], proposal: dict[str, Any]
) -> tuple[dict[str, Any], bool]:
    updated = copy.deepcopy(spec)
    proposal_type = str(proposal["proposal_type"])
    items = list(proposal.get("items", []))
    changed = False

    if proposal_type == "bronze_required_columns_addition" and items:
        updated["bronze"]["required_columns"] = _sorted_unique(
            list(updated["bronze"]["required_columns"]),
            items,
        )
        changed = True
    elif proposal_type == "silver_metadata_fields_addition" and items:
        updated["silver"]["metadata_fields"] = _sorted_unique(
            list(updated["silver"].get("metadata_fields", [])),
            items,
        )
        changed = True
    elif proposal_type == "metadata_boolean_validation_addition":
        silver_rules = list(updated["quality"]["validation_rules"].get("silver", []))
        updated["quality"]["validation_rules"]["silver"] = _sorted_unique(
            silver_rules,
            ["metadata_boolean_normalized"],
        )
        changed = True
    elif proposal_type == "gold_business_hours_metric_addition":
        updated["gold"]["required_columns"] = _sorted_unique(
            list(updated["gold"]["required_columns"]),
            ["business_hours_message_ratio"],
        )
        changed = True
    elif proposal_type == "intent_stage_negotiation_extension":
        updated["gold"]["valid_intent_stages"] = _sorted_unique(
            list(updated["gold"]["valid_intent_stages"]),
            ["negociacao_em_andamento"],
        )
        segmentation = updated["gold"].setdefault("segmentation", {})
        intent_stage = segmentation.setdefault("intent_stage", {})
        intent_stage["negotiation_label"] = "negociacao_em_andamento"
        changed = True
    elif proposal_type == "metadata_key_normalization_rule":
        updated["silver"]["metadata_key_normalization"] = copy.deepcopy(
            proposal.get("proposed_change", {})
        )
        changed = True
    elif proposal_type == "data_quality_drift_detected":
        drift_log = updated.setdefault("quality", {}).setdefault("drift_log", [])
        drift_log.append(
            {
                "drift_triggers": proposal.get("items", []),
                "recorded_at_utc": proposal.get("created_at_utc", ""),
            }
        )
        changed = True
    elif proposal_type in {
        "bronze_optional_columns_addition",
        "silver_preserve_extra_columns_addition",
        "silver_extra_aggregation_rule_addition",
        "gold_optional_columns_addition",
        "gold_passthrough_columns_addition",
        "gold_aggregation_rule_addition",
        "gold_macro_dimension_addition",
        "schema_contract_version_bump",
    }:
        main_change = proposal.get("proposed_change", {})
        if isinstance(main_change, dict):
            changed = _apply_structural_change(updated, main_change) or changed
            for companion in list(main_change.get("companion_actions", [])):
                changed = _apply_structural_change(updated, companion) or changed

    return updated, changed


def _list_diff(before: list[Any], after: list[Any]) -> dict[str, list[Any]]:
    before_set = {str(item) for item in before}
    after_set = {str(item) for item in after}
    return {
        "added": sorted(after_set - before_set),
        "removed": sorted(before_set - after_set),
    }


def build_spec_diff(
    baseline_spec: dict[str, Any], candidate_spec: dict[str, Any]
) -> dict[str, Any]:
    schema_diff = {
        "bronze.required_columns": _list_diff(
            list(baseline_spec["bronze"]["required_columns"]),
            list(candidate_spec["bronze"]["required_columns"]),
        ),
        "bronze.optional_columns": _list_diff(
            list(baseline_spec["bronze"].get("optional_columns", [])),
            list(candidate_spec["bronze"].get("optional_columns", [])),
        ),
        "silver.metadata_fields": _list_diff(
            list(baseline_spec["silver"].get("metadata_fields", [])),
            list(candidate_spec["silver"].get("metadata_fields", [])),
        ),
        "silver.preserve_extra_columns": _list_diff(
            list(baseline_spec["silver"].get("preserve_extra_columns", [])),
            list(candidate_spec["silver"].get("preserve_extra_columns", [])),
        ),
        "silver.extra_aggregation_rules": _dict_diff(
            dict(baseline_spec["silver"].get("extra_aggregation_rules", {})),
            dict(candidate_spec["silver"].get("extra_aggregation_rules", {})),
        ),
        "gold.required_columns": _list_diff(
            list(baseline_spec["gold"]["required_columns"]),
            list(candidate_spec["gold"]["required_columns"]),
        ),
        "gold.optional_columns": _list_diff(
            list(baseline_spec["gold"].get("optional_columns", [])),
            list(candidate_spec["gold"].get("optional_columns", [])),
        ),
        "gold.passthrough_columns": _list_diff(
            list(baseline_spec["gold"].get("passthrough_columns", [])),
            list(candidate_spec["gold"].get("passthrough_columns", [])),
        ),
        "gold.aggregation_rules": _dict_diff(
            dict(baseline_spec["gold"].get("aggregation_rules", {})),
            dict(candidate_spec["gold"].get("aggregation_rules", {})),
        ),
        "gold.valid_intent_stages": _list_diff(
            list(baseline_spec["gold"]["valid_intent_stages"]),
            list(candidate_spec["gold"]["valid_intent_stages"]),
        ),
        "gold_macro.categorical_dimensions": _list_diff(
            list(baseline_spec.get("gold_macro", {}).get("categorical_dimensions", [])),
            list(candidate_spec.get("gold_macro", {}).get("categorical_dimensions", [])),
        ),
        "schema_contract_version": {
            "before": baseline_spec.get("schema_contract_version"),
            "after": candidate_spec.get("schema_contract_version"),
        },
    }
    return {
        "schema_diff": schema_diff,
        "value_distribution_diff": {},
        "null_rate_diff": {},
        "privacy_diff": {
            "requires_privacy_scan": False,
            "blocked": False,
            "new_findings": [],
        },
        "taxonomy_diff": {
            "gold.valid_intent_stages": schema_diff["gold.valid_intent_stages"],
        },
        "quality_diff": {},
        "business_signal_diff": {
            "gold.required_columns": schema_diff["gold.required_columns"],
        },
    }


def _run_targeted_tests_gate(
    proposal: dict[str, Any],
    compiled_plan: dict[str, Any],
    diff: dict[str, Any],
    baseline_spec: dict[str, Any],
    candidate_spec: dict[str, Any],
) -> dict[str, Any]:
    agent_cfg = compiled_plan.get("agent", {})
    test_paths = list(agent_cfg.get("targeted_test_paths", []))
    timeout_sec = int(agent_cfg.get("targeted_tests_timeout_sec", 120))

    # CON-102: skip for low-impact validation_enhancement — regression risk is minimal
    mutation_family = str(proposal.get("proposal_family", ""))
    impact_class = str(proposal.get("impact_class", ""))
    if mutation_family == "validation_enhancement" and impact_class == "low":
        return {
            "passed": True,
            "executed": False,
            "returncode": None,
            "reason": "low_impact_validation_enhancement_skipped",
            "test_paths_run": [],
        }

    # REQ-105: no test paths configured → safe opt-in default
    if not test_paths:
        return {
            "passed": True,
            "executed": False,
            "returncode": None,
            "reason": "no_targeted_tests_configured",
            "test_paths_run": [],
        }

    # REQ-101: only run when candidate touches required_columns or validation_rules
    schema_diff = diff.get("schema_diff", {})
    required_col_changed = any(
        schema_diff.get(k, {}).get("added")
        or schema_diff.get(k, {}).get("removed")
        or schema_diff.get(k, {}).get("added_keys")
        or schema_diff.get(k, {}).get("removed_keys")
        or schema_diff.get(k, {}).get("changed_keys")
        for k in (
            "bronze.required_columns",
            "bronze.optional_columns",
            "silver.metadata_fields",
            "silver.preserve_extra_columns",
            "silver.extra_aggregation_rules",
            "gold.required_columns",
            "gold.optional_columns",
            "gold.passthrough_columns",
            "gold.aggregation_rules",
            "gold.valid_intent_stages",
            "gold_macro.categorical_dimensions",
        )
    )
    validation_rules_changed = baseline_spec.get("quality", {}).get(
        "validation_rules"
    ) != candidate_spec.get("quality", {}).get("validation_rules")

    if not required_col_changed and not validation_rules_changed:
        return {
            "passed": True,
            "executed": False,
            "returncode": None,
            "reason": "no_relevant_changes_in_candidate",
            "test_paths_run": [],
        }

    # REQ-102/CON-101: execute in a separate process to isolate test state
    try:
        proc = subprocess.run(
            ["venv/bin/python", "-m", "pytest"] + test_paths,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
        passed = proc.returncode == 0
        return {
            "passed": passed,
            "executed": True,
            "returncode": proc.returncode,
            "reason": "tests_passed" if passed else "tests_failed",
            "test_paths_run": test_paths,
        }
    except subprocess.TimeoutExpired:
        return {
            "passed": False,
            "executed": True,
            "returncode": None,
            "reason": "timeout",
            "test_paths_run": test_paths,
        }


def evaluate_candidate(
    proposal: dict[str, Any],
    baseline_spec: dict[str, Any],
    candidate_spec: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    validate_pipeline_spec(candidate_spec)
    compiled_plan = compile_pipeline_spec(candidate_spec)
    diff = build_spec_diff(baseline_spec, candidate_spec)
    removed_fields: list[str] = []
    for section_diff in diff["schema_diff"].values():
        removed_fields.extend(list(section_diff.get("removed", [])))
    requires_privacy_scan = bool(
        proposal.get("policy_snapshot", {}).get("requires_privacy_scan", False)
    )
    privacy_blocked = (
        requires_privacy_scan
        and str(proposal.get("privacy_impact", "none")) == "sensitive_detection"
    )
    targeted_tests_gate = _run_targeted_tests_gate(
        proposal, compiled_plan, diff, baseline_spec, candidate_spec
    )
    gate_results = {
        "contract_validation": {"passed": True},
        "targeted_tests": targeted_tests_gate,
        "backward_compatibility": {
            "passed": not removed_fields,
            "removed_fields": removed_fields,
        },
        "privacy": {
            "passed": not privacy_blocked,
            "requires_privacy_scan": requires_privacy_scan,
        },
    }
    return gate_results, diff


def persist_candidate_artifacts(
    paths: PipelinePaths,
    proposal: dict[str, Any],
    candidate_spec: dict[str, Any],
    gate_results: dict[str, Any],
    diff: dict[str, Any],
    validation_status: str,
) -> dict[str, str]:
    proposal_id = str(proposal["proposal_id"])
    root = candidate_dir(paths, proposal_id)
    root.mkdir(parents=True, exist_ok=True)
    candidate_spec_path = root / "candidate_spec.json"
    candidate_run_report_path = root / "candidate_run_report.json"
    candidate_agent_report_path = root / "candidate_agent_report.json"
    candidate_diff_path = root / "candidate_diff.json"
    candidate_metrics_path = root / "candidate_metrics.json"
    candidate_test_report_path = root / "candidate_test_report.json"
    write_json(candidate_spec, candidate_spec_path)
    write_json(
        {
            "proposal_id": proposal_id,
            "status": validation_status,
            "gate_results": gate_results,
        },
        candidate_run_report_path,
    )
    write_json(
        {
            "proposal_id": proposal_id,
            "impact_class": proposal.get("impact_class"),
            "requires_approval": proposal.get("requires_approval"),
            "safe_auto_promote": proposal.get("safe_auto_promote"),
        },
        candidate_agent_report_path,
    )
    write_json(diff, candidate_diff_path)
    write_json(
        {
            "proposal_id": proposal_id,
            "validation_passed": validation_status == "passed",
        },
        candidate_metrics_path,
    )
    write_json(gate_results["targeted_tests"], candidate_test_report_path)
    return {
        "candidate_root": str(root),
        "candidate_spec_path": str(candidate_spec_path),
        "candidate_run_report_path": str(candidate_run_report_path),
        "candidate_agent_report_path": str(candidate_agent_report_path),
        "candidate_diff_path": str(candidate_diff_path),
        "candidate_metrics_path": str(candidate_metrics_path),
        "candidate_test_report_path": str(candidate_test_report_path),
    }


def persist_proposal_record(paths: PipelinePaths, proposal: dict[str, Any]) -> None:
    write_json(proposal, proposal_record_path(paths, str(proposal["proposal_id"])))


def persist_autonomy_decision(paths: PipelinePaths, decision: dict[str, Any]) -> None:
    write_json(decision, autonomy_decision_path(paths))


def get_agent_auto_approve_threshold(paths: PipelinePaths, proposal_family: str) -> float | None:
    policy = load_autonomy_policy(paths)
    family_policy = policy.get("mutation_families", {}).get(proposal_family, {})
    threshold = family_policy.get("agent_auto_approve_if_confidence_ge")
    if threshold is None:
        return None
    return float(threshold)


def load_proposal_record(paths: PipelinePaths, proposal_id: str) -> dict[str, Any]:
    path = proposal_record_path(paths, proposal_id)
    if not path.exists():
        return {}
    return read_json(path, default={})


def get_rejection_cooloff_policy(paths: PipelinePaths) -> dict[str, Any]:
    policy = load_autonomy_policy(paths)
    return {
        "threshold_cycles": int(policy.get("rejection_cooloff_threshold_cycles", 10)),
        "threshold_hours": float(policy.get("rejection_cooloff_threshold_hours", 24.0)),
    }


def get_quality_drift_policy(paths: PipelinePaths) -> dict[str, Any]:
    policy = load_autonomy_policy(paths)
    return {
        "null_rate_columns": list(
            policy.get(
                "quality_drift_null_rate_columns",
                ["message_body", "conversation_outcome", "channel", "timestamp"],
            )
        ),
        "distribution_columns": list(
            policy.get("quality_drift_distribution_columns", ["conversation_outcome", "channel"])
        ),
        "null_rate_threshold_pp": float(policy.get("quality_drift_null_rate_threshold_pp", 10.0)),
        "record_count_drop_threshold_pct": float(
            policy.get("quality_drift_record_count_drop_threshold_pct", 20.0)
        ),
        "distribution_shift_threshold_pp": float(
            policy.get("quality_drift_distribution_shift_threshold_pp", 15.0)
        ),
    }


def get_awaiting_approval_stale_policy(paths: PipelinePaths) -> dict[str, Any]:
    policy = load_autonomy_policy(paths)
    return {
        "stale_threshold_cycles": int(policy.get("awaiting_approval_stale_threshold_cycles", 5)),
        "confidence_reduction_per_cycle": float(
            policy.get("awaiting_approval_confidence_reduction_per_cycle", 0.05)
        ),
        "confidence_floor": float(policy.get("awaiting_approval_confidence_floor", 0.70)),
    }


def promote_candidate_spec(
    paths: PipelinePaths,
    candidate_spec: dict[str, Any],
    *,
    planning_run_id: str,
    proposal: dict[str, Any],
    candidate_references: dict[str, str],
) -> None:
    save_pipeline_spec(candidate_spec, paths.pipeline_spec)
    history = read_json(paths.spec_history, default={"changes": []})
    history.setdefault("changes", []).append(
        {
            "planning_run_id": planning_run_id,
            "applied_at_utc": _utc_now_iso(),
            "applied_proposal_ids": [proposal["proposal_id"]],
            "applied_proposal_types": [proposal["proposal_type"]],
            "promotion_mode": "governed_autonomy",
            "candidate_reference": candidate_references,
            "proposal": proposal,
        }
    )
    write_json(history, paths.spec_history)
