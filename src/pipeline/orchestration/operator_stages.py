from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd

from pipeline.agent.agent import attempt_auto_remediation, diagnose_validation_failures
from pipeline.agent.execution_planner import LoopAction, build_observation, decide_loop_action
from pipeline.config import PipelinePaths
from pipeline.io.parquet_io import read_parquet, write_parquet
from pipeline.orchestration.operator_reports import (
    _extract_llm_diagnoses,
    _incident_id,
    _utc_now_iso,
)
from pipeline.quality.publication import sanitize_for_publication
from pipeline.quality.quality import (
    ValidationResult,
    summarize_validation_results,
    validate_bronze,
    validate_cross_layer_consistency,
    validate_gold,
    validate_gold_macro,
    validate_silver,
    validate_silver_conversations_llm,
    validate_silver_messages,
)
from pipeline.quality.quarantine import quarantine_bronze_records
from pipeline.quality.schema_drift import (
    DriftEvent,
)
from pipeline.runtime.terminal_logging import log_event
from pipeline.transforms.bronze import detect_bronze_schema_drift, load_bronze_frame
from pipeline.transforms.conversation_enrichment import build_conversation_enrichment
from pipeline.transforms.gold import build_gold
from pipeline.transforms.gold_macro import build_gold_macro
from pipeline.transforms.silver import build_silver, build_silver_leads

__all__ = [
    "StageDeps",
    "_get_llm_call",
    "_determine_retry_stage",
    "_run_react_iteration",
    "_run_validation_suite",
    "_run_bronze_stage",
    "_run_silver_stage",
    "_run_gold_stage",
    "_run_validation_stage",
]


@dataclass
class StageDeps:
    run_bronze: Callable[..., dict[str, Any]]
    run_silver: Callable[..., dict[str, Any]]
    run_gold: Callable[..., dict[str, Any]]
    run_validation: Callable[..., dict[str, Any]]

    @staticmethod
    def default() -> StageDeps:
        return StageDeps(
            run_bronze=_run_bronze_stage,
            run_silver=_run_silver_stage,
            run_gold=_run_gold_stage,
            run_validation=_run_validation_stage,
        )


def _get_llm_call() -> Any:
    from pipeline.runtime.env import env_flag

    if not env_flag("PIPELINE_ENABLE_LLM_AGENT", True):
        return None
    try:
        from pipeline.runtime.llm_runtime import call_llm

        return call_llm
    except Exception:
        return None


def _determine_retry_stage(
    failed_checks: list[dict[str, Any]],
    stage_failure_counts: dict[str, int],
) -> str | None:
    stage_order = ["bronze", "silver", "gold"]
    for stage in stage_order:
        for check in failed_checks:
            layer = str(check.get("layer", ""))
            if layer.startswith(stage) or layer == stage:
                count = stage_failure_counts.get(stage, 0)
                if count < 2:
                    return stage
    return None


def _run_bronze_stage(
    paths: PipelinePaths,
    compiled_plan: dict[str, Any],
    spec: dict[str, Any] | None = None,
) -> dict[str, Any]:
    bronze_df = load_bronze_frame(str(paths.raw_bronze_source))
    log_event(logging.INFO, "bronze_loaded", rows=int(len(bronze_df)))

    drift_events: list[DriftEvent] = []
    if spec:
        drift_events = detect_bronze_schema_drift(bronze_df, spec)
        if drift_events:
            log_event(
                logging.INFO,
                "schema_drift_detected",
                event_count=len(drift_events),
                unknown=sum(1 for e in drift_events if e.drift_class == "unknown"),
                missing_required=sum(
                    1 for e in drift_events if e.drift_class == "missing_required"
                ),
            )

    quarantine = quarantine_bronze_records(bronze_df, paths.quarantine, compiled_plan)
    bronze_df = quarantine["clean_df"]
    quarantine_report: dict[str, Any] = quarantine["report"]
    log_event(
        logging.INFO,
        "quarantine_completed",
        quarantined_rows=quarantine_report.get("quarantined_rows", 0),
        clean_rows=int(len(bronze_df)),
    )

    bronze_path = paths.bronze / "conversations.parquet"
    write_parquet(bronze_df, bronze_path)

    return {
        "bronze_df": bronze_df,
        "quarantine_report": quarantine_report,
        "drift_events": drift_events,
    }


def _run_silver_stage(
    bronze_df: pd.DataFrame,
    paths: PipelinePaths,
    compiled_plan: dict[str, Any],
    silver_conversations_llm_path: Path,
    spec: dict[str, Any] | None = None,
    drift_events: list[DriftEvent] | None = None,
) -> dict[str, Any]:
    silver_messages_runtime_df = build_silver(
        bronze_df,
        compiled_plan=compiled_plan,
        contract=spec,
        drift_events=drift_events,
    )
    silver_runtime_df = build_silver_leads(silver_messages_runtime_df, contract=spec)
    silver_df = sanitize_for_publication(silver_runtime_df, "silver")
    silver_messages_df = sanitize_for_publication(silver_messages_runtime_df, "silver_messages")
    log_event(
        logging.INFO,
        "silver_completed",
        silver_rows=int(len(silver_df)),
        silver_messages_rows=int(len(silver_messages_df)),
    )

    existing_enrichment = (
        read_parquet(silver_conversations_llm_path)
        if silver_conversations_llm_path.exists()
        else None
    )
    silver_conversations_llm_runtime_df = build_conversation_enrichment(
        silver_messages_runtime_df,
        compiled_plan=compiled_plan,
        existing_enrichment=existing_enrichment,
    )
    silver_conversations_llm_df = sanitize_for_publication(
        silver_conversations_llm_runtime_df, "silver_conversations_llm"
    )
    log_event(
        logging.INFO,
        "enrichment_completed",
        silver_conversations_llm_rows=int(len(silver_conversations_llm_df)),
    )

    silver_path = paths.silver / "silver_leads.parquet"
    silver_messages_path = paths.silver / "silver_messages.parquet"
    write_parquet(silver_df, silver_path)
    write_parquet(silver_messages_df, silver_messages_path)
    write_parquet(silver_conversations_llm_df, silver_conversations_llm_path)

    return {
        "silver_runtime_df": silver_runtime_df,
        "silver_messages_runtime_df": silver_messages_runtime_df,
        "silver_df": silver_df,
        "silver_messages_df": silver_messages_df,
        "silver_conversations_llm_runtime_df": silver_conversations_llm_runtime_df,
        "silver_conversations_llm_df": silver_conversations_llm_df,
    }


def _run_gold_stage(
    silver_runtime_df: pd.DataFrame,
    silver_messages_runtime_df: pd.DataFrame,
    silver_conversations_llm_runtime_df: pd.DataFrame | None,
    paths: PipelinePaths,
    compiled_plan: dict[str, Any],
    spec: dict[str, Any],
    llm_call: Any,
) -> dict[str, Any]:
    from pipeline.agent.gold_designer import design_gold_columns

    gold_column_plan = design_gold_columns(
        silver_leads_df=silver_runtime_df,
        silver_messages_df=silver_messages_runtime_df,
        spec=spec,
        paths=paths,
        llm_call=llm_call,
        compiled_plan=compiled_plan,
    )
    log_event(
        logging.INFO,
        "gold_column_plan_designed",
        source=gold_column_plan.source,
        column_count=len(gold_column_plan.columns),
    )

    gold_drop_events: list[DriftEvent] = []
    gold_runtime_df = build_gold(
        silver_runtime_df,
        silver_messages_runtime_df,
        silver_conversations_llm_runtime_df,
        compiled_plan=compiled_plan,
        gold_column_plan=gold_column_plan,
        contract=spec,
        drop_events=gold_drop_events,
    )
    gold_df = sanitize_for_publication(gold_runtime_df, "gold")
    gold_path = paths.gold / "conversations_gold.parquet"
    write_parquet(gold_df, gold_path)
    log_event(logging.INFO, "gold_completed", gold_rows=int(len(gold_df)))

    gold_macro_df = build_gold_macro(gold_df, contract=spec)
    gold_macro_path = paths.gold / "conversations_gold_macro.parquet"
    write_parquet(gold_macro_df, gold_macro_path)
    log_event(logging.INFO, "gold_macro_completed", gold_macro_rows=int(len(gold_macro_df)))

    return {
        "gold_column_plan": gold_column_plan,
        "gold_runtime_df": gold_runtime_df,
        "gold_df": gold_df,
        "gold_macro_df": gold_macro_df,
        "gold_drop_events": gold_drop_events,
    }


def _run_validation_suite(
    bronze_df: Any,
    silver_df: Any,
    silver_messages_df: Any,
    silver_conversations_llm_df: Any,
    gold_df: Any,
    gold_macro_df: Any,
    compiled_plan: dict[str, Any],
) -> list[ValidationResult]:
    return (
        validate_bronze(bronze_df, compiled_plan=compiled_plan)
        + validate_silver(silver_df, compiled_plan=compiled_plan)
        + validate_silver_messages(silver_messages_df, compiled_plan=compiled_plan)
        + validate_silver_conversations_llm(
            silver_conversations_llm_df,
            compiled_plan=compiled_plan,
        )
        + validate_gold(gold_df, compiled_plan=compiled_plan)
        + validate_cross_layer_consistency(
            silver_df,
            silver_messages_df,
            gold_df,
            compiled_plan=compiled_plan,
        )
        + validate_gold_macro(gold_macro_df, compiled_plan=compiled_plan)
    )


def _run_validation_stage(
    bronze_df: pd.DataFrame,
    silver_df: pd.DataFrame,
    silver_messages_df: pd.DataFrame,
    silver_conversations_llm_df: pd.DataFrame,
    gold_df: pd.DataFrame,
    gold_macro_df: pd.DataFrame | None,
    compiled_plan: dict[str, Any],
) -> dict[str, Any]:
    validation_results = _run_validation_suite(
        bronze_df,
        silver_df,
        silver_messages_df,
        silver_conversations_llm_df,
        gold_df,
        gold_macro_df,
        compiled_plan,
    )
    validation_summary = summarize_validation_results(validation_results)
    return {
        "validation_results": validation_results,
        "validation_summary": validation_summary,
    }


def _run_react_iteration(
    state: dict[str, Any],
    deps: StageDeps,
    paths: PipelinePaths,
    compiled_plan: dict[str, Any],
    spec: dict[str, Any],
    agent_ctx: dict[str, Any],
) -> dict[str, Any]:
    """Execute one ReAct loop iteration; mutates agent_ctx['current_stage'] in-place for exception
    tracking and returns an updated copy of agent_ctx extended with control/accumulation keys."""
    iteration: int = agent_ctx["iteration"]
    execution_plan = agent_ctx["execution_plan"]
    changed: bool = agent_ctx["changed"]
    current_fingerprint: dict[str, Any] = agent_ctx["current_fingerprint"]
    llm_call = agent_ctx["llm_call"]

    stage_failure_counts: dict[str, int] = dict(agent_ctx.get("stage_failure_counts", {}))
    executed_stages: set[str] = set(agent_ctx.get("executed_stages", set()))
    failed_stage: str | None = agent_ctx.get("failed_stage")
    validation_passed: bool = agent_ctx.get("validation_passed", False)
    run_status: str = agent_ctx.get("run_status", "success")
    agent_status: str = agent_ctx.get("agent_status", "healthy")

    bronze_df = agent_ctx.get("bronze_df")
    silver_df = agent_ctx.get("silver_df")
    silver_messages_df = agent_ctx.get("silver_messages_df")
    silver_runtime_df = agent_ctx.get("silver_runtime_df")
    silver_messages_runtime_df = agent_ctx.get("silver_messages_runtime_df")
    silver_conversations_llm_df = agent_ctx.get("silver_conversations_llm_df")
    silver_conversations_llm_runtime_df = agent_ctx.get("silver_conversations_llm_runtime_df")
    gold_df = agent_ctx.get("gold_df")
    gold_runtime_df = agent_ctx.get("gold_runtime_df")
    gold_macro_df = agent_ctx.get("gold_macro_df")
    gold_column_plan = agent_ctx.get("gold_column_plan")
    quarantine_report: dict[str, Any] = agent_ctx.get("quarantine_report", {"quarantined_rows": 0})
    validation_summary: dict[str, Any] = agent_ctx.get("validation_summary", {})
    row_counts: dict[str, Any] = agent_ctx.get("row_counts", {})
    remediation: dict[str, Any] = agent_ctx.get(
        "remediation",
        {
            "classification": "not_applicable",
            "attempted": False,
            "applied": False,
            "actions": [],
            "resolved": False,
            "candidate_path": None,
        },
    )

    iteration_diagnoses: list[dict[str, Any]] = []
    iteration_llm_diagnoses: list[dict[str, Any]] = []
    iteration_decisions: list[dict[str, Any]] = []

    silver_conversations_llm_path = paths.silver / "silver_conversations_llm.parquet"
    silver_path = paths.silver / "silver_leads.parquet"
    silver_messages_path = paths.silver / "silver_messages.parquet"
    gold_path = paths.gold / "conversations_gold.parquet"

    current_obs = build_observation(paths, state, changed)
    loop_action = decide_loop_action(
        execution_plan=execution_plan,
        observation=current_obs,
        stage_failure_counts=stage_failure_counts,
        iteration=iteration,
        validation_passed=validation_passed,
        executed_stages=executed_stages,
        failed_stage=failed_stage,
    )
    log_event(
        logging.INFO,
        "react_loop_action",
        kind=loop_action.kind,
        stage=loop_action.stage,
        reason=loop_action.reason,
        iteration=iteration,
    )
    final_loop_action: LoopAction | None = loop_action

    def _result(should_break: bool) -> dict[str, Any]:
        return {
            **agent_ctx,
            "stage_failure_counts": stage_failure_counts,
            "executed_stages": executed_stages,
            "failed_stage": failed_stage,
            "validation_passed": validation_passed,
            "run_status": run_status,
            "agent_status": agent_status,
            "final_loop_action": final_loop_action,
            "bronze_df": bronze_df,
            "silver_df": silver_df,
            "silver_messages_df": silver_messages_df,
            "silver_runtime_df": silver_runtime_df,
            "silver_messages_runtime_df": silver_messages_runtime_df,
            "silver_conversations_llm_df": silver_conversations_llm_df,
            "silver_conversations_llm_runtime_df": silver_conversations_llm_runtime_df,
            "gold_df": gold_df,
            "gold_runtime_df": gold_runtime_df,
            "gold_macro_df": gold_macro_df,
            "gold_column_plan": gold_column_plan,
            "quarantine_report": quarantine_report,
            "validation_summary": validation_summary,
            "row_counts": row_counts,
            "remediation": remediation,
            "should_break": should_break,
            "iteration_diagnoses": iteration_diagnoses,
            "iteration_llm_diagnoses": iteration_llm_diagnoses,
            "iteration_decisions": iteration_decisions,
        }

    if loop_action.kind == "complete":
        return _result(True)

    if loop_action.kind == "halt":
        if run_status not in ("halted_critical_failure",):
            run_status = "halted_stage_failure"
            agent_status = "halted_stage_failure"
        return _result(True)

    stage = loop_action.stage
    assert stage is not None
    failed_stage = None

    if stage == "bronze":
        agent_ctx["current_stage"] = "bronze_load"
        result = deps.run_bronze(paths, compiled_plan, spec)
        bronze_df = result["bronze_df"]
        quarantine_report = result["quarantine_report"]
        agent_ctx["drift_events"] = result.get("drift_events", [])
        executed_stages.add("bronze")

    elif stage == "silver":
        agent_ctx["current_stage"] = "silver_build"
        result = deps.run_silver(
            bronze_df,
            paths,
            compiled_plan,
            silver_conversations_llm_path,
            spec,
            agent_ctx.get("drift_events", []),
        )
        silver_runtime_df = result["silver_runtime_df"]
        silver_messages_runtime_df = result["silver_messages_runtime_df"]
        silver_df = result["silver_df"]
        silver_messages_df = result["silver_messages_df"]
        silver_conversations_llm_runtime_df = result["silver_conversations_llm_runtime_df"]
        silver_conversations_llm_df = result["silver_conversations_llm_df"]
        executed_stages.add("silver")
        executed_stages.discard("gold")
        executed_stages.discard("validation")

    elif stage == "gold":
        agent_ctx["current_stage"] = "gold_build"
        result = deps.run_gold(
            silver_runtime_df,
            silver_messages_runtime_df,
            silver_conversations_llm_runtime_df,
            paths,
            compiled_plan,
            spec,
            llm_call,
        )
        gold_column_plan = result["gold_column_plan"]
        gold_runtime_df = result["gold_runtime_df"]
        gold_df = result["gold_df"]
        gold_macro_df = result["gold_macro_df"]
        existing_drift = list(agent_ctx.get("drift_events", []))
        existing_drift.extend(result.get("gold_drop_events", []))
        agent_ctx["drift_events"] = existing_drift
        executed_stages.add("gold")
        executed_stages.discard("validation")

    elif stage == "validation":
        agent_ctx["current_stage"] = "validation"
        val_result = deps.run_validation(
            bronze_df,
            silver_df,
            silver_messages_df,
            silver_conversations_llm_df,
            gold_df,
            gold_macro_df,
            compiled_plan,
        )
        validation_summary = dict(val_result["validation_summary"])
        validation_summary["executed_at_utc"] = _utc_now_iso()
        validation_summary["row_counts"] = {
            "bronze": int(len(cast(pd.DataFrame, bronze_df))),
            "silver": int(len(cast(pd.DataFrame, silver_df))),
            "silver_messages": int(len(cast(pd.DataFrame, silver_messages_df))),
            "silver_conversations_llm": int(len(cast(pd.DataFrame, silver_conversations_llm_df))),
            "gold": int(len(cast(pd.DataFrame, gold_df))),
            "gold_macro": int(len(gold_macro_df)) if gold_macro_df is not None else 0,
        }
        validation_summary["source_fingerprint"] = current_fingerprint
        validation_summary["pipeline_spec_path"] = str(paths.pipeline_spec)

        failed_checks = cast(list[dict[str, Any]], validation_summary["failed_checks"])
        row_counts = cast(dict[str, Any], validation_summary["row_counts"])
        log_event(
            logging.INFO,
            "validation_completed",
            status=validation_summary["status"],
            failed_check_count=len(failed_checks),
            iteration=iteration,
        )

        if validation_summary["status"] == "passed":
            validation_passed = True
            executed_stages.add("validation")
            final_loop_action = LoopAction(kind="complete", stage=None, reason="validation_passed")
            return _result(True)

        diagnoses_list = diagnose_validation_failures(
            failed_checks, compiled_plan, llm_call=llm_call
        )
        iteration_diagnoses = [d.as_dict() for d in diagnoses_list]
        iteration_llm_diagnoses = _extract_llm_diagnoses(iteration_diagnoses)

        critical_found = any(d.get("severity") == "critical" for d in iteration_diagnoses)
        if critical_found:
            log_event(logging.ERROR, "critical_diagnosis_halt", iteration=iteration)
            final_loop_action = LoopAction(kind="halt", stage=None, reason="critical_diagnosis")
            run_status = "halted_critical_failure"
            agent_status = "halted_critical_failure"
            executed_stages.add("validation")
            return _result(True)

        rem = attempt_auto_remediation(
            bronze_df=bronze_df,
            silver_df=silver_df,
            silver_messages_df=silver_messages_df,
            gold_df=gold_df,
            failed_checks=failed_checks,
            compiled_plan=compiled_plan,
            llm_diagnoses=diagnoses_list,
            incident_id=_incident_id(),
            paths=paths,
        )
        iteration_decisions.extend(cast(list[dict[str, Any]], rem["decisions"]))

        if rem["resolved"]:
            silver_df = rem["silver_df"]
            silver_messages_df = rem["silver_messages_df"]
            gold_df = rem["gold_df"]
            write_parquet(silver_df, silver_path)
            write_parquet(silver_messages_df, silver_messages_path)
            write_parquet(silver_conversations_llm_df, silver_conversations_llm_path)
            write_parquet(gold_df, gold_path)
            remediation = {
                "classification": "auto_remediated",
                "attempted": True,
                "applied": bool(rem["actions"]),
                "actions": rem["actions"],
                "resolved": True,
                "candidate_path": rem.get("candidate_path"),
            }
            validation_passed = True
            agent_status = "auto_remediated"
            run_status = "success_after_auto_remediation"
            final_loop_action = LoopAction(
                kind="complete", stage=None, reason="auto_remediation_resolved"
            )
            executed_stages.add("validation")
            return _result(True)

        retry_stage = _determine_retry_stage(failed_checks, stage_failure_counts)
        if retry_stage is None:
            final_loop_action = LoopAction(kind="halt", stage=None, reason="no_recoverable_stage")
            return _result(True)

        stage_failure_counts[retry_stage] = stage_failure_counts.get(retry_stage, 0) + 1
        if stage_failure_counts[retry_stage] >= 2:
            final_loop_action = LoopAction(
                kind="halt",
                stage=retry_stage,
                reason=f"repeated_stage_failure:{retry_stage}",
            )
            return _result(True)

        log_event(
            logging.INFO,
            "react_retry_stage",
            stage=retry_stage,
            iteration=iteration,
            failure_count=stage_failure_counts[retry_stage],
        )
        _stage_cascade_order = ["bronze", "silver", "gold", "validation"]
        retry_idx = (
            _stage_cascade_order.index(retry_stage) if retry_stage in _stage_cascade_order else -1
        )
        if retry_idx >= 0:
            for _s in _stage_cascade_order[retry_idx:]:
                executed_stages.discard(_s)

        remediation = {
            "classification": "unresolved_manual_action"
            if any(bool(d.get("auto_remediable")) for d in iteration_diagnoses)
            else "not_auto_remediable",
            "attempted": True,
            "applied": bool(rem["actions"]),
            "actions": rem["actions"],
            "resolved": False,
            "candidate_path": rem.get("candidate_path"),
        }

    elif stage == "planning":
        executed_stages.add("planning")

    return _result(False)
