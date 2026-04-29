# GUD-001 EXCEPTION: This file exceeds 400 lines. Justification: run_cycle() must call
# load_bronze_frame, build_silver, build_gold, and validate_gold directly from this module's
# namespace so that existing tests can monkeypatch them via pipeline.orchestration.operator
# (CON-004). Delegating those calls to operator_stages would break the patches. All auxiliary
# logic has been extracted to operator_artifacts, operator_reports, and operator_stages; only
# run_cycle() and build_monitor_snapshot() are defined here.
from __future__ import annotations

import logging
from typing import Any, cast

from pipeline.agent.agent import (
    attempt_auto_remediation,
    diagnose_exception,
    diagnose_validation_failures,
)
from pipeline.agent.execution_planner import (
    VALID_STAGES,
    ExecutionPlan,
    LoopAction,
    build_execution_plan,
    build_observation,
    decide_loop_action,
)
from pipeline.agent.planner import plan_pipeline_spec
from pipeline.config import PipelinePaths, ensure_directories
from pipeline.io.parquet_io import read_json, read_parquet, write_parquet
from pipeline.orchestration.compiler import compile_pipeline_spec
from pipeline.orchestration.operator_artifacts import (  # noqa: F401
    PipelineArtifacts,
    _skip_artifacts,
    _success_artifacts,
    agent_report_file,
    alert_report_file,
    plan_report_file,
    state_file,
    validation_report_file,
)
from pipeline.orchestration.operator_reports import (  # noqa: F401
    _build_agent_report,
    _build_alert_report,
    _extract_llm_diagnoses,
    _incident_id,
    _planner_report_summary,
    _run_record,
    _utc_now_iso,
    _write_reports,
)
from pipeline.orchestration.operator_stages import (  # noqa: F401
    _determine_retry_stage,
    _get_llm_call,
    _run_bronze_stage,
    _run_gold_stage,
    _run_silver_stage,
    _run_validation_stage,
    _run_validation_suite,
)
from pipeline.quality.publication import sanitize_for_publication
from pipeline.quality.quality import (
    summarize_validation_results,
    validate_gold,  # noqa: F401 — patchable via operator namespace (CON-004)
)
from pipeline.quality.quarantine import quarantine_bronze_records
from pipeline.runtime.spec import ensure_pipeline_spec
from pipeline.runtime.state import (
    build_source_fingerprint,
    has_source_changed,
    load_pipeline_state,
    save_pipeline_state,
)
from pipeline.runtime.terminal_logging import log_event
from pipeline.transforms.bronze import load_bronze_frame
from pipeline.transforms.conversation_enrichment import build_conversation_enrichment
from pipeline.transforms.gold import build_gold
from pipeline.transforms.gold_macro import build_gold_macro
from pipeline.transforms.silver import build_silver, build_silver_leads

__all__ = [
    "PipelineArtifacts",
    "_EMPTY_PLANNER_REPORT",
    "run_cycle",
    "build_monitor_snapshot",
    "state_file",
    "validation_report_file",
    "agent_report_file",
    "alert_report_file",
    "plan_report_file",
    "_skip_artifacts",
    "_success_artifacts",
    "_build_agent_report",
    "_build_alert_report",
    "_write_reports",
    "_run_record",
    "_planner_report_summary",
    "_utc_now_iso",
    "_extract_llm_diagnoses",
    "_incident_id",
    "_get_llm_call",
    "_determine_retry_stage",
    "_run_bronze_stage",
    "_run_silver_stage",
    "_run_gold_stage",
    "_run_validation_stage",
    "_run_validation_suite",
    # patchable names used by tests (imported at module level)
    "load_bronze_frame",
    "build_silver",
    "build_gold",
    "validate_gold",
]

_MAX_REACT_ITERATIONS = 15  # 5 stages × up to 3 iterations each
_EMPTY_PLANNER_REPORT: dict[str, Any] = {
    "proposals": [],
    "applied": False,
    "skipped": True,
    "reason": "no_source_change_and_cadence_not_triggered",
    "proposal_count": 0,
    "promoted_proposal_ids": [],
}


def run_cycle(
    paths: PipelinePaths,
    force: bool = False,
    idle_cycle_count: int = 0,
    planner_cadence: int = 0,
) -> PipelineArtifacts:  # noqa: C901
    ensure_directories(paths)
    spec = ensure_pipeline_spec(paths.pipeline_spec)
    compiled_plan = compile_pipeline_spec(spec)

    state_path = state_file(paths)
    report_path = validation_report_file(paths)
    agent_report_path = agent_report_file(paths)
    alert_report_path = alert_report_file(paths)
    current_fingerprint_obj = build_source_fingerprint(paths.raw_bronze_source)
    current_fingerprint = current_fingerprint_obj.as_dict()
    state = load_pipeline_state(state_path)
    previous_fingerprint = state.get("last_source_fingerprint")
    changed = has_source_changed(current_fingerprint_obj, previous_fingerprint)

    cadence_triggered = (
        planner_cadence > 0 and idle_cycle_count > 0 and idle_cycle_count % planner_cadence == 0
    )
    should_plan = changed or force or cadence_triggered

    if should_plan:
        if cadence_triggered and not changed and not force:
            log_event(
                logging.INFO,
                "planner_triggered_by_cadence",
                idle_cycle_count=idle_cycle_count,
                cadence=planner_cadence,
            )
        planner_report = plan_pipeline_spec(paths)
    else:
        log_event(
            logging.INFO,
            "planner_skipped",
            reason="no_source_change_and_cadence_not_triggered",
            idle_cycle_count=idle_cycle_count,
            cadence=planner_cadence,
        )
        planner_report = _EMPTY_PLANNER_REPORT
    planner_summary = _planner_report_summary(paths, planner_report)

    log_event(logging.INFO, "run_started", force=force)
    log_event(logging.INFO, "source_change_evaluated", changed=changed, force=force)

    # Decision 1: Adaptive Execution Planning
    llm_call = _get_llm_call()
    observation = build_observation(paths, state, changed)
    execution_plan = build_execution_plan(
        observation,
        paths,
        llm_call=llm_call,
        compiled_plan=compiled_plan,
    )
    log_event(
        logging.INFO,
        "execution_plan_built",
        stages=execution_plan.stages,
        source=execution_plan.source,
        confidence=execution_plan.confidence,
    )

    # When forced with an empty plan, override to run all stages
    if force and not execution_plan.stages:
        execution_plan = ExecutionPlan(
            stages=list(VALID_STAGES),
            rationale="Forced execution of all stages.",
            confidence=1.0,
            source="deterministic_fallback",
            generated_at_utc=_utc_now_iso(),
        )
        log_event(logging.INFO, "execution_plan_forced", stages=execution_plan.stages)

    if not force and not changed and not execution_plan.stages:
        agent_report = _build_agent_report(
            incident_id=_incident_id(),
            status="idle_no_source_change",
            diagnoses=[],
            auto_remediation={
                "classification": "not_applicable",
                "attempted": False,
                "applied": False,
                "actions": [],
                "resolved": False,
            },
            fallback={"applied": False},
            planner_report=planner_summary,
            execution_plan=execution_plan.as_dict(),
        )
        run_record = _run_record(
            source_fingerprint=current_fingerprint,
            executed=False,
            status="skipped_no_source_change",
            details={"reason": "source fingerprint unchanged"},
            agent_summary={"status": agent_report["status"], "diagnosis_count": 0},
        )
        validation_summary: dict[str, Any] = {
            "status": "passed",
            "checks": [],
            "failed_checks": [],
            "executed_at_utc": _utc_now_iso(),
            "source_fingerprint": current_fingerprint,
            "pipeline_spec_path": str(paths.pipeline_spec),
        }
        alert_report = _build_alert_report(paths, run_record, agent_report, validation_summary)
        _write_reports(
            validation_summary,
            agent_report,
            alert_report,
            report_path,
            agent_report_path,
            alert_report_path,
            paths.agent_decisions,
        )
        state.setdefault("runs", []).append(run_record)
        state["last_seen_at_utc"] = _utc_now_iso()
        save_pipeline_state(state_path, state)
        log_event(logging.INFO, "reports_persisted", status="skipped_no_source_change")
        log_event(logging.WARNING, "run_skipped", reason="source_fingerprint_unchanged")
        return _skip_artifacts(paths)

    bronze_path = paths.bronze / "conversations.parquet"
    silver_path = paths.silver / "silver_leads.parquet"
    silver_messages_path = paths.silver / "silver_messages.parquet"
    silver_conversations_llm_path = paths.silver / "silver_conversations_llm.parquet"
    gold_path = paths.gold / "conversations_gold.parquet"

    current_stage = "initialization"

    try:
        # Initialize all stage variables (populated inside the ReAct loop)
        bronze_df: Any = None
        silver_df: Any = None
        silver_messages_df: Any = None
        silver_runtime_df: Any = None
        silver_messages_runtime_df: Any = None
        silver_conversations_llm_df: Any = None
        silver_conversations_llm_runtime_df: Any = None
        gold_df: Any = None
        gold_runtime_df: Any = None
        gold_macro_df: Any = None
        gold_column_plan: Any = None
        quarantine: dict[str, Any] = {"clean_df": None, "report": {"quarantined_rows": 0}}
        quarantine_report: dict[str, Any] = {"quarantined_rows": 0}

        # --- ReAct loop: stage execution + validation + remediation ---
        executed_stages: set[str] = set()
        failed_stage: str | None = None
        stage_failure_counts: dict[str, int] = {}
        all_diagnoses: list[dict[str, Any]] = []
        all_llm_diagnoses: list[dict[str, Any]] = []
        remediation: dict[str, Any] = {
            "classification": "not_applicable",
            "attempted": False,
            "applied": False,
            "actions": [],
            "resolved": False,
            "candidate_path": None,
        }
        decisions: list[dict[str, Any]] = []
        agent_status = "healthy"
        run_status = "success"
        final_loop_action: LoopAction | None = None
        validation_passed = False
        validation_summary = {
            "status": "pending",
            "checks": [],
            "failed_checks": [],
            "executed_at_utc": _utc_now_iso(),
            "source_fingerprint": current_fingerprint,
            "pipeline_spec_path": str(paths.pipeline_spec),
        }
        row_counts: dict[str, Any] = {}

        for iteration in range(_MAX_REACT_ITERATIONS):
            current_stage = f"loop_iteration_{iteration}"
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
            final_loop_action = loop_action

            if loop_action.kind == "complete":
                break

            if loop_action.kind == "halt":
                if run_status not in ("halted_critical_failure",):
                    run_status = "halted_stage_failure"
                    agent_status = "halted_stage_failure"
                break

            stage = loop_action.stage
            assert stage is not None
            failed_stage = None  # reset; set only on failure

            if stage == "bronze":
                current_stage = "bronze_load"
                bronze_df = load_bronze_frame(str(paths.raw_bronze_source))
                log_event(logging.INFO, "bronze_loaded", rows=int(len(bronze_df)))

                current_stage = "quarantine_processing"
                quarantine = quarantine_bronze_records(bronze_df, paths.quarantine, compiled_plan)
                bronze_df = cast(Any, quarantine["clean_df"])
                quarantine_report = cast(dict[str, Any], quarantine["report"])
                log_event(
                    logging.INFO,
                    "quarantine_completed",
                    quarantined_rows=quarantine_report.get("quarantined_rows", 0),
                    clean_rows=int(len(bronze_df)),
                )
                current_stage = "bronze_persist"
                write_parquet(bronze_df, bronze_path)
                executed_stages.add("bronze")

            elif stage == "silver":
                current_stage = "silver_build"
                silver_messages_runtime_df = build_silver(bronze_df, compiled_plan=compiled_plan)
                silver_runtime_df = build_silver_leads(silver_messages_runtime_df)
                silver_df = sanitize_for_publication(silver_runtime_df, "silver")
                silver_messages_df = sanitize_for_publication(
                    silver_messages_runtime_df, "silver_messages"
                )
                log_event(
                    logging.INFO,
                    "silver_completed",
                    silver_rows=int(len(silver_df)),
                    silver_messages_rows=int(len(silver_messages_df)),
                )

                current_stage = "enrichment_build"
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
                current_stage = "silver_persist"
                write_parquet(silver_df, silver_path)
                write_parquet(silver_messages_df, silver_messages_path)
                write_parquet(silver_conversations_llm_df, silver_conversations_llm_path)
                executed_stages.add("silver")
                executed_stages.discard("gold")
                executed_stages.discard("validation")

            elif stage == "gold":
                # Decision 2: Dynamic Gold Designer
                current_stage = "gold_design"
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
                current_stage = "gold_build"
                gold_runtime_df = build_gold(
                    silver_runtime_df,
                    silver_messages_runtime_df,
                    silver_conversations_llm_runtime_df,
                    compiled_plan=compiled_plan,
                    gold_column_plan=gold_column_plan,
                )
                gold_df = sanitize_for_publication(gold_runtime_df, "gold")
                current_stage = "gold_persist"
                write_parquet(gold_df, gold_path)
                log_event(logging.INFO, "gold_completed", gold_rows=int(len(gold_df)))
                current_stage = "gold_macro_build"
                gold_macro_df = build_gold_macro(gold_df)
                gold_macro_path_file = paths.gold / "conversations_gold_macro.parquet"
                write_parquet(gold_macro_df, gold_macro_path_file)
                log_event(
                    logging.INFO,
                    "gold_macro_completed",
                    gold_macro_rows=int(len(gold_macro_df)),
                )
                executed_stages.add("gold")
                executed_stages.discard("validation")

            elif stage == "validation":
                current_stage = "validation"
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
                validation_summary["executed_at_utc"] = _utc_now_iso()
                validation_summary["row_counts"] = {
                    "bronze": int(len(bronze_df)),
                    "silver": int(len(silver_df)),
                    "silver_messages": int(len(silver_messages_df)),
                    "silver_conversations_llm": int(len(silver_conversations_llm_df)),
                    "gold": int(len(gold_df)),
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
                    final_loop_action = LoopAction(
                        kind="complete", stage=None, reason="validation_passed"
                    )
                    break

                # Decision 3: Diagnose failures (with LLM for unknown checks)
                diagnoses_list = diagnose_validation_failures(
                    failed_checks, compiled_plan, llm_call=llm_call
                )
                iteration_diagnoses = [d.as_dict() for d in diagnoses_list]
                all_diagnoses.extend(iteration_diagnoses)
                iteration_llm_diagnoses = _extract_llm_diagnoses(iteration_diagnoses)
                all_llm_diagnoses.extend(iteration_llm_diagnoses)

                critical_found = any(d.get("severity") == "critical" for d in iteration_diagnoses)
                if critical_found:
                    log_event(logging.ERROR, "critical_diagnosis_halt", iteration=iteration)
                    final_loop_action = LoopAction(
                        kind="halt", stage=None, reason="critical_diagnosis"
                    )
                    run_status = "halted_critical_failure"
                    agent_status = "halted_critical_failure"
                    executed_stages.add("validation")
                    break

                # Attempt auto-remediation (GAP-02: llm_diagnoses; QUAL-03: incident/paths)
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
                decisions.extend(cast(list[dict[str, Any]], rem["decisions"]))

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
                    break

                # Remediation didn't resolve — determine retry stage
                retry_stage = _determine_retry_stage(failed_checks, stage_failure_counts)
                if retry_stage is None:
                    final_loop_action = LoopAction(
                        kind="halt", stage=None, reason="no_recoverable_stage"
                    )
                    break

                stage_failure_counts[retry_stage] = stage_failure_counts.get(retry_stage, 0) + 1
                if stage_failure_counts[retry_stage] >= 2:
                    final_loop_action = LoopAction(
                        kind="halt",
                        stage=retry_stage,
                        reason=f"repeated_stage_failure:{retry_stage}",
                    )
                    break

                log_event(
                    logging.INFO,
                    "react_retry_stage",
                    stage=retry_stage,
                    iteration=iteration,
                    failure_count=stage_failure_counts[retry_stage],
                )
                # Cascade-remove the failing stage and all downstream from executed_stages
                _stage_cascade_order = ["bronze", "silver", "gold", "validation"]
                retry_idx = (
                    _stage_cascade_order.index(retry_stage)
                    if retry_stage in _stage_cascade_order
                    else -1
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
                # Planning already ran before the loop
                executed_stages.add("planning")

        else:
            # Max iterations reached without break
            final_loop_action = LoopAction(kind="halt", stage=None, reason="max_iterations_reached")

        if run_status == "success":
            if validation_summary.get("status") != "passed":
                any_auto_remediable = any(bool(d.get("auto_remediable")) for d in all_diagnoses)
                agent_status = (
                    "manual_intervention_required" if any_auto_remediable else "not_auto_remediable"
                )
                run_status = "validation_failed"
            else:
                agent_status = "healthy"

        agent_report = _build_agent_report(
            incident_id=_incident_id(),
            status=agent_status,
            diagnoses=all_diagnoses,
            auto_remediation=remediation,
            fallback={"applied": False},
            decisions=decisions,
            planner_report=planner_summary,
            quarantine_report=quarantine_report,
            llm_diagnoses=all_llm_diagnoses,
            execution_plan=execution_plan.as_dict(),
        )

        if all_diagnoses and not validation_passed and run_status not in ("success",):
            non_auto = [d["kind"] for d in all_diagnoses if not d.get("auto_remediable")]
            agent_report["planner_report"] = agent_report["planner_report"] | {
                "related_incident_id": agent_report["incident_id"],
                "related_root_causes": non_auto,
            }

        run_record = _run_record(
            source_fingerprint=current_fingerprint,
            executed=True,
            status=run_status,
            validation_summary={
                "status": validation_summary.get("status"),
                "failed_checks": validation_summary.get("failed_checks", []),
            },
            details=row_counts | {"quarantined_rows": quarantine["report"]["quarantined_rows"]},
            agent_summary={
                "status": agent_report["status"],
                "diagnosis_count": len(agent_report["diagnoses"]),
                "incident_id": agent_report["incident_id"],
                "auto_remediation_applied": agent_report["auto_remediation"].get("applied", False),
                "auto_remediation_classification": agent_report["auto_remediation"].get(
                    "classification"
                ),
                "planner_proposal_count": agent_report["planner_report"].get("proposal_count", 0),
                "planner_applied": agent_report["planner_report"].get("applied", False),
                "planner_promoted_proposal_ids": agent_report["planner_report"].get(
                    "promoted_proposal_ids", []
                ),
                "react_loop_action": final_loop_action.kind if final_loop_action else None,
                "llm_diagnosis_count": len(all_llm_diagnoses),
            },
        )
        alert_report = _build_alert_report(paths, run_record, agent_report, validation_summary)
        _write_reports(
            validation_summary,
            agent_report,
            alert_report,
            report_path,
            agent_report_path,
            alert_report_path,
            paths.agent_decisions,
        )
        log_event(
            logging.INFO,
            "reports_persisted",
            status=run_status,
            incident_id=agent_report["incident_id"],
        )
        state.setdefault("runs", []).append(run_record)
        state["last_source_fingerprint"] = current_fingerprint
        if run_status in {"success", "success_after_auto_remediation"}:
            state["last_successful_run_at_utc"] = _utc_now_iso()
            state["last_successful_artifacts"] = {
                "bronze_path": str(bronze_path),
                "silver_path": str(silver_path),
                "silver_messages_path": str(silver_messages_path),
                "silver_conversations_llm_path": str(silver_conversations_llm_path),
                "gold_path": str(gold_path),
                "gold_macro_path": str(paths.gold / "conversations_gold_macro.parquet"),
                "validation_report_path": str(report_path),
                "agent_report_path": str(agent_report_path),
                "alert_report_path": str(alert_report_path),
            }
        state["last_seen_at_utc"] = _utc_now_iso()
        save_pipeline_state(state_path, state)
        log_event(
            logging.INFO,
            "run_succeeded",
            status=run_status,
            gold_rows=row_counts.get("gold"),
            incident_id=agent_report["incident_id"],
        )
        return _success_artifacts(paths, run_status)

    except Exception as exc:
        log_event(logging.ERROR, "run_failed", stage=current_stage, error=type(exc).__name__)
        diagnosis = diagnose_exception(exc).as_dict()
        last_successful_artifacts = state.get("last_successful_artifacts", {})
        fallback_applied = bool(last_successful_artifacts)
        run_status = "fallback_to_last_successful" if fallback_applied else "runtime_failed"
        validation_summary = {
            "status": "failed",
            "checks": [],
            "failed_checks": [
                {
                    "layer": "runtime",
                    "check": "unexpected_exception",
                    "status": "failed",
                    "detail": {"exception": f"{type(exc).__name__}: {exc}"},
                }
            ],
            "executed_at_utc": _utc_now_iso(),
            "source_fingerprint": current_fingerprint,
            "pipeline_spec_path": str(paths.pipeline_spec),
        }
        agent_report = _build_agent_report(
            incident_id=_incident_id(),
            status="fallback_applied" if fallback_applied else "manual_intervention_required",
            diagnoses=[diagnosis],
            auto_remediation={
                "classification": "escalated_to_fallback"
                if fallback_applied
                else "not_auto_remediable",
                "attempted": False,
                "applied": False,
                "actions": [],
                "resolved": False,
            },
            fallback={
                "applied": fallback_applied,
                "artifacts": last_successful_artifacts,
            },
            decisions=[
                {
                    "playbook_id": "fallback_to_last_successful_artifacts",
                    "selected": fallback_applied,
                    "reason": "Erro inesperado durante a execução do runtime.",
                }
            ],
            planner_report=planner_summary
            | {
                "related_incident_id": None,
                "related_root_causes": [diagnosis["kind"]],
            },
            exception=f"{type(exc).__name__}: {exc}",
            execution_plan=execution_plan.as_dict() if "execution_plan" in dir() else {},
        )
        agent_report["planner_report"]["related_incident_id"] = agent_report["incident_id"]
        run_record = _run_record(
            source_fingerprint=current_fingerprint,
            executed=True,
            status=run_status,
            validation_summary={
                "status": validation_summary["status"],
                "failed_checks": cast(list[dict[str, Any]], validation_summary["failed_checks"]),
            },
            details={"exception": f"{type(exc).__name__}: {exc}"},
            agent_summary={
                "status": agent_report["status"],
                "diagnosis_count": 1,
                "incident_id": agent_report["incident_id"],
                "fallback_applied": fallback_applied,
                "auto_remediation_classification": agent_report["auto_remediation"].get(
                    "classification"
                ),
                "planner_proposal_count": agent_report["planner_report"].get("proposal_count", 0),
                "planner_applied": agent_report["planner_report"].get("applied", False),
                "planner_promoted_proposal_ids": agent_report["planner_report"].get(
                    "promoted_proposal_ids", []
                ),
            },
        )
        alert_report = _build_alert_report(paths, run_record, agent_report, validation_summary)
        _write_reports(
            validation_summary,
            agent_report,
            alert_report,
            report_path,
            agent_report_path,
            alert_report_path,
            paths.agent_decisions,
        )
        state.setdefault("runs", []).append(run_record)
        state["last_seen_at_utc"] = _utc_now_iso()
        save_pipeline_state(state_path, state)
        log_event(
            logging.INFO if fallback_applied else logging.ERROR,
            "reports_persisted",
            status=run_status,
            incident_id=agent_report["incident_id"],
        )
        if fallback_applied:
            log_event(logging.WARNING, "fallback_applied", incident_id=agent_report["incident_id"])
        return _success_artifacts(paths, run_status)


def build_monitor_snapshot(paths: PipelinePaths) -> dict[str, Any]:
    state_path = state_file(paths)
    report_path = validation_report_file(paths)
    agent_report_path = agent_report_file(paths)
    alert_report_path = alert_report_file(paths)
    plan_report_path = plan_report_file(paths)
    state = load_pipeline_state(state_path)
    latest_report = read_json(report_path, default={})
    latest_agent_report = read_json(agent_report_path, default={})
    latest_alert_report = read_json(alert_report_path, default={})
    latest_plan_report = read_json(plan_report_path, default={})
    last_run = state.get("runs", [])[-1] if state.get("runs") else {}
    return {
        "state_path": str(state_path),
        "validation_report_path": str(report_path),
        "agent_report_path": str(agent_report_path),
        "alert_report_path": str(alert_report_path),
        "plan_report_path": str(plan_report_path),
        "autonomy_policy_path": str(paths.autonomy_policy),
        "autonomy_metrics_path": str(paths.autonomy_metrics),
        "pipeline_spec_path": str(paths.pipeline_spec),
        "last_run": last_run,
        "run_count": len(state.get("runs", [])),
        "last_source_fingerprint": state.get("last_source_fingerprint"),
        "latest_validation_status": latest_report.get("status"),
        "latest_failed_checks": latest_report.get("failed_checks", []),
        "latest_agent_status": latest_agent_report.get("status"),
        "latest_agent_diagnoses": latest_agent_report.get("diagnoses", []),
        "auto_remediation_applied": latest_agent_report.get("auto_remediation", {}).get(
            "applied", False
        ),
        "auto_remediation_actions": latest_agent_report.get("auto_remediation", {}).get(
            "actions", []
        ),
        "auto_remediation_classification": latest_agent_report.get("auto_remediation", {}).get(
            "classification"
        ),
        "latest_agent_fallback": latest_agent_report.get("fallback", {}),
        "planner_report_path": str(plan_report_path),
        "latest_planner_changes": latest_plan_report.get(
            "proposals",
            latest_plan_report.get("changes", []),
        ),
        "latest_planner_proposals": latest_plan_report.get("proposals", []),
        "latest_planner_applied": latest_plan_report.get("applied"),
        "planner_proposals": latest_plan_report.get("proposals", []),
        "planner_applied": latest_plan_report.get("applied", False),
        "latest_alert_severity": latest_alert_report.get("event", {}).get("severity"),
        "latest_alert_should_alert": latest_alert_report.get("event", {}).get("should_alert"),
        "latest_alert_delivery": latest_alert_report.get("delivery", {}),
    }
