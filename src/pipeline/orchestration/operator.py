# GUD-001 EXCEPTION: This file exceeds 400 lines. Justification: run_cycle() retains a
# non-trivial setup phase (planner, fingerprint, execution-plan, skip-path), a full
# exception-fallback handler, and report-writing logic that together exceed the threshold
# even after extracting all stage-dispatch logic to _run_react_iteration in operator_stages.py.
# Further decomposition is deferred to a separate spec.
from __future__ import annotations

import logging
from typing import Any, cast

from pipeline.agent.agent import diagnose_exception
from pipeline.agent.execution_planner import (
    VALID_STAGES,
    ExecutionPlan,
    LoopAction,
    build_execution_plan,
    build_observation,
)
from pipeline.agent.planner import plan_pipeline_spec
from pipeline.config import PipelinePaths, ensure_directories
from pipeline.io.parquet_io import read_json
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
    StageDeps,
    _determine_retry_stage,
    _get_llm_call,
    _run_bronze_stage,
    _run_gold_stage,
    _run_react_iteration,
    _run_silver_stage,
    _run_validation_stage,
    _run_validation_suite,
)
from pipeline.quality.quality import validate_gold  # noqa: F401 — re-exported (CON-003)
from pipeline.runtime.spec import ensure_pipeline_spec
from pipeline.runtime.state import (
    build_source_fingerprint,
    has_source_changed,
    load_pipeline_state,
    save_pipeline_state,
)
from pipeline.runtime.terminal_logging import log_event
from pipeline.transforms.bronze import load_bronze_frame  # noqa: F401 — re-exported (CON-003)
from pipeline.transforms.gold import build_gold  # noqa: F401 — re-exported (CON-003)
from pipeline.transforms.silver import build_silver  # noqa: F401 — re-exported (CON-003)

__all__ = [
    "PipelineArtifacts",
    "StageDeps",
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
    "_run_react_iteration",
    "_run_bronze_stage",
    "_run_silver_stage",
    "_run_gold_stage",
    "_run_validation_stage",
    "_run_validation_suite",
    # re-exported names kept for CON-003 compatibility
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
    *,
    stage_deps: StageDeps | None = None,
) -> PipelineArtifacts:
    deps = stage_deps or StageDeps.default()
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
        state = load_pipeline_state(state_path)
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

    agent_ctx: dict[str, Any] = {
        "execution_plan": execution_plan,
        "changed": changed,
        "current_fingerprint": current_fingerprint,
        "llm_call": llm_call,
        "stage_failure_counts": {},
        "executed_stages": set(),
        "failed_stage": None,
        "validation_passed": False,
        "run_status": "success",
        "agent_status": "healthy",
        "final_loop_action": None,
        "bronze_df": None,
        "silver_df": None,
        "silver_messages_df": None,
        "silver_runtime_df": None,
        "silver_messages_runtime_df": None,
        "silver_conversations_llm_df": None,
        "silver_conversations_llm_runtime_df": None,
        "gold_df": None,
        "gold_runtime_df": None,
        "gold_macro_df": None,
        "gold_column_plan": None,
        "quarantine_report": {"quarantined_rows": 0},
        "validation_summary": {
            "status": "pending",
            "checks": [],
            "failed_checks": [],
            "executed_at_utc": _utc_now_iso(),
            "source_fingerprint": current_fingerprint,
            "pipeline_spec_path": str(paths.pipeline_spec),
        },
        "row_counts": {},
        "remediation": {
            "classification": "not_applicable",
            "attempted": False,
            "applied": False,
            "actions": [],
            "resolved": False,
            "candidate_path": None,
        },
        "current_stage": "initialization",
    }

    all_diagnoses: list[dict[str, Any]] = []
    all_llm_diagnoses: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []

    try:
        for iteration in range(_MAX_REACT_ITERATIONS):
            agent_ctx["iteration"] = iteration
            iter_result = _run_react_iteration(state, deps, paths, compiled_plan, spec, agent_ctx)
            agent_ctx = iter_result
            all_diagnoses.extend(iter_result.get("iteration_diagnoses", []))
            all_llm_diagnoses.extend(iter_result.get("iteration_llm_diagnoses", []))
            decisions.extend(iter_result.get("iteration_decisions", []))
            if iter_result["should_break"]:
                break
        else:
            agent_ctx["final_loop_action"] = LoopAction(
                kind="halt", stage=None, reason="max_iterations_reached"
            )

        run_status = cast(str, agent_ctx["run_status"])
        agent_status = cast(str, agent_ctx["agent_status"])
        validation_summary = cast(dict[str, Any], agent_ctx["validation_summary"])
        validation_passed = cast(bool, agent_ctx["validation_passed"])
        row_counts = cast(dict[str, Any], agent_ctx["row_counts"])
        quarantine_report = cast(dict[str, Any], agent_ctx["quarantine_report"])
        final_loop_action = cast(LoopAction | None, agent_ctx["final_loop_action"])
        remediation = cast(dict[str, Any], agent_ctx["remediation"])

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
            details=row_counts | {"quarantined_rows": quarantine_report.get("quarantined_rows", 0)},
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
                "bronze_path": str(paths.bronze / "conversations.parquet"),
                "silver_path": str(paths.silver / "silver_leads.parquet"),
                "silver_messages_path": str(paths.silver / "silver_messages.parquet"),
                "silver_conversations_llm_path": str(
                    paths.silver / "silver_conversations_llm.parquet"
                ),
                "gold_path": str(paths.gold / "conversations_gold.parquet"),
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
        current_stage = agent_ctx.get("current_stage", "unknown")
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
