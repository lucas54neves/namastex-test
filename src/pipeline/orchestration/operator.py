from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from pipeline.agent.agent import (
    attempt_auto_remediation,
    diagnose_exception,
    diagnose_validation_failures,
)
from pipeline.agent.alerts import handle_alerting
from pipeline.agent.execution_planner import (
    LoopAction,
    build_execution_plan,
    build_observation,
    decide_loop_action,
)
from pipeline.agent.gold_designer import design_gold_columns
from pipeline.agent.planner import plan_pipeline_spec
from pipeline.config import PipelinePaths, ensure_directories
from pipeline.io.parquet_io import read_json, read_parquet, write_json, write_parquet
from pipeline.orchestration.compiler import compile_pipeline_spec
from pipeline.quality.publication import sanitize_for_publication
from pipeline.quality.quality import (
    ValidationResult,
    summarize_validation_results,
    validate_bronze,
    validate_cross_layer_consistency,
    validate_gold,
    validate_silver,
    validate_silver_conversations_llm,
    validate_silver_messages,
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
from pipeline.transforms.silver import build_silver, build_silver_leads

_MAX_REACT_ITERATIONS = 3


@dataclass(frozen=True)
class PipelineArtifacts:
    bronze_path: str
    silver_path: str
    silver_messages_path: str
    silver_conversations_llm_path: str
    gold_path: str
    state_path: str
    validation_report_path: str
    agent_report_path: str
    alert_report_path: str
    executed: bool
    status: str


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def state_file(paths: PipelinePaths) -> Path:
    return paths.state / "pipeline_state.json"


def validation_report_file(paths: PipelinePaths) -> Path:
    return paths.monitoring / "latest_run_report.json"


def agent_report_file(paths: PipelinePaths) -> Path:
    return paths.monitoring / "latest_agent_report.json"


def alert_report_file(paths: PipelinePaths) -> Path:
    return paths.monitoring / "latest_alert_report.json"


def plan_report_file(paths: PipelinePaths) -> Path:
    return paths.monitoring / "latest_plan_report.json"


def _planner_report_summary(paths: PipelinePaths, planner_report: dict[str, Any]) -> dict[str, Any]:
    return {
        "report_path": str(plan_report_file(paths)),
        "proposal_id": planner_report.get("proposal_id"),
        "proposal_count": len(planner_report.get("proposals", [])),
        "requires_approval": planner_report.get("requires_approval", False),
        "approved": planner_report.get("approved", False),
        "applied": planner_report.get("applied", False),
        "promoted_proposal_ids": planner_report.get("promoted_proposal_ids", []),
        "applied_proposal_ids": planner_report.get("applied_proposal_ids", []),
        "applied_proposal_types": planner_report.get("applied_proposal_types", []),
        "autonomy_policy_path": planner_report.get("autonomy_policy_path"),
        "autonomy_metrics_path": planner_report.get("autonomy_metrics_path"),
    }


def _incident_id() -> str:
    return f"incident_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}"


def _run_record(
    source_fingerprint: dict[str, Any],
    executed: bool,
    status: str,
    validation_summary: dict[str, Any] | None = None,
    details: dict[str, Any] | None = None,
    agent_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "run_at_utc": _utc_now_iso(),
        "executed": executed,
        "status": status,
        "source_fingerprint": source_fingerprint,
        "validation_summary": validation_summary or {},
        "agent_summary": agent_summary or {},
        "details": details or {},
    }


def _skip_artifacts(paths: PipelinePaths) -> PipelineArtifacts:
    return PipelineArtifacts(
        bronze_path=str(paths.bronze / "conversations.parquet"),
        silver_path=str(paths.silver / "silver_leads.parquet"),
        silver_messages_path=str(paths.silver / "silver_messages.parquet"),
        silver_conversations_llm_path=str(paths.silver / "silver_conversations_llm.parquet"),
        gold_path=str(paths.gold / "conversations_gold.parquet"),
        state_path=str(state_file(paths)),
        validation_report_path=str(validation_report_file(paths)),
        agent_report_path=str(agent_report_file(paths)),
        alert_report_path=str(alert_report_file(paths)),
        executed=False,
        status="skipped_no_source_change",
    )


def _build_agent_report(
    incident_id: str,
    status: str,
    diagnoses: list[dict[str, Any]],
    auto_remediation: dict[str, Any] | None = None,
    fallback: dict[str, Any] | None = None,
    exception: str | None = None,
    decisions: list[dict[str, Any]] | None = None,
    planner_report: dict[str, Any] | None = None,
    quarantine_report: dict[str, Any] | None = None,
    llm_diagnoses: list[dict[str, Any]] | None = None,
    execution_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "generated_at_utc": _utc_now_iso(),
        "incident_id": incident_id,
        "status": status,
        "diagnoses": diagnoses,
        "auto_remediation": auto_remediation or {},
        "fallback": fallback or {},
        "decisions": decisions or [],
        "planner_report": planner_report or {},
        "quarantine_report": quarantine_report or {},
        "exception": exception,
        "llm_diagnoses": llm_diagnoses or [],
        "execution_plan": execution_plan or {},
    }


def _write_reports(
    validation_summary: dict[str, Any],
    agent_report: dict[str, Any],
    alert_report: dict[str, Any],
    validation_report_path: Path,
    agent_report_path: Path,
    alert_report_path: Path,
    decisions_dir: Path,
) -> None:
    write_json(validation_summary, validation_report_path)
    write_json(agent_report, agent_report_path)
    write_json(alert_report, alert_report_path)
    decision_path = decisions_dir / "latest_agent_decision.json"
    write_json(
        {
            "generated_at_utc": agent_report["generated_at_utc"],
            "status": agent_report["status"],
            "decisions": agent_report.get("decisions", []),
        },
        decision_path,
    )


def _success_artifacts(paths: PipelinePaths, status: str) -> PipelineArtifacts:
    return PipelineArtifacts(
        bronze_path=str(paths.bronze / "conversations.parquet"),
        silver_path=str(paths.silver / "silver_leads.parquet"),
        silver_messages_path=str(paths.silver / "silver_messages.parquet"),
        silver_conversations_llm_path=str(paths.silver / "silver_conversations_llm.parquet"),
        gold_path=str(paths.gold / "conversations_gold.parquet"),
        state_path=str(state_file(paths)),
        validation_report_path=str(validation_report_file(paths)),
        agent_report_path=str(agent_report_file(paths)),
        alert_report_path=str(alert_report_file(paths)),
        executed=True,
        status=status,
    )


def _build_alert_report(
    paths: PipelinePaths,
    run_record: dict[str, Any],
    agent_report: dict[str, Any],
    validation_summary: dict[str, Any],
) -> dict[str, Any]:
    return handle_alerting(
        alerts_dir=paths.alerts,
        run_record=run_record,
        agent_report=agent_report,
        validation_report=validation_summary,
    )


def _extract_llm_diagnoses(agent_diagnoses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for d in agent_diagnoses:
        source = d.get("source", {})
        if isinstance(source, dict) and "llm_diagnosis" in source:
            result.append(source["llm_diagnosis"])
    return result


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


def _run_validation_suite(
    bronze_df: Any,
    silver_df: Any,
    silver_messages_df: Any,
    silver_conversations_llm_df: Any,
    gold_df: Any,
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
    )


def run_cycle(paths: PipelinePaths, force: bool = False) -> PipelineArtifacts:  # noqa: C901
    ensure_directories(paths)
    spec = ensure_pipeline_spec(paths.pipeline_spec)
    compiled_plan = compile_pipeline_spec(spec)
    planner_report = plan_pipeline_spec(paths)
    planner_summary = _planner_report_summary(paths, planner_report)

    state_path = state_file(paths)
    report_path = validation_report_file(paths)
    agent_report_path = agent_report_file(paths)
    alert_report_path = alert_report_file(paths)
    current_fingerprint_obj = build_source_fingerprint(paths.raw_bronze_source)
    current_fingerprint = current_fingerprint_obj.as_dict()
    state = load_pipeline_state(state_path)
    previous_fingerprint = state.get("last_source_fingerprint")
    changed = has_source_changed(current_fingerprint_obj, previous_fingerprint)

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
        # --- Bronze ---
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

        # --- Silver ---
        current_stage = "silver_build"
        silver_messages_runtime_df = build_silver(bronze_df, compiled_plan=compiled_plan)
        silver_runtime_df = build_silver_leads(silver_messages_runtime_df)
        silver_df = sanitize_for_publication(silver_runtime_df, "silver")
        silver_messages_df = sanitize_for_publication(silver_messages_runtime_df, "silver_messages")
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

        # Decision 2: Dynamic Gold Designer
        current_stage = "gold_design"
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

        # --- Gold ---
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

        # --- ReAct loop: validation + remediation ---
        stage_failure_counts: dict[str, int] = {}
        all_diagnoses: list[dict[str, Any]] = []
        all_llm_diagnoses: list[dict[str, Any]] = []
        remediation: dict[str, Any] = {
            "classification": "not_applicable",
            "attempted": False,
            "applied": False,
            "actions": [],
            "resolved": False,
        }
        decisions: list[dict[str, Any]] = []
        agent_status = "healthy"
        run_status = "success"
        final_loop_action: LoopAction | None = None
        validation_passed = False

        for iteration in range(_MAX_REACT_ITERATIONS):
            current_stage = "validation"
            current_obs = build_observation(paths, state, changed)

            loop_action = decide_loop_action(
                execution_plan=execution_plan,
                observation=current_obs,
                stage_failure_counts=stage_failure_counts,
                iteration=iteration,
                validation_passed=validation_passed,
                failed_stage=None,
            )

            if loop_action.kind == "complete":
                final_loop_action = loop_action
                break

            validation_results = _run_validation_suite(
                bronze_df,
                silver_df,
                silver_messages_df,
                silver_conversations_llm_df,
                gold_df,
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

            # Check for critical diagnosis (REQ-025)
            critical_found = any(d.get("severity") == "critical" for d in iteration_diagnoses)
            if critical_found:
                log_event(logging.ERROR, "critical_diagnosis_halt", iteration=iteration)
                final_loop_action = LoopAction(kind="halt", stage=None, reason="critical_diagnosis")
                run_status = "halted_critical_failure"
                agent_status = "halted_critical_failure"
                break

            # Attempt auto-remediation
            rem = attempt_auto_remediation(
                bronze_df=bronze_df,
                silver_df=silver_df,
                silver_messages_df=silver_messages_df,
                gold_df=gold_df,
                failed_checks=failed_checks,
                compiled_plan=compiled_plan,
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
                }
                validation_passed = True
                agent_status = "auto_remediated"
                run_status = "success_after_auto_remediation"
                final_loop_action = LoopAction(
                    kind="complete", stage=None, reason="auto_remediation_resolved"
                )
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

            # Rebuild the failing stage for next iteration
            log_event(
                logging.INFO,
                "react_retry_stage",
                stage=retry_stage,
                iteration=iteration,
                failure_count=stage_failure_counts[retry_stage],
            )
            if retry_stage in ("bronze", "silver"):
                silver_messages_runtime_df = build_silver(bronze_df, compiled_plan=compiled_plan)
                silver_runtime_df = build_silver_leads(silver_messages_runtime_df)
                silver_df = sanitize_for_publication(silver_runtime_df, "silver")
                silver_messages_df = sanitize_for_publication(
                    silver_messages_runtime_df, "silver_messages"
                )
                write_parquet(silver_df, silver_path)
                write_parquet(silver_messages_df, silver_messages_path)

            if retry_stage in ("bronze", "silver", "gold"):
                gold_runtime_df = build_gold(
                    silver_runtime_df,
                    silver_messages_runtime_df,
                    silver_conversations_llm_runtime_df,
                    compiled_plan=compiled_plan,
                    gold_column_plan=gold_column_plan,
                )
                gold_df = sanitize_for_publication(gold_runtime_df, "gold")
                write_parquet(gold_df, gold_path)

            remediation = {
                "classification": "unresolved_manual_action"
                if any(bool(d.get("auto_remediable")) for d in iteration_diagnoses)
                else "not_auto_remediable",
                "attempted": True,
                "applied": bool(rem["actions"]),
                "actions": rem["actions"],
                "resolved": False,
            }

        else:
            # Max iterations reached without break
            final_loop_action = LoopAction(kind="halt", stage=None, reason="max_iterations_reached")

        # Final validation pass if not already completed
        if not validation_passed and run_status not in (
            "halted_critical_failure",
            "success_after_auto_remediation",
        ):
            validation_results = _run_validation_suite(
                bronze_df,
                silver_df,
                silver_messages_df,
                silver_conversations_llm_df,
                gold_df,
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
            }
            validation_summary["source_fingerprint"] = current_fingerprint
            validation_summary["pipeline_spec_path"] = str(paths.pipeline_spec)
            failed_checks = cast(list[dict[str, Any]], validation_summary["failed_checks"])
            row_counts = cast(dict[str, Any], validation_summary["row_counts"])

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
            quarantine_report=cast(dict[str, Any], quarantine["report"]),
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
