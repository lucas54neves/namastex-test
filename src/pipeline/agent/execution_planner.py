from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pipeline.config import PipelinePaths
from pipeline.io.parquet_io import write_json
from pipeline.runtime.terminal_logging import log_event

VALID_STAGES = ["bronze", "silver", "gold", "validation", "planning"]

_EXECUTION_PLAN_PROMPT = """
You are a data pipeline orchestration agent. Given the current pipeline state, decide
which stages need to run.

## Current Observation
{observation_json}

## Available Stages
bronze, silver, gold, validation, planning

## Rules
- If source fingerprint is unchanged AND all layer artifacts exist
  AND last validation passed: return empty stages list.
- If an artifact is missing: include that stage and all downstream stages.
- If last validation failed for a layer: include that layer and all downstream stages.
- If source fingerprint changed: include all stages.

## Output Format (JSON only, no prose)
{{"stages": [...], "rationale": "...", "confidence": 0.0}}
"""


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class Observation:
    source_changed: bool
    layer_artifacts: dict[str, dict]
    last_validation_summary: dict[str, Any]
    last_run_status: str | None
    run_count: int
    generated_at_utc: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_changed": self.source_changed,
            "layer_artifacts": self.layer_artifacts,
            "last_validation_summary": self.last_validation_summary,
            "last_run_status": self.last_run_status,
            "run_count": self.run_count,
            "generated_at_utc": self.generated_at_utc,
        }


@dataclass(frozen=True)
class ExecutionPlan:
    stages: list[str]
    rationale: str
    confidence: float
    source: Literal["llm", "deterministic_fallback"]
    generated_at_utc: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "stages": list(self.stages),
            "rationale": self.rationale,
            "confidence": self.confidence,
            "source": self.source,
            "generated_at_utc": self.generated_at_utc,
        }


@dataclass(frozen=True)
class LoopAction:
    kind: Literal["run_stage", "retry_stage", "halt", "complete"]
    stage: str | None
    reason: str


def _artifact_info(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"exists": False, "age_hours": None}
    age_hours = (datetime.now(UTC).timestamp() - path.stat().st_mtime) / 3600.0
    return {"exists": True, "age_hours": round(age_hours, 2)}


def build_observation(
    paths: PipelinePaths,
    state: dict[str, Any],
    source_changed: bool,
) -> Observation:
    layer_artifacts = {
        "bronze": _artifact_info(paths.bronze / "conversations.parquet"),
        "silver": _artifact_info(paths.silver / "silver_leads.parquet"),
        "gold": _artifact_info(paths.gold / "conversations_gold.parquet"),
    }
    runs = state.get("runs", [])
    last_run = runs[-1] if runs else {}
    last_validation = last_run.get("validation_summary", {})
    return Observation(
        source_changed=source_changed,
        layer_artifacts=layer_artifacts,
        last_validation_summary=last_validation,
        last_run_status=last_run.get("status"),
        run_count=len(runs),
        generated_at_utc=_utc_now(),
    )


def _deterministic_plan(observation: Observation) -> ExecutionPlan:
    if observation.source_changed:
        stages = list(VALID_STAGES)
        rationale = "Source fingerprint changed — rebuilding all stages."
    else:
        stages = []
        downstream = False
        for stage in ("bronze", "silver", "gold"):
            artifact = observation.layer_artifacts.get(stage, {})
            if downstream or not artifact.get("exists"):
                stages.append(stage)
                downstream = True
        if stages:
            stages.append("validation")
        last_status = observation.last_validation_summary.get("status")
        if last_status == "failed" and not stages:
            stages = ["silver", "gold", "validation"]
        if stages:
            rationale = f"Missing or stale artifacts detected; stages required: {stages}."
        else:
            rationale = (
                "All artifacts present, source fingerprint unchanged, last validation passed."
            )
    return ExecutionPlan(
        stages=stages,
        rationale=rationale,
        confidence=1.0,
        source="deterministic_fallback",
        generated_at_utc=_utc_now(),
    )


def _parse_llm_plan(text: str, observation: Observation) -> ExecutionPlan:
    raw = text.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        if raw.startswith("json"):
            raw = raw[4:]
    parsed = json.loads(raw.strip())
    stages = [str(s) for s in parsed.get("stages", []) if str(s) in VALID_STAGES]
    return ExecutionPlan(
        stages=stages,
        rationale=str(parsed.get("rationale", "")),
        confidence=float(parsed.get("confidence", 0.5)),
        source="llm",
        generated_at_utc=_utc_now(),
    )


def build_execution_plan(
    observation: Observation,
    paths: PipelinePaths,
    llm_call: Callable[..., str] | None = None,
    compiled_plan: dict[str, Any] | None = None,
    timeout: float = 15.0,
) -> ExecutionPlan:
    plan_path = paths.monitoring / "latest_execution_plan.json"

    if llm_call is not None:
        try:
            prompt = _EXECUTION_PLAN_PROMPT.format(
                observation_json=json.dumps(observation.as_dict(), indent=2),
            )
            text = llm_call(prompt, compiled_plan or {}, timeout)
            plan = _parse_llm_plan(text, observation)
            log_event(
                logging.INFO,
                "execution_plan_built",
                source="llm",
                stages=plan.stages,
                confidence=plan.confidence,
            )
        except Exception as exc:
            log_event(logging.WARNING, "execution_plan_llm_failed", error=str(exc))
            plan = _deterministic_plan(observation)
    else:
        plan = _deterministic_plan(observation)

    write_json(plan.as_dict(), plan_path)
    return plan


def decide_loop_action(
    execution_plan: ExecutionPlan,
    observation: Observation,
    stage_failure_counts: dict[str, int],
    iteration: int,
    validation_passed: bool,
    failed_stage: str | None = None,
) -> LoopAction:
    if validation_passed:
        return LoopAction(kind="complete", stage=None, reason="all_stages_passed_validation")

    if iteration == 0:
        if not execution_plan.stages:
            return LoopAction(kind="complete", stage=None, reason="no_stages_in_plan")
        first = execution_plan.stages[0]
        return LoopAction(kind="run_stage", stage=first, reason="executing_execution_plan")

    if failed_stage is None:
        return LoopAction(kind="complete", stage=None, reason="no_failed_stage_to_retry")

    count = stage_failure_counts.get(failed_stage, 0)
    if count >= 2:
        return LoopAction(
            kind="halt",
            stage=failed_stage,
            reason=f"repeated_stage_failure:{failed_stage}",
        )

    return LoopAction(
        kind="retry_stage",
        stage=failed_stage,
        reason=f"retrying_{failed_stage}_after_validation_failure",
    )
