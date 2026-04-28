from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipeline.agent.execution_planner import (
    ExecutionPlan,
    Observation,
    build_execution_plan,
    build_observation,
    decide_loop_action,
)


def _make_observation(
    source_changed: bool = False,
    bronze_exists: bool = True,
    silver_exists: bool = True,
    gold_exists: bool = True,
    last_validation_status: str = "passed",
    run_count: int = 1,
) -> Observation:
    return Observation(
        source_changed=source_changed,
        layer_artifacts={
            "bronze": {"exists": bronze_exists, "age_hours": 1.0 if bronze_exists else None},
            "silver": {"exists": silver_exists, "age_hours": 1.0 if silver_exists else None},
            "gold": {"exists": gold_exists, "age_hours": 1.0 if gold_exists else None},
        },
        last_validation_summary={"status": last_validation_status},
        last_run_status="success" if run_count > 0 else None,
        run_count=run_count,
        generated_at_utc="2026-04-27T00:00:00+00:00",
    )


def _make_paths(tmp_path: Path) -> Any:
    from pipeline.config import build_paths

    return build_paths(root=tmp_path)


# --- build_observation tests ---


def test_build_observation_returns_observation(tmp_path: Path) -> None:
    from pipeline.config import build_paths

    paths = build_paths(root=tmp_path)
    state_path = paths.state / "pipeline_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state = {"runs": []}
    obs = build_observation(paths, state, source_changed=True)
    assert isinstance(obs, Observation)
    assert obs.source_changed is True
    assert "bronze" in obs.layer_artifacts
    assert obs.run_count == 0


def test_build_observation_artifact_exists(tmp_path: Path) -> None:
    from pipeline.config import build_paths

    paths = build_paths(root=tmp_path)
    paths.bronze.mkdir(parents=True, exist_ok=True)
    (paths.bronze / "conversations.parquet").write_bytes(b"fake")
    state = {"runs": [{"status": "success", "validation_summary": {"status": "passed"}}]}
    obs = build_observation(paths, state, source_changed=False)
    assert obs.layer_artifacts["bronze"]["exists"] is True
    assert obs.run_count == 1
    assert obs.last_run_status == "success"


# --- build_execution_plan: deterministic fallback tests ---


def test_plan_source_changed_runs_all_stages(tmp_path: Path) -> None:
    from pipeline.config import build_paths

    paths = build_paths(root=tmp_path)
    paths.monitoring.mkdir(parents=True, exist_ok=True)
    obs = _make_observation(source_changed=True)
    plan = build_execution_plan(obs, paths, llm_call=None)
    assert plan.source == "deterministic_fallback"
    assert "bronze" in plan.stages
    assert "silver" in plan.stages
    assert "gold" in plan.stages
    assert "validation" in plan.stages


def test_plan_unchanged_all_artifacts_empty_stages(tmp_path: Path) -> None:
    from pipeline.config import build_paths

    paths = build_paths(root=tmp_path)
    paths.monitoring.mkdir(parents=True, exist_ok=True)
    obs = _make_observation(source_changed=False, last_validation_status="passed")
    plan = build_execution_plan(obs, paths, llm_call=None)
    assert plan.source == "deterministic_fallback"
    assert plan.stages == []
    assert "unchanged" in plan.rationale.lower() or "present" in plan.rationale.lower()


def test_plan_missing_gold_includes_gold_and_validation(tmp_path: Path) -> None:
    from pipeline.config import build_paths

    paths = build_paths(root=tmp_path)
    paths.monitoring.mkdir(parents=True, exist_ok=True)
    obs = _make_observation(
        source_changed=False, bronze_exists=True, silver_exists=True, gold_exists=False
    )
    plan = build_execution_plan(obs, paths, llm_call=None)
    assert "gold" in plan.stages
    assert "validation" in plan.stages
    assert "bronze" not in plan.stages
    assert "silver" not in plan.stages


def test_plan_persisted_to_json(tmp_path: Path) -> None:
    from pipeline.config import build_paths

    paths = build_paths(root=tmp_path)
    paths.monitoring.mkdir(parents=True, exist_ok=True)
    obs = _make_observation(source_changed=True)
    build_execution_plan(obs, paths, llm_call=None)
    plan_file = paths.monitoring / "latest_execution_plan.json"
    assert plan_file.exists()
    data = json.loads(plan_file.read_text())
    assert "stages" in data
    assert "source" in data


# --- build_execution_plan: LLM path tests ---


def test_plan_llm_called_returns_llm_plan(tmp_path: Path) -> None:
    from pipeline.config import build_paths

    paths = build_paths(root=tmp_path)
    paths.monitoring.mkdir(parents=True, exist_ok=True)

    def mock_llm(prompt: str, compiled_plan: dict, timeout: float) -> str:
        return json.dumps(
            {
                "stages": ["silver", "gold", "validation"],
                "rationale": "Silver and downstream need rebuild.",
                "confidence": 0.85,
            }
        )

    obs = _make_observation(source_changed=False, silver_exists=False)
    plan = build_execution_plan(obs, paths, llm_call=mock_llm)
    assert plan.source == "llm"
    assert plan.confidence == 0.85
    assert "silver" in plan.stages


def test_plan_llm_timeout_falls_back_to_deterministic(tmp_path: Path) -> None:
    from pipeline.config import build_paths

    paths = build_paths(root=tmp_path)
    paths.monitoring.mkdir(parents=True, exist_ok=True)

    def mock_llm(prompt: str, compiled_plan: dict, timeout: float) -> str:
        raise TimeoutError("LLM timed out")

    obs = _make_observation(source_changed=True)
    plan = build_execution_plan(obs, paths, llm_call=mock_llm)
    assert plan.source == "deterministic_fallback"


def test_plan_llm_invalid_json_falls_back(tmp_path: Path) -> None:
    from pipeline.config import build_paths

    paths = build_paths(root=tmp_path)
    paths.monitoring.mkdir(parents=True, exist_ok=True)

    def mock_llm(prompt: str, compiled_plan: dict, timeout: float) -> str:
        return "not valid json at all"

    obs = _make_observation(source_changed=True)
    plan = build_execution_plan(obs, paths, llm_call=mock_llm)
    assert plan.source == "deterministic_fallback"


def test_plan_llm_filters_invalid_stage_names(tmp_path: Path) -> None:
    from pipeline.config import build_paths

    paths = build_paths(root=tmp_path)
    paths.monitoring.mkdir(parents=True, exist_ok=True)

    def mock_llm(prompt: str, compiled_plan: dict, timeout: float) -> str:
        return json.dumps(
            {
                "stages": ["bronze", "invalid_stage", "gold"],
                "rationale": "Test",
                "confidence": 0.7,
            }
        )

    obs = _make_observation(source_changed=True)
    plan = build_execution_plan(obs, paths, llm_call=mock_llm)
    assert plan.source == "llm"
    assert "invalid_stage" not in plan.stages
    assert "bronze" in plan.stages
    assert "gold" in plan.stages


# --- LoopAction / decide_loop_action tests ---


def test_decide_first_iteration_returns_run_stage() -> None:
    obs = _make_observation()
    plan = ExecutionPlan(
        stages=["bronze", "silver", "gold", "validation"],
        rationale="test",
        confidence=1.0,
        source="deterministic_fallback",
        generated_at_utc="2026-04-27T00:00:00+00:00",
    )
    action = decide_loop_action(
        execution_plan=plan,
        observation=obs,
        stage_failure_counts={},
        iteration=0,
        validation_passed=False,
    )
    assert action.kind == "run_stage"
    assert action.stage == "bronze"


def test_decide_validation_passed_returns_complete() -> None:
    obs = _make_observation()
    plan = ExecutionPlan(
        stages=[],
        rationale="nothing to do",
        confidence=1.0,
        source="deterministic_fallback",
        generated_at_utc="2026-04-27T00:00:00+00:00",
    )
    action = decide_loop_action(
        execution_plan=plan,
        observation=obs,
        stage_failure_counts={},
        iteration=1,
        validation_passed=True,
    )
    assert action.kind == "complete"


def test_decide_retry_stage_on_second_iteration() -> None:
    obs = _make_observation()
    plan = ExecutionPlan(
        stages=["silver", "gold", "validation"],
        rationale="silver missing",
        confidence=0.9,
        source="deterministic_fallback",
        generated_at_utc="2026-04-27T00:00:00+00:00",
    )
    action = decide_loop_action(
        execution_plan=plan,
        observation=obs,
        stage_failure_counts={"silver": 0},
        iteration=1,
        validation_passed=False,
        failed_stage="silver",
    )
    assert action.kind == "retry_stage"
    assert action.stage == "silver"


def test_decide_halt_on_repeated_stage_failure() -> None:
    obs = _make_observation()
    plan = ExecutionPlan(
        stages=["silver"],
        rationale="test",
        confidence=0.9,
        source="deterministic_fallback",
        generated_at_utc="2026-04-27T00:00:00+00:00",
    )
    action = decide_loop_action(
        execution_plan=plan,
        observation=obs,
        stage_failure_counts={"silver": 2},
        iteration=2,
        validation_passed=False,
        failed_stage="silver",
    )
    assert action.kind == "halt"
    assert "repeated_stage_failure" in action.reason


def test_decide_empty_plan_returns_complete() -> None:
    obs = _make_observation()
    plan = ExecutionPlan(
        stages=[],
        rationale="nothing needed",
        confidence=1.0,
        source="deterministic_fallback",
        generated_at_utc="2026-04-27T00:00:00+00:00",
    )
    action = decide_loop_action(
        execution_plan=plan,
        observation=obs,
        stage_failure_counts={},
        iteration=0,
        validation_passed=False,
    )
    assert action.kind == "complete"


# --- ExecutionPlan dataclass tests ---


def test_execution_plan_as_dict() -> None:
    plan = ExecutionPlan(
        stages=["bronze", "silver"],
        rationale="Test rationale",
        confidence=0.9,
        source="llm",
        generated_at_utc="2026-04-27T00:00:00+00:00",
    )
    d = plan.as_dict()
    assert d["stages"] == ["bronze", "silver"]
    assert d["source"] == "llm"
    assert d["confidence"] == 0.9


def test_observation_as_dict() -> None:
    obs = _make_observation(source_changed=True)
    d = obs.as_dict()
    assert d["source_changed"] is True
    assert "layer_artifacts" in d
    assert "bronze" in d["layer_artifacts"]
