from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch

from pipeline.orchestration.operator import _EMPTY_PLANNER_REPORT, run_cycle
from pipeline.orchestration.operator_reports import _planner_report_summary

_OPERATOR_PREFIX = "pipeline.orchestration.operator"
_DUMMY_REPORT = {"proposals": [], "applied": False}


class _MockCDCState:
    digest = "abc123"
    known_ids: frozenset = frozenset()
    row_count = 0

    def as_dict(self) -> dict:
        return {"digest": self.digest, "known_ids": [], "row_count": 0}


def _mock_execution_plan(stages: list | None = None):
    from pipeline.agent.execution_planner import ExecutionPlan

    return ExecutionPlan(
        stages=stages or [],
        rationale="test",
        confidence=1.0,
        source="test",
        generated_at_utc="2026-01-01T00:00:00+00:00",
    )


def _run_cycle_with_gate(
    tmp_path: Path,
    *,
    changed: bool,
    force: bool = False,
    idle_cycle_count: int = 0,
    planner_cadence: int = 0,
) -> None:
    from pipeline.config import build_paths

    paths = build_paths(tmp_path)

    with (
        patch(f"{_OPERATOR_PREFIX}.ensure_directories"),
        patch(f"{_OPERATOR_PREFIX}.ensure_pipeline_spec", return_value={}),
        patch(f"{_OPERATOR_PREFIX}.compile_pipeline_spec", return_value={}),
        patch(
            f"{_OPERATOR_PREFIX}.build_cdc_state",
            return_value=_MockCDCState(),
        ),
        patch(f"{_OPERATOR_PREFIX}.load_pipeline_state", return_value={}),
        patch(f"{_OPERATOR_PREFIX}.has_source_changed_cdc", return_value=changed),
        patch(f"{_OPERATOR_PREFIX}.plan_pipeline_spec", return_value=_DUMMY_REPORT) as mock_plan,
        patch(
            f"{_OPERATOR_PREFIX}.build_observation",
            return_value=MagicMock(),
        ),
        patch(
            f"{_OPERATOR_PREFIX}.build_execution_plan",
            return_value=_mock_execution_plan(),
        ),
        patch(f"{_OPERATOR_PREFIX}.save_pipeline_state"),
        patch(f"{_OPERATOR_PREFIX}._write_reports"),
        patch(f"{_OPERATOR_PREFIX}._build_alert_report", return_value={}),
        patch(f"{_OPERATOR_PREFIX}.log_event"),
    ):
        run_cycle(
            paths,
            force=force,
            idle_cycle_count=idle_cycle_count,
            planner_cadence=planner_cadence,
        )
        return mock_plan


def test_planner_not_called_on_idle_cycle(tmp_path: Path) -> None:
    mock_plan = _run_cycle_with_gate(
        tmp_path, changed=False, force=False, planner_cadence=0, idle_cycle_count=0
    )
    mock_plan.assert_not_called()


def test_planner_called_on_source_change(tmp_path: Path) -> None:
    mock_plan = _run_cycle_with_gate(tmp_path, changed=True, force=False, planner_cadence=0)
    mock_plan.assert_called_once()


def test_planner_called_on_force(tmp_path: Path) -> None:
    mock_plan = _run_cycle_with_gate(tmp_path, changed=False, force=True, planner_cadence=0)
    mock_plan.assert_called_once()


def test_cadence_triggers_on_boundary(tmp_path: Path) -> None:
    mock_plan = _run_cycle_with_gate(tmp_path, changed=False, planner_cadence=5, idle_cycle_count=5)
    mock_plan.assert_called_once()


def test_cadence_does_not_trigger_below_boundary(tmp_path: Path) -> None:
    mock_plan = _run_cycle_with_gate(tmp_path, changed=False, planner_cadence=5, idle_cycle_count=4)
    mock_plan.assert_not_called()


def test_cadence_zero_disables_secondary_gate(tmp_path: Path) -> None:
    mock_plan = _run_cycle_with_gate(
        tmp_path, changed=False, planner_cadence=0, idle_cycle_count=100
    )
    mock_plan.assert_not_called()


def test_empty_planner_report_no_key_error(tmp_path: Path) -> None:
    from pipeline.config import build_paths

    paths = build_paths(tmp_path)
    summary = _planner_report_summary(paths, _EMPTY_PLANNER_REPORT)

    assert summary["proposal_count"] == 0
    assert summary["applied"] is False
    assert summary["promoted_proposal_ids"] == []
    assert summary["skipped"] is True


def _load_daemon_module():
    root = Path(__file__).resolve().parents[1]
    module_path = root / "scripts" / "run_pipeline_daemon.py"
    spec = importlib.util.spec_from_file_location("run_pipeline_daemon", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _SkippedArtifacts:
    bronze_path = "b"
    silver_path = "s"
    gold_path = "g"
    state_path = "st"
    validation_report_path = "v"
    agent_report_path = "a"
    alert_report_path = "al"
    executed = False
    status = "skipped_no_source_change"


class _ActiveArtifacts:
    bronze_path = "b"
    silver_path = "s"
    gold_path = "g"
    state_path = "st"
    validation_report_path = "v"
    agent_report_path = "a"
    alert_report_path = "al"
    executed = True
    status = "success"


def test_idle_counter_increments_on_skip(monkeypatch) -> None:
    module = _load_daemon_module()
    call_kwargs: list[dict] = []

    def fake_run_pipeline(_paths, force=False, idle_cycle_count=0, **kwargs):
        call_kwargs.append({"force": force, "idle_cycle_count": idle_cycle_count})
        return _SkippedArtifacts()

    monkeypatch.setattr(module, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(module, "build_paths", lambda root: root)
    monkeypatch.setattr(module, "artifacts_as_dict", lambda a: {})
    monkeypatch.setattr(module.time, "sleep", lambda _: None)
    monkeypatch.setattr(module, "shutdown_langfuse_client", lambda: None)
    monkeypatch.setattr(module, "configure_terminal_logging", lambda: None)
    monkeypatch.setattr(module, "log_event", lambda *a, **kw: None)

    module.run_daemon(
        argparse.Namespace(
            force_first_run=False,
            poll_interval_seconds=1,
            max_cycles=3,
            max_backoff_seconds=300,
            planner_cadence_cycles=0,
        )
    )

    assert call_kwargs[0]["idle_cycle_count"] == 0
    assert call_kwargs[1]["idle_cycle_count"] == 1
    assert call_kwargs[2]["idle_cycle_count"] == 2


def test_idle_counter_resets_after_active_cycle(monkeypatch) -> None:
    module = _load_daemon_module()
    call_kwargs: list[dict] = []
    cycle = [0]

    def fake_run_pipeline(_paths, force=False, idle_cycle_count=0, **kwargs):
        call_kwargs.append({"force": force, "idle_cycle_count": idle_cycle_count})
        cycle[0] += 1
        if cycle[0] <= 2:
            return _SkippedArtifacts()
        return _ActiveArtifacts()

    monkeypatch.setattr(module, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(module, "build_paths", lambda root: root)
    monkeypatch.setattr(module, "artifacts_as_dict", lambda a: {})
    monkeypatch.setattr(module.time, "sleep", lambda _: None)
    monkeypatch.setattr(module, "shutdown_langfuse_client", lambda: None)
    monkeypatch.setattr(module, "configure_terminal_logging", lambda: None)
    monkeypatch.setattr(module, "log_event", lambda *a, **kw: None)

    module.run_daemon(
        argparse.Namespace(
            force_first_run=False,
            poll_interval_seconds=1,
            max_cycles=4,
            max_backoff_seconds=300,
            planner_cadence_cycles=0,
        )
    )

    assert call_kwargs[0]["idle_cycle_count"] == 0
    assert call_kwargs[1]["idle_cycle_count"] == 1
    assert call_kwargs[2]["idle_cycle_count"] == 2
    assert call_kwargs[3]["idle_cycle_count"] == 0
