from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path

import pytest


def _load_daemon_module():
    root = Path(__file__).resolve().parents[1]
    module_path = root / "scripts" / "run_pipeline_daemon.py"
    spec = importlib.util.spec_from_file_location("run_pipeline_daemon", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeArtifacts:
    bronze_path = "b"
    silver_path = "s"
    gold_path = "g"
    state_path = "st"
    validation_report_path = "v"
    agent_report_path = "a"
    alert_report_path = "al"
    executed = True
    status = "success"


def _make_args(**kwargs):
    defaults = dict(
        force_first_run=False,
        poll_interval_seconds=1,
        max_cycles=0,
        max_backoff_seconds=300,
        planner_cadence_cycles=0,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _setup(module, monkeypatch, side_effects):
    """side_effects: list where None means success and an Exception instance means failure."""
    calls = iter(side_effects)

    def fake_run_pipeline(_paths, force=False, **kwargs):
        result = next(calls)
        if result is not None:
            raise result
        return _FakeArtifacts()

    sleep_calls: list[float] = []
    log_calls: list[tuple[str, dict]] = []
    shutdown_calls: list[str] = []

    monkeypatch.setattr(module, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(module, "build_paths", lambda root: root)
    monkeypatch.setattr(module, "artifacts_as_dict", lambda a: {})
    monkeypatch.setattr(module.time, "sleep", lambda s: sleep_calls.append(s))
    monkeypatch.setattr(
        module, "log_event", lambda level, event, **kw: log_calls.append((event, kw))
    )
    monkeypatch.setattr(module, "shutdown_langfuse_client", lambda: shutdown_calls.append("done"))
    monkeypatch.setattr(module, "configure_terminal_logging", lambda: None)

    return sleep_calls, log_calls, shutdown_calls


def test_backoff_resets_on_success(monkeypatch) -> None:
    # fail → success → fail: both failures must have attempt=0 and backoff=1
    module = _load_daemon_module()
    side_effects = [RuntimeError("e1"), None, RuntimeError("e2")]
    sleep_calls, log_calls, _ = _setup(module, monkeypatch, side_effects)

    module.run_daemon(_make_args(max_cycles=3))

    failed = [kw for event, kw in log_calls if event == "daemon_cycle_failed"]
    assert len(failed) == 2
    assert failed[0]["attempt"] == 0
    assert failed[0]["backoff_seconds"] == 1
    assert failed[1]["attempt"] == 0
    assert failed[1]["backoff_seconds"] == 1


def test_backoff_cap_applied(monkeypatch) -> None:
    # 5 consecutive failures with cap=10; 5th failure has attempt=4 → min(16, 10)=10
    module = _load_daemon_module()
    side_effects = [RuntimeError("e")] * 5
    sleep_calls, log_calls, _ = _setup(module, monkeypatch, side_effects)

    module.run_daemon(_make_args(max_cycles=5, max_backoff_seconds=10))

    failed = [kw for event, kw in log_calls if event == "daemon_cycle_failed"]
    assert len(failed) == 5
    assert failed[4]["attempt"] == 4
    assert failed[4]["backoff_seconds"] == 10


def test_keyboard_interrupt_not_swallowed(monkeypatch) -> None:
    module = _load_daemon_module()
    side_effects = [KeyboardInterrupt()]
    sleep_calls, log_calls, shutdown_calls = _setup(module, monkeypatch, side_effects)

    with pytest.raises(KeyboardInterrupt):
        module.run_daemon(_make_args())

    assert shutdown_calls == ["done"]


def test_max_cycles_exits_on_failure(monkeypatch) -> None:
    # max_cycles=2, both cycles fail: cycle 1 sleeps backoff, cycle 2 exits without backoff sleep
    module = _load_daemon_module()
    side_effects = [RuntimeError("e"), RuntimeError("e")]
    sleep_calls, log_calls, _ = _setup(module, monkeypatch, side_effects)

    module.run_daemon(_make_args(max_cycles=2))

    assert sleep_calls == [1]  # only cycle 1 backoff; cycle 2 exits at max_cycles check

    stopped = [kw for event, kw in log_calls if event == "daemon_stopped"]
    assert len(stopped) == 1
    assert stopped[0]["reason"] == "max_cycles_reached"
    assert stopped[0]["cycle"] == 2


def test_successful_cycle_no_backoff(monkeypatch) -> None:
    # 2 successes with poll_interval=30: only poll sleep is called, never backoff
    module = _load_daemon_module()
    side_effects = [None, None]
    sleep_calls, log_calls, _ = _setup(module, monkeypatch, side_effects)

    module.run_daemon(_make_args(poll_interval_seconds=30, max_cycles=2))

    assert sleep_calls == [30]  # cycle 1 poll sleep; cycle 2 exits at max_cycles without sleep
    assert not any(event == "daemon_cycle_failed" for event, _ in log_calls)
    assert not any(event == "daemon_backoff" for event, _ in log_calls)


def test_daemon_cycle_failed_event_fields(monkeypatch) -> None:
    # Single failure with max_cycles=1: daemon_cycle_failed must have correct fields
    module = _load_daemon_module()
    side_effects = [RuntimeError("something went wrong")]
    sleep_calls, log_calls, _ = _setup(module, monkeypatch, side_effects)

    module.run_daemon(_make_args(max_cycles=1))

    failed = [(event, kw) for event, kw in log_calls if event == "daemon_cycle_failed"]
    assert len(failed) == 1
    _, kw = failed[0]
    assert kw["error_type"] == "RuntimeError"
    assert kw["error_message"] == "something went wrong"
    assert kw["backoff_seconds"] == 1
    assert kw["attempt"] == 0
    assert kw["cycle"] == 1
