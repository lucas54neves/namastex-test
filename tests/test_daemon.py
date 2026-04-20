from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path


def _load_daemon_module():
    root = Path(__file__).resolve().parents[1]
    module_path = root / "scripts" / "run_pipeline_daemon.py"
    spec = importlib.util.spec_from_file_location("run_pipeline_daemon", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_run_daemon_respects_max_cycles(monkeypatch, capsys) -> None:
    module = _load_daemon_module()
    calls: list[bool] = []

    class DummyArtifacts:
        bronze_path = "bronze"
        silver_path = "silver"
        gold_path = "gold"
        state_path = "state"
        validation_report_path = "validation"
        agent_report_path = "agent"
        alert_report_path = "alert"
        executed = True
        status = "success"

    def fake_run_pipeline(_paths, force: bool = False):
        calls.append(force)
        return DummyArtifacts()

    monkeypatch.setattr(module, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(module, "build_paths", lambda root: root)
    monkeypatch.setattr(
        module,
        "artifacts_as_dict",
        lambda artifacts: {
            "bronze_path": artifacts.bronze_path,
            "silver_path": artifacts.silver_path,
            "gold_path": artifacts.gold_path,
            "state_path": artifacts.state_path,
            "validation_report_path": artifacts.validation_report_path,
            "agent_report_path": artifacts.agent_report_path,
            "alert_report_path": artifacts.alert_report_path,
            "executed": artifacts.executed,
            "status": artifacts.status,
        },
    )
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)

    module.run_daemon(
        argparse.Namespace(force_first_run=True, poll_interval_seconds=1, max_cycles=2)
    )

    assert calls == [True, False]
    captured = capsys.readouterr()
    assert '"cycle": 1' in captured.out
    assert '"cycle": 2' in captured.out
