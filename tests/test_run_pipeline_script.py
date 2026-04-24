from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path


def _load_run_pipeline_module():
    root = Path(__file__).resolve().parents[1]
    module_path = root / "scripts" / "run_pipeline.py"
    spec = importlib.util.spec_from_file_location("run_pipeline", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_run_pipeline_main_shuts_down_langfuse(monkeypatch, capsys) -> None:
    module = _load_run_pipeline_module()
    shutdown_calls: list[str] = []

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

    monkeypatch.setattr(
        module,
        "parse_args",
        lambda: argparse.Namespace(
            force=False,
            input_file=None,
            data_dir=None,
            reports_dir=None,
            state_dir=None,
            runtime_dir=None,
            config_dir=None,
        ),
    )
    monkeypatch.setattr(module, "build_paths", lambda root, env=None: root)
    monkeypatch.setattr(module, "run_pipeline", lambda _paths, force=False: DummyArtifacts())
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
    monkeypatch.setattr(module, "shutdown_langfuse_client", lambda: shutdown_calls.append("done"))

    module.main()

    captured = capsys.readouterr()
    assert '"status": "success"' in captured.out
    assert shutdown_calls == ["done"]


def test_runtime_path_overrides_collects_only_explicit_values() -> None:
    module = _load_run_pipeline_module()

    overrides = module._runtime_path_overrides(
        argparse.Namespace(
            input_file="/volumes/input.parquet",
            data_dir="/volumes/data",
            reports_dir=None,
            state_dir="/volumes/state",
            runtime_dir=None,
            config_dir="/workspace/config",
            force=False,
        )
    )

    assert overrides == {
        "PIPELINE_INPUT_FILE": "/volumes/input.parquet",
        "PIPELINE_DATA_DIR": "/volumes/data",
        "PIPELINE_STATE_DIR": "/volumes/state",
        "PIPELINE_CONFIG_DIR": "/workspace/config",
    }


def test_resolve_root_falls_back_to_pipeline_config_dir(tmp_path: Path) -> None:
    module = _load_run_pipeline_module()

    root = module._resolve_root(
        None,
        {"PIPELINE_CONFIG_DIR": str(tmp_path / "workspace" / "config")},
    )

    assert root == (tmp_path / "workspace").resolve()
