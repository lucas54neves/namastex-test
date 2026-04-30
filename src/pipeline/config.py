from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelinePaths:
    root: Path
    config: Path
    docs: Path
    data: Path
    bronze: Path
    silver: Path
    gold: Path
    quarantine: Path
    reports: Path
    monitoring: Path
    alerts: Path
    agent_decisions: Path
    state: Path
    raw_bronze_source: Path
    pipeline_spec: Path
    approval_state: Path
    spec_history: Path
    autonomy_policy: Path
    autonomy_metrics: Path
    schema_drift_report: Path
    schema_promotion_history: Path
    candidates: Path
    autonomy_decisions: Path
    autonomy_proposals: Path


def _resolve_path(
    env: dict[str, str],
    name: str,
    default: Path,
    *,
    resolve: bool = True,
) -> Path:
    raw = env.get(name)
    candidate = Path(raw).expanduser() if raw else default
    return candidate.resolve() if resolve else candidate


def build_paths(root: Path | None = None, env: dict[str, str] | None = None) -> PipelinePaths:
    base = (root or Path(__file__).resolve().parents[2]).resolve()
    runtime_env = dict(os.environ if env is None else env)

    config = _resolve_path(runtime_env, "PIPELINE_CONFIG_DIR", base / "config")
    data = _resolve_path(runtime_env, "PIPELINE_DATA_DIR", base / "data")
    reports = _resolve_path(runtime_env, "PIPELINE_REPORTS_DIR", base / "reports")
    state = _resolve_path(runtime_env, "PIPELINE_STATE_DIR", base / "state")
    runtime = _resolve_path(runtime_env, "PIPELINE_RUNTIME_DIR", base / "runtime")
    raw_bronze_source = _resolve_path(
        runtime_env,
        "PIPELINE_INPUT_FILE",
        base / "docs" / "conversations_bronze.parquet",
    )

    return PipelinePaths(
        root=base,
        config=config,
        docs=base / "docs",
        data=data,
        bronze=data / "bronze",
        silver=data / "silver",
        gold=data / "gold",
        quarantine=data / "quarantine",
        reports=reports,
        monitoring=reports / "monitoring",
        alerts=reports / "alerts",
        agent_decisions=reports / "agent_decisions",
        state=state,
        raw_bronze_source=raw_bronze_source,
        pipeline_spec=config / "pipeline_spec.json",
        approval_state=state / "approval_state.json",
        spec_history=state / "pipeline_spec_history.json",
        autonomy_policy=config / "agent_autonomy_policy.json",
        autonomy_metrics=reports / "monitoring" / "agent_autonomy_metrics.json",
        schema_drift_report=reports / "monitoring" / "latest_schema_drift_report.json",
        schema_promotion_history=reports / "monitoring" / "schema_promotion_history.json",
        candidates=runtime / "candidates",
        autonomy_decisions=reports / "agent_decisions" / "autonomy",
        autonomy_proposals=reports / "agent_decisions" / "proposals",
    )


def ensure_directories(paths: PipelinePaths) -> None:
    for directory in (
        paths.config,
        paths.data,
        paths.bronze,
        paths.silver,
        paths.gold,
        paths.quarantine,
        paths.reports,
        paths.monitoring,
        paths.alerts,
        paths.agent_decisions,
        paths.autonomy_decisions,
        paths.autonomy_proposals,
        paths.state,
        paths.candidates,
    ):
        directory.mkdir(parents=True, exist_ok=True)
