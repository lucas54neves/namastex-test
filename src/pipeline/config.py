from __future__ import annotations

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


def build_paths(root: Path | None = None) -> PipelinePaths:
    base = (root or Path(__file__).resolve().parents[2]).resolve()
    data = base / "data"
    config = base / "config"
    return PipelinePaths(
        root=base,
        config=config,
        docs=base / "docs",
        data=data,
        bronze=data / "bronze",
        silver=data / "silver",
        gold=data / "gold",
        quarantine=data / "quarantine",
        reports=base / "reports",
        monitoring=base / "reports" / "monitoring",
        alerts=base / "reports" / "alerts",
        agent_decisions=base / "reports" / "agent_decisions",
        state=base / "state",
        raw_bronze_source=base / "docs" / "conversations_bronze.parquet",
        pipeline_spec=config / "pipeline_spec.json",
        approval_state=base / "state" / "approval_state.json",
        spec_history=base / "state" / "pipeline_spec_history.json",
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
        paths.state,
    ):
        directory.mkdir(parents=True, exist_ok=True)
