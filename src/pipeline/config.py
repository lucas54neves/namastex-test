from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelinePaths:
    root: Path
    docs: Path
    data: Path
    bronze: Path
    silver: Path
    gold: Path
    reports: Path
    monitoring: Path
    alerts: Path
    state: Path
    raw_bronze_source: Path


def build_paths(root: Path | None = None) -> PipelinePaths:
    base = (root or Path(__file__).resolve().parents[2]).resolve()
    data = base / "data"
    return PipelinePaths(
        root=base,
        docs=base / "docs",
        data=data,
        bronze=data / "bronze",
        silver=data / "silver",
        gold=data / "gold",
        reports=base / "reports",
        monitoring=base / "reports" / "monitoring",
        alerts=base / "reports" / "alerts",
        state=base / "state",
        raw_bronze_source=base / "docs" / "conversations_bronze.parquet",
    )


def ensure_directories(paths: PipelinePaths) -> None:
    for directory in (
        paths.data,
        paths.bronze,
        paths.silver,
        paths.gold,
        paths.reports,
        paths.monitoring,
        paths.alerts,
        paths.state,
    ):
        directory.mkdir(parents=True, exist_ok=True)
