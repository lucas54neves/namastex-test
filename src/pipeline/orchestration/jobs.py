from __future__ import annotations

from dataclasses import asdict

from pipeline.config import PipelinePaths
from pipeline.orchestration.operator import (
    PipelineArtifacts,
    run_cycle,
)
from pipeline.orchestration.operator import (
    build_monitor_snapshot as _build_monitor_snapshot,
)


def run_pipeline(
    paths: PipelinePaths,
    force: bool = False,
    idle_cycle_count: int = 0,
    planner_cadence: int = 0,
) -> PipelineArtifacts:
    return run_cycle(
        paths, force=force, idle_cycle_count=idle_cycle_count, planner_cadence=planner_cadence
    )


def build_monitor_snapshot(paths: PipelinePaths) -> dict[str, object]:
    return _build_monitor_snapshot(paths)


def artifacts_as_dict(artifacts: PipelineArtifacts) -> dict[str, object]:
    return asdict(artifacts)
