---
title: Operator run_cycle Decomposition via Dependency Injection — Resolving CON-004
version: 1.0
date_created: 2026-04-29
owner: Data Engineering
tags: [architecture, refactoring, maintainability, operator, testing, dependency-injection]
---

# Introduction

`src/pipeline/orchestration/operator.py` retains `run_cycle()`, a function approximately 645 lines long, because of a hard constraint documented as CON-004 in `spec-architecture-module-cyclomatic-decomposition.md`: existing tests monkeypatch `build_silver`, `build_gold`, and `load_bronze_frame` through the `pipeline.orchestration.operator` namespace. Moving those calls to `operator_stages.py` would break the patches.

The decomposition specs completed all other extractions (operator_reports, operator_artifacts, operator_stages) but explicitly deferred `run_cycle()` due to this constraint. This spec removes CON-004 by replacing monkeypatching with dependency injection (DI) in the test suite, which in turn allows `run_cycle()` to delegate stage execution to `operator_stages.py` functions and drop below the 400-line limit.

## 1. Purpose & Scope

**Purpose:** Define the requirements and interface for replacing the `monkeypatch.setattr` pattern in tests with explicit injectable callables, so that `run_cycle()` can be decomposed into smaller functions without breaking test contracts.

**Scope:**

Updated modules:
- `src/pipeline/orchestration/operator.py` — `run_cycle()` delegates each stage to `operator_stages.py`; retains only the ReAct loop skeleton and the `build_monitor_snapshot()` function
- `src/pipeline/orchestration/operator_stages.py` — receives `_run_react_iteration()` and stage-dispatch helpers extracted from `run_cycle()`
- `tests/test_jobs.py` — replaces `monkeypatch.setattr("pipeline.orchestration.operator.build_silver", ...)` with DI-compatible parameter injection
- `tests/test_transforms.py` and any other test file that monkeypatches symbols in the `pipeline.orchestration.operator` namespace

**Out of scope:**
- Changes to any Parquet schema or data contract
- Changes to `config/pipeline_spec.json`
- Changes to the ReAct loop logic itself — only structural relocation
- Changes to `agent.py`, `planner.py`, or any other agent module
- Reducing `operator.py` below 400 lines if CON-004 removal alone is insufficient; further decomposition is a separate spec

**Intended audience:** Engineers implementing the DI migration and PR reviewers.

## 2. Definitions

| Term | Definition |
|---|---|
| CON-004 | Constraint from Phase 1 decomposition spec: `run_cycle()` must call `load_bronze_frame`, `build_silver`, `build_gold` through the `pipeline.orchestration.operator` namespace so tests can monkeypatch them. |
| Monkeypatching | Python test technique using `monkeypatch.setattr("module.symbol", mock)` to replace a callable at the import-path level during a test. Requires the call site to be in the specified module. |
| Dependency Injection (DI) | A design pattern where a caller passes the callables it depends on as parameters rather than importing them directly. Enables test substitution without monkeypatching the import namespace. |
| ReAct loop skeleton | The `while iteration < _MAX_REACT_ITERATIONS` control structure in `run_cycle()` that decides which stage to execute next and when to stop. This is the only logic that should remain in `operator.py`. |
| Stage callable | A function of the form `(paths, compiled_plan, ...) -> dict` that executes a single pipeline stage (bronze, silver, gold, validation). |
| `StageDeps` | A dataclass bundling the four injectable stage callables passed to `run_cycle()`. |

## 3. Requirements, Constraints & Guidelines

### Dependency injection interface

- **REQ-001**: A new dataclass `StageDeps` MUST be defined in `src/pipeline/orchestration/operator_stages.py`:

```python
@dataclass
class StageDeps:
    run_bronze: Callable
    run_silver: Callable
    run_gold: Callable
    run_validation: Callable
```

- **REQ-002**: `StageDeps` MUST provide a factory `StageDeps.default()` that returns a `StageDeps` wired to the four canonical stage functions from `operator_stages.py`.
- **REQ-003**: `run_cycle()` signature MUST be extended with an optional `stage_deps: StageDeps | None = None` parameter. When `None`, `run_cycle()` calls `StageDeps.default()` internally. This keeps all existing non-test callers working without any changes.

### Extraction of `run_cycle()` body

- **REQ-010**: A new function `_run_react_iteration(state, stage_deps, paths, compiled_plan, ...) -> dict` MUST be defined in `operator_stages.py`. It MUST contain the body of a single ReAct iteration: determining the current stage, dispatching to `stage_deps.run_*`, handling failures, and returning the updated state dict.
- **REQ-011**: `run_cycle()` body MUST be reduced to: setup, the `while` loop calling `_run_react_iteration`, fallback handling, and report writing. Everything else MUST be in `operator_stages.py`.
- **REQ-012**: `run_cycle()` MUST continue to call `load_bronze_frame`, `build_silver`, `build_gold`, and `validate_gold` only through the injected `stage_deps` callables, never by direct import inside `run_cycle()`. This satisfies the spirit of CON-004 (test substitution) without requiring monkeypatching the `operator.py` namespace.

### Test migration

- **REQ-020**: Every `monkeypatch.setattr("pipeline.orchestration.operator.build_silver", ...)` call in the test suite MUST be replaced with a `StageDeps` instance that injects the mock callable directly.
- **REQ-021**: Every `monkeypatch.setattr("pipeline.orchestration.operator.build_gold", ...)` and `monkeypatch.setattr("pipeline.orchestration.operator.load_bronze_frame", ...)` call MUST be replaced in the same way.
- **REQ-022**: No test assertion logic may be changed. Only the wiring of the mock (from namespace-level patching to DI parameter) changes.
- **REQ-023**: After migration, no test file may contain a `monkeypatch.setattr` call targeting the `pipeline.orchestration.operator` namespace for `build_silver`, `build_gold`, or `load_bronze_frame`.

### Size target

- **REQ-030**: After extraction, `operator.py` MUST be below 400 lines. If `run_cycle()` plus `build_monitor_snapshot()` plus imports still exceed 400 lines, the remaining excess MUST be documented with a GUD-001 exception comment at the top of the file.
- **REQ-031**: `_run_react_iteration` in `operator_stages.py` MUST NOT cause `operator_stages.py` to exceed 400 lines. If it does, further extraction into a `_run_react_loop.py` submodule is permitted.

### Retrocompatibility

- **CON-001**: `run_cycle()` MUST remain importable from `pipeline.orchestration.operator` with the same positional parameter contract as today. The new `stage_deps` parameter MUST be keyword-only and optional.
- **CON-002**: All 351 existing tests MUST pass after the migration.
- **CON-003**: `build_silver`, `build_gold`, and `load_bronze_frame` MUST remain importable from `pipeline.orchestration.operator` (they are currently reexported via `# noqa: F401`). This is unchanged by this spec.

### Guidelines

- **GUD-001**: `StageDeps.default()` MUST be the single source of truth for the production wiring. No caller outside tests should pass a custom `StageDeps`.
- **GUD-002**: `_run_react_iteration` MUST be pure with respect to filesystem I/O. All disk writes inside an iteration remain in the stage functions, not in the iteration dispatcher.
- **GUD-003**: The DI pattern MUST NOT be introduced into any module other than `operator.py` and the test suite. This is a targeted change to resolve a specific architectural constraint.

## 4. Interfaces & Data Contracts

### `StageDeps` dataclass

```python
# src/pipeline/orchestration/operator_stages.py

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable

@dataclass
class StageDeps:
    run_bronze: Callable[..., dict[str, Any]]
    run_silver: Callable[..., dict[str, Any]]
    run_gold: Callable[..., dict[str, Any]]
    run_validation: Callable[..., dict[str, Any]]

    @staticmethod
    def default() -> "StageDeps":
        return StageDeps(
            run_bronze=_run_bronze_stage,
            run_silver=_run_silver_stage,
            run_gold=_run_gold_stage,
            run_validation=_run_validation_stage,
        )
```

### Updated `run_cycle()` signature

```python
# src/pipeline/orchestration/operator.py

from pipeline.orchestration.operator_stages import StageDeps

def run_cycle(
    paths: PipelinePaths,
    spec: dict[str, Any],
    *,
    stage_deps: StageDeps | None = None,
    force: bool = False,
) -> PipelineArtifacts:
    deps = stage_deps or StageDeps.default()
    ...
    while iteration < _MAX_REACT_ITERATIONS:
        result = _run_react_iteration(state, deps, paths, compiled_plan, ...)
        ...
```

### `_run_react_iteration` signature

```python
# src/pipeline/orchestration/operator_stages.py

def _run_react_iteration(
    state: dict[str, Any],
    deps: StageDeps,
    paths: PipelinePaths,
    compiled_plan: dict[str, Any],
    spec: dict[str, Any],
    agent_ctx: dict[str, Any],
) -> dict[str, Any]:  # returns updated state
    ...
```

### Test migration pattern

```python
# Before (monkeypatching)
def test_run_cycle_skips_on_no_change(monkeypatch, tmp_path):
    monkeypatch.setattr("pipeline.orchestration.operator.build_silver", lambda *a, **kw: mock_silver_df)
    monkeypatch.setattr("pipeline.orchestration.operator.build_gold", lambda *a, **kw: mock_gold_df)
    run_cycle(paths=..., spec=...)

# After (dependency injection)
from pipeline.orchestration.operator_stages import StageDeps

def test_run_cycle_skips_on_no_change(tmp_path):
    deps = StageDeps(
        run_bronze=lambda *a, **kw: {"bronze_df": mock_bronze_df, "quarantine_report": {}},
        run_silver=lambda *a, **kw: {"silver_df": mock_silver_df, ...},
        run_gold=lambda *a, **kw: {"gold_df": mock_gold_df, ...},
        run_validation=lambda *a, **kw: {"validation_results": [], ...},
    )
    run_cycle(paths=..., spec=..., stage_deps=deps)
```

## 5. Acceptance Criteria

- **AC-001**: Given the DI migration is applied, when `venv/bin/python -m pytest -q` is executed, then all 351 existing tests pass without modification of assertion logic.
- **AC-002**: Given the DI migration is applied, when `wc -l src/pipeline/orchestration/operator.py` is executed, then the result is less than 400 (or a GUD-001 exception comment is present with justification).
- **AC-003**: Given the DI migration is applied, when `grep -rn "monkeypatch.setattr.*operator.*build_silver\|monkeypatch.setattr.*operator.*build_gold\|monkeypatch.setattr.*operator.*load_bronze_frame" tests/` is executed, then the result is empty.
- **AC-004**: Given a test that passes a custom `StageDeps` with a mock `run_silver`, when `run_cycle` is called with that `stage_deps`, then the mock is invoked instead of the real `_run_silver_stage`.
- **AC-005**: Given a caller that does not pass `stage_deps`, when `run_cycle` is called, then `StageDeps.default()` is used and the pipeline executes normally end-to-end.
- **AC-006**: Given the DI migration is applied, when `from pipeline.orchestration.operator import run_cycle, build_monitor_snapshot` is executed, then both symbols import without error.
- **AC-007**: Given the DI migration is applied, when `from pipeline.orchestration.operator_stages import StageDeps, _run_react_iteration` is executed, then both symbols import without error.
- **AC-008**: Given the DI migration is applied, when `ruff check src/` is executed, then the result contains zero errors.
- **AC-009**: Given the DI migration is applied, when `mypy src/pipeline/orchestration/operator.py src/pipeline/orchestration/operator_stages.py` is executed, then zero type errors are reported.

## 6. Test Automation Strategy

- **Test levels**: The existing 351-test suite is the primary acceptance gate. No new test files are required.
- **New unit tests required in `tests/test_jobs.py`**:
  - `test_run_cycle_uses_injected_stage_deps` — verifies AC-004 (mock callable is actually invoked)
  - `test_run_cycle_default_deps_run_end_to_end` — verifies AC-005 (no `stage_deps` → defaults work)
- **Migration of existing tests**: All `monkeypatch.setattr` calls targeting the `operator` namespace for the three symbols above MUST be rewritten to use `StageDeps` injection.
- **Size check**: `wc -l src/pipeline/orchestration/operator.py` → must be below 400 (enforced in CI as a shell step).
- **Namespace check**: `grep -rn "monkeypatch.setattr.*operator.*(build_silver|build_gold|load_bronze_frame)" tests/` → must return empty (enforced in CI).
- **CI/CD**: Existing `venv/bin/python -m pytest -q` step covers all checks. No new workflow step is required.

## 7. Rationale & Context

**Why CON-004 was the blocking constraint:** Monkeypatching requires the mocked symbol to be looked up in the patched module's namespace at call time. If `run_cycle()` calls `build_silver` as `from pipeline.transforms.silver import build_silver` and then calls it as a local name, patching `pipeline.orchestration.operator.build_silver` no longer intercepts the call. This forced `run_cycle()` to remain the place where those names are resolved — making decomposition impossible without test rewrites.

**Why DI is the right resolution:** Dependency injection eliminates the namespace coupling entirely. The test passes in whatever callable it wants; the production path uses `StageDeps.default()`. The change is local to `run_cycle()` and the tests; no other module changes. The production behavior is identical.

**Why this is better than alternative approaches:** An alternative would be to keep monkeypatching but patch `operator_stages` instead of `operator`. That requires tests to know which submodule owns which function, creating fragile coupling between test internals and module structure. DI makes the injection point explicit in the function signature and decouples tests from module topology entirely.

**Expected line count reduction:** `run_cycle()` is currently ~645 lines. Moving the per-iteration dispatch logic (~400 lines) to `_run_react_iteration` reduces `operator.py` by approximately that amount. The remaining `run_cycle()` (loop skeleton, setup, fallback, report writing) plus `build_monitor_snapshot()` plus imports should total under 350 lines.

## 8. Dependencies & External Integrations

### Technology Platform Dependencies
- **PLT-001**: Python 3.11 — `dataclasses`, `typing.Callable`; no new packages.

### Internal Module Dependencies
- **INT-001**: `operator.py` gains an import of `StageDeps` from `operator_stages.py`. This is a new dependency in the same direction that already exists (`operator.py` imports from `operator_stages.py`).
- **INT-002**: No circular imports are introduced. The dependency order remains: `operator_artifacts → operator_reports → operator_stages → operator`.
- **INT-003**: Test files gain an import of `StageDeps` from `pipeline.orchestration.operator_stages`.

## 9. Examples & Edge Cases

```python
# Edge case: partial mock — only silver is replaced, others run real
deps = StageDeps(
    run_bronze=StageDeps.default().run_bronze,
    run_silver=lambda *a, **kw: {"silver_df": minimal_silver_df, ...},
    run_gold=StageDeps.default().run_gold,
    run_validation=StageDeps.default().run_validation,
)
result = run_cycle(paths=..., spec=..., stage_deps=deps)

# Edge case: stage raises inside _run_react_iteration
# The outer run_cycle fallback handler must still catch the exception
# and restore the last successful artifacts — this behavior is unchanged
# because the exception propagates out of the injected callable just as
# it would from a direct call.

# Edge case: StageDeps.default() called twice returns equivalent objects
# (not the same instance, but same callables)
a = StageDeps.default()
b = StageDeps.default()
assert a.run_bronze is b.run_bronze  # same function object
```

## 10. Validation Criteria

- `venv/bin/python -m pytest -q` → all 351 tests pass
- `wc -l src/pipeline/orchestration/operator.py` → less than 400 (or GUD-001 exception present)
- `grep -rn "monkeypatch.setattr.*operator.*(build_silver|build_gold|load_bronze_frame)" tests/` → empty
- `from pipeline.orchestration.operator import run_cycle` → no ImportError
- `from pipeline.orchestration.operator_stages import StageDeps` → no ImportError
- `venv/bin/python -m ruff check src/` → zero errors
- End-to-end pipeline run (`PIPELINE_ENABLE_LLM_ENRICHMENT=0 venv/bin/python scripts/run_pipeline.py --force`) → exits 0

## 11. Related Specifications / Further Reading

- `spec/spec-architecture-module-cyclomatic-decomposition.md` — Phase 1 decomposition that defined CON-004
- `spec/spec-architecture-decomposition-phase-two.md` — Phase 2 that resolved all other gaps except CON-004
- `spec/spec-architecture-semantic-contract-separation.md` — responsibility boundaries between orchestration, agent, and transform layers
