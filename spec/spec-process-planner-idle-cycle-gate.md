---
title: Planner Idle-Cycle Gate — Conditional Execution of plan_pipeline_spec (GAP-A02)
version: 1.0
date_created: 2026-04-29
owner: Data Engineering
tags: [process, architecture, planner, autonomy, cost-reduction, gap-a02]
---

# Introduction

This specification defines the requirements for gating the `plan_pipeline_spec()` call inside `run_cycle()` so that LLM advisor calls and all five detectors execute only when meaningful work is warranted — either because the source data changed, or because a configurable cadence counter indicates that a periodic drift check is due. The change closes GAP-A02 from `docs/agent-autonomy-gaps.md`.

## 1. Purpose & Scope

**Purpose:** `plan_pipeline_spec(paths)` is called unconditionally at line 118 of `src/pipeline/orchestration/operator.py`, before the source-fingerprint check at line 129. This means all five detectors and the LLM advisor run on every daemon cycle, including cycles that exit early with `skipped_no_source_change`. At a 60-second poll interval with a stable source, this produces dozens of unnecessary LLM calls per hour with no operational benefit.

**Goal:** Move the planner call to after the fingerprint check and gate it on `changed=True` (primary gate) or on a configurable cadence counter that fires every N consecutive idle cycles (secondary gate), so that every planning run corresponds to either real source change or intentional periodic drift detection.

**Affected files:**
- `src/pipeline/orchestration/operator.py` — gate logic and planner call relocation
- `scripts/run_pipeline_daemon.py` — new `--planner-cadence-cycles` CLI argument that passes the counter through `run_cycle()` or is tracked externally

**Out of scope:**
- Changes to `src/pipeline/agent/planner.py` — the planner itself is unchanged
- Changes to the five detectors or the LLM advisor
- Changes to approval, autonomy, or proposal-evaluation logic
- Any schema, Bronze/Silver/Gold transform, or report format changes

**Audience:** Engineers implementing the fix and PR reviewers.

## 2. Definitions

| Term | Definition |
|---|---|
| Idle cycle | A daemon cycle in which `has_source_changed()` returns `False` and `force=False` — the cycle exits early via the `skipped_no_source_change` path |
| Active cycle | A daemon cycle in which `changed=True` or `force=True` — the full ReAct loop executes |
| Primary gate | The condition `changed or force` that permits a planning run |
| Secondary gate (cadence) | An optional counter that triggers a planning run every N consecutive idle cycles, regardless of source change, to detect slow-accumulating drift |
| Idle cycle counter | An integer accumulated across daemon cycles, reset to zero after any active cycle or planning run triggered by the cadence |
| Planner | `plan_pipeline_spec(paths)` in `src/pipeline/agent/planner.py` — reads Bronze, runs five detectors, calls LLM advisor |
| Empty planner report | A sentinel `dict` returned when planning is skipped, containing `proposals=[]`, `applied=False`, and `skipped=True` |
| LLM advisor | The `get_llm_advice()` call inside `plan_pipeline_spec()` that consumes LLM tokens |
| `run_cycle()` | The function in `operator.py` that orchestrates one pipeline execution; the only caller of `plan_pipeline_spec()` |
| Cadence N | The number of consecutive idle cycles after which the secondary gate fires; `0` disables the secondary gate |

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: `plan_pipeline_spec(paths)` MUST NOT be called before `has_source_changed()` returns a result.
- **REQ-002**: `plan_pipeline_spec(paths)` MUST be called when `changed=True`, regardless of any cadence setting.
- **REQ-003**: `plan_pipeline_spec(paths)` MUST be called when `force=True`, regardless of source-change state or cadence.
- **REQ-004**: When neither `changed` nor `force` is true and the cadence secondary gate has not fired, `plan_pipeline_spec(paths)` MUST NOT be called. An empty planner report MUST be substituted.
- **REQ-005**: The secondary gate MUST fire when the idle cycle counter reaches the configured cadence N (N > 0), triggering a planning run on an idle cycle and resetting the counter to zero.
- **REQ-006**: After any active cycle (changed or forced) that calls the planner, the idle cycle counter MUST be reset to zero.
- **REQ-007**: After a cadence-triggered planning run on an idle cycle, the idle cycle counter MUST be reset to zero.
- **REQ-008**: When planning is skipped (REQ-004), the `planner_summary` passed to `_build_agent_report()` and `_write_reports()` MUST reflect the skipped state via `skipped=True` and `proposal_count=0`.
- **REQ-009**: The `run_cycle()` function signature MUST NOT change; the idle cycle counter MUST be managed by the caller (`run_daemon()`) and passed into `run_cycle()` as a parameter, OR managed as a module-level state in `run_cycle()`'s closure.
- **REQ-010**: The cadence N MUST be configurable via the CLI argument `--planner-cadence-cycles INT` in `run_pipeline_daemon.py`, defaulting to `int(os.getenv("PIPELINE_PLANNER_CADENCE_CYCLES", 0))` (disabled by default).
- **REQ-011**: When cadence N is `0`, the secondary gate MUST be permanently disabled — only `changed` or `force` triggers planning.
- **CON-001**: The idle cycle counter state MUST NOT be persisted to `pipeline_state.json` — it resets on daemon restart.
- **CON-002**: The empty planner report MUST be structurally compatible with `_planner_report_summary()` — it must not cause a `KeyError` or `TypeError` when passed as `planner_report`.
- **CON-003**: The `--planner-cadence-cycles` argument MUST accept only non-negative integers; negative values MUST be rejected by `argparse`.
- **GUD-001**: The default cadence of `0` (disabled) preserves the conservative behavior: no planning unless source changes.
- **GUD-002**: Operators who want periodic drift detection without source change can set `--planner-cadence-cycles 10` to run the planner once every ten idle cycles (approximately every 10 minutes at the default 60-second poll interval).
- **GUD-003**: The empty planner report constant SHOULD be defined as `_EMPTY_PLANNER_REPORT` at module level in `operator.py` for clarity.

## 4. Interfaces & Data Contracts

### 4.1 `run_cycle()` — internal call order change

Current order (lines 114–129 of `operator.py`):

```python
def run_cycle(paths: PipelinePaths, force: bool = False) -> PipelineArtifacts:
    ensure_directories(paths)
    spec = ensure_pipeline_spec(paths.pipeline_spec)
    compiled_plan = compile_pipeline_spec(spec)
    planner_report = plan_pipeline_spec(paths)          # ← line 118: BEFORE fingerprint check
    planner_summary = _planner_report_summary(paths, planner_report)

    current_fingerprint_obj = build_source_fingerprint(paths.raw_bronze_source)
    current_fingerprint = current_fingerprint_obj.as_dict()
    state = load_pipeline_state(state_path)
    previous_fingerprint = state.get("last_source_fingerprint")
    changed = has_source_changed(current_fingerprint_obj, previous_fingerprint)  # ← line 129
```

Required order after fix:

```python
def run_cycle(
    paths: PipelinePaths,
    force: bool = False,
    idle_cycle_count: int = 0,
    planner_cadence: int = 0,
) -> PipelineArtifacts:
    ensure_directories(paths)
    spec = ensure_pipeline_spec(paths.pipeline_spec)
    compiled_plan = compile_pipeline_spec(spec)

    current_fingerprint_obj = build_source_fingerprint(paths.raw_bronze_source)
    current_fingerprint = current_fingerprint_obj.as_dict()
    state = load_pipeline_state(state_path)
    previous_fingerprint = state.get("last_source_fingerprint")
    changed = has_source_changed(current_fingerprint_obj, previous_fingerprint)  # ← fingerprint first

    cadence_triggered = (
        planner_cadence > 0 and idle_cycle_count > 0 and idle_cycle_count % planner_cadence == 0
    )
    should_plan = changed or force or cadence_triggered

    if should_plan:
        planner_report = plan_pipeline_spec(paths)   # ← called only when warranted
    else:
        planner_report = _EMPTY_PLANNER_REPORT       # ← sentinel, no LLM call
    planner_summary = _planner_report_summary(paths, planner_report)
```

> **Note on signature:** If changing `run_cycle()`'s signature causes a break in existing test mocks, the alternative is to keep the signature unchanged and manage the counter entirely in `run_daemon()`, storing it as a local variable passed only to a new internal helper. Either approach is acceptable as long as REQ-009 is satisfied.

### 4.2 Empty planner report sentinel

```python
_EMPTY_PLANNER_REPORT: dict[str, Any] = {
    "proposals": [],
    "applied": False,
    "skipped": True,
    "reason": "no_source_change_and_cadence_not_triggered",
    "proposal_count": 0,
    "promoted_proposal_ids": [],
}
```

This sentinel MUST be returned whenever the gate suppresses planning. `_planner_report_summary()` MUST handle `skipped=True` without raising.

### 4.3 New CLI argument in `run_pipeline_daemon.py`

```
--planner-cadence-cycles INT
    Number of consecutive idle cycles after which the planner runs even without
    source change. 0 disables the secondary gate (default).
    Env var: PIPELINE_PLANNER_CADENCE_CYCLES
```

### 4.4 Idle cycle counter management in `run_daemon()`

```python
idle_cycle_count = 0

while True:
    cycle += 1
    force = bool(args.force_first_run and cycle == 1)
    artifacts = run_pipeline(
        build_paths(ROOT),
        force=force,
        idle_cycle_count=idle_cycle_count,
        planner_cadence=args.planner_cadence_cycles,
    )
    if artifacts.status == "skipped_no_source_change":
        idle_cycle_count += 1
    else:
        idle_cycle_count = 0
    ...
```

### 4.5 Log events

| Event | Level | Fields | Condition |
|---|---|---|---|
| `planner_skipped` | INFO | `reason`, `idle_cycle_count`, `cadence` | Planning gate suppressed the call |
| `planner_triggered_by_cadence` | INFO | `idle_cycle_count`, `cadence` | Secondary cadence gate fired on idle cycle |

### 4.6 `_planner_report_summary()` — empty-report compatibility

`_planner_report_summary()` in `operator_reports.py` MUST return a valid summary dict when given `_EMPTY_PLANNER_REPORT` as input. The returned summary MUST include at minimum:

```json
{
  "proposal_count": 0,
  "applied": false,
  "promoted_proposal_ids": [],
  "skipped": true
}
```

## 5. Acceptance Criteria

- **AC-001**: Given `changed=False` and `force=False` and `planner_cadence=0`, when `run_cycle()` executes, then `plan_pipeline_spec()` MUST NOT be called and the cycle MUST complete without LLM calls.
- **AC-002**: Given `changed=True` and `force=False`, when `run_cycle()` executes, then `plan_pipeline_spec()` MUST be called exactly once.
- **AC-003**: Given `changed=False` and `force=True`, when `run_cycle()` executes, then `plan_pipeline_spec()` MUST be called exactly once.
- **AC-004**: Given `planner_cadence=5` and `idle_cycle_count=5` (cadence boundary), when `run_cycle()` executes with `changed=False`, then `plan_pipeline_spec()` MUST be called and `idle_cycle_count` MUST reset to `0` after the cycle.
- **AC-005**: Given `planner_cadence=5` and `idle_cycle_count=3` (below boundary), when `run_cycle()` executes with `changed=False`, then `plan_pipeline_spec()` MUST NOT be called.
- **AC-006**: Given `planner_cadence=0` (disabled) and any `idle_cycle_count`, when `run_cycle()` executes with `changed=False`, then `plan_pipeline_spec()` MUST NOT be called regardless of counter value.
- **AC-007**: Given a skipped planning run, when `_build_agent_report()` is called with the resulting `planner_summary`, then the agent report MUST serialize without `KeyError` and `planner_report.skipped` MUST be `true`.
- **AC-008**: Given `--planner-cadence-cycles -1` passed via CLI, when the daemon starts, then `argparse` MUST reject the value before the daemon loop begins.
- **AC-009**: Given 10 consecutive idle cycles with `planner_cadence=10`, when the daemon runs, then `plan_pipeline_spec()` MUST be called exactly once (on cycle 10), not ten times.

## 6. Test Automation Strategy

- **Test Levels**: Unit — mock `plan_pipeline_spec`, `has_source_changed`, `build_source_fingerprint`, and `load_pipeline_state` to exercise the gate logic without I/O.
- **Frameworks**: `pytest`, `unittest.mock.patch`.
- **Test file**: `tests/test_planner_idle_gate.py` (new).

### Required test scenarios

| Test ID | Description |
|---|---|
| `test_planner_not_called_on_idle_cycle` | `changed=False`, `force=False`, `cadence=0` → `plan_pipeline_spec` not called |
| `test_planner_called_on_source_change` | `changed=True` → `plan_pipeline_spec` called once |
| `test_planner_called_on_force` | `changed=False`, `force=True` → `plan_pipeline_spec` called once |
| `test_cadence_triggers_on_boundary` | `cadence=5`, `idle_cycle_count=5`, `changed=False` → called once |
| `test_cadence_does_not_trigger_below_boundary` | `cadence=5`, `idle_cycle_count=4`, `changed=False` → not called |
| `test_cadence_zero_disables_secondary_gate` | `cadence=0`, `idle_cycle_count=100`, `changed=False` → not called |
| `test_empty_planner_report_no_key_error` | `_EMPTY_PLANNER_REPORT` passed to `_planner_report_summary()` → returns valid dict |
| `test_idle_counter_resets_after_active_cycle` | Active cycle (`changed=True`) → counter reset to 0 in `run_daemon` |
| `test_idle_counter_increments_on_skip` | Idle cycle → counter incremented by 1 in `run_daemon` |

### CI/CD

- `venv/bin/python -m pytest tests/test_planner_idle_gate.py -q` MUST pass without additional setup.
- `venv/bin/python -m pytest -q` (full suite) MUST continue to pass — no regressions in existing tests that mock `plan_pipeline_spec` via `pipeline.orchestration.operator`.

## 7. Rationale & Context

The planning phase in `plan_pipeline_spec()` performs three categories of work on every call: (1) reading and deserializing the Bronze Parquet file, (2) running five deterministic detectors over the data, and (3) calling the LLM advisor via `get_llm_advice()`. Category (3) consumes LLM tokens; categories (1) and (2) consume I/O and CPU. None of these operations produce useful output when the source data has not changed since the last cycle — the detectors will reach the same conclusions they reached the previous cycle, and the LLM advisor will receive the same inputs.

At the default poll interval of 60 seconds, a stable source generates 60 idle cycles per hour. Each idle cycle currently triggers a full planning run, meaning up to 60 LLM calls per hour with zero operational benefit. The fix reduces this to zero (primary gate) or to `60 / N` calls per hour (secondary gate), where N is the cadence setting. Every planning run after the fix corresponds to a meaningful event.

The secondary cadence gate addresses the objection that planning should occasionally run even on idle cycles to detect slow-accumulating drift — for example, a metadata field that gradually becomes more common across new records that are already in the source, without triggering a fingerprint change. Setting `--planner-cadence-cycles 10` at a 60-second interval provides one drift check every 10 minutes, which is sufficient for the drift patterns the current detectors observe.

The fingerprint-before-planner reordering also improves the logical clarity of `run_cycle()`: the fingerprint check is now the first decision gate, which matches the intended semantics of the function.

## 8. Dependencies & External Integrations

### External Systems
- **EXT-001**: None. All changes are internal to `operator.py` and `run_pipeline_daemon.py`.

### Infrastructure Dependencies
- **INF-001**: `os.getenv` — already used in `run_pipeline_daemon.py`; used to read `PIPELINE_PLANNER_CADENCE_CYCLES`.

### Technology Platform Dependencies
- **PLT-001**: Python 3.11+ — already required by the repository.

### Data Dependencies
- **DAT-001**: `pipeline_state.json` — read by `load_pipeline_state()` before the planner gate; the counter is NOT stored here (CON-001).

## 9. Examples & Edge Cases

### Normal idle suppression

```
Cycle 1: changed=False, force=False, cadence=0 → planner SKIPPED, counter=1
Cycle 2: changed=False, force=False, cadence=0 → planner SKIPPED, counter=2
Cycle 3: changed=True  → planner CALLED,   counter reset=0
Cycle 4: changed=False → planner SKIPPED, counter=1
```

### Cadence-triggered planning on idle source

```
cadence=5, poll_interval=60s → planner fires every ~5 minutes during idle

Cycle 1–4:  changed=False → SKIPPED (counter 1→4)
Cycle 5:    changed=False → CALLED (counter==cadence), counter reset=0
Cycle 6–10: changed=False → SKIPPED (counter 1→4 again)
Cycle 11:   changed=False → CALLED
```

### Source change resets counter mid-sequence

```
cadence=5
Cycle 1–3: idle → counter=3
Cycle 4:   changed=True → planner CALLED, counter=0  (source-change gate)
Cycle 5–8: idle → counter=1→4
Cycle 9:   idle, counter==5==cadence → planner CALLED (cadence gate), counter=0
```

### Edge case: force=True overrides gate entirely

```
changed=False, force=True, cadence=0, counter=0 → planner CALLED
```

### Edge case: planner_cadence=1 — planning on every idle cycle (equivalent to old behavior)

```
cadence=1
Every idle cycle → counter increments to 1 → fires → counter resets to 0
```

This effectively disables the gate and reproduces the old unconditional behavior, which is useful for debugging or for operators who explicitly want the old behavior.

### Edge case: first cycle with force_first_run

```
Cycle 1: force=True → planner CALLED regardless of changed or counter
```

## 10. Validation Criteria

- **VAL-001**: `venv/bin/python -m pytest tests/test_planner_idle_gate.py -q` MUST pass with all scenarios from Section 6.
- **VAL-002**: `venv/bin/python -m pytest -q` (full suite) MUST pass with no regressions.
- **VAL-003**: `grep -n "plan_pipeline_spec" src/pipeline/orchestration/operator.py` MUST show the call appearing AFTER `has_source_changed` in the file.
- **VAL-004**: `grep -n "_EMPTY_PLANNER_REPORT" src/pipeline/orchestration/operator.py` MUST return the constant definition and at least one usage site.
- **VAL-005**: `grep -n "planner_cadence_cycles\|PLANNER_CADENCE" scripts/run_pipeline_daemon.py` MUST return the CLI argument definition and the env var read.
- **VAL-006**: Running the daemon for 5 cycles against a static source with `--planner-cadence-cycles 0` MUST produce zero `planning_run_` entries in the plan report, confirmed by `grep -c "planning_run_" reports/monitoring/latest_plan_report.json`.

## 11. Related Specifications / Further Reading

- [docs/agent-autonomy-gaps.md](../docs/agent-autonomy-gaps.md) — Full autonomy gap catalogue; GAP-A02 is the source of this spec
- [spec-process-daemon-watchdog-exponential-backoff.md](./spec-process-daemon-watchdog-exponential-backoff.md) — GAP-A01 implementation; the daemon loop structure assumed here
- [spec-architecture-agentic-layer-gap-remediation.md](./spec-architecture-agentic-layer-gap-remediation.md) — Original GAP-01 to GAP-04 remediation architecture
- [spec-design-agentic-layer-quality-gaps.md](./spec-design-agentic-layer-quality-gaps.md) — Agentic layer quality gaps (QUAL-01 to QUAL-04)
