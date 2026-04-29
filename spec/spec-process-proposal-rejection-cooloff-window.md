---
title: Cooling-Off Window for Explicitly Rejected Proposals (GAP-A04)
version: 1.0
date_created: 2026-04-29
owner: Data Engineering
tags: [process, agent, autonomy, governance, planner, approval, gap-a04]
---

# Introduction

This specification defines the requirements to close GAP-A04 identified in `docs/agent-autonomy-gaps.md`. When a proposal is explicitly rejected (`APPROVAL_STATUS_REJECTED`), the planner's detectors run independently of the rejection history. If the detected pattern persists in the data across cycles, the same proposal (same deterministic `proposal_id` fingerprint) is re-generated, re-evaluated, and found rejected again — every planning cycle, indefinitely.

The fix introduces a **cooling-off window** per rejected proposal. When the rejection branch is reached for a given `proposal_id`, a cooloff record is written to `approval_state.json`. On subsequent cycles, the planner checks the cooloff before evaluation and suppresses re-generation for a configurable window (10 cycles or 24 hours, whichever comes first). After the window expires, the proposal is re-evaluated once to allow the operator to reconsider. If still rejected, a new cooloff window begins.

---

## 1. Purpose & Scope

**Purpose:** Eliminate the infinite re-evaluation loop for explicitly rejected proposals. Proposals that were deliberately rejected MUST not re-enter the evaluation pipeline every cycle; they MUST be suppressed during a cooling-off window and re-surfaced only when the window expires.

**Scope:** `src/pipeline/agent/approval.py` and `src/pipeline/agent/planner.py`. No changes to the Bronze, Silver, or Gold transformation layers, daemon loop, or LLM advisor.

**Files affected:**
- `src/pipeline/agent/autonomy.py` — two new top-level policy fields in `DEFAULT_AUTONOMY_POLICY`; new helper function `get_rejection_cooloff_policy`
- `src/pipeline/agent/approval.py` — new `rejection_cooloff` section in `approval_state.json`; five new helper functions
- `src/pipeline/agent/planner.py` — cooloff check inserted into the evaluation loop; cooloff start inserted into the rejection branch

**Audience:** Engineers implementing the fix and PR reviewers.

---

## 2. Definitions

| Term | Definition |
|---|---|
| `APPROVAL_STATUS_REJECTED` | Constant (`"rejected"`) returned by `get_proposal_approval_status()` when `decision.get("approved") is False` |
| Cooling-off window | A configurable suppression period following an explicit rejection; during this window the proposal is not re-evaluated |
| Cooloff record | A per-`proposal_id` entry in `approval_state.json` under the `rejection_cooloff` key tracking `cooloff_started_at_utc` and `cycle_count` |
| `cycle_count` | Integer field in the cooloff record tracking how many planning cycles the proposal has been suppressed since the rejection |
| Cooloff threshold cycles | Configurable maximum number of suppressed cycles before the window expires; default `10` |
| Cooloff threshold hours | Configurable maximum elapsed time (hours) from cooloff start before the window expires; default `24.0` |
| Window expiry | Either condition is met: `cycle_count >= cooloff_threshold_cycles` OR `elapsed_hours >= cooloff_threshold_hours` |
| Re-evaluation pass | The single planning cycle after window expiry in which the proposal is allowed through to normal evaluation |
| Fresh cooloff | A new cooloff record (reset `cycle_count=0`, updated `cooloff_started_at_utc`) started immediately after a re-evaluation pass that results in rejection again |
| `proposal_id` | Deterministic SHA-1 fingerprint derived from `proposal_type`, `proposal_family`, `proposed_change`, and `items` — stable across cycles for the same detected pattern |
| `approval_state.json` | File at `state/autonomy/approval_state.json` managed by `src/pipeline/agent/approval.py` |
| Planning run | One execution of `plan_pipeline_spec(paths)` |

---

## 3. Requirements, Constraints & Guidelines

### Cooloff activation

- **REQ-001**: When a proposal reaches the `approval_status == APPROVAL_STATUS_REJECTED` branch in `plan_pipeline_spec()`, `start_rejection_cooloff(paths, proposal_id)` MUST be called immediately after setting `proposal["status"] = PROPOSAL_STATUS_REJECTED` and before `persist_proposal_record`.
- **REQ-002**: `start_rejection_cooloff` MUST be idempotent: if a cooloff record already exists for `proposal_id` (e.g., from a previous window), it MUST overwrite it with `cycle_count=0` and `cooloff_started_at_utc=utc_now_iso()`.

### Cooloff suppression in the evaluation loop

- **REQ-003**: At the start of each proposal's evaluation in the main loop (before `build_candidate_actions`), `is_in_rejection_cooloff(paths, proposal_id)` MUST be called.
- **REQ-004**: When `is_in_rejection_cooloff` returns `True` AND the current `approval_status != APPROVAL_STATUS_REJECTED` (operator changed the decision since cooloff started), `expire_rejection_cooloff(paths, proposal_id)` MUST be called and the proposal MUST proceed to normal evaluation without suppression.
- **REQ-005**: When `is_in_rejection_cooloff` returns `True` AND `approval_status == APPROVAL_STATUS_REJECTED`, `increment_rejection_cooloff_cycle(paths, proposal_id)` MUST be called, returning the new `cycle_count`.
- **REQ-006**: After incrementing, elapsed time MUST be computed as `(utcnow - cooloff_started_at_utc).total_seconds() / 3600`. If `new_count >= cooloff_threshold_cycles` OR `elapsed_hours >= cooloff_threshold_hours`, `expire_rejection_cooloff(paths, proposal_id)` MUST be called and the proposal MUST proceed to normal evaluation (re-evaluation pass).
- **REQ-007**: If the window has NOT expired, the proposal MUST be marked `PROPOSAL_STATUS_CLOSED_NO_ACTION` with `decision_reason = "rejected_cooloff_active"`, persisted via `persist_proposal_record`, and skipped (no gate evaluation, no autonomy decision record, no metrics update).

### Cooloff policy configuration

- **REQ-008**: Two new top-level keys MUST be added to `DEFAULT_AUTONOMY_POLICY` in `autonomy.py`:
  - `"rejection_cooloff_threshold_cycles": 10`
  - `"rejection_cooloff_threshold_hours": 24.0`
- **REQ-009**: A new helper `get_rejection_cooloff_policy(paths: PipelinePaths) -> dict[str, Any]` MUST be added to `autonomy.py`, reading from `load_autonomy_policy(paths)` and returning a dict with keys `threshold_cycles` (int) and `threshold_hours` (float), using defaults from `DEFAULT_AUTONOMY_POLICY` if absent.
- **REQ-010**: The cooloff policy MUST be loaded once per planning run (before the proposal evaluation loop), not per-proposal, to avoid redundant I/O.

### New helper functions in `approval.py`

- **REQ-011**: `start_rejection_cooloff(paths: PipelinePaths, proposal_id: str) -> None` — writes `{"cooloff_started_at_utc": utcnow, "cycle_count": 0}` into `state["rejection_cooloff"][proposal_id]` in `approval_state.json`.
- **REQ-012**: `get_rejection_cooloff_record(paths: PipelinePaths, proposal_id: str) -> dict[str, Any]` — returns the cooloff entry dict for `proposal_id`, or `{}` if not present.
- **REQ-013**: `is_in_rejection_cooloff(paths: PipelinePaths, proposal_id: str) -> bool` — returns `True` if a cooloff entry exists for `proposal_id`.
- **REQ-014**: `increment_rejection_cooloff_cycle(paths: PipelinePaths, proposal_id: str) -> int` — increments `state["rejection_cooloff"][proposal_id]["cycle_count"]` by 1, persists the state, and returns the new count. MUST be a no-op (returns 0) if no entry exists for `proposal_id`.
- **REQ-015**: `expire_rejection_cooloff(paths: PipelinePaths, proposal_id: str) -> None` — removes `state["rejection_cooloff"][proposal_id]` from `approval_state.json`. MUST be a no-op if no entry exists.

### `approval_state.json` schema

- **REQ-016**: The `approval_state.json` schema MUST include a top-level `"rejection_cooloff"` key (dict), defaulted to `{}` if absent. Existing `load_approval_state` default MUST be updated to include this key.

### Metrics and decisions

- **REQ-017**: Proposals suppressed via cooloff (`rejected_cooloff_active`) MUST NOT call `update_autonomy_metrics`, `persist_autonomy_decision`, or gate evaluation functions. The only state mutation allowed is writing the cooloff cycle count and calling `persist_proposal_record` with `PROPOSAL_STATUS_CLOSED_NO_ACTION`.
- **REQ-018**: Proposals on the re-evaluation pass that are found rejected again MUST follow the same metric and decision path as any other newly-rejected proposal (existing `DECISION_REJECT`, `PROPOSAL_STATUS_REJECTED` path) — no special metrics for cooloff re-evaluation.

### Constraints

- **CON-001**: The `proposal_id` fingerprint calculation in `_proposal_fingerprint` MUST NOT be modified. The cooling-off mechanism depends on the stability of the fingerprint across cycles.
- **CON-002**: The cooloff state MUST be stored in `approval_state.json`, NOT in the proposal record file (`state/autonomy/proposals/<proposal_id>.json`). Approval lifecycle state belongs in the approval module.
- **CON-003**: The cooloff check MUST be placed inside the main proposal evaluation loop in `plan_pipeline_spec`, AFTER the `approval_status_by_proposal` dict is built but BEFORE any gate or candidate evaluation for the proposal.
- **CON-004**: The cooloff mechanism MUST NOT interfere with proposals in `APPROVAL_STATUS_APPROVED` status. Those are handled by the standard promotion path and bypass the rejection branch entirely.
- **CON-005**: `expire_rejection_cooloff` MUST be idempotent: calling it for a `proposal_id` not in the cooloff dict MUST NOT raise an error or mutate state.

### Guidelines

- **GUD-001**: Read `is_in_rejection_cooloff` and `get_rejection_cooloff_record` in a single block — avoid calling `get_rejection_cooloff_record` unless `is_in_rejection_cooloff` has already returned `True`.
- **GUD-002**: The cooloff policy load (`get_rejection_cooloff_policy`) MUST happen once per `plan_pipeline_spec` call, stored in a local variable, and passed or closed over as needed — not called per proposal.

---

## 4. Interfaces & Data Contracts

### 4.1 New policy fields in `DEFAULT_AUTONOMY_POLICY` (`autonomy.py`)

```python
DEFAULT_AUTONOMY_POLICY: dict[str, Any] = {
    # ... existing fields ...
    "rejection_cooloff_threshold_cycles": 10,
    "rejection_cooloff_threshold_hours": 24.0,
}
```

### 4.2 New helper in `autonomy.py`

```python
def get_rejection_cooloff_policy(paths: PipelinePaths) -> dict[str, Any]:
    policy = load_autonomy_policy(paths)
    return {
        "threshold_cycles": int(
            policy.get("rejection_cooloff_threshold_cycles", 10)
        ),
        "threshold_hours": float(
            policy.get("rejection_cooloff_threshold_hours", 24.0)
        ),
    }
```

### 4.3 Updated `approval_state.json` default in `approval.py`

```python
def load_approval_state(paths: PipelinePaths) -> dict[str, Any]:
    return read_json(
        paths.approval_state,
        default={
            "approved_proposals": {},
            "proposal_decisions": {},
            "rejection_cooloff": {},
        },
    )
```

### 4.4 New helper functions in `approval.py`

```python
def start_rejection_cooloff(paths: PipelinePaths, proposal_id: str) -> None:
    state = load_approval_state(paths)
    state.setdefault("rejection_cooloff", {})[proposal_id] = {
        "cooloff_started_at_utc": _utc_now_iso(),
        "cycle_count": 0,
    }
    write_json(state, paths.approval_state)


def get_rejection_cooloff_record(paths: PipelinePaths, proposal_id: str) -> dict[str, Any]:
    state = load_approval_state(paths)
    return state.get("rejection_cooloff", {}).get(proposal_id, {})


def is_in_rejection_cooloff(paths: PipelinePaths, proposal_id: str) -> bool:
    state = load_approval_state(paths)
    return proposal_id in state.get("rejection_cooloff", {})


def increment_rejection_cooloff_cycle(paths: PipelinePaths, proposal_id: str) -> int:
    state = load_approval_state(paths)
    cooloff = state.get("rejection_cooloff", {})
    if proposal_id not in cooloff:
        return 0
    cooloff[proposal_id]["cycle_count"] += 1
    write_json(state, paths.approval_state)
    return int(cooloff[proposal_id]["cycle_count"])


def expire_rejection_cooloff(paths: PipelinePaths, proposal_id: str) -> None:
    state = load_approval_state(paths)
    cooloff = state.get("rejection_cooloff", {})
    if proposal_id in cooloff:
        del cooloff[proposal_id]
        write_json(state, paths.approval_state)
```

### 4.5 Updated evaluation loop in `plan_pipeline_spec` (`planner.py`)

**Imports to add:**

```python
from pipeline.agent.approval import (
    # ... existing imports ...
    expire_rejection_cooloff,
    get_rejection_cooloff_record,
    increment_rejection_cooloff_cycle,
    is_in_rejection_cooloff,
    start_rejection_cooloff,
)
from pipeline.agent.autonomy import (
    # ... existing imports ...
    get_rejection_cooloff_policy,
)
```

**Before the proposal evaluation loop** (after compiling `proposals_to_evaluate`):

```python
cooloff_policy = get_rejection_cooloff_policy(paths)
```

**At the start of each proposal's iteration** (before `build_candidate_actions`, before any gate evaluation):

```python
proposal_id = str(proposal["proposal_id"])
approval_status = approval_status_by_proposal[proposal_id]

if is_in_rejection_cooloff(paths, proposal_id):
    if approval_status != APPROVAL_STATUS_REJECTED:
        expire_rejection_cooloff(paths, proposal_id)
        # fall through to normal evaluation
    else:
        cooloff_record = get_rejection_cooloff_record(paths, proposal_id)
        new_count = increment_rejection_cooloff_cycle(paths, proposal_id)
        started_at = datetime.fromisoformat(cooloff_record["cooloff_started_at_utc"])
        elapsed_hours = (datetime.now(UTC) - started_at).total_seconds() / 3600
        if (
            new_count >= cooloff_policy["threshold_cycles"]
            or elapsed_hours >= cooloff_policy["threshold_hours"]
        ):
            expire_rejection_cooloff(paths, proposal_id)
            # fall through to normal evaluation (re-evaluation pass)
        else:
            proposal["status"] = PROPOSAL_STATUS_CLOSED_NO_ACTION
            proposal["decision_reason"] = "rejected_cooloff_active"
            persist_proposal_record(paths, proposal)
            continue
```

**In the rejection branch** (after `elif approval_status == APPROVAL_STATUS_REJECTED:`):

```python
elif approval_status == APPROVAL_STATUS_REJECTED:
    proposal["status"] = PROPOSAL_STATUS_REJECTED
    decision = DECISION_REJECT
    decision_reason = "Proposal was explicitly rejected."
    start_rejection_cooloff(paths, proposal_id)
```

### 4.6 `approval_state.json` schema — `rejection_cooloff` section

```json
{
  "approved_proposals": {},
  "proposal_decisions": {},
  "rejection_cooloff": {
    "proposal_<fingerprint>": {
      "cooloff_started_at_utc": "2026-04-29T14:00:00+00:00",
      "cycle_count": 3
    }
  }
}
```

| Field | Type | Description |
|---|---|---|
| `cooloff_started_at_utc` | ISO-8601 UTC string | Timestamp when the current cooloff window started (set or reset by `start_rejection_cooloff`) |
| `cycle_count` | int | Number of planning cycles the proposal has been suppressed in the current window |

### 4.7 Autonomy policy — new top-level fields

| Field | Type | Default | Description |
|---|---|---|---|
| `rejection_cooloff_threshold_cycles` | int | `10` | Max suppressed cycles before window expires |
| `rejection_cooloff_threshold_hours` | float | `24.0` | Max elapsed hours since cooloff start before window expires |

---

## 5. Acceptance Criteria

- **AC-001**: Given a proposal is explicitly rejected for the first time, when the rejection branch is reached, then `start_rejection_cooloff(paths, proposal_id)` MUST be called and a cooloff record with `cycle_count=0` MUST appear in `approval_state.json["rejection_cooloff"]`.
- **AC-002**: Given a proposal is in cooloff with `cycle_count=3` and the window has not expired, when a planning cycle evaluates it, then the proposal MUST be marked `PROPOSAL_STATUS_CLOSED_NO_ACTION` with `decision_reason="rejected_cooloff_active"` and `cycle_count` in `approval_state.json` MUST equal `4`.
- **AC-003**: Given a proposal is in cooloff and `new_count >= cooloff_threshold_cycles`, when the planning cycle evaluates it, then `expire_rejection_cooloff` MUST be called, the cooloff entry MUST be removed from `approval_state.json`, and the proposal MUST proceed to normal evaluation.
- **AC-004**: Given a proposal is in cooloff and `elapsed_hours >= cooloff_threshold_hours` (even if `cycle_count < threshold_cycles`), when the planning cycle evaluates it, then `expire_rejection_cooloff` MUST be called and the proposal MUST proceed to normal evaluation.
- **AC-005**: Given a proposal is in cooloff and the operator has since approved it (`approval_status != APPROVAL_STATUS_REJECTED`), when the planning cycle evaluates it, then `expire_rejection_cooloff` MUST be called without incrementing `cycle_count`, and the proposal MUST proceed to the normal promotion path.
- **AC-006**: Given a proposal completes a re-evaluation pass and is found rejected again, when the rejection branch is reached, then `start_rejection_cooloff` MUST be called, resetting `cooloff_started_at_utc` and `cycle_count=0` (fresh window).
- **AC-007**: Given a proposal is suppressed by cooloff (`rejected_cooloff_active`), when the planning cycle completes, then `update_autonomy_metrics`, `persist_autonomy_decision`, and gate evaluation functions MUST NOT have been called for that proposal.
- **AC-008**: Given `cooloff_threshold_cycles=10` and `cooloff_threshold_hours=24.0`, when `start_rejection_cooloff` is called twice for the same `proposal_id`, then the second call MUST overwrite the first, resulting in `cycle_count=0` and an updated `cooloff_started_at_utc`.
- **AC-009**: Given `expire_rejection_cooloff` is called for a `proposal_id` that has no cooloff record, then the function MUST complete without error and MUST NOT mutate `approval_state.json`.
- **AC-010**: Given `increment_rejection_cooloff_cycle` is called for a `proposal_id` that has no cooloff record, then the function MUST return `0` without error and MUST NOT mutate `approval_state.json`.
- **AC-011**: Given custom policy values `{"rejection_cooloff_threshold_cycles": 5, "rejection_cooloff_threshold_hours": 12.0}` in `autonomy_policy.json`, when `get_rejection_cooloff_policy(paths)` is called, then it MUST return `{"threshold_cycles": 5, "threshold_hours": 12.0}`.
- **AC-012**: Given no `autonomy_policy.json` exists, when `get_rejection_cooloff_policy(paths)` is called, then it MUST return `{"threshold_cycles": 10, "threshold_hours": 24.0}`.
- **AC-013**: Given a proposal is in cooloff and the planning run processes it, when `persist_proposal_record` is called, then the proposal record MUST contain `"status": "closed_no_action"` and `"decision_reason": "rejected_cooloff_active"`.

---

## 6. Test Automation Strategy

- **Test Levels**: Unit — all logic is deterministic and testable with mocked I/O and mocked `datetime.now`.
- **Frameworks**: `pytest`, `unittest.mock.patch`.
- **Test file**: `tests/test_proposal_rejection_cooloff.py` (new).

### Required test cases

| Test | Description |
|---|---|
| `test_start_cooloff_creates_record` | No existing entry → entry created with `cycle_count=0` |
| `test_start_cooloff_resets_existing_record` | Existing entry with `cycle_count=5` → overwritten with `cycle_count=0`, fresh timestamp |
| `test_is_in_cooloff_true_when_entry_exists` | Entry present → returns `True` |
| `test_is_in_cooloff_false_when_no_entry` | No entry → returns `False` |
| `test_increment_cooloff_cycle_increments_count` | Entry with `cycle_count=3` → returns `4`, state persisted |
| `test_increment_cooloff_cycle_noop_when_missing` | No entry → returns `0`, no state mutation |
| `test_expire_cooloff_removes_entry` | Entry present → removed, state persisted |
| `test_expire_cooloff_noop_when_missing` | No entry → no error, no state mutation |
| `test_cooloff_suppresses_proposal_within_window` | `cycle_count=5 < threshold=10`, time < 24h → `CLOSED_NO_ACTION`, no gate evaluation |
| `test_cooloff_expires_on_cycle_threshold` | `new_count=10 >= threshold=10` → expire called, proposal evaluated |
| `test_cooloff_expires_on_time_threshold` | `elapsed_hours >= 24.0` even if `cycle_count < 10` → expire called, proposal evaluated |
| `test_cooloff_cleared_when_operator_approves` | In cooloff but `approval_status=APPROVED` → expire called, promotion path |
| `test_cooloff_started_after_rejection_branch` | Rejection branch executed → `start_rejection_cooloff` called |
| `test_fresh_cooloff_after_reevaluation_rejection` | Re-evaluation pass → rejected again → new cooloff started with `cycle_count=0` |
| `test_get_rejection_cooloff_policy_defaults` | No policy file → `{threshold_cycles: 10, threshold_hours: 24.0}` |
| `test_get_rejection_cooloff_policy_custom` | Custom policy → values read from file |
| `test_metrics_not_updated_for_suppressed_proposal` | Suppressed proposal → `update_autonomy_metrics` NOT called |

### CI/CD

- `venv/bin/python -m pytest tests/test_proposal_rejection_cooloff.py -q` MUST pass.
- `venv/bin/python -m pytest -q` (full suite) MUST continue passing without regressions.
- Tests MUST NOT require external LLM calls or real file I/O — all path-based helpers MUST be mocked.

---

## 7. Rationale & Context

In `plan_pipeline_spec`, the `approval_status_by_proposal` dict is built from `get_proposal_approval_status()` before the evaluation loop. When a proposal was previously rejected, the next cycle finds `APPROVAL_STATUS_REJECTED` and transitions the proposal to `PROPOSAL_STATUS_REJECTED` again. Since the detectors run independently of rejection history, the same proposal is re-generated, re-evaluated, and re-rejected on every cycle the pattern persists.

The consequence is metric noise: `approval_required_count_total` inflates on every cycle for proposals that will never be approved in their current form, and `unresolved_failure_count_total` may also accumulate. The signal is degraded — operators cannot distinguish genuine new issues from persistent rejected ones.

The cooling-off window resolves this without changing the governance contract:

1. After an explicit rejection, the proposal is suppressed for up to 10 cycles or 24 hours. This period is intended to reflect the realistic latency of operator feedback loops — if the operator rejected a proposal, they are unlikely to reverse the decision within a few minutes.
2. After the window, the proposal re-surfaces exactly once. This allows the operator to reconsider if the underlying data pattern has persisted long enough to warrant a second review, or if the operator's judgment has changed.
3. If still rejected, the window resets. The cycle continues at low cost (one re-evaluation per window) rather than one re-evaluation per cycle.
4. If the operator approves between cycles, the cooloff is cleared immediately on the next cycle and the proposal proceeds normally. The cooloff does not block approvals — it only suppresses re-evaluation of proposals that remain rejected.

The cooloff record is stored in `approval_state.json` rather than in the proposal record, because the cooloff is an approval-lifecycle concern: it tracks how the operator's decision is being respected, not the proposal's evaluation state. The `proposal_decisions` section already tracks the rejection; the `rejection_cooloff` section tracks its enforcement.

---

## 8. Dependencies & External Integrations

### External Systems
- **EXT-001**: None. The cooling-off mechanism is entirely internal to the pipeline agent.

### Infrastructure Dependencies
- **INF-001**: `state/autonomy/approval_state.json` — existing approval state file; the `rejection_cooloff` top-level key is added. Backward-compatible: the key defaults to `{}` if absent.
- **INF-002**: `state/autonomy/autonomy_policy.json` — existing policy file; two new top-level keys are read, falling back to defaults if absent.
- **INF-003**: `state/autonomy/proposals/<proposal_id>.json` — existing proposal record files; written for suppressed proposals with `PROPOSAL_STATUS_CLOSED_NO_ACTION`.

### Technology Platform Dependencies
- **PLT-001**: Python 3.11+ — already required by the repository; `datetime.fromisoformat` with timezone-aware ISO strings requires 3.11+.

---

## 9. Examples & Edge Cases

### Full cooloff lifecycle (threshold_cycles=10, threshold_hours=24.0)

```
Rejection cycle:
  approval_status=REJECTED → PROPOSAL_STATUS_REJECTED → start_rejection_cooloff()
  approval_state.json: rejection_cooloff[pid] = {cycle_count: 0, started_at: T0}

Cycles 1–9 (suppressed):
  is_in_cooloff=True, approval_status=REJECTED
  increment → count=1,2,...,9; elapsed < 24h → CLOSED_NO_ACTION (rejected_cooloff_active)

Cycle 10 (re-evaluation pass):
  is_in_cooloff=True, approval_status=REJECTED
  increment → count=10 >= threshold=10 → expire_rejection_cooloff()
  Proposal proceeds to normal evaluation
  approval_status=REJECTED → PROPOSAL_STATUS_REJECTED → start_rejection_cooloff()
  approval_state.json: rejection_cooloff[pid] = {cycle_count: 0, started_at: T1}
  (fresh window begins)
```

### Time-based expiry before cycle threshold

```
Rejection cycle: cooloff started at T0
Cycle 3: elapsed_hours = 25.1 >= 24.0 (threshold_hours) → expire_cooloff → re-evaluation pass
  (Even though cycle_count=3 < 10)
```

### Operator approves during cooloff

```
Rejection cycle: cooloff started, cycle_count=0
Cycle 3: operator runs approval CLI → approval_status=APPROVED
Cycle 4 (planning run):
  is_in_cooloff=True
  approval_status=APPROVED != REJECTED → expire_rejection_cooloff()
  Proposal proceeds to normal evaluation → PROMOTED
```

### Custom policy via `autonomy_policy.json`

```json
{
  "rejection_cooloff_threshold_cycles": 5,
  "rejection_cooloff_threshold_hours": 12.0
}
```

Behaviour: window expires after 5 suppressed cycles or 12 hours, whichever comes first.

### Edge case: two different proposals rejected in the same cycle

Each `proposal_id` has its own independent cooloff record. Two rejections in cycle N produce two separate entries in `rejection_cooloff`. Each advances independently through subsequent cycles.

### Edge case: `approval_state.json` created fresh (no prior state)

`load_approval_state` returns `{"approved_proposals": {}, "proposal_decisions": {}, "rejection_cooloff": {}}`. `is_in_rejection_cooloff` returns `False` for all proposals. No cooloff suppression until the first explicit rejection.

### Edge case: `start_rejection_cooloff` called on a proposal that just expired its cooloff

Re-evaluation pass finds `approval_status=REJECTED` → `start_rejection_cooloff` called → entry recreated with `cycle_count=0`. The previous entry was removed by `expire_rejection_cooloff` earlier in the same cycle, so no stale data.

---

## 10. Validation Criteria

- **VAL-001**: `venv/bin/python -m pytest tests/test_proposal_rejection_cooloff.py -q` MUST pass with all scenarios in section 6.
- **VAL-002**: `venv/bin/python -m pytest -q` (full suite) MUST continue passing without regressions.
- **VAL-003**: `grep -n "rejection_cooloff_threshold_cycles\|rejection_cooloff_threshold_hours" src/pipeline/agent/autonomy.py` MUST return both keys in `DEFAULT_AUTONOMY_POLICY`.
- **VAL-004**: `grep -n "get_rejection_cooloff_policy" src/pipeline/agent/autonomy.py` MUST return the function definition.
- **VAL-005**: `grep -n "start_rejection_cooloff\|expire_rejection_cooloff\|is_in_rejection_cooloff\|increment_rejection_cooloff_cycle\|get_rejection_cooloff_record" src/pipeline/agent/approval.py` MUST return all five function definitions.
- **VAL-006**: `grep -n "rejected_cooloff_active" src/pipeline/agent/planner.py` MUST return at least one reference in the evaluation loop.
- **VAL-007**: `grep -n "start_rejection_cooloff" src/pipeline/agent/planner.py` MUST return at least one reference in the rejection branch.
- **VAL-008**: A manual end-to-end test with a mock Bronze source that always triggers the same rejected proposal MUST result in `status="closed_no_action"` for all cycles within the cooloff window, and `status="rejected"` only on the re-evaluation cycle.
- **VAL-009**: `grep -n "rejection_cooloff" src/pipeline/agent/approval.py` MUST appear in the `load_approval_state` default dict.

---

## 11. Related Specifications / Further Reading

- [docs/agent-autonomy-gaps.md](../docs/agent-autonomy-gaps.md) — Full autonomy gap catalogue; GAP-A04 is the source of this spec
- [spec-process-daemon-watchdog-exponential-backoff.md](./spec-process-daemon-watchdog-exponential-backoff.md) — GAP-A01 remediation (daemon resilience)
- [spec-process-planner-idle-cycle-gate.md](./spec-process-planner-idle-cycle-gate.md) — GAP-A02 remediation (planner idle-cycle gate)
- [spec-process-proposal-awaiting-approval-expiry.md](./spec-process-proposal-awaiting-approval-expiry.md) — GAP-A03 remediation (awaiting-approval stale resolution)
- [spec-architecture-agent-autonomy-governed-by-impact.md](./spec-architecture-agent-autonomy-governed-by-impact.md) — Original governance-by-impact architecture
