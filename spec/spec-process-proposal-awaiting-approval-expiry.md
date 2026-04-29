---
title: Proposal Awaiting-Approval Expiry and Stale Resolution (GAP-A03)
version: 1.0
date_created: 2026-04-29
owner: Data Engineering
tags: [process, agent, autonomy, governance, planner, gap-a03]
---

# Introduction

This specification defines the requirements to close GAP-A03 identified in `docs/agent-autonomy-gaps.md`. Proposals that transition to `awaiting_approval` currently stay in that state indefinitely: each daemon cycle re-evaluates the same proposal, finds it still pending, and does nothing. The pipeline spec never evolves in the direction the planner detected, regardless of how long the pattern persists.

The fix introduces a `awaiting_approval_cycle_count` field in the proposal record. On each planning run, proposals already in `awaiting_approval` have their counter incremented. After a configurable stale threshold is reached, the agent triggers a secondary self-review with a progressively reduced confidence threshold (floored at a minimum). When the floor is reached and the secondary review still fails, the proposal is marked `stale` and closed. This prevents indefinite blocking while preserving the governance contract for genuinely risky changes.

---

## 1. Purpose & Scope

**Purpose:** Eliminate the infinite-hold failure mode in `awaiting_approval`. Proposals that the planner cannot self-approve, and that no operator has approved or rejected, must eventually self-resolve — either by succeeding via a reduced-threshold secondary review or by being closed as stale.

**Scope:** `src/pipeline/agent/planner.py` and `src/pipeline/agent/autonomy.py`. No changes to the Bronze, Silver, or Gold transformation layers, daemon loop, or approval CLI tooling.

**Files affected:**
- `src/pipeline/agent/autonomy.py` — new constant, new policy fields, new helper functions
- `src/pipeline/agent/planner.py` — stale resolution logic inserted into the `awaiting_approval` branch

**Audience:** Engineers implementing the fix and PR reviewers.

---

## 2. Definitions

| Term | Definition |
|---|---|
| `awaiting_approval` | Proposal status set when all deterministic gates pass but explicit human (or agent) approval has not been granted |
| `stale` | New proposal status indicating the proposal has waited beyond the stale threshold and failed all secondary reviews up to the confidence floor; the proposal is closed with no action |
| Stale threshold | Configurable number of consecutive `awaiting_approval` cycles before secondary review begins |
| `awaiting_approval_cycle_count` | Integer field in the proposal record tracking how many consecutive cycles the proposal has been in `awaiting_approval` |
| Secondary self-review | A second call to `agent_self_review_proposal` triggered when `cycle_count >= stale_threshold`, using a confidence threshold reduced by `confidence_reduction_per_cycle` per cycle past the threshold |
| Confidence floor | Minimum confidence threshold that the secondary review will use; approvals at this level or lower are considered the last opportunity before stale |
| Base threshold | The `agent_auto_approve_if_confidence_ge` value from the proposal family's autonomy policy |
| Reduced threshold | `max(base_threshold - confidence_reduction_per_cycle * (cycle_count - stale_threshold + 1), confidence_floor)` |
| Planning run | One execution of `plan_pipeline_spec(paths)` |
| Proposal record | JSON file at `state/autonomy/proposals/<proposal_id>.json` persisted by `persist_proposal_record` |
| `approval_state.json` | File at `state/autonomy/approval_state.json` managed by `src/pipeline/agent/approval.py` |

---

## 3. Requirements, Constraints & Guidelines

### Cycle counting

- **REQ-001**: The proposal record schema MUST include an integer field `awaiting_approval_cycle_count`, defaulting to `0` for proposals that have never entered `awaiting_approval`.
- **REQ-002**: Each time a proposal is about to be persisted with status `PROPOSAL_STATUS_AWAITING_APPROVAL`, `awaiting_approval_cycle_count` MUST be incremented by 1 relative to the value read from the existing record on disk (or from `0` if no record exists yet).
- **REQ-003**: When a proposal transitions OUT of `awaiting_approval` (approved, stale, or rejected), its final persisted record MUST retain the last `awaiting_approval_cycle_count` value for audit purposes.

### Stale threshold and secondary review

- **REQ-004**: When `awaiting_approval_cycle_count >= awaiting_approval_stale_threshold_cycles`, the planner MUST attempt a secondary self-review before deciding the final status of that cycle.
- **REQ-005**: The secondary self-review MUST use `agent_self_review_proposal` with the `reduced_threshold` computed as `max(base_threshold - confidence_reduction_per_cycle * (cycle_count - stale_threshold + 1), confidence_floor)`.
- **REQ-006**: If the secondary self-review returns `should_approve=True` AND `confidence >= reduced_threshold`, the planner MUST call `approve_proposal(paths, proposal_id, "agent_secondary_review")` and continue to the standard promotion path.
- **REQ-007**: If the secondary self-review returns `should_approve=False` OR `confidence < reduced_threshold`, AND `reduced_threshold <= confidence_floor`, the planner MUST set the proposal status to `PROPOSAL_STATUS_STALE` and persist the record. No further evaluation occurs for that proposal in the current cycle.
- **REQ-008**: If the secondary self-review fails AND `reduced_threshold > confidence_floor`, the proposal MUST remain in `PROPOSAL_STATUS_AWAITING_APPROVAL` with the updated `awaiting_approval_cycle_count`, and await the next cycle's evaluation.
- **REQ-009**: For proposals with no `agent_auto_approve_if_confidence_ge` defined in their family policy, when `cycle_count >= stale_threshold`, the planner MUST immediately set status to `PROPOSAL_STATUS_STALE` without attempting a secondary review (no threshold means secondary review is not possible).

### New constant and policy

- **REQ-010**: `PROPOSAL_STATUS_STALE = "stale"` MUST be added as a module-level constant in `autonomy.py` alongside the existing `PROPOSAL_STATUS_*` constants.
- **REQ-011**: `DECISION_STALE = "stale"` MUST be added as a module-level constant in `autonomy.py` alongside the existing `DECISION_*` constants.
- **REQ-012**: The three policy fields below MUST be added as top-level keys in `DEFAULT_AUTONOMY_POLICY` in `autonomy.py` (not inside any mutation family sub-dict):
  - `"awaiting_approval_stale_threshold_cycles": 5`
  - `"awaiting_approval_confidence_reduction_per_cycle": 0.05`
  - `"awaiting_approval_confidence_floor": 0.70`
- **REQ-013**: `load_autonomy_policy` already reads from `autonomy_policy.json` on disk; these new keys MUST be respected when present in the file, falling back to the defaults when absent (existing `read_json(…, default=…)` semantics already handle this).

### New helper in `autonomy.py`

- **REQ-014**: A new function `load_proposal_record(paths: PipelinePaths, proposal_id: str) -> dict[str, Any]` MUST be added to `autonomy.py`. It MUST return the parsed JSON from `proposal_record_path(paths, proposal_id)` if the file exists, or `{}` if it does not.
- **REQ-015**: A new function `get_awaiting_approval_stale_policy(paths: PipelinePaths) -> dict[str, Any]` MUST be added to `autonomy.py`. It MUST read the three stale-policy keys from `load_autonomy_policy` and return a dict with the keys `stale_threshold_cycles`, `confidence_reduction_per_cycle`, and `confidence_floor`, using defaults from `DEFAULT_AUTONOMY_POLICY` if absent.

### Metrics and decisions

- **REQ-016**: When a proposal is marked `PROPOSAL_STATUS_STALE`, `persist_autonomy_decision` MUST be called with `"decision": DECISION_STALE` and `"decision_reason": "awaiting_approval_expired_at_confidence_floor"`.
- **REQ-017**: When a proposal is marked `PROPOSAL_STATUS_STALE`, `update_autonomy_metrics` MUST be called with `unresolved_failure=True` for the proposal's family (same flag used for `PROPOSAL_STATUS_VALIDATION_FAILED`).
- **REQ-018**: The summary `status_counts` dict in the plan report MUST include a count for `PROPOSAL_STATUS_STALE`.

### Constraints

- **CON-001**: The `awaiting_approval_cycle_count` field MUST NOT be set or mutated by `_build_proposal`. It is a lifecycle field managed only by the stale-resolution logic in `plan_pipeline_spec`.
- **CON-002**: The stale logic MUST NOT alter the `proposal_id` fingerprint calculation. The fingerprint is content-derived and MUST remain stable across cycles for the same detected pattern.
- **CON-003**: The secondary review MUST reuse `agent_self_review_proposal` without modification. The reduced threshold is applied only to the comparison, not passed into the LLM call itself.
- **CON-004**: The stale mechanism MUST NOT affect proposals already in `APPROVAL_STATUS_APPROVED` or `APPROVAL_STATUS_REJECTED` — those are handled by existing paths and bypass the `awaiting_approval` branch entirely.
- **CON-005**: `PROPOSAL_STATUS_STALE` MUST be treated as a terminal status: once persisted, the proposal is never re-opened. A subsequent cycle detecting the same pattern generates a fresh proposal with `awaiting_approval_cycle_count=0`.

### Guidelines

- **GUD-001**: The `load_proposal_record` call MUST occur only inside the `awaiting_approval` branch (i.e., only when `gate_passed and proposal["requires_approval"] and approval_status != APPROVAL_STATUS_APPROVED`). Avoid loading proposal records unconditionally for every proposal.
- **GUD-002**: The stale policy values MUST be read once per proposal evaluation via `get_awaiting_approval_stale_policy`, not re-loaded on every cycle to avoid redundant I/O.

---

## 4. Interfaces & Data Contracts

### 4.1 New constants in `autonomy.py`

```python
PROPOSAL_STATUS_STALE = "stale"
DECISION_STALE = "stale"
```

### 4.2 New top-level keys in `DEFAULT_AUTONOMY_POLICY`

```python
DEFAULT_AUTONOMY_POLICY: dict[str, Any] = {
    # existing "mutation_families": { ... },
    "awaiting_approval_stale_threshold_cycles": 5,
    "awaiting_approval_confidence_reduction_per_cycle": 0.05,
    "awaiting_approval_confidence_floor": 0.70,
}
```

### 4.3 New helpers in `autonomy.py`

```python
def load_proposal_record(paths: PipelinePaths, proposal_id: str) -> dict[str, Any]:
    path = proposal_record_path(paths, proposal_id)
    if not path.exists():
        return {}
    return read_json(path, default={})


def get_awaiting_approval_stale_policy(paths: PipelinePaths) -> dict[str, Any]:
    policy = load_autonomy_policy(paths)
    return {
        "stale_threshold_cycles": int(
            policy.get("awaiting_approval_stale_threshold_cycles", 5)
        ),
        "confidence_reduction_per_cycle": float(
            policy.get("awaiting_approval_confidence_reduction_per_cycle", 0.05)
        ),
        "confidence_floor": float(
            policy.get("awaiting_approval_confidence_floor", 0.70)
        ),
    }
```

### 4.4 Updated `awaiting_approval` branch in `plan_pipeline_spec` (`planner.py`)

The current block (lines ~700–710):

```python
if (
    gate_passed
    and proposal["requires_approval"]
    and approval_status != APPROVAL_STATUS_APPROVED
):
    proposal["status"] = PROPOSAL_STATUS_AWAITING_APPROVAL
    decision = DECISION_HOLD_FOR_APPROVAL
    decision_reason = "Impact-governed policy requires explicit approval."
```

MUST be replaced by:

```python
if (
    gate_passed
    and proposal["requires_approval"]
    and approval_status != APPROVAL_STATUS_APPROVED
):
    existing_record = load_proposal_record(paths, proposal_id)
    cycle_count = int(existing_record.get("awaiting_approval_cycle_count", 0)) + 1
    proposal["awaiting_approval_cycle_count"] = cycle_count

    stale_policy = get_awaiting_approval_stale_policy(paths)
    stale_threshold = stale_policy["stale_threshold_cycles"]
    reduction = stale_policy["confidence_reduction_per_cycle"]
    floor = stale_policy["confidence_floor"]

    if cycle_count >= stale_threshold:
        base_threshold = get_agent_auto_approve_threshold(paths, str(proposal["proposal_family"]))
        if base_threshold is None:
            proposal["status"] = PROPOSAL_STATUS_STALE
            decision = DECISION_STALE
            decision_reason = "awaiting_approval_expired_no_auto_approve_threshold"
        else:
            reduced = max(base_threshold - reduction * (cycle_count - stale_threshold + 1), floor)
            review = agent_self_review_proposal(proposal, gate_results, diff, compiled_plan)
            if review["should_approve"] and float(review["confidence"]) >= reduced:
                approve_proposal(paths, proposal_id, "agent_secondary_review")
                approval_status = APPROVAL_STATUS_APPROVED
                proposal["approval_context"] = get_proposal_approval_record(paths, proposal_id)
                # fall through to promotion path below
            elif reduced <= floor:
                proposal["status"] = PROPOSAL_STATUS_STALE
                decision = DECISION_STALE
                decision_reason = "awaiting_approval_expired_at_confidence_floor"
            else:
                proposal["status"] = PROPOSAL_STATUS_AWAITING_APPROVAL
                decision = DECISION_HOLD_FOR_APPROVAL
                decision_reason = "Impact-governed policy requires explicit approval."
    else:
        proposal["status"] = PROPOSAL_STATUS_AWAITING_APPROVAL
        decision = DECISION_HOLD_FOR_APPROVAL
        decision_reason = "Impact-governed policy requires explicit approval."
```

### 4.5 Proposal record schema — new field

| Field | Type | Default | Description |
|---|---|---|---|
| `awaiting_approval_cycle_count` | `int` | `0` | Number of consecutive cycles the proposal has been in `awaiting_approval`. Set by stale-resolution logic; absent from fresh proposals. |

### 4.6 Autonomy policy schema — new top-level fields

| Field | Type | Default | Description |
|---|---|---|---|
| `awaiting_approval_stale_threshold_cycles` | `int` | `5` | Cycles in `awaiting_approval` before secondary review begins |
| `awaiting_approval_confidence_reduction_per_cycle` | `float` | `0.05` | Confidence threshold reduction per cycle past the stale threshold |
| `awaiting_approval_confidence_floor` | `float` | `0.70` | Minimum confidence threshold; reaching it with a failed review marks the proposal stale |

### 4.7 Reduced threshold formula

```
reduced_threshold = max(
    base_threshold - confidence_reduction_per_cycle * (cycle_count - stale_threshold + 1),
    confidence_floor
)
```

### 4.8 Plan report — updated `status_counts`

```python
"status_counts": {
    # ... existing entries ...
    PROPOSAL_STATUS_STALE: sum(
        1 for proposal in all_proposals
        if proposal["status"] == PROPOSAL_STATUS_STALE
    ),
}
```

---

## 5. Acceptance Criteria

- **AC-001**: Given a proposal enters `awaiting_approval` for the first time, when the planning run persists the proposal record, then `awaiting_approval_cycle_count` MUST equal `1`.
- **AC-002**: Given a proposal has `awaiting_approval_cycle_count=3` on disk and enters `awaiting_approval` again, when the record is persisted, then `awaiting_approval_cycle_count` MUST equal `4`.
- **AC-003**: Given `stale_threshold_cycles=5`, `base_threshold=0.90`, `confidence_reduction_per_cycle=0.05`, `confidence_floor=0.70`, and `cycle_count=5`, when secondary review is triggered, then `reduced_threshold` MUST equal `max(0.90 - 0.05*1, 0.70) = 0.85`.
- **AC-004**: Given `cycle_count=8` with the same policy values (base=0.90), when secondary review is triggered, then `reduced_threshold` MUST equal `max(0.90 - 0.05*4, 0.70) = 0.70 = floor`.
- **AC-005**: Given `cycle_count=8`, `reduced_threshold=0.70` (floor), and secondary review returns `confidence=0.65`, when the result is evaluated, then the proposal MUST be marked `PROPOSAL_STATUS_STALE` and `DECISION_STALE` MUST be persisted.
- **AC-006**: Given `cycle_count=6`, `reduced_threshold=0.80`, and secondary review returns `confidence=0.82`, when the result is evaluated, then `approve_proposal(paths, proposal_id, "agent_secondary_review")` MUST be called and the proposal MUST proceed to the promotion path.
- **AC-007**: Given `cycle_count=6`, `reduced_threshold=0.80` (not at floor), and secondary review returns `confidence=0.70`, when the result is evaluated, then the proposal MUST remain `PROPOSAL_STATUS_AWAITING_APPROVAL` with `awaiting_approval_cycle_count=6`.
- **AC-008**: Given a proposal family has no `agent_auto_approve_if_confidence_ge` in its policy and `cycle_count >= stale_threshold`, when the stale check runs, then the proposal MUST be immediately marked `PROPOSAL_STATUS_STALE` without calling `agent_self_review_proposal`.
- **AC-009**: Given a proposal is manually approved (`approval_status=APPROVAL_STATUS_APPROVED`) before `cycle_count` reaches the stale threshold, when the planning run evaluates it, then the stale logic MUST NOT run — the proposal MUST proceed directly to the promotion path.
- **AC-010**: Given a proposal is marked `PROPOSAL_STATUS_STALE` and persisted, when the next planning run detects the same pattern and builds a new proposal with the same `proposal_id`, then `awaiting_approval_cycle_count` MUST start at `1` (the stale record's count is not inherited — a fresh proposal record is written over it on the first `awaiting_approval` cycle).
- **AC-011**: Given a stale proposal, when `persist_autonomy_decision` is called, then `decision` MUST equal `"stale"` and `decision_reason` MUST be one of `"awaiting_approval_expired_at_confidence_floor"` or `"awaiting_approval_expired_no_auto_approve_threshold"`.
- **AC-012**: Given a stale proposal, when `update_autonomy_metrics` is called, then `unresolved_failure=True` MUST be passed for the proposal's family.
- **AC-013**: The plan report `summary.status_counts` MUST include a key `"stale"` with the count of stale proposals in the run.

---

## 6. Test Automation Strategy

- **Test Levels**: Unit — all logic is deterministic and testable with mocked I/O.
- **Frameworks**: `pytest`, `unittest.mock.patch`.
- **Test file**: `tests/test_proposal_stale_expiry.py` (new).

### Required test cases

| Test | Description |
|---|---|
| `test_cycle_count_initialized_on_first_awaiting` | No existing record → count written as 1 |
| `test_cycle_count_incremented_from_existing_record` | Existing record with count=3 → written as 4 |
| `test_reduced_threshold_formula_at_stale_threshold` | cycle=5, base=0.90 → reduced=0.85 |
| `test_reduced_threshold_formula_at_floor` | cycle=8, base=0.90 → reduced=0.70 (floor) |
| `test_stale_marked_when_floor_reached_and_review_fails` | reduced=floor, review confidence below → STALE |
| `test_stale_not_marked_when_above_floor_and_review_fails` | reduced>floor, review fails → AWAITING_APPROVAL |
| `test_secondary_review_approves_and_promotes` | review confidence >= reduced → approve called, promotion path |
| `test_no_threshold_marks_stale_immediately` | Family has no auto-approve threshold → STALE at stale_threshold |
| `test_manual_approval_bypasses_stale` | approval_status=APPROVED → stale logic not entered |
| `test_decision_persisted_with_stale_decision` | STALE path → `decision="stale"` in autonomy decision |
| `test_metrics_unresolved_failure_on_stale` | STALE path → `update_autonomy_metrics(unresolved_failure=True)` |
| `test_load_proposal_record_missing_file` | No file → returns `{}` |
| `test_load_proposal_record_existing_file` | File exists → returns parsed dict |
| `test_get_awaiting_approval_stale_policy_defaults` | No policy file → returns defaults (5, 0.05, 0.70) |
| `test_get_awaiting_approval_stale_policy_custom` | Policy file with custom values → returns those values |

### CI/CD

- `venv/bin/python -m pytest tests/test_proposal_stale_expiry.py -q` MUST pass.
- `venv/bin/python -m pytest -q` (full suite) MUST continue passing without regressions.
- Tests MUST NOT require external LLM calls — `agent_self_review_proposal` MUST be mocked.

---

## 7. Rationale & Context

The current `plan_pipeline_spec` sets `PROPOSAL_STATUS_AWAITING_APPROVAL` unconditionally whenever a proposal passes gates but lacks approval. On the next cycle, `_build_proposal` creates a fresh proposal object with `status=PROPOSAL_STATUS_PROPOSED`; the loop re-evaluates it; gates pass again; approval is still pending; status is set to `awaiting_approval` again. This repeats indefinitely.

The consequence is that real data patterns — a new segmentation class, a persisting schema drift, a consistent metadata key normalization gap — are observed by the planner every cycle but never acted upon. The autonomy metrics inflate (`approval_required_count_total` grows), while the spec stays frozen.

The stale mechanism resolves this without bypassing governance:

1. Proposals that do not gather approval within the threshold window get a secondary chance at a reduced confidence bar. This acknowledges that the pattern has been consistently confirmed across multiple planning runs, making the risk of a false positive lower.
2. If the agent still cannot reach confidence (or no auto-approve threshold exists), the proposal is closed as stale. This is the correct outcome: the pattern is real, the agent cannot safely act autonomously, and the proposal is not going to be approved. Closing it cleanly prevents metric noise and allows the next occurrence of the pattern to be evaluated fresh.
3. The stale mechanism does NOT prevent re-detection. If the same pattern persists after a proposal is closed as stale, the next planning run generates a new proposal with `awaiting_approval_cycle_count=0`. The operator can still intervene at any point by manually approving the proposal before it becomes stale.

---

## 8. Dependencies & External Integrations

### External Systems
- **EXT-001**: None. The stale mechanism is entirely internal to the pipeline agent.

### Infrastructure Dependencies
- **INF-001**: `state/autonomy/proposals/<proposal_id>.json` — existing proposal record files; the stale mechanism reads these to recover `awaiting_approval_cycle_count`.
- **INF-002**: `state/autonomy/autonomy_policy.json` — existing policy file; the three new top-level fields are read from here, falling back to defaults if absent.

### Technology Platform Dependencies
- **PLT-001**: Python 3.11+ — already required by the repository.

---

## 9. Examples & Edge Cases

### Full stale lifecycle for `schema_update` (base=0.90, floor=0.70, threshold=5)

```
Cycle 1: count=1 < threshold → AWAITING_APPROVAL (no secondary review)
Cycle 2: count=2 < threshold → AWAITING_APPROVAL
Cycle 3: count=3 < threshold → AWAITING_APPROVAL
Cycle 4: count=4 < threshold → AWAITING_APPROVAL
Cycle 5: count=5 >= threshold → reduced=0.85 → secondary review → confidence=0.70 < 0.85 → AWAITING_APPROVAL (reduced > floor)
Cycle 6: count=6 >= threshold → reduced=0.80 → secondary review → confidence=0.72 < 0.80 → AWAITING_APPROVAL (reduced > floor)
Cycle 7: count=7 >= threshold → reduced=0.75 → secondary review → confidence=0.68 < 0.75 → AWAITING_APPROVAL (reduced > floor)
Cycle 8: count=8 >= threshold → reduced=0.70 = floor → secondary review → confidence=0.65 < 0.70 → STALE (floor reached, review failed)
```

### Secondary review succeeds before floor

```
Cycle 5: count=5 → reduced=0.85 → secondary review → confidence=0.87 >= 0.85 → APPROVED → PROMOTED
```

### Manual approval before stale threshold

```
Cycle 3: operator runs approval CLI → approval_status=APPROVED
Cycle 4: stale logic skipped (approval_status=APPROVED) → standard promotion path
```

### Family without auto-approve threshold (e.g., hypothetical future family)

```
Cycle 5: count=5 >= threshold → get_agent_auto_approve_threshold returns None → STALE immediately
```

### Custom policy values via `autonomy_policy.json`

```json
{
  "awaiting_approval_stale_threshold_cycles": 3,
  "awaiting_approval_confidence_reduction_per_cycle": 0.10,
  "awaiting_approval_confidence_floor": 0.65
}
```

With base=0.90:
```
Cycle 3: reduced = max(0.90 - 0.10*1, 0.65) = 0.80
Cycle 4: reduced = max(0.90 - 0.10*2, 0.65) = 0.70
Cycle 5: reduced = max(0.90 - 0.10*3, 0.65) = 0.65 = floor → if fails → STALE
```

### Edge case: proposal manually approved between cycles while count=4

If the operator approves between cycles 4 and 5, the next planning run finds `approval_status=APPROVED` and enters the promotion path directly. The `awaiting_approval_cycle_count=4` is retained in the record for audit but has no effect on the outcome.

### Edge case: stale proposal with same pattern in next cycle

After cycle 8 marks the proposal as `STALE`:
```
State on disk: { "status": "stale", "awaiting_approval_cycle_count": 8, ... }

Cycle 9: same pattern detected → _build_proposal creates fresh proposal (same proposal_id, status=PROPOSED)
          gates pass → approval pending → load_proposal_record returns stale record
          stale record has no "approved" field → get_proposal_approval_status returns PENDING
          count = 8 + 1 = 9 → already at threshold → secondary review with reduced = max(0.90 - 0.05*5, 0.70) = 0.65 → floored to 0.70
```

> **Important:** The stale record is overwritten on first persistence by the new cycle. The count continues from the stale record's value, not from 0. If the intent is to give a fresh start, operators must manually delete or reset the proposal record file.

---

## 10. Validation Criteria

- **VAL-001**: `venv/bin/python -m pytest tests/test_proposal_stale_expiry.py -q` MUST pass with all scenarios in section 6.
- **VAL-002**: `venv/bin/python -m pytest -q` (full suite) MUST continue passing without regressions.
- **VAL-003**: `grep -n "PROPOSAL_STATUS_STALE\|DECISION_STALE" src/pipeline/agent/autonomy.py` MUST return both constant definitions.
- **VAL-004**: `grep -n "awaiting_approval_stale_threshold_cycles" src/pipeline/agent/autonomy.py` MUST return the key in `DEFAULT_AUTONOMY_POLICY`.
- **VAL-005**: `grep -n "load_proposal_record\|get_awaiting_approval_stale_policy" src/pipeline/agent/autonomy.py` MUST return both function definitions.
- **VAL-006**: `grep -n "awaiting_approval_cycle_count" src/pipeline/agent/planner.py` MUST return at least one reference in the `awaiting_approval` branch.
- **VAL-007**: A manual end-to-end test with a mock Bronze source that always produces the same schema drift proposal MUST result in `status="stale"` in the plan report after `stale_threshold_cycles + (base_threshold - floor) / reduction` cycles.
- **VAL-008**: `grep -n "PROPOSAL_STATUS_STALE" src/pipeline/agent/planner.py` MUST appear in the plan report `status_counts` construction.

---

## 11. Related Specifications / Further Reading

- [docs/agent-autonomy-gaps.md](../docs/agent-autonomy-gaps.md) — Full autonomy gap catalogue; GAP-A03 is the source of this spec
- [spec-process-daemon-watchdog-exponential-backoff.md](./spec-process-daemon-watchdog-exponential-backoff.md) — GAP-A01 remediation (daemon resilience)
- [spec-process-planner-idle-cycle-gate.md](./spec-process-planner-idle-cycle-gate.md) — GAP-A02 remediation (planner idle-cycle gate)
- [spec-architecture-agent-autonomy-governed-by-impact.md](./spec-architecture-agent-autonomy-governed-by-impact.md) — Original governance-by-impact architecture
