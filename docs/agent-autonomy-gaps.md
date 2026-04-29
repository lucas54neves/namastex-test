# Agent Autonomy Gaps

_Last reviewed: 2026-04-28_

This document catalogues the autonomy gaps identified in the current agentic pipeline implementation, ordered by implementation priority. Each gap describes the current behavior, the failure mode it produces, and the recommended remediation path.

---

## GAP-A01 — Daemon has no watchdog or auto-restart

**Layer affected:** `scripts/run_pipeline_daemon.py`

**Current behavior:**
The daemon loop calls `run_pipeline()` inside `run_daemon()` but has no outer exception guard. Any unhandled exception raised outside the `run_cycle()` try/except block (e.g., a write error to a report file, an import failure, or a transient OS error) terminates the process permanently.

**Failure mode:**
The pipeline stops updating silently. The source fingerprint is never refreshed, Gold stales, and no alert is generated because the alert system itself only runs inside `run_cycle()`.

**Recommended fix:**
Wrap the `run_pipeline()` call inside `run_daemon()` with a retry loop using exponential backoff. On each unhandled exception, log the error, sleep `2^n` seconds (capped at a configurable maximum, e.g. 300 s), and retry. Reset the backoff counter on a successful cycle.

**Estimated complexity:** Low
**Autonomy impact:** High — pipeline can stop silently with no operator intervention path

---

## GAP-A02 — Planner runs on every cycle, including idle ones

**Layer affected:** `src/pipeline/orchestration/operator.py:run_cycle()` (line 118)

**Current behavior:**
`plan_pipeline_spec(paths)` is called unconditionally at the top of `run_cycle()`, before the source-fingerprint check that determines whether the cycle will actually execute. This means LLM advisor calls and all five detectors run even on cycles that terminate early with `skipped_no_source_change`.

**Failure mode:**
Unnecessary LLM tokens consumed on every idle cycle. At a 60-second poll interval, this can be dozens of planning calls per hour when the source is not changing, with no benefit.

**Recommended fix:**
Move the planner call to after the fingerprint check. Call `plan_pipeline_spec(paths)` only when `changed=True`, or alternatively on a separate cadence counter (e.g., once every N idle cycles) to detect slow-accumulating drift even without a source change.

**Estimated complexity:** Low
**Autonomy impact:** Medium — removes wasteful LLM calls and makes each planning run signal meaningful change

---

## GAP-A03 — Proposals in `awaiting_approval` never expire or self-resolve

**Layer affected:** `src/pipeline/agent/planner.py`, `src/pipeline/agent/autonomy.py`

**Current behavior:**
When a proposal requires human approval (`requires_approval=True`) and the agent self-review does not reach the confidence threshold, the proposal transitions to `awaiting_approval` and stays there indefinitely. Subsequent daemon cycles re-generate the same proposal (same fingerprint, same `proposal_id`), find it in `awaiting_approval`, and do nothing with it.

**Failure mode:**
The pipeline spec never evolves in the direction the planner detected. If the underlying data pattern persists (e.g., a new segmentation class consistently appears in the data), the agent keeps observing it but cannot act on it.

**Recommended fix:**
Add a `cycle_count` field to the proposal record. On each planning run, increment it for proposals in `awaiting_approval`. After a configurable threshold (e.g., 5 cycles), trigger a secondary self-review with the confidence threshold reduced by 0.05 per cycle (floor at 0.70). If approval is still not reached, mark the proposal as `stale` and close it. This prevents infinite blocking while still preserving governance intent for genuinely risky changes.

**Estimated complexity:** Medium
**Autonomy impact:** Medium — unblocks spec evolution without bypassing the governance contract

---

## GAP-A04 — No cooling-off for explicitly rejected proposals

**Layer affected:** `src/pipeline/agent/planner.py`, `src/pipeline/agent/approval.py`

**Current behavior:**
When a proposal is explicitly rejected (`APPROVAL_STATUS_REJECTED`), the approval state records the rejection. However, the planner's detectors run independently of the approval history. If the detected pattern persists in the data (e.g., metadata key normalization gaps still exist), the next cycle generates the exact same proposal with the same `proposal_id` fingerprint and finds it rejected again — repeating the evaluation loop with no outcome.

**Failure mode:**
Noise: every planning cycle re-evaluates already-rejected proposals, logs decisions for them, and generates entries in autonomy metrics that inflate `approval_required_count_total` and `unresolved_failure_count_total`.

**Recommended fix:**
Before generating a proposal, the planner should query `get_proposal_approval_status()` for the prospective `proposal_id`. If the status is `APPROVAL_STATUS_REJECTED`, suppress re-generation for a configurable cooling-off window (e.g., 10 cycles or 24 hours, whichever comes first). After the window expires, re-generate once to allow the operator to reconsider. Record the cooling-off state in `approval_state.json`.

**Estimated complexity:** Medium
**Autonomy impact:** Medium — reduces proposal noise and makes autonomy metrics meaningful

---

## GAP-A05 — Data quality drift is not detected or proposed

**Layer affected:** `src/pipeline/agent/planner.py`

**Current behavior:**
The planner's five detectors cover structural drift: new Bronze columns, new metadata fields, boolean coercion gaps, segmentation vocabulary gaps, and metadata key normalization issues. None of them observe runtime quality signals: null-rate increases, record-count drops, or distribution shifts in key categorical columns (e.g., `conversation_outcome`, `channel`).

**Failure mode:**
The pipeline can silently degrade in quality — e.g., the source starts delivering 30% more nulls in `message_body`, or `conversation_outcome` distribution shifts significantly — and the agent never proposes any remediation. The validation layer catches structural contract violations but not statistical drift.

**Recommended fix:**
Add a `_quality_drift_proposals` detector to the planner. On each planning run, compare current Bronze statistics (null rate per key column, record count, `conversation_outcome` value distribution) against the statistics stored from the last successful run in `state/pipeline_state.json`. If any metric deviates beyond a configurable threshold (e.g., null rate increases by more than 10 pp, or record count drops by more than 20%), generate a proposal of family `data_quality_drift` that triggers a Silver rebuild with quarantine escalation.

Store the baseline statistics snapshot in `pipeline_state.json` at the end of each successful run so the comparison has a stable anchor.

**Estimated complexity:** High
**Autonomy impact:** High — closes the most significant blind spot: the pipeline can currently succeed while quality silently deteriorates

---

## Summary Table

| Gap | Affected Module | Complexity | Autonomy Impact |
|-----|----------------|------------|-----------------|
| GAP-A01: No daemon watchdog | `run_pipeline_daemon.py` | Low | High |
| GAP-A02: Planner on idle cycles | `operator.py` | Low | Medium |
| GAP-A03: Stale `awaiting_approval` | `planner.py`, `autonomy.py` | Medium | Medium |
| GAP-A04: No cooling-off for rejections | `planner.py`, `approval.py` | Medium | Medium |
| GAP-A05: No quality drift detection | `planner.py` | High | High |

Recommended implementation order: GAP-A01 → GAP-A02 → GAP-A04 → GAP-A03 → GAP-A05.
