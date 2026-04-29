---
title: Quality Drift Detection Detector in the Planner (GAP-A05)
version: 1.0
date_created: 2026-04-29
owner: Data Engineering
tags: [process, agent, autonomy, planner, quality, drift, gap-a05]
---

# Introduction

This specification defines the requirements to close GAP-A05 identified in `docs/agent-autonomy-gaps.md`. The planner's five existing detectors cover structural drift only (new columns, new metadata fields, boolean coercion gaps, segmentation vocabulary gaps, metadata key normalization). None observe statistical quality signals: null-rate increases, record-count drops, or distribution shifts in key categorical columns.

The fix adds a sixth detector, `_quality_drift_proposals`, to `planner.py`. On each planning run, the detector computes a quality snapshot from the current Bronze source and compares it against a stored baseline in `state/pipeline_state.json`. Any metric that deviates beyond a configurable threshold generates a `data_quality_drift` proposal. At the end of every planning run, the snapshot is persisted as the new baseline so subsequent comparisons have a stable anchor.

---

## 1. Purpose & Scope

**Purpose:** Close the most significant blind spot in the agent: the pipeline can currently run to completion and report `status=success` while quality silently deteriorates (rising null rates, dropping record counts, shifting outcome distributions). The detector gives the agent the ability to observe and propose remediation for statistical quality drift.

**Scope:** `src/pipeline/agent/planner.py`, `src/pipeline/agent/autonomy.py`, and `src/pipeline/runtime/state.py`. No changes to Bronze, Silver, Gold transformation logic, validation rules, daemon loop, or existing detectors.

**Files affected:**
- `src/pipeline/runtime/state.py` — two new helper functions: `compute_quality_snapshot` and `load_quality_baseline`
- `src/pipeline/agent/autonomy.py` — new mutation family `data_quality_drift`; five new top-level policy fields in `DEFAULT_AUTONOMY_POLICY`; new helper `get_quality_drift_policy`; new `build_candidate_actions` branch; new `apply_proposal_to_spec` branch
- `src/pipeline/agent/planner.py` — new constant; new detector function `_quality_drift_proposals`; call in `plan_pipeline_spec`; baseline persistence at end of planning run

**Audience:** Engineers implementing the fix and PR reviewers.

---

## 2. Definitions

| Term | Definition |
|---|---|
| Quality snapshot | A dict containing `record_count` (int), `null_rates` (dict col → float fraction), and `distribution` (dict col → dict value → float fraction), computed from a Bronze DataFrame |
| Baseline | The quality snapshot stored at `state/pipeline_state.json` under the top-level key `"quality_baseline"` from the previous planning run |
| Null rate | Fraction of rows in a column where the value is `NaN` or `None`, computed as `df[col].isna().mean()` |
| Null rate drift | Increase in null rate between current and baseline, in percentage points (pp): `(current_null_rate - baseline_null_rate) * 100` |
| Record count drop | Decrease in Bronze record count relative to baseline: `(baseline_record_count - current_record_count) / baseline_record_count * 100` |
| Distribution shift | Per-value frequency change in a categorical column between current and baseline, in percentage points: `abs(current_freq - baseline_freq) * 100` |
| Drift trigger | A string identifier naming the specific drift detected (e.g., `"message_body_null_rate_increase"`, `"record_count_drop"`) |
| Proposal fingerprint | Stable SHA-1 hash computed from `proposal_type`, `proposal_family`, `proposed_change` (drift trigger identifiers only), and `items` (list of drift trigger strings) |
| `PROPOSAL_FAMILY_QUALITY_DRIFT` | Constant `"data_quality_drift"` identifying the new mutation family |
| `data_quality_drift_detected` | New proposal type name for quality drift proposals |
| Planning run | One execution of `plan_pipeline_spec(paths)` |
| Key null-rate columns | Columns monitored for null rate drift; configurable; default: `["message_body", "conversation_outcome", "channel", "timestamp"]` |
| Key distribution columns | Columns monitored for distribution shift; configurable; default: `["conversation_outcome", "channel"]` |

---

## 3. Requirements, Constraints & Guidelines

### Quality snapshot computation

- **REQ-001**: A new function `compute_quality_snapshot(bronze_df: pd.DataFrame, null_rate_cols: list[str], distribution_cols: list[str]) -> dict[str, Any]` MUST be added to `src/pipeline/runtime/state.py`. It MUST return a dict with:
  - `"recorded_at_utc"`: ISO-8601 UTC string
  - `"record_count"`: int — total rows in `bronze_df`
  - `"null_rates"`: dict mapping each column in `null_rate_cols` that exists in `bronze_df` to its null fraction (`float`)
  - `"distribution"`: dict mapping each column in `distribution_cols` that exists in `bronze_df` to a dict of `{value_str: frequency_float}` — normalized value counts excluding `NaN`, rounded to 6 decimal places
- **REQ-002**: Columns in `null_rate_cols` or `distribution_cols` that are absent from `bronze_df` MUST be silently skipped (not raise an error).

### Baseline persistence

- **REQ-003**: A new function `load_quality_baseline(paths: PipelinePaths) -> dict[str, Any] | None` MUST be added to `src/pipeline/runtime/state.py`. It MUST load `pipeline_state.json` via `load_pipeline_state(state_file(paths))` and return the value of `state.get("quality_baseline")`, which is `None` when the key is absent.
- **REQ-004**: At the end of `plan_pipeline_spec(paths)`, AFTER all detector results have been collected, proposals evaluated, and the plan report written, the function MUST call `compute_quality_snapshot` using the already-loaded `bronze_df` and the quality drift policy, then persist the resulting snapshot by:
  1. Loading the current state via `load_pipeline_state(state_path)` where `state_path = paths.state / "pipeline_state.json"`
  2. Setting `state["quality_baseline"] = snapshot`
  3. Calling `save_pipeline_state(state_path, state)`
- **REQ-005**: The baseline MUST be updated on every planning run regardless of whether drift proposals were generated. This ensures the baseline always tracks the most recently observed source.
- **REQ-006**: `state_path` used in REQ-004 MUST resolve to `paths.state / "pipeline_state.json"` — the same file managed by `operator.py` via `state_file(paths)`.

### Policy configuration

- **REQ-007**: Five new top-level keys MUST be added to `DEFAULT_AUTONOMY_POLICY` in `autonomy.py`:
  - `"quality_drift_null_rate_columns"`: `["message_body", "conversation_outcome", "channel", "timestamp"]`
  - `"quality_drift_distribution_columns"`: `["conversation_outcome", "channel"]`
  - `"quality_drift_null_rate_threshold_pp"`: `10.0`
  - `"quality_drift_record_count_drop_threshold_pct"`: `20.0`
  - `"quality_drift_distribution_shift_threshold_pp"`: `15.0`
- **REQ-008**: A new helper `get_quality_drift_policy(paths: PipelinePaths) -> dict[str, Any]` MUST be added to `autonomy.py`. It MUST read from `load_autonomy_policy(paths)` and return a dict with keys:
  - `"null_rate_columns"`: list[str]
  - `"distribution_columns"`: list[str]
  - `"null_rate_threshold_pp"`: float
  - `"record_count_drop_threshold_pct"`: float
  - `"distribution_shift_threshold_pp"`: float
  — using defaults from `DEFAULT_AUTONOMY_POLICY` when absent.

### New mutation family

- **REQ-009**: A new entry `"data_quality_drift"` MUST be added to `DEFAULT_AUTONOMY_POLICY["mutation_families"]` in `autonomy.py`:
  ```python
  "data_quality_drift": {
      "default_impact_class": IMPACT_HIGH,
      "auto_promote": False,
      "requires_approval": True,
      "requires_backward_compatibility": False,
      "requires_privacy_scan": False,
      "agent_auto_approve_if_confidence_ge": 0.85,
  }
  ```

### Drift detection logic

- **REQ-010**: A new function `_quality_drift_proposals(paths, planning_run_id, bronze_df, baseline, policy) -> tuple[list[dict], list[dict]]` MUST be added to `planner.py`. It returns `(contexts, proposals)`.
- **REQ-011**: When `baseline` is `None` (no prior planning run has saved a baseline yet), `_quality_drift_proposals` MUST return `([], [])` — no proposals generated, no error.
- **REQ-012**: The function MUST detect three drift types. Each is checked independently; multiple drift types may coexist in a single planning run and each generates a separate `drift_trigger` identifier in the items list of the same proposal:
  - **Null rate increase**: For each column `c` in `policy["null_rate_columns"]` present in `bronze_df` AND in `baseline["null_rates"]`, if `(current_null_rates[c] - baseline_null_rates[c]) * 100 >= policy["null_rate_threshold_pp"]`, add trigger `f"{c}_null_rate_increase"`.
  - **Record count drop**: If `baseline["record_count"] > 0` and `(baseline_record_count - current_record_count) / baseline_record_count * 100 >= policy["record_count_drop_threshold_pct"]`, add trigger `"record_count_drop"`.
  - **Distribution shift**: For each column `c` in `policy["distribution_columns"]` present in `bronze_df` AND in `baseline["distribution"]`, compute the union of value keys across current and baseline distributions. For each value key `v`, compute `abs(current_freq.get(v, 0.0) - baseline_freq.get(v, 0.0)) * 100`. If any value's shift >= `policy["distribution_shift_threshold_pp"]`, add trigger `f"{c}_distribution_shift"`.
- **REQ-013**: If no drift triggers are detected, `_quality_drift_proposals` MUST return `([], [])`.
- **REQ-014**: If at least one drift trigger is detected, `_quality_drift_proposals` MUST generate exactly one proposal of type `data_quality_drift_detected` using `_build_proposal`, with:
  - `proposal_type = "data_quality_drift_detected"`
  - `proposal_family = PROPOSAL_FAMILY_QUALITY_DRIFT`
  - `items = sorted(drift_triggers)` — the list of drift trigger strings
  - `proposed_change = {"target_path": "quality.drift_log", "operation": "record_quality_drift_event", "drift_triggers": sorted(drift_triggers)}` — contains ONLY stable trigger identifiers, NOT actual metric values, to ensure fingerprint stability
  - `context_detected` — built via `_build_context` containing the current metrics, baseline metrics, and deviations as `evidence`
  - `requires_approval = True`
  - `risk = "high"`
  - `impact_scope = "cross_layer"`
  - `affected_layers = ["bronze", "silver", "gold"]`
  - `privacy_impact = "none"`
- **REQ-015**: The detector MUST be registered as the sixth entry in the `detector_results` list in `plan_pipeline_spec`, called AFTER the five existing detectors.

### `apply_proposal_to_spec` extension

- **REQ-016**: A new branch for `proposal_type == "data_quality_drift_detected"` MUST be added to `apply_proposal_to_spec` in `autonomy.py`. It MUST:
  1. Access `updated.setdefault("quality", {}).setdefault("drift_log", [])`
  2. Append `{"drift_triggers": proposal.get("items", []), "recorded_at_utc": proposal.get("created_at_utc", "")}` to the list
  3. Return `(updated, True)`

### `build_candidate_actions` extension

- **REQ-017**: A new branch for `proposal_type == "data_quality_drift_detected"` MUST be added to `build_candidate_actions` in `autonomy.py`. It MUST return:
  ```python
  [{
      "action_id": "action_01",
      "action_kind": "drift_escalation",
      "target_path": "config/pipeline_spec.json",
      "target_selector": "quality.drift_log",
      "operation": "record_quality_drift_event",
      "payload": {"drift_triggers": proposal.get("items", [])},
      "reversible": True,
      "validation_scope": ["spec_validation"],
  }]
  ```

### Fingerprint stability

- **REQ-018**: The `proposed_change` dict passed to `_build_proposal` for drift proposals MUST contain ONLY `"target_path"`, `"operation"`, and `"drift_triggers"` (the sorted list of trigger names). It MUST NOT contain actual metric values (null rates, record counts, distributions). Actual values MUST be stored in `context_detected.evidence` only, which is not included in the fingerprint hash.
- **REQ-019**: `items` passed to `_build_proposal` MUST equal `sorted(drift_triggers)`. This ensures the same set of drifting metrics always produces the same `proposal_id`, regardless of the magnitude of the drift.

### Constraints

- **CON-001**: `compute_quality_snapshot` MUST be a pure function with no I/O side effects. It MUST accept a DataFrame and two column lists, and return a dict without reading or writing any files.
- **CON-002**: The baseline update (REQ-004) MUST occur after the plan report is written (`write_json(report, paths.monitoring / "latest_plan_report.json")`). The baseline update MUST NOT affect the plan report content.
- **CON-003**: Drift proposals MUST NOT trigger LLM calls in `_quality_drift_proposals`. All detection logic MUST be purely deterministic (pandas operations).
- **CON-004**: The existing five detectors MUST NOT be modified. `_quality_drift_proposals` is an additive sixth entry.
- **CON-005**: The `proposed_change` dict for drift proposals MUST NOT be modified after `_build_proposal` returns (it participates in the fingerprint and must remain stable).
- **CON-006**: If `pipeline_state.json` does not exist (first run), `load_quality_baseline` MUST return `None`. This is handled by the existing `load_pipeline_state` default `{"runs": []}` which lacks `"quality_baseline"`.

### Guidelines

- **GUD-001**: `get_quality_drift_policy` MUST be called once per `plan_pipeline_spec` invocation and passed to `_quality_drift_proposals`, not re-loaded inside the detector.
- **GUD-002**: `load_quality_baseline` MUST be called once per `plan_pipeline_spec` invocation and passed to `_quality_drift_proposals`. Do not load it inside the detector function itself.
- **GUD-003**: The evidence dict in `context_detected` SHOULD include: `current_null_rates`, `baseline_null_rates`, `current_record_count`, `baseline_record_count`, `null_rate_drift_pp` (per drifting column), `record_count_drop_pct`, and per-column distribution diffs for drifting columns. This makes the plan report human-readable.

---

## 4. Interfaces & Data Contracts

### 4.1 New functions in `runtime/state.py`

```python
def compute_quality_snapshot(
    bronze_df: pd.DataFrame,
    null_rate_cols: list[str],
    distribution_cols: list[str],
) -> dict[str, Any]:
    from datetime import UTC, datetime
    null_rates: dict[str, float] = {}
    for col in null_rate_cols:
        if col in bronze_df.columns:
            null_rates[col] = round(float(bronze_df[col].isna().mean()), 6)
    distribution: dict[str, dict[str, float]] = {}
    for col in distribution_cols:
        if col in bronze_df.columns:
            counts = bronze_df[col].dropna().astype(str).value_counts(normalize=True)
            distribution[col] = {k: round(float(v), 6) for k, v in counts.items()}
    return {
        "recorded_at_utc": datetime.now(UTC).isoformat(),
        "record_count": int(len(bronze_df)),
        "null_rates": null_rates,
        "distribution": distribution,
    }


def load_quality_baseline(paths: PipelinePaths) -> dict[str, Any] | None:
    state_path = paths.state / "pipeline_state.json"
    state = load_pipeline_state(state_path)
    return state.get("quality_baseline")
```

### 4.2 New policy fields in `DEFAULT_AUTONOMY_POLICY` (`autonomy.py`)

```python
DEFAULT_AUTONOMY_POLICY: dict[str, Any] = {
    # ... existing fields ...
    "quality_drift_null_rate_columns": ["message_body", "conversation_outcome", "channel", "timestamp"],
    "quality_drift_distribution_columns": ["conversation_outcome", "channel"],
    "quality_drift_null_rate_threshold_pp": 10.0,
    "quality_drift_record_count_drop_threshold_pct": 20.0,
    "quality_drift_distribution_shift_threshold_pp": 15.0,
}
```

### 4.3 New mutation family in `DEFAULT_AUTONOMY_POLICY` (`autonomy.py`)

```python
"mutation_families": {
    # ... existing families ...
    "data_quality_drift": {
        "default_impact_class": IMPACT_HIGH,
        "auto_promote": False,
        "requires_approval": True,
        "requires_backward_compatibility": False,
        "requires_privacy_scan": False,
        "agent_auto_approve_if_confidence_ge": 0.85,
    },
}
```

### 4.4 New helper in `autonomy.py`

```python
def get_quality_drift_policy(paths: PipelinePaths) -> dict[str, Any]:
    policy = load_autonomy_policy(paths)
    return {
        "null_rate_columns": list(
            policy.get("quality_drift_null_rate_columns", ["message_body", "conversation_outcome", "channel", "timestamp"])
        ),
        "distribution_columns": list(
            policy.get("quality_drift_distribution_columns", ["conversation_outcome", "channel"])
        ),
        "null_rate_threshold_pp": float(
            policy.get("quality_drift_null_rate_threshold_pp", 10.0)
        ),
        "record_count_drop_threshold_pct": float(
            policy.get("quality_drift_record_count_drop_threshold_pct", 20.0)
        ),
        "distribution_shift_threshold_pp": float(
            policy.get("quality_drift_distribution_shift_threshold_pp", 15.0)
        ),
    }
```

### 4.5 New constant and detector in `planner.py`

```python
PROPOSAL_FAMILY_QUALITY_DRIFT = "data_quality_drift"


def _quality_drift_proposals(
    paths: PipelinePaths,
    planning_run_id: str,
    bronze_df: pd.DataFrame,
    baseline: dict[str, Any] | None,
    policy: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if baseline is None:
        return [], []

    drift_triggers: list[str] = []
    evidence: dict[str, Any] = {
        "current_record_count": int(len(bronze_df)),
        "baseline_record_count": int(baseline.get("record_count", 0)),
        "null_rate_drift_pp": {},
        "record_count_drop_pct": None,
        "distribution_diff": {},
    }

    # Null rate drift
    current_null_rates = {
        col: round(float(bronze_df[col].isna().mean()), 6)
        for col in policy["null_rate_columns"]
        if col in bronze_df.columns
    }
    baseline_null_rates = dict(baseline.get("null_rates", {}))
    evidence["current_null_rates"] = current_null_rates
    evidence["baseline_null_rates"] = baseline_null_rates
    for col, current_rate in current_null_rates.items():
        if col not in baseline_null_rates:
            continue
        drift_pp = (current_rate - float(baseline_null_rates[col])) * 100
        if drift_pp >= policy["null_rate_threshold_pp"]:
            drift_triggers.append(f"{col}_null_rate_increase")
            evidence["null_rate_drift_pp"][col] = round(drift_pp, 2)

    # Record count drop
    baseline_count = int(baseline.get("record_count", 0))
    current_count = int(len(bronze_df))
    if baseline_count > 0:
        drop_pct = (baseline_count - current_count) / baseline_count * 100
        if drop_pct >= policy["record_count_drop_threshold_pct"]:
            drift_triggers.append("record_count_drop")
            evidence["record_count_drop_pct"] = round(drop_pct, 2)

    # Distribution shift
    baseline_distribution = dict(baseline.get("distribution", {}))
    for col in policy["distribution_columns"]:
        if col not in bronze_df.columns or col not in baseline_distribution:
            continue
        current_freq: dict[str, float] = {
            k: round(float(v), 6)
            for k, v in bronze_df[col].dropna().astype(str).value_counts(normalize=True).items()
        }
        baseline_freq: dict[str, float] = {
            k: float(v) for k, v in baseline_distribution[col].items()
        }
        all_values = set(current_freq) | set(baseline_freq)
        max_shift_pp = max(
            abs(current_freq.get(v, 0.0) - baseline_freq.get(v, 0.0)) * 100
            for v in all_values
        ) if all_values else 0.0
        if max_shift_pp >= policy["distribution_shift_threshold_pp"]:
            drift_triggers.append(f"{col}_distribution_shift")
            evidence["distribution_diff"][col] = {
                v: round((current_freq.get(v, 0.0) - baseline_freq.get(v, 0.0)) * 100, 2)
                for v in all_values
            }

    if not drift_triggers:
        return [], []

    drift_triggers = sorted(drift_triggers)
    context = _build_context(
        "quality_drift_detected",
        "Desvio estatistico detectado no Bronze em relacao ao baseline registrado.",
        evidence | {"items": drift_triggers},
    )
    proposal = _build_proposal(
        paths=paths,
        planning_run_id=planning_run_id,
        proposal_type="data_quality_drift_detected",
        proposal_family=PROPOSAL_FAMILY_QUALITY_DRIFT,
        title="Desvio de qualidade detectado no Bronze",
        context_detected=context,
        proposed_change={
            "target_path": "quality.drift_log",
            "operation": "record_quality_drift_event",
            "drift_triggers": drift_triggers,
        },
        expected_impact=(
            "Registra evento de desvio de qualidade e sinaliza necessidade "
            "de rebuild da Silver com revisao de regras de quarentena."
        ),
        risk="high",
        impact_scope="cross_layer",
        requires_approval=True,
        rationale=(
            "Desvio estatistico no Bronze pode propagar silenciosamente "
            "para Silver e Gold sem acionar validacoes estruturais."
        ),
        affected_layers=["bronze", "silver", "gold"],
        affected_artifacts=[
            "config/pipeline_spec.json",
            "reports/monitoring/latest_plan_report.json",
        ],
        privacy_impact="none",
        items=drift_triggers,
    )
    return [context], [proposal]
```

### 4.6 Integration point in `plan_pipeline_spec` (`planner.py`)

**Imports to add:**
```python
from pipeline.agent.autonomy import (
    # ... existing imports ...
    get_quality_drift_policy,
)
from pipeline.runtime.state import (
    compute_quality_snapshot,
    load_quality_baseline,
    load_pipeline_state,
    save_pipeline_state,
)
```

**Before the detector_results list** (after `planning_run_id` is defined):

```python
quality_drift_policy = get_quality_drift_policy(paths)
quality_baseline = load_quality_baseline(paths)
```

**Adding the sixth detector to `detector_results`:**

```python
detector_results = [
    _schema_proposals(paths, planning_run_id, spec, observed_columns, observed_metadata_fields),
    _validation_proposals(paths, planning_run_id, metadata_objects),
    _derived_column_proposals(paths, planning_run_id, spec, metadata_objects),
    _segmentation_proposals(paths, planning_run_id, spec, bronze_df),
    _transformation_proposals(paths, planning_run_id, metadata_objects),
    _quality_drift_proposals(paths, planning_run_id, bronze_df, quality_baseline, quality_drift_policy),
]
```

**After `write_json(report, paths.monitoring / "latest_plan_report.json")`** (baseline update):

```python
# Persist quality baseline for next planning run's drift comparison
quality_snapshot = compute_quality_snapshot(
    bronze_df,
    quality_drift_policy["null_rate_columns"],
    quality_drift_policy["distribution_columns"],
)
state_path = paths.state / "pipeline_state.json"
pipeline_state = load_pipeline_state(state_path)
pipeline_state["quality_baseline"] = quality_snapshot
save_pipeline_state(state_path, pipeline_state)
```

### 4.7 `pipeline_state.json` — new `quality_baseline` key

```json
{
  "runs": [...],
  "last_source_fingerprint": {...},
  "last_successful_run_at_utc": "...",
  "last_successful_artifacts": {...},
  "quality_baseline": {
    "recorded_at_utc": "2026-04-29T17:54:03+00:00",
    "record_count": 100,
    "null_rates": {
      "message_body": 0.02,
      "conversation_outcome": 0.0,
      "channel": 0.0,
      "timestamp": 0.0
    },
    "distribution": {
      "conversation_outcome": {
        "vendido": 0.35,
        "em_negociacao": 0.20,
        "sem_retorno": 0.45
      },
      "channel": {
        "whatsapp": 1.0
      }
    }
  }
}
```

| Field | Type | Description |
|---|---|---|
| `recorded_at_utc` | ISO-8601 UTC string | Timestamp of the planning run that wrote this baseline |
| `record_count` | int | Total rows in Bronze source at time of recording |
| `null_rates` | dict[str, float] | Null fraction per monitored column (0.0 to 1.0) |
| `distribution` | dict[str, dict[str, float]] | Normalized value frequency per monitored categorical column |

### 4.8 Proposal fingerprint stability contract

The fingerprint for a quality drift proposal is computed from:

```python
{
    "proposal_type": "data_quality_drift_detected",
    "proposal_family": "data_quality_drift",
    "proposed_change": {
        "target_path": "quality.drift_log",
        "operation": "record_quality_drift_event",
        "drift_triggers": ["message_body_null_rate_increase", "record_count_drop"],
    },
    "items": ["message_body_null_rate_increase", "record_count_drop"],
}
```

The same set of drift triggers always produces the same `proposal_id`. Actual null rates, record counts, and distribution values are stored in `context_detected.evidence` only — they do NOT influence the fingerprint.

### 4.9 Plan report `summary.status_counts`

No new proposal statuses are introduced. The existing `PROPOSAL_STATUS_*` constants apply to quality drift proposals.

---

## 5. Acceptance Criteria

- **AC-001**: Given `load_quality_baseline` returns `None` (no baseline exists), when `_quality_drift_proposals` is called, then it MUST return `([], [])` without error.
- **AC-002**: Given a baseline with `record_count=100` and current `bronze_df` has 79 rows, and `record_count_drop_threshold_pct=20.0`, when the detector runs, then `"record_count_drop"` MUST be in `drift_triggers` (drop = 21%).
- **AC-003**: Given a baseline with `record_count=100` and current `bronze_df` has 85 rows, when the detector runs, then `"record_count_drop"` MUST NOT be in `drift_triggers` (drop = 15% < threshold 20%).
- **AC-004**: Given a baseline `null_rates["message_body"] = 0.02` and current `bronze_df["message_body"]` has 13% nulls, and `null_rate_threshold_pp=10.0`, when the detector runs, then `"message_body_null_rate_increase"` MUST be in `drift_triggers` (drift = 11 pp >= 10).
- **AC-005**: Given a baseline `null_rates["message_body"] = 0.02` and current null rate is 0.10, and `null_rate_threshold_pp=10.0`, when the detector runs, then `"message_body_null_rate_increase"` MUST NOT be in `drift_triggers` (drift = 8 pp < 10).
- **AC-006**: Given a baseline distribution `conversation_outcome: {"vendido": 0.80, "sem_retorno": 0.20}` and current `{"vendido": 0.60, "sem_retorno": 0.40}`, and `distribution_shift_threshold_pp=15.0`, when the detector runs, then `"conversation_outcome_distribution_shift"` MUST be in `drift_triggers` (shift = 20 pp >= 15).
- **AC-007**: Given a distribution shift of exactly 14 pp (below threshold 15), when the detector runs, then no distribution drift trigger MUST be added.
- **AC-008**: Given a new value appears in `conversation_outcome` that was absent in the baseline (baseline frequency 0.0, current frequency 0.20), and `distribution_shift_threshold_pp=15.0`, when the detector runs, then `"conversation_outcome_distribution_shift"` MUST be in `drift_triggers` (shift = 20 pp).
- **AC-009**: Given no drift triggers are detected, when `_quality_drift_proposals` returns, then BOTH the contexts list and proposals list MUST be empty.
- **AC-010**: Given drift triggers `["record_count_drop", "message_body_null_rate_increase"]`, when `_build_proposal` is called, then `proposal["proposed_change"]` MUST contain only `"target_path"`, `"operation"`, and `"drift_triggers"` — it MUST NOT contain actual metric values.
- **AC-011**: Given drift triggers `["record_count_drop", "message_body_null_rate_increase"]`, when `_build_proposal` is called, then `proposal["items"]` MUST equal `["message_body_null_rate_increase", "record_count_drop"]` (sorted).
- **AC-012**: Given the same Bronze source produces the same set of drift triggers across two planning runs, when `_proposal_fingerprint` is called for both, then both MUST return the same `proposal_id`.
- **AC-013**: Given different drift triggers (e.g., `["record_count_drop"]` vs `["message_body_null_rate_increase"]`), when `_proposal_fingerprint` is called for both, then they MUST produce different `proposal_id` values.
- **AC-014**: Given a planning run completes successfully, when the function returns, then `pipeline_state.json` MUST contain a `"quality_baseline"` key with `record_count`, `null_rates`, and `distribution` values computed from `bronze_df`.
- **AC-015**: Given a planning run with `baseline=None` (first run), when the run completes, then a baseline MUST still be written to `pipeline_state.json`. No proposal is generated, but the baseline is always persisted.
- **AC-016**: Given `compute_quality_snapshot` is called with `null_rate_cols=["missing_col"]` and `bronze_df` does not have `"missing_col"`, then the function MUST return normally and `null_rates` MUST be an empty dict.
- **AC-017**: Given the `data_quality_drift` family has no `auto_promote=True`, when a quality drift proposal passes all gates, then the proposal MUST NOT be auto-promoted — it MUST end up in `PROPOSAL_STATUS_AWAITING_APPROVAL` or `PROPOSAL_STATUS_CANDIDATE_MATERIALIZED`.
- **AC-018**: Given `apply_proposal_to_spec` is called with a `data_quality_drift_detected` proposal, then the returned spec MUST have `spec["quality"]["drift_log"]` as a non-empty list and `changed` MUST be `True`.
- **AC-019**: Given a custom policy `{"quality_drift_record_count_drop_threshold_pct": 10.0}`, when `get_quality_drift_policy` is called, then it MUST return `{"record_count_drop_threshold_pct": 10.0, ...defaults_for_other_keys...}`.

---

## 6. Test Automation Strategy

- **Test Levels**: Unit — all logic is deterministic and testable with synthetic DataFrames and mock I/O.
- **Frameworks**: `pytest`, `unittest.mock.patch`, `pandas`.
- **Test file**: `tests/test_quality_drift_detector.py` (new).

### Required test cases

| Test | Description |
|---|---|
| `test_no_baseline_returns_empty` | `baseline=None` → `([], [])` |
| `test_record_count_drop_above_threshold` | Drop 21% with threshold 20% → trigger generated |
| `test_record_count_drop_below_threshold` | Drop 15% with threshold 20% → no trigger |
| `test_record_count_drop_baseline_zero` | `baseline_record_count=0` → no trigger, no ZeroDivisionError |
| `test_null_rate_increase_above_threshold` | Drift 11 pp with threshold 10 pp → trigger generated |
| `test_null_rate_increase_below_threshold` | Drift 8 pp with threshold 10 pp → no trigger |
| `test_null_rate_decrease_not_triggered` | Null rate decreases → no trigger (only increases flagged) |
| `test_distribution_shift_above_threshold` | 20 pp shift with threshold 15 pp → trigger |
| `test_distribution_shift_below_threshold` | 14 pp shift → no trigger |
| `test_distribution_new_value_above_threshold` | New value at 20% (was 0%) with threshold 15 pp → trigger |
| `test_distribution_vanished_value_above_threshold` | Value at 0% (was 20%) with threshold 15 pp → trigger |
| `test_missing_column_skipped` | Column in policy absent from df → no error, no trigger |
| `test_multiple_triggers_single_proposal` | Two triggers → one proposal with both in items |
| `test_proposed_change_has_no_metric_values` | proposed_change must not contain actual null rates |
| `test_items_is_sorted` | items list is alphabetically sorted |
| `test_proposal_fingerprint_stable_same_triggers` | Same triggers → same proposal_id across calls |
| `test_proposal_fingerprint_differs_different_triggers` | Different trigger sets → different proposal_id |
| `test_compute_quality_snapshot_returns_null_rates` | Correct null fraction per column |
| `test_compute_quality_snapshot_returns_distribution` | Correct normalized frequencies, values cast to str |
| `test_compute_quality_snapshot_skips_missing_columns` | Missing columns → empty dicts, no error |
| `test_load_quality_baseline_none_when_absent` | No `quality_baseline` key in state → returns None |
| `test_load_quality_baseline_returns_stored` | Key present → returns stored dict |
| `test_baseline_written_after_plan` | After `plan_pipeline_spec`, `pipeline_state.json` has `quality_baseline` |
| `test_baseline_written_even_when_no_drift` | No drift detected → baseline still written |
| `test_apply_proposal_to_spec_adds_drift_log` | `drift_log` key added to spec, `changed=True` |
| `test_get_quality_drift_policy_defaults` | No policy file → correct defaults returned |
| `test_get_quality_drift_policy_custom` | Custom file → overridden values returned |
| `test_data_quality_drift_family_classified_high` | `classify_proposal("data_quality_drift", ...)` returns `impact_class="high"` |
| `test_data_quality_drift_requires_approval` | Family `requires_approval=True` |

### CI/CD

- `venv/bin/python -m pytest tests/test_quality_drift_detector.py -q` MUST pass.
- `venv/bin/python -m pytest -q` (full suite) MUST continue passing without regressions.
- Tests MUST NOT require LLM calls or external I/O — all file paths MUST be mocked.
- `compute_quality_snapshot` tests MUST use synthetic DataFrames created inline.

---

## 7. Rationale & Context

The existing five detectors in `plan_pipeline_spec` all work by comparing the observed Bronze schema and metadata structure against the current pipeline spec. They detect new columns, new metadata fields, boolean coercions, segmentation gaps, and normalization issues — all of which are detectable by inspecting the data's shape and key names.

None of them can detect degradation in the DATA ITSELF: if `message_body` starts arriving with 30% nulls instead of 2%, the spec contract is still satisfied (the column exists, has the right type). If the source starts sending 60 records where it used to send 500, validation passes because there is no record count contract. If the distribution of `conversation_outcome` shifts significantly — perhaps because the CRM started coding outcomes differently — the pipeline processes it silently.

The baseline snapshot + drift comparison approach was chosen over alternatives (e.g., Great Expectations integration, statistical process control) because:

1. **No new dependencies** — pandas operations are already available throughout the planner.
2. **Deterministic and auditable** — the threshold comparisons are simple arithmetic. The proposal contains the exact numbers that triggered the alert, making operator review straightforward.
3. **Consistent with existing proposal architecture** — the new detector integrates with `_build_proposal`, the evaluation loop, and the governance gates without requiring changes to any of those mechanisms.
4. **Fingerprint stability** — by excluding actual metric values from `proposed_change`, the same drift pattern (same columns drifting in the same direction) produces the same `proposal_id` across cycles. This allows the cooloff mechanism (GAP-A04) and awaiting-approval expiry (GAP-A03) to apply normally.

The baseline is updated after every planning run (not only after successful pipeline runs) because the planner runs on source change, which is the same condition that triggers pipeline execution. Updating after the planning run ensures the baseline always reflects the source that was actually observed, regardless of whether the downstream pipeline run succeeded.

---

## 8. Dependencies & External Integrations

### External Systems
- **EXT-001**: None. The detector is entirely internal to the pipeline agent.

### Infrastructure Dependencies
- **INF-001**: `state/pipeline_state.json` — existing state file; the `"quality_baseline"` top-level key is added. Backward-compatible: the key defaults to `None` via `state.get("quality_baseline")` when absent.
- **INF-002**: `config/agent_autonomy_policy.json` — existing policy file; five new top-level keys are read, with defaults when absent.
- **INF-003**: `docs/conversations_bronze.parquet` (or `PIPELINE_INPUT_FILE`) — read by `plan_pipeline_spec` via `pd.read_parquet(paths.raw_bronze_source)`. The drift detector uses the same `bronze_df` already loaded by the function.

### Technology Platform Dependencies
- **PLT-001**: Python 3.11+ — already required.
- **PLT-002**: `pandas` — already a direct dependency of `planner.py`.

---

## 9. Examples & Edge Cases

### Null rate increase triggers proposal

```
Baseline: {"null_rates": {"message_body": 0.02}, "record_count": 200}
Current bronze_df: 200 rows, 28 nulls in message_body → null_rate = 0.14
Drift = (0.14 - 0.02) * 100 = 12 pp >= threshold 10 pp
Trigger: "message_body_null_rate_increase"
Proposal generated with items=["message_body_null_rate_increase"]
```

### Record count drop triggers proposal

```
Baseline: {"record_count": 500}
Current bronze_df: 380 rows
Drop = (500 - 380) / 500 * 100 = 24% >= threshold 20%
Trigger: "record_count_drop"
Proposal generated with items=["record_count_drop"]
```

### Multiple triggers — single proposal

```
Triggers detected: ["record_count_drop", "message_body_null_rate_increase", "conversation_outcome_distribution_shift"]
Result: exactly one proposal generated
items = ["conversation_outcome_distribution_shift", "message_body_null_rate_increase", "record_count_drop"]  (sorted)
proposed_change.drift_triggers = same sorted list
```

### Distribution shift with new value

```
Baseline distribution["conversation_outcome"]: {"vendido": 0.80, "sem_retorno": 0.20}
Current distribution["conversation_outcome"]: {"vendido": 0.55, "sem_retorno": 0.20, "sem_interesse": 0.25}
Shift per value:
  "vendido": |0.55 - 0.80| * 100 = 25 pp  ← >= threshold 15 pp
  "sem_retorno": |0.20 - 0.20| * 100 = 0 pp
  "sem_interesse": |0.25 - 0.0| * 100 = 25 pp  ← >= threshold 15 pp
Max shift = 25 pp >= 15 pp → trigger "conversation_outcome_distribution_shift"
```

### First planning run — no baseline

```
load_quality_baseline(paths) → None
_quality_drift_proposals(..., baseline=None, ...) → ([], [])
No proposal generated.
At end of plan_pipeline_spec: quality_baseline written to pipeline_state.json
Next run will have a baseline to compare against.
```

### Baseline absent for a monitored column

```
Baseline: {"null_rates": {"message_body": 0.02}}  # "timestamp" not recorded
Current df has "timestamp" column
→ "timestamp" skipped (not in baseline_null_rates) → no trigger for timestamp
```

### Custom thresholds via `autonomy_policy.json`

```json
{
  "quality_drift_null_rate_threshold_pp": 5.0,
  "quality_drift_record_count_drop_threshold_pct": 10.0,
  "quality_drift_distribution_shift_threshold_pp": 10.0
}
```

With 8 pp null rate drift (normally below 10 pp threshold): now triggers with threshold 5 pp.

### Edge case: `baseline_record_count=0` (empty first source)

```
baseline["record_count"] = 0
current_count = 50
Drop calculation skipped (guard: baseline_count > 0 required)
→ No record_count_drop trigger
```

### Edge case: column present in policy but absent from df and absent from baseline

Both sides skip the column → no trigger, no error. The snapshot written at end also omits the column.

---

## 10. Validation Criteria

- **VAL-001**: `venv/bin/python -m pytest tests/test_quality_drift_detector.py -q` MUST pass with all scenarios in section 6.
- **VAL-002**: `venv/bin/python -m pytest -q` (full suite) MUST pass without regressions.
- **VAL-003**: `grep -n "quality_drift_null_rate_threshold_pp\|quality_drift_record_count_drop_threshold_pct\|quality_drift_distribution_shift_threshold_pp" src/pipeline/agent/autonomy.py` MUST return all three keys in `DEFAULT_AUTONOMY_POLICY`.
- **VAL-004**: `grep -n "data_quality_drift" src/pipeline/agent/autonomy.py` MUST return the mutation family definition.
- **VAL-005**: `grep -n "get_quality_drift_policy" src/pipeline/agent/autonomy.py` MUST return the function definition.
- **VAL-006**: `grep -n "compute_quality_snapshot\|load_quality_baseline" src/pipeline/runtime/state.py` MUST return both function definitions.
- **VAL-007**: `grep -n "_quality_drift_proposals" src/pipeline/agent/planner.py` MUST return both the function definition and the call inside `detector_results`.
- **VAL-008**: `grep -n "quality_baseline" src/pipeline/agent/planner.py` MUST return at least one reference to the baseline persistence call at the end of `plan_pipeline_spec`.
- **VAL-009**: `grep -n "PROPOSAL_FAMILY_QUALITY_DRIFT" src/pipeline/agent/planner.py` MUST return the constant definition.
- **VAL-010**: A manual end-to-end test with a synthetic Bronze source where `message_body` has 30% nulls (baseline 2%) MUST produce a plan report containing a `data_quality_drift_detected` proposal with `"message_body_null_rate_increase"` in `items`.
- **VAL-011**: After a planning run on a clean source (no drift), `cat state/pipeline_state.json | python3 -c "import json,sys; s=json.load(sys.stdin); print('ok' if 'quality_baseline' in s else 'missing')"` MUST print `"ok"`.

---

## 11. Related Specifications / Further Reading

- [docs/agent-autonomy-gaps.md](../docs/agent-autonomy-gaps.md) — Full autonomy gap catalogue; GAP-A05 is the source of this spec
- [spec-process-daemon-watchdog-exponential-backoff.md](./spec-process-daemon-watchdog-exponential-backoff.md) — GAP-A01 remediation (daemon resilience)
- [spec-process-planner-idle-cycle-gate.md](./spec-process-planner-idle-cycle-gate.md) — GAP-A02 remediation (planner idle-cycle gate)
- [spec-process-proposal-awaiting-approval-expiry.md](./spec-process-proposal-awaiting-approval-expiry.md) — GAP-A03 remediation (awaiting-approval stale resolution)
- [spec-process-proposal-rejection-cooloff-window.md](./spec-process-proposal-rejection-cooloff-window.md) — GAP-A04 remediation (rejection cooloff window)
- [spec-architecture-agent-autonomy-governed-by-impact.md](./spec-architecture-agent-autonomy-governed-by-impact.md) — Original governance-by-impact architecture
- [spec-design-agentic-layer-quality-gaps.md](./spec-design-agentic-layer-quality-gaps.md) — Existing quality gap analysis
