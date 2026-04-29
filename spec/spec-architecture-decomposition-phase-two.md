---
title: Module Decomposition Phase 2 — Gold Canonicalization and Operator Helper Consolidation
version: 1.0
date_created: 2026-04-28
owner: Data Engineering
tags: [architecture, refactoring, maintainability, gold, operator, silver]
---

# Introduction

After the first decomposition commit (`c798a94`), three gaps remain that prevent full compliance with the acceptance criteria defined in `spec-architecture-module-cyclomatic-decomposition.md`:

1. **`operator.py` retains four helper functions** that were not migrated to submodules, violating REQ-204. One of them (`_run_validation_suite`) was correctly implemented in `operator_stages.py` but its duplicate was never removed from `operator.py`.
2. **`silver.py` has 849 lines**, violating AC-004 (`< 400`), because `build_gold` (~290 lines) and its Gold-segmentation helpers (~260 lines) were deferred by REQ-106 and were never migrated.
3. **`gold.py` is not the canonical owner of `build_gold`**, having only 6 lines that re-export from `silver.py`.

This spec defines the complete resolution of all three gaps, maintaining retrocompatibility with all existing import paths and passing all 265 existing tests without modification of test logic.

## 1. Purpose & Scope

**Purpose:** Complete the module decomposition begun in Phase 1 by: (a) removing orphaned helper functions from `operator.py`; (b) migrating `build_gold` and its dependencies to `gold.py`, making it the canonical owner; (c) extracting shared aggregation helpers to a new `silver_aggregators.py` submodule to prevent circular imports.

**Scope:**

New modules:
- `src/pipeline/transforms/silver_aggregators.py` — shared aggregation helpers used by both `build_silver_leads` and `build_gold`
- `src/pipeline/transforms/gold_segments.py` — Gold segmentation logic (`add_gold_segments` and its helper functions)

Updated modules:
- `src/pipeline/transforms/gold.py` — becomes the canonical owner of `build_gold`
- `src/pipeline/transforms/silver.py` — drops Gold content; retains Silver entry point + retrocompatible reexports from gold
- `src/pipeline/orchestration/operator_reports.py` — receives `_incident_id`
- `src/pipeline/orchestration/operator_stages.py` — receives `_get_llm_call` and `_determine_retry_stage`; retains the already-correct `_run_validation_suite`
- `src/pipeline/orchestration/operator.py` — removes four local function definitions, imports them from submodules, keeps them in `__all__`

**Out of scope:**
- Modification of any function signature or business logic
- Changes to any Parquet schema (Bronze, Silver, Gold)
- Changes to `config/pipeline_spec.json`
- Modification of test logic — only import paths may change if a test imports directly from a moved submodule
- Further decomposition of `run_cycle()` — this remains justified by CON-004

**Intended audience:** Engineers implementing this change and PR reviewers.

## 2. Definitions

| Term | Definition |
|---|---|
| Canonical owner | The module where a symbol is defined (not re-exported from another module) |
| Retrocompatibility | Guarantee that all existing `from module import symbol` statements continue to work after the change |
| Reexport | Technique of making a symbol accessible in module A after moving it to module B: `from B import X  # noqa: F401` in A, with X listed in A's `__all__` |
| Circular import | A situation where module A imports from module B and module B imports from module A, causing Python to fail at load time |
| Aggregation helper | A small function used as a `groupby().agg()` callable in pandas pipelines (`_last_non_null`, `_max_or_false`, etc.) |
| Duplicate definition | A function defined in two modules simultaneously. In this context: `_run_validation_suite` exists in both `operator.py` (lines 147–172) and `operator_stages.py` (lines 158–183). The definition in `operator.py` must be removed. |
| CON-004 | Constraint from Phase 1 spec: `run_cycle()` must call `load_bronze_frame`, `build_silver`, `build_gold`, and `validate_gold` through the `pipeline.orchestration.operator` module namespace so that existing tests can monkeypatch them via `monkeypatch.setattr("pipeline.orchestration.operator.<fn>", ...)` |
| GUD-001 exception | An inline comment at the top of a module that documents why the 400-line limit is exceeded and what constraint prevents resolution |

## 3. Requirements, Constraints & Guidelines

### Retrocompatibility — inherited constraints

- **CON-001**: Every symbol currently importable from `pipeline.transforms.silver` MUST remain importable after this change. No regressions.
- **CON-002**: Every symbol currently importable from `pipeline.orchestration.operator` MUST remain importable after this change.
- **CON-003**: No existing test file may have its assertion logic modified. Import paths that change (because a test imports directly from a moved submodule) may be updated.
- **CON-004**: `run_cycle()` must continue to call `load_bronze_frame`, `build_silver`, `build_gold`, and `validate_gold` via names resolved in the `pipeline.orchestration.operator` namespace. Do not delegate these calls to `operator_stages.py`.

### New constraints

- **CON-005**: No circular imports between any two modules in `src/pipeline/transforms/` or `src/pipeline/orchestration/`. The dependency order for transforms must be: `silver_patterns → silver_masking → silver_signals → silver_context / silver_dedup / silver_aggregators → silver → gold_segments → gold`. The dependency order for orchestration must be: `operator_artifacts → operator_reports → operator_stages → operator`.
- **CON-006**: After this change, `build_gold` MUST be importable via both `from pipeline.transforms.gold import build_gold` (canonical) and `from pipeline.transforms.silver import build_gold` (retrocompat).

### Requirements — `silver.py` → `gold.py` migration

- **REQ-301**: A new module `src/pipeline/transforms/silver_aggregators.py` MUST be created containing the eight aggregation helpers that are currently defined in `silver.py`: `_first_non_empty`, `_json_sorted_unique`, `_first_non_null`, `_last_non_null`, `_max_or_false`, `_bool_series_or_default`, `_int_series_or_default`, `_value_series_or_default`. This module MUST import only from `pipeline.transforms.silver_patterns` and stdlib. It MUST NOT import from any other silver submodule or from gold.

- **REQ-302**: A new module `src/pipeline/transforms/gold_segments.py` MUST be created containing: `_canonical_audience_for_persona`, `_response_latency_band`, `_price_objection_intensity`, `_commercial_urgency_signal`, `_competitor_pressure_level`, and `add_gold_segments`. This module MUST import from `pipeline.transforms.silver_patterns` (for `_safe_string`, `_safe_int`, `_safe_float`, `_normalize_for_match`) and from `pipeline.orchestration.compiler` (for `get_default_compiled_plan`). It MUST NOT import from `gold.py`.

- **REQ-303**: `src/pipeline/transforms/gold.py` MUST be expanded to become the canonical owner of `build_gold`. It MUST also define `_parse_json_list` and `_normalize_outcome_group`, which are internal helpers used exclusively by `build_gold`. It MUST import `_last_non_null`, `_max_or_false`, `_bool_series_or_default`, `_int_series_or_default`, `_value_series_or_default` from `silver_aggregators`; `_count_tone_hits`, `derive_conversation_sentiment_label`, `derive_conversation_sentiment_support` from `silver_signals`; `_normalize_for_match`, `_safe_string`, `_safe_float`, `_safe_int`, `POSITIVE_TONE_PATTERNS`, `NEGATIVE_TONE_PATTERNS` from `silver_patterns`; and `add_gold_segments` from `gold_segments`. It MUST reexport all symbols from `gold_segments` and `gold_macro` so that `from pipeline.transforms.gold import add_gold_segments` works. It MUST define a complete `__all__`.

- **REQ-304**: `src/pipeline/transforms/silver.py` MUST remove the definitions of `build_gold`, `add_gold_segments`, `_canonical_audience_for_persona`, `_parse_json_list`, `_normalize_outcome_group`, `_response_latency_band`, `_price_objection_intensity`, `_commercial_urgency_signal`, `_competitor_pressure_level`, `_bool_series_or_default`, `_int_series_or_default`, `_value_series_or_default`, `_first_non_empty`, `_json_sorted_unique`, `_first_non_null`, `_last_non_null`, `_max_or_false`. All of these MUST be reexported via imports from their new canonical modules so that every symbol that previously appeared in `silver.__all__` continues to appear there.

- **REQ-305**: `src/pipeline/transforms/silver.py` MUST add reexports for `build_gold`, `add_gold_segments`, and all Gold segment helpers from `pipeline.transforms.gold` and `pipeline.transforms.gold_segments`. All reexported symbols MUST be present in `silver.__all__`.

- **REQ-306**: `src/pipeline/transforms/silver.py` MUST add reexports for all eight aggregation helpers from `pipeline.transforms.silver_aggregators`. All reexported symbols MUST be present in `silver.__all__`.

### Requirements — `operator.py` helper consolidation

- **REQ-401**: The function `_incident_id` MUST be moved from `operator.py` to `operator_reports.py`. It MUST be added to `operator_reports.__all__`. The definition in `operator.py` MUST be replaced by an import reexport: `from pipeline.orchestration.operator_reports import _incident_id  # noqa: F401`.

- **REQ-402**: The function `_get_llm_call` MUST be moved from `operator.py` to `operator_stages.py`. It MUST be added to `operator_stages.__all__`. The definition in `operator.py` MUST be replaced by an import reexport.

- **REQ-403**: The function `_determine_retry_stage` MUST be moved from `operator.py` to `operator_stages.py`. It MUST be added to `operator_stages.__all__`. The definition in `operator.py` MUST be replaced by an import reexport.

- **REQ-404**: The duplicate definition of `_run_validation_suite` in `operator.py` (lines 147–172) MUST be removed. `operator.py` MUST import it from `operator_stages.py` via reexport. `_run_validation_suite` MUST remain in `operator.__all__`. The canonical definition in `operator_stages.py` (lines 158–183) is authoritative and MUST NOT be changed.

- **REQ-405**: After REQ-401 through REQ-404, `operator.py` MUST still list `_incident_id`, `_get_llm_call`, `_determine_retry_stage`, and `_run_validation_suite` in its `__all__`.

### Size guidelines

- **GUD-001**: No module resulting from this spec may exceed 400 lines. If a module exceeds 400 lines, a justified GUD-001 exception comment MUST appear at the top of the file stating the reason and the blocking constraint. The pre-existing exception in `operator.py` (justified by CON-004 / size of `run_cycle()`) is valid and does not need to be resolved by this spec.
- **GUD-002**: `silver_aggregators.py` and `gold_segments.py` MUST each have a single, clearly identifiable responsibility.
- **GUD-003**: `gold.py` MUST NOT exceed 400 lines after receiving `build_gold`.

## 4. Interfaces & Data Contracts

### New module: `silver_aggregators.py`

```
Canonical path: src/pipeline/transforms/silver_aggregators.py
Imports from:   pipeline.transforms.silver_patterns (for _safe_string, _safe_float, _safe_int)
                stdlib: json, pandas
Exported symbols:
  _first_non_empty(values: pd.Series) -> str
  _json_sorted_unique(values: pd.Series) -> str
  _first_non_null(values: pd.Series) -> object
  _last_non_null(values: pd.Series) -> object
  _max_or_false(values: pd.Series) -> bool
  _bool_series_or_default(frame, column, fallback_column=None) -> pd.Series
  _int_series_or_default(frame, column, fallback_column=None) -> pd.Series
  _value_series_or_default(frame, column, fallback_column=None) -> pd.Series
```

### New module: `gold_segments.py`

```
Canonical path: src/pipeline/transforms/gold_segments.py
Imports from:   pipeline.transforms.silver_patterns (_safe_string, _safe_int, _safe_float,
                  _normalize_for_match)
                pipeline.orchestration.compiler (get_default_compiled_plan)
                stdlib: typing, pandas
Exported symbols:
  _canonical_audience_for_persona(persona_profile: object) -> str
  _response_latency_band(avg_response_time_sec: object) -> str
  _price_objection_intensity(price_objection_hits, competitor_mentions,
                              quoted_price_mentions) -> str
  _commercial_urgency_signal(urgency_strength_max, urgency_hits,
                              lead_lifecycle_hours) -> str
  _competitor_pressure_level(primary_competitor, competitor_mentions,
                              competitor_comparison_hits) -> str
  add_gold_segments(gold: pd.DataFrame, compiled_plan=None) -> pd.DataFrame
```

### Updated module: `gold.py`

```
Canonical path: src/pipeline/transforms/gold.py
Imports from:   pipeline.transforms.silver_patterns
                pipeline.transforms.silver_signals
                pipeline.transforms.silver_aggregators
                pipeline.transforms.gold_segments  (reexported)
                pipeline.transforms.gold_macro      (reexported)
                pipeline.transforms.conversation_enrichment (lazy import inside build_gold)
                pipeline.orchestration.compiler
                stdlib: json, typing, pandas
Defines:
  _parse_json_list(raw: object) -> list[str]
  _normalize_outcome_group(observed_outcomes: object) -> str
  build_gold(silver_leads, silver_messages, silver_conversations_llm=None,
             compiled_plan=None, gold_column_plan=None) -> pd.DataFrame
Reexports (noqa: F401):
  add_gold_segments, _canonical_audience_for_persona,
  _response_latency_band, _price_objection_intensity,
  _commercial_urgency_signal, _competitor_pressure_level  ← from gold_segments
  build_gold_macro  ← from gold_macro
```

### Symbol migration map — `silver.py`

| Symbol | Origin (was in silver.py) | New canonical location | Reexported by silver.py |
|---|---|---|---|
| `_first_non_empty` | `silver.py` | `silver_aggregators.py` | Yes |
| `_json_sorted_unique` | `silver.py` | `silver_aggregators.py` | Yes |
| `_first_non_null` | `silver.py` | `silver_aggregators.py` | Yes |
| `_last_non_null` | `silver.py` | `silver_aggregators.py` | Yes |
| `_max_or_false` | `silver.py` | `silver_aggregators.py` | Yes |
| `_bool_series_or_default` | `silver.py` | `silver_aggregators.py` | Yes |
| `_int_series_or_default` | `silver.py` | `silver_aggregators.py` | Yes |
| `_value_series_or_default` | `silver.py` | `silver_aggregators.py` | Yes |
| `_canonical_audience_for_persona` | `silver.py` | `gold_segments.py` | Yes (via gold.py) |
| `_response_latency_band` | `silver.py` | `gold_segments.py` | Yes (via gold.py) |
| `_price_objection_intensity` | `silver.py` | `gold_segments.py` | Yes (via gold.py) |
| `_commercial_urgency_signal` | `silver.py` | `gold_segments.py` | Yes (via gold.py) |
| `_competitor_pressure_level` | `silver.py` | `gold_segments.py` | Yes (via gold.py) |
| `add_gold_segments` | `silver.py` | `gold_segments.py` | Yes (via gold.py) |
| `_parse_json_list` | `silver.py` | `gold.py` | Yes (via gold.py) |
| `_normalize_outcome_group` | `silver.py` | `gold.py` | Yes (via gold.py) |
| `build_gold` | `silver.py` | `gold.py` | Yes |
| `load_bronze_frame` | `silver.py` | `silver.py` (stays) | N/A |
| `parse_metadata` | `silver.py` | `silver.py` (stays) | N/A |
| `build_silver` | `silver.py` | `silver.py` (stays) | N/A |
| `build_silver_leads` | `silver.py` | `silver.py` (stays) | N/A |

### Symbol migration map — `operator.py`

| Symbol | Current state | Action | New canonical location |
|---|---|---|---|
| `_incident_id` | Defined in `operator.py` (line 115) | Move definition | `operator_reports.py` |
| `_get_llm_call` | Defined in `operator.py` (line 119) | Move definition | `operator_stages.py` |
| `_determine_retry_stage` | Defined in `operator.py` (line 132) | Move definition | `operator_stages.py` |
| `_run_validation_suite` | Duplicate: defined in `operator.py` (line 147) AND in `operator_stages.py` (line 158) | Remove `operator.py` copy; import from `operator_stages.py` | `operator_stages.py` (already correct) |

### Expected line counts after implementation

| Module | Current | Target | Notes |
|---|---|---|---|
| `silver.py` | 849 | < 250 | Drops all Gold content and aggregation helpers |
| `silver_aggregators.py` | — (new) | ~80 | 8 aggregation helpers |
| `gold_segments.py` | — (new) | ~175 | 6 segmentation helpers + `add_gold_segments` |
| `gold.py` | 6 | ~380 | Receives `build_gold`, `_parse_json_list`, `_normalize_outcome_group` |
| `operator.py` | 869 | ~800 | Removes 4 function bodies (~70 lines); CON-004 exception remains |
| `operator_reports.py` | 124 | ~135 | Receives `_incident_id` |
| `operator_stages.py` | 208 | ~235 | Receives `_get_llm_call`, `_determine_retry_stage` |

## 5. Acceptance Criteria

- **AC-001**: Given the implementation is applied, when `venv/bin/python -m pytest -q` is executed, then all 265 existing tests pass without modification of assertion logic.
- **AC-002**: Given the implementation is applied, when `wc -l src/pipeline/transforms/silver.py` is executed, then the result is less than 400.
- **AC-003**: Given the implementation is applied, when `wc -l src/pipeline/transforms/gold.py` is executed, then the result is less than 400.
- **AC-004**: Given the implementation is applied, when `from pipeline.transforms.silver import build_gold, add_gold_segments, _bool_series_or_default, _last_non_null` is executed, then all symbols are importable without error.
- **AC-005**: Given the implementation is applied, when `from pipeline.transforms.gold import build_gold, add_gold_segments, build_gold_macro` is executed, then all symbols are importable without error (gold is the canonical owner).
- **AC-006**: Given the implementation is applied, when `from pipeline.orchestration.operator import _incident_id, _get_llm_call, _determine_retry_stage, _run_validation_suite` is executed, then all symbols are importable without error.
- **AC-007**: Given the implementation is applied, when `python -c "import pipeline.transforms.silver_aggregators; import pipeline.transforms.gold_segments"` is executed, then both imports succeed.
- **AC-008**: Given the implementation is applied, when `python -c "from pipeline.transforms.silver import *; from pipeline.transforms.gold import *; from pipeline.orchestration.operator import *"` is executed, then exit code is 0.
- **AC-009**: Given the implementation is applied, when `ruff check src/` is executed, then the result contains zero errors.
- **AC-010**: Given the implementation is applied, when `python -c "import pipeline.transforms.gold; import pipeline.transforms.silver"` is executed without `ImportError` or `CircularImport` error, then the module graph is acyclic.
- **AC-011**: Given the implementation is applied, when `git diff tests/` is inspected, then no line of assertion logic has been modified.

## 6. Test Automation Strategy

- **Test Levels**: The existing 265-test suite is the primary acceptance gate. No new tests are required for this spec.
- **Smoke tests**: Three one-liners MUST pass as part of CI:
  ```
  venv/bin/python -c "from pipeline.transforms.silver import *"
  venv/bin/python -c "from pipeline.transforms.gold import build_gold, add_gold_segments, build_gold_macro"
  venv/bin/python -c "from pipeline.orchestration.operator import _incident_id, _run_validation_suite"
  ```
- **Size check**: `wc -l src/pipeline/transforms/silver.py src/pipeline/transforms/gold.py` — both results MUST be less than 400 (no GUD-001 exception needed for these two files after this spec).
- **Circular import check**: `venv/bin/python -c "import pipeline.transforms.gold; import pipeline.orchestration.operator"` — MUST return exit code 0.
- **Ruff**: `venv/bin/python -m ruff check src/` — MUST return zero errors after the change.

## 7. Rationale & Context

**Why `silver_aggregators.py` is necessary:** After migrating `build_gold` to `gold.py`, both `build_silver_leads` (staying in `silver.py`) and `build_gold` (now in `gold.py`) need `_last_non_null` and `_max_or_false`. If these helpers stayed in `silver.py`, then `gold.py` would import from `silver.py`, creating a circular dependency because `silver.py` retrocompatibly reexports `build_gold` from `gold.py`. Extracting all eight helpers to `silver_aggregators.py` (which has no upstream transforms dependency) eliminates the cycle.

**Why `gold_segments.py` is separate from `gold.py`:** `build_gold` alone is ~290 lines. Adding `add_gold_segments` (~100 lines), its six helper functions (~92 lines), `_parse_json_list` (~12 lines), `_normalize_outcome_group` (~23 lines), and module-level imports would push `gold.py` past 400 lines. Separating segmentation logic into `gold_segments.py` keeps both modules under the GUD-001 limit with clear single responsibilities: `gold_segments.py` owns segmentation rules; `gold.py` owns the Gold build pipeline.

**Why operator.py still exceeds 400 lines after this spec:** `run_cycle()` is ~645 lines and cannot be decomposed without violating CON-004 (tests monkeypatch `build_silver`, `build_gold`, etc. through `operator.py`'s namespace). Removing the four orphaned helpers reduces `operator.py` by ~70 lines (869 → ~800) but does not resolve the size violation. The pre-existing GUD-001 exception remains valid.

**Why `_run_validation_suite` already exists in `operator_stages.py`:** During Phase 1, the function was correctly implemented in `operator_stages.py` but the corresponding removal from `operator.py` was not applied. This spec closes that gap by removing the duplicate from `operator.py`.

## 8. Dependencies & External Integrations

### Technology Platform Dependencies
- **PLT-001**: Python 3.11 — standard module system; no new packages required.

### Data Dependencies
- No data schema changes. This spec reorganizes code only.

### Internal Module Dependencies
- **INT-001**: `silver_aggregators.py` depends on `silver_patterns.py` for `_safe_string`, `_safe_float`, `_safe_int`.
- **INT-002**: `gold_segments.py` depends on `silver_patterns.py` for `_safe_string`, `_safe_int`, `_safe_float`, `_normalize_for_match` and on `pipeline.orchestration.compiler` for `get_default_compiled_plan`.
- **INT-003**: `gold.py` depends on `silver_patterns`, `silver_signals`, `silver_aggregators`, `gold_segments`, `gold_macro`. It MUST NOT import from `silver.py` directly to avoid circular imports.

## 9. Examples & Edge Cases

### Dependency graph — transforms (after implementation)

```
silver_patterns ──────────────────────────────────────────────────────────────────┐
    └── silver_masking ──────────────────────────────────────────────────────────┐ │
            └── silver_signals ─────────────────────────────────────────────────┐ │ │
    └── silver_context ──────────────────────────────────────────────────────────│ │ │
    └── silver_dedup ────────────────────────────────────────────────────────────│ │ │
    └── silver_aggregators ──────────────────────────────────────────────────────│ │ │
    └── gold_segments ───────────────────────────────────────────────────────────│ │ │
                                                                                  ▼ ▼ ▼
silver.py  ←── reexports from all above ──────────────────────────────────────────────┘
silver.py  ←── reexports from gold.py  (build_gold, add_gold_segments, ...)
gold.py    ←── imports from silver_patterns, silver_signals, silver_aggregators, gold_segments
```

### Retrocompat smoke test — silver.py

```python
# All of these must continue to work after the migration
from pipeline.transforms.silver import (
    # originally in silver.py, now in silver_aggregators.py:
    _first_non_empty, _json_sorted_unique, _first_non_null, _last_non_null,
    _max_or_false, _bool_series_or_default, _int_series_or_default, _value_series_or_default,
    # originally in silver.py, now in gold_segments.py (via gold.py):
    add_gold_segments, _canonical_audience_for_persona,
    _response_latency_band, _price_objection_intensity,
    _commercial_urgency_signal, _competitor_pressure_level,
    # originally in silver.py, now in gold.py:
    build_gold, _parse_json_list, _normalize_outcome_group,
    # stayed in silver.py:
    load_bronze_frame, parse_metadata, build_silver, build_silver_leads,
    # already reexported in phase 1:
    mask_sender_name, detect_unmasked_sensitive_classes, deduplicate_events,
    add_message_signals, add_conversation_context, add_lead_context,
)
```

### Retrocompat smoke test — operator.py

```python
# All of these must continue to work after the consolidation
from pipeline.orchestration.operator import (
    run_cycle,
    build_monitor_snapshot,
    PipelineArtifacts,
    _incident_id,       # moved to operator_reports.py, reexported
    _get_llm_call,      # moved to operator_stages.py, reexported
    _determine_retry_stage,  # moved to operator_stages.py, reexported
    _run_validation_suite,   # duplicate removed from operator.py, reexported from operator_stages
)
```

### Edge case: imports inside `build_gold`

`build_gold` contains two lazy imports inside its function body:

```python
# Inside build_gold — these must be preserved exactly as-is when moving the function
from pipeline.transforms.conversation_enrichment import consolidate_gold_semantics
from pipeline.agent.gold_designer import GoldColumnPlan, apply_gold_column_plan
```

These are intentionally lazy to avoid circular imports at module load time. They MUST remain as lazy imports inside the function body in `gold.py`.

### Edge case: `_normalize_for_match` used in `_normalize_outcome_group`

`_normalize_outcome_group` (moving to `gold.py`) calls `_normalize_for_match` from `silver_patterns.py`. This import MUST be added to `gold.py`'s import block if not already present.

## 10. Validation Criteria

- `venv/bin/python -m pytest -q` → all 265 tests pass
- `wc -l src/pipeline/transforms/silver.py` → less than 400
- `wc -l src/pipeline/transforms/gold.py` → less than 400
- `wc -l src/pipeline/transforms/silver_aggregators.py` → less than 200
- `wc -l src/pipeline/transforms/gold_segments.py` → less than 300
- `from pipeline.transforms.silver import build_gold` → no ImportError
- `from pipeline.transforms.gold import build_gold` → no ImportError
- `from pipeline.orchestration.operator import _incident_id, _run_validation_suite` → no ImportError
- `venv/bin/python -m ruff check src/` → zero errors
- `python -c "import pipeline.transforms.gold; import pipeline.transforms.silver"` → no CircularImportError
- `git diff tests/` → no assertion logic modified

## 11. Related Specifications / Further Reading

- `spec/spec-architecture-module-cyclomatic-decomposition.md` — Phase 1 decomposition that this spec completes
- `spec/spec-architecture-project-folder-organization.md` — module organization conventions
- `spec/spec-architecture-semantic-contract-separation.md` — responsibility boundaries between transformation, quality, and orchestration layers
