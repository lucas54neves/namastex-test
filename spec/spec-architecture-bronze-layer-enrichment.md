---
title: Bronze Layer Enrichment — Typed Ingestion, Metadata Expansion and Structural Validation
version: 1.0
date_created: 2026-04-29
owner: Data Engineering
tags: [architecture, bronze, ingestion, schema, type-casting, metadata]
---

# Introduction

`src/pipeline/transforms/bronze.py` is currently a 5-line re-export shim that delegates `load_bronze_frame` entirely to `silver.py`. The Bronze layer has no parsing responsibility of its own: timestamp coercion and metadata expansion happen inside `silver.py`, mixed with Silver transformation logic. This contradicts the Medallion principle that Bronze is the type-safe, schema-validated gateway of the pipeline.

This spec promotes Bronze to a first-class transformation stage: it owns ingestion, type casting, metadata column expansion, and a structural validation output. Silver continues to receive a clean, typed DataFrame instead of raw strings.

## 1. Purpose & Scope

**Purpose:** Define the requirements and interface for an enriched Bronze stage that produces a typed, metadata-expanded DataFrame from the raw Parquet source — without altering Bronze's faithful-replica principle.

**Scope:**

New symbols defined in `src/pipeline/transforms/bronze.py`:
- `build_bronze(source_path, compiled_plan=None) -> BronzeResult` — primary entry point
- `load_bronze_frame(source_path) -> pd.DataFrame` — re-implemented here (migrated from `silver.py`)
- `parse_metadata(df) -> pd.DataFrame` — re-implemented here (migrated from `silver.py`)
- `cast_bronze_dtypes(df) -> pd.DataFrame` — new: casts deterministic-domain columns
- `validate_bronze_schema(df) -> BronzeValidationReport` — new: structural check output
- `BronzeResult` — new: typed dataclass bundling `df`, `validation_report`, and `metadata_parsed_ok`

Updated modules:
- `silver.py` — drops definitions of `load_bronze_frame` and `parse_metadata`; reexports both from `bronze.py` for retrocompatibility
- `operator_stages.py` — calls `build_bronze` instead of `load_bronze_frame` directly

**Out of scope:**
- Row-level business validation (quarantine rules remain in `quality/quarantine.py`)
- Semantic enrichment of `message_body` — Silver's responsibility
- Masking or deduplication — Silver's responsibility
- Changes to Silver, Gold, or any Parquet schema

**Intended audience:** Engineers implementing the Bronze enrichment and PR reviewers.

## 2. Definitions

| Term | Definition |
|---|---|
| Bronze layer | The first persistence layer; stores a structurally typed replica of the raw source. No business transformation is applied. |
| Faithful-replica principle | Bronze preserves the source data faithfully. Rows are not removed or rewritten; only dtypes and column structure are normalized. |
| Metadata expansion | Parsing the JSON string in the `metadata` column into flat columns prefixed `metadata_`. |
| Deterministic-domain column | A column whose complete valid value set is known and fixed (e.g., `direction ∈ {outbound, inbound}`). |
| Categorical dtype | A pandas `CategoricalDtype` with defined categories, reducing memory and enabling fast equality checks. |
| Structural validation | A schema-level check verifying that all required columns exist and have the expected dtypes, without inspecting row values. |
| BronzeResult | A dataclass bundling the enriched DataFrame, structural validation report, and a metadata-parsed flag. |
| CON-004 | Constraint from `spec-architecture-module-cyclomatic-decomposition.md`: `load_bronze_frame` must remain importable from `pipeline.transforms.bronze` and `pipeline.transforms.silver` for test monkeypatching compatibility. |

## 3. Requirements, Constraints & Guidelines

### Migration from `silver.py`

- **REQ-001**: `load_bronze_frame(source_path: str) -> pd.DataFrame` MUST be defined in `src/pipeline/transforms/bronze.py`. The current definition in `silver.py` MUST be replaced by a reexport: `from pipeline.transforms.bronze import load_bronze_frame  # noqa: F401`.
- **REQ-002**: `parse_metadata(df: pd.DataFrame) -> pd.DataFrame` MUST be defined in `src/pipeline/transforms/bronze.py`. The current definition in `silver.py` MUST be replaced by a reexport.
- **REQ-003**: Both symbols MUST remain importable via `from pipeline.transforms.silver import load_bronze_frame, parse_metadata` for retrocompatibility (CON-004).
- **REQ-004**: `silver.__all__` MUST continue to list `load_bronze_frame` and `parse_metadata` after the migration.

### Timestamp normalization

- **REQ-010**: `load_bronze_frame` MUST apply `pd.to_datetime(df["timestamp"], errors="coerce")` exactly as today. No timezone coercion is added at this stage; UTC-awareness is deferred to Silver if needed.

### Metadata expansion

- **REQ-020**: `parse_metadata` MUST parse the `metadata` column as JSON. Rows with unparseable JSON MUST yield `NaN` in all `metadata_*` columns rather than raising an exception.
- **REQ-021**: The following `metadata_*` columns MUST be present after parsing, regardless of whether the raw JSON contained the key:

| Column | Pandas dtype | Default when missing |
|---|---|---|
| `metadata_device` | `object` (string) | `""` |
| `metadata_city` | `object` (string) | `""` |
| `metadata_state` | `object` (string) | `""` |
| `metadata_response_time_sec` | `Int64` (nullable int) | `pd.NA` |
| `metadata_is_business_hours` | `boolean` (nullable bool) | `pd.NA` |
| `metadata_lead_source` | `object` (string) | `""` |

- **REQ-022**: The original `metadata` column MUST be dropped from the output DataFrame, exactly as the current `parse_metadata` behavior.

### Dtype casting

- **REQ-030**: `cast_bronze_dtypes(df: pd.DataFrame) -> pd.DataFrame` MUST cast the following columns to `CategoricalDtype` with the defined categories:

| Column | Valid categories |
|---|---|
| `direction` | `["outbound", "inbound"]` |
| `message_type` | `["text", "image", "audio", "document", "sticker"]` |
| `status` | `["sent", "delivered", "read", "failed"]` |
| `channel` | `["whatsapp"]` |
| `conversation_outcome` | `["venda_fechada", "perdido_preco", "perdido_concorrente", "ghosting", "desistencia_lead", "proposta_enviada", "em_negociacao"]` |

- **REQ-031**: Values outside the defined category set MUST NOT cause an exception. They MUST be retained as `NaN` in the categorical column (pandas default behavior for unrecognized categories when `dtype=CategoricalDtype(categories=..., ordered=False)`).
- **REQ-032**: `cast_bronze_dtypes` MUST be applied inside `build_bronze` after `parse_metadata`. It MUST NOT be called inside `load_bronze_frame` to preserve the current contract expected by existing tests that monkeypatch `load_bronze_frame`.

### Structural validation

- **REQ-040**: `validate_bronze_schema(df: pd.DataFrame) -> BronzeValidationReport` MUST check that all nine source columns are present: `message_id`, `conversation_id`, `timestamp`, `sender_phone`, `sender_name`, `message_body`, `campaign_id`, `agent_id`, `direction`.
- **REQ-041**: The report MUST record `missing_columns: list[str]`, `schema_ok: bool`, and `row_count: int`.
- **REQ-042**: `validate_bronze_schema` MUST NOT modify the DataFrame. It is a read-only check.
- **REQ-043**: The report is informational. A non-empty `missing_columns` list MUST NOT raise an exception inside `build_bronze`; the failure propagates to the caller via `BronzeResult.validation_report.schema_ok == False`.

### Primary entry point

- **REQ-050**: `build_bronze(source_path: str | Path, compiled_plan: dict | None = None) -> BronzeResult` MUST:
  1. Call `load_bronze_frame(source_path)` to read and coerce timestamps.
  2. Call `parse_metadata(df)` to expand the JSON column.
  3. Call `cast_bronze_dtypes(df)` to apply categorical dtypes.
  4. Call `validate_bronze_schema(df)` to produce the structural report.
  5. Return a `BronzeResult(df=df, validation_report=report, metadata_parsed_ok=True)`.
- **REQ-051**: `build_bronze` MUST accept an optional `compiled_plan` parameter for future use. If `None`, all casting rules are applied from the fixed defaults defined in REQ-030.
- **REQ-052**: `build_bronze` MUST NOT write to disk. Writing is the caller's responsibility (`operator_stages._run_bronze_stage`).

### Retrocompatibility constraint

- **CON-001**: `load_bronze_frame` MUST remain callable and contract-compatible from `pipeline.transforms.silver` after migration.
- **CON-002**: `parse_metadata` MUST remain callable and contract-compatible from `pipeline.transforms.silver` after migration.
- **CON-003**: The existing 351 tests MUST pass without modification of assertion logic.

### Guidelines

- **GUD-001**: `bronze.py` MUST NOT exceed 150 lines after implementation.
- **GUD-002**: `bronze.py` MUST NOT import from `silver.py` or any Silver submodule to avoid circular imports.
- **GUD-003**: `BronzeResult` MUST be a `dataclass` with `frozen=False` to allow field reassignment in `_run_bronze_stage` without copying.

## 4. Interfaces & Data Contracts

### `BronzeResult` dataclass

```python
from dataclasses import dataclass, field
import pandas as pd

@dataclass
class BronzeValidationReport:
    schema_ok: bool
    missing_columns: list[str]
    row_count: int

@dataclass
class BronzeResult:
    df: pd.DataFrame
    validation_report: BronzeValidationReport
    metadata_parsed_ok: bool
```

### `build_bronze` signature

```python
from pathlib import Path

def build_bronze(
    source_path: str | Path,
    compiled_plan: dict | None = None,
) -> BronzeResult: ...
```

### `cast_bronze_dtypes` — column map

```python
BRONZE_CATEGORICAL_COLUMNS: dict[str, list[str]] = {
    "direction": ["outbound", "inbound"],
    "message_type": ["text", "image", "audio", "document", "sticker"],
    "status": ["sent", "delivered", "read", "failed"],
    "channel": ["whatsapp"],
    "conversation_outcome": [
        "venda_fechada",
        "perdido_preco",
        "perdido_concorrente",
        "ghosting",
        "desistencia_lead",
        "proposta_enviada",
        "em_negociacao",
    ],
}
```

### Updated `_run_bronze_stage` in `operator_stages.py`

```python
from pipeline.transforms.bronze import build_bronze

def _run_bronze_stage(paths, compiled_plan):
    result = build_bronze(str(paths.raw_bronze_source), compiled_plan=compiled_plan)
    bronze_df = result.df
    log_event(logging.INFO, "bronze_loaded", rows=int(len(bronze_df)),
              schema_ok=result.validation_report.schema_ok)

    quarantine = quarantine_bronze_records(bronze_df, paths.quarantine, compiled_plan)
    bronze_df = quarantine["clean_df"]
    ...
    write_parquet(bronze_df, bronze_path)
    return {"bronze_df": bronze_df, "quarantine_report": quarantine["report"],
            "bronze_validation": result.validation_report}
```

### Schema contract — output DataFrame after `build_bronze`

| Column group | Columns | Dtype after enrichment |
|---|---|---|
| Identifiers | `message_id`, `conversation_id`, `campaign_id`, `agent_id` | `object` |
| Timestamp | `timestamp` | `datetime64[ns]` |
| Contacts | `sender_phone`, `sender_name` | `object` |
| Content | `message_body` | `object` |
| Categoricals | `direction`, `message_type`, `status`, `channel`, `conversation_outcome` | `CategoricalDtype` |
| Metadata flat | `metadata_device`, `metadata_city`, `metadata_state`, `metadata_lead_source` | `object` |
| Metadata numeric | `metadata_response_time_sec` | `Int64` (nullable) |
| Metadata bool | `metadata_is_business_hours` | `boolean` (nullable) |

## 5. Acceptance Criteria

- **AC-001**: Given `build_bronze` is called with the test Parquet fixture, when the result is inspected, then `result.df["direction"].dtype` is a `CategoricalDtype` with categories `["outbound", "inbound"]`.
- **AC-002**: Given `build_bronze` is called with the test fixture, when the result is inspected, then `"metadata_response_time_sec" in result.df.columns` is `True` and the column dtype is `Int64`.
- **AC-003**: Given `build_bronze` is called with the test fixture, when `result.validation_report` is inspected, then `schema_ok` is `True` and `missing_columns` is `[]`.
- **AC-004**: Given a Parquet where the `metadata` column contains malformed JSON for one row, when `build_bronze` is called, then no exception is raised and that row has `NaN` in all `metadata_*` columns.
- **AC-005**: Given `build_bronze` is called with a Parquet missing the `direction` column, when `result.validation_report` is inspected, then `schema_ok` is `False` and `missing_columns == ["direction"]`.
- **AC-006**: Given the migration is applied, when `from pipeline.transforms.silver import load_bronze_frame, parse_metadata` is executed, then both imports succeed without error.
- **AC-007**: Given the migration is applied, when `venv/bin/python -m pytest -q` is executed, then all 351 existing tests pass without modification of assertion logic.
- **AC-008**: Given the migration is applied, when `wc -l src/pipeline/transforms/bronze.py` is executed, then the result is less than 150.
- **AC-009**: Given the migration is applied, when `ruff check src/` is executed, then the result contains zero errors.
- **AC-010**: Given `build_bronze` is called, when `result.df["metadata"].notna().any()` is evaluated, then it returns `False` (the raw `metadata` column is always dropped).

## 6. Test Automation Strategy

- **Test file**: `tests/test_transforms.py` — add a `test_build_bronze_*` group alongside existing transform tests.
- **Test data**: Reuse the existing Parquet fixture already loaded in `conftest.py`. No new fixture files needed.
- **Unit tests required**:
  - `test_build_bronze_casts_categoricals` — covers AC-001
  - `test_build_bronze_expands_metadata` — covers AC-002
  - `test_build_bronze_validation_report_ok` — covers AC-003
  - `test_build_bronze_malformed_metadata_no_exception` — covers AC-004
  - `test_build_bronze_missing_column_report` — covers AC-005
  - `test_build_bronze_drops_raw_metadata` — covers AC-010
- **Retrocompat test**: `test_silver_still_exports_load_bronze_frame` in `tests/test_transforms.py` — covers AC-006.
- **CI/CD**: Tests run in the existing `venv/bin/python -m pytest -q` step. No new CI step required.
- **Coverage requirement**: The six new test functions fully cover `build_bronze`, `cast_bronze_dtypes`, and `validate_bronze_schema`. `parse_metadata` and `load_bronze_frame` are already covered by existing tests.

## 7. Rationale & Context

**Why `load_bronze_frame` and `parse_metadata` live in `silver.py` today:** Bronze was originally a thin pass-through; all transformation logic grew inside Silver. The decomposition specs (`spec-architecture-module-cyclomatic-decomposition.md`, `spec-architecture-decomposition-phase-two.md`) moved Silver sub-responsibilities out of `silver.py`, but Bronze itself was never promoted to a real module.

**Why Bronze should own these functions:** The Medallion architecture assigns Bronze the role of typed ingestion gateway. Putting `parse_metadata` in Silver creates an ordering problem: Silver receives a DataFrame that still has a raw JSON string column, forcing Silver to be aware of Bronze's source format. Moving these functions to Bronze means Silver always receives a flat, typed DataFrame regardless of source format changes.

**Why `cast_bronze_dtypes` is new:** Categorical dtypes for known-domain columns (direction, message_type, etc.) reduce memory footprint and make downstream `groupby` and equality checks faster. This is a safe, non-destructive change: values outside the defined category set become `NaN` rather than raising, preserving the faithful-replica principle.

**Why `build_bronze` does not write to disk:** Writing is an orchestration responsibility (`_run_bronze_stage`). Keeping `build_bronze` pure (transform only, no I/O side effects) makes it independently testable and reusable.

## 8. Dependencies & External Integrations

### Technology Platform Dependencies
- **PLT-001**: Python 3.11 — `pandas` with `CategoricalDtype`, `Int64`, `boolean` nullable dtypes; `json` stdlib.

### Internal Module Dependencies
- **INT-001**: `bronze.py` MUST import only from `stdlib` and `pandas`. It MUST NOT import from `silver.py` or any Silver submodule.
- **INT-002**: `silver.py` gains two reexports from `bronze.py`. No circular dependency is introduced because `bronze.py` does not import from `silver.py`.
- **INT-003**: `operator_stages.py` replaces `load_bronze_frame` call with `build_bronze`. Import path changes from `pipeline.transforms.bronze` (already the source) to `pipeline.transforms.bronze.build_bronze`.

### Data Dependencies
- **DAT-001**: Input Parquet at `docs/conversations_bronze.parquet` — 9 source columns as defined in `docs/data-dictionary-data-ai-engineering.md`. No format change.

## 9. Examples & Edge Cases

```python
# Happy path
from pipeline.transforms.bronze import build_bronze

result = build_bronze("docs/conversations_bronze.parquet")
assert result.validation_report.schema_ok
assert result.df["direction"].dtype.name == "category"
assert "metadata" not in result.df.columns
assert "metadata_response_time_sec" in result.df.columns

# Edge case: value outside category set
import pandas as pd
from pipeline.transforms.bronze import cast_bronze_dtypes

df = pd.DataFrame({"direction": ["outbound", "UNKNOWN"], "message_type": ["text", "text"],
                   "status": ["sent", "sent"], "channel": ["whatsapp", "whatsapp"],
                   "conversation_outcome": ["venda_fechada", "venda_fechada"]})
result = cast_bronze_dtypes(df)
assert pd.isna(result["direction"].iloc[1])   # "UNKNOWN" becomes NaN, not error

# Edge case: missing metadata key
import json
df = pd.DataFrame({"metadata": ['{"device": "android"}', '{"city": "SP"}']})
from pipeline.transforms.bronze import parse_metadata
out = parse_metadata(df)
assert "metadata_device" in out.columns
assert "metadata_city" in out.columns
assert out["metadata_device"].iloc[1] == ""     # missing key → default ""

# Edge case: malformed JSON
df = pd.DataFrame({"metadata": ['{"device": "android"}', 'not_json']})
out = parse_metadata(df)                         # must not raise
assert pd.isna(out["metadata_device"].iloc[1])  # bad JSON row → NaN

# Retrocompat
from pipeline.transforms.silver import load_bronze_frame, parse_metadata  # must still work
```

## 10. Validation Criteria

- `venv/bin/python -m pytest -q` → all 351 existing tests pass
- `venv/bin/python -m pytest tests/test_transforms.py -q` → all six new `test_build_bronze_*` tests pass
- `from pipeline.transforms.silver import load_bronze_frame, parse_metadata` → no `ImportError`
- `from pipeline.transforms.bronze import build_bronze, BronzeResult` → no `ImportError`
- `result.df["direction"].dtype.name == "category"` for a standard fixture run
- `"metadata" not in result.df.columns` for any valid input
- `wc -l src/pipeline/transforms/bronze.py` → less than 150
- `venv/bin/python -m ruff check src/` → zero errors
- `venv/bin/python -m mypy src/pipeline/transforms/bronze.py` → zero type errors

## 11. Related Specifications / Further Reading

- `spec/spec-architecture-module-cyclomatic-decomposition.md` — Phase 1 decomposition that established Silver submodules
- `spec/spec-architecture-decomposition-phase-two.md` — Phase 2 that canonicalized `gold.py`; Bronze was out of scope
- `spec/spec-architecture-semantic-contract-separation.md` — responsibility boundaries between Bronze, Silver, and Gold
- `docs/data-dictionary-data-ai-engineering.md` — source schema and column definitions
