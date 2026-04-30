---
title: CDC Row-Level Source Fingerprint to Replace mtime/size Detection
version: 1.0
date_created: 2026-04-29
tags: [architecture, pipeline, bronze, cdc, incremental, state]
---

# Introduction

The pipeline currently detects source changes by comparing the file's `mtime` and `size_bytes`
against the value stored in `state/pipeline_state.json`. This approach produces false negatives
when two versions of the file have the same size and the same modification timestamp (e.g., file
copied or overwritten with identical byte count), and false positives when only file metadata
changes without any new data rows. It also forces a full reprocessing of every layer every time
any new row arrives, even if only a small batch was appended.

This specification replaces the file-level fingerprint with a row-level CDC (Change Data Capture)
strategy based on the stable natural key `message_id`, enabling precise detection of new rows and
laying the foundation for incremental stage execution.

## 1. Purpose & Scope

**Purpose**: Replace `SourceFingerprint` (path + size_bytes + mtime) with a content-addressable,
row-level change record that accurately identifies which `message_id` values are new since the
last successful run.

**Scope**: This specification covers:
- The new `SourceCDCState` data structure stored in `state/pipeline_state.json`.
- The replacement of `build_source_fingerprint` and `has_source_changed` in
  `src/pipeline/runtime/state.py`.
- The operator integration in `src/pipeline/orchestration/operator.py`.
- The incremental execution path that restricts Silver and Gold reprocessing to only the rows
  that are genuinely new.
- Backward compatibility with the existing state file format.

**Out of scope**:
- Row-level updates or deletes (the source is an append-only Parquet file in this project).
- Streaming ingestion or Kafka/Kinesis integration.
- Unity Catalog Delta Lake change feeds (treated as a future extension).
- Changes to the Databricks deploy workflow beyond path compatibility.

**Audience**: engineers implementing or reviewing changes to `runtime/state.py`,
`orchestration/operator.py`, `orchestration/operator_stages.py`, and the daemon script.

## 2. Definitions

| Term | Definition |
|---|---|
| **CDC** | Change Data Capture — tracking which rows are new or changed since the last processed state. |
| **Natural key** | A column or column combination that uniquely identifies a row in the source data by business meaning. In this project: `message_id`. |
| **SourceCDCState** | The new dataclass that replaces `SourceFingerprint`. Stores the SHA-256 of the sorted `message_id` set plus the known ID set itself. |
| **new_ids** | The set difference `current_message_ids − known_message_ids`. |
| **Full run** | All three stages (Bronze → Silver → Gold) are executed on the full dataset. |
| **Incremental run** | Only the rows whose `message_id` is in `new_ids` are passed to Silver and Gold. |
| **mtime** | Unix modification timestamp of a file, as returned by `os.stat().st_mtime`. |
| **SHA-256 digest** | A 64-character hex string produced by hashing the canonical representation of the sorted `message_id` set. |
| **state file** | `state/pipeline_state.json` — the persistent JSON file where pipeline state is written after each successful run. |
| **fast pre-check** | An optional size/mtime comparison used to skip the expensive Parquet read when the file has not changed at the filesystem level. |

## 3. Requirements, Constraints & Guidelines

### Requirements

- **REQ-001**: The system MUST compute the set of `message_id` values present in the source
  Parquet file after every successful run and persist it in the state file.
- **REQ-002**: On each cycle, the system MUST compute the set difference
  `current_message_ids − known_message_ids` to produce `new_ids`.
- **REQ-003**: If `new_ids` is empty and `force=False`, the pipeline MUST be skipped
  (status `skipped_no_source_change`), consistent with the current behaviour.
- **REQ-004**: If `new_ids` is non-empty, the pipeline MUST execute at minimum the Silver and
  Gold stages, and MAY restrict their input to only the rows whose `message_id` is in `new_ids`
  (incremental run).
- **REQ-005**: The `SourceCDCState` MUST include a SHA-256 digest of the sorted `message_id`
  set so that equality can be checked in O(1) without deserializing the full ID set.
- **REQ-006**: Backward compatibility MUST be preserved: if `state/pipeline_state.json` contains
  a `last_source_fingerprint` key (old format) but no `last_cdc_state` key, the system MUST
  treat the state as "no prior CDC state" and trigger a full run.
- **REQ-007**: After a successful full run, the system MUST persist `last_cdc_state` and MUST
  remove `last_source_fingerprint` from the state file.
- **REQ-008**: The fast pre-check (mtime + size_bytes) MAY be retained as a first-pass filter
  to avoid reading the Parquet file when the filesystem reports no change. When the fast
  pre-check indicates no change, the system MUST still verify via the SHA-256 digest if a prior
  `last_cdc_state` exists.
- **REQ-009**: The count of new rows (`len(new_ids)`) MUST be included in the run report and
  in the structured log event `source_change_evaluated`.
- **REQ-010**: The incremental run path MUST produce output artefacts that are
  schema-identical to those produced by a full run.

### Constraints

- **CON-001**: `message_id` is the only column used as the natural key. Multi-column keys are
  out of scope for this version.
- **CON-002**: The ID set stored in the state file MUST be serialised as a JSON array of
  strings, sorted lexicographically, to ensure deterministic SHA-256 digests across runs and
  platforms.
- **CON-003**: The state file MUST remain a single JSON file (`state/pipeline_state.json`).
  No additional files may be introduced for CDC state in this version.
- **CON-004**: The Parquet read for CDC comparison MUST request only the `message_id` column
  (column pruning) to minimise memory and I/O overhead.
- **CON-005**: The incremental run is only valid when the source is append-only. If the Bronze
  validation detects that rows previously known have disappeared (i.e.,
  `known_ids − current_ids` is non-empty), the system MUST fall back to a full run and emit a
  `WARNING` log event `cdc_shrink_detected`.

### Guidelines

- **GUD-001**: Keep `build_source_fingerprint` and `has_source_changed` in `state.py` but mark
  them as deprecated via a module-level comment. Remove them only after the incremental path is
  validated in production.
- **GUD-002**: The SHA-256 digest should be computed from
  `hashlib.sha256(json.dumps(sorted_ids).encode()).hexdigest()` to ensure cross-platform
  reproducibility.
- **GUD-003**: Log the size of `new_ids`, `known_ids`, and the digest at `INFO` level for every
  evaluation, regardless of whether the pipeline runs or is skipped.

## 4. Interfaces & Data Contracts

### 4.1 SourceCDCState dataclass

```python
@dataclass(frozen=True)
class SourceCDCState:
    digest: str           # SHA-256 hex of sorted message_id list
    known_ids: frozenset[str]  # full set of known message_ids (in-memory only)
    row_count: int        # len(known_ids) for quick sanity checks

    def as_dict(self) -> dict:
        return {
            "digest": self.digest,
            "known_ids": sorted(self.known_ids),  # serialise as sorted list
            "row_count": self.row_count,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SourceCDCState":
        ids = frozenset(data["known_ids"])
        return cls(digest=data["digest"], known_ids=ids, row_count=data["row_count"])
```

### 4.2 New functions in `runtime/state.py`

```python
def build_cdc_state(source_path: Path) -> SourceCDCState:
    """Read only the message_id column from the source Parquet and build a SourceCDCState."""

def compute_new_ids(current: SourceCDCState, previous: SourceCDCState | None) -> frozenset[str]:
    """Return current.known_ids - previous.known_ids (empty set if previous is None triggers full run)."""

def has_source_changed_cdc(current: SourceCDCState, previous: SourceCDCState | None) -> bool:
    """True if previous is None or current.digest != previous.digest."""
```

### 4.3 State file schema (`state/pipeline_state.json`)

```json
{
  "last_cdc_state": {
    "digest": "e3b0c44298fc1c149afb...",
    "known_ids": ["msg_0001", "msg_0002", "..."],
    "row_count": 15432
  },
  "quality_baseline": { "...": "..." },
  "runs": []
}
```

The key `last_source_fingerprint` MUST NOT be written by new code. It may be present in
existing state files and must be ignored gracefully.

### 4.4 Run report additions

The field `source_change_evaluation` in `reports/monitoring/latest_run_report.json` MUST be
extended:

```json
{
  "source_change_evaluation": {
    "method": "cdc_message_id",
    "changed": true,
    "new_row_count": 312,
    "total_row_count": 15432,
    "digest": "e3b0c44298fc1c149afb...",
    "run_type": "incremental"
  }
}
```

`run_type` is one of `"full"` or `"incremental"`.

### 4.5 Structured log events

| Event key | Level | New fields |
|---|---|---|
| `source_change_evaluated` | INFO | `method`, `new_row_count`, `total_row_count`, `digest`, `run_type` |
| `cdc_shrink_detected` | WARNING | `missing_row_count`, `fallback` |
| `cdc_state_persisted` | INFO | `digest`, `row_count` |

## 5. Acceptance Criteria

- **AC-001**: Given a state file with no `last_cdc_state` key, when the pipeline runs, then it
  performs a full run and writes `last_cdc_state` to the state file.
- **AC-002**: Given a state file with `last_cdc_state.digest` equal to the current source
  digest, when the pipeline runs with `force=False`, then it exits with
  `status="skipped_no_source_change"` and does not rewrite any artefact.
- **AC-003**: Given a state file with a prior CDC state and 200 new `message_id` values in the
  source, when the pipeline runs, then `new_row_count=200` is logged and only those 200 rows
  flow into the Silver transform (incremental path).
- **AC-004**: Given a state file with `last_source_fingerprint` (legacy format) and no
  `last_cdc_state`, when the pipeline runs, then it treats it as first run, performs a full run,
  and writes `last_cdc_state` without `last_source_fingerprint`.
- **AC-005**: Given a source file where `known_ids − current_ids` is non-empty (rows
  disappeared), when the pipeline evaluates CDC, then it logs `cdc_shrink_detected` at WARNING
  and falls back to a full run.
- **AC-006**: Given two consecutive runs where no rows change between them, when the second run
  executes, then the SHA-256 digest is identical in both state files.
- **AC-007**: Given an incremental run, when the Gold artefact is inspected, then it has the
  same schema and column set as the artefact produced by a full run.
- **AC-008**: Given `force=True`, when the pipeline runs regardless of CDC state, then it
  performs a full run and updates `last_cdc_state` to reflect the current full row set.

## 6. Test Automation Strategy

- **Test Levels**: Unit (state functions), Integration (operator cycle with synthetic Parquet
  fixtures).
- **Frameworks**: `pytest`, existing `conftest.py` fixtures.
- **Test Data Management**: synthetic Parquet files constructed in-memory via `pandas` within
  test fixtures; no external files required.
- **New test file**: `tests/test_cdc_state.py` covering `build_cdc_state`,
  `compute_new_ids`, `has_source_changed_cdc`, and state serialisation round-trip.
- **Extend existing**: `tests/test_requirements_adherence.py` should add a case verifying that
  after a successful run `last_cdc_state` is present in the state file and
  `last_source_fingerprint` is absent.
- **Coverage requirement**: all new functions in `runtime/state.py` must reach 100% branch
  coverage in unit tests.
- **CI/CD Integration**: existing `venv/bin/python -m pytest -q` command; no new commands
  required.

## 7. Rationale & Context

The current `SourceFingerprint` approach has two failure modes relevant to this project:

1. **False negative**: if the source file is replaced with a file of exactly the same byte count
   and an identical or earlier mtime (common in copy-and-overwrite workflows), the pipeline
   silently skips processing new data.
2. **Unnecessary full reprocessing**: any single new row forces a full Bronze → Silver → Gold
   cycle over all ~15 000 rows, which is wasteful when only a small batch was appended.

CDC via `message_id` set comparison solves both: the SHA-256 digest changes whenever any ID is
added (fixing the false negative), and `compute_new_ids` gives the pipeline enough information
to process only the delta (enabling incremental runs and fixing the wasteful full reprocessing).

The decision to store the full ID set (not just the digest) in the state file is intentional:
it makes `compute_new_ids` deterministic and allows the shrink-detection guard (`CON-005`)
without re-reading the previous Parquet file.

The fast pre-check (mtime + size_bytes) is retained as an optimisation: reading the `message_id`
column of a ~15 000-row Parquet file is inexpensive, but in larger datasets the pre-check saves
the Parquet read entirely on unchanged cycles.

## 8. Dependencies & External Integrations

### External Systems
- None new.

### Infrastructure Dependencies
- **INF-001**: `state/pipeline_state.json` — must remain writable by the pipeline process;
  schema is extended but not replaced.

### Data Dependencies
- **DAT-001**: Source Parquet file (`docs/conversations_bronze.parquet` locally,
  Unity Catalog Volume path on Databricks) — the `message_id` column must be present and
  non-null for all rows. Nulls in `message_id` must be treated as a Bronze validation error
  before CDC evaluation.

### Technology Platform Dependencies
- **PLT-001**: Python standard library `hashlib` and `json` — no new third-party dependencies.
- **PLT-002**: `pandas.read_parquet` with `columns=["message_id"]` for column pruning — already
  a project dependency.

## 9. Examples & Edge Cases

### First run (no prior state)

```python
current = build_cdc_state(source_path)
# previous = None
new_ids = compute_new_ids(current, previous=None)
# new_ids == current.known_ids  →  full run
```

### Incremental run (300 new rows)

```python
current = build_cdc_state(source_path)
# previous.known_ids = {"msg_0001", ..., "msg_15000"}
new_ids = compute_new_ids(current, previous)
# new_ids = {"msg_15001", ..., "msg_15300"}  →  incremental run
bronze_df_incremental = bronze_df[bronze_df["message_id"].isin(new_ids)]
```

### Shrink detected (source lost rows)

```python
missing = previous.known_ids - current.known_ids
if missing:
    log_event(logging.WARNING, "cdc_shrink_detected",
              missing_row_count=len(missing), fallback="full_run")
    new_ids = current.known_ids  # treat as full run
```

### Legacy state migration

```python
state = load_pipeline_state(state_path)
if "last_cdc_state" in state:
    previous_cdc = SourceCDCState.from_dict(state["last_cdc_state"])
else:
    previous_cdc = None  # triggers full run regardless of last_source_fingerprint
```

### SHA-256 computation

```python
import hashlib, json

def _digest(ids: frozenset[str]) -> str:
    canonical = json.dumps(sorted(ids), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()
```

## 10. Validation Criteria

- `state/pipeline_state.json` after a successful run contains `last_cdc_state` with `digest`,
  `known_ids` (sorted list), and `row_count`.
- `state/pipeline_state.json` does NOT contain `last_source_fingerprint` after the first run
  with the new implementation.
- Two runs over the same unchanged source produce the same `digest` value.
- A run over a source with N new rows reports `new_row_count=N` in the run report.
- `venv/bin/python -m pytest tests/test_cdc_state.py -q` exits 0 with no failures.
- `venv/bin/python -m pytest tests/test_requirements_adherence.py -q` exits 0 with no failures.
- Running the pipeline with `PIPELINE_ENABLE_LLM_ENRICHMENT=0 venv/bin/python scripts/run_pipeline.py --force`
  produces all expected Gold artefacts after the migration.

## 11. Related Specifications / Further Reading

- [spec-architecture-operator-run-cycle-di-decomposition.md](./spec-architecture-operator-run-cycle-di-decomposition.md) — operator decomposition context
- [spec-architecture-pipeline-spec-runtime-parity.md](./spec-architecture-pipeline-spec-runtime-parity.md) — runtime contract alignment
- [spec-process-daemon-watchdog-exponential-backoff.md](./spec-process-daemon-watchdog-exponential-backoff.md) — daemon cycle context
