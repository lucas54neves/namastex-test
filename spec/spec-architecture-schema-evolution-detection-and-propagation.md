---
title: Architecture Specification for Schema Evolution Detection and Controlled Propagation Across Bronze, Silver, Gold, and Gold Macro
version: 1.0
date_created: 2026-04-30
last_updated: 2026-04-30
owner: Lucas Neves
tags: [architecture, pipeline, schema, governance, bronze, silver, gold, gold-macro, agentic]
---

# Introduction

This specification defines a controlled schema evolution contract for the medallion pipeline. It establishes how the pipeline shall detect unexpected, missing, renamed, or removed columns in the Bronze source, how those events shall be reported and governed, and how known optional columns shall be propagated through Silver, Gold, and Gold Macro without silent data loss.

The current pipeline behavior was triaged as fail-soft: extra source columns pass silently into Bronze and Silver and are then dropped without warning at Gold aggregation. The objective of this specification is to replace silent drift with explicit detection, classification, propagation policy, and governance signals.

## 1. Purpose & Scope

This specification defines requirements, constraints, interfaces, and validation rules for handling source schema changes end-to-end in the medallion pipeline.

Scope:

- Detection of schema drift at Bronze ingestion, including unexpected top-level columns, unexpected metadata keys, missing required columns, type mismatches, and category-domain mismatches.
- Classification of each drift event as `expected`, `optional_known`, `unknown`, `missing_required`, `type_mismatch`, or `category_drift`.
- Policy resolution per drift class, including `passthrough_with_alert`, `passthrough_silent`, `quarantine`, and `block`.
- Controlled propagation of approved optional columns from Bronze through Silver, Gold, and Gold Macro.
- Emission of a structured schema drift report consumable by the agentic governance layer.
- Versioning of the schema contract in `config/pipeline_spec.json`.

Out of scope:

- Inferring business semantics for newly observed columns.
- Auto-promoting unknown columns to Gold or Gold Macro without an explicit governance decision.
- Replacing the medallion architecture or the existing CDC fingerprint strategy.
- Modifying the LLM enrichment contract beyond drift surfacing.

Intended audience:

- Engineers implementing schema drift handling in `src/pipeline/transforms/bronze.py`, `silver.py`, `gold.py`, `gold_macro.py`, and `src/pipeline/quality/`.
- Reviewers validating the drift reporting and propagation behavior.
- The agentic governance layer responsible for converting drift events into proposals.
- Future maintainers responsible for evolving the source contract.

Assumptions:

- The repository-local execution path is the canonical source of operational truth.
- The Bronze input remains a parquet file with a `metadata` JSON string column as defined today.
- The agentic layer described in the existing architecture specs is the destination for high-severity drift events.

## 2. Definitions

- **Schema Drift**: Any deviation between the observed Bronze input schema and the contract declared in `config/pipeline_spec.json`.
- **Schema Contract**: The set of declared required, optional, and policy-controlled columns and types in `config/pipeline_spec.json` for Bronze, Silver, Gold, and Gold Macro.
- **Schema Contract Version**: An integer field in `config/pipeline_spec.json` that increments when the contract changes in a way that affects propagation or validation.
- **Required Column**: A column declared mandatory at a given layer. Absence is a hard failure unless explicitly downgraded.
- **Optional Known Column**: A column declared in the contract as expected-but-not-mandatory. Absence is non-fatal. Presence triggers structured handling.
- **Unknown Column**: A column observed in the input but not declared anywhere in the contract.
- **Unexpected Metadata Key**: A key inside the `metadata` JSON column that is not declared in `bronze.metadata_fields` or `bronze.optional_metadata_fields`.
- **Drift Class**: One of `expected`, `optional_known`, `unknown`, `missing_required`, `type_mismatch`, `category_drift`.
- **Propagation Policy**: The decision applied to a drift class at a specific layer. One of `passthrough_with_alert`, `passthrough_silent`, `quarantine`, `block`.
- **Drift Report**: A structured artifact emitted per pipeline run that lists drift events and the policy decision applied.
- **Carry-Through**: Propagation of an approved optional or unknown column into Silver and Gold under a controlled namespace.
- **Promotion**: A governance action that moves a previously unknown column into the contract as required or optional and integrates it into Silver or Gold transforms.
- **Governance Layer**: The agentic layer that consumes drift events and produces proposals for contract changes.

## 3. Requirements, Constraints & Guidelines

### Detection

- **REQ-001**: Bronze ingestion shall classify every observed source column into exactly one drift class on every run.
- **REQ-002**: Bronze ingestion shall classify every observed metadata key under the same drift class taxonomy after metadata parsing.
- **REQ-003**: Detection shall produce a deterministic result given a fixed input and a fixed contract version.
- **REQ-004**: Detection shall not depend on optional LLM enrichment.
- **REQ-005**: Missing required columns shall be detected with the same severity as today and shall surface in `BronzeValidationReport`.
- **REQ-006**: Type mismatches against declared dtypes shall be detected and classified as `type_mismatch` rather than silently coerced when the coercion would lose information.
- **REQ-007**: Category-domain drift in `BRONZE_CATEGORICAL_COLUMNS` shall be detected and classified as `category_drift` when an observed value is outside the declared category set.

### Policy resolution

- **REQ-010**: The pipeline shall resolve a single propagation policy per drift event using the contract declarations in `config/pipeline_spec.json`.
- **REQ-011**: The default policy for `unknown` top-level columns shall be `passthrough_with_alert`.
- **REQ-012**: The default policy for `unknown` metadata keys shall be `passthrough_with_alert`.
- **REQ-013**: The default policy for `missing_required` shall be `block` for Bronze required columns and `block` for Silver and Gold required columns.
- **REQ-014**: The default policy for `type_mismatch` and `category_drift` shall be `quarantine` at the row level when feasible, otherwise `passthrough_with_alert` at the run level.
- **REQ-015**: The contract shall allow per-column overrides through `bronze.column_policies` and `silver.column_policies`.
- **REQ-016**: A `block` decision at any layer shall fail the run with a non-zero exit and a clear drift summary.

### Propagation through Silver

- **REQ-020**: Silver transforms shall preserve any column listed in `silver.preserve_extra_columns` from Bronze without renaming.
- **REQ-021**: Silver transforms shall preserve any column whose policy resolves to `passthrough_with_alert` or `passthrough_silent` under the namespace `bronze_passthrough__<column_name>` when the column is not declared in `silver.preserve_extra_columns`.
- **REQ-022**: Silver aggregators shall not crash on the presence of preserved extra columns.
- **REQ-023**: Silver aggregators shall apply a documented aggregation rule for preserved extras at the lead level. The default rule shall be `first_non_null`.
- **REQ-024**: Silver shall log every preserved extra column once per run with its source layer, drift class, applied policy, and aggregation rule.

### Propagation through Gold

- **REQ-030**: Gold shall not silently drop columns. Any column dropped during Gold aggregation shall be enumerated in the drift report with reason `not_in_gold_contract`.
- **REQ-031**: A column shall reach Gold only if it is declared in `gold.required_columns`, `gold.optional_columns`, or `gold.passthrough_columns`.
- **REQ-032**: Promotion of an unknown column to Gold shall require a contract update and a schema contract version bump.
- **REQ-033**: Gold aggregation rules for `gold.optional_columns` and `gold.passthrough_columns` shall be declared in `gold.aggregation_rules` with one of `first_non_null`, `mode`, `max`, `mean`, `sum`, `any`, `all`, `last_non_null`.

### Propagation through Gold Macro

- **REQ-040**: Gold Macro shall consume only dimensions and metrics declared in `gold_macro.categorical_dimensions` and `gold_macro.numeric_metrics`.
- **REQ-041**: Gold Macro shall not include dimensions for `passthrough_columns` unless they are explicitly added to `gold_macro.categorical_dimensions`.
- **REQ-042**: When a Gold Macro dimension references a column that is not present in Gold, Gold Macro shall emit a row with `dimension_value = "sem_contrato"` and `lead_count = 0` rather than crashing.
- **REQ-043**: Schema contract version shall be embedded in every Gold Macro snapshot row through a stable column `schema_contract_version`.

### Reporting

- **REQ-050**: Every pipeline run shall emit `reports/monitoring/schema_drift_<run_id>.json`.
- **REQ-051**: The drift report shall include drift events classified by layer, drift class, applied policy, observed example values masked according to the existing privacy contract, and the affected row count.
- **REQ-052**: The drift report shall include a `summary` block with counts per drift class and the highest severity policy applied.
- **REQ-053**: When at least one drift event has policy `passthrough_with_alert`, the run report shall surface a top-level `schema_drift_alert` flag.
- **REQ-054**: When the agentic governance layer is enabled, the drift report shall be picked up as input for proposal generation, consistent with the existing impact-governed agent contract.

### Governance and versioning

- **REQ-060**: `config/pipeline_spec.json` shall include a top-level `schema_contract_version` integer.
- **REQ-061**: Contract changes that affect detection or propagation shall increment `schema_contract_version`.
- **REQ-062**: A drift event observed under a contract version shall be tagged with the contract version that produced the classification.
- **REQ-063**: The agentic governance layer shall be the only authorized actor for proposing automated promotion of an `unknown` column into the contract.

### Constraints

- **CON-001**: Implementation shall use the repository-local virtual environment as defined in `AGENTS.md`.
- **CON-002**: Implementation shall not require network access to detect or report drift.
- **CON-003**: Implementation shall not weaken the existing privacy contract. Drift report examples shall be masked using the existing masking utilities.
- **CON-004**: Implementation shall preserve the existing CDC fingerprint behavior. Drift detection shall be additive to fingerprinting.
- **CON-005**: Implementation shall not change the published schemas of Silver, Gold, or Gold Macro unless a `passthrough_columns` declaration is added in the contract.
- **CON-006**: Implementation shall not introduce silent type coercion for `type_mismatch` events on declared columns.
- **CON-007**: Implementation shall not auto-promote unknown columns to Silver or Gold required sets at runtime.

### Guidelines

- **GUD-001**: Prefer surfacing drift through structured reports over crashing the run when the drift class is non-fatal.
- **GUD-002**: Prefer namespaced carry-through (`bronze_passthrough__*`) over polluting the canonical Silver or Gold schema.
- **GUD-003**: Prefer declaring expected optional columns in the contract over relying on default unknown handling.
- **GUD-004**: Prefer minimum-information masking when surfacing observed sample values for unknown columns.
- **GUD-005**: Prefer agent-driven proposals over implicit contract changes for promotion to required.

### Patterns

- **PAT-001**: Detection pattern: read input, classify columns, classify metadata keys, resolve policies, build drift events, attach to result objects.
- **PAT-002**: Propagation pattern: declare in contract, then preserve in Silver, then declare in Gold, then optionally surface in Gold Macro.
- **PAT-003**: Governance pattern: drift report -> agentic proposal -> contract update -> contract version bump -> code change adopting the new column.

## 4. Interfaces & Data Contracts

### 4.1 Relevant files

| Path | Role | Required outcome |
| --- | --- | --- |
| `src/pipeline/transforms/bronze.py` | Bronze ingestion and validation | Adds drift classification, metadata key classification, dtype and category checks |
| `src/pipeline/transforms/silver.py` and `silver_*.py` | Silver build and aggregation | Preserves declared and policy-controlled extras; never crashes on extras |
| `src/pipeline/transforms/gold.py` | Gold aggregation | Drops only contract-declared columns; emits drop reasons; supports `optional_columns` and `passthrough_columns` |
| `src/pipeline/transforms/gold_macro.py` | Gold Macro snapshot | Reads dimensions and metrics from contract; tolerates missing columns; embeds `schema_contract_version` |
| `src/pipeline/quality/quality.py` | Validation contract | Consumes drift report; aligns enforcement with declared policies |
| `src/pipeline/quality/quarantine.py` | Quarantine path | Routes `type_mismatch` and `category_drift` rows when the policy is `quarantine` |
| `config/pipeline_spec.json` | Contract source of truth | Adds `schema_contract_version`, `optional_columns`, `passthrough_columns`, `column_policies`, `aggregation_rules`, `categorical_dimensions`, `numeric_metrics` |
| `reports/monitoring/schema_drift_<run_id>.json` | Drift report artifact | Emitted on every run |
| `tests/test_bronze.py`, `tests/test_silver.py`, `tests/test_gold.py`, `tests/test_quality.py`, `tests/test_jobs.py` | Test coverage | Adds drift classification and propagation tests |

### 4.2 Contract additions in `config/pipeline_spec.json`

The following keys shall be added or extended:

```jsonc
{
  "schema_contract_version": 2,
  "bronze": {
    "required_columns": ["..."],
    "optional_columns": ["lead_segment_hint", "agent_team"],
    "passthrough_columns": [],
    "metadata_fields": ["device", "city", "state", "response_time_sec", "is_business_hours", "lead_source"],
    "optional_metadata_fields": ["utm_term", "utm_content"],
    "column_policies": {
      "lead_segment_hint": "passthrough_with_alert",
      "agent_team": "passthrough_silent"
    },
    "category_domains": {
      "direction": ["outbound", "inbound"],
      "message_type": ["text", "image", "audio", "document", "sticker"]
    },
    "unknown_column_default_policy": "passthrough_with_alert",
    "unknown_metadata_default_policy": "passthrough_with_alert"
  },
  "silver": {
    "preserve_extra_columns": ["lead_segment_hint", "agent_team"],
    "extra_aggregation_rules": {
      "lead_segment_hint": "first_non_null",
      "agent_team": "mode"
    }
  },
  "gold": {
    "required_columns": ["..."],
    "optional_columns": ["agent_team"],
    "passthrough_columns": [],
    "aggregation_rules": {
      "agent_team": "mode"
    }
  },
  "gold_macro": {
    "categorical_dimensions": ["..."],
    "numeric_metrics": ["..."]
  }
}
```

### 4.3 Drift report contract

`reports/monitoring/schema_drift_<run_id>.json` shall match this shape:

```json
{
  "run_id": "string",
  "schema_contract_version": 2,
  "summary": {
    "by_class": {
      "expected": 14,
      "optional_known": 1,
      "unknown": 1,
      "missing_required": 0,
      "type_mismatch": 0,
      "category_drift": 0
    },
    "highest_policy_applied": "passthrough_with_alert",
    "schema_drift_alert": true
  },
  "events": [
    {
      "layer": "bronze",
      "scope": "top_level",
      "column": "lead_segment_hint",
      "drift_class": "optional_known",
      "applied_policy": "passthrough_with_alert",
      "row_count": 1234,
      "non_null_count": 1100,
      "sample_values_masked": ["segment_a", "segment_b"],
      "propagation": {
        "silver": "preserve_extra_columns",
        "gold": "not_in_gold_contract",
        "gold_macro": "not_in_dimensions"
      },
      "contract_version_at_event": 2
    }
  ]
}
```

### 4.4 Bronze API additions

`BronzeValidationReport` shall be extended with the following fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `schema_ok` | bool | Existing field. True if and only if no `missing_required` or `block`-policy event occurred. |
| `missing_columns` | list[str] | Existing field. |
| `unknown_columns` | list[str] | New. Top-level columns not declared in the contract. |
| `optional_known_columns_present` | list[str] | New. Optional columns observed in the input. |
| `unknown_metadata_keys` | list[str] | New. Metadata keys not declared in the contract. |
| `type_mismatch_columns` | list[dict] | New. Each dict: `column`, `expected_dtype`, `observed_dtype`. |
| `category_drift_columns` | list[dict] | New. Each dict: `column`, `unexpected_values_masked`. |
| `applied_policies` | dict[str, str] | New. Resolved policy per drift event keyed by `<scope>:<column>`. |
| `contract_version` | int | New. Echoes `schema_contract_version`. |

`BronzeResult` shall expose `validation_report` and a new `drift_report_path: Path | None` pointing at the emitted artifact.

### 4.5 Silver propagation contract

Silver shall support two preservation modes:

| Mode | Trigger | Output column name |
| --- | --- | --- |
| Declared preservation | Column appears in `silver.preserve_extra_columns` | Same name as in Bronze |
| Namespaced carry-through | Column has policy `passthrough_with_alert` or `passthrough_silent` and is not declared in `silver.preserve_extra_columns` | `bronze_passthrough__<column_name>` |

Aggregation at the lead level shall use `silver.extra_aggregation_rules` if declared, otherwise `first_non_null`.

### 4.6 Gold propagation contract

Gold shall partition columns into four sets at finalization:

| Set | Source | Behavior |
| --- | --- | --- |
| `gold.required_columns` | Existing | Must be present; absence is a hard failure |
| `gold.optional_columns` | New | Preserved if upstream provides it; absence does not fail |
| `gold.passthrough_columns` | New | Preserved verbatim from Silver, including `bronze_passthrough__*` names |
| Anything else | Implicit | Dropped, with the drop reason recorded as `not_in_gold_contract` in the drift report |

### 4.7 Gold Macro propagation contract

Gold Macro shall:

- Use `gold_macro.categorical_dimensions` for dimensional rows.
- Use `gold_macro.numeric_metrics` for the numeric snapshot block.
- Embed `schema_contract_version` on every emitted row.
- Tolerate missing columns by emitting a single placeholder row with `dimension_value = "sem_contrato"` and `lead_count = 0` rather than failing.

## 5. Acceptance Criteria

- **AC-001**: Given a Bronze input that contains every required column and no extra columns, When Bronze ingestion runs, Then the drift report shall contain zero events with class `unknown`, `missing_required`, `type_mismatch`, or `category_drift`.
- **AC-002**: Given a Bronze input that adds a new column `lead_segment_hint` declared as `optional_known` with policy `passthrough_with_alert`, When the pipeline runs, Then the drift report shall contain one event with `drift_class = optional_known`, the run shall not fail, the column shall be preserved in Silver under the same name, and the run report shall raise `schema_drift_alert = true`.
- **AC-003**: Given a Bronze input that adds a column `unexpected_x` not declared in the contract, When the pipeline runs with default policy `passthrough_with_alert`, Then the column shall reach Silver as `bronze_passthrough__unexpected_x`, shall not reach Gold, and the drift report shall list one event with class `unknown`.
- **AC-004**: Given a Bronze input missing a required column, When the pipeline runs, Then the run shall fail with a non-zero exit and the drift report shall list a `missing_required` event with policy `block`.
- **AC-005**: Given a Bronze input where `direction` contains a value outside the declared category set, When the pipeline runs, Then the affected rows shall be quarantined or the event shall be reported as `category_drift` according to the configured policy, and the published Gold shall not contain rows derived from the unexpected category.
- **AC-006**: Given a column declared in `gold.optional_columns`, When the pipeline runs, Then Gold shall preserve the column when present in Silver and shall not fail when it is absent.
- **AC-007**: Given a column declared in `gold.passthrough_columns`, When the pipeline runs, Then Gold shall include the column in the published frame using the declared aggregation rule.
- **AC-008**: Given a column not declared in any Gold set, When the pipeline runs, Then Gold shall drop it and the drift report shall record one event with reason `not_in_gold_contract`.
- **AC-009**: Given a Gold Macro dimension referencing a column not present in Gold, When Gold Macro runs, Then Gold Macro shall emit one row with `dimension_value = "sem_contrato"` and `lead_count = 0` and shall not raise.
- **AC-010**: Given any successful run, When inspecting the published Gold Macro, Then every row shall carry the `schema_contract_version` value declared in the contract.
- **AC-011**: Given a contract change that adds a new optional column, When `schema_contract_version` is not bumped, Then the test suite shall fail with a guard that detects contract drift without versioning.
- **AC-012**: Given a drift report containing at least one `passthrough_with_alert` event, When the agentic governance layer runs, Then the layer shall consume the report as input and shall not crash if no proposal is produced.

## 6. Test Automation Strategy

- **Test levels**: Unit tests on detection, propagation, and report emission. Integration tests covering the full Bronze to Gold Macro path. Contract tests on `config/pipeline_spec.json`.
- **Frameworks**: `pytest` via `venv/bin/python -m pytest`.
- **Test data management**: Synthetic DataFrame fixtures for drift classes. A focused fixture parquet for the integration path that includes one `optional_known`, one `unknown`, one `missing_required`, one `type_mismatch`, and one `category_drift`.
- **CI/CD integration**: Same `venv/bin/python -m pytest -q` command used locally.
- **Coverage requirements**: Every drift class shall have at least one detection test, one policy resolution test, and one propagation test.
- **Performance testing**: Not required.

Required automated checks:

- A Bronze unit test shall prove classification of `expected`, `optional_known`, `unknown`, `missing_required`.
- A Bronze unit test shall prove `type_mismatch` and `category_drift` classification.
- A Silver unit test shall prove preservation of declared extras and namespaced carry-through of undeclared extras.
- A Silver unit test shall prove that aggregation rules are applied at the lead level for preserved extras.
- A Gold unit test shall prove preservation of `optional_columns` and `passthrough_columns`.
- A Gold unit test shall prove that columns not in any Gold set are dropped with a recorded reason.
- A Gold Macro unit test shall prove the `sem_contrato` placeholder row when a dimension column is missing.
- A Gold Macro unit test shall prove that `schema_contract_version` is embedded on every row.
- An integration test shall run the full pipeline on the focused fixture and assert the drift report contents.
- A contract guard test shall detect changes to `bronze.required_columns`, `silver.preserve_extra_columns`, `gold.required_columns`, `gold.optional_columns`, `gold.passthrough_columns`, `gold_macro.categorical_dimensions`, or `gold_macro.numeric_metrics` without a `schema_contract_version` bump.

Recommended validation commands:

```bash
venv/bin/python -m pytest -q
venv/bin/python -m pytest tests/test_bronze.py -q
venv/bin/python -m pytest tests/test_silver.py -q
venv/bin/python -m pytest tests/test_gold.py -q
venv/bin/python -m pytest tests/test_jobs.py -q
PIPELINE_ENABLE_LLM_ENRICHMENT=0 venv/bin/python scripts/run_pipeline.py --force
```

## 7. Rationale & Context

The triage of the current pipeline showed three properties that this specification corrects:

- Bronze ingestion only checks for missing required columns. Extra source columns and unexpected metadata keys pass silently.
- Silver transforms operate on the full Bronze frame and propagate any extra column without acknowledgement.
- Gold aggregation drops a fixed list of intermediate and semantic columns and silently loses any extra column that was not promoted to the canonical schema.

This produces two failure modes that the specification eliminates:

- Silent loss. A useful new source column reaches Silver, is dropped by Gold, and never appears in Gold Macro. Operators are unaware that data is being discarded.
- Silent breakage. A renamed or removed source column produces inconsistent downstream behavior because validation only flags absence of declared required fields.

The design choices in this specification are:

- Detection is centralized in Bronze because Bronze is the only place that sees the full source surface. Detection runs deterministically and does not rely on optional LLM enrichment.
- Policy resolution is contract-driven and not code-driven. New columns are absorbed by editing `config/pipeline_spec.json`, not by editing transforms, until a promotion is justified.
- Propagation is explicit at every layer. Silver carries through declared and policy-approved extras under controlled names. Gold preserves only declared columns. Gold Macro dimensions are contract-driven.
- Governance is delegated to the agentic layer. Drift events are surfaced through a structured report so the existing impact-governed agent contract can produce proposals for promotion.
- Versioning is mandatory. A `schema_contract_version` integer protects the pipeline from undocumented contract changes and ensures Gold Macro snapshots are interpretable across time.

This approach favors visibility over silent passthrough, contract over implicit behavior, and governance over ad-hoc code change.

## 8. Dependencies & External Integrations

### External Systems

- **EXT-001**: Local filesystem - Required for reading Bronze input, emitting drift reports, and writing artifacts.

### Third-Party Services

- **SVC-001**: Optional LLM providers - Not required for drift detection or propagation. Only required for the existing semantic enrichment path.

### Infrastructure Dependencies

- **INF-001**: Repository-local virtual environment at `venv/` - Required execution baseline for tests and pipeline runs.

### Data Dependencies

- **DAT-001**: `data/conversations_bronze.parquet` - Source for end-to-end runtime confirmation of drift behavior.
- **DAT-002**: `config/pipeline_spec.json` - Contract source of truth for detection and propagation.
- **DAT-003**: `reports/monitoring/schema_drift_<run_id>.json` - New artifact emitted by every run.

### Technology Platform Dependencies

- **PLT-001**: Python runtime available via `venv/bin/python` - Required for tests and runtime validation.
- **PLT-002**: pandas as the in-memory frame engine - Required for the existing transformation surface.

### Compliance Dependencies

- **COM-001**: Repository privacy expectations for published artifacts - Drift report sample values shall be masked using existing masking utilities so detection of unknown columns does not leak raw sensitive data.

## 9. Examples & Edge Cases

```text
Case 1: New optional column declared
Contract: bronze.optional_columns includes "lead_segment_hint"
Input: source parquet adds "lead_segment_hint" with values like "segment_a"
Expected:
  - drift event class = optional_known, applied_policy = passthrough_with_alert
  - Silver preserves column under the same name
  - Gold drops it because "lead_segment_hint" is not in any Gold set
  - Drift report records propagation.gold = not_in_gold_contract
  - Run does not fail; schema_drift_alert = true

Case 2: Truly unknown column
Contract: no declaration for "internal_score"
Input: source parquet adds "internal_score" as numeric
Expected:
  - drift event class = unknown, applied_policy = passthrough_with_alert
  - Silver carries it through as "bronze_passthrough__internal_score"
  - Gold drops it
  - Agentic governance layer receives the drift event and may propose promotion

Case 3: Missing required column
Contract: bronze.required_columns includes "campaign_id"
Input: source parquet does not include "campaign_id"
Expected:
  - drift event class = missing_required, applied_policy = block
  - Run fails with non-zero exit
  - schema_ok = false in BronzeValidationReport

Case 4: Type mismatch on declared column
Contract: timestamp expected as datetime
Input: timestamp column arrives as float epoch with non-coercible nulls
Expected:
  - drift event class = type_mismatch
  - default policy = quarantine for affected rows or passthrough_with_alert at run level
  - downstream Silver receives only safely typed rows when policy = quarantine

Case 5: Category drift
Contract: bronze.category_domains.direction = ["outbound", "inbound"]
Input: a row has direction = "system"
Expected:
  - drift event class = category_drift
  - affected row routed to quarantine when policy = quarantine
  - Gold shall not contain a lead derived solely from quarantined rows

Case 6: Renamed column
Contract: bronze.required_columns includes "sender_name"
Input: source renames "sender_name" to "sender_display_name"
Expected:
  - drift event 1: missing_required for "sender_name", applied_policy = block
  - drift event 2: unknown for "sender_display_name", applied_policy = passthrough_with_alert
  - Run fails because the rename triggers a missing required column
  - Drift report makes the rename visible to the governance layer

Case 7: Gold Macro dimension referencing a column not present in Gold
Contract: gold_macro.categorical_dimensions includes "agent_team"
Input: agent_team is missing because it was dropped upstream
Expected:
  - Gold Macro emits one placeholder row: dimension = "agent_team", dimension_value = "sem_contrato", lead_count = 0
  - Run does not crash

Case 8: Contract change without version bump
Action: a developer adds an entry to gold.optional_columns and does not bump schema_contract_version
Expected:
  - Contract guard test fails
  - PR cannot land until the version is bumped
```

## 10. Validation Criteria

The implementation shall be considered compliant with this specification when all of the following are true:

- Bronze ingestion produces a drift classification for every observed top-level column and metadata key on every run.
- A schema drift report is emitted at `reports/monitoring/schema_drift_<run_id>.json` on every run.
- The default policy for `unknown` is `passthrough_with_alert` and surfaces a top-level alert flag in the run report.
- Silver preserves declared extras under the same name and undeclared extras under the namespaced carry-through name.
- Gold preserves only columns declared in `required_columns`, `optional_columns`, or `passthrough_columns`, and records every drop reason.
- Gold Macro consumes dimensions and metrics from the contract and embeds the contract version on every row.
- Missing required columns fail the run.
- Renames are visible as the combination of one `missing_required` event and one `unknown` event.
- The contract guard test prevents contract changes without a `schema_contract_version` bump.
- The agentic governance layer consumes the drift report as input.
- The published privacy contract is preserved for sample values in the drift report.
- `README.md` is updated to describe the schema evolution behavior, the drift report location, and the propagation contract.

## 11. Related Specifications / Further Reading

- `spec/spec-architecture-pipeline-spec-runtime-parity.md`
- `spec/spec-architecture-pipeline-validation-contract-alignment.md`
- `spec/spec-architecture-bronze-layer-enrichment.md`
- `spec/spec-architecture-cdc-row-level-source-fingerprint.md`
- `spec/spec-architecture-gold-macro-view.md`
- `spec/spec-architecture-agent-autonomy-governed-by-impact.md`
- `spec/spec-architecture-agentic-layer-gap-remediation.md`
- `config/pipeline_spec.json`
- `reports/monitoring/latest_run_report.json`
