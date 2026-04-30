---
title: Architecture Specification for Agentic Schema Promotion Ladder Driven by the Schema Drift Report
version: 1.0
date_created: 2026-04-30
last_updated: 2026-04-30
owner: Lucas Neves
tags: [architecture, agentic, planner, schema, governance, autonomy, promotion, drift]
---

# Introduction

This specification defines how the agentic layer shall convert schema-drift events into structured promotion proposals so that newly observed columns can be progressively adopted by the pipeline (Bronze → Silver → Gold → Gold Macro) under explicit governance.

The previous schema evolution work (`spec-architecture-schema-evolution-detection-and-propagation.md`) introduced detection, propagation policies, and the per-run drift report at `reports/monitoring/latest_schema_drift_report.json`. That work intentionally stops at carry-through into Silver and leaves Gold untouched until a column is explicitly promoted into the contract.

This specification fills that gap by defining a tiered promotion ladder, the proposal families, the decision matrix, the confidence model, the privacy gate, and the autonomy reclassification needed for the planner and the autonomy module to evaluate, propose, and either auto-promote (low-risk levels) or hold for human approval (high-risk levels) the adoption of a new column.

## 1. Purpose & Scope

This specification defines requirements, constraints, interfaces, and validation rules for evolving the agentic layer (`src/pipeline/agent/planner.py`, `src/pipeline/agent/autonomy.py`, `src/pipeline/agent/approval.py`) so it can:

- Read the schema drift report as primary input.
- Emit one of several proposal types per drift event, each targeting a specific level of the promotion ladder.
- Resolve a confidence score for each proposed promotion using stable, reproducible signals.
- Apply the autonomy decision matrix (auto-promote, hold for approval, reject) according to impact class and confidence.
- Block any promotion that fails a privacy gate.

Scope:

- Promotion of unknown or optional_known columns into `bronze.optional_columns`, `silver.preserve_extra_columns`, `silver.extra_aggregation_rules`, `gold.optional_columns`, `gold.passthrough_columns`, `gold_macro.categorical_dimensions`, with a mandatory `schema_contract_version` bump.
- Definition of the tiered ladder: namespaced carry-through (already automatic), Silver preserved name, Silver aggregation rule, Gold optional, Gold passthrough, Gold Macro dimension.
- Confidence signals: stability across cycles, type consistency, cardinality fit, density, privacy-clean sample.
- Reclassification of mutation families in the autonomy policy with per-family impact class.
- Cooloff awareness for rejected promotion proposals.

Out of scope:

- Inferring business semantics for new columns. The agent shall recommend a level of usage but not assign meaning beyond a generic aggregation rule.
- Modifying the deterministic transforms in `src/pipeline/transforms/` to use the new column in derivations or segmentation.
- Replacing the existing approval lifecycle (`approval.py`).
- Auto-promotion to Gold Macro dimensions (always human approval).
- Numeric metric promotions in Gold Macro (`gold_macro.numeric_metrics`).

Intended audience:

- Engineers extending the planner and the autonomy module.
- Reviewers validating that the promotion ladder is safe and reversible.
- Future maintainers responsible for tuning confidence thresholds.

Assumptions:

- The schema drift report described in `spec-architecture-schema-evolution-detection-and-propagation.md` is emitted on every run.
- The autonomy policy in `config/agent_autonomy_policy.json` is the source of truth for impact class and auto-promote thresholds.
- The repository-local execution path is canonical.
- The agent already supports the `proposed → candidate_materialized → awaiting_approval → approved → promoted → rolled_back` lifecycle.

## 2. Definitions

- **Drift Report**: The artifact emitted at `reports/monitoring/latest_schema_drift_report.json` containing classified drift events.
- **Promotion**: A change to `config/pipeline_spec.json` that elevates a column from a lower level on the ladder to a higher one, accompanied by a `schema_contract_version` bump.
- **Promotion Ladder**: The ordered set of usage levels a new column can occupy in the contract.
- **Proposal Family**: A label in the autonomy policy that groups proposals by impact class and approval requirements (e.g. `schema_promotion_silver`, `schema_promotion_gold`).
- **Proposal Type**: A finer-grained label that maps one-to-one to a `spec_patch` action (e.g. `bronze_optional_columns_addition`).
- **Promotion Confidence**: A scalar in [0, 1] computed from drift-report signals; used to gate auto-promotion.
- **Stability Cycles**: The number of consecutive runs in which a given column has been observed with the same drift class and type.
- **Privacy Gate**: A pre-promotion check that scans masked sample values against `SENSITIVE_PATTERNS` and `forbidden_raw_columns`.
- **Promotion History Store**: A persisted JSON file at `reports/monitoring/schema_promotion_history.json` that tracks per-column observation cycles and prior decisions.
- **Cooloff Window**: The existing rejection cooloff defined in `agent_autonomy_policy.json` (`rejection_cooloff_threshold_cycles`, `rejection_cooloff_threshold_hours`).

## 3. Requirements, Constraints & Guidelines

### Promotion ladder

- **REQ-001**: The ladder shall consist of the following levels, in increasing impact class:
  1. `level_namespaced_silver` (already automatic, no proposal needed)
  2. `level_silver_preserved` — column listed in `silver.preserve_extra_columns`
  3. `level_silver_aggregation` — companion aggregation rule in `silver.extra_aggregation_rules`
  4. `level_bronze_optional` — column declared in `bronze.optional_columns`
  5. `level_gold_optional` — column declared in `gold.optional_columns` with rule in `gold.aggregation_rules`
  6. `level_gold_passthrough` — column declared in `gold.passthrough_columns` with rule in `gold.aggregation_rules`
  7. `level_gold_macro_dimension` — column declared in `gold_macro.categorical_dimensions`

- **REQ-002**: A promotion proposal shall target exactly one level. Multi-level promotion shall be expressed as a sequence of proposals chained by `precedes`/`requires` relations, not as a single proposal.

- **REQ-003**: Every promotion proposal shall include a companion `schema_contract_version_bump` action in the same `spec_patch` set, applied atomically.

- **REQ-004**: Promotion to `level_gold_macro_dimension` shall always require human approval, regardless of confidence.

### Proposal families and types

- **REQ-010**: The planner shall emit proposals using the following types, each backed by a corresponding `spec_patch` in `build_candidate_actions`:

  | Proposal type | `target_path` | Operation |
  | --- | --- | --- |
  | `bronze_optional_columns_addition` | `bronze.optional_columns` | `add_items` |
  | `silver_preserve_extra_columns_addition` | `silver.preserve_extra_columns` | `add_items` |
  | `silver_extra_aggregation_rule_addition` | `silver.extra_aggregation_rules` | `set_keys` |
  | `gold_optional_columns_addition` | `gold.optional_columns` | `add_items` |
  | `gold_passthrough_columns_addition` | `gold.passthrough_columns` | `add_items` |
  | `gold_aggregation_rule_addition` | `gold.aggregation_rules` | `set_keys` |
  | `gold_macro_dimension_addition` | `gold_macro.categorical_dimensions` | `add_items` |
  | `schema_contract_version_bump` | `schema_contract_version` | `set_value` |

- **REQ-011**: Each proposal shall declare the affected ladder level in `proposed_change.ladder_level`.

- **REQ-012**: The planner shall not emit `bronze_required_columns_addition` for new columns observed via the drift report. That existing proposal type remains for explicit "make required" intent and is out of the promotion ladder.

### Drift report consumer

- **REQ-020**: `_schema_proposals` in `src/pipeline/agent/planner.py` shall be refactored to consume `reports/monitoring/latest_schema_drift_report.json` as its primary input.
- **REQ-021**: The planner shall iterate over events with `drift_class in {"unknown", "optional_known"}` and `scope == "top_level"`.
- **REQ-022**: The planner shall ignore events whose `applied_policy == "block"` (those are surfaced as runtime failures, not promotions).
- **REQ-023**: The planner shall ignore events with `drift_class == "missing_required"` (those are runtime failures).
- **REQ-024**: When the drift report is missing or empty, the planner shall emit zero promotion proposals and shall not raise.

### Decision matrix

- **REQ-030**: Given a candidate column, the planner shall infer a recommended ladder level using the following deterministic rules, evaluated in order:

  | Signal | Recommendation |
  | --- | --- |
  | Density `< 0.2` OR stability cycles `< 3` | No proposal (observe more) |
  | Privacy gate fails | No proposal, record `closed_no_action` |
  | Cardinality `<= 20` AND density `> 0.7` AND value class is categorical text | `level_gold_macro_dimension` (with required Gold optional pre-step) |
  | Numeric continuous (mean and std defined) | `level_gold_passthrough` with rule `mean` |
  | Cardinality `> 100` AND density `> 0.5` | `level_silver_preserved` only |
  | Otherwise | `level_bronze_optional` only |

- **REQ-031**: A `level_gold_macro_dimension` recommendation shall always be split into two chained proposals: first `level_gold_optional`, then `level_gold_macro_dimension`. The second shall declare `requires` referencing the first.

- **REQ-032**: When the recommended level requires a parent level not yet in the contract, the planner shall emit the parent proposal first.

### Confidence model

- **REQ-040**: Promotion confidence shall be computed as:
  ```
  confidence = w_stab * stability_score
             + w_type * type_consistency_score
             + w_card * cardinality_fit_score
             + w_priv * privacy_clean_score
  ```
  where weights satisfy `w_stab + w_type + w_card + w_priv == 1.0`.

- **REQ-041**: Default weights shall be `w_stab=0.4`, `w_type=0.2`, `w_card=0.2`, `w_priv=0.2`.

- **REQ-042**: Component scores shall be deterministic and bounded in [0, 1]:
  - `stability_score` = `min(1.0, observed_cycles / 5)`
  - `type_consistency_score` = `1.0` if no `type_mismatch` event in the last 5 cycles, else `0.0`
  - `cardinality_fit_score` = `1.0` if observed cardinality matches the recommended ladder level, else `0.5`
  - `privacy_clean_score` = `1.0` if privacy gate passes, else `0.0`

- **REQ-043**: Confidence shall be persisted on the proposal at `proposal.confidence`.

### Promotion history store

- **REQ-050**: The agent shall persist `reports/monitoring/schema_promotion_history.json` with per-column observation history.
- **REQ-051**: The store shape shall be:
  ```json
  {
    "version": 1,
    "columns": {
      "<column_name>": {
        "first_seen_at_utc": "ISO-8601",
        "last_seen_at_utc": "ISO-8601",
        "observed_cycles": 0,
        "observed_drift_classes": ["unknown"],
        "observed_dtypes": ["int64"],
        "type_mismatch_recent_cycles": 0,
        "last_proposal_id": "string|null",
        "last_decision": "promote|hold_for_approval|reject|null",
        "rejection_cooloff_until_utc": "ISO-8601|null"
      }
    }
  }
  ```
- **REQ-052**: The store shall be updated at the end of every run, before the planner builds proposals.
- **REQ-053**: A column shall be removed from the store if it has not been observed for 20 consecutive cycles.

### Autonomy reclassification

- **REQ-060**: The autonomy policy shall declare the following families with the listed defaults:

  | Family | Impact class | `auto_promote` | `requires_approval` | `requires_privacy_scan` | `agent_auto_approve_if_confidence_ge` |
  | --- | --- | --- | --- | --- | --- |
  | `schema_promotion_silver` | `low` | `true` | `false` | `true` | `0.85` |
  | `schema_promotion_silver_rule` | `medium` | `false` | `true` | `false` | `0.90` |
  | `schema_promotion_bronze_optional` | `low` | `true` | `false` | `true` | `0.85` |
  | `schema_promotion_gold_optional` | `medium` | `false` | `true` | `true` | `0.90` |
  | `schema_promotion_gold_passthrough` | `medium` | `false` | `true` | `true` | `0.90` |
  | `schema_promotion_gold_macro_dimension` | `high` | `false` | `true` | `true` | `null` |
  | `schema_contract_version_bump` | `low` | `true` | `false` | `false` | `null` |

- **REQ-061**: `schema_promotion_gold_macro_dimension` shall never auto-promote, even at confidence `1.0`.
- **REQ-062**: The existing `schema_update` family shall be retained for the legacy `bronze_required_columns_addition` and `silver_metadata_fields_addition` types and shall not be touched.

### Privacy gate

- **REQ-070**: Before any proposal targeting `level_gold_optional`, `level_gold_passthrough`, or `level_gold_macro_dimension` is emitted, the planner shall run a privacy gate.
- **REQ-071**: The gate shall scan `event.sample_values_masked` against `SENSITIVE_PATTERNS` defined in `src/pipeline/transforms/silver_patterns.py`.
- **REQ-072**: The gate shall also reject any column whose name matches a `forbidden_raw_columns` entry from the contract.
- **REQ-073**: A failed gate shall block the proposal, mark the column with `last_decision = "closed_no_action"` in the history store, and increment `privacy_block_count_total` via `update_autonomy_metrics`.

### Cooloff awareness

- **REQ-080**: Before emitting a promotion proposal for a column, the planner shall check `is_in_rejection_cooloff(paths, last_proposal_id)`.
- **REQ-081**: A column in cooloff shall be skipped silently for the cooloff duration.
- **REQ-082**: Cooloff state for promotion proposals shall reuse the existing `approval.py` cooloff API.

### Constraints

- **CON-001**: Implementation shall use the repository-local virtual environment defined in `AGENTS.md`.
- **CON-002**: Implementation shall not require network access.
- **CON-003**: Implementation shall not change the schema-drift detection behavior defined in `spec-architecture-schema-evolution-detection-and-propagation.md`.
- **CON-004**: Implementation shall not auto-promote to Gold Macro dimensions.
- **CON-005**: Implementation shall not bypass the existing approval lifecycle for high-impact promotions.
- **CON-006**: Implementation shall preserve backward compatibility for existing proposal types.
- **CON-007**: Confidence weights and thresholds shall be sourced from the autonomy policy when present, falling back to the defaults in REQ-041 and REQ-060.
- **CON-008**: Privacy gate shall not weaken the existing privacy contract; it adds checks, never removes.

### Guidelines

- **GUD-001**: Prefer chained proposals over multi-target patches. One proposal, one ladder rung.
- **GUD-002**: Prefer observe-more over premature promotion. The cost of a missed promotion is small; the cost of a wrong promotion that ships to Gold is high.
- **GUD-003**: Prefer reusing existing autonomy infrastructure (cooloff, approval, candidate materialization) over building a parallel path for promotions.
- **GUD-004**: Prefer surfacing low-confidence cases as recommendations (`recommendation_only`) over discarding them.
- **GUD-005**: Prefer narrow privacy patterns at the gate over broad regex weakening.

### Patterns

- **PAT-001**: Promotion sequence: drift event → history update → privacy gate → ladder recommendation → confidence score → proposal emission → autonomy classification → candidate materialization → approval (auto or human) → spec patch + version bump → metrics update.
- **PAT-002**: Two-step pattern for Gold Macro: emit `gold_optional_columns_addition` first, then `gold_macro_dimension_addition` with `requires` referencing the first.
- **PAT-003**: Privacy-first pattern: privacy gate runs before confidence and before ladder recommendation, so privacy-blocked columns never appear as recommendations.

## 4. Interfaces & Data Contracts

### 4.1 Relevant files

| Path | Role | Required outcome |
| --- | --- | --- |
| `src/pipeline/agent/planner.py` | Proposal generation | Add `_promotion_proposals_from_drift_report`; refactor `_schema_proposals` to no longer emit `bronze_required_columns_addition` for unknown drift events |
| `src/pipeline/agent/autonomy.py` | Impact classification, candidate actions | Add new families to `DEFAULT_AUTONOMY_POLICY.mutation_families`; extend `build_candidate_actions` for the new proposal types |
| `src/pipeline/agent/approval.py` | Approval and cooloff lifecycle | No new functions; reused as-is |
| `src/pipeline/agent/llm_advisor.py` | Optional confidence boost | May be consulted to refine ladder recommendation when present |
| `src/pipeline/quality/schema_drift.py` | Drift report producer | No changes; consumer is the planner |
| `src/pipeline/runtime/spec.py` | Spec validator | Add `bronze.optional_columns` and similar lists to the structural validator if the implementation chooses to enforce shape |
| `config/agent_autonomy_policy.json` | Live autonomy policy | Update with new families per REQ-060 |
| `config/pipeline_spec.json` | Contract source of truth | No changes for this spec; promotions modify it at runtime via `apply_proposal_to_spec` |
| `reports/monitoring/latest_schema_drift_report.json` | Drift report | Read by planner |
| `reports/monitoring/schema_promotion_history.json` | New: per-column promotion history | Created and updated by planner |
| `tests/test_planner.py`, `tests/test_autonomy_candidate.py`, `tests/test_approval.py`, `tests/test_schema_drift.py` | Coverage | Add planner tests for the new proposal flow, candidate action tests for each new type, end-to-end tests for the promotion lifecycle |

### 4.2 New `proposed_change` payload shape

Each new proposal type shall use the following payload contract:

```json
{
  "target_path": "<one of the values in REQ-010 table>",
  "operation": "<add_items | set_keys | set_value>",
  "items": ["lead_segment_hint"],
  "ladder_level": "level_silver_preserved",
  "companion_actions": [
    {
      "target_path": "schema_contract_version",
      "operation": "set_value",
      "value": 3
    }
  ]
}
```

`set_keys` shall update one or more keys inside an object (used for aggregation-rule maps). `companion_actions` shall be applied atomically with the main action; if any of them fails, all are rolled back.

### 4.3 Proposal context shape

The drift-report-driven proposal shall use this `context_detected` block:

```json
{
  "context_type": "schema_promotion_candidate",
  "summary": "Column observed via drift report and recommended for promotion",
  "evidence": {
    "column": "lead_segment_hint",
    "drift_class": "unknown",
    "applied_policy": "passthrough_with_alert",
    "row_count": 1234,
    "non_null_count": 1100,
    "observed_cycles": 4,
    "cardinality_estimate": 7,
    "recommended_level": "level_gold_optional",
    "confidence": 0.92,
    "privacy_gate": "passed"
  }
}
```

### 4.4 Promotion history store schema

Defined in REQ-051. The store shall be loaded and persisted using the existing `read_json` and `write_json` helpers from `pipeline.io.parquet_io`.

### 4.5 Autonomy policy patch

The implementation shall extend `DEFAULT_AUTONOMY_POLICY.mutation_families` with the seven new keys listed in REQ-060. The existing `schema_update` family shall remain unchanged.

### 4.6 Privacy gate API

```python
def privacy_gate(
    column_name: str,
    sample_values_masked: list[str],
    contract: Mapping[str, Any],
) -> PrivacyGateResult:
    """Returns PrivacyGateResult(passed: bool, reason: str | None)."""
```

The function shall live in `src/pipeline/agent/planner.py` or a new module `src/pipeline/agent/promotion.py`. It shall not depend on network resources.

## 5. Acceptance Criteria

- **AC-001**: Given a drift report containing one `unknown` event for a numeric column with cardinality `> 100` observed for `>= 3` cycles, When the planner runs, Then it shall emit one proposal of type `silver_preserve_extra_columns_addition` with `ladder_level == "level_silver_preserved"` and `confidence >= 0.85`.
- **AC-002**: Given a drift report containing one `unknown` event for a low-cardinality categorical column observed for `>= 5` cycles, When the planner runs, Then it shall emit two chained proposals: first `gold_optional_columns_addition`, then `gold_macro_dimension_addition` with `requires` referencing the first.
- **AC-003**: Given a column whose masked sample matches a `SENSITIVE_PATTERNS` entry, When the planner runs, Then no promotion proposal shall be emitted, the history store shall record `last_decision = "closed_no_action"`, and `privacy_block_count_total` shall increment.
- **AC-004**: Given a column observed for `< 3` cycles, When the planner runs, Then no promotion proposal shall be emitted regardless of cardinality or density.
- **AC-005**: Given a column whose `last_proposal_id` is in rejection cooloff, When the planner runs, Then no new promotion proposal shall be emitted for that column for the cooloff duration.
- **AC-006**: Given an `unknown` event with `applied_policy == "block"` (e.g. category_drift escalated to block by per-column override), When the planner runs, Then no promotion proposal shall be emitted for that column.
- **AC-007**: Given a confidence `>= 0.85` proposal of family `schema_promotion_silver` or `schema_promotion_bronze_optional`, When the autonomy module classifies it, Then the proposal shall be promoted automatically and the spec shall be patched in the same cycle.
- **AC-008**: Given a confidence `1.0` proposal of family `schema_promotion_gold_macro_dimension`, When the autonomy module classifies it, Then the proposal shall be held for human approval.
- **AC-009**: Given any successful promotion, When inspecting `config/pipeline_spec.json`, Then `schema_contract_version` shall be incremented atomically with the target list change.
- **AC-010**: Given the drift report is missing on disk, When the planner runs, Then it shall emit zero promotion proposals and shall not raise.
- **AC-011**: Given a column not observed for `>= 20` cycles, When the planner persists the history store, Then that column shall be removed from the store.
- **AC-012**: Given a Gold Macro dimension promotion is approved and applied, When the next pipeline run executes, Then `conversations_gold_macro.parquet` shall include rows for that dimension and the run shall not regress on existing tests.

## 6. Test Automation Strategy

- **Test levels**: Unit tests for the privacy gate, the confidence calculator, the ladder decision matrix, the history store; integration tests for `_promotion_proposals_from_drift_report`; lifecycle tests for autonomy classification and approval flow on the new families; contract tests on the autonomy policy default.
- **Frameworks**: `pytest` via `venv/bin/python -m pytest`.
- **Test data management**: Synthetic drift report fixtures in JSON; synthetic history store fixtures in JSON; isolated `tmp_path` for each lifecycle test.
- **CI/CD integration**: Existing `venv/bin/python -m pytest -q` command suffices.
- **Coverage requirements**: Each acceptance criterion shall have at least one regression test.
- **Performance testing**: Not required.

Required automated checks:

- A unit test shall prove that the privacy gate rejects sample values matching `SENSITIVE_PATTERNS`.
- A unit test shall prove that the confidence formula is bounded in [0, 1] and respects the weight invariant.
- A unit test shall prove the decision matrix mapping for the cases in REQ-030.
- An integration test shall prove that a low-cardinality categorical column produces the chained Gold-optional-then-Macro proposals.
- An integration test shall prove that observation under 3 cycles emits no proposal.
- A lifecycle test shall prove that a `schema_promotion_silver` proposal at confidence `0.86` is auto-promoted and the spec is patched.
- A lifecycle test shall prove that a `schema_promotion_gold_macro_dimension` proposal at confidence `1.0` is held for approval.
- A contract test shall prove the autonomy policy defaults for all seven new families match REQ-060.
- A regression test shall prove that legacy `bronze_required_columns_addition` proposals continue to flow through the legacy `schema_update` family.

Recommended validation commands:

```bash
venv/bin/python -m pytest -q
venv/bin/python -m pytest tests/test_planner.py -q
venv/bin/python -m pytest tests/test_autonomy_candidate.py -q
venv/bin/python -m pytest tests/test_schema_drift.py -q
PIPELINE_ENABLE_LLM_ENRICHMENT=0 venv/bin/python scripts/run_pipeline.py --force
```

## 7. Rationale & Context

The schema-drift work shipped in commit `feat(schema): add drift detection and controlled propagation` introduced visibility but left the column unused beyond namespaced carry-through into Silver. Three observations from the post-implementation triage motivated this specification:

1. **The existing planner only knows how to make a column required.** The lone schema proposal type, `bronze_required_columns_addition`, is the wrong target for almost every drift event because making a column required would fail past data and is a high-impact change disguised as a declarative one. The promotion ladder makes the impact explicit.

2. **The drift report already contains the right signals.** Sample values, applied policy, propagation state, row count, non-null count, contract version. Reading the report once produces a richer input than re-deriving from the Bronze frame.

3. **The autonomy policy already has the right shape.** Mutation families with impact classes, `auto_promote`, `requires_approval`, and a confidence-based auto-approval threshold. Adding new families is additive and reuses the existing approval and cooloff machinery.

The design choices below were made deliberately:

- **Tiered ladder over single promotion**: Each rung has a different impact. A column reaching Silver as `bronze_passthrough__X` is invisible to analytics; a column reaching Gold Macro becomes a public dimension. Forcing one proposal per rung makes the impact class match the actual blast radius.
- **Always require human approval for Gold Macro dimensions**: The macro view drives reporting and dashboards. Adding a dimension changes the analytic surface in ways the agent cannot validate.
- **Observe-more by default**: Three-cycle minimum observation reduces churn from transient one-off columns. The cost of waiting three runs is negligible compared to the cost of promoting a column that was a typo or a one-off probe.
- **Privacy-first**: The gate runs before everything else. A privacy violation aborts the whole proposal generation for that column regardless of confidence.
- **Atomic version bump**: Every promotion bumps `schema_contract_version`. This is the audit trail. The companion action shape ensures both happen or neither happens.
- **Reuse the existing approval lifecycle**: No new state machine. The new proposal families flow through the same `proposed → candidate_materialized → awaiting_approval → approved → promoted → rolled_back` path.

The promotion history store is the only new persistent artifact. It is bounded (REQ-053 expires unobserved columns), auditable, and small.

## 8. Dependencies & External Integrations

### External Systems

- **EXT-001**: Local filesystem - Required for reading the drift report, the history store, and the autonomy policy.

### Third-Party Services

- **SVC-001**: Optional LLM advisor (`pipeline.agent.llm_advisor`) - Not required. May be consulted to refine recommendations when enabled.

### Infrastructure Dependencies

- **INF-001**: Repository-local virtual environment at `venv/` - Required execution baseline.

### Data Dependencies

- **DAT-001**: `reports/monitoring/latest_schema_drift_report.json` - Primary input for the planner.
- **DAT-002**: `reports/monitoring/schema_promotion_history.json` - New per-column observation store.
- **DAT-003**: `config/agent_autonomy_policy.json` - Source of truth for impact and auto-promote thresholds.
- **DAT-004**: `config/pipeline_spec.json` - Mutation target for promoted columns.

### Technology Platform Dependencies

- **PLT-001**: Python runtime via `venv/bin/python`.

### Compliance Dependencies

- **COM-001**: Repository privacy expectations - The privacy gate enforces the same masking and forbidden-raw-column rules already in place for published artifacts.

## 9. Examples & Edge Cases

```text
Case 1: Stable categorical column with low cardinality
Drift event: "lead_segment_hint", drift_class=unknown, observed 5 cycles, cardinality=4, density=0.95
Privacy gate: passed
Recommended level: level_gold_macro_dimension
Confidence: 0.4*1.0 + 0.2*1.0 + 0.2*1.0 + 0.2*1.0 = 1.0
Proposals emitted (chained):
  1. gold_optional_columns_addition (auto-promoted at conf >= 0.90)
  2. gold_macro_dimension_addition (held for approval, regardless of confidence)
schema_contract_version bumped twice across the two proposals

Case 2: High-cardinality numeric continuous column
Drift event: "internal_score", drift_class=unknown, observed 4 cycles, density=0.7, dtype=float
Privacy gate: passed
Recommended level: level_gold_passthrough with rule "mean"
Confidence: 0.4*0.8 + 0.2*1.0 + 0.2*1.0 + 0.2*1.0 = 0.92
Proposal emitted: gold_passthrough_columns_addition + gold_aggregation_rule_addition + schema_contract_version_bump
Family: schema_promotion_gold_passthrough
Auto-approve threshold for family: 0.90
Decision: held for approval (requires_approval=true takes precedence over confidence)

Case 3: Brand-new column observed once
Drift event: "experiment_flag", drift_class=unknown, observed 1 cycle
Recommended level: none (REQ-030 first row)
Proposals emitted: none
History store updated with first_seen_at, observed_cycles=1

Case 4: Column whose sample matches PII pattern
Drift event: "third_party_phone", drift_class=unknown, sample_values_masked=["+5511..."]
Privacy gate: failed (matches PHONE_PATTERN)
Proposals emitted: none
History store: last_decision="closed_no_action"
privacy_block_count_total incremented

Case 5: Column previously rejected and still in cooloff
Drift event: "experiment_a", history.rejection_cooloff_until_utc > now
Proposals emitted: none for this column
Cooloff respected silently

Case 6: Column unobserved for 25 cycles
History store: observed_cycles=0 across last 25 runs
Action: column entry removed from history store
Effect: future appearances treated as new (cycle counter restarts)

Case 7: Low-confidence column with a sound recommendation
Drift event: "agent_team", observed 3 cycles, cardinality=12, density=0.5
Confidence: 0.4*0.6 + 0.2*1.0 + 0.2*1.0 + 0.2*1.0 = 0.84
Family: schema_promotion_gold_optional, threshold 0.90
Decision: held for approval (below threshold)
Proposal still emitted to make it visible to operators

Case 8: Drift report missing
Action: planner finds no file or empty events list
Proposals emitted: none
History store: not updated (nothing observed)
Run: ok, no error
```

## 10. Validation Criteria

The implementation shall be considered compliant with this specification when all of the following are true:

- The planner reads the drift report on every run and never raises when the file is absent.
- Each `unknown` or `optional_known` top-level event with sufficient stability and a passing privacy gate produces at most one promotion proposal per cycle, optionally chained for the Gold Macro path.
- Every promotion proposal carries a `ladder_level`, a confidence score, a privacy-gate result, and a companion `schema_contract_version` bump.
- The autonomy policy declares the seven new families with the impact classes and thresholds in REQ-060.
- The promotion history store is created on the first run and pruned of unobserved columns over time.
- Privacy-failed columns produce no proposal and update the metrics counter.
- Rejection cooloff is honored.
- Gold Macro dimension promotion always requires human approval.
- All acceptance criteria have at least one passing test.
- `README.md` is updated to describe the promotion ladder and where the history store lives.

## 11. Related Specifications / Further Reading

- `spec/spec-architecture-schema-evolution-detection-and-propagation.md`
- `spec/spec-architecture-agent-autonomy-governed-by-impact.md`
- `spec/spec-architecture-agentic-layer-gap-remediation.md`
- `spec/spec-architecture-agentic-pipeline-decisions.md`
- `spec/spec-process-proposal-rejection-cooloff-window.md`
- `spec/spec-process-proposal-awaiting-approval-expiry.md`
- `config/agent_autonomy_policy.json`
- `config/pipeline_spec.json`
- `reports/monitoring/latest_schema_drift_report.json`
