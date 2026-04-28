---
title: Agentic Pipeline Decisions — Execution Planning, Dynamic Gold Design, and LLM Diagnosis
version: 1.0
date_created: 2026-04-27
owner: Lucas Neves
tags: [architecture, agent, pipeline, llm, gold, orchestration, react]
---

# Introduction

This specification defines the three agent decision capabilities required to make the data pipeline genuinely agentic. Currently the pipeline executes a fixed Bronze → Silver → Gold sequence driven entirely by a static `pipeline_spec.json`. The agent observes results after the fact and proposes spec mutations, but does not decide *how* the pipeline runs. This spec introduces three runtime decision points where the agent reasons over observed state and drives execution, not merely reacts to it.

The three decisions are:

1. **Adaptive Execution Planning** — the agent decides *which stages* to run before execution starts.
2. **Dynamic Gold Designer** — the agent decides *which analytical columns and segments* to create in Gold, based on observed Silver data.
3. **LLM-based Failure Diagnosis** — the agent reasons about novel validation failures instead of mapping them to a static playbook dictionary.

These decisions are integrated into a ReAct (Reason → Act) loop that replaces the current linear `run_cycle()` in `operator.py`.

---

## 1. Purpose & Scope

**Purpose:** define the interfaces, data contracts, behavioral requirements, and acceptance criteria for introducing genuine agentic decision-making into the pipeline orchestration layer.

**Scope:** covers `src/pipeline/agent/`, `src/pipeline/orchestration/operator.py`, `src/pipeline/transforms/gold.py`, and the LLM runtime integration in `src/pipeline/runtime/llm_runtime.py`. Does not cover changes to Bronze or Silver transformation logic, PII masking rules, or the existing autonomy/approval governance system (those remain as-is).

**Audience:** engineers implementing or reviewing the agent decision layer. Assumes familiarity with the existing pipeline architecture.

**Assumptions:**
- An LLM provider (Anthropic or OpenAI) is configured and reachable; all three decisions must degrade gracefully to deterministic fallback if the LLM is unavailable.
- The existing `pipeline_spec.json` contract and `autonomy.py` governance remain authoritative for spec mutations; this spec governs runtime decisions only.
- `pandas.DataFrame` is the in-memory data format throughout.

---

## 2. Definitions

| Term | Definition |
|---|---|
| **ReAct loop** | A Reason → Act execution pattern where an agent observes state, produces a structured reasoning step (thought), selects an action, executes it, and observes the result before deciding the next action. |
| **Execution plan** | A structured decision produced by the agent before the pipeline runs, specifying which stages are needed and why. |
| **Gold designer** | An agent component that samples Silver data and produces a `GoldColumnPlan` — a list of column definitions and segmentation rules to materialize in Gold. |
| **LLM diagnosis** | A free-reasoning diagnosis produced by the LLM for a validation failure that has no static entry in `VALIDATION_CHECK_MAP`. |
| **Deterministic fallback** | A rule-based response used when the LLM is unavailable, returns an invalid response, or exceeds the timeout. Must produce the same output type as the LLM path. |
| **Stage** | One of `bronze`, `silver`, `gold`, `validation`, `planning`. Stages are the atomic units the agent can include or skip in the execution plan. |
| **Sample** | A representative subset of a DataFrame passed to the LLM to avoid token limits. Default: 50 random rows, serialized as JSON records. |
| **Observation** | A structured dict summarizing the current pipeline state (fingerprint, layer sizes, validation errors, last run timestamp) passed to the agent at the start of each ReAct iteration. |
| **GoldColumnPlan** | The structured output of the Gold designer: a list of column definitions each with `name`, `derivation_logic`, `data_type`, `rationale`, and `segment_values` (for categorical columns). |
| **VALIDATION_CHECK_MAP** | The existing static dict in `agent.py` mapping `(layer, check_name)` to a playbook and action. LLM diagnosis supplements this for unknown keys. |

---

## 3. Requirements, Constraints & Guidelines

### Decision 1 — Adaptive Execution Planning

- **REQ-001**: Before any stage executes, the agent MUST produce an `ExecutionPlan` specifying which stages to run, in order, with a `rationale` string and a `confidence` score (0.0–1.0).
- **REQ-002**: The `ExecutionPlan` MUST be derived from an `Observation` that includes: source fingerprint comparison result, existence and age of each layer's artifact, last validation summary per layer, and last run timestamp.
- **REQ-003**: The agent MUST use an LLM call to produce the `ExecutionPlan` when an LLM provider is configured.
- **REQ-004**: If the LLM is unavailable or returns an invalid plan, the agent MUST fall back to a deterministic plan that runs all stages where artifacts are absent or the source fingerprint has changed.
- **REQ-005**: The `ExecutionPlan` MUST be persisted to `reports/monitoring/latest_execution_plan.json` before stages execute.
- **REQ-006**: The operator MUST respect the plan — it MUST NOT run stages absent from the plan unless a forced re-run flag is set.
- **CON-001**: The LLM prompt for plan generation MUST NOT include raw message content from the dataset. Only metadata and statistics are allowed.
- **CON-002**: The execution plan decision MUST complete within 15 seconds. If it does not, the deterministic fallback MUST be used.
- **GUD-001**: The `rationale` field should be human-readable and reference specific observations (e.g., "Silver artifact is 3 days old and source fingerprint changed — rebuilding Silver and Gold").

### Decision 2 — Dynamic Gold Designer

- **REQ-010**: After Silver is built and validated, the agent MUST call the Gold designer to produce a `GoldColumnPlan` before `build_gold()` executes.
- **REQ-011**: The `GoldColumnPlan` MUST include at minimum: two engagement-related columns, one intent/commercial signal column, one behavioral/persona column, and one contextual column. These are floors, not ceilings.
- **REQ-012**: The Gold designer MUST sample Silver leads and Silver messages DataFrames (≤ 50 rows each) and pass them to the LLM as JSON records.
- **REQ-013**: The `GoldColumnPlan` MUST be validated against a schema contract before `build_gold()` uses it. Invalid plans MUST be rejected and replaced with the deterministic fallback plan.
- **REQ-014**: The fallback `GoldColumnPlan` MUST reproduce the existing deterministic Gold columns from `gold.py` so that the pipeline output is always a valid Gold artifact.
- **REQ-015**: The `GoldColumnPlan` MUST be persisted to `reports/agent_decisions/latest_gold_column_plan.json`.
- **REQ-016**: `build_gold()` MUST accept a `GoldColumnPlan` parameter and apply it instead of hardcoded column logic.
- **REQ-017**: Each column in the plan that uses a categorical segmentation MUST define its `segment_values` list explicitly in the plan. `build_gold()` MUST NOT infer segment values dynamically from data distribution.
- **CON-003**: The LLM prompt for Gold design MUST include the data dictionary field descriptions from `docs/data-dictionary-data-ai-engineering.md` as context.
- **CON-004**: Column `derivation_logic` in the plan MUST be expressed as one of: `aggregation` (with `agg_fn` and `source_col`), `conditional_bucket` (with `conditions` list), or `llm_enriched` (delegating to `conversation_enrichment.py`). Free-form Python expressions are not allowed.
- **CON-005**: Gold designer MUST NOT propose columns that require raw PII fields (any column in `forbidden_columns` from the pipeline spec).
- **GUD-002**: The agent should favor columns that go beyond the examples given in the technical brief (email provider distribution, basic persona). Novel segmentation dimensions are preferred.

### Decision 3 — LLM Failure Diagnosis

- **REQ-020**: When `diagnose_validation_failures()` encounters a `(layer, check_name)` pair absent from `VALIDATION_CHECK_MAP`, it MUST invoke an LLM diagnosis call instead of returning a generic fallback.
- **REQ-021**: The LLM diagnosis MUST receive: `layer`, `check_name`, a sample of failing records (≤ 20 rows), the current `pipeline_spec.json` content, and the existing `VALIDATION_CHECK_MAP` entries as context.
- **REQ-022**: The LLM MUST return a structured `LLMDiagnosis` with fields: `kind`, `severity` (`low`|`medium`|`high`|`critical`), `summary`, `suggested_action`, `is_safe_to_auto_apply` (bool), `confidence` (0.0–1.0), and `rationale`.
- **REQ-023**: If `is_safe_to_auto_apply` is `true` AND `confidence` ≥ 0.80 AND `severity` is not `critical`, the agent MAY attempt auto-remediation using the `suggested_action` if it maps to a known playbook ID.
- **REQ-024**: The LLM diagnosis result MUST be persisted alongside the standard agent report in `reports/monitoring/latest_agent_report.json` under a `llm_diagnoses` key.
- **REQ-025**: If the LLM returns a diagnosis with `severity: critical`, the pipeline MUST halt and NOT proceed to subsequent stages, regardless of `is_safe_to_auto_apply`.
- **REQ-026**: The deterministic fallback for unknown checks MUST return a diagnosis with `severity: medium`, `is_safe_to_auto_apply: false`, and a `suggested_action` of `"Manual investigation required — check not recognized by static map"`.
- **CON-006**: The LLM diagnosis prompt MUST NOT include full message body content. Failing records MUST have `message_body` truncated to 60 characters before being passed to the LLM.
- **CON-007**: LLM diagnosis calls MUST be gated by a circuit breaker: if 3 consecutive LLM calls fail within a single run, all subsequent unknown checks MUST use the deterministic fallback for the remainder of that run.

### ReAct Loop (Orchestration)

- **REQ-030**: `run_cycle()` in `operator.py` MUST be refactored into a ReAct loop with a maximum of 3 iterations.
- **REQ-031**: Each iteration MUST begin with an `observe()` step that builds the current `Observation` from filesystem state, validation results, and run history.
- **REQ-032**: Each iteration MUST include a `decide()` step that returns a `LoopAction` — one of: `run_stage(stage_name)`, `retry_stage(stage_name, reason)`, `halt(reason)`, or `complete()`.
- **REQ-033**: The `decide()` step MUST use the `ExecutionPlan` (Decision 1) on the first iteration. On subsequent iterations (after remediations), it MUST re-observe and produce a new `LoopAction` based on current state.
- **REQ-034**: The loop MUST terminate with `complete()` if all planned stages passed validation, or with `halt()` if max iterations are reached or a `critical` diagnosis is raised.
- **CON-008**: The total wall-clock time budget for all LLM calls within one `run_cycle()` invocation is 60 seconds. Calls exceeding this budget MUST be cancelled and replaced with deterministic fallback.

---

## 4. Interfaces & Data Contracts

### 4.1 `ExecutionPlan`

```python
@dataclass(frozen=True)
class ExecutionPlan:
    stages: list[str]           # Ordered list of stage names to execute
    rationale: str              # Human-readable reasoning
    confidence: float           # 0.0–1.0
    source: Literal["llm", "deterministic_fallback"]
    generated_at_utc: str       # ISO 8601
```

Valid stage names: `"bronze"`, `"silver"`, `"gold"`, `"validation"`, `"planning"`.

**JSON schema for `latest_execution_plan.json`:**
```json
{
  "stages": ["bronze", "silver", "gold", "validation", "planning"],
  "rationale": "Source fingerprint changed. All artifacts rebuilt.",
  "confidence": 0.92,
  "source": "llm",
  "generated_at_utc": "2026-04-27T12:00:00Z"
}
```

### 4.2 `GoldColumnPlan`

```python
@dataclass
class GoldColumnDefinition:
    name: str
    data_type: Literal["float64", "int64", "string", "bool"]
    derivation_logic: dict[str, Any]   # see CON-004 for allowed shapes
    rationale: str
    segment_values: list[str] | None   # required when data_type == "string" and type == "conditional_bucket"

@dataclass
class GoldColumnPlan:
    columns: list[GoldColumnDefinition]
    source: Literal["llm", "deterministic_fallback"]
    generated_at_utc: str
    llm_rationale: str | None
```

**Allowed `derivation_logic` shapes:**

`aggregation`:
```json
{ "type": "aggregation", "agg_fn": "sum", "source_col": "has_price_signal" }
```

`conditional_bucket`:
```json
{
  "type": "conditional_bucket",
  "conditions": [
    { "when": "message_count > 20", "then": "high_engagement" },
    { "when": "message_count > 5",  "then": "medium_engagement" },
    { "else": "low_engagement" }
  ]
}
```

`llm_enriched`:
```json
{ "type": "llm_enriched", "field": "sentiment_label" }
```

### 4.3 `LLMDiagnosis`

```python
@dataclass(frozen=True)
class LLMDiagnosis:
    kind: str
    severity: Literal["low", "medium", "high", "critical"]
    summary: str
    suggested_action: str
    is_safe_to_auto_apply: bool
    confidence: float           # 0.0–1.0
    rationale: str
    source: Literal["llm", "deterministic_fallback"]
```

### 4.4 `Observation`

```python
@dataclass(frozen=True)
class Observation:
    source_changed: bool
    layer_artifacts: dict[str, dict]   # {"bronze": {"exists": True, "age_hours": 2.1}, ...}
    last_validation_summary: dict[str, Any]
    last_run_status: str | None
    run_count: int
    generated_at_utc: str
```

### 4.5 `LoopAction`

```python
@dataclass(frozen=True)
class LoopAction:
    kind: Literal["run_stage", "retry_stage", "halt", "complete"]
    stage: str | None           # populated for run_stage and retry_stage
    reason: str
```

### 4.6 New/Modified Module Locations

| Module | Status | Path |
|---|---|---|
| `execution_planner.py` | **New** | `src/pipeline/agent/execution_planner.py` |
| `gold_designer.py` | **New** | `src/pipeline/agent/gold_designer.py` |
| `agent.py` | **Modified** | `src/pipeline/agent/agent.py` |
| `operator.py` | **Modified** | `src/pipeline/orchestration/operator.py` |
| `gold.py` | **Modified** | `src/pipeline/transforms/gold.py` |

---

## 5. Acceptance Criteria

- **AC-001**: Given a source file that has not changed, When `run_cycle()` executes, Then the `ExecutionPlan` MUST have an empty `stages` list (or `["planning"]` only) and `rationale` MUST reference the unchanged fingerprint.
- **AC-002**: Given a new source file, When `run_cycle()` executes without a prior Silver artifact, Then `ExecutionPlan.stages` MUST include `"bronze"`, `"silver"`, `"gold"`, and `"validation"`.
- **AC-003**: Given a valid Silver artifact and an unchanged source fingerprint but a corrupted Gold artifact, When `run_cycle()` executes, Then `ExecutionPlan.stages` MUST include `"gold"` and `"validation"` but NOT `"bronze"` or `"silver"`.
- **AC-004**: Given an LLM timeout during execution planning, When `run_cycle()` executes, Then the `ExecutionPlan.source` MUST be `"deterministic_fallback"` and the pipeline MUST still complete without error.
- **AC-005**: Given a valid Silver dataset, When the Gold designer executes with LLM available, Then `GoldColumnPlan.columns` MUST contain at least 5 columns with distinct `name` values and no duplicates.
- **AC-006**: Given a `GoldColumnPlan` containing a column with `derivation_logic.type == "conditional_bucket"` and no `segment_values`, When validation runs, Then the plan MUST be rejected and replaced with the deterministic fallback.
- **AC-007**: Given a `GoldColumnPlan` proposing a column using a field in `forbidden_columns`, When validation runs, Then that column MUST be removed from the plan and logged as a privacy violation.
- **AC-008**: Given the Gold designer produces a valid `GoldColumnPlan`, When `build_gold()` executes, Then the resulting Gold parquet MUST contain all columns named in the plan with the correct `data_type`.
- **AC-009**: Given a validation failure with a `(layer, check_name)` pair absent from `VALIDATION_CHECK_MAP`, When `diagnose_validation_failures()` executes with LLM available, Then the result MUST include an `LLMDiagnosis` with `source == "llm"` and `confidence > 0`.
- **AC-010**: Given an `LLMDiagnosis` with `severity == "critical"`, When the operator processes the diagnosis, Then `run_cycle()` MUST return with `status == "halted_critical_failure"` and MUST NOT execute subsequent stages.
- **AC-011**: Given 3 consecutive LLM call failures in a single run, When a 4th unknown check is encountered, Then the diagnosis MUST use the deterministic fallback without attempting another LLM call.
- **AC-012**: Given a validation failure after stage execution in the ReAct loop, When the agent decides to `retry_stage`, Then the loop MUST re-execute that stage and re-validate before proceeding.
- **AC-013**: Given the ReAct loop has executed 3 iterations without reaching `complete()`, When iteration 4 would start, Then the loop MUST terminate with `halt(reason="max_iterations_reached")`.
- **AC-014**: Given a complete run, When the run finishes, Then `reports/monitoring/latest_execution_plan.json`, `reports/agent_decisions/latest_gold_column_plan.json`, and `reports/monitoring/latest_agent_report.json` MUST all exist and be valid JSON.

---

## 6. Test Automation Strategy

- **Test Levels**: Unit tests for each new dataclass and LLM prompt builder; integration tests for the full ReAct loop with mocked LLM responses.
- **Frameworks**: `pytest`, `pytest-mock` for LLM call mocking, existing `tests/` structure.
- **LLM Mocking**: All LLM calls in new modules MUST be injectable via a callable parameter (default to `llm_runtime.call_llm`), enabling tests to pass fixture responses without network calls.
- **Deterministic Fallback Coverage**: Each decision (execution plan, Gold designer, LLM diagnosis) MUST have a dedicated test asserting correct fallback behavior when the LLM callable raises `TimeoutError` and when it returns structurally invalid JSON.
- **Gold plan validation**: Tests MUST cover: forbidden column rejection, missing `segment_values` rejection, duplicate column name rejection, and minimum column floor enforcement.
- **ReAct loop tests**: Tests MUST cover: single-iteration happy path, retry after Silver failure, max-iteration halt, and critical-diagnosis halt.
- **CI Integration**: All new tests run in the existing GitHub Actions pipeline with no additional environment variables required (LLM calls are mocked in CI).
- **Coverage Requirement**: New modules (`execution_planner.py`, `gold_designer.py`) MUST reach ≥ 85% line coverage.

---

## 7. Rationale & Context

**Why adaptive execution planning?** The current `run_cycle()` always rebuilds all stages unless the source fingerprint is unchanged (full skip). This is binary: either everything runs or nothing runs. In practice, partial rebuilds are common (Gold corrupted, Silver schema updated) and the agent should identify and execute only the minimum necessary work.

**Why dynamic Gold designer?** The technical test explicitly evaluates creativity in segmentation and rewards going beyond the example insights. Hardcoding Gold columns in `gold.py` means every dataset receives the same analysis regardless of its actual characteristics. An agent that observes the Silver data and decides what to measure is demonstrably more valuable and more aligned with the test's intent.

**Why LLM failure diagnosis instead of static map?** `VALIDATION_CHECK_MAP` can only handle failures that were anticipated at authoring time. A production pipeline encounters novel failures. The LLM diagnosis path handles the long tail of unknown failures with structured output that integrates with the existing remediation machinery, rather than silently dropping to a generic error.

**Why ReAct loop?** The current linear `run_cycle()` cannot iterate: if Silver fails and is auto-remediated, the operator does not re-validate Silver before proceeding to Gold. A ReAct loop makes re-observation and re-decision first-class operations, improving correctness without requiring deep restructuring of the existing stage implementations.

---

## 8. Dependencies & External Integrations

### External Systems
- **EXT-001**: LLM provider (Anthropic Claude or OpenAI) — used for execution planning, Gold design, and failure diagnosis. Must support structured/JSON output mode.

### Third-Party Services
- **SVC-001**: Langfuse (optional) — if configured, all three LLM decision calls MUST be traced with their prompts and structured outputs for observability.

### Infrastructure Dependencies
- **INF-001**: Existing `llm_runtime.py` — all LLM calls in new modules MUST go through `llm_runtime.call_llm()` to inherit retry logic, timeout enforcement, and Langfuse tracing.

### Data Dependencies
- **DAT-001**: `docs/data-dictionary-data-ai-engineering.md` — Gold designer prompt context. Must be read at runtime, not hardcoded.
- **DAT-002**: `config/pipeline_spec.json` — forbidden columns list and bucket definitions injected into Gold designer validation and LLM diagnosis prompts.
- **DAT-003**: Silver DataFrames (`silver_leads.parquet`, `silver_messages.parquet`) — sampled for Gold designer input.

### Technology Platform Dependencies
- **PLT-001**: Python ≥ 3.11 — required for `Literal` type hints and `dataclass(frozen=True)` pattern used throughout.

### Compliance Dependencies
- **COM-001**: PII isolation — LLM prompts MUST NOT contain raw PII. `message_body` fields in samples must be truncated or masked before passing to any LLM call. This is enforced by CON-001, CON-005, and CON-006.

---

## 9. Examples & Edge Cases

### LLM Execution Plan prompt structure

```python
EXECUTION_PLAN_PROMPT = """
You are a data pipeline orchestration agent. Given the current pipeline state, decide
which stages need to run.

## Current Observation
{observation_json}

## Available Stages
bronze, silver, gold, validation, planning

## Rules
- If source fingerprint is unchanged AND all layer artifacts exist AND last validation passed: return empty stages list.
- If an artifact is missing: include that stage and all downstream stages.
- If last validation failed for a layer: include that layer and all downstream stages.
- If source fingerprint changed: include all stages.

## Output Format (JSON only, no prose)
{{"stages": [...], "rationale": "...", "confidence": 0.0}}
"""
```

### Edge case: Gold designer receives a Silver with no commercial signals

If the Silver sample contains zero rows with `has_price_signal == True` or `has_competitor_signal == True`, the LLM may propose zero commercial columns. The Gold designer validator MUST detect this and supplement the plan with the deterministic fallback columns for the commercial dimension before returning.

### Edge case: LLM returns a `GoldColumnPlan` with a `derivation_logic` referencing a non-existent Silver column

`build_gold()` MUST validate that every `source_col` referenced in `derivation_logic.agg_fn` exists in the Silver DataFrame columns before applying the plan. Missing source columns MUST cause that specific column to be dropped from the plan, logged as a warning, and replaced with `None` values in the Gold output.

### Edge case: ReAct loop retry creates an infinite remediation cycle

If iteration N remediates Silver and iteration N+1 validates Silver and it still fails, the loop MUST NOT retry Silver a third time. After two consecutive failures of the same stage, the loop MUST produce `halt(reason="repeated_stage_failure:<stage>")`.

---

## 10. Validation Criteria

- `execution_planner.py` exports `build_execution_plan(observation: Observation, paths: PipelinePaths) -> ExecutionPlan` with no side effects other than writing `latest_execution_plan.json`.
- `gold_designer.py` exports `design_gold_columns(silver_leads_df: pd.DataFrame, silver_messages_df: pd.DataFrame, spec: dict, paths: PipelinePaths) -> GoldColumnPlan` with no side effects other than writing `latest_gold_column_plan.json`.
- `agent.py:diagnose_validation_failures()` returns `list[AgentDiagnosis]` where each unknown check produces a diagnosis with `source` field set to either `"llm"` or `"deterministic_fallback"`.
- `operator.py:run_cycle()` persists all three decision artifacts (`latest_execution_plan.json`, `latest_gold_column_plan.json`, updated `latest_agent_report.json`) on every executed run.
- The pipeline produces a valid Gold parquet with no fewer columns than the deterministic baseline, regardless of LLM availability.
- All tests pass with LLM calls fully mocked (no network dependency in CI).

---

## 11. Related Specifications / Further Reading

- [spec-architecture-agent-autonomy-governed-by-impact.md](./spec-architecture-agent-autonomy-governed-by-impact.md) — governance layer for spec mutations (unchanged by this spec)
- [spec-architecture-llm-conversation-enrichment.md](./spec-architecture-llm-conversation-enrichment.md) — LLM enrichment for conversation-level Silver fields
- [spec-architecture-llm-provider-runtime-integration.md](./spec-architecture-llm-provider-runtime-integration.md) — `llm_runtime.py` contract that all LLM calls in this spec must use
- [spec-architecture-gold-audience-segment-contract-correction.md](./spec-architecture-gold-audience-segment-contract-correction.md) — existing Gold segment contract (baseline for dynamic designer fallback)
- [docs/data-dictionary-data-ai-engineering.md](../docs/data-dictionary-data-ai-engineering.md) — source dataset field descriptions injected into Gold designer prompt
