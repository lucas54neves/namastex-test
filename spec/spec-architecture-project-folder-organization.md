---
title: Architecture Specification for Project Folder Organization and Safe Module Refactor
version: 1.0
date_created: 2026-04-22
last_updated: 2026-04-22
owner: Lucas Neves
tags: [architecture, refactor, repository, structure, pipeline, maintainability]
---

# Introduction

This specification defines the target folder architecture for the repository and the constraints for implementing it safely.

The goal is to improve navigability, evaluation clarity, and maintenance quality for a technical-test delivery without turning the project into an over-engineered platform. The change is structural. It must preserve the current pipeline behavior, entrypoints, published artifacts, and validation semantics.

## 1. Purpose & Scope

This specification defines requirements, constraints, interfaces, migration rules, and acceptance criteria for reorganizing the repository source tree around technical responsibilities instead of keeping all pipeline modules flattened under `src/pipeline/`.

Scope:

- Reorganize the `src/pipeline/` package into smaller subpackages with explicit responsibilities.
- Preserve the public execution flow through the current `scripts/` entrypoints.
- Preserve existing Bronze, Silver, Gold, state, report, and quarantine artifact locations unless explicitly changed by a later specification.
- Preserve the current runtime behavior for deterministic execution and optional LLM enrichment.
- Improve test discoverability by separating test intent where appropriate.
- Document the target structure well enough that implementation can be performed incrementally.

Out of scope:

- Redesigning Bronze, Silver, or Gold business logic.
- Changing output schemas of published parquet artifacts.
- Replacing the current agentic operational model.
- Introducing external orchestration frameworks.
- Moving configuration, data, reports, or state out of their current top-level directories.
- Converting the repository into a monorepo or multi-package workspace.

Intended audience:

- Engineers implementing the refactor
- Reviewers evaluating project organization and maintainability
- Future maintainers extending the pipeline or the operational agent

Assumptions:

- The repository remains a single Python package rooted at `src/pipeline/`.
- The repository is primarily evaluated as a technical test delivery.
- The local virtual environment at `venv/` remains the required execution environment.
- Refactor risk must be minimized because the repository already contains working runtime and tests.

## 2. Definitions

- **Flattened Package**: A package organization in which most modules exist at the same directory level, regardless of distinct responsibilities.
- **Responsibility-Oriented Structure**: A package organization in which modules are grouped by technical concern, such as transforms, orchestration, quality, and agent operations.
- **Entrypoint**: A script or callable used to execute pipeline flows, such as `scripts/run_pipeline.py`.
- **Published Artifact Contract**: The set of file paths, schemas, and semantics already exposed by Bronze, Silver, Gold, reports, state, and quarantine outputs.
- **Safe Refactor**: A structural code change that preserves externally visible behavior and keeps tests green.
- **Technical-Test Readability**: The property that a reviewer can quickly locate the source of ingestion, transformation, validation, publication, and operational logic.
- **Operational Agent Layer**: The modules responsible for planning, diagnosing, approval, alerting, playbooks, and remediation logic.
- **Runtime Layer**: The modules responsible for environment loading, spec handling, state persistence, and optional LLM runtime integration.
- **Transformation Layer**: The modules responsible for Bronze loading, Silver derivation, Gold derivation, and conversation enrichment preparation.

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: The repository shall keep `src/pipeline/` as the single import root for application code.
- **REQ-002**: The implementation shall reorganize source files into subpackages grouped by technical responsibility rather than by a single flat directory.
- **REQ-003**: The implementation shall preserve the current script entrypoints in `scripts/`.
- **REQ-004**: The implementation shall preserve the current top-level repository directories `config/`, `docs/`, `scripts/`, `src/`, `tests/`, `data/`, `reports/`, `state/`, and `spec/`.
- **REQ-005**: The implementation shall preserve the runtime behavior of the deterministic pipeline execution path.
- **REQ-006**: The implementation shall preserve the optional LLM enrichment behavior and its current separation from the main operational agent.
- **REQ-007**: The implementation shall preserve the published artifact paths unless a separate specification explicitly changes them.
- **REQ-008**: The implementation shall preserve current public Python interfaces used by scripts and tests, either directly or through compatibility imports.
- **REQ-009**: The implementation shall document the target folder structure in `README.md` when the refactor is implemented.
- **REQ-010**: The implementation shall avoid mixing transformation logic, runtime state management, and operational agent logic in the same package level when a clearer boundary is available.
- **REQ-011**: The implementation shall define a clear target mapping from each current module to its target subpackage.
- **REQ-012**: The implementation shall support incremental migration so the refactor can be applied in more than one commit if needed.

- **TST-001**: The automated test suite shall continue to run through `venv/bin/python -m pytest -q`.
- **TST-002**: Focused runtime tests shall continue to validate repository execution through the current scripts.
- **TST-003**: Refactor validation shall include checks for import stability after module relocation.

- **CON-001**: The refactor shall not introduce a second top-level application package.
- **CON-002**: The refactor shall not require consumers to run the pipeline through a new command syntax.
- **CON-003**: The refactor shall not change the semantic contract of Bronze, Silver, Gold, quarantine, reports, or state outputs.
- **CON-004**: The refactor shall use the repository virtual environment commands defined in `AGENTS.md`.
- **CON-005**: The refactor shall not depend on network access for validation.
- **CON-006**: The refactor shall not require speculative abstractions such as plugin systems, service containers, or custom framework layers.
- **CON-007**: The refactor shall remain easy to evaluate for a reviewer with limited time.

- **GUD-001**: Prefer a small number of high-signal subpackages over many thin folders with only one unstable file each.
- **GUD-002**: Prefer names that describe technical responsibility directly, such as `transforms`, `quality`, `agent`, and `orchestration`.
- **GUD-003**: Keep operational and analytical code discoverable without requiring the reader to understand internal implementation details first.
- **GUD-004**: Use compatibility imports during migration when they materially reduce churn in scripts and tests.
- **GUD-005**: Remove or ignore local temporary directories and cache artifacts from the repository structure narrative.

- **PAT-001**: Recommended structural pattern: one package root, responsibility-oriented subpackages, stable script entrypoints.
- **PAT-002**: Recommended migration pattern: create target subpackages -> move modules with compatibility shims if needed -> update imports -> run tests -> update documentation.
- **PAT-003**: Recommended review pattern: verify tree readability first, then import stability, then runtime stability.

## 4. Interfaces & Data Contracts

### 4.1 Target Repository Structure

The target source structure shall be:

```text
src/pipeline/
  __init__.py
  config.py
  agent/
    __init__.py
    agent.py
    alerts.py
    approval.py
    llm_advisor.py
    planner.py
    playbooks.py
  io/
    __init__.py
    parquet_io.py
  orchestration/
    __init__.py
    compiler.py
    jobs.py
    operator.py
  quality/
    __init__.py
    publication.py
    quality.py
    quarantine.py
  runtime/
    __init__.py
    env.py
    llm_runtime.py
    spec.py
    state.py
  transforms/
    __init__.py
    bronze.py
    conversation_enrichment.py
    gold.py
    silver.py
```

The top-level repository structure shall remain:

```text
config/
docs/
scripts/
src/
tests/
data/
reports/
state/
spec/
```

### 4.2 Target Module Mapping

The current modules shall migrate according to the following target map:

| Current Path | Target Path | Responsibility |
| --- | --- | --- |
| `src/pipeline/agent.py` | `src/pipeline/agent/agent.py` | operational diagnosis and remediation entrypoints |
| `src/pipeline/alerts.py` | `src/pipeline/agent/alerts.py` | alert generation and handling |
| `src/pipeline/approval.py` | `src/pipeline/agent/approval.py` | approval workflow |
| `src/pipeline/planner.py` | `src/pipeline/agent/planner.py` | structural planning |
| `src/pipeline/playbooks.py` | `src/pipeline/agent/playbooks.py` | approved remediation playbooks |
| `src/pipeline/llm_advisor.py` | `src/pipeline/agent/llm_advisor.py` | LLM advisory logic for operational support |
| `src/pipeline/compiler.py` | `src/pipeline/orchestration/compiler.py` | spec compilation into runtime plan |
| `src/pipeline/jobs.py` | `src/pipeline/orchestration/jobs.py` | public runtime job wrappers |
| `src/pipeline/operator.py` | `src/pipeline/orchestration/operator.py` | cycle orchestration |
| `src/pipeline/quality.py` | `src/pipeline/quality/quality.py` | validation and quality rules |
| `src/pipeline/publication.py` | `src/pipeline/quality/publication.py` | publication sanitization rules |
| `src/pipeline/quarantine.py` | `src/pipeline/quality/quarantine.py` | quarantine behavior |
| `src/pipeline/env.py` | `src/pipeline/runtime/env.py` | environment loading and runtime flags |
| `src/pipeline/spec.py` | `src/pipeline/runtime/spec.py` | spec defaulting and validation |
| `src/pipeline/state.py` | `src/pipeline/runtime/state.py` | runtime state persistence |
| `src/pipeline/llm_runtime.py` | `src/pipeline/runtime/llm_runtime.py` | provider runtime integration |
| `src/pipeline/io.py` | `src/pipeline/io/parquet_io.py` | filesystem and parquet/json persistence |
| `src/pipeline/conversation_enrichment.py` | `src/pipeline/transforms/conversation_enrichment.py` | conversation-level enrichment assembly |
| `src/pipeline/transforms.py` | `src/pipeline/transforms/bronze.py`, `src/pipeline/transforms/silver.py`, `src/pipeline/transforms/gold.py` | Bronze loading, Silver derivation, Gold derivation |

### 4.3 Import Compatibility Contract

The implementation shall preserve the following import-level expectations during migration:

| Consumer Type | Contract |
| --- | --- |
| Existing scripts in `scripts/` | Must continue to work without changing command names |
| Existing tests in `tests/` | Must continue to import callable functionality with minimal or no behavioral change |
| Internal modules | May update imports to target subpackages as part of the refactor |

Recommended compatibility options:

```python
# Option A: direct import update everywhere after file moves
from pipeline.orchestration.operator import run_cycle

# Option B: temporary compatibility re-export in the old module path
from pipeline.orchestration.operator import run_cycle
```

The implementation may use temporary compatibility re-export modules if doing so reduces migration risk. If compatibility files are used, they should be removed only after all internal and test imports are updated and validated.

### 4.4 Test Layout Contract

The test tree may be reorganized to improve discoverability, but it shall remain compatible with current pytest discovery.

Recommended test structure:

```text
tests/
  conftest.py
  unit/
  integration/
  contract/
```

The following semantic split is recommended:

| Test Group | Purpose |
| --- | --- |
| `tests/unit/` | isolated function and module behavior |
| `tests/integration/` | end-to-end runtime and script-level behavior |
| `tests/contract/` | schema, artifact, and pipeline contract validation |

### 4.5 Non-Goals Contract

The refactor defined by this specification shall not:

- move `data/`, `reports/`, or `state/` inside `src/`
- collapse the project into a framework-specific app layout
- change CLI names or script semantics
- introduce external infrastructure dependencies
- rewrite business rules just to fit the new tree

## 5. Acceptance Criteria

- **AC-001**: Given the current repository, When the source tree is reorganized according to this specification, Then `src/pipeline/` shall contain responsibility-oriented subpackages instead of a flat module list.
- **AC-002**: Given the current script entrypoints, When `venv/bin/python scripts/run_pipeline.py --force` is executed after the refactor, Then the pipeline shall complete successfully under the supported deterministic validation mode.
- **AC-003**: Given the existing automated suite, When `venv/bin/python -m pytest -q` is executed after the refactor, Then the suite shall pass without requiring global Python tooling.
- **AC-004**: Given the current published artifact contracts, When the refactor is complete, Then Bronze, Silver, Gold, quarantine, state, and report outputs shall keep the same paths and behavioral semantics.
- **AC-005**: Given a reviewer opening the repository for the first time, When they inspect `src/pipeline/`, Then they shall be able to identify where orchestration, transformations, quality rules, runtime concerns, and agent operations live without reading unrelated files.
- **AC-006**: Given the migration plan, When module files are moved, Then each current module shall have a defined target destination or an explicit decision to remain in place.
- **AC-007**: Given optional compatibility imports, When transitional modules are used, Then they shall forward behavior without changing runtime semantics.
- **AC-008**: Given repository documentation, When the refactor is implemented, Then `README.md` shall reflect the updated source tree and explain the new package boundaries.

## 6. Test Automation Strategy

- **Test Levels**: Unit, integration, contract
- **Frameworks**: `pytest` via `venv/bin/python -m pytest`
- **Test Data Management**: Reuse current repository fixtures and Bronze sample inputs; do not require external services for baseline validation
- **CI/CD Integration**: The local commands shall remain suitable for CI execution without depending on globally installed binaries
- **Coverage Requirements**: Validate import stability, runtime entrypoints, and artifact contract preservation
- **Performance Testing**: Not required for this refactor

Required automated validation steps:

- `venv/bin/python -m pytest -q`
- `venv/bin/python -m pytest tests/test_jobs.py -q`
- `venv/bin/python -m pytest tests/test_requirements_adherence.py -q`

Recommended additional validation checks:

- import smoke tests for relocated modules
- a repository-tree assertion if the implementation adds explicit structural tests
- a script-entrypoint smoke test covering `run_pipeline.py` and `monitor_pipeline.py`

## 7. Rationale & Context

The current repository already shows strong separation at the top level. The main structural weakness is concentrated inside `src/pipeline/`, where multiple concerns currently live at the same depth:

- pipeline orchestration
- transformation logic
- publication and quality rules
- runtime environment and state handling
- operational agent logic
- optional LLM runtime logic

That layout is still workable for a small project, but it becomes harder to evaluate quickly because the package no longer signals responsibility clearly. For a technical test, this matters. Reviewers usually want to answer a short list of questions fast:

- Where is the pipeline executed?
- Where are Bronze, Silver, and Gold transformations defined?
- Where are validations and privacy rules defined?
- Where is the operational agent logic located?
- Where are runtime and provider-specific concerns handled?

The responsibility-oriented structure defined here improves those answers without pushing the project into unnecessary complexity. It keeps one package, one set of scripts, and one repository. It does not impose framework ceremony. It simply makes the current architectural boundaries visible.

This structure is preferable to a pure Bronze/Silver/Gold package split because this repository contains a meaningful operational layer that is not itself a data layer. Grouping only by medallion layer would force planner, approval, alerts, runtime state, and publication logic into awkward locations or duplicate cross-cutting concerns.

## 8. Dependencies & External Integrations

### External Systems

- **EXT-001**: Local filesystem - Required for code organization, versioned config, Bronze source access, state, reports, and published artifacts

### Third-Party Services

- **SVC-001**: Optional LLM providers - Existing optional integration that must remain isolated in the runtime-related area and must not be required for baseline validation

### Infrastructure Dependencies

- **INF-001**: Repository-local virtual environment at `venv/` - Required execution environment for tests and validation

### Data Dependencies

- **DAT-001**: `docs/conversations_bronze.parquet` - Required source dataset for runtime validation after refactor

### Technology Platform Dependencies

- **PLT-001**: Python package layout rooted at `src/` - Required package organization strategy
- **PLT-002**: `pytest` test discovery compatible with current repository conventions - Required test execution contract

### Compliance Dependencies

- **COM-001**: Existing privacy, masking, and publication rules - Must remain behaviorally unchanged by the structural refactor

## 9. Examples & Edge Cases

```text
Example 1: Safe module relocation
- Current import: from pipeline.operator import run_cycle
- Target implementation: move code to pipeline.orchestration.operator
- Safe migration path:
  1. create the new target module
  2. move implementation
  3. add compatibility re-export if needed
  4. update internal imports
  5. run tests

Edge Case 1: Large transforms module
- Current state: Bronze loading, Silver derivation, and Gold derivation share one file
- Risk: a single file hides distinct transformation boundaries
- Expected handling: split by responsibility into bronze.py, silver.py, and gold.py while preserving function signatures where possible

Edge Case 2: Runtime and agent overlap
- Current state: some operational and runtime concerns may appear adjacent
- Risk: reviewers confuse pipeline execution orchestration with diagnosis and remediation logic
- Expected handling: keep runtime state/spec/env in runtime/, and planning/alerts/approval/remediation in agent/

Edge Case 3: Over-fragmentation
- Risk: creating too many tiny folders or too many helper modules
- Expected handling: keep only high-value subpackages and avoid speculative abstractions
```

## 10. Validation Criteria

The refactor shall be considered compliant with this specification only if all of the following are true:

- the source tree matches the responsibility-oriented structure or an equivalent structure with the same boundaries
- all current modules have been mapped to a target location or intentionally retained with justification
- the baseline pipeline execution still works through the current scripts
- the automated tests still pass via the repository virtual environment
- published artifact paths and semantics remain unchanged
- `README.md` documents the resulting architecture clearly
- no new external runtime dependency is required for deterministic validation

## 11. Related Specifications / Further Reading

- [spec/spec-architecture-pipeline-spec-runtime-parity.md](./spec-architecture-pipeline-spec-runtime-parity.md)
- [spec/spec-architecture-llm-provider-runtime-integration.md](./spec-architecture-llm-provider-runtime-integration.md)
- [README.md](../README.md)
- [AGENTS.md](../AGENTS.md)
