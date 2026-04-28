---
title: Architecture Specification for Pipeline Spec Runtime Parity and Execution Validation
version: 1.0
date_created: 2026-04-21
last_updated: 2026-04-21
owner: Lucas Neves
tags: [architecture, pipeline, spec, runtime, validation, testing, configuration]
---

# Introduction

This specification defines the correction required to restore parity between the versioned pipeline specification, the runtime validator, the repository entrypoints, and the automated test environment.

The immediate goal is to ensure that the project can be executed successfully from the real repository workspace using the documented commands, without relying on implicit test-only conditions or temporary directories that bypass versioned configuration.

## 1. Purpose & Scope

This specification defines requirements, constraints, interfaces, and validation rules for correcting the current execution failure in which the repository test suite passes while the real pipeline runtime fails before processing the Bronze source.

Scope:

- Align `config/pipeline_spec.json` with the schema enforced by `src/pipeline/spec.py`.
- Ensure the runtime can execute successfully using the repository's documented entrypoints.
- Ensure automated tests cover the repository-local execution path instead of only isolated temporary-path execution.
- Define explicit behavior for environment-driven LLM toggles during validation.
- Update repository documentation to reflect the corrected runtime expectations.

Out of scope:

- Redesigning the medallion pipeline.
- Changing the business semantics of Bronze, Silver, or Gold transformations.
- Replacing the current planner, operator, or approval model.
- Expanding LLM enrichment capabilities beyond what is already implemented.
- Introducing external orchestration systems or CI-specific runtime dependencies.

Intended audience:

- Engineers implementing the correction
- Reviewers validating runtime readiness
- Future maintainers responsible for keeping tests and repository configuration aligned

Assumptions:

- The repository is executed via the local virtual environment at `venv/`.
- `config/pipeline_spec.json` is a versioned artifact intended to be consumed by the runtime.
- `.env` can influence runtime behavior and therefore must be considered part of execution validation.
- A passing test suite is insufficient if the repository-local entrypoint still fails.

## 2. Definitions

- **Pipeline Spec**: The versioned JSON contract stored at `config/pipeline_spec.json` and consumed by the runtime.
- **Default Spec**: The in-code fallback specification returned by `default_pipeline_spec()` in `src/pipeline/spec.py`.
- **Runtime Parity**: The condition in which repository-local execution, configuration files, and automated tests reflect the same effective behavior.
- **Repository-Local Execution**: Running the project from the checked-out repository root using documented scripts and existing versioned configuration.
- **Isolated Test Execution**: Running the pipeline in a temporary directory or synthetic environment that does not reuse the repository's checked-in configuration files.
- **Spec Drift**: Any mismatch between the versioned pipeline spec and the schema expected by the runtime validator.
- **Validation Command**: A documented command used to confirm the project runs correctly in the intended local environment.
- **LLM Enrichment Toggle**: The environment-driven flag `PIPELINE_ENABLE_LLM_ENRICHMENT` that enables or disables external semantic enrichment.
- **Execution Baseline**: The minimum supported repository-local configuration under which the pipeline must complete successfully.

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: The versioned file `config/pipeline_spec.json` shall be valid under `validate_pipeline_spec()` without requiring runtime-generated repair.
- **REQ-002**: The repository-local command `venv/bin/python scripts/run_pipeline.py --force` shall complete successfully when executed from the repository root under the supported baseline environment.
- **REQ-003**: The supported baseline environment for local validation shall be explicit and reproducible.
- **REQ-004**: Automated tests shall include at least one repository-local execution path that uses the checked-in `config/pipeline_spec.json`.
- **REQ-005**: Automated tests shall fail when the versioned spec is missing fields required by the runtime validator.
- **REQ-006**: If the runtime relies on environment variables that materially change execution behavior, test coverage shall include those execution modes or explicitly pin them during validation.
- **REQ-007**: The correction shall define whether the canonical fix is to update `config/pipeline_spec.json`, relax validator requirements, or both, and that decision shall be reflected consistently in code and documentation.
- **REQ-008**: The runtime shall not silently bypass an invalid versioned spec by falling back to the in-code default when the versioned file exists.
- **REQ-009**: The documentation shall describe the validated execution path, including any required environment flags for deterministic local validation.
- **REQ-010**: The correction shall preserve existing published artifact contracts unless an intentional schema change is documented.
- **REQ-011**: If `.env` enables optional external integrations by default, the documented validation flow shall state whether those integrations are part of the supported baseline or must be disabled for local validation.
- **REQ-012**: The acceptance path for this correction shall include both runtime execution and automated tests.

- **TST-001**: The test suite shall distinguish between unit-level schema validity and repository-level executable validity.
- **TST-002**: Tests that create a temporary root shall not be treated as sufficient evidence that the checked-in repository is runnable.
- **TST-003**: At least one automated check shall assert that `config/pipeline_spec.json` and `default_pipeline_spec()` are compatible at the required fields level.

- **CON-001**: The correction shall use the repository virtual environment commands documented in `AGENTS.md`.
- **CON-002**: The correction shall not depend on globally installed Python executables or test runners.
- **CON-003**: The correction shall not require network access for the minimum local validation path.
- **CON-004**: The correction shall preserve the existing repository structure, including `config/`, `src/`, `tests/`, and `spec/`.
- **CON-005**: The correction shall not introduce hidden runtime mutation of checked-in configuration solely to make execution pass.

- **GUD-001**: Prefer making the checked-in spec valid over adding compensating runtime behavior that obscures configuration drift.
- **GUD-002**: Prefer explicit test names that distinguish `default spec`, `versioned spec`, and `repository-local runtime`.
- **GUD-003**: Keep the execution baseline simple: deterministic local run first, optional LLM-enabled runtime second.
- **GUD-004**: Document the difference between deterministic validation mode and integration mode with external providers.

- **PAT-001**: Recommended validation pattern: validate versioned spec -> run repository-local pipeline -> run full test suite -> run focused adherence suite.
- **PAT-002**: Recommended configuration parity pattern: update versioned spec and tests together in the same change set.

## 4. Interfaces & Data Contracts

### 4.1 Relevant Files

| Path | Role | Required Outcome |
| --- | --- | --- |
| `config/pipeline_spec.json` | Versioned runtime spec | Must satisfy `validate_pipeline_spec()` |
| `src/pipeline/spec.py` | Default spec and validator | Must define the canonical schema rules |
| `scripts/run_pipeline.py` | Repository-local entrypoint | Must execute successfully under baseline validation mode |
| `tests/test_spec.py` | Spec contract tests | Must validate both default and versioned spec behavior |
| `tests/test_jobs.py` | Runtime execution tests | Must cover repository-representative execution paths |
| `README.md` | Operator-facing execution documentation | Must describe the validated commands and environment expectations |
| `.env` / `.env.example` | Runtime toggle sources | Must not leave the validation path ambiguous |

### 4.2 Required Spec Parity Contract

The runtime parity correction shall enforce the following contract:

```json
{
  "versioned_spec_exists": true,
  "versioned_spec_is_valid": true,
  "default_spec_is_valid": true,
  "versioned_spec_required_fields_superset_or_equal_to_validator_minimum": true,
  "repository_local_entrypoint_runs": true
}
```

### 4.3 Minimum Required Compatibility Areas

The correction shall explicitly validate compatibility for at least the following sections:

| Section | Minimum compatibility rule |
| --- | --- |
| `silver.derived_fields` | Must exist and be a non-empty list if required by validator |
| `gold.valid_*` enumerations | Must exist for every validator-enforced list path |
| `quality.validation_rules` | Must exist and provide non-empty lists for Bronze, Silver, and Gold |
| `agent.*` planner structure | Must remain valid for planner compilation |
| `llm.*` runtime settings | Must remain valid for runtime compilation and deterministic fallback mode |

### 4.4 Execution Mode Contract

The repository shall define at least two explicit local execution modes:

| Mode | Purpose | External providers | Required outcome |
| --- | --- | --- | --- |
| `deterministic_validation` | Baseline local validation | Disabled | Pipeline must complete without network access |
| `integration_runtime` | Optional provider-enabled execution | Optional | Behavior depends on credentials and provider availability, but must be documented |

Recommended command examples:

```bash
PIPELINE_ENABLE_LLM_ENRICHMENT=0 venv/bin/python scripts/run_pipeline.py --force
venv/bin/python -m pytest -q
venv/bin/python -m pytest tests/test_requirements_adherence.py -q
```

### 4.5 Test Interface Expectations

The automated test layer shall provide distinct assertions for:

- Schema validity of `default_pipeline_spec()`
- Schema validity of `config/pipeline_spec.json`
- Successful repository-local execution using versioned config
- Deterministic validation mode with LLM enrichment disabled

## 5. Acceptance Criteria

- **AC-001**: Given the checked-in repository state, When `config/pipeline_spec.json` is loaded by `validate_pipeline_spec()`, Then no validation error shall be raised.
- **AC-002**: Given the repository root and the local virtual environment, When `PIPELINE_ENABLE_LLM_ENRICHMENT=0 venv/bin/python scripts/run_pipeline.py --force` is executed, Then the pipeline shall complete and emit artifact metadata instead of raising a spec validation error.
- **AC-003**: Given the automated test suite, When the versioned spec drifts from validator-required fields, Then at least one test shall fail before runtime validation is attempted manually.
- **AC-004**: Given the repository documentation, When an operator follows the documented validation commands, Then the documented baseline execution path shall match the behavior exercised in tests.
- **AC-005**: Given the baseline deterministic validation mode, When local validation is performed without network access, Then the project shall still be runnable.
- **AC-006**: Given optional provider-enabled execution, When `.env` enables LLM enrichment, Then the documentation shall make clear whether this is required, optional, or excluded from baseline validation.

## 6. Test Automation Strategy

- **Test Levels**: Unit, integration, repository-local execution
- **Frameworks**: `pytest` via `venv/bin/python -m pytest`
- **Test Data Management**: Reuse existing repository fixtures for isolated tests and add repository-local validation coverage against checked-in configuration
- **CI/CD Integration**: The same commands used locally should be suitable for CI execution without relying on global binaries
- **Coverage Requirements**: Cover both spec-schema validity and runnable-entrypoint validity
- **Performance Testing**: Not required for this correction

Required automated checks:

- `tests/test_spec.py` shall verify `default_pipeline_spec()` validity.
- A spec-focused test shall verify `config/pipeline_spec.json` validity.
- A runtime-focused test shall verify repository-local execution under deterministic validation mode.
- `venv/bin/python -m pytest -q` shall remain green after the correction.
- `venv/bin/python -m pytest tests/test_requirements_adherence.py -q` shall remain green after the correction.

## 7. Rationale & Context

The current repository exhibits a correctness gap between controlled tests and the actual user-facing execution path.

Observed behavior:

- The full test suite passes.
- The adherence suite passes.
- The documented runtime entrypoint fails immediately because `config/pipeline_spec.json` is missing fields now required by `validate_pipeline_spec()`.
- The test suite primarily exercises isolated temporary roots, which can avoid the invalid versioned spec by relying on `default_pipeline_spec()`.
- The real repository environment also loads `.env`, which enables LLM enrichment by default and therefore introduces another difference between test execution and local runtime execution.

This is an execution-readiness problem, not merely a unit-test problem. A repository that passes tests but cannot run through its documented entrypoint is not operationally valid as a technical delivery. The correction must therefore restore a single coherent truth across code, config, tests, and documentation.

## 8. Dependencies & External Integrations

### External Systems

- **EXT-001**: Local filesystem - Required for reading versioned config, Bronze input, state, and generated artifacts

### Third-Party Services

- **SVC-001**: Optional LLM providers - Not required for baseline deterministic validation mode

### Infrastructure Dependencies

- **INF-001**: Repository-local virtual environment at `venv/` - Required execution baseline

### Data Dependencies

- **DAT-001**: `docs/conversations_bronze.parquet` - Required source artifact for repository-local execution validation

### Technology Platform Dependencies

- **PLT-001**: Python runtime available through `venv/bin/python` - Required by repository commands and tests

### Compliance Dependencies

- **COM-001**: Existing repository privacy and masking rules - Must remain unchanged by this correction

## 9. Examples & Edge Cases

```text
Edge Case 1: Versioned spec drift
- Versioned file exists
- Runtime validator requires new list fields
- Tests that only use temporary directories still pass
- Repository-local execution fails immediately
- Expected correction: add direct test coverage and restore config validity

Edge Case 2: Deterministic validation mode
- `.env` enables LLM enrichment by default
- Local machine has no network access or no valid provider credentials
- Expected correction: documented validation command disables optional enrichment explicitly

Edge Case 3: Intentional future schema evolution
- Validator adds new required spec fields
- Expected correction: same change updates default spec, versioned spec, and tests in one cycle
```

## 10. Validation Criteria

- `config/pipeline_spec.json` is valid against the active validator.
- Repository-local pipeline execution succeeds in deterministic validation mode.
- Tests explicitly detect future spec drift in checked-in configuration.
- Documentation reflects the real supported validation commands.
- No change regresses current artifact contracts for Bronze, Silver, or Gold.

## 11. Related Specifications / Further Reading

- [README.md](/home/lucas/projects/lucas54neves/namastex-test/README.md)
- [config/pipeline_spec.json](/home/lucas/projects/lucas54neves/namastex-test/config/pipeline_spec.json)
- [src/pipeline/spec.py](/home/lucas/projects/lucas54neves/namastex-test/src/pipeline/spec.py)
- [tests/test_spec.py](/home/lucas/projects/lucas54neves/namastex-test/tests/test_spec.py)
- [tests/test_jobs.py](/home/lucas/projects/lucas54neves/namastex-test/tests/test_jobs.py)
