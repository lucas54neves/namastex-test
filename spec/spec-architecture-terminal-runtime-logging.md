---
title: Architecture Specification for Terminal Runtime Logging in the Pipeline
version: 1.0
date_created: 2026-04-22
last_updated: 2026-04-22
owner: Lucas Neves
tags: [architecture, logging, observability, runtime, terminal, pipeline]
---

# Introduction

This specification defines the requirements, constraints, interfaces, and validation rules for adding terminal-only runtime logs to the pipeline execution flow.

The objective is to make pipeline progress visible while commands are running, without introducing file-based log persistence, external observability services, or changes to the current artifact contracts.

## 1. Purpose & Scope

This specification defines how terminal logs shall be exposed during execution of the repository runtime entrypoints.

Scope:

- Add human-readable logs to terminal execution paths.
- Cover the single-run entrypoint and the daemon entrypoint.
- Emit progress logs for the main pipeline stages executed inside the orchestration layer.
- Preserve the current JSON artifact outputs and report generation behavior.
- Define logging boundaries for sensitive data handling.

Out of scope:

- Persisting logs to files under `reports/`, `state/`, or any other directory.
- Sending logs to external systems, dashboards, or monitoring platforms.
- Replacing validation reports, agent reports, alert reports, or monitor snapshots.
- Changing Bronze, Silver, Gold, enrichment, planner, or quality business rules.
- Introducing asynchronous logging infrastructure or distributed tracing.

Intended audience:

- Engineers implementing the runtime logging behavior
- Reviewers validating operational usability of the CLI execution flow
- Maintainers extending observability later without breaking terminal behavior

Assumptions:

- The repository is executed through `venv/bin/python`.
- The main execution path remains centralized in `src/pipeline/orchestration/operator.py`.
- Existing JSON summaries printed at the end of the scripts remain part of the public CLI behavior unless explicitly changed in a later specification.

## 2. Definitions

- **Terminal Log**: A human-readable line emitted to standard output or standard error while the process is running.
- **Runtime Entrypoint**: A user-invoked script that starts pipeline execution, specifically `scripts/run_pipeline.py` or `scripts/run_pipeline_daemon.py`.
- **Stage Log**: A log line that marks the start, completion, skip, or failure of a meaningful pipeline step.
- **Run Context**: The metadata associated with a pipeline execution, such as execution mode, cycle number, force flag, and status.
- **Structured Report**: A JSON artifact persisted to disk, such as validation, agent, alert, plan, or state reports.
- **PII**: Personally Identifiable Information, including raw names, phone numbers, and free-text message bodies already treated as sensitive by the repository.
- **Terminal-Only Logging**: Logging behavior that is visible during process execution but is not persisted as a standalone log file by the application.

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: The pipeline shall emit visible progress logs while the runtime is executing, rather than only printing a final JSON summary after completion.
- **REQ-002**: Terminal logs shall cover, at minimum, run start, source-change decision, Bronze load, quarantine processing, Silver build, conversation enrichment, Gold build, validation, report persistence, successful completion, skipped execution, and runtime failure handling.
- **REQ-003**: The implementation shall support both `scripts/run_pipeline.py` and `scripts/run_pipeline_daemon.py`.
- **REQ-004**: The daemon mode shall emit logs for each cycle before the final per-cycle JSON summary is printed.
- **REQ-005**: The logging behavior shall be implemented in a way that keeps the orchestration layer as the primary source of stage-level execution events.
- **REQ-006**: Each stage log shall clearly indicate whether the event represents a start, success, skip, warning, or failure condition.
- **REQ-007**: Success logs shall include compact operational context when available, including stage name and relevant row counts or status summaries.
- **REQ-008**: Skip logs shall explicitly state when execution is skipped because the source fingerprint did not change.
- **REQ-009**: Failure logs shall clearly identify the failing stage or failure category without requiring the operator to inspect persisted JSON files first.
- **REQ-010**: Terminal logs shall remain readable by a human operator running the scripts directly in a shell.
- **REQ-011**: The current final JSON output contract of the runtime scripts shall remain intact unless a future specification explicitly changes it.
- **REQ-012**: The implementation shall avoid duplicating the same stage log in both orchestration and script layers unless the duplicate adds distinct run-context information.
- **REQ-013**: The terminal logging solution shall use Python's standard `logging` module instead of ad hoc scattered `print()` calls for stage events.

- **CON-001**: The implementation shall not create standalone `.log` files.
- **CON-002**: The implementation shall not require any external logging dependency or observability service.
- **CON-003**: The implementation shall not log raw values of protected fields such as `sender_name`, `sender_phone`, `message_body`, `conversation_lead_name`, `conversation_lead_phone`, or similar PII-bearing columns.
- **CON-004**: The implementation shall not dump full dataframe contents to the terminal.
- **CON-005**: The implementation shall not alter the schema or persistence format of Bronze, Silver, Gold, monitoring, state, alert, or agent artifacts.
- **CON-006**: The logging behavior shall function in the repository-local execution environment without requiring network access.

- **GUD-001**: Prefer concise single-line logs with stable wording over verbose multi-line diagnostics during normal execution.
- **GUD-002**: Prefer log fields that help operators answer "what is running now", "what already finished", and "why did it stop".
- **GUD-003**: Keep stage naming aligned with pipeline terminology already used in the repository, including Bronze, Silver, Gold, enrichment, validation, remediation, fallback, and reporting.
- **GUD-004**: Use `INFO` for normal progress, `WARNING` for recoverable anomalies or skipped branches that may need attention, and `ERROR` for failures.
- **GUD-005**: Keep the logging initialization simple and local to the CLI runtime so the repository does not gain unnecessary configuration complexity.

- **PAT-001**: The orchestration layer should emit stage-level events, while entrypoint scripts should add outer execution context such as daemon cycle boundaries.
- **PAT-002**: Log messages should be deterministic enough to support assertions in automated tests without depending on timestamps.

## 4. Interfaces & Data Contracts

### 4.1 Runtime Coverage

The following execution surfaces shall participate in terminal logging:

| Surface | Responsibility |
| --- | --- |
| `scripts/run_pipeline.py` | Initialize terminal logging for a single execution and preserve final JSON output |
| `scripts/run_pipeline_daemon.py` | Initialize terminal logging, annotate cycle boundaries, and preserve final per-cycle JSON output |
| `src/pipeline/orchestration/operator.py` | Emit stage-level runtime logs from the central execution flow |

### 4.2 Minimum Stage Event Contract

The implementation shall emit terminal logs for at least the following logical events:

| Event ID | Event type | Minimum required meaning |
| --- | --- | --- |
| `LOG-EVT-001` | `run_started` | A pipeline execution has started |
| `LOG-EVT-002` | `source_change_evaluated` | The source fingerprint was checked and execution will run or skip |
| `LOG-EVT-003` | `bronze_loaded` | Bronze source was read successfully |
| `LOG-EVT-004` | `quarantine_completed` | Quarantine processing completed with row impact summary |
| `LOG-EVT-005` | `silver_completed` | Silver artifacts were built and sanitized |
| `LOG-EVT-006` | `enrichment_completed` | Conversation enrichment stage completed |
| `LOG-EVT-007` | `gold_completed` | Gold artifact was built and sanitized |
| `LOG-EVT-008` | `validation_completed` | Validation finished with status summary |
| `LOG-EVT-009` | `reports_persisted` | Validation, agent, and alert reports were written |
| `LOG-EVT-010` | `run_skipped` | Execution was skipped because no source change was detected |
| `LOG-EVT-011` | `run_succeeded` | The run completed successfully |
| `LOG-EVT-012` | `run_failed` | The run failed or moved to fallback |

### 4.3 Log Content Contract

Each log line should be understandable without opening source code. At minimum, stage logs shall carry:

- stage or event name
- execution outcome
- compact context relevant to the event

Recommended contextual fields:

- `status`
- `cycle`
- `force`
- `row_counts`
- `quarantined_rows`
- `failed_check_count`
- `incident_id`

Example informational log lines:

```text
INFO pipeline.runtime run_started force=true
INFO pipeline.runtime source_change_evaluated changed=true
INFO pipeline.runtime quarantine_completed quarantined_rows=3 clean_rows=124
INFO pipeline.runtime validation_completed status=passed failed_check_count=0
INFO pipeline.runtime run_succeeded status=success gold_rows=58
```

Example skip and failure log lines:

```text
WARNING pipeline.runtime run_skipped reason=source_fingerprint_unchanged
ERROR pipeline.runtime run_failed stage=gold_build error=RuntimeError
WARNING pipeline.runtime fallback_applied incident_id=incident_20260422T120000
```

### 4.4 Output Channel Contract

- Normal progress logs may be emitted to standard output.
- Failure-oriented logs may be emitted to standard error if the chosen logging configuration supports that split.
- Final JSON summaries currently printed by the scripts remain separate from progress logs and must remain parseable by a human operator reading the terminal output stream.

## 5. Acceptance Criteria

- **AC-001**: Given `venv/bin/python scripts/run_pipeline.py --force`, When the pipeline runs, Then the terminal shall show progress logs before the final JSON artifact summary is printed.
- **AC-002**: Given `venv/bin/python scripts/run_pipeline_daemon.py --max-cycles 1`, When one cycle runs, Then the terminal shall show cycle-aware progress logs and then the final per-cycle JSON summary.
- **AC-003**: Given an unchanged source fingerprint and no force flag, When the runtime is executed, Then the terminal shall explicitly show that execution was skipped because the source did not change.
- **AC-004**: Given a successful execution, When validation completes, Then the terminal shall show a validation status log before process completion.
- **AC-005**: Given a runtime exception during execution, When the failure path is reached, Then the terminal shall emit a failure-oriented log that identifies the failure category or stage.
- **AC-006**: Given terminal logging is enabled, When the pipeline processes sensitive data in memory, Then terminal output shall not expose raw PII-bearing field values.
- **AC-007**: Given the implementation is complete, When the scripts finish, Then the existing final JSON summaries shall still be printed.

## 6. Test Automation Strategy

- **Test Levels**: Unit and integration-style CLI behavior tests
- **Frameworks**: `pytest` executed through `venv/bin/python -m pytest`
- **Test Data Management**: Reuse synthetic parquet inputs already created in temporary repository roots
- **CI/CD Integration**: No CI-specific behavior is required beyond repository-local test execution
- **Coverage Requirements**: Cover single-run success, skip path, and failure path log emission
- **Performance Testing**: Not required for this change

Required automated checks:

- A test shall verify that single-run execution emits progress logs during a successful run.
- A test shall verify that skip behavior emits a terminal log indicating unchanged source fingerprint.
- A test shall verify that daemon execution emits cycle-scoped log output.
- A test shall verify that failure handling emits a failure-oriented terminal log.
- Existing tests for artifact generation and runtime behavior shall remain valid after logging is added.

## 7. Rationale & Context

The repository already persists rich structured reports after execution, but that is not the same as operational visibility during execution.

At present, the user primarily sees a final JSON object from the runtime scripts after the work is already complete. This creates an operational blind spot:

- there is no immediate indication that the process has started correctly
- there is no visibility into which stage is currently running
- a long-running enrichment or validation step appears indistinguishable from a stalled process
- failure context is available mainly after inspecting persisted reports

The architecture is already favorable for terminal logging because the runtime flow is centralized in a single orchestration function. This makes it possible to add meaningful stage logs with low implementation risk and without scattering control logic across many modules.

Terminal-only logging is intentionally narrow in scope. It improves operator experience during execution while preserving the existing monitoring artifacts as the authoritative persisted audit trail.

## 8. Dependencies & External Integrations

### External Systems

- **EXT-001**: Local terminal session - Required to display runtime logs to the operator

### Third-Party Services

- **SVC-001**: None required

### Infrastructure Dependencies

- **INF-001**: Repository-local virtual environment at `venv/` - Required execution environment

### Data Dependencies

- **DAT-001**: `docs/conversations_bronze.parquet` or equivalent test parquet input - Required to exercise the runtime path

### Technology Platform Dependencies

- **PLT-001**: Python standard library `logging` module - Required logging mechanism

### Compliance Dependencies

- **COM-001**: Existing repository privacy and publication constraints - Terminal logging must not leak protected raw fields

## 9. Examples & Edge Cases

Example sequence for a successful single-run execution:

```text
INFO pipeline.runtime run_started force=true
INFO pipeline.runtime source_change_evaluated changed=true
INFO pipeline.runtime bronze_loaded rows=124
INFO pipeline.runtime quarantine_completed quarantined_rows=2 clean_rows=122
INFO pipeline.runtime silver_completed silver_rows=40 silver_message_rows=122
INFO pipeline.runtime enrichment_completed conversation_rows=40 status=disabled
INFO pipeline.runtime gold_completed gold_rows=40
INFO pipeline.runtime validation_completed status=passed failed_check_count=0
INFO pipeline.runtime reports_persisted
INFO pipeline.runtime run_succeeded status=success
{
  "bronze_path": "...",
  "silver_path": "...",
  "status": "success"
}
```

Edge cases:

- Source unchanged: the runtime must log skip intent before returning the final skipped JSON summary.
- Quarantine applied: the runtime should log the number of quarantined rows without printing any quarantined record contents.
- Validation failed with auto-remediation resolved: the runtime should log validation failure detection, remediation attempt, and final successful resolution.
- Runtime exception with fallback: the runtime should log the failure and whether fallback to last successful artifacts was applied.
- Daemon cycle loop: each cycle should be visually distinguishable so operators can separate one run from the next.

## 10. Validation Criteria

- Terminal output includes stage progress during execution, not only final JSON.
- Output remains understandable in both single-run and daemon modes.
- No raw PII appears in log lines under normal success, skip, remediation, or failure flows.
- Existing persisted reports and artifact outputs remain generated as before.
- Automated tests executed from `venv/bin/python -m pytest -q` remain green after the change.

## 11. Related Specifications / Further Reading

- `README.md`
- `spec/spec-architecture-pipeline-spec-runtime-parity.md`
- `src/pipeline/orchestration/operator.py`
- `scripts/run_pipeline.py`
- `scripts/run_pipeline_daemon.py`
