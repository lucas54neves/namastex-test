---
title: GitHub Actions Workflow for Databricks Deployment and Pipeline Execution
version: 1.0
date_created: 2026-04-23
last_updated: 2026-04-23
owner: Lucas Neves
tags: [infrastructure, github-actions, databricks, deployment, orchestration]
---

# Introduction

This specification defines the required behavior, interfaces, constraints, and validation criteria for implementing a GitHub Actions workflow that deploys this repository to Databricks, provisions the required Databricks resources, uploads the Bronze input dataset, and executes the pipeline in Databricks without manual operational steps in the Databricks workspace beyond obtaining credentials for GitHub secrets.

## 1. Purpose & Scope

The purpose of this specification is to define an implementation-ready contract for automating the lifecycle of this project in Databricks through GitHub Actions.

Scope:

- Deploy repository code from GitHub to Databricks workspace storage.
- Provision Databricks resources required for execution.
- Upload the pipeline input file to Unity Catalog Volumes.
- Execute the project in Databricks through a managed Databricks Job.
- Persist pipeline outputs in Databricks-managed storage.
- Adapt the Python project so it can run against configurable storage roots rather than only local repository-relative paths.

Out of scope:

- Manual notebook execution.
- Manual job creation in Databricks.
- Terraform-based infrastructure provisioning.
- Redesign of the pipeline business logic.
- Packaging the project as a wheel unless required later by a separate specification.

Intended audience:

- Maintainers of this repository.
- Engineers implementing CI/CD automation.
- Generative AI agents producing the workflow and the required code changes.

Assumptions:

- A Databricks workspace already exists.
- The GitHub repository can store secrets and variables.
- The Databricks principal used by GitHub Actions has permissions to create or update Unity Catalog objects and Jobs.

## 2. Definitions

- **GitHub Actions**: GitHub-hosted CI/CD automation service used to run workflows.
- **Databricks Job**: Managed Databricks execution resource that runs a defined task on a cluster or compute policy.
- **Unity Catalog (UC)**: Databricks governance layer for catalogs, schemas, tables, and volumes.
- **Volume**: Unity Catalog storage location used for files.
- **Workspace Files**: Databricks workspace file storage used to host source files that can be executed by jobs.
- **Bronze input**: Source file `docs/conversations_bronze.parquet`.
- **Pipeline root paths**: Runtime-resolved filesystem locations for input, data outputs, reports, state, runtime candidates, and config.
- **Provisioning**: Creating or reconciling Databricks resources so they match the declared repository configuration.
- **Reconciliation**: Safe create-or-update behavior applied by the workflow for Databricks resources.
- **Manual dispatch**: GitHub Actions `workflow_dispatch` trigger used to run the workflow on demand.

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: The repository shall define a GitHub Actions workflow triggered by pushes to the main branch and by `workflow_dispatch`.
- **REQ-002**: The workflow shall authenticate to Databricks using credentials stored in GitHub Secrets only.
- **REQ-003**: The workflow shall provision or reconcile the target Databricks catalog, schema, volume set, workspace deployment path, and job definition.
- **REQ-004**: The workflow shall upload the repository code to a deterministic workspace path in Databricks.
- **REQ-005**: The workflow shall upload `docs/conversations_bronze.parquet` to a deterministic Unity Catalog Volume path before job execution.
- **REQ-006**: The workflow shall create or update a Databricks Job that runs the project code from the deployed workspace path.
- **REQ-007**: The workflow shall trigger the Databricks Job after deployment and shall fail if the job run fails.
- **REQ-008**: The Python project shall support configurable runtime paths for input, output, reports, state, runtime candidates, and optional config roots.
- **REQ-009**: The project shall preserve the current local-development behavior when Databricks-specific path overrides are not provided.
- **REQ-010**: The Databricks execution path shall persist outputs in Unity Catalog Volumes rather than only in ephemeral local disk.
- **REQ-011**: The workflow shall expose deterministic job parameters or environment variables so the Databricks runtime knows the correct workspace and volume paths.
- **REQ-012**: The workflow shall support idempotent re-execution without requiring manual cleanup in Databricks.
- **REQ-013**: The workflow shall produce enough logs and status output to diagnose authentication failures, provisioning failures, upload failures, and job execution failures.
- **REQ-014**: The repository documentation shall be updated in the same work cycle to explain GitHub Actions deployment, required secrets, Databricks prerequisites, and runtime behavior.
- **REQ-015**: The implementation shall include automated tests covering the new runtime path resolution logic and any Databricks-specific configuration assembly that can be tested locally.

- **SEC-001**: Databricks secrets, tokens, client secrets, and host values that are sensitive shall not be committed to the repository.
- **SEC-002**: The workflow shall avoid printing secret values in logs.
- **SEC-003**: The workflow shall use least-privilege credentials consistent with creating catalogs, schemas, volumes, workspace files, and jobs in the target workspace.
- **SEC-004**: The workflow shall treat all persisted pipeline outputs as potentially sensitive and shall not publish them to public GitHub artifacts by default.

- **CON-001**: No manual setup in Databricks is allowed as part of the operational runbook except retrieving credentials and placing them in GitHub Secrets.
- **CON-002**: Existing pipeline transformation semantics shall remain unchanged unless required to support configurable paths.
- **CON-003**: The current repository structure shall remain the canonical source of project code.
- **CON-004**: The solution shall prefer ASCII-only edits unless a file already uses non-ASCII content.
- **CON-005**: Specification files shall be written under `spec/` and shall not be committed unless explicitly requested.

- **GUD-001**: Prefer deterministic resource names and paths derived from repository-level configuration.
- **GUD-002**: Prefer declarative reconciliation over imperative one-off mutations.
- **GUD-003**: Separate deploy concerns from runtime concerns so path resolution and Databricks resource configuration are testable without Databricks access.
- **GUD-004**: Keep the initial Databricks integration script-based instead of introducing wheel packaging unless necessary.

- **PAT-001**: Use a single workflow as the operator entry point for deploy-and-run behavior.
- **PAT-002**: Use distinct Unity Catalog Volumes for at least input and persistent outputs, even if they share the same catalog and schema.
- **PAT-003**: Use repository-level configuration or environment variables to define Databricks object names and workspace paths.

## 4. Interfaces & Data Contracts

### 4.1 GitHub Secrets Contract

The workflow shall require a secret contract equivalent to the following:

| Name | Required | Description |
| --- | --- | --- |
| `DATABRICKS_HOST` | Yes | Base HTTPS URL of the target Databricks workspace |
| `DATABRICKS_TOKEN` or service principal credentials | Yes | Authentication material for Databricks CLI/API |
| `DATABRICKS_CLIENT_ID` | Conditional | Required if service principal OAuth is used |
| `DATABRICKS_CLIENT_SECRET` | Conditional | Required if service principal OAuth is used |

The implementation may support token auth first and service principal auth as an extension, but the workflow contract must make the chosen mode explicit.

### 4.2 GitHub Variables Contract

The workflow shall read non-secret deployment configuration from repository variables or workflow-level environment values.

| Name | Required | Description |
| --- | --- | --- |
| `DBX_CATALOG` | Yes | Unity Catalog catalog name to provision |
| `DBX_SCHEMA` | Yes | Unity Catalog schema name to provision |
| `DBX_VOLUME_INPUT` | Yes | Volume name for source file upload |
| `DBX_VOLUME_OUTPUT` | Yes | Volume name for pipeline outputs |
| `DBX_WORKSPACE_ROOT` | Yes | Workspace Files root directory for deployed code |
| `DBX_JOB_NAME` | Yes | Databricks Job display name |
| `DBX_COMPUTE_MODE` | Yes | Compute strategy identifier for job creation |

### 4.3 Runtime Path Resolution Contract

The Python code shall support path resolution through explicit environment variables. Local defaults shall remain unchanged when overrides are absent.

| Environment Variable | Purpose | Example |
| --- | --- | --- |
| `PIPELINE_ROOT_DIR` | Optional override for project root semantics | `/Workspace/Users/.../namastex-test` |
| `PIPELINE_RAW_BRONZE_SOURCE_PATH` | Full path to input parquet in Databricks | `/Volumes/main/ops/bronze-input/conversations_bronze.parquet` |
| `PIPELINE_DATA_DIR` | Base directory for `bronze/`, `silver/`, `gold/`, `quarantine/` | `/Volumes/main/ops/pipeline-output/data` |
| `PIPELINE_REPORTS_DIR` | Base directory for reports | `/Volumes/main/ops/pipeline-output/reports` |
| `PIPELINE_STATE_DIR` | Base directory for state | `/Volumes/main/ops/pipeline-output/state` |
| `PIPELINE_RUNTIME_DIR` | Base directory for runtime candidates and runtime metadata | `/Volumes/main/ops/pipeline-output/runtime` |
| `PIPELINE_CONFIG_DIR` | Optional config directory override | `/Workspace/Users/.../namastex-test/config` |

Resolution rules:

- If no overrides are provided, current repository-relative behavior shall remain unchanged.
- If directory overrides are provided, all derived internal paths shall be built from those overrides.
- The raw Bronze source path shall support a fully qualified file path independent of the docs directory.
- Directory creation logic shall create overridden output directories when they do not exist.

### 4.4 Databricks Job Contract

The Databricks Job shall satisfy the following runtime contract:

| Field | Requirement |
| --- | --- |
| Task type | Python script or equivalent executable from Workspace Files |
| Source location | Deployed repository path in Workspace Files |
| Input source | Unity Catalog Volume file uploaded by workflow |
| Output location | Unity Catalog Volume paths passed by environment variables or task parameters |
| Trigger style | Invoked by GitHub Actions after reconciliation |
| Failure semantics | Non-zero run result must fail the GitHub Actions workflow |

### 4.5 Provisioning Contract

The provisioning logic shall reconcile the following resources:

| Resource | Expected behavior |
| --- | --- |
| Catalog | Create if absent; reuse if present |
| Schema | Create if absent; reuse if present |
| Input volume | Create if absent; reuse if present |
| Output volume | Create if absent; reuse if present |
| Workspace root directory | Create or synchronize deployed files |
| Databricks Job | Create if absent; update if present |

### 4.6 Example Runtime Mapping

```text
Workspace Files:
  /Workspace/Shared/namastex-test/
    scripts/run_pipeline.py
    src/pipeline/...
    config/...

Volumes:
  /Volumes/main/ops/bronze-input/conversations_bronze.parquet
  /Volumes/main/ops/pipeline-output/data/bronze/conversations.parquet
  /Volumes/main/ops/pipeline-output/data/silver/silver_leads.parquet
  /Volumes/main/ops/pipeline-output/reports/monitoring/latest_run_report.json
  /Volumes/main/ops/pipeline-output/state/pipeline_state.json
  /Volumes/main/ops/pipeline-output/runtime/candidates/
```

## 5. Acceptance Criteria

- **AC-001**: Given a push to `main`, when the workflow starts, then it authenticates to Databricks using GitHub-managed secrets without requiring manual input.
- **AC-002**: Given a workspace with no target catalog, schema, volumes, or job, when the workflow runs, then it provisions all required resources successfully.
- **AC-003**: Given the resources already exist, when the workflow runs again, then it reconciles them without failing due to duplicate creation attempts.
- **AC-004**: Given the workflow deploys the repository, when the Databricks job is triggered, then it runs the pipeline code from the deployed workspace path.
- **AC-005**: Given `docs/conversations_bronze.parquet` exists in the repository, when the workflow uploads inputs, then the Databricks run reads the uploaded file from the configured input volume path.
- **AC-006**: Given the Databricks job succeeds, when the workflow finishes, then Bronze, Silver, Gold, reports, state, and runtime artifacts exist under the configured output volume root.
- **AC-007**: Given no Databricks path overrides are defined locally, when a developer runs `venv/bin/python scripts/run_pipeline.py --force`, then the current local behavior remains unchanged.
- **AC-008**: Given Databricks path overrides are provided, when the pipeline runs, then all path-dependent artifacts are written to the configured target directories.
- **AC-009**: Given a provisioning or runtime failure in Databricks, when the workflow completes, then the GitHub Actions job exits with failure and logs indicate the failed stage.
- **AC-010**: Given repository documentation is reviewed after implementation, when a maintainer follows it, then they can configure secrets and understand how the deploy-and-run workflow behaves.

## 6. Test Automation Strategy

- **Test Levels**: Unit and repository-level integration tests executed locally in the project virtual environment; workflow validation tests through static checks and, where practical, dry-run validation of generated configuration.
- **Frameworks**: `pytest` executed with `venv/bin/python -m pytest`.
- **Test Data Management**: Use the existing repository fixtures and small deterministic path overrides; do not require live Databricks access for unit tests.
- **CI/CD Integration**: The repository test workflow shall validate path resolution logic and Databricks configuration assembly before any deploy-and-run job is used in production.
- **Coverage Requirements**: New logic for path resolution, Databricks environment construction, and workflow configuration generation shall have direct automated coverage.
- **Performance Testing**: Not required for the initial implementation; operational validation is limited to successful deploy-and-run execution.

Required automated checks:

- Unit tests for path override precedence and defaults.
- Unit tests for derived `PipelinePaths` behavior with custom roots.
- Tests for any helper that builds Databricks environment variables or job payloads.
- Basic validation that the workflow file exists and references the required secrets and triggers.

Repository-standard execution commands:

```bash
venv/bin/python -m pytest -q
venv/bin/python -m pytest tests/test_jobs.py -q
```

Additional tests may be added for the new path/config modules as needed.

## 7. Rationale & Context

The current project is designed around a local repository root and writes all artifacts into local directories such as `data/`, `reports/`, `state/`, and `runtime/`. That design is suitable for local execution but is not sufficient for a Databricks runtime where source files and persistent outputs live in different storage systems.

The selected design keeps the existing pipeline behavior intact while externalizing storage locations through runtime configuration. This minimizes risk because the transformation logic stays unchanged and only the execution context is generalized.

Using GitHub Actions as the single orchestration surface aligns with the requirement that no manual Databricks operation should be necessary beyond credential retrieval. Provisioning catalog, schema, volumes, workspace files, and jobs in the workflow removes hidden operational knowledge from the process.

Using Workspace Files plus a Python script execution model is the least disruptive integration path for the current repository. It avoids premature packaging work and keeps the deployed runtime close to what already works locally.

## 8. Dependencies & External Integrations

### External Systems

- **EXT-001**: GitHub Actions - executes CI/CD automation and stores repository secrets and variables.
- **EXT-002**: Databricks Workspace - hosts the deployed source files and executes the managed job.
- **EXT-003**: Unity Catalog - governs storage objects used for file-based input and output persistence.

### Third-Party Services

- **SVC-001**: Databricks control plane APIs or CLI - required for authentication, resource reconciliation, workspace file synchronization, and job execution.

### Infrastructure Dependencies

- **INF-001**: A Databricks workspace with Unity Catalog enabled.
- **INF-002**: Compute capacity in Databricks suitable for running a Python script with the repository dependencies.
- **INF-003**: GitHub repository permissions to run workflows on pushes to `main` and via manual dispatch.

### Data Dependencies

- **DAT-001**: `docs/conversations_bronze.parquet` - canonical Bronze input file stored in the repository and uploaded during deploy.
- **DAT-002**: Output directories persisted in Unity Catalog Volumes for `data`, `reports`, `state`, and `runtime`.

### Technology Platform Dependencies

- **PLT-001**: Python 3.11 compatible Databricks execution environment.
- **PLT-002**: A Databricks-supported execution path for repository-hosted Python scripts.
- **PLT-003**: Repository virtual environment for local test execution.

### Compliance Dependencies

- **COM-001**: Repository rule requiring documentation updates for relevant operational changes.
- **COM-002**: Repository rule requiring specification files to live in `spec/` and remain uncommitted unless explicitly requested.

## 9. Examples & Edge Cases

Example GitHub Actions behavior:

```yaml
on:
  push:
    branches: [main]
  workflow_dispatch:

jobs:
  deploy-and-run:
    steps:
      - checkout repository
      - authenticate to Databricks
      - reconcile catalog/schema/volumes
      - sync repository to Workspace Files
      - upload docs/conversations_bronze.parquet to input volume
      - create or update Databricks Job
      - run Databricks Job and wait for completion
```

Edge cases:

- The workflow runs twice with no repository changes: resource reconciliation must remain safe and non-destructive.
- The input volume exists but the Bronze input file is missing: the workflow must upload the repository file before triggering the job.
- The output volume exists with older artifacts: the job may overwrite current outputs but must not require manual deletion to run successfully.
- Local execution without Databricks variables must continue to use repository-relative paths.
- A Databricks job may fail after successful provisioning: the workflow must report execution failure distinctly from provisioning failure.
- The Databricks workspace may not permit catalog creation for the provided principal: the workflow must fail clearly during provisioning.

## 10. Validation Criteria

- The repository contains a GitHub Actions workflow file implementing the required triggers and stages.
- The repository contains code changes enabling runtime path overrides without breaking local execution.
- The workflow definition references only GitHub Secrets and Variables for Databricks configuration.
- Automated tests pass with the repository virtual environment commands.
- Documentation describes required secrets, variables, deploy behavior, and the Databricks execution model.
- A manual review of the workflow confirms that no Databricks notebook or job setup is required outside the workflow.

## 11. Related Specifications / Further Reading

- [README.md](/home/lucas/projects/lucas54neves/namastex-test/README.md)
- [docs/technical-test-data-ai-engineering.md](/home/lucas/projects/lucas54neves/namastex-test/docs/technical-test-data-ai-engineering.md)
- [spec/spec-architecture-project-folder-organization.md](/home/lucas/projects/lucas54neves/namastex-test/spec/spec-architecture-project-folder-organization.md)
- [spec/spec-architecture-pipeline-spec-runtime-parity.md](/home/lucas/projects/lucas54neves/namastex-test/spec/spec-architecture-pipeline-spec-runtime-parity.md)
