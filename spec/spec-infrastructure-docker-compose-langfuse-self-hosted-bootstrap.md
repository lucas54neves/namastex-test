---
title: Infrastructure Specification for Docker Compose Langfuse Self-Hosted Bootstrap
version: 1.0
date_created: 2026-04-22
last_updated: 2026-04-22
owner: Lucas Neves
tags: [infrastructure, docker-compose, langfuse, self-hosted, bootstrap, prompts, observability]
---

# Introduction

This specification defines the infrastructure and runtime contracts for delivering a fully self-contained `docker compose` experience that starts the conversation-enrichment pipeline together with a self-hosted Langfuse stack and automatic prompt bootstrap.

The goal is to let another user run one command and obtain a working local environment in which the pipeline can emit Langfuse traces, resolve a Langfuse-managed prompt, and continue to degrade safely to the local prompt when Langfuse bootstrap is incomplete or unavailable.

## 1. Purpose & Scope

This specification defines the requirements, constraints, service boundaries, configuration contracts, and acceptance criteria for option 2:

- one `docker compose` stack for the pipeline runtime
- one self-hosted Langfuse deployment within the same stack
- automatic headless initialization of Langfuse organization, user, project, and API keys
- automatic bootstrap of the runtime prompt in Langfuse Prompt Management
- a user experience in which a fresh machine can run `docker compose up` without manual Langfuse UI setup

Scope:

- Define the Compose-level services required for a low-scale self-hosted Langfuse stack.
- Define how the pipeline service receives Langfuse credentials and base URL from the same stack.
- Define the prompt-bootstrap service responsible for creating or updating the runtime prompt and promoting the intended label.
- Define service startup ordering, readiness contracts, and failure-handling behavior.
- Define what must happen when bootstrap succeeds, is skipped, or fails.
- Preserve the current privacy boundary, deterministic fallback behavior, and optional LLM provider behavior already implemented in the repository.
- Define the documentation and operational artifacts that must accompany this infrastructure path.

Out of scope:

- High-availability or horizontally scaled Langfuse production architecture.
- Kubernetes, Terraform, or cloud-native deployment targets.
- Replacing the current repository runtime with an API server or web application.
- Eliminating the local prompt fallback from the pipeline runtime.
- Building a generalized secrets-management platform beyond Docker Compose environment injection.
- Publishing container images to an external registry unless explicitly requested later.

Intended audience:

- Engineers implementing the Compose stack and bootstrap automation
- Reviewers validating reproducibility, privacy, and operational simplicity
- Future maintainers extending the local self-hosted observability environment

Assumptions:

- The repository already contains runtime support for optional Langfuse tracing and prompt retrieval.
- The repository runtime can fall back to the local prompt when Langfuse prompt resolution fails and fallback is allowed.
- The repository is executed locally via the project virtual environment today, but this specification targets a containerized execution path for end users.
- The Langfuse self-hosted architecture is low-scale and intended for local development, demos, and technical validation rather than HA production use.

## 2. Definitions

- **Compose Stack**: The full set of containers started by one `docker compose up` invocation for this repository.
- **Pipeline Service**: The container that runs the repository pipeline entrypoint.
- **Langfuse Web**: The main Langfuse application container serving UI and API endpoints.
- **Langfuse Worker**: The Langfuse background-processing container that handles queued events and asynchronous processing.
- **Bootstrap Service**: A one-shot container that waits for Langfuse readiness and then creates or updates the runtime prompt and label assignment.
- **Headless Initialization**: Langfuse self-hosted capability that creates organization, project, user, and API keys on startup from environment variables.
- **Prompt Bootstrap**: The process of ensuring that the required prompt exists in Langfuse Prompt Management with the intended name, content, and label.
- **Fresh Startup**: A stack startup on a machine or directory where the Langfuse data volumes do not yet contain initialized state.
- **Warm Startup**: A stack startup in which Langfuse state already exists in persistent volumes.
- **Ready State**: A state in which a service is healthy enough for dependent services to proceed.
- **Low-Scale Local Deployment**: A non-HA self-hosted deployment suitable for local validation and development.
- **Idempotent Bootstrap**: Bootstrap behavior that can run multiple times without creating conflicting or duplicate logical state.
- **Local Prompt Fallback**: Existing repository behavior in which the runtime uses the prompt built in code when Langfuse prompt resolution is unavailable or disallowed.

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: The repository shall support an option-2 local deployment mode in which `docker compose up` starts the pipeline and a self-hosted Langfuse stack together.
- **REQ-002**: The Compose stack shall include the Langfuse application components and their required stateful dependencies for low-scale self-hosted operation.
- **REQ-003**: The Compose stack shall include a pipeline service configured to talk to the Langfuse service over the internal Compose network.
- **REQ-004**: The Compose stack shall include a one-shot prompt-bootstrap service that creates or updates the required prompt in Langfuse Prompt Management.
- **REQ-005**: The Langfuse self-hosted deployment shall use headless initialization so that organization, project, user, and API keys can be created without manual UI setup.
- **REQ-006**: The pipeline service shall receive `LANGFUSE_PUBLIC_KEY`, `LANGFUSE_SECRET_KEY`, `LANGFUSE_BASE_URL`, `PIPELINE_LANGFUSE_PROMPT_NAME`, `PIPELINE_LANGFUSE_PROMPT_LABEL`, and `PIPELINE_ENABLE_LANGFUSE` from Compose configuration.
- **REQ-007**: The pipeline service shall be able to run successfully on a fresh stack startup without requiring the user to create Langfuse resources manually in the browser.
- **REQ-008**: The prompt-bootstrap service shall be idempotent and safe to rerun on warm startups.
- **REQ-009**: The prompt-bootstrap service shall ensure that the runtime prompt name and label used by the pipeline exist in Langfuse after a successful bootstrap.
- **REQ-010**: The prompt-bootstrap service shall use the same prompt contract already expected by the runtime, including `allowed_values_json` and `payload_json` variables.
- **REQ-011**: The Compose stack shall define health or readiness conditions such that the bootstrap service does not run before Langfuse is ready to accept API requests.
- **REQ-012**: The pipeline service shall not crash solely because the prompt-bootstrap service failed, provided local prompt fallback is enabled.
- **REQ-013**: The Compose stack shall persist Langfuse state in Docker volumes so that prompts, traces, and initialized resources survive container restarts by default.
- **REQ-014**: The implementation shall document one minimal startup command and one minimal shutdown command for end users.
- **REQ-015**: The implementation shall document which ports are exposed to the host and which services remain internal to the Docker network.
- **REQ-016**: The implementation shall define a single source of truth for Langfuse initialization values used by both self-hosted Langfuse and the pipeline container.
- **REQ-017**: The implementation shall update repository documentation in the same work cycle to explain the Compose option, startup sequence, and operational caveats.
- **REQ-018**: The implementation shall include automated repository-local checks that validate the presence and integrity of the Compose and bootstrap assets.
- **REQ-019**: The implementation shall preserve the baseline non-container path of the repository and shall not make Docker Compose mandatory for all users.
- **REQ-020**: The implementation shall preserve the current deterministic pipeline path when `PIPELINE_ENABLE_LLM_ENRICHMENT=0`, even inside the Compose deployment.
- **REQ-021**: The implementation shall make it possible to use tracing-only mode or prompt-management mode from the same Compose assets through configuration.
- **REQ-022**: The implementation shall keep the prompt-bootstrap logic repository-local and version-controlled rather than relying on manual out-of-band instructions only.
- **REQ-023**: The implementation shall support a user journey in which the pipeline can begin with local prompt fallback and later switch automatically to the bootstrapped Langfuse prompt once available.
- **REQ-024**: The implementation shall expose clear failure signals in container logs when Langfuse initialization or prompt bootstrap fails.
- **REQ-025**: The implementation shall not require the end user to edit the Langfuse upstream repository manually to start the stack.

- **SEC-001**: Secrets shall not be hardcoded into committed Compose files, bootstrap scripts, or repository documentation examples beyond placeholder values.
- **SEC-002**: The Compose solution shall support secret injection via `.env`, Compose environment variables, or equivalent local-only mechanisms.
- **SEC-003**: The prompt-bootstrap service shall not log raw secrets, provider keys, or raw unmasked payload content.
- **SEC-004**: The self-hosted Langfuse stack shall remain inside the privacy boundary already approved for provider-safe prompt payloads; it shall not cause the runtime to emit broader raw conversation data than the pipeline already sends to providers intentionally.
- **SEC-005**: Exposed host ports shall be limited to the minimum operational set needed for end-user interaction and debugging.
- **SEC-006**: Internal storage credentials used by the self-hosted Langfuse stack shall be configurable and replaceable through environment injection.

- **CON-001**: The Compose-based self-hosted Langfuse option is a low-scale local deployment and shall not be presented as a production-grade HA architecture.
- **CON-002**: The repository shall not vendor-copy the entire upstream Langfuse repository into the project merely to satisfy this deployment option.
- **CON-003**: The implementation shall not require a human to open the Langfuse UI to create the initial project or prompt for the happy path.
- **CON-004**: The Compose stack shall not assume internet access after images and dependencies are available, except where external LLM providers are intentionally used.
- **CON-005**: The pipeline service shall not become blocked on Langfuse bootstrap success when local prompt fallback is explicitly enabled.
- **CON-006**: The bootstrap service shall not mutate runtime code or generated repository files at container startup.
- **CON-007**: The implementation shall not store prompt snapshots or trace exports in version-controlled repository paths unless explicitly requested later.
- **CON-008**: The Compose assets shall remain understandable and maintainable by repository contributors without hidden orchestration layers.
- **CON-009**: This specification shall not require Compose-only features unavailable in current Docker Compose v2 environments unless explicitly documented as prerequisites.
- **CON-010**: The solution shall not weaken current testability by introducing required live network calls into baseline repository tests.

- **GUD-001**: Prefer a repository-owned wrapper Compose file that references official Langfuse images or upstream-supported service topology rather than reimplementing Langfuse architecture ad hoc.
- **GUD-002**: Prefer separating responsibilities into `langfuse-infra`, `prompt-bootstrap`, and `pipeline` services for clarity.
- **GUD-003**: Prefer explicit readiness checks over naive `sleep`-only startup sequencing.
- **GUD-004**: Prefer idempotent prompt upsert logic over create-only logic so repeated local startups remain safe.
- **GUD-005**: Prefer one `.env` contract for local operators with clearly grouped variables for pipeline, Langfuse, providers, and bootstrap behavior.
- **GUD-006**: Prefer keeping bootstrap logic in a small script or one-shot utility container that is easy to inspect and test.
- **GUD-007**: Prefer internal Docker DNS names such as `http://langfuse-web:3000` for container-to-container communication and reserve host port access for human operators.
- **GUD-008**: Prefer preserving `PIPELINE_LANGFUSE_ALLOW_LOCAL_PROMPT_FALLBACK=1` in the first rollout so the local user experience is resilient.
- **GUD-009**: Prefer persistent named volumes over bind mounts for Langfuse databases in the default quickstart.
- **GUD-010**: Prefer documenting when users should run `docker compose down` versus `docker compose down -v`.

- **PAT-001**: Recommended startup pattern: infra services start -> Langfuse self-initializes org/project/user/keys -> bootstrap service waits for readiness -> prompt is created or updated -> pipeline runs with Langfuse enabled.
- **PAT-002**: Recommended resilience pattern: if bootstrap fails, pipeline remains able to run with local prompt fallback while surfacing bootstrap failure in logs.
- **PAT-003**: Recommended state pattern: Langfuse databases and blob storage persist in named Docker volumes across warm restarts.
- **PAT-004**: Recommended ownership pattern: repository owns the wrapper Compose file, bootstrap script, and user-facing docs; upstream Langfuse internals remain external dependencies.

## 4. Interfaces & Data Contracts

### 4.1 Compose Service Contract

The minimal recommended Compose stack shall contain the following logical services:

| Service | Responsibility | Host Exposure |
| --- | --- | --- |
| `pipeline` | Runs repository pipeline command with Langfuse enabled | Optional, typically none |
| `langfuse-web` | Serves Langfuse UI and API | Yes, default user-facing HTTP port |
| `langfuse-worker` | Processes Langfuse background jobs | No |
| `postgres` | Transactional datastore for Langfuse | No |
| `clickhouse` | OLAP datastore for traces and observations | No |
| `redis` or `valkey` | Queue/cache backend for Langfuse | No |
| `minio` or supported blob store | Object/blob storage for Langfuse artifacts | Optional, only if needed for debugging |
| `langfuse-bootstrap` | One-shot prompt bootstrap job | No |

The exact internal service names may differ, but the responsibilities shall remain explicit and documented.

### 4.2 Environment Contract

The Compose option shall define variables for four groups:

| Group | Purpose |
| --- | --- |
| Pipeline runtime | Existing pipeline execution toggles and provider keys |
| Langfuse self-hosting | Internal Langfuse infrastructure configuration |
| Langfuse headless initialization | Org/project/user/API key creation |
| Prompt bootstrap | Prompt name, label, template source, and bootstrap behavior |

Required runtime-facing variables:

| Variable | Required When | Purpose |
| --- | --- | --- |
| `PIPELINE_ENABLE_LANGFUSE` | Langfuse tracing or prompt management desired | Master toggle for repository runtime |
| `LANGFUSE_PUBLIC_KEY` | Langfuse enabled | Runtime authentication to Langfuse |
| `LANGFUSE_SECRET_KEY` | Langfuse enabled | Runtime authentication to Langfuse |
| `LANGFUSE_BASE_URL` | Langfuse enabled | Base URL for self-hosted Langfuse API |
| `PIPELINE_LANGFUSE_PROMPT_NAME` | Prompt management desired | Prompt identifier |
| `PIPELINE_LANGFUSE_PROMPT_LABEL` | Prompt management desired | Label selector such as `production` |
| `PIPELINE_LANGFUSE_TRACE_NAME` | Optional | Runtime trace name override |
| `PIPELINE_LANGFUSE_ALLOW_LOCAL_PROMPT_FALLBACK` | Recommended | Enables resilient local prompt fallback |

Required headless-initialization variables:

| Variable | Purpose |
| --- | --- |
| `LANGFUSE_INIT_ORG_ID` | Deterministic organization identifier |
| `LANGFUSE_INIT_ORG_NAME` | Human-readable organization name |
| `LANGFUSE_INIT_PROJECT_ID` | Deterministic project identifier |
| `LANGFUSE_INIT_PROJECT_NAME` | Human-readable project name |
| `LANGFUSE_INIT_PROJECT_PUBLIC_KEY` | Project public API key |
| `LANGFUSE_INIT_PROJECT_SECRET_KEY` | Project secret API key |
| `LANGFUSE_INIT_USER_EMAIL` | Initial owner user email |
| `LANGFUSE_INIT_USER_NAME` | Initial owner user name |
| `LANGFUSE_INIT_USER_PASSWORD` | Initial owner password |

Compose mapping rule:

- `LANGFUSE_INIT_PROJECT_PUBLIC_KEY` shall map to the same effective value provided to the pipeline as `LANGFUSE_PUBLIC_KEY`.
- `LANGFUSE_INIT_PROJECT_SECRET_KEY` shall map to the same effective value provided to the pipeline as `LANGFUSE_SECRET_KEY`.

### 4.3 Prompt Bootstrap Contract

The bootstrap service shall implement a repository-owned prompt-upsert contract:

```python
def ensure_langfuse_prompt(
    base_url: str,
    public_key: str,
    secret_key: str,
    prompt_name: str,
    prompt_label: str,
    prompt_template: str,
) -> dict[str, object]:
    """
    Ensure that the expected prompt exists, is updated to the repository-owned
    template, and has the desired label assigned.
    Returns a result record with status, prompt identity, and error detail.
    """
```

Bootstrap invariants:

- The prompt type shall be `text`.
- The prompt body shall remain semantically aligned with the repository runtime contract.
- The prompt shall support at least the variables `allowed_values_json` and `payload_json`.
- The desired label shall be intentionally assigned to the bootstrapped prompt version.

### 4.4 Prompt Template Contract

The bootstrapped prompt shall remain compatible with the current runtime and compile with:

```text
{{allowed_values_json}}
{{payload_json}}
```

The prompt content shall preserve:

- required JSON-only output instruction
- controlled-vocabulary enforcement
- privacy-safe `explanation_short` constraint
- payload insertion from the sanitized runtime payload only

### 4.5 Readiness and Startup Contract

The stack shall use dependency and readiness rules equivalent to the following sequence:

1. Start stateful Langfuse dependencies.
2. Start `langfuse-web` and `langfuse-worker`.
3. Wait until `langfuse-web` is healthy and API-ready.
4. Run `langfuse-bootstrap`.
5. Start `pipeline` or allow the user to run the pipeline command against the now-ready stack.

Allowed variations:

- The pipeline may start before bootstrap completes if local prompt fallback remains enabled.
- The pipeline may be a long-running daemon or a one-shot job, but this choice shall be documented explicitly.

### 4.6 User-Facing Commands Contract

The documentation shall expose at least:

```bash
docker compose up --build
docker compose down
docker compose down -v
```

If different profiles or overrides are required, the documentation shall make the happy path explicit and minimal.

## 5. Acceptance Criteria

- **AC-001**: Given a fresh machine with Docker and Docker Compose available, When the user runs `docker compose up`, Then the Langfuse self-hosted services, prompt-bootstrap service, and pipeline-related services shall start from repository-owned assets without manual UI setup.
- **AC-002**: Given a fresh stack startup, When Langfuse starts, Then organization, project, initial user, and API keys shall be initialized automatically from environment configuration.
- **AC-003**: Given a fresh stack startup, When `langfuse-bootstrap` runs after Langfuse readiness, Then the required prompt shall exist in Langfuse Prompt Management with the intended name and label.
- **AC-004**: Given a warm stack startup, When `langfuse-bootstrap` runs again, Then it shall complete idempotently without corrupting prior Langfuse state.
- **AC-005**: Given the pipeline service is configured with the initialized Langfuse keys and base URL, When a conversation enrichment execution runs, Then traces shall be emitted to the self-hosted Langfuse instance.
- **AC-006**: Given prompt bootstrap succeeded, When the pipeline resolves the runtime prompt, Then it shall be able to fetch the Langfuse-managed prompt using the configured name and label.
- **AC-007**: Given prompt bootstrap failed but local prompt fallback is enabled, When the pipeline runs, Then enrichment shall continue with the local prompt path instead of failing solely because bootstrap failed.
- **AC-008**: Given the user stops the stack with `docker compose down`, When containers stop, Then persisted Langfuse state shall remain available for the next warm startup unless volumes are explicitly removed.
- **AC-009**: Given the user stops the stack with `docker compose down -v`, When the stack is started again, Then headless initialization and bootstrap shall be able to reconstruct a working fresh environment.
- **AC-010**: Given repository documentation is reviewed, When the Compose option is described, Then the docs shall clearly distinguish low-scale local self-hosted usage from production-grade infrastructure.
- **AC-011**: Given the repository test and validation assets are executed, When Compose-related static checks run, Then they shall confirm the expected files, variables, and bootstrap contracts exist.
- **AC-012**: Given `PIPELINE_ENABLE_LLM_ENRICHMENT=0`, When the pipeline runs inside the Compose environment, Then the pipeline shall still complete without requiring successful external provider inference.

## 6. Test Automation Strategy

- **Test Levels**: Static validation, unit tests for bootstrap helpers, integration-style checks for Compose asset integrity, and optional smoke tests for container startup
- **Frameworks**: `pytest` for repository-local logic; shell or JSON/YAML validation tools for Compose asset checks
- **Test Data Management**: Reuse synthetic repository fixtures and avoid live Langfuse or provider requirements in baseline CI
- **CI/CD Integration**: Baseline repository tests shall not require actually starting the full self-hosted Langfuse stack unless an explicit smoke-test stage is added later
- **Coverage Requirements**: Cover bootstrap idempotency helpers, prompt template generation, environment mapping rules, and static Compose contract validation
- **Performance Testing**: Limited to smoke validation that the startup sequence is coherent for local development; no load test requirement in the first cycle

Required automated checks:

- Verify the presence of the Compose file(s) implementing this option.
- Verify that the Compose assets define the required service names or roles.
- Verify that required environment variables are documented and wired consistently.
- Verify that prompt-bootstrap assets exist and contain the expected prompt variables.
- Verify that repository docs reference the Compose startup and shutdown commands.
- Verify that baseline repository tests still pass without starting the Compose stack.

Repository-local commands may include:

- `venv/bin/python -m pytest -q`
- `venv/bin/python -m pytest tests/test_env.py -q`
- `venv/bin/python -m pytest tests/test_llm_runtime.py -q`
- a future Compose-asset test module such as `venv/bin/python -m pytest tests/test_docker_compose_langfuse.py -q`

## 7. Rationale & Context

The core user need behind option 2 is not just "support Langfuse", but "remove manual setup friction". In the current state, the runtime can already talk to Langfuse if the environment is configured correctly. The remaining friction is operational: obtaining a Langfuse instance, creating a project, extracting keys, creating the prompt, promoting the label, and ensuring the pipeline points to the same environment.

Docker Compose is the correct abstraction for this need because it lets the repository own the entire local dependency graph. It also makes the setup reproducible for reviewers and other users who should not have to create resources manually in a browser before they can validate the solution.

The use of Langfuse headless initialization is central because it removes the need for manual creation of organization, project, user, and API keys. However, headless initialization alone does not fully solve Prompt Management because prompts are application-domain assets, not infrastructure primitives. That is why a repository-owned prompt-bootstrap step is required.

The local prompt fallback remains strategically important. It converts bootstrap failure from a hard system failure into a degraded but still functional startup path. This matches the repository's broader architectural stance: observability and prompt hosting should improve the runtime, not become a single point of failure for local reproducibility.

Separating Langfuse infra from prompt bootstrap also creates a clean maintenance boundary. Infrastructure state belongs to Langfuse; the prompt contract belongs to the repository. Keeping that distinction explicit makes the Compose stack easier to reason about and update.

## 8. Dependencies & External Integrations

### External Systems

- **EXT-001**: Self-hosted Langfuse API and UI - Observability, prompt management, and trace inspection
- **EXT-002**: OpenAI API - Optional primary LLM provider when LLM enrichment is enabled
- **EXT-003**: Anthropic API - Optional fallback LLM provider when LLM enrichment is enabled

### Third-Party Services

- **SVC-001**: Docker Engine and Docker Compose - Local container orchestration required for the option-2 deployment path

### Infrastructure Dependencies

- **INF-001**: Containerized PostgreSQL - Required transactional datastore for self-hosted Langfuse
- **INF-002**: Containerized ClickHouse - Required analytical datastore for traces and observations
- **INF-003**: Containerized Redis or Valkey - Required queue/cache service for self-hosted Langfuse
- **INF-004**: Containerized object storage such as MinIO - Required blob storage for self-hosted Langfuse capabilities
- **INF-005**: Named Docker volumes - Required persistence layer for warm restarts

### Data Dependencies

- **DAT-001**: Repository-owned runtime prompt template - Required source for prompt bootstrap content
- **DAT-002**: Privacy-safe conversation payload contract - Required runtime data contract used both by providers and Langfuse-linked prompt execution

### Technology Platform Dependencies

- **PLT-001**: Docker Compose-compatible local development environment - Required orchestration runtime
- **PLT-002**: Langfuse self-hosted architecture supported for Docker Compose low-scale deployments - Required observability platform
- **PLT-003**: Repository-local Python runtime or compatible bootstrap execution environment - Required to run prompt-bootstrap logic if implemented in Python

### Compliance Dependencies

- **COM-001**: Repository privacy contract for masked and publish-safe data - Langfuse deployment shall remain inside this boundary
- **COM-002**: Repository documentation policy in `AGENTS.md` - Material infrastructure and execution changes shall update docs in the same work cycle

## 9. Examples & Edge Cases

### 9.1 Happy Path Example

```yaml
services:
  langfuse-web:
    # self-hosted Langfuse web/API service
  langfuse-worker:
    # self-hosted background worker
  langfuse-bootstrap:
    # waits for langfuse-web, then ensures prompt exists
  pipeline:
    environment:
      PIPELINE_ENABLE_LANGFUSE: "1"
      LANGFUSE_BASE_URL: "http://langfuse-web:3000"
      LANGFUSE_PUBLIC_KEY: "${LANGFUSE_INIT_PROJECT_PUBLIC_KEY}"
      LANGFUSE_SECRET_KEY: "${LANGFUSE_INIT_PROJECT_SECRET_KEY}"
      PIPELINE_LANGFUSE_PROMPT_NAME: "conversation-enrichment-v1"
      PIPELINE_LANGFUSE_PROMPT_LABEL: "production"
      PIPELINE_LANGFUSE_ALLOW_LOCAL_PROMPT_FALLBACK: "1"
```

Expected property:

- one stack owns the observability environment and the pipeline configuration
- no manual browser setup is required for the happy path

### 9.2 Edge Case: Prompt Bootstrap Fails

Scenario:

- Langfuse infra is healthy
- `langfuse-bootstrap` fails because the prompt API call is rejected or times out

Expected behavior:

- bootstrap container exits with clear error logs
- pipeline can still run with local prompt fallback if enabled
- operator can inspect logs and rerun the bootstrap service after fixing configuration

### 9.3 Edge Case: Warm Restart with Existing Prompt

Scenario:

- named volumes persist Langfuse data
- prompt already exists with the desired name

Expected behavior:

- bootstrap logic updates or confirms the prompt deterministically
- duplicate logical prompts or label confusion are avoided

### 9.4 Edge Case: User Removes Volumes

Scenario:

- user runs `docker compose down -v`

Expected behavior:

- next startup behaves like a fresh environment
- headless initialization recreates org/project/user/keys
- bootstrap recreates the prompt and label

### 9.5 Edge Case: LLM Providers Disabled

Scenario:

- user starts the full Compose stack but keeps `PIPELINE_ENABLE_LLM_ENRICHMENT=0`

Expected behavior:

- Langfuse infra may still start
- pipeline remains functionally runnable in deterministic mode
- absence of provider inference does not invalidate the Compose deployment

## 10. Validation Criteria

The implementation shall be considered compliant with this specification only if all of the following are true:

- The repository contains Compose assets for option 2 in version-controlled paths.
- The Compose assets define a self-hosted Langfuse stack, prompt-bootstrap path, and pipeline integration.
- Langfuse headless initialization values and runtime keys are wired consistently.
- Prompt bootstrap is repository-owned, idempotent, and aligned with the runtime prompt contract.
- The user-facing path can be described accurately as a `docker compose up` experience for a fresh machine.
- Failure of prompt bootstrap does not hard-break the pipeline when local prompt fallback is enabled.
- Documentation reflects startup, shutdown, persistence, and low-scale limitations.
- Baseline repository tests continue to pass without requiring live Langfuse infrastructure.

## 11. Related Specifications / Further Reading

- [spec/spec-architecture-langfuse-phased-prompt-observability.md](/home/lucas/projects/lucas54neves/namastex-test/spec/spec-architecture-langfuse-phased-prompt-observability.md)
- [spec/spec-architecture-llm-provider-runtime-integration.md](/home/lucas/projects/lucas54neves/namastex-test/spec/spec-architecture-llm-provider-runtime-integration.md)
- [README.md](/home/lucas/projects/lucas54neves/namastex-test/README.md)
- Langfuse self-hosted Docker Compose documentation: https://langfuse.com/self-hosting/deployment/docker-compose
- Langfuse headless initialization documentation: https://langfuse.com/self-hosting/administration/headless-initialization
- Langfuse environment configuration documentation: https://langfuse.com/self-hosting/configuration
- Langfuse Prompt Management documentation: https://langfuse.com/docs/prompt-management/get-started
- Langfuse prompt labels and versions documentation: https://langfuse.com/docs/prompt-management/features/prompt-version-control
