---
title: Architecture Specification for Phased Langfuse Prompt Observability and Prompt Management
version: 1.0
date_created: 2026-04-22
last_updated: 2026-04-22
owner: Lucas Neves
tags: [architecture, langfuse, observability, prompts, llm, pipeline]
---

# Introduction

This specification defines a phased architecture for adding Langfuse-based observability and prompt management to the existing conversation enrichment runtime.

The goal is to improve prompt and inference visibility without weakening the repository's current privacy guarantees, deterministic fallback behavior, cache semantics, or optional-provider execution model.

## 1. Purpose & Scope

This specification defines the requirements, constraints, integration boundaries, and acceptance criteria for implementing Langfuse across three sequential phases in the existing LLM enrichment runtime.

Scope:

- Introduce optional Langfuse tracing for current OpenAI and Anthropic calls.
- Preserve the existing runtime path in which prompts are still built in code during the first phase.
- Migrate the hardcoded runtime prompt to Langfuse Prompt Management during the second phase.
- Enrich traces with conversation-level operational metadata and evaluation-ready identifiers during the third phase.
- Keep the current deterministic fallback path, validation behavior, artifact publication, and pipeline execution model intact.
- Define how Langfuse interacts with privacy-safe payloads, cache keys, prompt versioning, and short-lived versus long-running entrypoints.

Out of scope:

- Replacing LangChain or LangGraph with another orchestration model.
- Making Langfuse mandatory for the baseline local runtime.
- Redesigning the business taxonomy or the semantic output schema used by `silver_conversations_llm`.
- Moving the operational agent, planner, or approval subsystems to Langfuse.
- Introducing online evaluation loops, human feedback collection UIs, or automated prompt experimentation in the first implementation cycle.
- Turning the repository into a general-purpose prompt platform beyond the needs of the conversation enrichment runtime.

Intended audience:

- Engineers implementing the phased Langfuse integration
- Reviewers validating privacy, observability, and runtime compatibility
- Future maintainers extending tracing, prompt versioning, or evaluation workflows

Assumptions:

- The project already builds sanitized conversation payloads before provider invocation.
- The current runtime prompt is assembled in `src/pipeline/runtime/llm_runtime.py`.
- The current cache behavior depends on `llm_input_hash`, `prompt_version`, and runtime model identity.
- The existing local execution model uses the repository virtual environment at `venv/`.
- External observability must remain optional and must not block deterministic local execution when disabled or unavailable.

## 2. Definitions

- **Langfuse**: External observability and prompt management platform used for tracing, prompt versioning, and future evaluation workflows.
- **Phase 1**: Optional tracing integration for the current runtime prompt flow, without migrating prompt source-of-truth out of code.
- **Phase 2**: Prompt Management migration in which the runtime prompt is retrieved from Langfuse instead of being hardcoded locally.
- **Phase 3**: Extended trace enrichment that adds conversation-level metadata, deterministic trace identifiers, and scoring-ready observability hooks.
- **Tracing Mode**: Langfuse integration mode in which traces are emitted for model calls while prompt content may still originate from application code.
- **Prompt Management Mode**: Langfuse integration mode in which the runtime prompt is stored, versioned, and retrieved from Langfuse.
- **Prompt Version**: The version identifier used by the pipeline runtime to control cache invalidation and auditability.
- **Prompt Label**: Langfuse-side deployment label such as `production`, used to retrieve a selected prompt version intentionally.
- **Trace**: A Langfuse record representing one observed execution, optionally containing nested observations for generation, validation, and runtime metadata.
- **Generation Observation**: A Langfuse observation corresponding to a model invocation and its associated prompt, inputs, outputs, and metadata.
- **Short-Lived Entrypoint**: A script such as `scripts/run_pipeline.py` that exits after one execution and may require explicit queue flushing or shutdown.
- **Long-Running Entrypoint**: A daemon process such as `scripts/run_pipeline_daemon.py` that stays alive across multiple pipeline cycles.
- **Privacy-Safe Payload**: The sanitized request payload already prepared by the project for enrichment, excluding raw PII-bearing values.
- **Evaluation Readiness**: The state in which traces contain sufficient identifiers and metadata to support later manual or automated scoring without redesigning the runtime.

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: The Langfuse integration shall be implemented as a three-phase roadmap within one coherent runtime architecture.
- **REQ-002**: Phase 1 shall add optional tracing for existing OpenAI and Anthropic runtime calls without changing the current prompt source-of-truth.
- **REQ-003**: Phase 1 shall preserve the current prompt assembly behavior in `src/pipeline/runtime/llm_runtime.py` while making traced model calls observable in Langfuse.
- **REQ-004**: Phase 1 shall remain compatible with the existing LangChain and LangGraph provider runtime already used by the repository.
- **REQ-005**: Phase 1 shall allow Langfuse to be disabled fully through configuration or environment variables without changing enrichment behavior.
- **REQ-006**: Phase 1 shall emit traces only for payloads that the project already considers safe to send to providers.
- **REQ-007**: Phase 2 shall migrate the runtime prompt from hardcoded application text to Langfuse Prompt Management.
- **REQ-008**: Phase 2 shall preserve semantic parity between the Langfuse-managed prompt and the current prompt contract, including required fields, controlled vocabularies, and privacy-safe explanation constraints.
- **REQ-009**: Phase 2 shall define how the pipeline `prompt_version` relates to Langfuse prompt identifiers, labels, or versions.
- **REQ-010**: Phase 2 shall preserve cache correctness when prompt source changes from local code to Langfuse-managed prompt versions.
- **REQ-011**: Phase 2 shall support retrieving a production-selected prompt version intentionally rather than relying on mutable prompt text without traceability.
- **REQ-012**: Phase 3 shall enrich traces with deterministic identifiers and operational metadata such as conversation identity, provider identity, model identity, and inference outcome.
- **REQ-013**: Phase 3 shall prepare the runtime for future scoring and evaluation workflows without making scoring mandatory in the first delivery.
- **REQ-014**: The implementation shall preserve the current deterministic fallback when providers fail, return invalid output, or when Langfuse is unavailable.
- **REQ-015**: The implementation shall preserve the current published artifact contract of `silver_conversations_llm` unless a later explicit schema change is specified and documented.
- **REQ-016**: The implementation shall preserve the current provider execution order and provider fallback semantics.
- **REQ-017**: The implementation shall preserve the baseline local runtime path in which `PIPELINE_ENABLE_LLM_ENRICHMENT=0` executes successfully without requiring Langfuse or network access.
- **REQ-018**: The implementation shall update `README.md` in the same work cycle when a Langfuse phase changes runtime operation, environment configuration, prompt source-of-truth, or observability behavior materially.
- **REQ-019**: The implementation shall update `.env.example` in the same work cycle when new Langfuse environment variables are introduced.
- **REQ-020**: The implementation shall include repository-local automated coverage for phase-specific runtime behavior, toggles, and failure handling.
- **REQ-021**: Phase 1 and Phase 3 shall ensure that short-lived runtime entrypoints flush or shut down Langfuse cleanly before process exit when tracing is enabled.
- **REQ-022**: Phase 1 and Phase 3 shall avoid per-cycle forced synchronous flushing in the daemon path unless required for correctness or graceful shutdown semantics.
- **REQ-023**: The runtime shall expose stable test seams so Langfuse can be stubbed or disabled in tests without live network access.
- **REQ-024**: The implementation shall make it possible to trace prompt use back to a specific conversation enrichment execution.
- **REQ-025**: When Phase 2 is implemented, the runtime shall attach the Langfuse-managed prompt to its traced generation so prompt-level metrics can be attributed to the correct prompt version.

- **SEC-001**: Langfuse credentials shall be read only from environment variables or equivalent secret injection mechanisms.
- **SEC-002**: The Langfuse integration shall not capture raw credentials, raw provider secrets, or raw unmasked PII in traces, logs, or persisted artifacts.
- **SEC-003**: The integration shall treat the current project privacy boundary as the upper bound for observable prompt and payload data; it shall not widen the data exposure surface beyond what is already sent to providers intentionally.
- **SEC-004**: If the runtime prompt includes payload content, only the privacy-safe payload already assembled by the repository may be attached to Langfuse observations.
- **SEC-005**: Any trace enrichment added in Phase 3 shall use technical identifiers and compact metadata rather than raw conversation dumps beyond the approved prompt payload.
- **SEC-006**: The implementation shall preserve existing privacy validation for `explanation_short` and shall not treat successful tracing as evidence that the response is publish-safe.

- **CON-001**: Langfuse shall remain optional and shall not become a hard dependency for baseline deterministic validation or publication.
- **CON-002**: The integration shall not make the operational agent, planner, approval, or remediation flows depend on Langfuse availability.
- **CON-003**: The integration shall not weaken current cache key semantics by introducing volatile trace metadata into cache identity.
- **CON-004**: The integration shall not require live Langfuse access for local tests that exercise deterministic or mocked provider paths.
- **CON-005**: The first implementation cycle shall not depend on Langfuse-hosted prompt experimentation, A/B testing, or online evaluators.
- **CON-006**: The implementation shall remain compatible with repository-local execution through `venv/bin/python`.
- **CON-007**: The implementation shall not commit secrets, exported prompt payload snapshots, or trace dumps into the repository.
- **CON-008**: The implementation shall not alter provider business semantics solely to satisfy an observability tool.
- **CON-009**: The implementation shall not force synchronous Langfuse network operations on every model call when background batching is sufficient.
- **CON-010**: The implementation shall not treat Langfuse scoring or evaluation support as a prerequisite for successful pipeline execution.

- **GUD-001**: Keep Langfuse-specific wiring isolated in the runtime layer rather than scattering observability code through transforms or publication logic.
- **GUD-002**: Prefer one Langfuse integration boundary that wraps model invocation and trace context propagation for both providers.
- **GUD-003**: Prefer explicit environment toggles such as `PIPELINE_ENABLE_LANGFUSE` over implicit activation based only on key presence.
- **GUD-004**: Prefer deterministic trace metadata names that correspond to pipeline terms already used in artifacts and logs.
- **GUD-005**: Keep prompt-management rollout isolated from tracing rollout so Phase 1 can ship independently and safely.
- **GUD-006**: Prefer attaching compact metadata to traces instead of duplicating large runtime objects.
- **GUD-007**: Keep the current prompt contract and Langfuse prompt template close enough that regression comparison remains straightforward during Phase 2 migration.

- **PAT-001**: Recommended phased pattern: Phase 1 tracing wrapper around current calls -> Phase 2 prompt source migration with version mapping -> Phase 3 trace enrichment and scoring readiness.
- **PAT-002**: Recommended runtime pattern: build privacy-safe payload -> build or fetch prompt -> invoke provider with Langfuse tracing when enabled -> validate output -> persist success or deterministic fallback.
- **PAT-003**: Recommended prompt migration pattern: mirror current prompt contract in Langfuse -> compare output parity in tests -> switch runtime retrieval behind a feature toggle -> promote one labeled production prompt.
- **PAT-004**: Recommended trace pattern: one trace per conversation enrichment execution, with provider attempts and final outcome recorded as structured metadata.

## 4. Interfaces & Data Contracts

### 4.1 Phased Runtime Boundaries

| Phase | Primary objective | Prompt source | Langfuse role |
| --- | --- | --- | --- |
| Phase 1 | Observe current provider calls | Local code in `llm_runtime.py` | Tracing only |
| Phase 2 | Externalize prompt versioning and retrieval | Langfuse Prompt Management | Tracing plus prompt management |
| Phase 3 | Add operational trace context and scoring readiness | Langfuse-managed prompt | Tracing, prompt linkage, metadata enrichment, scoring-ready trace IDs |

### 4.2 Code Ownership Boundaries

Recommended ownership boundaries:

| Module | Responsibility |
| --- | --- |
| `src/pipeline/runtime/llm_runtime.py` | Provider invocation, Langfuse integration, prompt assembly or retrieval, trace metadata |
| `src/pipeline/transforms/conversation_enrichment.py` | Payload assembly, cache checks, deterministic fallback, artifact persistence |
| `src/pipeline/runtime/spec.py` and `config/pipeline_spec.json` | Default prompt-related and Langfuse-related runtime configuration |
| `scripts/run_pipeline.py` | Single-run runtime entrypoint and short-lived Langfuse shutdown handling |
| `scripts/run_pipeline_daemon.py` | Daemon entrypoint and graceful Langfuse lifecycle handling |
| `README.md` and `.env.example` | Operational documentation and environment configuration |

### 4.3 Configuration Contract

The phased integration shall support explicit runtime configuration through compiled plan values and environment variables.

Recommended environment variables:

| Variable | Required When | Purpose |
| --- | --- | --- |
| `PIPELINE_ENABLE_LANGFUSE` | Always optional | Master toggle for Langfuse integration |
| `LANGFUSE_PUBLIC_KEY` | Langfuse enabled | Langfuse authentication |
| `LANGFUSE_SECRET_KEY` | Langfuse enabled | Langfuse authentication |
| `LANGFUSE_BASE_URL` | Optional | Region or self-hosted base URL |
| `PIPELINE_LANGFUSE_PROMPT_NAME` | Phase 2 enabled | Langfuse prompt identifier used by runtime |
| `PIPELINE_LANGFUSE_PROMPT_LABEL` | Phase 2 enabled | Label used to select prompt version intentionally |
| `PIPELINE_LANGFUSE_TRACE_NAME` | Optional | Override for top-level trace naming |

Recommended compiled plan shape extension:

```json
{
  "llm": {
    "enabled": false,
    "prompt_version": "v1",
    "providers": {
      "openai": {
        "enabled": true,
        "model": "gpt-5-mini"
      },
      "anthropic": {
        "enabled": true,
        "model": "claude-sonnet"
      }
    },
    "langfuse": {
      "enabled": false,
      "prompt_name": "conversation-enrichment-v1",
      "prompt_label": "production",
      "trace_name": "conversation-enrichment"
    }
  }
}
```

Configuration rules:

- Langfuse configuration shall not enable provider execution by itself.
- Langfuse configuration shall be ignored safely when the feature is disabled.
- Missing Langfuse credentials while Langfuse is enabled shall degrade to non-traced provider execution or explicitly documented no-op behavior instead of crashing the pipeline by default.

### 4.4 Phase 1 Invocation Contract

During Phase 1, the runtime prompt remains local.

Recommended behavior:

```python
def invoke_provider_with_optional_tracing(
    provider: str,
    payload: dict[str, object],
    compiled_plan: dict[str, object],
) -> dict[str, object]:
    """
    Build prompt locally, invoke provider through LangChain, attach Langfuse
    callback handler when enabled, and return the same runtime contract used today.
    """
```

Phase 1 invariants:

- `_build_prompt(payload, compiled_plan)` remains the prompt builder.
- The provider result contract returned to `conversation_enrichment.py` remains unchanged.
- Langfuse activation shall not change accepted output schema or fallback semantics.

### 4.5 Phase 2 Prompt Management Contract

During Phase 2, the runtime fetches the prompt from Langfuse instead of relying solely on locally hardcoded prompt text.

Recommended runtime contract:

```python
def resolve_runtime_prompt(
    payload: dict[str, object],
    compiled_plan: dict[str, object],
) -> dict[str, object]:
    """
    Returns a prompt resolution record such as:
      {
        "source": "langfuse" | "local_fallback",
        "prompt_text": str,
        "prompt_name": str | None,
        "prompt_label": str | None,
        "prompt_version": str,
        "langfuse_prompt_ref": object | None
      }
    """
```

Prompt-resolution rules:

- The runtime shall support explicit fallback to a local prompt path if Langfuse prompt retrieval fails and that fallback is allowed by configuration.
- The runtime shall persist or expose enough metadata to distinguish local prompt use from Langfuse-managed prompt use.
- The runtime shall define cache identity using a stable prompt version concept rather than raw prompt text or ephemeral trace metadata.

### 4.6 Prompt Versioning and Cache Contract

The cache contract shall remain explicit.

Recommended cache identity inputs:

| Field | Meaning |
| --- | --- |
| `llm_input_hash` | Stable hash of the privacy-safe request payload |
| `prompt_version` | Pipeline-level prompt version or Langfuse-derived mapped version |
| `llm_model` | Effective provider model identity already used by the runtime |

Versioning rules:

- Phase 1 may keep the current local `prompt_version` unchanged.
- Phase 2 shall define one explicit mapping from Langfuse prompt selection to persisted `prompt_version`.
- Phase 2 shall not rely on mutable labels alone as cache identity without a stable resolved version.
- Phase 3 may enrich traces with prompt IDs and labels, but those fields shall not replace the persisted cache contract unless explicitly versioned.

### 4.7 Phase 3 Trace Metadata Contract

Phase 3 shall add structured metadata sufficient for operational review and future scoring.

Recommended trace metadata fields:

| Field | Type | Description |
| --- | --- | --- |
| `conversation_id` | `string` | Stable conversation identifier |
| `lead_key` | `string` | Stable lead identifier |
| `prompt_version` | `string` | Persisted runtime prompt version |
| `prompt_source` | `string` | `local` or `langfuse` |
| `prompt_name` | `string/null` | Langfuse prompt name when available |
| `prompt_label` | `string/null` | Langfuse prompt label when available |
| `provider_name` | `string/null` | Accepted provider or attempted provider context |
| `model_name` | `string/null` | Effective model used |
| `inference_status` | `string` | Success, fallback, invalid output, or provider error path |
| `attempted_providers` | `array[string]` | Ordered provider attempts |
| `llm_input_hash` | `string` | Stable request identity for debugging and later scoring correlation |
| `trace_id` | `string/null` | Deterministic or captured Langfuse trace identifier |

Phase 3 shall allow later scoring workflows to target a specific trace without requiring a runtime redesign.

### 4.8 Entrypoint Lifecycle Contract

Langfuse uses background batching and shall be integrated according to process lifecycle.

Entrypoint requirements:

- `scripts/run_pipeline.py` shall flush or shut down Langfuse before process exit when Langfuse tracing is enabled.
- `scripts/run_pipeline_daemon.py` shall keep Langfuse alive across cycles and shall shut it down gracefully when the daemon exits.
- The daemon implementation should avoid explicit per-cycle blocking flushes during normal operation unless a specific operational need is documented.

## 5. Acceptance Criteria

- **AC-001**: Given Phase 1 is enabled and Langfuse credentials are configured, When a provider call is executed, Then Langfuse shall receive a trace for that execution without changing the provider output contract returned to the pipeline.
- **AC-002**: Given Phase 1 is disabled, When enrichment runs, Then provider execution shall behave as it does today without requiring Langfuse configuration.
- **AC-003**: Given Phase 1 is enabled but Langfuse is unavailable or misconfigured, When enrichment runs, Then the runtime shall degrade gracefully according to the documented fallback policy and shall not fail the entire pipeline solely because tracing is unavailable.
- **AC-004**: Given the current prompt contract in code, When Phase 1 tracing is added, Then the prompt content observed by providers shall remain semantically equivalent to the pre-Langfuse implementation.
- **AC-005**: Given Phase 2 is enabled, When the runtime resolves the prompt, Then it shall fetch a Langfuse-managed prompt by explicit name and intended label or equivalent stable selector.
- **AC-006**: Given Phase 2 is enabled and a Langfuse prompt version changes, When the pipeline reruns, Then the prompt version used for cache identity shall reflect that change so affected conversations become eligible for re-enrichment.
- **AC-007**: Given Phase 2 is enabled and prompt retrieval fails, When the runtime is configured to allow local prompt fallback, Then enrichment shall continue using the local prompt path and the prompt source shall remain distinguishable in diagnostics or traces.
- **AC-008**: Given Phase 2 has migrated the prompt, When a traced generation is inspected in Langfuse, Then the trace shall be linked to the specific Langfuse prompt used by that execution.
- **AC-009**: Given Phase 3 is enabled, When one conversation enrichment execution runs, Then the resulting trace shall include `conversation_id`, `lead_key`, `prompt_version`, provider metadata, and inference status.
- **AC-010**: Given Phase 3 is enabled, When a later evaluator or operator needs to score one execution, Then the runtime shall expose a stable trace identifier or equivalent correlation handle for that execution.
- **AC-011**: Given `PIPELINE_ENABLE_LLM_ENRICHMENT=0`, When the baseline deterministic runtime path is executed, Then the pipeline shall still complete successfully without Langfuse or network access.
- **AC-012**: Given a short-lived single-run execution with Langfuse enabled, When the script exits, Then pending Langfuse events shall be flushed or shut down so traces are not silently lost.
- **AC-013**: Given the daemon entrypoint with Langfuse enabled, When multiple cycles run, Then tracing shall remain functional across cycles without mandatory blocking flush after every cycle.
- **AC-014**: Given the implementation is complete for any phase, When repository documentation is reviewed, Then `README.md` and `.env.example` shall reflect the runtime behavior and configuration introduced in that phase.
- **AC-015**: Given the implementation is complete, When `venv/bin/python -m pytest -q` is executed, Then the relevant test coverage for toggles, graceful degradation, and runtime contracts shall pass.

## 6. Test Automation Strategy

- **Test Levels**: Unit tests for runtime helpers and integration-style tests for enrichment flow and entrypoint lifecycle behavior
- **Frameworks**: `pytest` executed via `venv/bin/python -m pytest -q`
- **Test Data Management**: Reuse synthetic conversation fixtures and payload builders already present in the repository
- **CI/CD Integration**: No network-dependent tests shall be required for baseline verification
- **Coverage Requirements**: Cover enabled and disabled Langfuse paths, prompt-resolution behavior, cache-version effects, and graceful shutdown semantics
- **Performance Testing**: Limited to ensuring the integration does not introduce obvious blocking behavior in the daemon path

Required automated checks by phase:

- Phase 1 tests shall verify Langfuse toggle behavior, callback attachment or no-op behavior, and graceful degradation when Langfuse is unavailable.
- Phase 1 tests shall verify that current provider runtime output shape remains unchanged when tracing is added.
- Phase 2 tests shall verify prompt resolution from Langfuse-managed configuration, prompt-source fallback behavior, and cache-version semantics.
- Phase 2 tests shall verify that a prompt-version change makes an otherwise unchanged payload eligible for re-enrichment.
- Phase 3 tests shall verify trace metadata assembly, deterministic trace correlation fields, and lifecycle handling for short-lived entrypoints.
- Entrypoint tests shall verify that single-run execution calls Langfuse flush or shutdown when enabled.
- Existing runtime tests shall continue to pass without requiring live Langfuse connectivity.

Repository-local commands:

- `venv/bin/python -m pytest -q`
- `venv/bin/python -m pytest tests/test_llm_runtime.py -q`
- `venv/bin/python -m pytest tests/test_transforms.py -q`
- Additional Langfuse-focused tests may be added if the implementation creates a dedicated test module.

## 7. Rationale & Context

The project already has a focused and testable runtime boundary for LLM enrichment. Prompts are currently assembled in one place, payloads are already sanitized before provider use, deterministic fallback behavior is explicit, and cache semantics are already tied to prompt and model identity. This is a favorable architecture for phased Langfuse adoption.

The main reason to separate the implementation into three phases is risk control.

Phase 1 adds observability without changing prompt source-of-truth. This delivers operational value quickly while keeping business behavior stable.

Phase 2 changes a more sensitive boundary: who owns prompt text and versioning. That step affects cache semantics, deployment workflow, and traceability, so it needs its own explicit contract.

Phase 3 is intentionally about enrichment of observability rather than enrichment of model semantics. The purpose is to make traces operationally useful and evaluation-ready without turning Langfuse into a required runtime dependency.

Privacy is the central architectural constraint. This repository was designed to publish masked artifacts and avoid exposing raw message content outside the approved runtime path. Langfuse must fit inside that boundary rather than redefining it. The correct interpretation is not "Langfuse may see anything the process sees"; it is "Langfuse may observe only what the runtime is already intentionally sending through the approved provider prompt boundary."

The lifecycle distinction between `scripts/run_pipeline.py` and `scripts/run_pipeline_daemon.py` also matters. Background batching is compatible with long-running execution, but short-lived scripts require explicit flushing or shutdown to avoid missing traces.

## 8. Dependencies & External Integrations

### External Systems

- **EXT-001**: Langfuse API or self-hosted Langfuse deployment - Trace ingestion, prompt management, and future scoring support
- **EXT-002**: OpenAI API - Primary provider runtime already used by the repository
- **EXT-003**: Anthropic API - Fallback provider runtime already used by the repository

### Third-Party Services

- **SVC-001**: Langfuse observability service - Required only when Langfuse integration is enabled

### Infrastructure Dependencies

- **INF-001**: Repository-local virtual environment at `venv/` - Required execution environment
- **INF-002**: Environment-based secret injection - Required for Langfuse and provider credentials
- **INF-003**: Optional outbound network access - Required only when provider calls or Langfuse tracing are enabled

### Data Dependencies

- **DAT-001**: Privacy-safe conversation enrichment payload - Existing runtime input to providers and upper bound for observable request data

### Technology Platform Dependencies

- **PLT-001**: LangChain callback-compatible runtime - Required for low-friction tracing integration in Phase 1
- **PLT-002**: LangGraph provider execution flow - Existing orchestration context that shall remain intact
- **PLT-003**: Python runtime compatible with repository tooling - Required for Langfuse client lifecycle management and tests

### Compliance Dependencies

- **COM-001**: Repository privacy contract for masked data publication - Langfuse integration shall not violate this boundary
- **COM-002**: Repository documentation policy in `AGENTS.md` - Relevant runtime or execution changes shall update documentation in the same work cycle

## 9. Examples & Edge Cases

### 9.1 Phase 1 Example

```python
prompt = _build_prompt(payload, compiled_plan)
callbacks = [langfuse_handler] if langfuse_enabled else []
response = ChatOpenAI(
    model=provider_cfg["model"],
    timeout=provider_cfg["timeout_seconds"],
    max_retries=provider_cfg["max_retries"],
).invoke(prompt, config={"callbacks": callbacks} if callbacks else None)
```

Expected property:

- The prompt still comes from local code.
- Langfuse sees the generation only when enabled.
- The provider response parsing and validation contract is unchanged.

### 9.2 Phase 2 Example

```python
resolved_prompt = resolve_runtime_prompt(payload, compiled_plan)
compiled_prompt = resolved_prompt["prompt_text"]
prompt_version = resolved_prompt["prompt_version"]
```

Expected property:

- The runtime can identify whether the prompt came from Langfuse or local fallback.
- `prompt_version` remains stable enough to support cache invalidation.

### 9.3 Phase 3 Example

```python
trace_metadata = {
    "conversation_id": payload["conversation_id"],
    "lead_key": payload["lead_key"],
    "llm_input_hash": llm_input_hash,
    "prompt_version": prompt_version,
    "provider_name": provider_name,
    "inference_status": status,
}
```

Expected property:

- The trace can be correlated with one persisted enrichment row.
- Later scoring can target the same execution deterministically.

### 9.4 Edge Cases

- Langfuse enabled, but credentials missing:
  The runtime should degrade gracefully according to documented policy and should not abort the whole pipeline only because tracing cannot start.
- Langfuse prompt retrieval fails in Phase 2:
  If local fallback is enabled, the runtime should use the local prompt path and mark prompt source accordingly.
- Prompt label remains the same, but Langfuse prompt version changes:
  The runtime must still resolve a stable prompt version for cache invalidation rather than treating the label alone as immutable identity.
- Single-run script exits immediately after one traced generation:
  The process must flush or shut down Langfuse so events are not lost.
- Daemon executes many cycles:
  The runtime should avoid unnecessary synchronous flushing after each cycle.
- Provider output is invalid but trace emission succeeds:
  The runtime still validates and falls back deterministically; observability success does not imply business acceptance.
- Langfuse is fully disabled:
  The runtime should remain behaviorally equivalent to the pre-Langfuse path.

## 10. Validation Criteria

The implementation shall be considered compliant with this specification only if all of the following are true:

- Phase boundaries remain explicit and testable.
- Langfuse can be enabled and disabled without changing baseline deterministic behavior.
- Phase 1 does not change the prompt source-of-truth.
- Phase 2 defines and implements a stable prompt-version-to-cache contract.
- Phase 3 exposes enough structured metadata to support later scoring without redesign.
- Short-lived and long-running entrypoints handle Langfuse client lifecycle correctly.
- Privacy boundaries remain consistent with current repository rules.
- Documentation and environment configuration are updated in the same work cycle as each material runtime change.
- Repository-local tests pass using the commands defined in `AGENTS.md`.

## 11. Related Specifications / Further Reading

- [spec/spec-architecture-llm-provider-runtime-integration.md](/home/lucas/projects/lucas54neves/namastex-test/spec/spec-architecture-llm-provider-runtime-integration.md)
- [spec/spec-architecture-llm-output-normalization-contract.md](/home/lucas/projects/lucas54neves/namastex-test/spec/spec-architecture-llm-output-normalization-contract.md)
- [spec/spec-architecture-terminal-runtime-logging.md](/home/lucas/projects/lucas54neves/namastex-test/spec/spec-architecture-terminal-runtime-logging.md)
- [README.md](/home/lucas/projects/lucas54neves/namastex-test/README.md)
- Langfuse documentation for LangChain tracing, prompt linking, and prompt management
