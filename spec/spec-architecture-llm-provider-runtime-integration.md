---
title: Architecture Specification for Runtime LLM Provider Integration with LangChain and LangGraph
version: 1.0
date_created: 2026-04-21
last_updated: 2026-04-21
owner: Lucas Neves
tags: [architecture, llm, langchain, langgraph, openai, anthropic, pipeline]
---

# Introduction

This specification defines the target runtime architecture for connecting the existing conversation enrichment stage to real LLM providers using LangChain and LangGraph.

The goal is to preserve the current deterministic pipeline behavior while enabling structured semantic inference through a provider stack in which OpenAI is the primary provider and Anthropic is the provider-level fallback.

## 1. Purpose & Scope

This specification defines requirements, constraints, interfaces, runtime behavior, and validation rules for implementing a production-ready LLM provider integration for the existing conversation-level enrichment flow.

Scope:

- Connect the existing `src/pipeline/conversation_enrichment.py` flow to a real LLM runtime.
- Use LangChain abstractions for model client calls and schema-oriented structured output handling.
- Use LangGraph to model the provider execution flow, including primary execution and fallback routing.
- Use OpenAI as the first provider attempted for enrichment requests.
- Use Anthropic as the provider fallback when the OpenAI attempt fails or returns invalid output.
- Preserve deterministic fallback behavior already implemented in the repository when no provider result can be accepted.

Out of scope:

- Changing the business taxonomy already defined for conversation enrichment.
- Moving LLM logic into the operational agent, planner, approval flow, or auto-remediation flow.
- Replacing deterministic Gold consolidation rules with non-deterministic logic.
- Supporting more than two external providers in the first implementation cycle.
- Building a generic multi-tenant prompt management platform.

Intended audience:

- Engineers implementing the runtime integration
- Reviewers evaluating architectural alignment
- Future maintainers extending provider support

Assumptions:

- The repository already persists `silver_conversations_llm.parquet`.
- The current code already assembles sanitized payloads and deterministic fallback output.
- The implementation will run inside the repository virtual environment and test suite.
- Provider credentials will be injected through environment variables, not committed files.

## 2. Definitions

- **LangChain**: Library layer used for model client integration, prompt composition, and structured output parsing.
- **LangGraph**: Graph-based orchestration layer used to model the provider execution flow and fallback routing.
- **Primary Provider**: The first external provider attempted for a conversation enrichment request. In this specification, OpenAI.
- **Fallback Provider**: The provider attempted when the primary provider call fails or produces unusable output. In this specification, Anthropic.
- **Deterministic Fallback**: Existing rule-based semantic output already available in the codebase and used when all provider attempts fail or are disabled.
- **Provider Failure**: Transport error, timeout, rate limit, authentication error, or any other failure that prevents obtaining a valid structured response.
- **Invalid Output**: A provider response that is syntactically malformed, schema-incompatible, outside allowed vocabularies, or contains privacy leakage.
- **Graph Run**: A single LangGraph execution for one conversation payload.
- **Structured Output Contract**: The strict JSON-shaped output expected from the model and validated before persistence.
- **Provider Trace Metadata**: Runtime metadata recorded for auditability, such as provider used, model used, attempt count, and fallback path.

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: The implementation shall use LangChain for model client invocation and structured output handling.
- **REQ-002**: The implementation shall use LangGraph to orchestrate the OpenAI-first, Anthropic-second provider flow.
- **REQ-003**: The implementation shall attempt OpenAI before Anthropic for every eligible enrichment request.
- **REQ-004**: The implementation shall invoke Anthropic only when the OpenAI attempt fails or returns invalid output.
- **REQ-005**: The implementation shall preserve the current deterministic fallback when neither provider yields an accepted result.
- **REQ-006**: The implementation shall keep the conversation enrichment artifact contract backward-compatible with the current published parquet schema unless explicitly versioned.
- **REQ-007**: The implementation shall record provider execution metadata sufficient to identify which provider and model produced or failed a classification.
- **REQ-008**: The implementation shall validate provider output against the existing controlled vocabulary before persistence.
- **REQ-009**: The implementation shall support runtime disabling of external provider calls through configuration or environment variables.
- **REQ-010**: The implementation shall keep cache behavior based on `llm_input_hash`, `prompt_version`, and model identity.
- **REQ-011**: The implementation shall ensure that provider fallback does not cause full pipeline failure when both providers are unavailable.
- **REQ-012**: The implementation shall expose test seams that allow provider calls to be stubbed or mocked without network access.
- **REQ-013**: The implementation shall avoid persisting raw credentials, raw prompts with secrets, or unmasked user PII.
- **REQ-014**: The implementation shall support bounded timeout and retry behavior per provider attempt.
- **REQ-015**: The implementation shall allow prompt and model selection to be configured independently for OpenAI and Anthropic.

- **SEC-001**: Provider API keys shall be read only from environment variables or equivalent secret injection mechanisms.
- **SEC-002**: Structured explanations returned by providers shall continue to pass privacy leakage validation before persistence.
- **SEC-003**: Runtime logs and reports shall not include raw conversation text beyond the already sanitized payload and approved diagnostics.

- **CON-001**: The operational agent modules shall remain independent from the new provider runtime.
- **CON-002**: LangGraph shall orchestrate only the enrichment request lifecycle, not the whole medallion pipeline.
- **CON-003**: OpenAI shall be the architecturally preferred provider in the first implementation.
- **CON-004**: Anthropic shall be the only provider fallback in the first implementation cycle.
- **CON-005**: The first implementation shall not require asynchronous distributed execution infrastructure.
- **CON-006**: The implementation shall remain compatible with repository-local execution through `venv/bin/python`.
- **CON-007**: The cache key semantics shall not depend on volatile runtime metadata such as retry count or provider latency.

- **GUD-001**: Keep the provider runtime isolated in a dedicated module instead of embedding client logic directly in `build_conversation_enrichment`.
- **GUD-002**: Prefer explicit graph nodes such as `prepare_request`, `call_openai`, `validate_openai_output`, `call_anthropic`, `validate_anthropic_output`, and `emit_deterministic_fallback`.
- **GUD-003**: Prefer provider-specific model configuration in one place to keep tests and future extension simpler.
- **GUD-004**: Prefer machine-readable execution metadata over prose diagnostics.
- **GUD-005**: Keep the graph small and linear for the first implementation; avoid speculative branches not required by current behavior.

- **PAT-001**: Recommended runtime pattern: cache check -> graph execution -> schema validation -> accepted provider result or deterministic fallback -> parquet persistence.
- **PAT-002**: Recommended provider pattern: OpenAI structured call -> validation -> Anthropic structured call -> validation -> deterministic fallback.
- **PAT-003**: Recommended code organization pattern: `conversation_enrichment.py` owns orchestration entry, a dedicated provider runtime module owns LangChain/LangGraph integration, and tests mock provider nodes.

## 4. Interfaces & Data Contracts

### 4.1 Runtime Module Boundary

Recommended module boundary:

| Module | Responsibility |
| --- | --- |
| `src/pipeline/conversation_enrichment.py` | Assemble payloads, perform cache checks, call provider runtime, apply deterministic fallback, persist final rows |
| `src/pipeline/llm_runtime.py` | Own LangChain model clients, LangGraph flow, provider config, provider metadata mapping |
| `src/pipeline/quality.py` | Validate persisted output and provider-related metadata |

### 4.2 Configuration Contract

The implementation shall support runtime configuration through compiled plan fields and environment variables.

Recommended compiled plan shape:

```json
{
  "llm": {
    "enabled": true,
    "prompt_version": "v1",
    "timeout_seconds": 20,
    "max_retries": 1,
    "providers": {
      "openai": {
        "enabled": true,
        "role": "primary",
        "model": "gpt-5-mini"
      },
      "anthropic": {
        "enabled": true,
        "role": "fallback",
        "model": "claude-sonnet"
      }
    }
  }
}
```

Required environment variables:

| Variable | Required When | Purpose |
| --- | --- | --- |
| `PIPELINE_ENABLE_LLM_ENRICHMENT` | Always | Global runtime enable switch |
| `OPENAI_API_KEY` | OpenAI enabled | OpenAI authentication |
| `ANTHROPIC_API_KEY` | Anthropic enabled | Anthropic authentication |

Optional environment variables:

| Variable | Purpose |
| --- | --- |
| `PIPELINE_LLM_TIMEOUT_SECONDS` | Override provider call timeout |
| `PIPELINE_LLM_MAX_RETRIES` | Override retry count |
| `PIPELINE_LLM_OPENAI_MODEL` | Override OpenAI model |
| `PIPELINE_LLM_ANTHROPIC_MODEL` | Override Anthropic model |

### 4.3 LangGraph State Contract

The graph state shall be explicit and serializable.

```json
{
  "conversation_id": "conv_00012847",
  "lead_key": "lead_a1b2c3",
  "payload": {
    "prompt_version": "v1",
    "conversation_statistics": {},
    "messages": []
  },
  "attempted_providers": [],
  "provider_result": null,
  "provider_errors": [],
  "selected_provider": null,
  "selected_model": null,
  "validation_error": null,
  "final_status": null
}
```

Required graph state fields:

| Field | Type | Description |
| --- | --- | --- |
| `conversation_id` | `string` | Stable conversation key |
| `lead_key` | `string` | Stable lead key |
| `payload` | `object` | Sanitized LLM request payload |
| `attempted_providers` | `array[string]` | Ordered list of provider names attempted |
| `provider_result` | `object/null` | Candidate structured output |
| `provider_errors` | `array[object]` | Provider failure or validation history |
| `selected_provider` | `string/null` | Accepted provider name |
| `selected_model` | `string/null` | Accepted model name |
| `validation_error` | `string/null` | Last validation error |
| `final_status` | `string/null` | `success`, `provider_error`, `invalid_output`, or `fallback` |

### 4.4 Provider Runtime Interface

Recommended Python interface:

```python
def run_conversation_enrichment_graph(
    payload: dict[str, object],
    compiled_plan: dict[str, object],
) -> dict[str, object]:
    """
    Returns:
      {
        "status": "success" | "provider_error" | "invalid_output" | "fallback",
        "provider_name": "openai" | "anthropic" | None,
        "model_name": str | None,
        "output": dict[str, object] | None,
        "validation_error": str | None,
        "provider_errors": list[dict[str, object]],
        "attempted_providers": list[str]
      }
    """
```

### 4.5 Structured Output Contract

The accepted provider output shall remain compatible with the current validation layer.

```json
{
  "sentiment_label": "neutro",
  "sentiment_confidence_band": "moderado",
  "intent_stage": "cotacao_ativa",
  "persona_profile": "cotador_comparador",
  "audience_segment": "oferta_competitiva",
  "price_objection_intensity": "forte",
  "competitor_pressure_level": "alta",
  "commercial_urgency_signal": "moderada",
  "recommended_next_action": "reforcar_diferenciais_e_retirar_objecao_preco",
  "explanation_short": "Lead compara concorrente e responde com interesse ativo."
}
```

No provider response shall be accepted unless:

- all required fields are present
- all categorical values belong to the allowed vocabularies
- `explanation_short` is privacy-safe
- the output is parseable without heuristic repair

### 4.6 Persistence Contract Extension

The persisted artifact shall remain `data/silver/silver_conversations_llm.parquet`.

Additional recommended metadata columns:

| Column | Type | Description |
| --- | --- | --- |
| `provider_name` | `string/null` | `openai`, `anthropic`, or `null` when deterministic fallback only |
| `provider_attempt_count` | `integer` | Number of providers attempted in the graph run |
| `provider_error_summary` | `string/null` | Short machine-readable summary of provider failures |

These columns are recommended additions. If added, they shall be validated and documented in the same implementation cycle.

## 5. Acceptance Criteria

- **AC-001**: Given `PIPELINE_ENABLE_LLM_ENRICHMENT=0`, when the pipeline runs, then no external provider call shall be attempted and deterministic fallback shall be used.
- **AC-002**: Given OpenAI is enabled and returns a valid structured response, when enrichment runs, then Anthropic shall not be called and the persisted row shall identify OpenAI as the accepted provider.
- **AC-003**: Given OpenAI fails with a provider error, when enrichment runs, then Anthropic shall be attempted in the same graph run.
- **AC-004**: Given OpenAI returns schema-invalid output and Anthropic returns valid output, when enrichment runs, then the persisted row shall use Anthropic output and record that OpenAI failed validation.
- **AC-005**: Given both providers fail or return invalid output, when enrichment runs, then deterministic fallback shall be persisted and Bronze, Silver, and Gold publication shall still complete.
- **AC-006**: Given a conversation payload with unchanged `llm_input_hash`, `prompt_version`, and effective model identity, when the pipeline reruns, then the provider graph shall not be executed and the cached result shall be reused.
- **AC-007**: Given OpenAI credentials are missing while OpenAI is configured as enabled, when enrichment runs, then the graph shall treat OpenAI as a provider failure and continue to Anthropic if Anthropic is eligible.
- **AC-008**: Given Anthropic credentials are also missing, when enrichment runs, then the system shall emit deterministic fallback instead of failing the pipeline.
- **AC-009**: Given a provider returns `explanation_short` containing unmasked PII, when validation runs, then the output shall be rejected as invalid and the graph shall continue to fallback behavior.
- **AC-010**: Given the graph accepts a provider response, when the row is persisted, then the result shall remain compatible with the current `silver_conversations_llm` schema and Gold consolidation flow.

## 6. Test Automation Strategy

- **Test Levels**: Unit, integration, and pipeline-level acceptance tests.
- **Frameworks**: `pytest` executed through `venv/bin/python`.
- **Test Data Management**: Synthetic conversation fixtures and in-memory provider stubs shall be used; tests shall not require network access.
- **CI/CD Integration**: All provider integration tests shall run as part of `venv/bin/python -m pytest -q`.
- **Coverage Requirements**: Cover graph routing, provider selection, invalid output rejection, deterministic fallback, cache reuse, configuration gating, and privacy validation.
- **Performance Testing**: Add bounded tests ensuring one graph run per uncached conversation and zero graph runs per cache hit.

Required automated test groups:

- Unit tests for provider config resolution.
- Unit tests for LangGraph state transitions.
- Unit tests for OpenAI success without Anthropic call.
- Unit tests for OpenAI failure followed by Anthropic success.
- Unit tests for dual-provider failure followed by deterministic fallback.
- Unit tests for invalid structured output rejection.
- Unit tests for credential-missing behavior.
- Integration tests for `build_conversation_enrichment` using mocked provider runtime.
- Pipeline tests ensuring the operator still publishes artifacts when providers are unavailable.

## 7. Rationale & Context

The repository already contains the correct architectural split: deterministic pipeline execution plus optional semantic enrichment. The missing piece is a concrete provider runtime.

LangChain is appropriate because the implementation needs:

- model client adapters for multiple providers
- prompt and structured output composition
- testable model invocation seams

LangGraph is appropriate because the execution path is a small but explicit workflow:

1. prepare state
2. call OpenAI
3. validate output
4. conditionally call Anthropic
5. emit accepted output or deterministic fallback

This workflow is more explicit and auditable as a graph than as deeply nested conditional code. It also leaves room for future provider telemetry or human-review checkpoints without moving orchestration into the main pipeline operator.

OpenAI is primary because the requested implementation prioritizes it operationally. Anthropic is the only provider fallback in the first cycle to keep the graph and tests constrained. Deterministic fallback remains the terminal safety mechanism because the technical test values a living, resilient pipeline over best-effort semantic enrichment.

## 8. Dependencies & External Integrations

### External Systems

- **EXT-001**: OpenAI API - primary structured inference provider over HTTPS.
- **EXT-002**: Anthropic API - fallback structured inference provider over HTTPS.

### Third-Party Services

- **SVC-001**: LLM provider service with stable API authentication, bounded latency, and JSON-capable model responses.

### Infrastructure Dependencies

- **INF-001**: Secure environment variable injection for provider API keys.
- **INF-002**: Outbound network access from the runtime environment to provider APIs.

### Data Dependencies

- **DAT-001**: Sanitized conversation payloads assembled from `silver_messages` runtime data.

### Technology Platform Dependencies

- **PLT-001**: LangChain-compatible Python runtime in the repository virtual environment.
- **PLT-002**: LangGraph-compatible Python runtime in the repository virtual environment.

### Compliance Dependencies

- **COM-001**: Repository privacy rules requiring masked PII in published artifacts.
- **COM-002**: AGENTS.md repository rule requiring documentation updates when operational behavior changes.

## 9. Examples & Edge Cases

Primary provider success:

```json
{
  "status": "success",
  "provider_name": "openai",
  "model_name": "gpt-5-mini",
  "attempted_providers": ["openai"],
  "validation_error": null
}
```

Primary provider invalid output, fallback provider success:

```json
{
  "status": "success",
  "provider_name": "anthropic",
  "model_name": "claude-sonnet",
  "attempted_providers": ["openai", "anthropic"],
  "provider_errors": [
    {
      "provider": "openai",
      "kind": "invalid_output",
      "detail": "invalid_intent_stage:<empty>"
    }
  ]
}
```

Both providers unavailable:

```json
{
  "status": "fallback",
  "provider_name": null,
  "model_name": null,
  "attempted_providers": ["openai", "anthropic"],
  "provider_errors": [
    {
      "provider": "openai",
      "kind": "provider_error",
      "detail": "authentication_failed"
    },
    {
      "provider": "anthropic",
      "kind": "provider_error",
      "detail": "timeout"
    }
  ]
}
```

Edge cases:

- OpenAI returns non-JSON text.
- OpenAI returns JSON with valid shape but disallowed category values.
- Anthropic succeeds after OpenAI rate limiting.
- Both providers are enabled but only Anthropic credentials are present.
- Cache hit occurs after a previous Anthropic-produced success.
- Prompt version changes while payload content remains identical.

## 10. Validation Criteria

The implementation is compliant only if all of the following are true:

- LangChain is used for provider client invocation.
- LangGraph is used for the provider routing workflow.
- OpenAI is attempted before Anthropic for eligible requests.
- Anthropic is attempted only after OpenAI failure or invalid output.
- Deterministic fallback remains the terminal safe path.
- `venv/bin/python -m pytest -q` passes with network-free automated tests.
- The `README.md` and any relevant operational documentation are updated in the same implementation cycle.
- No credentials or unmasked PII are persisted in artifacts, reports, or tests.

## 11. Related Specifications / Further Reading

- [spec/spec-architecture-llm-conversation-enrichment.md](/home/lucas/projects/lucas54neves/namastex-test/spec/spec-architecture-llm-conversation-enrichment.md)
- [README.md](/home/lucas/projects/lucas54neves/namastex-test/README.md)
- LangChain documentation
- LangGraph documentation
- OpenAI API documentation
- Anthropic API documentation
