---
title: Architecture Specification for LLM Output Normalization Before Runtime Validation
version: 1.0
date_created: 2026-04-21
last_updated: 2026-04-21
owner: Lucas Neves
tags: [architecture, llm, normalization, validation, pipeline]
---

# Introduction

This specification defines the first corrective change required to make the optional LLM conversation enrichment path operationally useful under real provider responses.

The immediate issue is that the OpenAI provider can return semantically correct values that do not match the pipeline's controlled vocabulary exactly, causing runtime validation to reject the provider output and fall back to deterministic enrichment. The goal of this specification is to introduce a canonical normalization layer between raw provider output and strict vocabulary validation.

## 1. Purpose & Scope

This specification defines the requirements, constraints, interfaces, and validation rules for normalizing LLM provider outputs into the pipeline's accepted vocabulary before the output is validated and published.

Scope:

- Add an explicit normalization step for provider output fields used by `silver_conversations_llm`.
- Preserve strict validation after normalization.
- Define canonical mappings, fallback behavior, observability, and tests for normalized values.
- Cover the current OpenAI drift observed in real execution, including values such as `negative` and `skeptical` for sentiment-like fields.

Out of scope:

- Changing the published column schema of `silver_conversations_llm.parquet`.
- Replacing strict vocabulary validation with fuzzy acceptance.
- Redesigning the deterministic fallback logic.
- Fixing third-party provider credentials or network availability.
- Expanding the LLM prompt to request additional fields beyond the current contract.

Intended audience:

- Engineers implementing the runtime correction
- Reviewers validating LLM enrichment behavior
- Future maintainers responsible for provider compatibility and vocabulary evolution

Assumptions:

- The controlled vocabulary defined by the compiled pipeline spec remains the canonical runtime contract.
- Provider outputs may vary lexically even when semantically close to the target labels.
- The runtime must remain safe when provider output is unrecognized, incomplete, or privacy-unsafe.

## 2. Definitions

- **Controlled Vocabulary**: The set of allowed values for semantic fields compiled from `config/pipeline_spec.json`.
- **Provider Output**: The raw JSON returned by the LLM provider before runtime normalization.
- **Normalization Layer**: The deterministic transformation that converts provider output into canonical pipeline values before validation.
- **Canonical Value**: A value that exactly matches the controlled vocabulary accepted by the validator.
- **Synonym Mapping**: A deterministic mapping from non-canonical provider terms to canonical pipeline values.
- **Semantic Drift**: A provider response that is conceptually close to the target meaning but lexically outside the accepted vocabulary.
- **Hard Validation**: The current strict membership check against the compiled vocabulary after normalization.
- **Unmappable Value**: A provider value that cannot be safely converted to a canonical value.
- **Fallback Enrichment**: The deterministic enrichment path used when provider output cannot be accepted.

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: The runtime shall introduce a normalization step that executes after provider output is received and before `_validate_llm_response()` performs strict vocabulary checks.
- **REQ-002**: The normalization step shall be deterministic and side-effect free.
- **REQ-003**: The normalization step shall preserve the current published schema and shall not add required output fields to `silver_conversations_llm`.
- **REQ-004**: The runtime shall continue to reject unmappable or unsafe provider output after normalization.
- **REQ-005**: Normalization rules shall be field-aware. A synonym accepted for one field shall not automatically be accepted for another field.
- **REQ-006**: The normalization layer shall support at least the fields `sentiment_label`, `sentiment_confidence_band`, `intent_stage`, `persona_profile`, `audience_segment`, `price_objection_intensity`, `competitor_pressure_level`, `commercial_urgency_signal`, and `recommended_next_action`.
- **REQ-007**: The runtime shall normalize values using a canonical procedure that includes trimming, case normalization, accent normalization, and separator normalization before synonym lookup.
- **REQ-008**: The runtime shall explicitly map currently observed OpenAI drift values when a safe canonical interpretation exists.
- **REQ-009**: If a normalized value is still not part of the controlled vocabulary, the runtime shall treat the provider output as invalid and use deterministic fallback exactly as it does today.
- **REQ-010**: The runtime shall make normalization decisions observable in diagnostics or tests, even if the published artifact contract remains unchanged.
- **REQ-011**: The normalization logic shall not weaken privacy validation for `explanation_short`.
- **REQ-012**: The runtime shall keep the compiled spec as the source of truth for allowed values; normalization may only map into values already accepted by the compiled plan.
- **REQ-013**: The correction shall improve real-provider success rate without changing deterministic fallback semantics for provider timeouts, malformed JSON, or missing fields.

- **CON-001**: The correction shall not replace exact validation with substring matching, embedding similarity, or heuristic acceptance of arbitrary free text.
- **CON-002**: The correction shall not silently coerce ambiguous values when multiple canonical mappings are plausible.
- **CON-003**: The correction shall not change the persisted fallback rows into partial-success rows.
- **CON-004**: The correction shall remain compatible with both OpenAI and Anthropic providers, even if only one provider is currently working.
- **CON-005**: The correction shall use the repository-local Python environment and test commands defined in `AGENTS.md`.

- **GUD-001**: Prefer an explicit per-field synonym table over implicit generic matching.
- **GUD-002**: Prefer conservative mappings that preserve business semantics over aggressive normalization.
- **GUD-003**: Keep canonical normalization logic centralized so new provider drift can be added without scattering rules across validators and publishers.
- **GUD-004**: Record the distinction between raw provider drift and canonical accepted values in tests, even if the runtime does not persist the raw response.

- **PAT-001**: Recommended execution flow: provider JSON -> normalize output -> validate normalized output -> publish success or fallback deterministically.
- **PAT-002**: Recommended mapping pattern: normalize raw token -> field-specific synonym lookup -> strict membership check against compiled vocabulary.

## 4. Interfaces & Data Contracts

### 4.1 Relevant Files

| Path | Role | Required Outcome |
| --- | --- | --- |
| `src/pipeline/conversation_enrichment.py` | Runtime LLM enrichment and validation path | Must normalize provider output before strict validation |
| `src/pipeline/llm_runtime.py` | Provider prompt and invocation graph | Must remain compatible with normalized output handling |
| `config/pipeline_spec.json` | Controlled vocabularies | Remains canonical source of allowed values |
| `tests/test_llm_runtime.py` | Provider/runtime tests | Must cover normalization and invalid-output boundaries |
| `tests/test_jobs.py` or dedicated integration tests | Runtime integration coverage | Must verify normalized provider output can produce `success` |
| `README.md` | Runtime behavior documentation | Should reflect that provider drift is normalized before validation if documentation is updated as part of implementation |

### 4.2 Normalization Interface Contract

The runtime shall define a normalization interface conceptually equivalent to:

```python
def normalize_llm_output(
    response: dict[str, Any],
    compiled_plan: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    ...
```

Expected behavior:

- Input:
  - raw provider JSON
  - compiled runtime vocabulary
- Output:
  - normalized payload candidate
  - zero or more normalization decision records for diagnostics/tests

The exact function name may differ, but the behavior shall remain equivalent.

### 4.3 Canonical Normalization Procedure

For each supported categorical field, the runtime shall apply the following steps in order:

1. Read the raw provider value as string.
2. Trim surrounding whitespace.
3. Normalize case to lowercase.
4. Remove accents/diacritics for matching.
5. Replace punctuation and repeated separators with a stable token form.
6. Apply field-specific synonym mapping.
7. Validate the resulting canonical value against the compiled vocabulary.

The published value must be the canonical pipeline label, not the raw provider label.

### 4.4 Minimum Required Field Mappings

The implementation shall support at least the following initial normalization cases:

| Field | Raw provider value example | Canonical value |
| --- | --- | --- |
| `sentiment_label` | `negative` | `negativo` |
| `sentiment_label` | `neutral` | `neutro` |
| `sentiment_label` | `positive` | `positivo` |
| `sentiment_confidence_band` | `weak` | `fraco` |
| `sentiment_confidence_band` | `medium` | `moderado` |
| `sentiment_confidence_band` | `strong` | `forte` |

The implementation may add more mappings, but it shall not map terms whose meaning is ambiguous relative to the canonical contract.

### 4.5 Explicit Non-Mapping Rule

The runtime shall not automatically map semantically ambiguous values such as:

- `skeptical`
- `mixed`
- `frustrated`
- `warm`
- `urgent-ish`

These values must remain invalid unless the specification is intentionally expanded to define an exact canonical interpretation.

### 4.6 Validation Flow Contract

The corrected runtime flow shall behave as follows:

```text
provider response
-> required field presence check
-> canonical normalization
-> strict vocabulary validation
-> privacy validation for explanation_short
-> publish success
or
-> deterministic fallback with invalid_output status
```

### 4.7 Observability Contract

The implementation shall expose enough evidence to prove normalization occurred. Acceptable mechanisms include one or more of the following:

- unit-test-visible helper outputs
- structured debug metadata kept in-memory during runtime
- explicit test fixtures that verify raw input to canonical output transformation

Persisting raw provider output in published parquet files is not required and should be avoided unless specifically justified.

## 5. Acceptance Criteria

- **AC-001**: Given a provider output with `sentiment_label = "negative"`, When normalization runs, Then the value shall become `negativo` before strict validation.
- **AC-002**: Given a provider output with `sentiment_label = "neutral"`, When normalization runs, Then the value shall become `neutro`.
- **AC-003**: Given a provider output with `sentiment_confidence_band = "strong"`, When normalization runs, Then the value shall become `forte`.
- **AC-004**: Given a provider output containing only canonical values after normalization, When runtime validation executes, Then the row shall be published with `inference_status = "success"`.
- **AC-005**: Given a provider output with `sentiment_label = "skeptical"`, When normalization runs, Then the value shall remain invalid and the runtime shall publish deterministic fallback with `inference_status = "invalid_output"` or equivalent fallback path semantics already used by the runtime.
- **AC-006**: Given a provider output with canonical semantic values but privacy-unsafe `explanation_short`, When validation executes, Then the row shall still be rejected.
- **AC-007**: Given current deterministic fallback logic, When provider output is unmappable, Then fallback output semantics shall remain unchanged from the current runtime contract.
- **AC-008**: Given real provider output that previously failed only because of canonical vocabulary drift, When the corrected runtime is executed, Then at least one normalization-covered case shall pass validation without changing the published schema.

## 6. Test Automation Strategy

- **Test Levels**: Unit, runtime integration, provider-contract simulation
- **Frameworks**: `pytest` via `venv/bin/python -m pytest`
- **Test Data Management**: Use synthetic provider payload fixtures for normalization tests and reuse the existing small pipeline fixtures for integration behavior
- **CI/CD Integration**: The normalization tests shall run in the default repository test suite without requiring live provider access
- **Coverage Requirements**: Cover positive mappings, non-mappings, missing fields, and privacy validation after normalization
- **Performance Testing**: Not required for this correction

Required automated checks:

- A unit test shall verify field-specific normalization for accepted synonyms.
- A unit test shall verify that ambiguous values remain invalid.
- A runtime-level test shall verify that normalized output can produce `inference_status = "success"`.
- A runtime-level test shall verify that unmappable values still produce deterministic fallback.
- Existing deterministic fallback tests shall remain green.

## 7. Rationale & Context

Real provider execution showed that the OpenAI path is already capable of returning structured JSON, but the runtime rejects some values because the provider uses English or otherwise non-canonical labels while the pipeline expects Portuguese controlled vocabulary terms.

Examples observed during validation:

- `invalid_sentiment_label:negative`
- `invalid_sentiment_label:skeptical`

These failures are not equivalent.

- `negative` is a lexical compatibility problem and should be normalized to `negativo`.
- `skeptical` is not a canonical pipeline label and does not have a safe one-to-one mapping, so it should remain invalid.

Without a normalization layer, the runtime treats both cases the same and loses valid semantic signal that could have been accepted safely. This creates unnecessary fallback usage and makes the optional LLM path appear broken even when the provider response is structurally useful.

The correct fix is not to relax validation globally. The correct fix is to insert a deterministic canonicalization layer that preserves the strict contract while handling known lexical drift.

## 8. Dependencies & External Integrations

### External Systems

- **EXT-001**: Local filesystem - Required for reading the versioned spec and writing parquet/report artifacts

### Third-Party Services

- **SVC-001**: Optional LLM providers - Supply raw semantic classifications that must be normalized before publication

### Infrastructure Dependencies

- **INF-001**: Repository-local virtual environment at `venv/` - Required for implementation and validation commands

### Data Dependencies

- **DAT-001**: Provider JSON payloads - Required as runtime input to the normalization layer

### Technology Platform Dependencies

- **PLT-001**: Python runtime via `venv/bin/python` - Required by runtime and tests

### Compliance Dependencies

- **COM-001**: Existing masking and privacy rules for `explanation_short` - Must remain enforced after normalization

## 9. Examples & Edge Cases

```json
{
  "raw_provider_output": {
    "sentiment_label": "negative",
    "sentiment_confidence_band": "strong",
    "intent_stage": "cotacao_ativa",
    "persona_profile": "lead_engajado_com_dados",
    "audience_segment": "close_comercial",
    "price_objection_intensity": "forte",
    "competitor_pressure_level": "leve",
    "commercial_urgency_signal": "moderada",
    "recommended_next_action": "enviar_cotacao_objetiva",
    "explanation_short": "Lead quer comparar opções e pediu cotação."
  },
  "normalized_output": {
    "sentiment_label": "negativo",
    "sentiment_confidence_band": "forte",
    "intent_stage": "cotacao_ativa",
    "persona_profile": "lead_engajado_com_dados",
    "audience_segment": "close_comercial",
    "price_objection_intensity": "forte",
    "competitor_pressure_level": "leve",
    "commercial_urgency_signal": "moderada",
    "recommended_next_action": "enviar_cotacao_objetiva",
    "explanation_short": "Lead quer comparar opções e pediu cotação."
  }
}
```

```text
Edge Case 1: Ambiguous sentiment
- Raw value: skeptical
- No safe one-to-one canonical mapping exists
- Expected behavior: reject as invalid_output and use deterministic fallback

Edge Case 2: Mixed-language payload
- Raw values: neutral, strong, cotacao_ativa
- Expected behavior: normalize only the non-canonical fields and accept the row if all final values are canonical

Edge Case 3: Canonical semantics but privacy leak
- Raw explanation contains unmasked sensitive data
- Expected behavior: reject even if all categorical fields normalize successfully
```

## 10. Validation Criteria

- The runtime normalizes supported lexical drift before strict validation.
- Mapped values always land inside the compiled vocabulary.
- Ambiguous or unsafe values remain invalid.
- Deterministic fallback behavior remains unchanged for invalid or failed provider outputs.
- At least one previously observed lexical drift case is converted into a successful runtime output in automated tests.

## 11. Related Specifications / Further Reading

- [spec-architecture-llm-provider-runtime-integration.md](/home/lucas/projects/lucas54neves/namastex-test/spec/spec-architecture-llm-provider-runtime-integration.md)
- [spec-architecture-llm-conversation-enrichment.md](/home/lucas/projects/lucas54neves/namastex-test/spec/spec-architecture-llm-conversation-enrichment.md)
- [src/pipeline/conversation_enrichment.py](/home/lucas/projects/lucas54neves/namastex-test/src/pipeline/conversation_enrichment.py)
- [src/pipeline/llm_runtime.py](/home/lucas/projects/lucas54neves/namastex-test/src/pipeline/llm_runtime.py)
- [config/pipeline_spec.json](/home/lucas/projects/lucas54neves/namastex-test/config/pipeline_spec.json)
