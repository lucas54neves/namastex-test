---
title: Architecture Specification for LLM Conversation Enrichment in the Medallion Pipeline
version: 1.0
date_created: 2026-04-21
last_updated: 2026-04-21
owner: Lucas Neves
tags: [architecture, design, process, data, llm, pipeline]
---

# Introduction

This specification defines the target architecture for introducing Large Language Model (LLM) enrichment into the existing medallion pipeline while preserving deterministic pipeline operation, auditability, and privacy controls.

The intended result is a hybrid pipeline in which deterministic logic continues to own ingestion, masking, validation, aggregation safety, and remediation, while an LLM is used only for high-value semantic classification at the conversation level.

## 1. Purpose & Scope

This specification defines requirements, constraints, interfaces, and validation rules for adding a conversation-level semantic enrichment stage to the current pipeline.

Scope:

- Add a new conversation-level enrichment artifact derived from published Silver history.
- Use an LLM to classify subjective or ambiguous commercial semantics from sanitized conversation context.
- Preserve deterministic Gold publication and deterministic operational control.
- Document the rationale for choosing conversation-level inference over message-level or lead-level inference.

Out of scope:

- Replacing the current deterministic Bronze, Silver, or Gold foundations.
- Allowing the LLM to auto-apply structural pipeline changes.
- Introducing unrestricted natural-language outputs into published analytical artifacts.
- Selecting a specific LLM vendor as an architectural hard dependency.

Intended audience:

- Repository maintainers
- Engineers implementing the pipeline evolution
- Reviewers evaluating architectural adherence to the technical test

Assumptions:

- The repository already contains deterministic Bronze, Silver, Gold, validation, state, and operational agent modules.
- Sensitive raw fields must remain absent from published Silver and Gold artifacts.
- The new LLM stage must be optional, observable, and safe to disable.

## 2. Definitions

- **Bronze**: Controlled raw replication of the source parquet file.
- **Silver**: Cleaned, masked, and normalized data products derived from Bronze.
- **Gold**: Analytical dataset aggregated by `lead_key`.
- **LLM**: Large Language Model used for semantic classification.
- **Conversation Enrichment**: Structured semantic inference persisted with one row per `conversation_id`.
- **Deterministic Fallback**: Rule-based behavior used when LLM inference is disabled, invalid, or unavailable.
- **Conversation Assembler**: Component that prepares sanitized, ordered conversation payloads for inference.
- **Enrichment Store**: Persisted artifact holding structured LLM outputs and inference metadata.
- **Prompt Version**: Explicit version identifier for the prompt template and expected output schema.
- **Input Hash**: Stable hash representing the exact LLM input payload used for cache and replay control.
- **PII**: Personally Identifiable Information.

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: The system shall introduce a new intermediate semantic enrichment artifact with one row per `conversation_id`.
- **REQ-002**: The enrichment artifact shall be produced from sanitized conversation payloads assembled from published Silver-safe data or equivalent masked in-memory data.
- **REQ-003**: The LLM shall return a strict structured output compatible with a versioned schema.
- **REQ-004**: The system shall persist inference metadata including `llm_input_hash`, `prompt_version`, `llm_model`, `inference_status`, and processing timestamp.
- **REQ-005**: The Gold layer shall aggregate conversation-level semantic outputs into lead-level analytical classifications using deterministic consolidation rules.
- **REQ-006**: The system shall support incremental reprocessing and avoid recomputing unchanged conversations when input hash, prompt version, and model are unchanged.
- **REQ-007**: The system shall support deterministic fallback for semantically classified fields when LLM inference fails, is disabled, or returns invalid output.
- **REQ-008**: The system shall expose validation checks for schema conformity, allowed categorical values, missing enrichment coverage, and privacy leakage in the intermediate artifact.
- **REQ-009**: The system shall keep LLM usage outside the critical control path for remediation and structural pipeline mutation.
- **REQ-010**: The system shall allow the LLM enrichment stage to be disabled without breaking Bronze, Silver, Gold, or the operational agent.

- **CON-001**: The architecture shall preserve deterministic runtime operation for ingestion, masking, validation, fallback, and publication.
- **CON-002**: The architecture shall not require the LLM to orchestrate the pipeline, decide approval state, or mutate the pipeline specification automatically.
- **CON-003**: Published artifacts shall not expose raw `sender_name`, `sender_phone`, `message_body`, or equivalent raw PII.
- **CON-004**: Free-text rationale returned by the LLM shall be bounded, optional, and privacy-scanned before persistence.
- **CON-005**: The enrichment stage shall be idempotent for the same input hash, prompt version, and model.

- **GUD-001**: Prefer deterministic extraction for objective signals such as contact patterns, plates, explicit competitor names, timestamps, and message counts.
- **GUD-002**: Use the LLM only for subjective or context-dependent classifications such as persona, intent stage, urgency, objection intensity, and semantic sentiment.
- **GUD-003**: Keep the LLM output taxonomy finite and versioned to simplify validation and downstream aggregation.
- **GUD-004**: Preserve auditability by storing enough metadata to replay or explain any conversation-level classification.

- **PAT-001**: The recommended pattern is deterministic preprocessing -> conversation assembly -> structured LLM inference -> schema validation -> persisted enrichment -> deterministic Gold aggregation.
- **PAT-002**: The recommended failure pattern is partial degradation, not full pipeline failure, when only LLM enrichment is unavailable.

## 4. Interfaces & Data Contracts

### 4.1 Conversation Assembler Input Contract

The Conversation Assembler consumes ordered Silver message history grouped by `conversation_id`.

Required fields:

| Field | Type | Description |
| --- | --- | --- |
| `conversation_id` | `string` | Stable conversation identifier |
| `lead_key` | `string` | Stable lead identifier |
| `timestamp` | `datetime` | Ordered event timestamp |
| `direction` | `string` | `inbound` or `outbound` |
| `message_type` | `string` | Message modality |
| `message_body_masked` | `string` | Sanitized free text |
| `mentions_competitor` | `boolean` | Deterministic competitor signal |
| `mentions_sinistro` | `boolean` | Deterministic claim-history signal |
| `mentions_vehicle` | `boolean` | Deterministic vehicle signal |
| `quoted_price` | `number/null` | Deterministic extracted price if available |
| `metadata_response_time_sec` | `number/null` | Response-time context if available |

### 4.2 LLM Request Contract

The LLM request payload shall be machine-oriented and self-contained.

```json
{
  "conversation_id": "conv_00012847",
  "lead_key": "lead_a1b2c3",
  "prompt_version": "v1",
  "conversation_statistics": {
    "message_count": 14,
    "inbound_messages": 6,
    "outbound_messages": 8,
    "has_vehicle_signal": true,
    "has_competitor_signal": true,
    "has_sinistro_signal": false,
    "latest_quoted_price": 2450.0,
    "avg_response_time_sec": 187.0
  },
  "messages": [
    {
      "timestamp": "2026-02-05T09:12:43Z",
      "direction": "outbound",
      "message_type": "text",
      "text": "Oi XXXXX, tudo bem?"
    },
    {
      "timestamp": "2026-02-05T09:15:50Z",
      "direction": "inbound",
      "message_type": "text",
      "text": "A porto fez mais barato, mas quero comparar"
    }
  ]
}
```

### 4.3 LLM Response Contract

The LLM shall return a strict JSON object. No prose-only response is allowed.

```json
{
  "sentiment_label": "neutro",
  "sentiment_confidence_band": "moderada",
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

### 4.4 Conversation Enrichment Store Contract

Recommended persisted artifact: `data/silver/silver_conversations_llm.parquet`

Required columns:

| Column | Type | Description |
| --- | --- | --- |
| `conversation_id` | `string` | Primary conversation key |
| `lead_key` | `string` | Lead foreign key |
| `llm_input_hash` | `string` | Stable hash of request payload |
| `prompt_version` | `string` | Prompt/schema version |
| `llm_model` | `string` | Model identifier |
| `inference_status` | `string` | `success`, `fallback`, `invalid_output`, `provider_error`, `disabled`, or `skipped_cache_hit` |
| `processed_at_utc` | `datetime` | Processing timestamp |
| `sentiment_label` | `string` | Allowed sentiment label |
| `sentiment_confidence_band` | `string` | Allowed support/confidence band |
| `intent_stage` | `string` | Allowed intent stage |
| `persona_profile` | `string` | Allowed persona |
| `audience_segment` | `string` | Allowed audience |
| `price_objection_intensity` | `string` | Allowed objection intensity |
| `competitor_pressure_level` | `string` | Allowed competitor pressure |
| `commercial_urgency_signal` | `string` | Allowed urgency signal |
| `recommended_next_action` | `string` | Finite next-action taxonomy |
| `explanation_short` | `string` | Optional short bounded explanation |
| `fallback_reason` | `string/null` | Reason when deterministic fallback is used |
| `validation_error` | `string/null` | Output validation error if any |

### 4.5 Gold Consolidation Contract

Gold shall remain one row per `lead_key`.

Deterministic consolidation rules:

| Field | Rule |
| --- | --- |
| `intent_stage` | Most recent non-null conversation classification |
| `persona_profile` | Most frequent conversation classification, tie broken by most recent |
| `audience_segment` | Derived from dominant persona and highest-value commercial signal |
| `price_objection_intensity` | Maximum severity observed across conversations |
| `competitor_pressure_level` | Maximum severity observed across conversations |
| `commercial_urgency_signal` | Most recent non-null, tie broken by maximum severity |
| `conversation_sentiment_label` | Dominant conversation-level label, tie broken by recency |

## 5. Acceptance Criteria

- **AC-001**: Given a conversation with unchanged input hash, prompt version, and model, when the pipeline reruns, then the enrichment stage shall reuse the cached result instead of invoking the LLM again.
- **AC-002**: Given a valid sanitized conversation payload, when the LLM returns a schema-compliant response, then the system shall persist one enrichment row for that `conversation_id`.
- **AC-003**: Given an invalid LLM response, when schema validation runs, then the system shall mark `inference_status` as `invalid_output` and apply deterministic fallback for affected fields.
- **AC-004**: Given the LLM provider is disabled or unavailable, when the pipeline runs, then Bronze, Silver, and Gold publication shall still complete with fallback semantics and explicit status reporting.
- **AC-005**: Given multiple conversations for the same lead, when Gold is built, then the lead-level semantic fields shall be consolidated using deterministic rules defined in this specification.
- **AC-006**: Given a published conversation enrichment artifact, when privacy validation scans it, then no raw PII patterns shall be present in persisted text fields.
- **AC-007**: Given a new prompt version, when the pipeline reruns, then conversations affected by the version change shall be eligible for re-enrichment.
- **AC-008**: Given LLM enrichment is disabled by configuration, when the pipeline executes, then operational reports shall record the disabled state without raising a runtime failure.

## 6. Test Automation Strategy

- **Test Levels**: Unit, integration, and pipeline-level acceptance tests.
- **Frameworks**: `pytest` executed through the repository virtual environment.
- **Test Data Management**: Synthetic parquet fixtures and minimal conversation samples shall be created inside test-specific temporary directories.
- **CI/CD Integration**: Tests shall remain executable via `venv/bin/python -m pytest -q` and targeted module runs.
- **Coverage Requirements**: New logic shall be covered for request assembly, cache behavior, schema validation, fallback behavior, Gold consolidation, and privacy checks.
- **Performance Testing**: Add lightweight tests for cache-hit execution paths and bounded payload generation to prevent accidental prompt bloat.

Required test groups:

- Unit tests for conversation payload assembly.
- Unit tests for response schema validation.
- Unit tests for deterministic fallback mapping.
- Unit tests for Gold semantic consolidation rules.
- Integration tests for partial LLM failure without full pipeline failure.
- Integration tests for cache reuse across reruns.
- Privacy tests for the enrichment artifact.

## 7. Rationale & Context

The technical test emphasizes both persistent pipeline management and analytical creativity. The repository already demonstrates strong deterministic operation through validation contracts, fallback handling, state persistence, and automated reruns. The largest gap relative to the test prompt is semantic analytical richness, not infrastructure control.

Three enrichment granularities were considered:

1. Message-level inference
2. Conversation-level inference
3. Lead-level inference

Conversation-level inference is the recommended architecture for these reasons:

- It matches the business unit described by the problem statement. A sales negotiation unfolds across a conversation, not a single message.
- It preserves enough context to infer sentiment, intent, objections, urgency, and persona more accurately than message-level classification.
- It avoids the cost and noise explosion of invoking an LLM for every message.
- It avoids over-merging multiple independent interactions from the same lead, which would happen with lead-level-only inference.
- It provides a stable entity for caching, replay, audit, and later deterministic aggregation into Gold.

The selected hybrid model is also preferable to a fully LLM-driven pipeline:

- Deterministic extraction remains superior for objective patterns already handled well by regex and rules.
- Deterministic publication and remediation reduce operational risk and improve explainability.
- The LLM is used where it adds the most value: ambiguous semantic interpretation.

This approach directly strengthens the test dimensions of creativity, classification quality, and practical agent design while preserving the operational strengths already present in the repository.

## 8. Dependencies & External Integrations

### External Systems

- **EXT-001**: Raw parquet Bronze source - upstream conversation dataset consumed by the pipeline.

### Third-Party Services

- **SVC-001**: LLM provider service - required for structured semantic classification of sanitized conversation payloads.

### Infrastructure Dependencies

- **INF-001**: Local filesystem artifact storage - required for persisted Bronze, Silver, Gold, reports, state, and the new enrichment artifact.
- **INF-002**: Configuration and secret management - required to enable, disable, and authenticate the optional LLM provider.

### Data Dependencies

- **DAT-001**: `silver_messages.parquet` - required ordered message history and deterministic features per conversation.
- **DAT-002**: `silver_leads.parquet` - required lead-level context for Gold consolidation.

### Technology Platform Dependencies

- **PLT-001**: Python runtime and repository virtual environment - required execution platform for pipeline jobs and tests.
- **PLT-002**: Parquet-compatible data processing stack - required for artifact IO and schema validation.

### Compliance Dependencies

- **COM-001**: Repository privacy and masking policy - all persisted artifacts must avoid raw sensitive fields and avoid PII leakage in generated text.
- **COM-002**: Auditability requirement from the technical test - pipeline behavior must remain inspectable and reproducible.

## 9. Examples & Edge Cases

```json
{
  "edge_case": "provider_unavailable",
  "behavior": {
    "inference_status": "provider_error",
    "fallback_reason": "llm_provider_unreachable",
    "gold_publication": "continues_with_fallback"
  }
}
```

```json
{
  "edge_case": "same_lead_multiple_conversations",
  "conversation_classifications": [
    {
      "conversation_id": "conv_old",
      "intent_stage": "pesquisa_mercado",
      "competitor_pressure_level": "alta"
    },
    {
      "conversation_id": "conv_new",
      "intent_stage": "cotacao_ativa",
      "competitor_pressure_level": "leve"
    }
  ],
  "expected_gold_resolution": {
    "intent_stage": "cotacao_ativa",
    "competitor_pressure_level": "alta"
  }
}
```

```json
{
  "edge_case": "invalid_output",
  "llm_response": {
    "persona_profile": "cliente_confuso_nao_catalogado"
  },
  "expected_behavior": {
    "inference_status": "invalid_output",
    "validation_error": "persona_profile_not_allowed",
    "fallback_applied": true
  }
}
```

## 10. Validation Criteria

- The implementation shall persist the conversation enrichment artifact with the required columns.
- The implementation shall validate categorical outputs against finite allowed sets.
- The implementation shall not leak raw sensitive values in persisted enrichment text fields.
- The implementation shall preserve successful Gold publication when the LLM stage is disabled or fails partially.
- The implementation shall demonstrate incremental behavior through cache reuse keyed by input hash, prompt version, and model.
- The implementation shall document the architectural rationale for conversation-level inference.
- The implementation shall maintain compatibility with the repository virtual-environment test commands.

## 11. Related Specifications / Further Reading

- [`README.md`](/home/lucas/projects/lucas54neves/namastex-test/README.md)
- [`docs/technical-test-data-ai-engineering.md`](/home/lucas/projects/lucas54neves/namastex-test/docs/technical-test-data-ai-engineering.md)
- [`docs/data-dictionary-data-ai-engineering.md`](/home/lucas/projects/lucas54neves/namastex-test/docs/data-dictionary-data-ai-engineering.md)
- [`config/pipeline_spec.json`](/home/lucas/projects/lucas54neves/namastex-test/config/pipeline_spec.json)
