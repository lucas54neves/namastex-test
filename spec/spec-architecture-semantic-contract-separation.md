---
title: Architecture Specification for Deterministic and LLM Semantic Contract Separation
version: 1.0
date_created: 2026-04-22
last_updated: 2026-04-22
owner: Lucas Neves
tags: [architecture, pipeline, gold, validation, llm, semantics, contracts]
---

# Introduction

This specification defines the architectural direction required to make Gold semantic publication, deterministic feature engineering, optional LLM enrichment, and runtime validation mutually consistent.

The immediate issue is not a single implementation bug. The current runtime mixes deterministic evidence, per-conversation semantic inference, lead-level consolidation, and strict validation as if they were all the same contract. This causes false validation failures, obscures true data-quality problems, and makes the optional LLM path appear unreliable even when it is behaving within its intended semantic role.

The goal of this specification is to establish an explicit separation between deterministic fields and interpretive fields, define provenance-aware contracts for each class, and require validation rules that are aligned with the actual derivation model.

## 1. Purpose & Scope

This specification defines requirements, constraints, interfaces, and validation rules for a provenance-aware semantic architecture in which deterministic and interpretive outputs are produced, consolidated, published, and validated under distinct contracts.

Scope:

- Define two explicit semantic classes for published Gold attributes: deterministic and interpretive.
- Define how message-level evidence is transformed into deterministic lead-level features.
- Define how optional LLM or fallback conversation-level semantics are consolidated into lead-level interpretive outputs.
- Define provenance and observability requirements so operators can identify where a published semantic field came from.
- Define validation models appropriate for each semantic class.
- Define the minimum schema and testing changes required to support the separation.
- Define migration expectations for existing Silver, `silver_conversations_llm`, and Gold behavior.

Out of scope:

- Replacing the medallion architecture.
- Introducing external orchestration, online feature stores, or non-local infrastructure.
- Requiring live provider execution for baseline local runtime validation.
- Replacing deterministic feature extraction with LLM inference for low-level message signals.
- Defining provider prompt wording in detail beyond what is needed to support the semantic contract.

Intended audience:

- Engineers evolving the pipeline architecture
- Reviewers evaluating correctness of semantic publication
- Future maintainers responsible for runtime validation and artifact contracts

Assumptions:

- The repository-local baseline path remains valid with LLM enrichment disabled.
- Optional provider-based enrichment remains valuable for interpretive fields, but must not redefine the contract of deterministic fields.
- Gold is the primary analytical output and must remain operator-auditable.

## 2. Definitions

- **Deterministic Field**: A published attribute whose value is derived from explicit, reproducible transformation rules over source or intermediate data.
- **Interpretive Field**: A published attribute whose value may depend on conversation-level inference, heuristic summarization, semantic judgment, or LLM classification.
- **Evidence Signal**: A low-level observable feature extracted from messages, such as `quoted_price`, `mentions_competitor`, or `positive_tone_hits`.
- **Conversation Semantic Output**: The structured enrichment row in `silver_conversations_llm.parquet` for one `conversation_id`.
- **Lead Semantic Consolidation**: The process that converts one or more conversation semantic outputs into a single Gold row per `lead_key`.
- **Provenance**: Metadata that indicates how a semantic field or semantic family was derived, such as deterministic, fallback, provider-based, or mixed.
- **Deterministic Validation**: Validation that checks exact reproducibility from source evidence.
- **Interpretive Validation**: Validation that checks domain membership, internal consistency, completeness, and coarse evidence compatibility rather than exact deterministic equality.
- **Semantic Family**: A logically grouped set of related fields, such as sentiment, competitor pressure, urgency, segmentation, or price objection.
- **Baseline Local Runtime**: The repository-local execution path using the checked-in source and local virtual environment, with no dependency on external providers.

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: The architecture shall classify every published Gold semantic field as either deterministic or interpretive.
- **REQ-002**: A field shall not be validated under deterministic equality rules unless its derivation contract is deterministic.
- **REQ-003**: Gold publication shall preserve a clear distinction between low-level evidence fields and higher-level semantic interpretation fields.
- **REQ-004**: Message-level feature engineering in `silver_messages` shall remain deterministic and auditable.
- **REQ-005**: Low-level message signals used as business evidence shall be direction-aware when the business meaning depends on who said the message.
- **REQ-006**: Price objection and urgency evidence intended to represent lead intent shall not be inferred solely from outbound seller prompts or follow-up language.
- **REQ-007**: The runtime shall not use LLM inference to replace deterministic extraction of low-level evidence fields such as `quoted_price`, `contains_*`, `mentions_*`, or lexical tone-hit counters.
- **REQ-008**: `silver_conversations_llm` shall remain the conversation-level semantic contract for interpretive enrichment, whether the row originated from provider output or deterministic fallback.
- **REQ-009**: Gold lead-level interpretive semantics shall be consolidated from conversation-level semantics using one explicit, documented consolidation contract.
- **REQ-010**: The runtime shall surface provenance sufficient to distinguish at least provider-based, deterministic fallback, and mixed lead-level semantic outcomes.
- **REQ-011**: Deterministic Gold fields shall be derived from deterministic evidence only and shall remain reproducible without provider access.
- **REQ-012**: Interpretive Gold fields shall be validated using interpretive validation rules rather than deterministic recomputation from unrelated proxy features.
- **REQ-013**: The validation layer shall not require lexical tone counters alone to perfectly explain interpretive sentiment labels generated by conversation-level semantics.
- **REQ-014**: If an interpretive field depends on evidence that is not directly published as a deterministic feature, the runtime shall expose enough provenance or supporting metadata to make the result reviewable.
- **REQ-015**: The architecture shall define a stable policy for each semantic family indicating whether it belongs to the deterministic contract or the interpretive contract.
- **REQ-016**: The implementation shall update documentation in the same work cycle when semantic field meaning, provenance, or validation behavior changes materially.
- **REQ-017**: The baseline local runtime with LLM disabled shall continue to succeed using deterministic paths for required publication and validation.
- **REQ-018**: Provider-based enrichment shall remain optional and shall enhance interpretive fields without redefining deterministic evidence semantics.
- **REQ-019**: The architecture shall support safe future expansion of interpretive fields without forcing deterministic validators to accept semantic guesswork.

- **SEM-001**: The semantic family policy shall, at minimum, classify `quoted_price_*`, `positive_tone_hits`, `negative_tone_hits`, `contains_*`, `mentions_*`, `primary_competitor`, and other directly computed evidence as deterministic.
- **SEM-002**: The semantic family policy shall, at minimum, classify `conversation_sentiment_label`, `conversation_sentiment_support`, `intent_stage`, `persona_profile`, `audience_segment`, and `recommended_next_action` as interpretive.
- **SEM-003**: The semantic family policy shall explicitly decide whether `price_objection_intensity`, `commercial_urgency_signal`, and `competitor_pressure_level` are deterministic or interpretive. The chosen policy shall be implemented consistently in transforms, consolidation, publication, and validation.
- **SEM-004**: If a semantic family is classified as deterministic, the provider or fallback enrichment may compute an advisory value for debugging, but the published Gold value shall come from deterministic derivation.
- **SEM-005**: If a semantic family is classified as interpretive, the validation layer shall not enforce exact equality with deterministic heuristics that are not the field's source of truth.

- **VAL-001**: Deterministic validation shall verify exact agreement with the deterministic derivation inputs and rules.
- **VAL-002**: Interpretive validation shall verify allowed vocabulary, non-null constraints where applicable, privacy safety, internal consistency, and coarse evidence compatibility.
- **VAL-003**: Interpretive validation may reject impossible or contradictory outputs, but shall not reject reasonable outputs solely because a simpler deterministic heuristic would have chosen another label.
- **VAL-004**: Validation reports shall identify whether a failure came from deterministic contract violation, interpretive contract violation, privacy violation, or provenance inconsistency.

- **CON-001**: The correction shall use the repository-local virtual environment commands defined in `AGENTS.md`.
- **CON-002**: The baseline runtime path shall not require network access.
- **CON-003**: The correction shall not move specs outside `spec/`.
- **CON-004**: The correction shall not commit `spec/` files unless explicitly requested by the user.
- **CON-005**: The correction shall preserve auditability of published artifacts.
- **CON-006**: The correction shall avoid introducing broad free-text acceptance in validation as a substitute for proper semantic contracts.

- **GUD-001**: Prefer deterministic rules for observable evidence and LLM-based inference for interpretation.
- **GUD-002**: Prefer provenance-aware schemas over implicit assumptions about where a field came from.
- **GUD-003**: Keep evidence extraction, conversation semantics, lead consolidation, and validation as separate responsibilities with well-defined interfaces.
- **GUD-004**: When a semantic family is ambiguous, choose one source of truth and make the other representation explicitly secondary.
- **GUD-005**: Prefer narrower, business-meaningful inbound-oriented evidence for lead intent over generic message-level keyword hits across both directions.

- **PAT-001**: Recommended architecture pattern: source messages -> deterministic evidence extraction -> optional conversation semantics -> lead consolidation -> provenance-aware Gold publication -> contract-aware validation.
- **PAT-002**: Recommended validation pattern: deterministic fields use exact derivation checks; interpretive fields use consistency and provenance checks.
- **PAT-003**: Recommended migration pattern: classify field families -> add provenance fields -> align publication -> align validators -> update tests and docs.

## 4. Interfaces & Data Contracts

### 4.1 Relevant Files

| Path | Role | Required Outcome |
| --- | --- | --- |
| `src/pipeline/transforms.py` | Deterministic message and lead feature engineering | Must remain source of truth for deterministic evidence |
| `src/pipeline/conversation_enrichment.py` | Conversation-level semantic enrichment and lead-level consolidation | Must publish and consolidate interpretive semantics under an explicit contract |
| `src/pipeline/quality.py` | Runtime validation contract | Must validate deterministic and interpretive fields differently |
| `src/pipeline/publication.py` | Publication-safe artifacts | Must remain compatible with provenance-aware Gold output |
| `src/pipeline/jobs.py` | End-to-end orchestration | Must assemble artifacts under the new semantic contract |
| `tests/test_quality.py` | Validation coverage | Must cover both deterministic and interpretive validation models |
| `tests/test_transforms.py` | Deterministic derivation coverage | Must cover corrected direction-aware evidence extraction |
| `tests/test_jobs.py` | Runtime contract coverage | Must verify baseline success and provenance-aware Gold publication |
| `README.md` | Operator-facing documentation | Must describe semantic field classes and runtime expectations |

### 4.2 Gold Semantic Class Contract

The architecture shall maintain a machine-readable classification for every Gold semantic field.

Minimum required families:

| Semantic family | Example fields | Contract class |
| --- | --- | --- |
| Evidence and factual aggregates | `quoted_price_min`, `quoted_price_max`, `quoted_price_last`, `positive_tone_hits`, `negative_tone_hits`, `contains_*`, `mentions_*`, `primary_competitor` | Deterministic |
| Sentiment | `conversation_sentiment_label`, `conversation_sentiment_support` | Interpretive |
| Segmentation | `intent_stage`, `persona_profile`, `audience_segment` | Interpretive |
| Next action | `recommended_next_action` if published in Gold later | Interpretive |
| Commercial severity families | `price_objection_intensity`, `commercial_urgency_signal`, `competitor_pressure_level` | Explicitly classified by implementation policy |

The implementation shall encode the classification in one canonical location rather than scattering it across validators.

### 4.3 Provenance Contract

Gold shall expose provenance at a granularity sufficient for runtime diagnostics and validation. Acceptable forms include dedicated columns or a structured metadata column, but the semantics shall be equivalent to the following model:

```text
semantic_source_family = {
  deterministic: field published from deterministic derivation
  llm_provider: field published from provider-based conversation semantics
  deterministic_fallback: field published from conversation fallback semantics
  mixed: lead-level field consolidated from multiple conversation semantic sources
}
```

Minimum provenance requirements:

- The runtime shall identify whether lead-level interpretive output depends on provider rows, fallback rows, or both.
- The runtime shall expose whether a deterministic field was published from deterministic derivation rather than copied from conversation enrichment.
- Validation and operator diagnostics shall be able to reference provenance when reporting failures.

### 4.4 Deterministic Evidence Contract

The deterministic evidence layer shall include direction-aware business intent signals when the meaning depends on speaker role.

Conceptual examples:

```python
{
    "price_objection_signal_inbound": bool,
    "price_objection_signal_outbound": bool,
    "urgency_signal_inbound": int,
    "urgency_signal_outbound": int,
    "competitor_mention_inbound": bool,
    "competitor_mention_outbound": bool,
}
```

The exact published shape may differ, but the deterministic layer shall preserve enough information to avoid conflating seller prompts with lead intent.

### 4.5 Conversation Semantic Contract

`silver_conversations_llm` shall remain the canonical conversation-level semantic artifact.

The contract shall guarantee:

- one row per `conversation_id`
- vocabulary-controlled categorical fields
- explicit indication of provider success or deterministic fallback
- privacy-safe explanation fields
- stable fields for sentiment, segmentation, and other interpretive semantics

The contract may include additional provenance fields if needed to support Gold consolidation and validation.

### 4.6 Gold Publication Contract

Gold publication shall follow one of the following models for each semantic family:

1. **Deterministic publication model**
   The published Gold field is computed directly from deterministic lead-level evidence.
2. **Interpretive publication model**
   The published Gold field is consolidated from `silver_conversations_llm` conversation semantics.

No field may be published using one model and validated as if it came from the other.

### 4.7 Validation Contract

The validation layer shall support at least two semantic validation modes.

Deterministic mode:

- exact recomputation from deterministic inputs
- equality check against published field
- failure indicates transform, publication, or data corruption issue

Interpretive mode:

- vocabulary membership
- nullability and required-field checks
- internal consistency checks
- provenance compatibility checks
- coarse evidence compatibility checks

Examples of acceptable interpretive checks:

- `conversation_sentiment_support = forte` must not appear with a missing or invalid sentiment label
- `persona_profile` and `audience_segment` shall belong to a compatible mapping family
- `conversation_sentiment_label = negativo` may be allowed with zero lexical negative hits if the conversation semantic source is interpretive and not contradictory to published evidence

### 4.8 Migration Contract for Commercial Severity Families

The implementation shall make one explicit decision for `price_objection_intensity`, `commercial_urgency_signal`, and `competitor_pressure_level`.

Allowed policy options:

| Policy | Meaning | Consequence |
| --- | --- | --- |
| `deterministic_gold` | Gold publishes deterministic lead-level values | Validation remains exact; conversation enrichment values are advisory only |
| `interpretive_gold` | Gold publishes conversation-level interpretive consolidation | Validation becomes interpretive; deterministic heuristics become support signals only |
| `dual_contract` | Gold publishes both deterministic and interpretive variants | Consumers choose explicitly; validation applies per field class |

The implementation shall choose one policy and encode it consistently.

Recommended policy:

- `price_objection_intensity`: deterministic or dual-contract
- `commercial_urgency_signal`: deterministic or dual-contract
- `competitor_pressure_level`: deterministic or dual-contract

This recommendation reflects the current evidence that low-level signal quality and contract mismatch, not absence of LLM, are the main failure sources for these families.

## 5. Acceptance Criteria

- **AC-001**: Given a published Gold field classified as deterministic, When validation runs, Then the field shall be validated by exact deterministic recomputation from its source evidence.
- **AC-002**: Given a published Gold field classified as interpretive, When validation runs, Then the field shall not fail solely because a deterministic heuristic would have chosen a different label.
- **AC-003**: Given a lead with only outbound mentions of `cotacao`, When deterministic lead-intent evidence is computed, Then the lead shall not be forced into high price objection solely from those outbound messages.
- **AC-004**: Given a lead with only outbound commercial follow-up urgency language, When deterministic lead-intent evidence is computed, Then the lead shall not be forced into elevated lead urgency solely from that outbound language.
- **AC-005**: Given `conversation_sentiment_label` published from conversation-level interpretive semantics, When lexical tone counters are zero, Then validation may still pass if the label is valid, provenance is interpretive, and no contradictory deterministic evidence exists.
- **AC-006**: Given a Gold semantic family configured as deterministic, When provider-based conversation enrichment returns another value, Then the provider value shall not silently replace the deterministic Gold field.
- **AC-007**: Given a Gold semantic family configured as interpretive, When the field is published, Then provenance shall indicate whether the result came from provider output, deterministic fallback, or mixed consolidation.
- **AC-008**: Given the baseline local runtime with LLM disabled, When `venv/bin/python scripts/run_pipeline.py --force` is executed, Then the pipeline shall complete successfully under the corrected semantic contract.
- **AC-009**: Given the corrected architecture, When `venv/bin/python -m pytest -q` is executed, Then the full test suite shall pass with explicit coverage for both validation modes.
- **AC-010**: Given updated semantic publication rules, When documentation is reviewed, Then `README.md` shall describe the distinction between deterministic and interpretive semantics.

## 6. Test Automation Strategy

- **Test Levels**: Unit, contract, integration, repository-local runtime
- **Frameworks**: `pytest` via `venv/bin/python -m pytest`
- **Test Data Management**: Use synthetic DataFrame fixtures for precise evidence and validation cases, plus the repository-local Bronze source for runtime verification
- **CI/CD Integration**: The baseline test suite shall remain runnable without live provider access
- **Coverage Requirements**: Add explicit regression coverage for direction-aware evidence extraction, semantic family classification, provenance handling, deterministic validation, and interpretive validation
- **Performance Testing**: Not required for this architectural correction

Required automated checks:

- A transform-level test shall prove that outbound seller mentions of `cotacao` do not by themselves create lead price-objection evidence under the corrected deterministic policy.
- A transform-level test shall prove that outbound urgency prompts do not by themselves create elevated lead urgency under the corrected deterministic policy.
- A transform-level test shall verify that inbound lead price objections still create deterministic objection evidence.
- A contract-level test shall verify the configured semantic family classification for all Gold semantic fields.
- A validation test shall verify deterministic fields are checked by exact recomputation.
- A validation test shall verify interpretive fields are checked by vocabulary, consistency, and provenance rather than exact equality to lexical heuristics.
- A runtime integration test shall verify baseline success with LLM disabled.
- A runtime integration test shall verify provider or fallback provenance is reflected in semantic publication where required.

Recommended validation commands:

```bash
venv/bin/python scripts/run_pipeline.py --force
venv/bin/python -m pytest -q
venv/bin/python -m pytest tests/test_transforms.py -q
venv/bin/python -m pytest tests/test_quality.py -q
venv/bin/python -m pytest tests/test_jobs.py -q
```

## 7. Rationale & Context

Repository-local triage showed that the current Gold validation failures are not explained by a single class of defect.

Observed failure sources:

- direction-insensitive deterministic evidence extraction that overcounts seller prompts as lead-intent evidence
- lead-level deterministic recomputation that disagrees with conversation-level semantic consolidation
- interpretive sentiment outputs being validated as if they were direct lexical derivatives

These failure types imply an architectural mismatch:

- deterministic evidence extraction is appropriate for observable facts and bounded heuristics
- conversation enrichment is appropriate for interpretive semantics
- Gold currently mixes both without exposing the boundary clearly
- validation assumes one semantic model when publication sometimes used another

The correct architectural response is not to replace the whole pipeline with LLM inference. That would reduce auditability and move avoidable low-level logic into a probabilistic component.

The correct response is:

1. keep evidence extraction deterministic
2. make conversation semantics explicitly interpretive
3. expose provenance
4. validate each class under its own contract

This architecture preserves operator trust, baseline local reproducibility, and safe future expansion of provider-based enrichment.

## 8. Dependencies & External Integrations

### External Systems

- **EXT-001**: Local Bronze parquet source - required for repository-local runtime and end-to-end contract verification

### Third-Party Services

- **SVC-001**: Optional LLM provider runtime - required only for provider-based interpretive enrichment, not for baseline deterministic publication

### Infrastructure Dependencies

- **INF-001**: Local filesystem persistence for Bronze, Silver, Gold, reports, and state artifacts

### Data Dependencies

- **DAT-001**: WhatsApp conversation source in Bronze format - required input for deterministic evidence extraction and conversation-level semantic assembly

### Technology Platform Dependencies

- **PLT-001**: Repository-local Python runtime in `venv/` - mandatory execution environment for tests and pipeline runs

### Compliance Dependencies

- **COM-001**: Existing privacy publication and anti-leak controls - must remain enforced for both deterministic and interpretive artifacts

## 9. Examples & Edge Cases

```text
Example A: Outbound quotation language without lead objection

Messages:
- outbound: "pra eu montar a cotacao preciso saber qual eh o carro"
- outbound: "a cotacao ficou em R$ 900/ano"
- inbound: "[sticker]"

Expected deterministic outcome:
- quoted price evidence may exist
- lead price objection evidence shall not be elevated solely from outbound quotation wording

Expected interpretive outcome:
- conversation sentiment may still be classified by fallback or provider contract if supported by the conversation

Example B: Interpretive sentiment with low lexical evidence

Messages:
- inbound: "nao eh o momento pra mim, desculpa"
- inbound: "vou deixar pra depois"

Expected deterministic outcome:
- lexical negative-hit counters may remain low or zero if patterns are narrow

Expected interpretive outcome:
- conversation sentiment may still be `negativo` or `neutro` depending on the semantic contract

Validation expectation:
- interpretive validation checks validity and consistency, not exact equality with lexical counters

Example C: Mixed lead-level provenance

Lead conversations:
- one conversation enriched via provider
- one conversation enriched via deterministic fallback

Expected Gold outcome:
- interpretive field may be published from consolidated semantics
- provenance shall indicate `mixed` or equivalent

Example D: Dual-contract commercial severity

If the implementation chooses `dual_contract`:
- `price_objection_intensity_rule_based` is deterministic and exactly validated
- `price_objection_intensity_interpretive` is conversation-based and interpretively validated
- consumers choose explicitly which contract they need
```

## 10. Validation Criteria

- The implementation shall produce a canonical classification for Gold semantic fields.
- The implementation shall expose provenance sufficient for semantic debugging and contract-aware validation.
- Deterministic evidence extraction shall be direction-aware where business meaning depends on speaker role.
- Deterministic and interpretive validation logic shall be distinct and tested independently.
- No Gold field shall be published under one semantic contract and validated under another.
- The local baseline runtime shall complete successfully after the architectural correction is implemented.
- Repository documentation shall describe the resulting semantic field model and operator expectations.

## 11. Related Specifications / Further Reading

- [spec/spec-architecture-llm-conversation-enrichment.md](/home/lucas/projects/lucas54neves/namastex-test/spec/spec-architecture-llm-conversation-enrichment.md)
- [spec/spec-architecture-llm-output-normalization-contract.md](/home/lucas/projects/lucas54neves/namastex-test/spec/spec-architecture-llm-output-normalization-contract.md)
- [spec/spec-architecture-pipeline-validation-contract-alignment.md](/home/lucas/projects/lucas54neves/namastex-test/spec/spec-architecture-pipeline-validation-contract-alignment.md)
- [README.md](/home/lucas/projects/lucas54neves/namastex-test/README.md)
