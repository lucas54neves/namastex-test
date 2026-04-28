---
title: Architecture Specification for Gold Audience Segment Contract Correction
version: 1.0
date_created: 2026-04-22
last_updated: 2026-04-22
owner: Lucas Neves
tags: [architecture, gold, semantics, validation, audience, segmentation]
---

# Introduction

This specification defines the correction required for the `Gold` lead-level semantic consolidation rule that currently rewrites `audience_segment` based on commercial severity heuristics.

The observed production failure is precise: `silver_conversations_llm` produced a coherent interpretive combination of `persona_profile = lead_frio` and `audience_segment = nutricao_basica`, but `Gold` publication rewrote `audience_segment` to `oferta_competitiva` when `competitor_pressure_level = alta`. This created an invalid semantic pair and caused the runtime validation failure `gold.persona_audience_semantic_alignment`.

The goal of this specification is to restore semantic contract integrity by ensuring that `audience_segment` remains an interpretive field whose published value is derived from the conversation semantic contract rather than from deterministic commercial severity heuristics.

## 1. Purpose & Scope

This specification defines the required correction for `Gold` publication and validation behavior related to `audience_segment`, `persona_profile`, and their interaction with deterministic commercial severity fields.

Scope:

- Define the invalid rule that must be removed or replaced.
- Define the correct source of truth for `audience_segment` in `Gold`.
- Define publication and validation expectations for `persona_profile` and `audience_segment`.
- Define how deterministic fields such as `competitor_pressure_level` may inform diagnostics without mutating interpretive segmentation output.
- Define the minimum test and documentation updates required for this correction.

Out of scope:

- Reworking the full LLM enrichment contract.
- Reclassifying `competitor_pressure_level` as interpretive.
- Redesigning the full persona or audience taxonomy.
- Changing the baseline medallion architecture or execution model.
- Introducing new provider dependencies or external services.

Intended audience:

- Engineers implementing `Gold` semantic consolidation
- Reviewers validating semantic contract integrity
- Maintainers responsible for runtime validation and operator-facing documentation

Assumptions:

- `persona_profile` and `audience_segment` are interpretive `Gold` fields.
- `competitor_pressure_level`, `price_objection_intensity`, and `commercial_urgency_signal` remain deterministic `Gold` fields.
- `silver_conversations_llm` is the conversation-level source of truth for interpretive semantics.
- Runtime validation should reject internally contradictory `Gold` semantic combinations.

## 2. Definitions

- **Audience Segment**: A lead-level interpretive segmentation field published in `Gold`, such as `nutricao_basica`, `oferta_competitiva`, `retencao_pos_sinistro`, or `close_comercial`.
- **Persona Profile**: A lead-level interpretive semantic field representing the dominant lead persona, such as `lead_frio` or `cotador_comparador`.
- **Semantic Alignment**: The rule that `persona_profile` and `audience_segment` must remain compatible under the published taxonomy mapping.
- **Commercial Severity Field**: A deterministic `Gold` field representing a measurable business condition, such as `competitor_pressure_level`.
- **Interpretive Consolidation**: The process that derives one lead-level semantic output from one or more conversation-level semantic rows in `silver_conversations_llm`.
- **Semantic Rewrite Rule**: Any post-consolidation rule that mutates the value of an interpretive field using a different semantic family as its source.
- **Contract Violation**: A published output that does not respect the field classification, source of truth, or allowed internal consistency rules.

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: `audience_segment` in `Gold` shall be published from the interpretive conversation semantic consolidation contract, not from deterministic commercial severity heuristics.
- **REQ-002**: The runtime shall not rewrite a valid consolidated `audience_segment` solely because `competitor_pressure_level` is `alta`.
- **REQ-003**: `persona_profile` and `audience_segment` shall remain semantically aligned under one explicit taxonomy mapping.
- **REQ-004**: If `persona_profile` and `audience_segment` are both sourced from interpretive consolidation, any additional post-processing rule shall preserve their valid pairing.
- **REQ-005**: Deterministic commercial severity fields may be used for diagnostics, analytics, or future secondary recommendations, but shall not silently override interpretive segmentation fields unless the contract explicitly classifies them as the same semantic family.
- **REQ-006**: `Gold` publication shall preserve the original interpretive `audience_segment` from `silver_conversations_llm` consolidation when that value is non-null and valid.
- **REQ-007**: If `silver_conversations_llm` provides an invalid or missing `audience_segment`, the fallback behavior shall remain explicit, documented, and provenance-compatible.
- **REQ-008**: Validation shall continue to reject impossible `persona_profile` and `audience_segment` combinations in `Gold`.
- **REQ-009**: The correction shall ensure that a lead with `persona_profile = lead_frio` is not published with `audience_segment = oferta_competitiva` unless that pairing is intentionally allowed by the canonical taxonomy.
- **REQ-010**: The correction shall preserve `competitor_pressure_level` as an independently published deterministic field.
- **REQ-011**: The implementation shall update repository documentation in the same work cycle to describe the corrected interpretive source of truth for `audience_segment`.
- **REQ-012**: The implementation shall include regression coverage using the local project virtual environment described in `AGENTS.md`.

- **VAL-001**: Validation shall treat `persona_profile` and `audience_segment` as an aligned interpretive family.
- **VAL-002**: Validation shall report a failure if `Gold` publication mutates one field of the pair without preserving mapping coherence.
- **VAL-003**: Validation shall not require commercial severity to deterministically imply a specific audience segment.

- **CON-001**: The specification file shall remain in `spec/`.
- **CON-002**: This spec shall not be included in a commit unless explicitly requested by the user.
- **CON-003**: The correction shall not require network access or live provider execution.
- **CON-004**: The correction shall preserve publication-safe behavior and privacy constraints already enforced by the pipeline.

- **GUD-001**: Prefer preserving the direct output of the interpretive consolidation contract over deriving a replacement from a different semantic family.
- **GUD-002**: If deterministic evidence should influence segmentation strategy, encode that influence inside the interpretive contract itself rather than as an after-the-fact overwrite.
- **GUD-003**: Keep semantic family boundaries explicit: severity fields describe measurable pressure; segmentation fields describe interpretive targeting.

- **PAT-001**: Recommended correction pattern: consolidate interpretive fields -> validate coherence -> publish unchanged interpretive pair -> publish deterministic severity fields separately.
- **PAT-002**: Recommended regression pattern: use one real-world-like lead with `competitor_pressure_level = alta` and `persona_profile = lead_frio` to prove the runtime preserves `audience_segment = nutricao_basica`.

## 4. Interfaces & Data Contracts

### 4.1 Relevant Files

| Path | Role | Required Outcome |
| --- | --- | --- |
| `src/pipeline/conversation_enrichment.py` | Lead-level semantic consolidation | Must not rewrite `audience_segment` from commercial severity |
| `src/pipeline/transforms.py` | `Gold` publication assembly | Must preserve corrected interpretive semantic output |
| `src/pipeline/quality.py` | Runtime validation | Must continue enforcing valid `persona_profile` and `audience_segment` alignment |
| `tests/test_transforms.py` | Consolidation regression coverage | Must prove `audience_segment` is not overwritten by `competitor_pressure_level` |
| `tests/test_quality.py` | Validation regression coverage | Must verify alignment failures are real contract failures, not side effects of publication rewrites |
| `README.md` | Operator-facing documentation | Must describe that `audience_segment` remains interpretive |

### 4.2 Current Invalid Behavior

Observed problematic pattern:

```text
dominant_persona -> derive default audience from persona
if audience == nutricao_basica and competitor_pressure_level == alta:
    audience = oferta_competitiva
```

This pattern is invalid because:

- `audience_segment` is an interpretive field
- `competitor_pressure_level` is a deterministic field
- the rewrite changes the meaning of the interpretive family after consolidation
- the rewrite can create a `persona_profile` and `audience_segment` pair that violates the canonical taxonomy

### 4.3 Required Gold Publication Contract

The `Gold` publication contract for the segmentation family shall follow this rule:

| Field | Contract class | Source of truth | Allowed post-processing |
| --- | --- | --- | --- |
| `persona_profile` | Interpretive | Consolidated `silver_conversations_llm` | Normalization only if taxonomy-preserving |
| `audience_segment` | Interpretive | Consolidated `silver_conversations_llm` | Normalization only if taxonomy-preserving |
| `competitor_pressure_level` | Deterministic | Deterministic lead evidence | Independent publication only |

Mandatory publication behavior:

- `audience_segment` shall be published as the consolidated interpretive value when present and valid.
- `competitor_pressure_level` shall not alter `audience_segment` during `Gold` assembly.
- If a fallback interpretive path is used, the fallback pair `persona_profile` and `audience_segment` shall be produced together from the same interpretive contract.

### 4.4 Taxonomy Alignment Contract

Canonical mapping:

```text
lead_frio -> nutricao_basica
cliente_pos_sinistro -> retencao_pos_sinistro
cotador_comparador -> oferta_competitiva
lead_engajado_com_dados -> close_comercial
```

The implementation shall preserve this mapping unless a future specification explicitly changes the taxonomy.

### 4.5 Provenance Expectations

If `persona_profile_source_family` and `audience_segment_source_family` are published, they shall remain identical for a given lead when both fields come from the same interpretive consolidation contract.

Acceptable example:

```text
persona_profile = lead_frio
audience_segment = nutricao_basica
persona_profile_source_family = deterministic_fallback
audience_segment_source_family = deterministic_fallback
```

Unacceptable example:

```text
persona_profile = lead_frio
audience_segment = oferta_competitiva
persona_profile_source_family = deterministic_fallback
audience_segment_source_family = deterministic_fallback
```

This is unacceptable because the published pair is contradictory under the canonical mapping even though provenance claims both fields came from one coherent interpretive source.

## 5. Acceptance Criteria

- **AC-001**: Given a lead whose consolidated interpretive output is `persona_profile = lead_frio` and `audience_segment = nutricao_basica`, When `Gold` is published, Then `audience_segment` shall remain `nutricao_basica` even if `competitor_pressure_level = alta`.
- **AC-002**: Given a lead whose deterministic commercial severity is high, When `Gold` is published, Then `competitor_pressure_level` shall remain available as its own field without changing the interpretive segmentation pair.
- **AC-003**: Given a `Gold` row with `persona_profile = lead_frio` and `audience_segment = oferta_competitiva`, When validation runs, Then the row shall fail `persona_audience_semantic_alignment`.
- **AC-004**: Given a coherent `silver_conversations_llm` row with `persona_profile = cotador_comparador` and `audience_segment = oferta_competitiva`, When `Gold` is published, Then the same aligned pair shall be preserved.
- **AC-005**: Given the baseline local runtime with LLM disabled, When the pipeline runs, Then the fallback interpretive segmentation pair shall remain aligned and validation shall pass.
- **AC-006**: Given regression fixtures where urgency, price objection, or competitor severity are high, When the consolidation executes, Then those deterministic signals shall not by themselves rewrite `audience_segment`.

## 6. Test Automation Strategy

- **Test Levels**: Unit, contract, repository-local integration
- **Frameworks**: `pytest` executed via `venv/bin/python -m pytest`
- **Test Data Management**: Synthetic DataFrame fixtures for consolidation and validation edge cases, plus the local repository runtime path when needed
- **CI/CD Integration**: The existing repository test suite shall continue to validate the correction without network access
- **Coverage Requirements**:
  - one transform-level regression for the exact failure mode observed in local artifacts
  - one validation-level regression for invalid persona and audience pairing
  - one integration-level check that `Gold` publication no longer introduces this mismatch
- **Performance Testing**: Not required for this targeted correction

Recommended validation commands:

```bash
venv/bin/python -m pytest tests/test_transforms.py -q
venv/bin/python -m pytest tests/test_quality.py -q
venv/bin/python -m pytest -q
```

## 7. Rationale & Context

The local artifacts showed a narrow but important architectural inconsistency:

- `silver_conversations_llm` produced a coherent interpretive pair
- `Gold` consolidation introduced a rewrite rule based on `competitor_pressure_level`
- validation correctly rejected the final `Gold` row

This means the validation layer behaved correctly. The defect is in publication logic.

The architectural reason this matters is that semantic families must not silently redefine one another:

- deterministic severity expresses measurable business pressure
- interpretive segmentation expresses targeting semantics
- severity can correlate with segmentation, but correlation is not source-of-truth identity

Allowing deterministic severity to rewrite interpretive segmentation creates three problems:

1. it breaks provenance trust
2. it creates false validation failures
3. it hides the true source of meaning of published semantic fields

The correct fix is to preserve the interpretive pair and keep deterministic severity as a separate published analytical field.

## 8. Dependencies & External Integrations

### External Systems

- **EXT-001**: Local Bronze parquet source - required only for repository-local runtime verification

### Third-Party Services

- **SVC-001**: Optional LLM provider runtime - not required for this correction because the failure reproduces with deterministic fallback semantics

### Infrastructure Dependencies

- **INF-001**: Local filesystem artifact persistence for Bronze, Silver, Gold, state, and reports

### Data Dependencies

- **DAT-001**: `silver_conversations_llm.parquet` - required as the interpretive source contract for `persona_profile` and `audience_segment`
- **DAT-002**: `silver_messages.parquet` - required as the deterministic source contract for `competitor_pressure_level`

### Technology Platform Dependencies

- **PLT-001**: Repository-local Python environment in `venv/` - mandatory runtime and test environment

### Compliance Dependencies

- **COM-001**: Existing publication-safe and privacy masking rules - must remain unchanged by this correction

## 9. Examples & Edge Cases

```text
Example A: High competitor pressure without comparator persona

Conversation semantics:
- persona_profile = lead_frio
- audience_segment = nutricao_basica

Deterministic lead evidence:
- competitor_pressure_level = alta

Expected Gold output:
- persona_profile = lead_frio
- audience_segment = nutricao_basica
- competitor_pressure_level = alta

Example B: Valid competitive targeting

Conversation semantics:
- persona_profile = cotador_comparador
- audience_segment = oferta_competitiva

Deterministic lead evidence:
- competitor_pressure_level = alta

Expected Gold output:
- persona_profile = cotador_comparador
- audience_segment = oferta_competitiva
- competitor_pressure_level = alta

Example C: Invalid rewritten pair

Published Gold output:
- persona_profile = lead_frio
- audience_segment = oferta_competitiva

Validation expectation:
- fail semantic alignment validation
```

## 10. Validation Criteria

- The implementation shall remove or neutralize the post-consolidation rule that rewrites `audience_segment` from `competitor_pressure_level`.
- `Gold` shall preserve coherent interpretive segmentation pairs from `silver_conversations_llm` consolidation.
- The validation layer shall continue detecting invalid `persona_profile` and `audience_segment` combinations.
- The repository test suite shall contain explicit regression coverage for the exact failure mode observed in the local artifacts.
- `README.md` shall describe that `audience_segment` remains interpretive and is not overwritten by deterministic severity fields.

## 11. Related Specifications / Further Reading

- [spec/spec-architecture-semantic-contract-separation.md](/home/lucas/projects/lucas54neves/namastex-test/spec/spec-architecture-semantic-contract-separation.md)
- [spec/spec-architecture-pipeline-validation-contract-alignment.md](/home/lucas/projects/lucas54neves/namastex-test/spec/spec-architecture-pipeline-validation-contract-alignment.md)
- [README.md](/home/lucas/projects/lucas54neves/namastex-test/README.md)
