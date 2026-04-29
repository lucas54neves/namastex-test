---
title: README LLM Output Documentation — Gold Enrichment Example and Baseline Delta
version: 1.0
date_created: 2026-04-29
owner: Data Engineering
tags: [process, documentation, readme, llm, gold, enrichment, example]
---

# Introduction

The pipeline supports two execution modes: a baseline deterministic mode (`PIPELINE_ENABLE_LLM_ENRICHMENT=0`) and an LLM-enriched mode that calls an external provider (OpenAI or Anthropic) to populate additional semantic fields in the Silver conversations layer, which then propagate to Gold analytical dimensions.

The current README documents both modes at the operational level (how to configure, which env vars to set) but does not show *what changes in the output*. An evaluator who runs only the baseline — the recommended default — sees Gold records with deterministic fallback values for fields such as `conversation_sentiment`, `intent_stage`, and `lead_temperature`, and has no reference to understand what the LLM-enriched values look like or how they differ.

This spec defines the content, placement, and format of an "LLM enrichment output" section in `README.md` that demonstrates the concrete delta between baseline and enriched Gold records.

## 1. Purpose & Scope

**Purpose:** Define the requirements for a README section that shows example Gold output records with and without LLM enrichment, explains which fields are affected, and documents the fallback contract so evaluators understand the full capability of the pipeline without needing external credentials.

**Scope:**

Files modified:
- `README.md` — new section added between "Execução com Docker Compose" and "Variáveis de ambiente relevantes"

Files unchanged:
- All source code
- All tests
- All spec files
- `config/pipeline_spec.json`

**Out of scope:**
- Changes to the enrichment logic itself
- Adding new Gold fields
- Generating the example JSON automatically via a CI step (examples are static, representative records)

**Intended audience:** Technical evaluators, hiring reviewers, and new engineers reading the repository without access to API credentials.

## 2. Definitions

| Term | Definition |
|---|---|
| Baseline mode | Execution with `PIPELINE_ENABLE_LLM_ENRICHMENT=0`. All Gold fields are populated by deterministic fallback rules. No external API calls are made. |
| LLM-enriched mode | Execution with `PIPELINE_ENABLE_LLM_ENRICHMENT=1` and a valid `OPENAI_API_KEY` or `ANTHROPIC_API_KEY`. The Silver conversations layer receives semantic annotations from the LLM provider; Gold fields derived from those annotations are richer. |
| Enriched fields | Gold columns whose values differ between baseline and LLM-enriched mode: `conversation_sentiment`, `sentiment_support`, `intent_stage`, `lead_temperature`, and any LLM-derived persona signals. |
| Deterministic fallback | The rule-based computation applied when LLM enrichment is disabled or when the provider returns an invalid response. Produces a valid output but from a limited signal set. |
| Delta | The set of column values that change between a baseline run and an LLM-enriched run on the same input data. |

## 3. Requirements, Constraints & Guidelines

### Section placement and heading

- **REQ-001**: The new section MUST be added under a second-level heading `## LLM Enrichment — Baseline vs. Enriched Output` placed immediately after the "Execução com Docker Compose" subsection and before "Variáveis de ambiente relevantes".
- **REQ-002**: The section MUST open with a one-paragraph explanation of why the two modes exist and why the baseline is the recommended default for evaluation.

### Enriched fields table

- **REQ-010**: The section MUST include a table listing every Gold column that changes between modes, the source of the value in each mode, and the expected value range.

| Column | Baseline source | LLM-enriched source |
|---|---|---|
| `conversation_sentiment` | Rule-based tone pattern counts | LLM semantic classification |
| `sentiment_support` | Threshold on positive/negative hit counts | LLM confidence level |
| `intent_stage` | Keyword match on message signals | LLM intent classification |
| `lead_temperature` | Derived from message count and engagement bucket | LLM composite score |
| `llm_persona_hint` | Not present / empty | LLM-provided persona label |
| `llm_summary` | Not present / empty | LLM conversation summary (if configured) |

- **REQ-011**: The table MUST note which columns are present in the baseline output with a deterministic value and which are absent or empty in baseline mode.

### Side-by-side JSON example

- **REQ-020**: The section MUST include a fenced code block (` ```json `) showing a representative Gold record in baseline mode.
- **REQ-021**: The section MUST include a second fenced code block showing the same record in LLM-enriched mode, with the enriched fields highlighted via inline comments (`// enriched`).
- **REQ-022**: Both examples MUST use masked values for all PII fields (lead contact, name) consistent with the masking policy.
- **REQ-023**: The example records MUST be realistic — values should reflect plausible outputs for a WhatsApp auto-insurance sales conversation, not placeholder strings like `"example"` or `"TODO"`.
- **REQ-024**: The examples MUST be static (hardcoded in the README). They are illustrative, not guaranteed to match any specific run output.

### Fallback contract note

- **REQ-030**: After the examples, the section MUST include a short note explaining the fallback contract: when `PIPELINE_ENABLE_LLM_ENRICHMENT=1` but the provider is unavailable or returns invalid JSON, the pipeline falls back to deterministic rules and the run completes successfully. The output in that case is identical to baseline mode.
- **REQ-031**: The note MUST reference the relevant env var (`PIPELINE_ENABLE_LLM_ENRICHMENT`) and the two supported providers (`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`).

### Style constraints

- **CON-001**: The section MUST NOT exceed 80 lines in the README (excluding the JSON code blocks).
- **CON-002**: The section MUST be written in the same language as the surrounding README content (Portuguese).
- **CON-003**: JSON field names in the example records MUST match the actual Gold schema column names as defined in `config/pipeline_spec.json`.
- **CON-004**: No PII may appear in the example records. All personal data fields must use the masked format already established in the codebase (e.g., `"xxxx.xxxxx@xxxxx.xxx"` for email, `"XXX9X99"` for plate).

### Guidelines

- **GUD-001**: The enriched example record SHOULD show at least one field that changes significantly between modes (e.g., `conversation_sentiment` changing from `"neutro"` to `"frustrado_com_preco"`) to make the delta immediately legible.
- **GUD-002**: A brief one-line caption SHOULD appear immediately above each code block (e.g., `**Modo baseline (sem LLM):**`) to orient the reader without requiring them to read surrounding prose.
- **GUD-003**: The section SHOULD end with a link to the `.env.example` file and a one-line instruction for how to enable LLM enrichment locally, to reduce friction for evaluators who want to reproduce the enriched output.

## 4. Interfaces & Data Contracts

### Required section skeleton

```markdown
## LLM Enrichment — Baseline vs. Enriched Output

O pipeline suporta dois modos de execução: baseline determinístico e modo enriquecido com LLM.
No modo baseline (padrão recomendado para validação local), todos os campos analíticos da Gold
são calculados por regras determinísticas. No modo enriquecido, a camada Silver recebe
anotações semânticas de um provider externo (OpenAI ou Anthropic), que se propagam para
dimensões adicionais da Gold.

### Campos afetados pelo enrichment

| Campo | Baseline | LLM-enriched |
|---|---|---|
| `conversation_sentiment` | Contagem de padrões de tom | Classificação semântica do LLM |
| `sentiment_support` | Threshold sobre hits positivos/negativos | Nível de confiança do LLM |
| `intent_stage` | Match de palavras-chave | Classificação de intenção pelo LLM |
| `lead_temperature` | Derivado de mensagens e engajamento | Score composto pelo LLM |

### Exemplo de registro Gold

**Modo baseline (sem LLM):**

```json
{
  "lead_key": "XXXXX9XXXXXXXXXXX",
  "conversation_sentiment": "neutro",
  "sentiment_support": "fraco",
  "intent_stage": "sem_evidencia",
  "lead_temperature": "frio",
  "persona_profile": "lead_frio",
  "audience_segment": "nutricao_basica",
  "engagement_bucket": "low",
  "message_count": 4,
  "dominant_email_provider": "xxxxx.xxx"
}
```

**Modo enriquecido (com LLM):**

```json
{
  "lead_key": "XXXXX9XXXXXXXXXXX",
  "conversation_sentiment": "frustrado_com_preco",      // enriched
  "sentiment_support": "forte",                         // enriched
  "intent_stage": "objecao_preco",                      // enriched
  "lead_temperature": "morno",                          // enriched
  "persona_profile": "cotador_comparador",              // enriched
  "audience_segment": "oferta_competitiva",             // enriched
  "engagement_bucket": "low",
  "message_count": 4,
  "dominant_email_provider": "xxxxx.xxx"
}
```

### Contrato de fallback

...
```

### Actual example values — requirements

The baseline example MUST use values that are plausible deterministic outputs:
- `conversation_sentiment`: one of `["positivo", "negativo", "neutro"]`
- `sentiment_support`: one of `["forte", "moderado", "fraco", "sem_evidencia"]`
- `intent_stage`: one of `["cotacao_iniciada", "proposta_recebida", "objecao_preco", "sem_evidencia"]`
- `lead_temperature`: one of `["quente", "morno", "frio"]`

The LLM-enriched example MUST show at least two fields with different values than the baseline example for the same record.

## 5. Acceptance Criteria

- **AC-001**: Given the section is added, when a reader opens README.md and searches for "LLM Enrichment", then the section heading is found within the first 500 lines of the file.
- **AC-002**: Given the section is added, when the enriched fields table is inspected, then all six columns listed in REQ-010 are present.
- **AC-003**: Given the section is added, when the two JSON code blocks are inspected, then the enriched block contains at least two fields with `// enriched` comments and different values from the baseline block.
- **AC-004**: Given the section is added, when all personal data fields in both JSON examples are inspected, then no unmasked email, phone, name, CPF, or plate is present.
- **AC-005**: Given the section is added, when the fallback contract note is inspected, then `PIPELINE_ENABLE_LLM_ENRICHMENT`, `OPENAI_API_KEY`, and `ANTHROPIC_API_KEY` are all mentioned.
- **AC-006**: Given the section is added, when the JSON field names in the examples are compared against `config/pipeline_spec.json`, then all field names in the examples exist in the Gold schema.
- **AC-007**: Given the change is applied, when `venv/bin/python -m pytest -q` is executed, then all 351 existing tests pass (documentation-only change, no code regression).
- **AC-008**: Given the section is added, when the total line count of the new section is measured (excluding JSON code blocks), then it is at most 80 lines.

## 6. Test Automation Strategy

- **Test levels**: Documentation change only. No automated test covers README content by default.
- **Optional linting**: `markdownlint` or `remark` can be added to CI to enforce heading hierarchy and code block syntax. This is optional and not a blocking criterion for this spec.
- **Manual verification**: The reviewer MUST confirm AC-001 through AC-008 by inspection during the PR review.
- **Field name verification**: Run `venv/bin/python -c "import json; spec=json.load(open('config/pipeline_spec.json')); print(spec['gold']['required_columns'])"` and compare the output against the JSON example field names.
- **CI/CD**: The existing `venv/bin/python -m pytest -q` step confirms no code regression (AC-007). No new CI step is required.

## 7. Rationale & Context

**Why this matters for evaluation:** The pipeline's most distinctive capability — semantic classification via LLM — is invisible when running in baseline mode. An evaluator who follows the README defaults sees a working pipeline but cannot observe the enriched dimensions. Without an example in the README, the LLM enrichment layer looks like an unused configuration option rather than a core architectural feature.

**Why static examples instead of a generated report:** A generated report (e.g., a CI step that runs the pipeline with a test API key and captures output) would require managing credentials in CI and introduces flakiness from provider availability. Static examples are stable, require no credentials, and can be crafted to highlight the most informative fields. The README already uses static examples for payload formats (webhook section); this is consistent with that pattern.

**Why the section goes before "Variáveis de ambiente":** The reader who wants to understand what the pipeline produces should encounter the output examples before the configuration reference. The current README flows from execution instructions to configuration; inserting the output section after execution and before configuration keeps the logical reading order intact.

**Why Portuguese:** The surrounding README content is in Portuguese, targeting the same audience as the evaluation brief. Changing the language of a single section would create inconsistency.

## 8. Dependencies & External Integrations

### Data Dependencies
- **DAT-001**: `config/pipeline_spec.json` — Gold schema column names MUST be consulted to verify example field names are valid.

### Documentation Dependencies
- **DOC-001**: `.env.example` — the section MUST link to this file for the local enablement instruction.

## 9. Examples & Edge Cases

```markdown
<!-- Edge case: field not present in baseline -->
<!-- For fields that are absent (not just empty) in baseline mode,
     the example JSON MUST use a comment to make the absence explicit: -->

// Baseline record — llm_summary field is absent
{
  "lead_key": "XXXXX9XXXXXXXXXXX",
  "conversation_sentiment": "neutro"
  // llm_summary: not present in baseline output
}

// Enriched record
{
  "lead_key": "XXXXX9XXXXXXXXXXX",
  "conversation_sentiment": "frustrado_com_preco",  // enriched
  "llm_summary": "Lead comparou preco com concorrente e solicitou desconto."  // enriched
}
```

```markdown
<!-- Edge case: baseline value is already informative -->
<!-- If a field has the same value in both modes for the chosen example record,
     the reviewer MUST select a different example record where the delta is visible.
     The purpose is to demonstrate enrichment value, not to show identical outputs. -->
```

## 10. Validation Criteria

- Section heading `## LLM Enrichment — Baseline vs. Enriched Output` exists in README.md
- Enriched fields table contains all six columns from REQ-010
- Two fenced JSON code blocks present in the section
- At least two `// enriched` annotations in the second code block with different values from the first
- No unmasked PII in either code block
- `PIPELINE_ENABLE_LLM_ENRICHMENT`, `OPENAI_API_KEY`, `ANTHROPIC_API_KEY` mentioned in the fallback note
- All JSON field names in examples are valid Gold schema columns per `config/pipeline_spec.json`
- `venv/bin/python -m pytest -q` → all 351 tests pass

## 11. Related Specifications / Further Reading

- `spec/spec-architecture-llm-conversation-enrichment.md` — design of the LLM enrichment stage
- `spec/spec-architecture-llm-output-normalization-contract.md` — contract for LLM output validation and fallback
- `spec/spec-architecture-langfuse-phased-prompt-observability.md` — Langfuse integration for prompt management
- `docs/data-dictionary-data-ai-engineering.md` — source schema reference
