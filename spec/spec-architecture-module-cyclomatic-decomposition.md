---
title: Module Cyclomatic Decomposition — Decomposição de Módulos de Alta Complexidade
version: 1.0
date_created: 2026-04-28
owner: Data Engineering
tags: [architecture, refactoring, maintainability, silver, operator, design]
---

# Introduction

Dois módulos acumularam responsabilidades demais durante o desenvolvimento iterativo do pipeline:

- `src/pipeline/transforms/silver.py` (~1.280 linhas): contém extração de sinais, mascaramento PII, deduplicação, construção de contexto de conversa e de lead, além da lógica de build das visões Silver, tudo no mesmo arquivo.
- `src/pipeline/orchestration/operator.py` (~975 linhas): contém o ciclo ReAct completo, persistência de artefatos, relatórios, alertas, snapshot de monitoramento e funções auxiliares de suporte ao ciclo.

Esta spec define a decomposição incremental e retrocompatível desses dois módulos em submódulos coesos, sem alterar nenhum contrato público de função, schema parquet ou comportamento observável do pipeline.

## 1. Purpose & Scope

**Propósito:** Reduzir a complexidade ciclomática e o tamanho dos dois módulos maiores do projeto por meio de decomposição em submódulos coesos, mantendo retrocompatibilidade total via reexportação nos módulos originais.

**Escopo:**

**`silver.py` → decomposição em:**
- `src/pipeline/transforms/silver_patterns.py` — constantes e regex compilados
- `src/pipeline/transforms/silver_masking.py` — funções de mascaramento PII
- `src/pipeline/transforms/silver_signals.py` — extração de sinais (email, veículo, concorrente, urgência, preço, sinistro, sentimento)
- `src/pipeline/transforms/silver_context.py` — construção de contexto de conversa e lead (`add_conversation_context`, `add_lead_context`)
- `src/pipeline/transforms/silver_dedup.py` — deduplicação semântica (`deduplicate_events`)
- `src/pipeline/transforms/silver.py` — mantido como ponto de entrada; importa e reexporta tudo

**`operator.py` → decomposição em:**
- `src/pipeline/orchestration/operator_reports.py` — builders de relatório e funções de escrita (`_build_agent_report`, `_build_alert_report`, `_write_reports`, `_run_record`, `_planner_report_summary`)
- `src/pipeline/orchestration/operator_stages.py` — execução de cada stage do ReAct loop (`_run_bronze_stage`, `_run_silver_stage`, `_run_gold_stage`, `_run_validation_stage`)
- `src/pipeline/orchestration/operator_artifacts.py` — estruturas de paths e artefatos (`PipelineArtifacts`, `_skip_artifacts`, `_success_artifacts`, funções de path)
- `src/pipeline/orchestration/operator.py` — mantido como ponto de entrada; importa e reexporta; contém apenas `run_cycle()` e `build_monitor_snapshot()`

**Fora de escopo:**

- Alteração de qualquer assinatura de função pública
- Alteração de qualquer schema Parquet (Bronze, Silver, Gold)
- Alteração de `config/pipeline_spec.json`
- Alteração de qualquer arquivo de teste existente além das importações
- Refatoração de lógica de negócio (apenas reorganização estrutural)
- Novos testes — a suíte existente de 234 testes deve continuar passando sem modificação

**Público-alvo:** Engenheiros implementando a decomposição e revisores de PR.

## 2. Definitions

| Termo | Definição |
|---|---|
| Retrocompatibilidade | Garantia de que todos os importadores externos existentes funcionam sem alteração após a decomposição |
| Reexportação | Técnica de manter um símbolo acessível em um módulo A mesmo após movê-lo para módulo B: `from B import X` em A seguido de `__all__ = ["X", ...]` |
| Submódulo coeso | Módulo com uma única responsabilidade claramente identificável, sem dependências circulares com outros submódulos do mesmo pacote |
| Complexidade ciclomática | Medida de caminhos independentes em um módulo; reduzida quando responsabilidades são separadas |
| Ponto de entrada | Módulo original (silver.py, operator.py) que reexporta tudo após a decomposição, preservando imports existentes |
| Stage function | Função extraída de `run_cycle()` responsável por executar um stage específico do pipeline (bronze, silver, gold, validation) |

## 3. Requirements, Constraints & Guidelines

### Retrocompatibilidade — regra absoluta

- **CON-001**: Todo símbolo atualmente exportável de `silver.py` DEVE continuar acessível via `from pipeline.transforms.silver import <symbol>` após a decomposição. A ausência de qualquer símbolo é uma regressão proibida.
- **CON-002**: Todo símbolo atualmente exportável de `operator.py` DEVE continuar acessível via `from pipeline.orchestration.operator import <symbol>` após a decomposição.
- **CON-003**: Nenhum arquivo de teste existente pode ser alterado além de importações de path que tenham mudado. Se um teste importa `from pipeline.transforms.silver import mask_sender_name`, esse import DEVE continuar funcionando.
- **CON-004**: A suíte completa de 234 testes DEVE passar sem modificação de lógica após a decomposição.

### Decomposição de `silver.py`

- **REQ-101**: `silver_patterns.py` DEVE conter todas as constantes e objetos `re.Pattern` compilados: `EMAIL_PATTERN`, `PHONE_PATTERN`, `CPF_PATTERN`, `CEP_PATTERN`, `PLATE_PATTERN`, `YEAR_PATTERN`, `PRICE_PATTERN`, `PRICE_OBJECTION_PATTERN`, `URGENCY_STRONG_PATTERN`, `URGENCY_MODERATE_PATTERN`, `COMPETITOR_COMPARISON_PATTERN`, `EMAIL_PROVIDER_DOMAIN_PATTERN`, `COMPETITOR_PATTERNS`, `SINISTRO_PATTERNS`, `VEHICLE_MAKES`, `VEHICLE_MODELS`, `STATUS_PRIORITY`, `SENSITIVE_PATTERNS`, `POSITIVE_TONE_PATTERNS`, `NEGATIVE_TONE_PATTERNS`, `CONVERSATION_SENTIMENT_LABELS`, `CONVERSATION_SENTIMENT_SUPPORT_LEVELS`. Não deve importar nada de outros submódulos Silver.
- **REQ-102**: `silver_masking.py` DEVE conter: `_mask_digits`, `_mask_email`, `_mask_alpha_numeric`, `_mask_name_token`, `_mask_known_names`, `mask_sender_name`, `mask_message_body`, `detect_sensitive_classes`, `detect_unmasked_sensitive_classes`, `_is_masked_email`, `_is_masked_plate`. Deve importar apenas de `silver_patterns.py` e stdlib.
- **REQ-103**: `silver_signals.py` DEVE conter: `_extract_first`, `_extract_competitor`, `_extract_sinistro_type`, `_extract_vehicle_make`, `_extract_vehicle_model`, `_extract_vehicle_year`, `_extract_price`, `_extract_email_provider`, `_extract_price_objection_signal`, `_extract_urgency_strength`, `_extract_competitor_comparison_signal`, `_count_tone_hits`, `derive_conversation_sentiment_label`, `derive_conversation_sentiment_support`, `add_message_signals`. Deve importar de `silver_patterns.py` e `silver_masking.py`.
- **REQ-104**: `silver_context.py` DEVE conter: `add_conversation_context`, `add_lead_context`, `_stable_hash_token`, `_normalize_for_match`, `_normalize_ascii`. Deve importar de `silver_masking.py` e `silver_patterns.py`.
- **REQ-105**: `silver_dedup.py` DEVE conter: `deduplicate_events`. Deve importar de `silver_patterns.py`.
- **REQ-106**: As funções auxiliares de agregação (`_first_non_empty`, `_json_sorted_unique`, `_first_non_null`, `_last_non_null`, `_max_or_false`, `_safe_string`, `_safe_float`, `_safe_int`) e as funções de segmentação Gold (`add_gold_segments`, `_canonical_audience_for_persona`, funções `_response_latency_band`, `_price_objection_intensity`, etc.) e `build_silver`, `build_silver_leads`, `build_gold` DEVEM permanecer no módulo que fizer sentido semântico. `build_gold` e seus helpers de segmentação Gold podem permanecer em `silver.py` até que `gold.py` seja o local correto — esta spec não define a migração de `build_gold`; apenas a de `silver.py`.
- **REQ-107**: O `silver.py` resultante DEVE importar de todos os submódulos e reexportar todos os símbolos que existiam anteriormente. O `__all__` DEVE ser explicitamente definido.

### Decomposição de `operator.py`

- **REQ-201**: `operator_reports.py` DEVE conter: `_build_agent_report`, `_build_alert_report`, `_write_reports`, `_run_record`, `_planner_report_summary`, `_utc_now_iso`, `_extract_llm_diagnoses`. Deve importar de `pipeline.agent.alerts`, `pipeline.io.parquet_io` e stdlib.
- **REQ-202**: `operator_artifacts.py` DEVE conter: `PipelineArtifacts` (dataclass), `state_file`, `validation_report_file`, `agent_report_file`, `alert_report_file`, `plan_report_file`, `_skip_artifacts`, `_success_artifacts`. Deve importar de `pipeline.config` e stdlib.
- **REQ-203**: `operator_stages.py` DEVE conter funções de execução de stage extraídas do corpo do loop ReAct: `_run_bronze_stage`, `_run_silver_stage`, `_run_gold_stage`, `_run_validation_stage`. Cada função recebe os argumentos necessários e retorna um dict com o estado atualizado do stage. Deve importar de `pipeline.transforms.*`, `pipeline.quality.*`, `pipeline.agent.*` e `pipeline.io.*`.
- **REQ-204**: O `operator.py` resultante DEVE conter apenas: `run_cycle()` e `build_monitor_snapshot()`. Todas as funções auxiliares DEVEM ser importadas dos submódulos. O `__all__` DEVE ser explicitamente definido com os símbolos públicos.
- **REQ-205**: A lógica de `run_cycle()` DEVE ser preservada identicamente — apenas as chamadas internas migram para funções em `operator_stages.py`. O comportamento externo (artefatos produzidos, relatórios, estado) NÃO deve mudar.

### Tamanho alvo dos módulos

- **GUD-001**: Nenhum módulo resultante deve ultrapassar 400 linhas. Se ultrapassar, justificar em comentário inline no topo do arquivo.
- **GUD-002**: Cada submódulo deve ter uma única responsabilidade descritível em uma frase curta.
- **GUD-003**: Não introduzir dependências circulares entre submódulos. A ordem de dependência deve ser: `patterns → masking → signals → context/dedup → silver (ponto de entrada)`.

### Imports e `__init__.py`

- **REQ-210**: O `src/pipeline/transforms/__init__.py` NÃO deve ser modificado durante esta spec. Se importa de `silver`, continuará funcionando via reexportação.
- **REQ-211**: Os novos submódulos `silver_patterns.py`, `silver_masking.py`, `silver_signals.py`, `silver_context.py`, `silver_dedup.py` NÃO devem ser importados diretamente por outros módulos fora de `src/pipeline/transforms/`. O ponto de entrada público é `silver.py`.
- **REQ-212**: Os novos submódulos `operator_reports.py`, `operator_artifacts.py`, `operator_stages.py` NÃO devem ser importados diretamente por módulos fora de `src/pipeline/orchestration/`. O ponto de entrada público é `operator.py`.

## 4. Interfaces & Data Contracts

### Mapa de migração — `silver.py`

| Símbolo atual | Destino |
|---|---|
| `EMAIL_PATTERN`, `PHONE_PATTERN`, `CPF_PATTERN`, `CEP_PATTERN`, `PLATE_PATTERN`, `YEAR_PATTERN`, `PRICE_PATTERN`, `PRICE_OBJECTION_PATTERN`, `URGENCY_STRONG_PATTERN`, `URGENCY_MODERATE_PATTERN`, `COMPETITOR_COMPARISON_PATTERN`, `EMAIL_PROVIDER_DOMAIN_PATTERN`, `COMPETITOR_PATTERNS`, `SINISTRO_PATTERNS`, `VEHICLE_MAKES`, `VEHICLE_MODELS`, `STATUS_PRIORITY`, `SENSITIVE_PATTERNS`, `POSITIVE_TONE_PATTERNS`, `NEGATIVE_TONE_PATTERNS`, `CONVERSATION_SENTIMENT_LABELS`, `CONVERSATION_SENTIMENT_SUPPORT_LEVELS` | `silver_patterns.py` |
| `_mask_digits`, `_mask_email`, `_mask_alpha_numeric`, `_mask_name_token`, `_mask_known_names`, `mask_sender_name`, `mask_message_body`, `detect_sensitive_classes`, `detect_unmasked_sensitive_classes`, `_is_masked_email`, `_is_masked_plate` | `silver_masking.py` |
| `_extract_first`, `_extract_competitor`, `_extract_sinistro_type`, `_extract_vehicle_make`, `_extract_vehicle_model`, `_extract_vehicle_year`, `_extract_price`, `_extract_email_provider`, `_extract_price_objection_signal`, `_extract_urgency_strength`, `_extract_competitor_comparison_signal`, `_count_tone_hits`, `derive_conversation_sentiment_label`, `derive_conversation_sentiment_support`, `add_message_signals` | `silver_signals.py` |
| `add_conversation_context`, `add_lead_context`, `_stable_hash_token`, `_normalize_for_match`, `_normalize_ascii` | `silver_context.py` |
| `deduplicate_events` | `silver_dedup.py` |
| `load_bronze_frame`, `parse_metadata`, `build_silver`, `build_silver_leads`, `add_gold_segments`, `build_gold`, funções auxiliares de agregação e segmentação | `silver.py` (permanece) |

### Mapa de migração — `operator.py`

| Símbolo atual | Destino |
|---|---|
| `PipelineArtifacts`, `state_file`, `validation_report_file`, `agent_report_file`, `alert_report_file`, `plan_report_file`, `_skip_artifacts`, `_success_artifacts` | `operator_artifacts.py` |
| `_build_agent_report`, `_build_alert_report`, `_write_reports`, `_run_record`, `_planner_report_summary`, `_utc_now_iso`, `_extract_llm_diagnoses` | `operator_reports.py` |
| Corpo de execução de `stage == "bronze"` → `_run_bronze_stage` | `operator_stages.py` |
| Corpo de execução de `stage == "silver"` → `_run_silver_stage` | `operator_stages.py` |
| Corpo de execução de `stage == "gold"` → `_run_gold_stage` | `operator_stages.py` |
| Corpo de execução de `stage == "validation"` → `_run_validation_stage` | `operator_stages.py` |
| `run_cycle`, `build_monitor_snapshot` | `operator.py` (permanece) |

### Assinatura das stage functions em `operator_stages.py`

```python
from dataclasses import dataclass
from typing import Any
import pandas as pd
from pipeline.config import PipelinePaths

@dataclass
class StageResult:
    """Resultado imutável de uma stage do ReAct loop."""
    stage: str
    success: bool
    updated_state: dict[str, Any]   # chaves: bronze_df, silver_df, etc.
    log_event_data: dict[str, Any]  # dados para log_event após o stage

def _run_bronze_stage(
    paths: PipelinePaths,
    compiled_plan: dict[str, Any],
) -> StageResult: ...

def _run_silver_stage(
    bronze_df: pd.DataFrame,
    paths: PipelinePaths,
    compiled_plan: dict[str, Any],
    silver_conversations_llm_path: Any,
) -> StageResult: ...

def _run_gold_stage(
    silver_runtime_df: pd.DataFrame,
    silver_messages_runtime_df: pd.DataFrame,
    silver_conversations_llm_runtime_df: pd.DataFrame | None,
    paths: PipelinePaths,
    compiled_plan: dict[str, Any],
    spec: dict[str, Any],
    llm_call: Any,
) -> StageResult: ...

def _run_validation_stage(
    bronze_df: pd.DataFrame,
    silver_df: pd.DataFrame,
    silver_messages_df: pd.DataFrame,
    silver_conversations_llm_df: pd.DataFrame,
    gold_df: pd.DataFrame,
    compiled_plan: dict[str, Any],
) -> StageResult: ...
```

## 5. Acceptance Criteria

- **AC-001**: Dado que a decomposição foi aplicada, quando `python -m pytest -q` é executado, então todos os 234 testes existentes passam sem alteração de lógica.
- **AC-002**: Dado que a decomposição foi aplicada, quando `from pipeline.transforms.silver import mask_sender_name, build_silver, build_silver_leads, detect_unmasked_sensitive_classes` é executado, então todos os símbolos são importáveis sem erro.
- **AC-003**: Dado que a decomposição foi aplicada, quando `from pipeline.orchestration.operator import run_cycle, build_monitor_snapshot, PipelineArtifacts` é executado, então todos os símbolos são importáveis sem erro.
- **AC-004**: Dado que a decomposição foi aplicada, quando `wc -l src/pipeline/transforms/silver.py` é executado, então o resultado é menor que 400.
- **AC-005**: Dado que a decomposição foi aplicada, quando `wc -l src/pipeline/orchestration/operator.py` é executado, então o resultado é menor que 400.
- **AC-006**: Dado que a decomposição foi aplicada, quando `python -c "import pipeline.transforms.silver_patterns"` é executado, então retorna importação bem-sucedida.
- **AC-007**: Dado que a decomposição foi aplicada, quando `python -c "from pipeline.orchestration import operator_stages"` é executado, então retorna importação bem-sucedida.
- **AC-008**: Dado que a decomposição foi aplicada, quando `ruff check src/` é executado, então retorna zero erros.
- **AC-009**: Dado que a decomposição foi aplicada, quando `mypy src/pipeline/` é executado, então retorna zero erros de tipo nos módulos afetados.
- **AC-010**: Dado que nenhum arquivo de teste foi modificado além de importações, quando `git diff tests/` é inspecionado, então nenhuma linha de lógica de asserção foi alterada.

## 6. Test Automation Strategy

- **Test Levels**: A suíte existente é o critério principal — não é necessário criar novos testes para esta spec.
- **Verificação estrutural**: Adicionar ao CI um check `wc -l` ou `radon cc` para garantir que nenhum módulo ultrapasse 400 linhas.
- **Verificação de imports**: Script de smoke test: `python -c "from pipeline.transforms.silver import *; from pipeline.orchestration.operator import *"` — deve retornar exit 0.
- **Verificação de dependências circulares**: `python -c "import ast; ..."` ou ferramenta `pydeps` — verificar que não há ciclos entre os submódulos novos.
- **CI/CD**: Os checks acima devem ser executados no mesmo job de `pre-commit` ou em step separado no workflow de PR.

## 7. Rationale & Context

Módulos grandes não são um problema de estilo — são um risco operacional. Quando `silver.py` e `operator.py` acumulam mais de 1.000 linhas cada, três consequências práticas emergem: (1) merges concorrentes causam conflitos frequentes em arquivos que todos tocam; (2) a leitura para diagnóstico de incidente exige scroll extensivo para localizar a função relevante; (3) a cobertura de testes é difícil de mapear visualmente para responsabilidades. A decomposição proposta não altera lógica — apenas redistribui responsabilidades em módulos menores e coesos, mantendo os módulos originais como pontos de entrada via reexportação. O custo é zero em retrocompatibilidade e o ganho é imediato em legibilidade e manutenção.

## 8. Dependencies & External Integrations

### Technology Platform Dependencies
- **PLT-001**: Python 3.11 — módulos e packages Python padrão; sem dependências novas.

### Data Dependencies
- Nenhuma dependência de dados externa. A decomposição não altera a leitura ou escrita de Parquet.

## 9. Examples & Edge Cases

```python
# silver.py após a decomposição — exemplo do cabeçalho do arquivo resultante
from __future__ import annotations

# Reexportações de retrocompatibilidade
from pipeline.transforms.silver_patterns import (  # noqa: F401
    EMAIL_PATTERN,
    PHONE_PATTERN,
    SENSITIVE_PATTERNS,
    POSITIVE_TONE_PATTERNS,
    NEGATIVE_TONE_PATTERNS,
    # ... todos os demais
)
from pipeline.transforms.silver_masking import (  # noqa: F401
    mask_sender_name,
    mask_message_body,
    detect_sensitive_classes,
    detect_unmasked_sensitive_classes,
    # ...
)
from pipeline.transforms.silver_signals import (  # noqa: F401
    add_message_signals,
    derive_conversation_sentiment_label,
    derive_conversation_sentiment_support,
    # ...
)
from pipeline.transforms.silver_context import (  # noqa: F401
    add_conversation_context,
    add_lead_context,
    # ...
)
from pipeline.transforms.silver_dedup import deduplicate_events  # noqa: F401

__all__ = [
    "EMAIL_PATTERN",
    "mask_sender_name",
    "build_silver",
    "build_silver_leads",
    "load_bronze_frame",
    "parse_metadata",
    "deduplicate_events",
    # ... todos os símbolos que existiam antes
]

# Edge case: símbolo privado usado em teste direto
# Ex: tests/test_transforms.py importa `_safe_string` de silver.
# Deve permanecer acessível via silver.py mesmo após migração.
# Solução: incluir no __all__ ou deixar no silver.py sem mover.
```

## 10. Validation Criteria

- Zero erros em `python -m pytest -q` após a decomposição
- Zero erros em `ruff check src/`
- Nenhum módulo resultante com mais de 400 linhas
- `from pipeline.transforms.silver import X` funciona para todo X que funcionava antes
- `from pipeline.orchestration.operator import X` funciona para todo X que funcionava antes
- Nenhuma dependência circular entre `silver_patterns`, `silver_masking`, `silver_signals`, `silver_context`, `silver_dedup`
- Nenhuma dependência circular entre `operator_artifacts`, `operator_reports`, `operator_stages`

## 11. Related Specifications / Further Reading

- `spec-architecture-project-folder-organization.md` — convenções de organização de módulos do projeto
- `spec-architecture-semantic-contract-separation.md` — separação de responsabilidades entre transformação, qualidade e orquestração
- `spec-architecture-agentic-layer-gap-remediation.md` — contexto de evolução do `operator.py`
