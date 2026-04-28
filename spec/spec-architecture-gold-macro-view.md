---
title: Gold Macro View — Visão Agregada de Toda a Base na Camada Gold
version: 1.0
date_created: 2026-04-28
owner: Data Engineering
tags: [architecture, gold, analytics, aggregation, pipeline]
---

# Introduction

A camada Gold atual produz uma visão analítica por `lead_key` — granularidade correta para operações de CRM e segmentação individual. Porém, não existe nenhum artefato que responda perguntas sobre a base como um todo: "qual é o provedor de email mais usado?", "quantos leads estão em cada persona?", "qual a distribuição de sentimento nas conversas?". Esta spec define o artefato `conversations_gold_macro.parquet` e as tabelas de ranking que o compõem.

## 1. Purpose & Scope

**Propósito:** Definir o schema, as regras de derivação e o contrato de publicação de um segundo artefato Gold — `data/gold/conversations_gold_macro.parquet` — que agrega toda a base de leads em visões de distribuição, ranking e proporção.

**Escopo:**

- Novo artefato Parquet: `data/gold/conversations_gold_macro.parquet`
- Novos steps em `src/pipeline/transforms/gold.py` ou módulo auxiliar `src/pipeline/transforms/gold_macro.py`
- Atualização de `config/pipeline_spec.json` para declarar o novo artefato e suas colunas obrigatórias
- Atualização das validações em `src/pipeline/quality/quality.py` para cobrir `gold_macro`
- Atualização da política de publicação em `src/pipeline/quality/publication.py`
- Atualização dos scripts `scripts/run_pipeline.py` e `scripts/run_pipeline_daemon.py` para persistir o novo artefato
- Atualização do `README.md` para listar o novo artefato e seu conteúdo

**Fora de escopo:**

- Alteração do schema de `conversations_gold.parquet` (granularidade por lead)
- Integração com banco de dados ou API externa
- Visualização ou dashboard

**Público-alvo:** Engenheiros implementando a feature e revisores de PR.

## 2. Definitions

| Termo | Definição |
|---|---|
| Gold Macro | Artefato Gold de granularidade agregada — cada linha representa um grupo (ex: um valor de `persona_profile`), não um lead individual |
| Gold Lead | Artefato Gold existente — granularidade por `lead_key` — não alterado por esta spec |
| Dimension | Coluna categórica da Gold Lead usada como chave de agrupamento no Macro (ex: `persona_profile`, `dominant_email_provider`) |
| Distribution Row | Uma linha do Macro representando uma combinação (dimension, value) com contagens e proporções |
| Metric Snapshot | Bloco de linhas no Macro representando métricas numéricas resumidas de toda a base (ex: média de mensagens por lead) |
| `dimension` column | Coluna do Macro que identifica qual atributo da Gold Lead está sendo agregado |
| `dimension_value` column | Valor do atributo agrupado (ex: `"gmail"`, `"cotador_comparador"`) |
| `lead_count` | Contagem de leads distintos com aquele `dimension_value` |
| `lead_pct` | Proporção de `lead_count` sobre o total de leads na base (valor entre 0.0 e 1.0) |
| `rank` | Posição ordinal do `dimension_value` dentro da `dimension`, ordenado por `lead_count` decrescente |

## 3. Requirements, Constraints & Guidelines

### Artefato e schema

- **REQ-001**: O pipeline DEVE produzir `data/gold/conversations_gold_macro.parquet` em toda execução que produz `conversations_gold.parquet`.
- **REQ-002**: O artefato DEVE conter as colunas obrigatórias: `dimension` (string), `dimension_value` (string), `lead_count` (int64), `lead_pct` (float64), `rank` (int64), `computed_at_utc` (string ISO-8601).
- **REQ-003**: Cada linha DEVE representar exatamente um par `(dimension, dimension_value)` de forma única — a combinação desses dois campos forma a chave primária do artefato.
- **REQ-004**: O campo `lead_pct` DEVE ser calculado como `lead_count / total_leads`, onde `total_leads` é o número de linhas em `conversations_gold.parquet`. O valor DEVE estar no intervalo `[0.0, 1.0]`.
- **REQ-005**: O campo `rank` DEVE ser inteiro positivo começando em 1, ordenado por `lead_count` decrescente dentro de cada `dimension`. Em caso de empate, a ordem é determinística (ex: alfabética por `dimension_value`).
- **REQ-006**: Valores nulos ou string vazia em `dimension_value` DEVEM ser representados como a string `"sem_informacao"` — nunca como `None`/`NaN`.

### Dimensões obrigatórias

- **REQ-010**: A dimensão `persona_profile` DEVE estar presente, agrupando leads por valor de `persona_profile` da Gold Lead.
- **REQ-011**: A dimensão `audience_segment` DEVE estar presente.
- **REQ-012**: A dimensão `dominant_email_provider` DEVE estar presente, considerando apenas leads com `contains_email = True`.
- **REQ-013**: A dimensão `lead_temperature` DEVE estar presente.
- **REQ-014**: A dimensão `engagement_bucket` DEVE estar presente.
- **REQ-015**: A dimensão `conversation_sentiment_label` DEVE estar presente.
- **REQ-016**: A dimensão `closure_outcome_group` DEVE estar presente.
- **REQ-017**: A dimensão `competitor_pressure_level` DEVE estar presente.
- **REQ-018**: A dimensão `price_objection_intensity` DEVE estar presente.
- **REQ-019**: A dimensão `commercial_urgency_signal` DEVE estar presente.
- **REQ-020**: A dimensão `intent_stage` DEVE estar presente.

### Linha de snapshot numérico

- **REQ-030**: DEVE existir um bloco de linhas com `dimension = "numeric_snapshot"` contendo pelo menos as seguintes métricas como `dimension_value`: `avg_total_messages`, `avg_conversation_count`, `avg_data_shared_score`, `total_leads`, `leads_with_email_pct`, `leads_with_competitor_signal_pct`, `leads_with_sinistro_signal_pct`, `leads_closed_pct`.
- **REQ-031**: Para linhas de `numeric_snapshot`, `lead_count` DEVE conter o valor numérico arredondado para 2 casas decimais representado como inteiro quando inteiro ou float serializado como string em `dimension_value`. Alternativamente, o campo `lead_count` pode ser `0` e o valor real ficar em uma coluna extra `metric_value` (float64, nullable). A implementação DEVE escolher uma representação e documentá-la na spec da coluna em `pipeline_spec.json`.
- **REQ-032**: As métricas `_pct` DEVEM estar no intervalo `[0.0, 1.0]`.

### Derivação e ordenação

- **REQ-040**: O Macro DEVE ser derivado exclusivamente a partir de `conversations_gold.parquet` já publicado (pós-sanitização). Não pode ler a Gold runtime não publicada.
- **REQ-041**: O Macro DEVE ser computado após a persistência da Gold Lead.
- **REQ-042**: O Macro DEVE ser ordenado por `dimension` (ascendente) e `rank` (ascendente).

### Publicação segura

- **CON-001**: O Macro NÃO DEVE conter nenhuma coluna com `lead_key`, `lead_contact_ref`, `canonical_lead_name_masked` ou qualquer identificador individual.
- **CON-002**: O Macro NÃO DEVE conter valores originais (não mascarados) de email, telefone, CPF, CEP ou placa.
- **CON-003**: A política de publicação em `publication.py` DEVE incluir o layer `gold_macro` com sua lista de colunas permitidas.

### Compatibilidade

- **CON-010**: Esta spec NÃO altera o schema de `conversations_gold.parquet`.
- **CON-011**: A função `build_gold()` existente NÃO deve ser modificada internamente. O Macro DEVE ser calculado por função separada chamada após `build_gold()`.
- **GUD-001**: Nomear a função de derivação `build_gold_macro(gold_df: pd.DataFrame) -> pd.DataFrame`.

## 4. Interfaces & Data Contracts

### Input

```
data/gold/conversations_gold.parquet
  Colunas consumidas:
    lead_key                  (string)   — chave de contagem de leads
    persona_profile           (string)   — dimensão REQ-010
    audience_segment          (string)   — dimensão REQ-011
    dominant_email_provider   (string)   — dimensão REQ-012 (filtrada por contains_email)
    contains_email            (bool)     — filtro para dominant_email_provider
    lead_temperature          (string)   — dimensão REQ-013
    engagement_bucket         (string)   — dimensão REQ-014
    conversation_sentiment_label (string) — dimensão REQ-015
    closure_outcome_group     (string)   — dimensão REQ-016
    competitor_pressure_level (string)   — dimensão REQ-017
    price_objection_intensity (string)   — dimensão REQ-018
    commercial_urgency_signal (string)   — dimensão REQ-019
    intent_stage              (string)   — dimensão REQ-020
    total_messages            (int)      — para avg_total_messages
    conversation_count        (int)      — para avg_conversation_count
    data_shared_score         (int)      — para avg_data_shared_score
    mentioned_competitor      (bool)     — para leads_with_competitor_signal_pct
    mentioned_sinistro        (bool)     — para leads_with_sinistro_signal_pct
    has_closed_outcome        (bool)     — para leads_closed_pct
```

### Output

```
data/gold/conversations_gold_macro.parquet
  Schema:
    dimension         string    — nome da dimensão (ex: "persona_profile")
    dimension_value   string    — valor agrupado (ex: "cotador_comparador")
    lead_count        int64     — contagem de leads neste grupo
    lead_pct          float64   — proporção sobre total de leads [0.0, 1.0]
    rank              int64     — posição dentro da dimensão (1 = maior grupo)
    computed_at_utc   string    — ISO-8601 UTC timestamp de geração
```

### pipeline_spec.json — adição mínima

```json
{
  "gold_macro": {
    "required_columns": [
      "dimension",
      "dimension_value",
      "lead_count",
      "lead_pct",
      "rank",
      "computed_at_utc"
    ],
    "forbidden_raw_columns": [],
    "dimensions": [
      "persona_profile",
      "audience_segment",
      "dominant_email_provider",
      "lead_temperature",
      "engagement_bucket",
      "conversation_sentiment_label",
      "closure_outcome_group",
      "competitor_pressure_level",
      "price_objection_intensity",
      "commercial_urgency_signal",
      "intent_stage",
      "numeric_snapshot"
    ]
  }
}
```

## 5. Acceptance Criteria

- **AC-001**: Dado que `conversations_gold.parquet` existe, quando `build_gold_macro()` for chamado, então o resultado contém pelo menos 11 valores distintos de `dimension` (as 11 dimensões de REQ-010 a REQ-020).
- **AC-002**: Dado qualquer dimensão presente, quando o Macro for gerado, então `rank = 1` corresponde ao `dimension_value` com maior `lead_count` naquela dimensão.
- **AC-003**: Dado que há N leads na Gold Lead, quando o Macro for gerado, então `SUM(lead_count)` para qualquer dimensão de contagem de leads (exceto `numeric_snapshot`) é igual a N.
- **AC-004**: Dado que `lead_pct` é calculado, então para qualquer dimensão a `SUM(lead_pct)` está no intervalo `[0.99, 1.01]` (tolerância de arredondamento).
- **AC-005**: Dado `dimension_value` nulo na Gold Lead, quando o Macro for gerado, então o valor aparece como `"sem_informacao"` no Macro.
- **AC-006**: Dado o Macro gerado, quando inspecionado, então nenhuma coluna com nome contendo `lead_key`, `contact_ref` ou `name_masked` está presente.
- **AC-007**: Dado que `run_pipeline.py` é executado, quando concluído, então `data/gold/conversations_gold_macro.parquet` existe no filesystem.
- **AC-008**: Dado o Macro gerado, quando `validate_gold_macro()` for chamado, então retorna `status = "passed"` para todos os checks obrigatórios.
- **AC-009**: Dado `dimension = "numeric_snapshot"` e `dimension_value = "total_leads"`, quando o Macro for gerado, então `lead_count` corresponde ao número de linhas em `conversations_gold.parquet`.

## 6. Test Automation Strategy

- **Test Levels**: Unit (build_gold_macro), Integration (run_cycle end-to-end com assertiva no artefato Macro)
- **Frameworks**: pytest, pandas, pathlib
- **Test Data**: Fixture `silver_leads_df` e `silver_messages_df` existentes em `tests/conftest.py`; derivar `gold_df` via `build_gold()` e passar para `build_gold_macro()`
- **Cobertura mínima**: Todos os 11 dimensions; nulos em dimension_value; proporções sommando 1.0; ausência de PII
- **Arquivo de teste**: `tests/test_transforms.py` (adicionar seção `TestBuildGoldMacro`) ou arquivo separado `tests/test_gold_macro.py`
- **Teste de integração**: `tests/test_requirements_adherence.py` — adicionar assertiva que `data/gold/conversations_gold_macro.parquet` existe após `run_cycle()`

## 7. Rationale & Context

A ausência de uma visão macro força qualquer análise agregada a ser feita fora do pipeline, ad hoc, sem reprodutibilidade garantida. Um pipeline medalhão completo deve produzir tanto visões operacionais por entidade (Gold Lead, já existente) quanto visões analíticas de toda a base (Gold Macro, esta spec). O artefato Macro é o que permite responder "qual provedor de email domina a base?" ou "quantos leads estão em cada audiência?" sem acessar microdados individuais — o que também melhora o perfil de privacidade da entrega.

## 8. Dependencies & External Integrations

### Technology Platform Dependencies
- **PLT-001**: Python 3.11, pandas — mesmas dependências do pipeline existente.

### Data Dependencies
- **DAT-001**: `data/gold/conversations_gold.parquet` — artefato de entrada; deve existir antes da chamada a `build_gold_macro()`.

## 9. Examples & Edge Cases

```python
# Exemplo de saída esperada para dimension = "persona_profile"
# (base hipotética de 1000 leads)
#
# dimension           dimension_value          lead_count  lead_pct  rank
# persona_profile     lead_frio                450         0.45      1
# persona_profile     lead_engajado_com_dados  300         0.30      2
# persona_profile     cotador_comparador       200         0.20      3
# persona_profile     cliente_pos_sinistro      50         0.05      4

# Edge case: dominant_email_provider com NaN (lead sem email)
# O filtro contains_email=True remove esses leads antes do groupby.
# Se 0 leads têm email, a dimensão dominant_email_provider aparece com
# dimension_value = "sem_informacao", lead_count = 0, lead_pct = 0.0, rank = 1.
```

## 10. Validation Criteria

- `validate_gold_macro(df, compiled_plan)` retorna `status = "passed"` para:
  - Colunas obrigatórias presentes (`required_columns`)
  - Sem duplicatas em `(dimension, dimension_value)`
  - `lead_pct` entre 0.0 e 1.0 para todas as linhas
  - `lead_count >= 0` para todas as linhas
  - `rank >= 1` para todas as linhas
  - Nenhum `None`/`NaN` em `dimension` ou `dimension_value`
  - Ausência de colunas proibidas (`CON-001`, `CON-002`)
  - Todas as dimensions de REQ-010 a REQ-020 presentes + `numeric_snapshot`

## 11. Related Specifications / Further Reading

- `spec-architecture-semantic-contract-separation.md` — separação entre camadas de contrato
- `spec-architecture-pipeline-validation-contract-alignment.md` — padrão de validações por layer
- `config/pipeline_spec.json` — contrato declarativo central do pipeline
