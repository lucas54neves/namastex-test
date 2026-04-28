---
title: Agentic Layer Gap Remediation — ReAct Integration, LLM Remediação, Safe Eval, LLM Advisor
version: 1.0
date_created: 2026-04-27
owner: Data Engineering
tags: [architecture, agent, react, remediation, security, llm]
---

# Introduction

Esta especificação endereça quatro lacunas identificadas na camada agêntica do pipeline de transformação medalha. Cada lacuna compromete a autonomia declarada do agente ou introduz risco de segurança. As correções são incrementais e retrocompatíveis com o contrato de artefatos existente.

## 1. Purpose & Scope

**Propósito:** Definir os requisitos de implementação para corrigir quatro problemas estruturais na camada agêntica (`src/pipeline/agent/` e `src/pipeline/orchestration/operator.py`):

1. **GAP-01** — O loop ReAct em `run_cycle()` não controla a execução de stages; as stages rodam fora e antes do loop, tornando o `ExecutionPlan` ineficaz.
2. **GAP-02** — Falhas diagnosticadas pelo LLM (`playbook_id=None`) nunca disparam remediação automática; o diagnóstico existe mas não fecha o ciclo de ação.
3. **GAP-03** — `apply_gold_column_plan()` usa `pandas.DataFrame.eval()` com expressões geradas pelo LLM, criando superfície de injeção de código.
4. **GAP-04** — `get_llm_advice()` em `llm_advisor.py` é um stub que retorna `not_implemented` e nunca influencia decisões do `plan_pipeline_spec()`.

**Escopo:** Apenas os arquivos listados abaixo. Sem alterações no contrato público de artefatos (schema parquet), no `pipeline_spec.json` ou nos testes de transformação existentes.

**Arquivos afetados:**
- `src/pipeline/orchestration/operator.py`
- `src/pipeline/agent/agent.py`
- `src/pipeline/agent/gold_designer.py`
- `src/pipeline/agent/llm_advisor.py`
- `src/pipeline/agent/playbooks.py` (leitura)
- `tests/test_execution_planner.py`
- `tests/test_gold_designer.py`
- `tests/test_jobs.py`

**Público-alvo:** Engenheiros implementando as correções e revisores de PR.

## 2. Definitions

| Termo | Definição |
|---|---|
| ReAct Loop | Ciclo Reason-Act: observa estado, decide ação, executa, valida — repete até completar ou haltar |
| ExecutionPlan | Estrutura produzida por `build_execution_plan()` listando quais stages precisam rodar e por quê |
| Stage | Unidade de execução do pipeline: `bronze`, `silver`, `gold`, `validation`, `planning` |
| Playbook | Ação de remediação pré-definida com nível de risco e flag `safe_auto_apply` |
| LLM Diagnosis | Diagnóstico produzido pelo modelo de linguagem para falhas fora do `VALIDATION_CHECK_MAP` |
| AgentDiagnosis | Estrutura dataclass unificada com `kind`, `severity`, `auto_remediable`, `playbook_id` |
| LLM Advisor | Componente que gera recomendações de evolução de spec baseado em contexto observado |
| safe_eval | Avaliador restrito de expressões booleanas sem acesso a `builtins`, `__import__` ou lambdas |
| conditional_bucket | Tipo de derivação de coluna Gold baseado em condições sequenciais avaliadas sobre o DataFrame |
| Proposal | Proposta de mutação de spec gerada pelo Planner, com família, impacto e status de aprovação |
| circuit_breaker | Contador que desativa chamadas LLM após N falhas consecutivas, revertendo ao fallback |

## 3. Requirements, Constraints & Guidelines

### GAP-01 — ReAct Loop Integrado à Execução de Stages

- **REQ-101**: O `run_cycle()` DEVE mover a execução de stages (bronze, silver, gold) para dentro do corpo do loop ReAct, controlada pelo `ExecutionPlan` e por `decide_loop_action()`.
- **REQ-102**: O `ExecutionPlan.stages` DEVE ser consumido iterativamente: a cada iteração do loop, o agente executa a próxima stage pendente ou valida o resultado.
- **REQ-103**: O `decide_loop_action()` DEVE retornar `run_stage` apontando para a stage atual do plano enquanto houver stages pendentes e validação não tiver passado.
- **REQ-104**: O `decide_loop_action()` DEVE retornar `retry_stage` quando a stage falhou e `stage_failure_counts[stage] < 2`.
- **REQ-105**: O `decide_loop_action()` DEVE retornar `halt` quando `stage_failure_counts[stage] >= 2` ou quando diagnóstico for `critical`.
- **REQ-106**: O `decide_loop_action()` DEVE retornar `complete` quando todas as stages do plano foram executadas e a validação passou.
- **REQ-107**: O estado de stages executadas DEVE ser rastreado em `executed_stages: set[str]` dentro do loop para evitar re-execução desnecessária.
- **CON-101**: O `_MAX_REACT_ITERATIONS` DEVE ser igual ao número máximo de stages possíveis (5) mais o número de retries por stage (2 × número de stages), ou seja, valor mínimo de 10.
- **CON-102**: A interface pública `run_cycle(paths, force)` NÃO deve mudar; apenas a implementação interna.
- **CON-103**: A ordem de execução de stages DEVE ser preservada: `planning → bronze → silver → gold → validation`.
- **GUD-101**: O loop DEVE logar cada `LoopAction` via `log_event()` com campos `kind`, `stage`, `reason`, `iteration`.

### GAP-02 — Ponte entre Diagnóstico LLM e Remediação

- **REQ-201**: Quando `AgentDiagnosis.playbook_id is None` e `auto_remediable is True` (diagnóstico LLM com `confidence >= 0.80` e `severity != "critical"`), o agente DEVE mapear o `kind` do diagnóstico para um playbook genérico de fallback seguro.
- **REQ-202**: DEVE existir um `LLM_KIND_TO_PLAYBOOK_MAP: dict[str, str]` mapeando `kind`s de diagnóstico LLM para `playbook_id`s seguros. O mapa inicial DEVE cobrir pelo menos:
  - `"unknown_validation_failure"` → `"rebuild_silver_from_bronze"`
  - `"silver_*"` (qualquer kind iniciando com `silver_`) → `"rebuild_silver_from_bronze"`
  - `"gold_*"` (qualquer kind iniciando com `gold_`) → `"rebuild_gold_from_silver"`
  - `"pii_masking_leak"` → `"rebuild_silver_from_bronze"`
- **REQ-203**: `attempt_auto_remediation()` DEVE aceitar `llm_diagnoses: list[AgentDiagnosis]` como parâmetro adicional e aplicar `LLM_KIND_TO_PLAYBOOK_MAP` para diagnósticos sem `playbook_id`.
- **REQ-204**: Ações de remediação disparadas via mapeamento LLM DEVE ser registradas em `actions` com sufixo `_via_llm_kind_map` para rastreabilidade.
- **REQ-205**: O `auto_remediable` de um `AgentDiagnosis` baseado em LLM DEVE ser `True` apenas se: `is_safe_to_auto_apply=True`, `confidence >= 0.80`, `severity != "critical"` E o `kind` mapeado tiver `safe_auto_apply=True` no `PLAYBOOKS`.
- **CON-201**: Nenhum playbook com `safe_auto_apply=False` DEVE ser aplicado automaticamente, mesmo via mapeamento LLM.
- **CON-202**: O `LLM_KIND_TO_PLAYBOOK_MAP` DEVE ser definido em `agent.py` próximo ao `VALIDATION_CHECK_MAP`, não em `playbooks.py`.
- **GUD-201**: O mapeamento DEVE usar correspondência por prefixo (`startswith`) para kinds `silver_*` e `gold_*`, com fallback para `exact match`.

### GAP-03 — Safe Evaluator para Conditional Buckets

- **REQ-301**: `apply_gold_column_plan()` DEVE substituir `result.eval(cond["when"])` por um avaliador seguro (`safe_eval_condition`) que restringe as operações permitidas.
- **REQ-302**: O `safe_eval_condition(expr: str, df: pd.DataFrame) -> pd.Series[bool]` DEVE:
  - Suportar operadores: `==`, `!=`, `>`, `>=`, `<`, `<=`, `and`, `or`, `not`, `in`
  - Suportar referências a colunas do DataFrame por nome (sem aspas adicionais)
  - Suportar literais: strings (aspas simples ou duplas), inteiros, floats, booleanos (`True`, `False`)
  - Suportar valores nulos via `pd.isna()` quando a coluna tiver NaN
- **REQ-303**: O `safe_eval_condition` DEVE rejeitar qualquer expressão que contenha: `import`, `exec`, `eval`, `__`, `lambda`, `open`, `os`, `sys`, chamadas de função não permitidas.
- **REQ-304**: Em caso de expressão inválida ou rejeitada, `safe_eval_condition` DEVE retornar uma `pd.Series` de `False` (toda a condição é ignorada) e logar `log_event(WARNING, "gold_plan_unsafe_expr_rejected", ...)`.
- **REQ-305**: `safe_eval_condition` DEVE ser implementado sem `eval()` ou `exec()` do Python; DEVE usar `ast.parse()` + visitor para validação e `pandas` vectorizado para avaliação.
- **CON-301**: O avaliador NÃO precisa suportar operações aritméticas complexas (`+`, `-`, `*`, `/`), funções de string (`str.contains`), nem acesso a índice.
- **CON-302**: O avaliador DEVE funcionar sem dependências externas além de `ast`, `pandas` e `re` (já presentes).
- **CON-303**: A interface de `apply_gold_column_plan()` NÃO muda; apenas a implementação da linha `result.eval(cond["when"])`.
- **GUD-301**: Expressões do LLM devem ser normalizadas antes da validação: `strip()`, remoção de espaços extras, substituição de `True`/`False` Python para garantir parsing AST correto.

### GAP-04 — LLM Advisor Implementado

- **REQ-401**: `get_llm_advice()` em `llm_advisor.py` DEVE fazer uma chamada real ao LLM quando `PIPELINE_ENABLE_LLM_ADVISOR=true` e credenciais estiverem disponíveis.
- **REQ-402**: O prompt enviado ao LLM DEVE incluir: `observed_columns`, `observed_metadata_fields`, `detected_contexts`, resumo de proposals (tipo, família, impacto), e instruções para retornar JSON estruturado.
- **REQ-403**: O output do LLM Advisor DEVE seguir o schema:
  ```json
  {
    "priority_proposals": ["<proposal_type>", ...],
    "deferred_proposals": ["<proposal_type>", ...],
    "risk_flags": ["<free-text flag>", ...],
    "summary": "<one sentence>"
  }
  ```
- **REQ-404**: `plan_pipeline_spec()` DEVE consumir `llm_advice["priority_proposals"]` para reordenar a lista de proposals antes da avaliação de candidatos: proposals priorizadas pelo LLM DEVEM ser avaliadas primeiro.
- **REQ-405**: `plan_pipeline_spec()` DEVE usar `llm_advice["deferred_proposals"]` para marcar proposals como `PROPOSAL_STATUS_CLOSED_NO_ACTION` sem avaliação, registrando `decision_reason="deferred_by_llm_advisor"`.
- **REQ-406**: `get_llm_advice()` DEVE usar `call_llm()` de `pipeline.runtime.llm_runtime` com timeout de 15s e fallback: se a chamada falhar, retornar `{"status": "llm_failed", "priority_proposals": [], "deferred_proposals": [], ...}` sem lançar exceção.
- **REQ-407**: O retorno de `get_llm_advice()` DEVE sempre incluir o campo `"status"` com valor `"ok"`, `"llm_failed"`, `"disabled"` ou `"not_implemented"` para diagnóstico.
- **CON-401**: A feature DEVE ser ativada apenas via `PIPELINE_ENABLE_LLM_ADVISOR=true`; o comportamento padrão (flag ausente ou `false`) DEVE ser idêntico ao atual.
- **CON-402**: `get_llm_advice()` NÃO deve lançar exceções; todas as falhas DEVEM ser tratadas internamente com retorno degradado.
- **CON-403**: O LLM Advisor DEVE usar `call_llm()` diretamente (single-shot), não `run_conversation_enrichment_graph()`.
- **GUD-401**: O prompt do LLM Advisor DEVE ser uma constante em `llm_advisor.py` (`_LLM_ADVISOR_PROMPT`) com placeholders `{context_json}`.

## 4. Interfaces & Data Contracts

### 4.1 LoopAction (sem mudança de interface)

```python
@dataclass(frozen=True)
class LoopAction:
    kind: Literal["run_stage", "retry_stage", "halt", "complete"]
    stage: str | None  # stage name when kind in ("run_stage", "retry_stage", "halt")
    reason: str
```

### 4.2 `decide_loop_action()` — assinatura revisada

```python
def decide_loop_action(
    execution_plan: ExecutionPlan,
    observation: Observation,
    stage_failure_counts: dict[str, int],
    iteration: int,
    validation_passed: bool,
    executed_stages: set[str],          # NOVO: stages já executadas neste ciclo
    failed_stage: str | None = None,
) -> LoopAction:
    ...
```

**Lógica de decisão revisada:**
1. Se `validation_passed` → `complete`
2. Se todas as stages do plano estão em `executed_stages` e não `validation_passed` → `complete` (forçar validação final)
3. Para cada stage no `execution_plan.stages` em ordem:
   - Se stage não está em `executed_stages` → `run_stage(stage)`
   - Se stage está em `executed_stages` e `failed_stage == stage` e `count < 2` → `retry_stage(stage)`
   - Se stage está em `executed_stages` e `failed_stage == stage` e `count >= 2` → `halt(stage)`
4. Se nenhuma stage restante → `complete`

### 4.3 `attempt_auto_remediation()` — assinatura revisada

```python
def attempt_auto_remediation(
    bronze_df: pd.DataFrame,
    silver_df: pd.DataFrame,
    silver_messages_df: pd.DataFrame,
    gold_df: pd.DataFrame,
    failed_checks: list[dict[str, Any]],
    compiled_plan: dict[str, Any],
    llm_diagnoses: list[AgentDiagnosis] | None = None,  # NOVO: diagnósticos LLM
) -> dict[str, Any]:
    ...
```

### 4.4 `LLM_KIND_TO_PLAYBOOK_MAP` em `agent.py`

```python
LLM_KIND_TO_PLAYBOOK_MAP: dict[str, str] = {
    "unknown_validation_failure": "rebuild_silver_from_bronze",
    "pii_masking_leak": "rebuild_silver_from_bronze",
    "silver_schema_break": "rebuild_silver_from_bronze",
    "silver_lead_identity_break": "rebuild_silver_from_bronze",
    "silver_timestamp_parse_failure": "quarantine_invalid_records",
    "silver_aggregate_corruption": "rebuild_silver_from_bronze",
    "silver_deduplication_failure": "rebuild_silver_from_bronze",
    "silver_feature_inconsistency": "rebuild_silver_from_bronze",
    "silver_publication_policy_violation": "rebuild_silver_from_bronze",
    "silver_publication_schema_break": "rebuild_silver_from_bronze",
    "gold_schema_break": "rebuild_gold_from_silver",
    "gold_aggregation_duplication": "rebuild_gold_from_silver",
    "gold_metric_corruption": "rebuild_gold_from_silver",
    "gold_publication_policy_violation": "rebuild_gold_from_silver",
    "gold_bucket_invalid": "rebuild_gold_from_silver",
    # prefixos genéricos — lookup via startswith antes do exact match
}

_LLM_KIND_PREFIX_MAP: list[tuple[str, str]] = [
    ("silver_", "rebuild_silver_from_bronze"),
    ("gold_", "rebuild_gold_from_silver"),
]
```

### 4.5 `safe_eval_condition()` — interface

```python
def safe_eval_condition(expr: str, df: pd.DataFrame) -> pd.Series:
    """
    Avalia uma expressão booleana restrita sobre um DataFrame pandas.
    Retorna pd.Series[bool]. Em caso de expressão inválida, retorna Series de False.
    Não usa eval() ou exec() internamente.
    """
    ...
```

**Gramática suportada (informal):**

```
expr     ::= comparison ( ('and' | 'or') comparison )*
comparison ::= column_ref op literal
             | column_ref 'in' list_literal
             | 'not' comparison
op       ::= '==' | '!=' | '>' | '>=' | '<' | '<='
column_ref ::= NAME  (deve existir em df.columns)
literal    ::= STRING | NUMBER | 'True' | 'False' | 'None'
list_literal ::= '[' literal (',' literal)* ']'
```

**Tokens rejeitados imediatamente (blocklist):**
```python
BLOCKLIST = {"import", "exec", "eval", "__", "lambda", "open", "os", "sys",
             "subprocess", "globals", "locals", "getattr", "setattr", "delattr"}
```

### 4.6 `get_llm_advice()` — interface revisada

```python
def get_llm_advice(
    context: dict[str, Any],
    compiled_plan: dict[str, Any],
) -> dict[str, Any]:
    """
    Retorna recomendações do LLM Advisor sobre o conjunto de proposals.
    Nunca lança exceção. Status sempre presente no retorno.
    """
    # Retorno mínimo:
    return {
        "enabled": bool,
        "provider": str | None,
        "status": "ok" | "llm_failed" | "disabled" | "not_implemented",
        "priority_proposals": list[str],   # proposal_types a avaliar primeiro
        "deferred_proposals": list[str],   # proposal_types a adiar/fechar
        "risk_flags": list[str],           # alertas textuais livres
        "summary": str,
        "context_keys": list[str],
    }
```

## 5. Acceptance Criteria

### GAP-01 — ReAct Loop

- **AC-101**: Dado um `ExecutionPlan` com `stages=["silver", "gold", "validation"]`, quando `run_cycle()` executa, então as stages `silver` e `gold` DEVEM ser executadas dentro do loop ReAct, não antes.
- **AC-102**: Dado um `ExecutionPlan` com `stages=[]` (source inalterado, artefatos existentes, validação passou), quando `run_cycle()` executa, então nenhuma stage DEVE ser executada e o status DEVE ser `skipped_no_source_change`.
- **AC-103**: Dado que a stage `silver` falhou 2 vezes, quando `decide_loop_action()` é chamado, então DEVE retornar `LoopAction(kind="halt", stage="silver", reason="repeated_stage_failure:silver")`.
- **AC-104**: Dado que todas as stages do plano foram executadas com sucesso e a validação passou, quando `decide_loop_action()` é chamado, então DEVE retornar `LoopAction(kind="complete", ...)`.
- **AC-105**: O campo `react_loop_action` no `agent_summary` do `run_record` DEVE refletir a ação final do loop em todos os cenários.

### GAP-02 — Remediação LLM

- **AC-201**: Dado um `AgentDiagnosis` com `playbook_id=None`, `kind="silver_feature_inconsistency"`, `auto_remediable=True`, quando `attempt_auto_remediation()` é chamado com `llm_diagnoses=[diag]`, então o playbook `rebuild_silver_from_bronze` DEVE ser aplicado e `actions` DEVE conter `"rebuild_silver_for_llm_kind_silver_feature_inconsistency_via_llm_kind_map"`.
- **AC-202**: Dado um `AgentDiagnosis` com `kind="gold_aggregation_duplication"` via LLM, quando remediado, então o playbook `rebuild_gold_from_silver` DEVE ser aplicado.
- **AC-203**: Dado um `AgentDiagnosis` com `confidence=0.75` (abaixo de 0.80), quando `attempt_auto_remediation()` é chamado, então nenhum playbook DEVE ser aplicado para esse diagnóstico.
- **AC-204**: Dado um `AgentDiagnosis` com `severity="critical"`, quando `attempt_auto_remediation()` é chamado, então nenhum playbook DEVE ser aplicado para esse diagnóstico.
- **AC-205**: Dado um kind desconhecido `"totally_unknown_kind"` sem correspondência no mapa, quando remediado, então nenhum playbook DEVE ser aplicado e o diagnóstico DEVE permanecer `auto_remediable=False`.

### GAP-03 — Safe Evaluator

- **AC-301**: Dado a expressão `"engagement_bucket == 'lead_frio' and data_shared_score == 0"`, quando `safe_eval_condition()` é chamado com um DataFrame válido, então DEVE retornar uma `pd.Series[bool]` correta sem chamar `eval()` ou `exec()`.
- **AC-302**: Dado a expressão `"__import__('os').system('rm -rf /')"`, quando `safe_eval_condition()` é chamado, então DEVE retornar `pd.Series` de `False` e logar `gold_plan_unsafe_expr_rejected`.
- **AC-303**: Dado a expressão `"lambda x: x"`, quando `safe_eval_condition()` é chamado, então DEVE retornar `pd.Series` de `False` e logar `gold_plan_unsafe_expr_rejected`.
- **AC-304**: Dado uma expressão com coluna inexistente `"coluna_que_nao_existe == 'valor'"`, quando `safe_eval_condition()` é chamado, então DEVE retornar `pd.Series` de `False` e logar `gold_plan_missing_column`.
- **AC-305**: Dado a expressão `"mentioned_sinistro and contains_cpf"` com colunas booleanas, quando `safe_eval_condition()` é chamado, então DEVE retornar `pd.Series[bool]` equivalente a `df["mentioned_sinistro"] & df["contains_cpf"]`.

### GAP-04 — LLM Advisor

- **AC-401**: Dado `PIPELINE_ENABLE_LLM_ADVISOR=true` e credenciais disponíveis, quando `get_llm_advice()` é chamado, então `status` DEVE ser `"ok"` e `priority_proposals` DEVE ser uma lista (possivelmente vazia).
- **AC-402**: Dado `PIPELINE_ENABLE_LLM_ADVISOR=false` (ou ausente), quando `get_llm_advice()` é chamado, então `status` DEVE ser `"disabled"` e o comportamento de `plan_pipeline_spec()` DEVE ser idêntico ao atual.
- **AC-403**: Dado que `priority_proposals=["gold_business_hours_metric_addition"]` e `deferred_proposals=["metadata_key_normalization_rule"]`, quando `plan_pipeline_spec()` processa proposals, então `gold_business_hours_metric_addition` DEVE ser avaliado antes de `metadata_key_normalization_rule`, e `metadata_key_normalization_rule` DEVE ter status `PROPOSAL_STATUS_CLOSED_NO_ACTION` com `decision_reason="deferred_by_llm_advisor"`.
- **AC-404**: Dado que a chamada LLM falha (timeout ou provider error), quando `get_llm_advice()` é chamado, então DEVE retornar `{"status": "llm_failed", "priority_proposals": [], "deferred_proposals": [], ...}` sem lançar exceção, e `plan_pipeline_spec()` DEVE continuar com a ordem original de proposals.
- **AC-405**: O campo `llm_advice.status` DEVE aparecer no relatório `latest_plan_report.json` em todos os cenários.

## 6. Test Automation Strategy

- **Test Levels**: Unitário (cada função isolada) e integração (ciclo completo via `run_cycle()`).
- **Frameworks**: `pytest`, `unittest.mock.patch`, `pandas` para fixtures de DataFrame.
- **Cobertura mínima**: 90% das linhas novas adicionadas pelos 4 fixes.

### Estratégia por GAP

**GAP-01:**
- Testar `decide_loop_action()` com todos os cenários de `executed_stages` (vazio, parcial, completo).
- Testar `run_cycle()` com mock de `build_silver()` e `build_gold()` para verificar que são chamados dentro do loop, não antes.
- Testar que `_MAX_REACT_ITERATIONS` é respeitado.

**GAP-02:**
- Testar `attempt_auto_remediation()` com `llm_diagnoses` cobrindo: kind mapeado, kind por prefixo, kind desconhecido, confidence baixa, severity crítica.
- Testar que o sufixo `_via_llm_kind_map` aparece em `actions`.

**GAP-03:**
- Testar `safe_eval_condition()` com: expressões válidas (comparação simples, `and/or`, `in`), expressões bloqueadas (cada token da blocklist), colunas ausentes, literais de diferentes tipos.
- Testar que `apply_gold_column_plan()` não usa `DataFrame.eval()` (verificar via AST do código ou mock).

**GAP-04:**
- Testar `get_llm_advice()` com `PIPELINE_ENABLE_LLM_ADVISOR=true` e mock de `call_llm()`: sucesso, timeout, JSON inválido.
- Testar `plan_pipeline_spec()` com mock de `get_llm_advice()` retornando `priority_proposals` e `deferred_proposals` não vazios; verificar ordem de processamento e status final das proposals.

### CI/CD

- Todos os testes existentes DEVEM continuar passando sem modificação.
- Novos testes DEVEM ser adicionados em `tests/test_execution_planner.py`, `tests/test_gold_designer.py`, e `tests/test_jobs.py`.

## 7. Rationale & Context

### GAP-01

O `run_cycle()` atual executa as stages antes do loop, tornando o `ExecutionPlan` uma peça decorativa: ele é gerado e logado, mas não controla o fluxo. O ReAct pattern exige que a decisão de ação preceda a execução — `Observe → Reason → Act`. Mover a execução para dentro do loop habilita: (a) skip de stages desnecessárias com base no plano, (b) retry granular por stage, (c) halt preemptivo antes de stages downstream quando upstream falhou.

### GAP-02

O LLM é chamado para diagnosticar falhas desconhecidas mas o resultado nunca fecha o ciclo: o agente diagnostica, descreve a ação recomendada em texto, e para. Isso reduz a autonomia declarada do sistema para cenários de falha não mapeados — exatamente os mais críticos. O mapeamento `LLM_KIND_TO_PLAYBOOK_MAP` conecta o diagnóstico semântico à ação estruturada sem precisar de playbooks novos — reutiliza os seguros já existentes.

### GAP-03

`pandas.DataFrame.eval()` executa código Python arbitrário quando `parser='python'` (default). Expressões geradas por LLM não são confiáveis e podem conter injeções. Um avaliador baseado em `ast.parse()` + visitor permite validação estrutural da expressão antes de qualquer avaliação, eliminando a superfície de ataque sem sacrificar a expressividade necessária para conditional buckets.

### GAP-04

O LLM Advisor foi arquitetado como ponto de decisão de alto nível sobre quais proposals priorizar, mas permaneceu como stub. Sem ele, o planner avalia proposals em ordem arbitrária de detecção — o que pode resultar em avaliar proposals de baixo valor antes de proposals críticas, ou avaliar proposals que deveriam ser adiadas. O LLM Advisor fecha esse gap com custo incremental (uma chamada single-shot por ciclo de planejamento).

## 8. Dependencies & External Integrations

### External Systems
- **EXT-001**: LLM Provider (Anthropic ou OpenAI) — usado para GAP-02 (diagnosis), GAP-04 (LLM Advisor). Disponível via `call_llm()` já implementado em `llm_runtime.py`.

### Infrastructure Dependencies
- **INF-001**: `PIPELINE_ENABLE_LLM_ADVISOR` env var — flag de feature para GAP-04; valor padrão `false`.
- **INF-002**: `ANTHROPIC_API_KEY` ou `OPENAI_API_KEY` — credenciais existentes; nenhuma nova variável necessária para GAP-01, GAP-02 e GAP-03.

### Technology Platform Dependencies
- **PLT-001**: Python `ast` stdlib — módulo padrão para validação de expressões em GAP-03; sem nova dependência.
- **PLT-002**: `pandas` — já presente; usado para avaliação vectorizada em GAP-03.

## 9. Examples & Edge Cases

### GAP-01 — Fluxo do Loop Revisado

```
Iteração 0: observe() → ExecutionPlan(stages=["bronze","silver","gold","validation"])
             decide_loop_action() → run_stage("bronze")
             execute bronze_load + quarantine + bronze_persist
             executed_stages = {"bronze"}

Iteração 1: observe() → decide_loop_action() → run_stage("silver")
             execute silver_build + silver_persist
             executed_stages = {"bronze", "silver"}

Iteração 2: observe() → decide_loop_action() → run_stage("gold")
             execute gold_design + gold_build + gold_persist
             executed_stages = {"bronze", "silver", "gold"}

Iteração 3: observe() → decide_loop_action() → run_stage("validation")
             execute validation_suite → failed_checks = [...]
             execute diagnose + attempt_auto_remediation → resolved=True
             decide_loop_action() → complete (validation_passed=True)
```

### GAP-03 — safe_eval_condition edge cases

```python
# Válido: comparação simples
safe_eval_condition("engagement_bucket == 'lead_frio'", df)
# → df["engagement_bucket"] == "lead_frio"

# Válido: compound
safe_eval_condition("mentioned_sinistro and contains_cpf", df)
# → df["mentioned_sinistro"] & df["contains_cpf"]

# Válido: operador in
safe_eval_condition("engagement_bucket in ['media', 'longa']", df)
# → df["engagement_bucket"].isin(["media", "longa"])

# BLOQUEADO: injeção via dunder
safe_eval_condition("__class__.__bases__[0]", df)
# → Series(False), log WARNING gold_plan_unsafe_expr_rejected

# BLOQUEADO: lambda
safe_eval_condition("(lambda: None)()", df)
# → Series(False), log WARNING gold_plan_unsafe_expr_rejected

# Edge: coluna com NaN em comparação
safe_eval_condition("avg_quoted_price == avg_quoted_price", df)
# → ~df["avg_quoted_price"].isna()  (convenção pandas NaN != NaN)
```

### GAP-04 — LLM Advisor Prompt (constante)

```python
_LLM_ADVISOR_PROMPT = """
You are a data pipeline planning advisor.
Review the following pipeline planning context and return a structured recommendation.

## Context
{context_json}

## Output Format (JSON only, no prose)
{{
  "priority_proposals": ["<proposal_type>", ...],
  "deferred_proposals": ["<proposal_type>", ...],
  "risk_flags": ["<free text>", ...],
  "summary": "<one sentence>"
}}

Rules:
- priority_proposals: proposal_types that should be evaluated first this cycle
- deferred_proposals: proposal_types that should be deferred (closed without action) this cycle
- A proposal_type can appear in only one list or in neither
- Return only proposal_types that appear in the context; do not invent new ones
- risk_flags: free-text warnings about observed risks (max 3)
"""
```

## 10. Validation Criteria

- **VAL-001**: `run_cycle()` com source inalterado e artefatos existentes DEVE terminar em < 2s sem chamar `build_silver()` ou `build_gold()`.
- **VAL-002**: `run_cycle()` com source alterado DEVE chamar `build_silver()` e `build_gold()` exatamente uma vez por estágio de execução planejado.
- **VAL-003**: `safe_eval_condition()` NÃO DEVE conter as strings `"eval("`, `"exec("` no seu próprio código-fonte.
- **VAL-004**: `get_llm_advice()` com LLM desabilitado DEVE retornar em < 5ms (sem I/O).
- **VAL-005**: Todos os testes existentes em `tests/` DEVEM passar sem modificação após as mudanças.
- **VAL-006**: O campo `react_loop_action` DEVE estar presente em `latest_agent_report.json` após qualquer execução de `run_cycle()`.
- **VAL-007**: O campo `llm_advice.status` DEVE estar presente em `latest_plan_report.json` após qualquer execução de `plan_pipeline_spec()`.
- **VAL-008**: Nenhuma ação de remediação com sufixo `_via_llm_kind_map` DEVE aparecer se o diagnóstico original tinha `confidence < 0.80`.

## 11. Related Specifications / Further Reading

- [spec-architecture-agentic-pipeline-decisions.md](./spec-architecture-agentic-pipeline-decisions.md)
- [spec-architecture-agent-autonomy-governed-by-impact.md](./spec-architecture-agent-autonomy-governed-by-impact.md)
- [spec-architecture-llm-provider-runtime-integration.md](./spec-architecture-llm-provider-runtime-integration.md)
- [spec-architecture-pipeline-validation-contract-alignment.md](./spec-architecture-pipeline-validation-contract-alignment.md)
