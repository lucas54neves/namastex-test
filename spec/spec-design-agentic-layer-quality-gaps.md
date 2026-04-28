---
title: Agentic Layer Quality Gaps — Targeted Tests Gate, Aggregation Logic, Remediation Duality, LLM Integration Tests
version: 1.0
date_created: 2026-04-28
owner: Data Engineering
tags: [architecture, agent, quality, testing, autonomy, gold, remediation]
---

# Introduction

Esta especificação endereça quatro lacunas de qualidade identificadas na camada agêntica após a implementação dos GAP-01 a GAP-04 originais. As lacunas afetam a confiabilidade do gate de autonomia, a corretude semântica de derivações Gold, a auditabilidade da remediação reativa e a cobertura de testes dos paths LLM. Nenhuma das correções altera contratos públicos de artefatos ou schema de dados.

## 1. Purpose & Scope

**Propósito:** Definir os requisitos de implementação para corrigir quatro problemas identificados na camada agêntica atual:

1. **QUAL-01** — O gate `targeted_tests` em `autonomy.py:evaluate_candidate()` retorna sempre `passed=True, executed=False`. Nenhum teste real é executado antes de promover um candidato, tornando o gate ineficaz como barreira de qualidade.
2. **QUAL-02** — `apply_gold_column_plan()` em `gold_designer.py` copia a coluna source para todos os `agg_fn` (sum, mean, max, min, count) sem aplicar a função de agregação. A Gold já está no nível de lead, mas o comportamento é semanticamente incorreto e enganoso.
3. **QUAL-03** — Existe dualidade não documentada entre dois mecanismos de remediação: a remediação reativa (`agent.py:attempt_auto_remediation`) opera em memória sem materializar candidatos; o ciclo proativo (`autonomy.py`) materializa e governa com auditoria completa. A dualidade não é capturada em nenhuma interface ou documentação, tornando o comportamento difícil de auditar.
4. **QUAL-04** — Os paths LLM de `llm_advisor.py`, `execution_planner.py` e `gold_designer.py` não têm testes de integração com mock. Os paths de fallback determinístico não são testados explicitamente como substitutos dos paths LLM.

**Escopo:** Apenas os arquivos listados abaixo. Sem alterações em schema parquet, `pipeline_spec.json`, contratos de publicação ou testes de transformação existentes.

**Arquivos afetados:**
- `src/pipeline/agent/autonomy.py`
- `src/pipeline/agent/gold_designer.py`
- `src/pipeline/agent/agent.py`
- `src/pipeline/orchestration/operator.py`
- `tests/test_autonomy_candidate.py` (novo)
- `tests/test_gold_designer.py` (existente — expandir)
- `tests/test_llm_integration.py` (novo)

**Público-alvo:** Engenheiros implementando as correções e revisores de PR.

## 2. Definitions

| Termo | Definição |
|---|---|
| Gate | Verificação determinística que deve passar para que um candidato seja promovido |
| targeted_tests | Gate em `evaluate_candidate()` que deveria executar um subconjunto da suíte de testes antes da promoção |
| agg_fn | Função de agregação declarada em `GoldColumnDefinition.derivation_logic` para tipo `aggregation` |
| Remediação reativa | Ciclo de remediação em `agent.py:attempt_auto_remediation()` — ativado após falha de validação no ReAct loop |
| Remediação proativa | Ciclo de autonomia em `autonomy.py:plan_pipeline_spec()` — ativado a cada ciclo para detectar drift e propor melhorias |
| Materialização de candidato | Persistência isolada de artefatos de uma proposta em `runtime/candidates/<proposal_id>/` antes de qualquer promoção |
| Fallback determinístico | Implementação que opera sem LLM e é ativada quando o provider está desabilitado ou falha |
| Integration test com mock | Teste que isola a fronteira com o LLM substituindo `call_llm` por `unittest.mock.patch` |

## 3. Requirements, Constraints & Guidelines

### QUAL-01 — Gate `targeted_tests` com execução real

- **REQ-101**: `evaluate_candidate()` em `autonomy.py` DEVE executar um subconjunto real da suíte de testes quando há uma suíte disponível e o candidato altera colunas obrigatórias (`required_columns`) ou regras de validação (`quality.validation_rules`).
- **REQ-102**: O gate DEVE executar `pytest` de forma programática usando `subprocess.run` com o ambiente virtual local (`venv/bin/python -m pytest`) sobre os testes definidos em `compiled_plan["agent"].get("targeted_test_paths", [])`.
- **REQ-103**: O campo `targeted_tests.executed` DEVE ser `True` quando os testes foram efetivamente executados; `False` apenas quando não há testes configurados ou quando a mudança do candidato não afeta áreas cobertas.
- **REQ-104**: O campo `targeted_tests.passed` DEVE refletir o resultado real da execução (`returncode == 0`). Se a execução falhar (`returncode != 0`), o gate DEVE ser marcado `passed=False`.
- **REQ-105**: Quando `targeted_test_paths` não estiver configurado na `pipeline_spec.json`, o gate DEVE retornar `executed=False, passed=True` com `reason="no_targeted_tests_configured"` — comportamento atual preservado como opt-in default.
- **REQ-106**: O timeout da execução de testes DEVE ser configurável via `compiled_plan["agent"].get("targeted_tests_timeout_sec", 120)`. Se excedido, o gate DEVE retornar `passed=False` com `reason="timeout"`.
- **CON-101**: A execução de testes DEVE ocorrer em um processo separado (`subprocess.run`) para não contaminar o processo do pipeline com estado de teste.
- **CON-102**: O gate NÃO DEVE executar testes para propostas com `impact_class="low"` e `mutation_family="validation_enhancement"`, onde o risco de regressão é mínimo.
- **CON-103**: A interface de `evaluate_candidate()` NÃO deve mudar; apenas a implementação interna do gate `targeted_tests`.
- **GUD-101**: O campo `targeted_tests` no `gate_results` DEVE sempre incluir `executed`, `passed`, `reason`, `returncode` (ou `None` se não executado) e `test_paths_run`.

### QUAL-02 — Corretude semântica de `aggregation` em `apply_gold_column_plan`

- **REQ-201**: `apply_gold_column_plan()` DEVE aplicar a `agg_fn` correta sobre a coluna source quando o tipo de derivação é `aggregation` e os dados já estão no nível de agregação da Gold (um registro por lead).
- **REQ-202**: Para `agg_fn="sum"` e `agg_fn="count"`: a coluna result DEVE ser uma cópia da coluna source (`result[col_def.name] = result[source_col]`), documentando explicitamente que a agregação já ocorreu na Silver — comportamento atual, mas com comentário e sem o branch morto de outros `agg_fn`.
- **REQ-203**: Para `agg_fn="mean"` e `agg_fn="avg"`: se `source_col` for numérica e o DataFrame tiver uma coluna de contagem associada (`source_col + "_count"` ou `total_messages`), DEVE calcular a média ponderada; caso contrário, DEVE copiar `source_col` e logar `log_event(WARNING, "gold_agg_mean_no_denominator", ...)`.
- **REQ-204**: Para `agg_fn="max"` e `agg_fn="min"`: DEVE simplesmente copiar a coluna source, pois o máximo/mínimo de um valor escalar é o próprio valor — mas DEVE ser explicitamente documentado via `reason` no log.
- **REQ-205**: Para `agg_fn="count"`: se `source_col` contém contagens pré-agregadas, DEVE copiar; se `source_col` contém valores não-nulos para contar (booleanos ou IDs), DEVE somar os valores `True`/não-nulos.
- **REQ-206**: Qualquer `agg_fn` desconhecido DEVE logar `log_event(WARNING, "gold_agg_unknown_fn", agg_fn=agg_fn, column=col_def.name)` e copiar `source_col` como fallback seguro.
- **CON-201**: A interface de `apply_gold_column_plan()` NÃO deve mudar.
- **CON-202**: O comportamento de `agg_fn="sum"` NÃO deve mudar (a Gold já está agregada por lead) — apenas remover o branch morto que produz o mesmo resultado.
- **GUD-201**: O código DEVE conter um comentário explicando que, na Gold, `aggregation` opera sobre valores já pré-agregados por lead vindos da Silver, não sobre linhas individuais de transação.

### QUAL-03 — Unificação da auditoria de remediação reativa e proativa

- **REQ-301**: `attempt_auto_remediation()` em `agent.py` DEVE persistir um artefato de auditoria em `runtime/candidates/reactive_<incident_id>/` quando aplicar pelo menos um playbook. O artefato DEVE conter: lista de playbooks aplicados, `failed_checks` de entrada, snapshot do resultado de validação pós-remediação e timestamp.
- **REQ-302**: O nome do diretório de auditoria da remediação reativa DEVE seguir o padrão `reactive_<incident_id>` para ser distinguível dos candidatos proativos (`proposal_<fingerprint>`).
- **REQ-303**: `attempt_auto_remediation()` DEVE receber `incident_id: str` e `paths: PipelinePaths` como parâmetros adicionais opcionais. Quando ausentes, o comportamento atual (sem persistência) DEVE ser preservado para retrocompatibilidade.
- **REQ-304**: O campo `auto_remediation` no `agent_report` DEVE incluir `candidate_path` quando um artefato de auditoria foi persistido, ou `null` quando a remediação não persistiu artefatos.
- **REQ-305**: `operator.py:run_cycle()` DEVE passar `incident_id` e `paths` para `attempt_auto_remediation()` para que a persistência seja ativada no ciclo real.
- **REQ-306**: A documentação interna (docstrings ou comentários) em `agent.py` e `autonomy.py` DEVE explicitar a distinção entre os dois mecanismos:
  - **Reativo**: ativado após falha de validação; remedia em memória; materializa para auditoria; não altera `pipeline_spec.json`.
  - **Proativo**: ativado a cada ciclo; detecta drift; propõe mudanças estruturais; materializa candidatos; pode alterar `pipeline_spec.json` com aprovação.
- **CON-301**: A persistência de auditoria da remediação reativa DEVE ser um caminho adicional, não substituto. O retorno de `attempt_auto_remediation()` NÃO deve mudar (ainda retorna `dict` com `silver_df`, `gold_df`, `actions`, etc.).
- **CON-302**: A persistência DEVE ser best-effort: falha ao persistir NÃO DEVE impedir a remediação em memória nem causar erro no pipeline.
- **GUD-301**: O diretório `runtime/candidates/` DEVE conter apenas candidatos proativos (`proposal_*`) e artefatos reativos (`reactive_*`), distinguíveis pelo prefixo.

### QUAL-04 — Testes de integração com mock para paths LLM

- **REQ-401**: DEVE existir pelo menos um teste de integração para cada um dos três paths LLM: `get_llm_advice()`, `build_execution_plan()` com LLM, e `design_gold_columns()` com LLM — usando `unittest.mock.patch("pipeline.runtime.llm_runtime.call_llm")`.
- **REQ-402**: Para cada path LLM, DEVE existir um teste cobrindo o cenário de **sucesso**: LLM retorna JSON válido e o comportamento resultante diverge do fallback determinístico.
- **REQ-403**: Para cada path LLM, DEVE existir um teste cobrindo o cenário de **fallback**: LLM retorna JSON inválido (ex: `"not valid json"`) ou lança `Exception`, e o sistema usa o fallback determinístico sem propagar exceção.
- **REQ-404**: Para cada path LLM, DEVE existir um teste cobrindo o cenário de **LLM desabilitado** (`PIPELINE_ENABLE_LLM_ENRICHMENT=0` ou flag específico `False`): o sistema não chama `call_llm` e retorna o fallback diretamente.
- **REQ-405**: Os testes de fallback DEVEM verificar explicitamente que o fallback determinístico produz o mesmo resultado independentemente de o LLM ter sido chamado ou não.
- **REQ-406**: O teste de `design_gold_columns()` com LLM DEVE verificar que colunas retornadas pelo mock LLM com `segment_values=None` em `conditional_bucket` são rejeitadas pelo `_validate_plan()` e não aparecem no plano final.
- **REQ-407**: O teste de `build_execution_plan()` com LLM DEVE verificar que stages inválidas retornadas pelo mock LLM (não presentes em `VALID_STAGES`) são filtradas do plano final.
- **REQ-408**: O teste de `get_llm_advice()` com LLM DEVE verificar que `proposal_type`s inventados pelo mock LLM (não presentes nas proposals de entrada) são ignorados no reordenamento de `plan_pipeline_spec()`.
- **CON-401**: Os novos testes DEVEM usar `venv/bin/python -m pytest` conforme a regra do repositório; NÃO devem depender de providers externos reais.
- **CON-402**: Os mocks DEVEM ser aplicados com `unittest.mock.patch` como context manager ou decorator, não como monkey-patching direto de módulos.
- **CON-403**: Os testes DEVEM ser adicionados em arquivos dedicados (`test_llm_integration.py`) ou em arquivos existentes expandidos, sem modificar testes já passando.
- **GUD-401**: Cada teste de integração LLM DEVE incluir no nome a fonte do path (`_llm_path`, `_fallback_path`, `_disabled_path`) para facilitar triagem em CI.

## 4. Interfaces & Data Contracts

### 4.1 `gate_results["targeted_tests"]` — schema revisado

```python
{
    "passed": bool,             # True somente se returncode == 0 ou executed=False com razão segura
    "executed": bool,           # True se testes foram efetivamente chamados
    "returncode": int | None,   # Código de retorno do subprocess, None se não executado
    "reason": str,              # Motivo de não execução ou descrição do resultado
    "test_paths_run": list[str] # Caminhos de teste executados (vazio se executed=False)
}
```

### 4.2 `attempt_auto_remediation()` — assinatura revisada

```python
def attempt_auto_remediation(
    bronze_df: pd.DataFrame,
    silver_df: pd.DataFrame,
    silver_messages_df: pd.DataFrame,
    gold_df: pd.DataFrame,
    failed_checks: list[dict[str, Any]],
    compiled_plan: dict[str, Any],
    llm_diagnoses: list[AgentDiagnosis] | None = None,
    incident_id: str | None = None,      # NOVO: para nomear o artefato de auditoria
    paths: PipelinePaths | None = None,  # NOVO: para persistir o artefato
) -> dict[str, Any]:
    ...
    # Retorno inclui campo adicional:
    # "candidate_path": str | None  — path do artefato de auditoria ou None
```

### 4.3 Artefato de auditoria de remediação reativa

Persistido em `runtime/candidates/reactive_<incident_id>/reactive_remediation_report.json`:

```json
{
    "incident_id": "<incident_id>",
    "persisted_at_utc": "<ISO timestamp>",
    "mechanism": "reactive",
    "playbooks_applied": ["<playbook_id>", ...],
    "failed_checks_input": [...],
    "post_remediation_validation": {
        "status": "passed | failed",
        "failed_checks": [...]
    },
    "actions": ["<action_name>", ...]
}
```

### 4.4 `apply_gold_column_plan()` — lógica de `aggregation` revisada

```python
elif logic_type == "aggregation":
    source_col = logic.get("source_col", "")
    agg_fn = logic.get("agg_fn", "sum")
    if source_col not in result.columns:
        log_event(WARNING, "gold_plan_missing_source_col", ...)
        result[col_def.name] = None
        continue

    # Gold já está agregada por lead; agg_fn documenta a semântica original
    if agg_fn in ("sum", "count", "max", "min"):
        result[col_def.name] = result[source_col]
    elif agg_fn in ("mean", "avg"):
        count_col = source_col + "_count"
        if count_col in result.columns:
            result[col_def.name] = result[source_col] / result[count_col].replace(0, 1)
        else:
            log_event(WARNING, "gold_agg_mean_no_denominator", ...)
            result[col_def.name] = result[source_col]
    else:
        log_event(WARNING, "gold_agg_unknown_fn", agg_fn=agg_fn, ...)
        result[col_def.name] = result[source_col]
```

### 4.5 Configuração de `targeted_tests` na `pipeline_spec.json`

```json
{
  "agent": {
    "targeted_test_paths": [
      "tests/test_jobs.py"
    ],
    "targeted_tests_timeout_sec": 120,
    "safe_auto_apply_playbooks": ["rebuild_silver_from_bronze", "rebuild_gold_from_silver", "quarantine_invalid_records"]
  }
}
```

Quando `targeted_test_paths` está ausente ou vazio, o gate retorna `executed=False, passed=True, reason="no_targeted_tests_configured"`.

## 5. Acceptance Criteria

### QUAL-01 — Gate `targeted_tests`

- **AC-101**: Dado `targeted_test_paths=["tests/test_jobs.py"]` na spec e um candidato que altera `gold.required_columns`, quando `evaluate_candidate()` é chamado, então `gate_results["targeted_tests"]["executed"]` DEVE ser `True` e `returncode` DEVE ser `0` (se os testes passarem).
- **AC-102**: Dado `targeted_test_paths=[]` (não configurado) na spec, quando `evaluate_candidate()` é chamado, então `gate_results["targeted_tests"]["executed"]` DEVE ser `False` e `passed` DEVE ser `True` com `reason="no_targeted_tests_configured"`.
- **AC-103**: Dado que `subprocess.run` retorna `returncode=1` (falha de teste), quando `evaluate_candidate()` é chamado, então `gate_results["targeted_tests"]["passed"]` DEVE ser `False` e o candidato NÃO DEVE ser promovido.
- **AC-104**: Dado timeout configurado de 5 segundos e testes que demoram mais, quando `evaluate_candidate()` é chamado, então `gate_results["targeted_tests"]["passed"]` DEVE ser `False` com `reason="timeout"`.
- **AC-105**: Dado proposta com `mutation_family="validation_enhancement"` e `impact_class="low"`, quando `evaluate_candidate()` é chamado, então `gate_results["targeted_tests"]["executed"]` DEVE ser `False` com `reason="low_impact_validation_enhancement_skipped"`.

### QUAL-02 — Corretude de `aggregation`

- **AC-201**: Dado uma `GoldColumnDefinition` com `agg_fn="mean"` e `source_col="total_messages"` sem coluna `total_messages_count`, quando `apply_gold_column_plan()` é chamado, então a coluna result DEVE ser uma cópia de `total_messages` e DEVE ser logado `gold_agg_mean_no_denominator`.
- **AC-202**: Dado uma `GoldColumnDefinition` com `agg_fn="mean"`, `source_col="total_messages"` e coluna `total_messages_count` presente, quando `apply_gold_column_plan()` é chamado, então a coluna result DEVE ser `total_messages / total_messages_count` (com proteção contra divisão por zero).
- **AC-203**: Dado `agg_fn="unknown_fn"`, quando `apply_gold_column_plan()` é chamado, então DEVE ser logado `gold_agg_unknown_fn` e a coluna result DEVE ser uma cópia de `source_col`.
- **AC-204**: Dado `agg_fn="sum"` ou `"count"` ou `"max"` ou `"min"`, quando `apply_gold_column_plan()` é chamado, então a coluna result DEVE ser igual à coluna source sem transformação (comportamento atual preservado).
- **AC-205**: Dado `source_col` ausente no DataFrame, quando `apply_gold_column_plan()` é chamado para qualquer `agg_fn`, então a coluna result DEVE ser `None` e DEVE ser logado `gold_plan_missing_source_col`.

### QUAL-03 — Auditoria de remediação reativa

- **AC-301**: Dado que `attempt_auto_remediation()` aplica pelo menos um playbook e recebe `incident_id` e `paths`, quando executado, então DEVE existir um arquivo `runtime/candidates/reactive_<incident_id>/reactive_remediation_report.json` com `playbooks_applied` não vazio.
- **AC-302**: Dado que nenhum playbook é aplicado (todos os checks não têm playbook seguro), quando `attempt_auto_remediation()` é chamado com `incident_id` e `paths`, então NENHUM diretório `reactive_*` DEVE ser criado.
- **AC-303**: Dado que a persistência falha (ex: permissão negada), quando `attempt_auto_remediation()` é chamado, então a remediação em memória DEVE continuar e o retorno DEVE ter `candidate_path=None`.
- **AC-304**: Dado que `incident_id` e `paths` não são fornecidos (chamada legada), quando `attempt_auto_remediation()` é chamado, então o comportamento DEVE ser idêntico ao atual sem criar nenhum artefato.
- **AC-305**: O campo `auto_remediation.candidate_path` no `agent_report` DEVE ser o path do artefato quando persistido, ou `null` quando não persistido.

### QUAL-04 — Testes de integração LLM

- **AC-401**: `test_llm_integration.py` DEVE conter testes para `get_llm_advice` com mock LLM retornando JSON válido, JSON inválido, e com LLM desabilitado — totalizando pelo menos 3 testes para esse path.
- **AC-402**: `test_llm_integration.py` DEVE conter testes para `build_execution_plan` com mock LLM retornando stages válidas, stages inválidas (não em `VALID_STAGES`), e com LLM desabilitado — totalizando pelo menos 3 testes.
- **AC-403**: `test_llm_integration.py` DEVE conter testes para `design_gold_columns` com mock LLM retornando plano válido, plano com `segment_values=None` em `conditional_bucket`, e com LLM desabilitado — totalizando pelo menos 3 testes.
- **AC-404**: O teste de fallback para `design_gold_columns` DEVE verificar que o plano resultante é idêntico ao retorno de `_fallback_gold_plan()` quando o LLM falha.
- **AC-405**: Todos os testes em `test_llm_integration.py` DEVEM passar com `venv/bin/python -m pytest tests/test_llm_integration.py -q` sem acessar providers externos.
- **AC-406**: Testes existentes em `tests/test_gold_designer.py` e `tests/test_jobs.py` DEVEM continuar passando sem modificação após as mudanças de QUAL-02 e QUAL-03.

## 6. Test Automation Strategy

- **Test Levels**: Unitário (funções isoladas com fixtures mínimas) e integração (interação entre componentes com mock de fronteiras externas).
- **Frameworks**: `pytest`, `unittest.mock.patch`, `pandas`, `pytest.raises`, `pytest.mark.parametrize`.
- **Cobertura mínima**: 85% das linhas novas adicionadas pelos 4 fixes.

### Estratégia por QUAL

**QUAL-01 — Gate `targeted_tests`:**
- Testar `evaluate_candidate()` com `compiled_plan` incluindo `targeted_test_paths` e mockando `subprocess.run` com `returncode=0` e `returncode=1`.
- Testar com `targeted_test_paths=[]` para verificar `executed=False`.
- Testar timeout usando `side_effect=subprocess.TimeoutExpired(...)` no mock.
- Não executar `pytest` real nos testes de QUAL-01; usar mock de `subprocess.run`.

**QUAL-02 — Corretude de `aggregation`:**
- Testar `apply_gold_column_plan()` com DataFrames mínimos contendo as colunas necessárias.
- Cobrir: `agg_fn` conhecido com e sem denominador, `agg_fn` desconhecido, `source_col` ausente.
- Verificar que o log correto é emitido em cada cenário.

**QUAL-03 — Auditoria reativa:**
- Testar `attempt_auto_remediation()` com `paths` real usando `tmp_path` do pytest.
- Verificar existência do arquivo de auditoria no diretório temporário.
- Testar comportamento quando `paths=None` (retrocompatibilidade).
- Testar comportamento quando o diretório não pode ser criado (`mock OSError`).

**QUAL-04 — Testes LLM:**
- Usar `unittest.mock.patch("pipeline.runtime.llm_runtime.call_llm")` como context manager.
- Para cenário de JSON inválido, usar `side_effect=lambda *a, **kw: "not valid json"`.
- Para cenário de exceção, usar `side_effect=Exception("timeout")`.
- Para cenário desabilitado, setar env var via `monkeypatch.setenv`.

### CI/CD

- Novos testes DEVEM ser adicionados aos comandos de CI existentes sem configuração adicional.
- `venv/bin/python -m pytest -q` DEVE incluir os novos arquivos automaticamente via descoberta do pytest.
- Testes de QUAL-01 que mockam `subprocess.run` NÃO DEVEM interferir com outros testes que usam processos reais.

## 7. Rationale & Context

### QUAL-01

O gate `targeted_tests` foi projetado como barreira de qualidade antes de promover mudanças estruturais na spec. Com `executed=False, passed=True` permanente, qualquer candidato que passe os outros gates (contrato, backward compatibility, privacy) é automaticamente promovido sem validação de comportamento. Isso significa que uma mutação `low_impact` de tipo `validation_enhancement` com `auto_promote=True` pode ser promovida sem nenhum teste ter rodado — o que contradiz o objetivo declarado de autonomia governada. Executar `tests/test_jobs.py` via subprocess é uma solução simples que não exige framework de teste embutido e mantém isolamento de processo.

### QUAL-02

O código em `gold_designer.py:590-610` tem branches para todos os `agg_fn` mas todos produzem o mesmo resultado (`result[source_col]`). Isso é correto para `sum`, `count`, `max` e `min` porque a Gold já está no nível de lead, mas é semanticamente incorreto para `mean`/`avg` quando existe um denominador. A distinção importa quando o LLM gera colunas do tipo `average_response_time_per_conversation` — o agente deve ser capaz de calcular a média real quando os dados estiverem disponíveis, não apenas copiar o numerador.

### QUAL-03

A dualidade não documentada entre os dois mecanismos de remediação cria problemas de auditoria: quando um pipeline falha e é auto-reparado pela remediação reativa, não existe artefato que comprove o que foi feito, quais playbooks foram aplicados e qual era o estado antes e depois. O ciclo proativo tem materialização completa em `runtime/candidates/`. Alinhar a remediação reativa com uma auditoria mínima (um arquivo JSON no mesmo diretório `candidates/`) resolve o gap sem mudar a semântica nem o desempenho do caminho quente.

### QUAL-04

Os três componentes LLM (`llm_advisor`, `execution_planner`, `gold_designer`) têm fallback determinístico implementado, mas apenas o fallback é exercitado pelos testes existentes. O path LLM só é exercitado com um provider real. Isso significa que bugs de parsing de JSON, filtragem de outputs inválidos e lógica de reordenamento de proposals no path LLM só seriam descobertos em produção. Testes com mock de `call_llm` cobrem esses casos sem dependência de provider externo e sem custo de API.

## 8. Dependencies & External Integrations

### External Systems
- **EXT-001**: Nenhum novo sistema externo. As correções usam apenas componentes já presentes.

### Infrastructure Dependencies
- **INF-001**: `subprocess` stdlib — para execução de `pytest` em processo separado em QUAL-01.
- **INF-002**: `pytest` instalado no ambiente virtual local (`venv/`) — já obrigatório pelo repositório.
- **INF-003**: Diretório `runtime/candidates/` — já criado pela `ensure_directories()`. QUAL-03 adiciona subdiretórios `reactive_*` no mesmo local.

### Technology Platform Dependencies
- **PLT-001**: Python 3.11+ — já obrigatório.
- **PLT-002**: `unittest.mock` stdlib — para QUAL-04; sem nova dependência.
- **PLT-003**: `pandas` — já presente; QUAL-02 usa apenas operações vetorizadas existentes.

### Compliance Dependencies
- **COM-001**: Artefatos de auditoria em `runtime/candidates/reactive_*/` DEVEM seguir a mesma política de privacidade dos candidatos proativos: NÃO persistir colunas PII brutas, apenas metadados de validação.

## 9. Examples & Edge Cases

### QUAL-01 — Configuração mínima de `targeted_tests` na spec

```json
// config/pipeline_spec.json — trecho relevante
{
  "agent": {
    "targeted_test_paths": ["tests/test_jobs.py"],
    "targeted_tests_timeout_sec": 120
  }
}
```

```python
# Resultado esperado em gate_results quando testes passam:
{
    "targeted_tests": {
        "passed": True,
        "executed": True,
        "returncode": 0,
        "reason": "tests_passed",
        "test_paths_run": ["tests/test_jobs.py"]
    }
}

# Resultado esperado quando testes falham:
{
    "targeted_tests": {
        "passed": False,
        "executed": True,
        "returncode": 1,
        "reason": "tests_failed",
        "test_paths_run": ["tests/test_jobs.py"]
    }
}
```

### QUAL-02 — Comportamento de `mean` com denominador

```python
# DataFrame Gold (já agregado por lead):
# | lead_key | total_response_time | total_response_time_count |
# | lead_001 | 450                 | 5                          |
# | lead_002 | 200                 | 2                          |

col_def = GoldColumnDefinition(
    name="avg_response_time",
    data_type="float64",
    derivation_logic={
        "type": "aggregation",
        "agg_fn": "mean",
        "source_col": "total_response_time"
    },
    rationale="Tempo médio de resposta por lead.",
    segment_values=None,
)

# Resultado esperado:
# | lead_key | avg_response_time |
# | lead_001 | 90.0              |  # 450 / 5
# | lead_002 | 100.0             |  # 200 / 2
```

### QUAL-03 — Artefato de auditoria reativa

```json
// runtime/candidates/reactive_incident_20260428T142300/reactive_remediation_report.json
{
    "incident_id": "incident_20260428T142300",
    "persisted_at_utc": "2026-04-28T14:23:01.123456+00:00",
    "mechanism": "reactive",
    "playbooks_applied": ["rebuild_silver_from_bronze"],
    "failed_checks_input": [
        {"layer": "silver", "check": "lead_key_unique", "status": "failed"}
    ],
    "post_remediation_validation": {
        "status": "passed",
        "failed_checks": []
    },
    "actions": ["rebuild_silver_for_lead_key_unique"]
}
```

### QUAL-04 — Teste de integração com mock LLM

```python
# tests/test_llm_integration.py
from unittest.mock import patch
import json
import pytest
from pipeline.agent.llm_advisor import get_llm_advice

def _make_compiled_plan(enabled=True):
    return {"llm": {"enabled": enabled, "provider": "openai"}, "agent": {}}

def test_get_llm_advice_llm_path_success():
    mock_response = json.dumps({
        "priority_proposals": ["gold_business_hours_metric_addition"],
        "deferred_proposals": ["metadata_key_normalization_rule"],
        "risk_flags": ["potential schema drift detected"],
        "summary": "Prioritize Gold metric addition."
    })
    context = {
        "observed_columns": ["conversation_id", "message_body"],
        "observed_metadata_fields": ["is_business_hours"],
        "detected_contexts": [{"context_type": "bronze_observation_summary"}],
        "proposals": [
            {"proposal_type": "gold_business_hours_metric_addition", "proposal_family": "derived_column_addition", "expected_impact": "adds metric"},
            {"proposal_type": "metadata_key_normalization_rule", "proposal_family": "transformation_rule_change", "expected_impact": "normalizes keys"},
        ]
    }
    with patch("pipeline.runtime.llm_runtime.call_llm", return_value=mock_response):
        result = get_llm_advice(context, _make_compiled_plan(enabled=True))
    assert result["status"] == "ok"
    assert "gold_business_hours_metric_addition" in result["priority_proposals"]
    assert "metadata_key_normalization_rule" in result["deferred_proposals"]

def test_get_llm_advice_fallback_path_on_invalid_json():
    with patch("pipeline.runtime.llm_runtime.call_llm", return_value="not valid json"):
        result = get_llm_advice({}, _make_compiled_plan(enabled=True))
    assert result["status"] == "llm_failed"
    assert result["priority_proposals"] == []
    assert result["deferred_proposals"] == []

def test_get_llm_advice_disabled_path():
    result = get_llm_advice({}, _make_compiled_plan(enabled=False))
    assert result["status"] == "disabled"
    assert result["priority_proposals"] == []
```

### QUAL-01 — Edge case: `low_impact validation_enhancement` — testes pulados

```python
# Proposta com mutation_family="validation_enhancement" e impact_class="low"
# Gate targeted_tests deve ser pulado sem executar subprocess

proposal = {
    "proposal_family": "validation_enhancement",
    "impact_class": "low",
    ...
}

# Resultado esperado:
gate_results["targeted_tests"] = {
    "passed": True,
    "executed": False,
    "returncode": None,
    "reason": "low_impact_validation_enhancement_skipped",
    "test_paths_run": []
}
```

## 10. Validation Criteria

- **VAL-001**: `venv/bin/python -m pytest tests/test_autonomy_candidate.py -q` DEVE passar após QUAL-01.
- **VAL-002**: `venv/bin/python -m pytest tests/test_gold_designer.py -q` DEVE passar após QUAL-02 sem modificar testes existentes.
- **VAL-003**: `venv/bin/python -m pytest tests/test_llm_integration.py -q` DEVE passar após QUAL-04 sem acessar providers externos.
- **VAL-004**: `venv/bin/python -m pytest -q` (suíte completa) DEVE passar após todos os 4 fixes.
- **VAL-005**: O campo `targeted_tests.executed` DEVE ser `True` em `gate_results` persistidos em `runtime/candidates/<proposal_id>/candidate_run_report.json` quando `targeted_test_paths` está configurado.
- **VAL-006**: O diretório `runtime/candidates/` após um ciclo com remediação reativa DEVE conter pelo menos um subdiretório com prefixo `reactive_` quando um playbook foi aplicado.
- **VAL-007**: `grep -r "\.eval(" src/pipeline/agent/gold_designer.py` DEVE retornar zero resultados após QUAL-02 (confirmar que `DataFrame.eval()` foi removido, se presente).
- **VAL-008**: Nenhum teste em `tests/test_llm_integration.py` DEVE fazer HTTP requests reais (verificável via `pytest-socket` ou inspeção manual dos mocks).
- **VAL-009**: O campo `auto_remediation.candidate_path` DEVE aparecer em `latest_agent_report.json` em todos os cenários onde `attempt_auto_remediation()` é chamado pelo `run_cycle()`.

## 11. Related Specifications / Further Reading

- [spec-architecture-agentic-layer-gap-remediation.md](./spec-architecture-agentic-layer-gap-remediation.md) — Especificação dos GAP-01 a GAP-04 originais já implementados
- [spec-architecture-agent-autonomy-governed-by-impact.md](./spec-architecture-agent-autonomy-governed-by-impact.md) — Design do sistema de autonomia governada por impacto
- [spec-architecture-agentic-pipeline-decisions.md](./spec-architecture-agentic-pipeline-decisions.md) — Decisões arquiteturais do agente
- [spec-architecture-llm-provider-runtime-integration.md](./spec-architecture-llm-provider-runtime-integration.md) — Integração com providers LLM
