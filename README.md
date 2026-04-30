# Entrega Final: Pipeline Medalhao com Camada Agentica

Este repositorio implementa a entrega final do teste descrito em [docs/technical-test-data-ai-engineering.md](/home/lucas/projects/lucas54neves/namastex-test/namastex-test-1/docs/technical-test-data-ai-engineering.md). A solucao foi desenhada para atender o ponto central do enunciado: nao apenas gerar analises sobre conversas de WhatsApp, mas construir uma infraestrutura persistente em Python que ingere dados transacionais, publica camadas Bronze -> Silver -> Gold e opera com uma camada agentica responsavel por monitorar, planejar remediacoes, propor evolucoes e manter a Gold atualizada conforme a fonte cresce.

O projeto separa claramente a transformacao de dados da governanca operacional. O pipeline produz artefatos analiticos reprocessaveis e a camada agentica observa o ciclo, registra diagnosticos, materializa candidatos de mudanca, respeita politicas de impacto e preserva o ultimo estado valido quando uma promocao nao e segura.

## O que esta sendo entregue

O enunciado pede quatro capacidades principais:

1. pipeline em Python puro
2. arquitetura em 3 camadas
3. pipeline vivo, com atualizacao automatica quando a fonte muda
4. agente que cria e gerencia a operacao, incluindo deteccao de falhas e acoes corretivas ou propostas governadas

Esta entrega responde a isso com os seguintes blocos:

- `Bronze`: replica controlada da fonte original `docs/conversations_bronze.parquet`
- `Silver`: limpeza, normalizacao, deduplicacao, mascaramento e organizacao por lead
- `Gold`: visao analitica por lead e visao macro agregada
- `Camada agentica`: monitoramento, planejamento, propostas, isolamento de candidatos, promocao segura, alertas e fallback
- `Execucao continua`: daemon com polling e CDC por `message_id` para decidir quando reprocessar

## Como a arquitetura funciona

### Camadas de dados

- `Bronze` preserva a estrutura da origem em `data/bronze/conversations.parquet`
- `Silver` publica:
  - `data/silver/silver_messages.parquet`
  - `data/silver/silver_leads.parquet`
  - `data/silver/silver_conversations_llm.parquet`
- `Gold` publica:
  - `data/gold/conversations_gold.parquet`
  - `data/gold/conversations_gold_macro.parquet`

### Camada agentica

A camada agentica nao substitui o pipeline; ela governa o pipeline. Na pratica, ela observa os artefatos e os contratos de execucao, detecta desvios e decide qual acao e segura dentro da politica configurada em `config/agent_autonomy_policy.json`.

Os principais componentes ficam em `src/pipeline/agent/`:

- `planner.py`: detecta gaps, drift e oportunidades de evolucao
- `autonomy.py`: aplica a politica de impacto e decide se algo pode ser promovido automaticamente
- `approval.py`: controla o caminho de aprovacao humana para mudancas de alto impacto
- `alerts.py` e `alert_channels.py`: registram e entregam alertas
- `playbooks.py`: organiza respostas operacionais padrao
- `gold_designer.py` e `llm_advisor.py`: apoiam propostas estruturadas de melhoria analitica

## Ciclo operacional da camada agentica

O funcionamento esperado pelo teste pode ser resumido neste fluxo:

1. o runtime le a fonte Bronze e calcula o estado atual do dataset
2. o operador decide se houve mudanca real usando CDC por `message_id`
3. se houve mudanca, Bronze -> Silver -> Gold sao reprocessadas
4. validacoes de qualidade, schema e publicacao segura sao executadas
5. a camada agentica consolida diagnosticos, drift e sinais operacionais
6. se houver oportunidade de remediacao ou evolucao, o agente gera uma proposal
7. a proposal e materializada isoladamente em `runtime/candidates/<proposal_id>/`
8. a politica classifica o impacto como `low`, `medium` ou `high`
9. somente mudancas dentro do envelope seguro podem ser promovidas automaticamente
10. mudancas estruturais ou de alto impacto ficam em espera para aprovacao explicita
11. se a promocao falhar ou violar o contrato, o pipeline preserva o ultimo estado valido e registra fallback

Esse desenho separa autonomia de permissao. O agente pode diagnosticar e preparar a mudanca sozinho, mas a promocao depende do nivel de risco. Isso e o ponto central da governanca desta entrega.

## O que o agente faz de forma autonoma

- detecta mudancas na fonte e evita reprocessamento inutil em ciclos ociosos
- executa monitoramento e gera snapshots operacionais
- identifica schema drift e classifica o tipo de desvio
- gera proposals estruturadas para evolucao de contrato e camada analitica
- materializa candidatos isolados para validacao segura
- promove apenas mudancas permitidas pela politica de impacto
- dispara alertas locais ou por webhook quando encontra estados criticos

## O que continua governado

- promocao de mudancas `high impact`
- alteracoes estruturais de contrato que exigem aprovacao humana
- exposicao de novas colunas quando a privacy gate bloquear promocao
- qualquer situacao em que o candidato nao comprove seguranca suficiente para substituir o estado atual

## Artefatos que demonstram a operacao agentica

Os artefatos abaixo mostram o comportamento da camada agentica durante a execucao:

- `reports/monitoring/latest_run_report.json`: resumo do ultimo ciclo
- `reports/monitoring/latest_plan_report.json`: plano gerado pelo planner
- `reports/monitoring/latest_schema_drift_report.json`: drift detectado e politica aplicada
- `reports/monitoring/agent_autonomy_metrics.json`: metricas de autonomia e bloqueios
- `reports/monitoring/latest_agent_report.json`: consolidado do agente
- `reports/monitoring/latest_alert_report.json`: resultado da emissao de alertas
- `reports/agent_decisions/proposals/`: historico de propostas geradas
- `reports/agent_decisions/autonomy/latest_autonomy_decision.json`: ultima decisao de promocao ou retencao
- `runtime/candidates/`: materializacao isolada de candidatos
- `state/pipeline_state.json`: estado persistido do operador e CDC
- `state/approval_state.json`: estado de aprovacoes humanas quando aplicavel

## Como o projeto atende o enunciado

| Requisito do teste | Resposta da entrega |
| --- | --- |
| Python puro | Implementacao em `src/pipeline/` e `scripts/` |
| Pipeline em 3 camadas | Publicacao em `data/bronze/`, `data/silver/` e `data/gold/` |
| Pipeline vivo | Daemon com polling e CDC linha a linha por `message_id` |
| Agente autonomo | Planejamento, diagnostico, proposals, alertas e fallback |
| Atualizacao automatica da Gold | Reprocessamento quando a Bronze muda |
| Dados sensiveis mascarados | Regras de masking e publication policy nas camadas publicadas |

## Estrutura principal do repositorio

```text
src/pipeline/
  agent/          autonomia, aprovacao, alertas, playbooks e planejamento
  orchestration/  operador, jobs publicos, reports e compilacao de spec
  transforms/     bronze, silver, enrichment, gold e gold macro
  quality/        validacoes, publication policy, quarantine e schema drift
  runtime/        ambiente, estado, runtime opcional de LLM e observabilidade
  io/             leitura e escrita de parquet/json
  infrastructure/ integracao opcional com Databricks

scripts/
  run_pipeline.py
  run_pipeline_daemon.py
  monitor_pipeline.py
  plan_pipeline.py
  databricks_deploy_run.py
```

## Execucao local

### Pre-requisitos

- Python 3.11
- ambiente virtual local em `venv/`
- dependencias instaladas a partir de `requirements.txt`

### Setup

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
cp .env.example .env
```

Se quiser instalar os hooks locais:

```bash
venv/bin/python -m pre_commit install
venv/bin/python -m pre_commit install --hook-type pre-push
```

### Execucao baseline

O baseline da entrega nao depende de credenciais externas. O enrichment semantico pode operar em fallback deterministico.

```bash
PIPELINE_ENABLE_LLM_ENRICHMENT=0 venv/bin/python scripts/run_pipeline.py --force
```

Principais saidas esperadas:

- `data/bronze/conversations.parquet`
- `data/silver/silver_messages.parquet`
- `data/silver/silver_leads.parquet`
- `data/silver/silver_conversations_llm.parquet`
- `data/gold/conversations_gold.parquet`
- `data/gold/conversations_gold_macro.parquet`
- artefatos de monitoramento em `reports/monitoring/`

### Execucao continua

Para manter o pipeline vivo:

```bash
PIPELINE_ENABLE_LLM_ENRICHMENT=0 venv/bin/python scripts/run_pipeline_daemon.py --force-first-run --poll-interval-seconds 300
```

Nesse modo, o daemon:

- compara o estado atual da Bronze com o ultimo snapshot persistido
- pula ciclos ociosos quando nao houve mudanca real
- reprocessa as camadas quando entram novos `message_id` ou quando ha encolhimento da fonte
- aplica watchdog com backoff exponencial em caso de falha
- executa o planner em cadencia controlada para detectar drift mesmo em janelas estaveis

### Monitoramento e planejamento

```bash
venv/bin/python scripts/monitor_pipeline.py
venv/bin/python scripts/plan_pipeline.py
```

## Schema evolution e governanca

Um dos papeis mais importantes da camada agentica e evitar que colunas novas aparecam e sumam sem controle. O pipeline registra schema drift em `reports/monitoring/latest_schema_drift_report.json` e classifica eventos como:

- `expected`
- `optional_known`
- `unknown`
- `missing_required`
- `type_mismatch`
- `category_drift`

Com base nisso, a politica pode:

- propagar silenciosamente
- propagar com alerta
- colocar linhas em quarantine
- bloquear a execucao

Quando a coluna nova se mostra estavel, o agente pode propor promocao de contrato em uma escada governada:

1. `Bronze optional`
2. `Silver preserve`
3. `Gold optional` ou `Gold passthrough`
4. `Gold Macro dimension`

Promocoes barradas por risco ou privacidade nao entram automaticamente em producao; ficam registradas para decisao humana.

## Enrichment com LLM

O projeto suporta dois modos:

- `baseline deterministico`: recomendado para validacao local e entrega sem dependencias externas
- `llm-enriched`: usa provider externo para classificacoes semanticas adicionais

Mesmo com `PIPELINE_ENABLE_LLM_ENRICHMENT=1`, o contrato preve fallback automatico para regras deterministicas quando o provider falha ou retorna payload invalido. Isso impede que a Gold fique indisponivel por dependencia externa.

Variaveis mais relevantes:

- `PIPELINE_ENABLE_LLM_ENRICHMENT`
- `OPENAI_API_KEY`
- `ANTHROPIC_API_KEY`
- `PIPELINE_ENABLE_LANGFUSE`
- `PIPELINE_LLM_OPENAI_MODEL`
- `PIPELINE_LLM_ANTHROPIC_MODEL`

## Alertas

Por padrao, os alertas sao persistidos localmente em `reports/alerts/`. Opcionalmente, o agente pode entregar eventos por webhook definindo `PIPELINE_ALERT_WEBHOOK_URL`.

Exemplo:

```bash
PIPELINE_ALERT_WEBHOOK_URL=https://hooks.slack.com/services/T000/B000/xxx \
PIPELINE_ALERT_WEBHOOK_TOKEN=token \
venv/bin/python scripts/run_pipeline.py --force
```

A entrega do webhook e non-blocking: falha no canal de alerta nao interrompe o pipeline.

## Databricks

O repositorio inclui suporte opcional para deploy e execucao no Databricks, incluindo automacao por `scripts/databricks_deploy_run.py` e testes dedicados. Isso atende ao diferencial sugerido no enunciado, mas nao e obrigatorio para validar o baseline local.

## Testes

Use sempre o ambiente virtual local do repositorio:

```bash
venv/bin/python -m pytest -q
venv/bin/python -m pytest tests/test_jobs.py -q
```

## Referencias

- [Enunciado do teste](/home/lucas/projects/lucas54neves/namastex-test/namastex-test-1/docs/technical-test-data-ai-engineering.md)
- [Dicionario de dados](/home/lucas/projects/lucas54neves/namastex-test/namastex-test-1/docs/data-dictionary-data-ai-engineering.md)
- [Spec da autonomia governada por impacto](/home/lucas/projects/lucas54neves/namastex-test/namastex-test-1/spec/spec-architecture-agent-autonomy-governed-by-impact.md)
- [Spec de schema evolution](/home/lucas/projects/lucas54neves/namastex-test/namastex-test-1/spec/spec-architecture-schema-evolution-detection-and-propagation.md)
- [Spec da escada de promocao agentica](/home/lucas/projects/lucas54neves/namastex-test/namastex-test-1/spec/spec-architecture-agentic-schema-promotion-ladder.md)
