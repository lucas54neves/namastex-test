# Pipeline Medalhão com Agente Autônomo Governado por Impacto

Entrega final do teste técnico de Data & AI Engineering descrito em [docs/technical-test-data-ai-engineering.md](/home/lucas/projects/lucas54neves/namastex-test/docs/technical-test-data-ai-engineering.md). O projeto implementa um pipeline em Python para transformar conversas transacionais de WhatsApp em uma arquitetura Bronze -> Silver -> Gold, com atualização automática da camada analítica quando a fonte cresce e um agente governado por impacto para diagnóstico, proposta estruturada, candidate materialization, promoção segura, aprovação explícita e fallback.

## Objetivo da entrega

O enunciado pede mais do que uma análise pontual: pede uma infraestrutura persistente que permaneça "viva", gerencie o pipeline, monitore falhas e mantenha a camada Gold atualizada. A solução entregue atende esse objetivo com os seguintes blocos:

- Pipeline em Python puro, organizado em camadas claras.
- Bronze como réplica controlada da fonte original.
- Silver como camada de limpeza, normalização, deduplicação, mascaramento e organização por lead.
- Gold como camada analítica para segmentação, personas, audiência, sinais comerciais e sentimento.
- Agente governado por impacto para classificar propostas em `low`/`medium`/`high`, materializar candidatos isolados, promover mudanças seguras, solicitar aprovação para mutações estruturais e preservar o último estado íntegro.
- Execução pontual e execução contínua com polling para reprocessar automaticamente quando a Bronze muda.

## Como a solução responde ao enunciado

| Requisito do teste | Como foi atendido |
| --- | --- |
| Python puro | Implementação em `src/pipeline/` e `scripts/` |
| Pipeline em 3 camadas | Publicação em `data/bronze/`, `data/silver/` e `data/gold/` |
| Pipeline vivo | Fingerprint da fonte em `state/pipeline_state.json` e daemon em `scripts/run_pipeline_daemon.py` |
| Agente que gerencia o pipeline | Diagnóstico, planejamento, alerta, fallback e playbooks em `src/pipeline/agent/` |
| Limpeza, transformação e análise | Regras em `src/pipeline/transforms/` e validações em `src/pipeline/quality/` |
| Mascaramento de dados sensíveis | Política de publicação segura e validações anti-vazamento em Silver e Gold |

## Arquitetura do projeto

### Visão em camadas

- `docs/conversations_bronze.parquet` é a fonte transacional de entrada usada como Bronze raw source.
- `Bronze` replica a fonte para uma área controlada de processamento.
- `Silver` gera três artefatos: uma visão principal por lead, uma visão auxiliar por mensagem e uma visão intermediária por conversa para enriquecimento semântico.
- `Gold` consolida métricas e classificações analíticas por lead.
- O agente observa o ciclo, valida contratos, registra relatórios, materializa candidatos em `runtime/candidates/`, aciona alertas e executa fallback quando necessário.

### Diagrama mermaid

```mermaid
flowchart TD
    A[docs/conversations_bronze.parquet] --> B[Bronze Ingestion]
    B --> C[data/bronze/conversations.parquet]
    C --> D[Silver Transform]
    D --> E[data/silver/silver_messages.parquet]
    D --> F[data/silver/silver_leads.parquet]
    E --> G[Conversation Enrichment]
    G --> H[data/silver/silver_conversations_llm.parquet]
    F --> I[Gold Aggregation]
    E --> I
    H --> I
    I --> J[data/gold/conversations_gold.parquet]

    K[config/pipeline_spec.json] --> D
    K --> G
    K --> I

    L[Agent Planner + Diagnostics] --> K
    L --> M[reports/monitoring]
    L --> N[reports/alerts]
    L --> O[reports/agent_decisions]
    L --> Q[runtime/candidates]
    L --> P[state/pipeline_state.json]
    I --> L
    D --> L
    G --> L
```

### Organização do código

```text
src/pipeline/
  agent/          diagnostico, aprovacao, alertas, playbooks e planejamento
  io/             leitura/escrita parquet e json
  orchestration/  ciclo de execucao, jobs publicos e compilacao da spec
  quality/        validacoes, politica de publicacao e quarentena
  runtime/        ambiente, estado, spec, Langfuse e runtime opcional de LLM
  transforms/     transformacoes Bronze, Silver, Gold e enrichment por conversa

scripts/
  run_pipeline.py
  run_pipeline_daemon.py
  monitor_pipeline.py
  plan_pipeline.py
  bootstrap_langfuse_prompt.py
```

## Fluxo de dados

### Bronze

- Lê `docs/conversations_bronze.parquet`.
- Replica a fonte para `data/bronze/conversations.parquet`.
- Valida colunas obrigatórias, canal suportado e consistência básica.
- Trata `first_message_outbound` como sinal diagnóstico, não como bloqueio hard, para preservar fidelidade da fonte real.

### Silver

Publica três artefatos:

- `data/silver/silver_leads.parquet`: visão principal por `lead_key`.
- `data/silver/silver_messages.parquet`: rastreabilidade por mensagem com deduplicação e campos mascarados.
- `data/silver/silver_conversations_llm.parquet`: enriquecimento semântico por `conversation_id`.

Principais responsabilidades:

- organização por lead
- parsing de `metadata`
- deduplicação por chave semântica estável
- extração de sinais de contato, veículo, concorrente e sinistro
- mascaramento de PII mantendo dimensão do valor original
- publicação apenas de colunas seguras

### Gold

Publica `data/gold/conversations_gold.parquet` com uma visão analítica por lead. Entre os atributos calculados estão:

- volume e distribuição de mensagens
- bucket de engajamento
- persona e audiência
- lead temperature
- intent stage
- price sensitivity
- contact readiness
- competitor pressure
- commercial urgency
- dominant email provider
- closure outcome group
- conversation sentiment

## Como rodar o projeto

### Pré-requisitos

- Python 3.11
- ambiente virtual local em `venv/`
- dependências instaladas a partir de `requirements.txt`

### Setup local

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
cp .env.example .env
```

### Execução local baseline

O caminho baseline do repositório não depende de rede nem de credenciais externas. Ele usa fallback determinístico para o enrichment semântico.

```bash
PIPELINE_ENABLE_LLM_ENRICHMENT=0 venv/bin/python scripts/run_pipeline.py --force
```

Saídas principais após a execução:

- `data/bronze/conversations.parquet`
- `data/silver/silver_leads.parquet`
- `data/silver/silver_messages.parquet`
- `data/silver/silver_conversations_llm.parquet`
- `data/gold/conversations_gold.parquet`
- `reports/monitoring/latest_run_report.json`
- `reports/monitoring/latest_plan_report.json`
- `reports/monitoring/agent_autonomy_metrics.json`
- `reports/monitoring/latest_agent_report.json`
- `reports/monitoring/latest_alert_report.json`
- `reports/agent_decisions/proposals/`
- `reports/agent_decisions/autonomy/latest_autonomy_decision.json`
- `runtime/candidates/<proposal_id>/`
- `state/pipeline_state.json`

### Runtime com paths configuráveis

O pipeline continua assumindo o layout local do repositório por padrão, mas agora também aceita overrides por ambiente para separar código de storage persistente. Isso permite executar o mesmo `scripts/run_pipeline.py` em Databricks usando Workspace Files para código e Unity Catalog Volumes para entrada e saídas.

Variáveis de path suportadas:

- `PIPELINE_INPUT_FILE`: arquivo parquet bruto de entrada
- `PIPELINE_DATA_DIR`: raiz persistente de `bronze/`, `silver/`, `gold/` e `quarantine/`
- `PIPELINE_REPORTS_DIR`: raiz persistente de relatórios operacionais
- `PIPELINE_STATE_DIR`: raiz persistente de estado do pipeline
- `PIPELINE_RUNTIME_DIR`: raiz persistente de `runtime/candidates/`
- `PIPELINE_CONFIG_DIR`: diretório opcional para `pipeline_spec.json` e `agent_autonomy_policy.json`

Sem essas variáveis, o comportamento local atual permanece o mesmo.

### Execução contínua

Para manter o pipeline vivo com polling periódico:

```bash
PIPELINE_ENABLE_LLM_ENRICHMENT=0 venv/bin/python scripts/run_pipeline_daemon.py --force-first-run --poll-interval-seconds 300
```

Comportamento:

- o runtime calcula fingerprint da fonte a partir de path, tamanho e `mtime`
- se nada mudou, a execução é pulada
- se a fonte cresceu ou mudou, Bronze, Silver e Gold são reprocessadas
- o estado operacional é persistido em `state/pipeline_state.json`

### Monitoramento e planejamento

```bash
venv/bin/python scripts/monitor_pipeline.py
venv/bin/python scripts/plan_pipeline.py
```

- `monitor_pipeline.py` consolida snapshot operacional do pipeline.
- `plan_pipeline.py` roda o planner de autonomia que detecta drift, classifica impacto, materializa candidatos e promove apenas mudanças permitidas por política.

### Autonomia governada por impacto

- A política canônica fica em `config/agent_autonomy_policy.json`.
- Propostas estruturadas são persistidas em `reports/agent_decisions/proposals/`.
- Cada candidato é materializado de forma isolada em `runtime/candidates/<proposal_id>/`.
- A última decisão de promoção ou retenção fica em `reports/agent_decisions/autonomy/latest_autonomy_decision.json`.
- Métricas do ciclo autônomo ficam em `reports/monitoring/agent_autonomy_metrics.json`.
- Mudanças `high impact` nunca são auto-promovidas; ficam em `awaiting_approval` até registro em `state/approval_state.json`.

### Execução com Docker Compose

Há suporte opcional a `docker-compose.yml` para subir o pipeline em modo contínuo junto com uma stack local de Langfuse.

```bash
docker compose up --build
```

Esse modo é útil para observabilidade e integração com providers, mas não é necessário para o baseline de validação local.

## Variáveis de ambiente relevantes

As variáveis estão exemplificadas em [`.env.example`](/home/lucas/projects/lucas54neves/namastex-test/.env.example). As mais importantes para a entrega são:

- `PIPELINE_ENABLE_LLM_ENRICHMENT`: habilita ou desabilita enrichment por provider externo.
- `PIPELINE_POLL_INTERVAL_SECONDS`: intervalo do daemon.
- `PIPELINE_ENABLE_LANGFUSE`: habilita observabilidade do enrichment.
- `OPENAI_API_KEY`: credencial opcional para provider OpenAI.
- `ANTHROPIC_API_KEY`: credencial opcional para provider Anthropic.

### Variáveis para execução em Databricks

O deploy automatizado via GitHub Actions usa duas classes de configuração.

GitHub Secrets:

| Nome | Obrigatória | Uso |
| --- | --- | --- |
| `DATABRICKS_HOST` | Sim | Host HTTPS do workspace Databricks |
| `DATABRICKS_TOKEN` | Sim | Token usado pelo CLI/API do Databricks |
| `OPENAI_API_KEY` | Não | Credencial para habilitar enrichment com OpenAI |
| `LANGFUSE_PUBLIC_KEY` | Não | Credencial de observabilidade Langfuse |
| `LANGFUSE_SECRET_KEY` | Não | Credencial de observabilidade Langfuse |

GitHub Variables:

| Nome | Obrigatória | Uso | Default interno |
| --- | --- | --- | --- |
| `DATABRICKS_WORKSPACE_ROOT` | Não | Raiz de deploy em Workspace Files | `/Workspace/Shared/namastex-test` |
| `DATABRICKS_JOB_NAME` | Não | Nome do job gerenciado | `namastex-test-pipeline` |
| `DATABRICKS_CATALOG` | Não | Catalog Unity Catalog | `main` |
| `DATABRICKS_SCHEMA` | Não | Schema Unity Catalog | `ops` |
| `DATABRICKS_INPUT_VOLUME` | Não | Volume UC para input Bronze | `bronze_input` |
| `DATABRICKS_OUTPUT_VOLUME` | Não | Volume UC para outputs persistidos | `pipeline_output` |
| `DATABRICKS_INPUT_FILENAME` | Não | Nome do arquivo Bronze enviado ao volume | `conversations_bronze.parquet` |
| `DATABRICKS_SPARK_VERSION` | Não | Runtime Spark do job | `15.4.x-scala2.12` |
| `DATABRICKS_NODE_TYPE_ID` | Não | Tipo de nó do cluster do job | `Standard_DS3_v2` |
| `DATABRICKS_NUM_WORKERS` | Não | Quantidade de workers do job | `1` |
| `DATABRICKS_RUN_POLL_SECONDS` | Não | Intervalo de polling até o término da run | `10` |
| `PIPELINE_ENABLE_LLM_ENRICHMENT` | Não | Liga enrichment com provider externo | nenhum |
| `PIPELINE_LLM_OPENAI_MODEL` | Não | Modelo OpenAI usado no enrichment | nenhum |
| `PIPELINE_LLM_TIMEOUT_SECONDS` | Não | Timeout por chamada do runtime LLM | nenhum |
| `PIPELINE_LLM_MAX_RETRIES` | Não | Máximo de tentativas do runtime LLM | nenhum |
| `PIPELINE_ENABLE_LANGFUSE` | Não | Liga observabilidade Langfuse | nenhum |
| `PIPELINE_LANGFUSE_ALLOW_LOCAL_PROMPT_FALLBACK` | Não | Permite fallback local do prompt | nenhum |
| `PIPELINE_LANGFUSE_PROMPT_NAME` | Não | Nome do prompt no Langfuse | nenhum |
| `PIPELINE_LANGFUSE_PROMPT_LABEL` | Não | Label do prompt no Langfuse | nenhum |
| `PIPELINE_LANGFUSE_TRACE_NAME` | Não | Nome base dos traces | nenhum |
| `LANGFUSE_BASE_URL` | Não | Base URL da instância Langfuse | nenhum |

Defaults internos:

- workspace root: `/Workspace/Shared/namastex-test`
- catalog/schema: `main.ops`
- input volume: `bronze_input`
- output volume: `pipeline_output`
- job name: `namastex-test-pipeline`

## Deploy e execução no Databricks

O repositório agora inclui o workflow [databricks-deploy-run.yml](/home/lucas/projects/lucas54neves/namastex-test/.github/workflows/databricks-deploy-run.yml), acionado em `push` para `main` e por `workflow_dispatch`.

Fluxo do workflow:

- instala dependências Python e Databricks CLI
- valida os testes locais relevantes antes do deploy
- sincroniza o repositório para `Workspace Files`
- reconcilia catalog, schema, input volume, output volume e job
- envia `docs/conversations_bronze.parquet` diretamente para o volume de input já reconciliado
- propaga para o job Databricks apenas as variáveis explícitas de runtime de LLM/Langfuse quando estiverem definidas no workflow
- dispara o job Databricks e falha o workflow se a run falhar

Para um cenário com OpenAI habilitada no Databricks, configure no GitHub:

- Secret: `OPENAI_API_KEY`
- Variable: `PIPELINE_ENABLE_LLM_ENRICHMENT=1`
- Variable: `PIPELINE_LLM_OPENAI_MODEL=gpt-5-mini`

Se também quiser Langfuse:

- Secrets: `LANGFUSE_PUBLIC_KEY`, `LANGFUSE_SECRET_KEY`
- Variables: `PIPELINE_ENABLE_LANGFUSE=1`, `LANGFUSE_BASE_URL`, `PIPELINE_LANGFUSE_PROMPT_NAME`, `PIPELINE_LANGFUSE_PROMPT_LABEL`

O script operacional chamado pelo workflow é [scripts/databricks_deploy_run.py](/home/lucas/projects/lucas54neves/namastex-test/scripts/databricks_deploy_run.py), com a lógica de provisionamento e payload do job em [src/pipeline/infrastructure/databricks.py](/home/lucas/projects/lucas54neves/namastex-test/src/pipeline/infrastructure/databricks.py).

Mapeamento de runtime no Databricks:

- código: `/Workspace/Shared/namastex-test/...`
- input bruto: `/Volumes/<catalog>/<schema>/<input_volume>/conversations_bronze.parquet`
- outputs persistidos: `/Volumes/<catalog>/<schema>/<output_volume>/{data,reports,state,runtime}`

Pré-requisitos operacionais no workspace:

- Unity Catalog habilitado
- permissão do principal usado no GitHub para criar ou atualizar catalogs, schemas, volumes e jobs
- compute compatível com os parâmetros do job

### Configuração do Langfuse

O projeto continua com suporte a Langfuse self-hosted para prompt management e tracing do enrichment semântico, mas isso depende de uma instância realmente disponível e de credenciais válidas.

Para usar Langfuse de verdade, o ambiente precisa ter:

- `PIPELINE_ENABLE_LANGFUSE=1`
- `LANGFUSE_BASE_URL` apontando para a instância ativa
- `LANGFUSE_PUBLIC_KEY` válida
- `LANGFUSE_SECRET_KEY` válida
- `PIPELINE_LANGFUSE_PROMPT_NAME` e `PIPELINE_LANGFUSE_PROMPT_LABEL` compatíveis com o prompt bootstrapado

No `docker-compose.yml`, a stack local inclui:

- `langfuse-web`
- `langfuse-worker`
- `langfuse-bootstrap`

O bootstrap do prompt é feito por `scripts/bootstrap_langfuse_prompt.py`.

Importante:

- Se `PIPELINE_ENABLE_LANGFUSE=1` estiver ativo com credenciais placeholder como `lf_pk_change_me` e `lf_sk_change_me`, o runtime não usa Langfuse de forma real.
- Nesse caso, o pipeline faz fallback para prompt local e segue executando quando o modo fallback está permitido.
- Para validação local baseline, o caminho mais previsível continua sendo `PIPELINE_ENABLE_LLM_ENRICHMENT=0`.
- Para validar Langfuse end-to-end localmente, suba primeiro a stack com `docker compose up --build` antes de rodar o pipeline com enrichment habilitado.

## Testes

Conforme a regra do repositório, os testes devem ser executados com o ambiente virtual local:

```bash
venv/bin/python -m pytest -q
venv/bin/python -m pytest tests/test_jobs.py -q
venv/bin/python -m pytest tests/test_requirements_adherence.py -q
```

A suíte de aderência cobre explicitamente:

- Silver principal com granularidade por lead
- ausência de PII crua nos artefatos publicados
- persistência do enrichment por conversa
- atualização automática da Gold após crescimento da Bronze
- persistência de estado operacional
- geração de diagnóstico e alerta em falha simulada

## Agente operacional

O agente deste projeto continua auditável e restrito, mas agora executa um ciclo explícito de autonomia governada por impacto. O uso de LLM permanece opcional e nunca substitui os gates determinísticos. Suas responsabilidades são:

- compilar e aplicar a `pipeline_spec.json`
- detectar drift de schema e metadados
- gerar propostas estruturadas com `impact_class`, `candidate_actions`, evidência e política aplicada
- materializar candidatos isolados antes de qualquer promoção
- promover automaticamente apenas mudanças autorizadas por política e gates determinísticos
- reter mudanças `high impact` em aprovação explícita
- classificar falhas conhecidas em diagnósticos operacionais
- executar apenas playbooks seguros permitidos
- isolar registros inválidos em quarentena quando aplicável
- emitir relatórios e alertas
- restaurar o último estado íntegro em falhas inesperadas

Principais artefatos operacionais:

- `reports/monitoring/latest_run_report.json`
- `reports/monitoring/latest_agent_report.json`
- `reports/monitoring/latest_plan_report.json`
- `reports/monitoring/agent_autonomy_metrics.json`
- `reports/alerts/`
- `reports/agent_decisions/latest_agent_decision.json`
- `reports/agent_decisions/proposals/`
- `reports/agent_decisions/autonomy/latest_autonomy_decision.json`
- `runtime/candidates/`
- `state/pipeline_state.json`
- `state/approval_state.json`
- `state/pipeline_spec_history.json`

## Decisões técnicas do projeto

### 1. Agente governado por impacto em vez de autonomia irrestrita

O enunciado exige um agente que crie e mantenha o pipeline. A escolha foi evoluir para autonomia governada por impacto: mudanças aditivas e reversíveis podem ser promovidas automaticamente; mudanças estruturais e semânticas ficam bloqueadas por política e aprovação humana. Isso eleva autonomia sem perder auditabilidade.

### 2. Bronze como réplica fiel e não como camada de correção

A Bronze foi tratada como réplica controlada da fonte original. Isso preserva reprodutibilidade, permite auditoria da entrada e evita esconder problemas reais logo na ingestão.

### 3. Silver principal por lead e Silver auxiliar por mensagem

O enunciado pede Silver organizada por usuário/lead, mas a rastreabilidade por mensagem continua importante para auditoria e agregações. Por isso a solução separa:

- um artefato principal por `lead_key`
- um artefato auxiliar por mensagem
- um artefato intermediário por conversa para semântica

Essa escolha equilibra requisito de negócio, rastreabilidade e clareza do contrato.

### 4. Mascaramento com preservação de dimensão

Dados sensíveis são mascarados mantendo a forma geral do valor, atendendo ao enunciado e preservando utilidade operacional. Além do mascaramento, a publicação remove colunas proibidas e as validações procuram vazamento real em campos textuais.

### 5. Gold atualizada por fingerprint da fonte

Para manter o pipeline "vivo" sem reprocessar inutilmente, o runtime compara fingerprint da Bronze raw source usando caminho, tamanho e tempo de modificação. Essa estratégia é simples, reprodutível e suficiente para o escopo do teste.

### 6. Enrichment semântico opcional com fallback determinístico

O enrichment por conversa suporta providers externos e observabilidade com Langfuse, mas o baseline do projeto não depende disso. Quando o provider está desabilitado, indisponível ou retorna saída inválida, o sistema faz fallback determinístico e mantém o contrato publicado.

### 7. Contrato declarativo central em `config/pipeline_spec.json`

A spec centraliza colunas obrigatórias, buckets válidos, domínios semânticos e regras estruturais do pipeline. Isso facilita validação, planejamento de drift e evolução controlada.

### 8. Fallback para último estado íntegro

Em falhas inesperadas, o operador preserva o último conjunto válido de artefatos. Isso foi escolhido para privilegiar continuidade operacional e evitar publicar saídas parcialmente corrompidas.

### 9. Separação entre qualidade, transformação e orquestração

O código foi organizado para deixar explícita a diferença entre:

- regras de transformação
- política de publicação
- validação de contrato
- orquestração do ciclo
- comportamento do agente

Essa divisão melhora manutenção, testes e legibilidade da entrega.

## Limitações conhecidas

- O fingerprint observa mudança do arquivo de entrada, não CDC linha a linha.
- O modo com provider externo depende de credenciais, rede e disponibilidade do serviço.
- O planner autônomo ainda restringe a promoção ao conjunto inicial de famílias suportadas e validadas deterministicamente.
- O projeto foi otimizado para o dataset e o escopo do teste, não como plataforma multi-tenant completa.

## Referências

- [Enunciado técnico](/home/lucas/projects/lucas54neves/namastex-test/docs/technical-test-data-ai-engineering.md)
- [Data dictionary](/home/lucas/projects/lucas54neves/namastex-test/docs/data-dictionary-data-ai-engineering.md)
