# Pipeline Medalhão com Agente Operacional Determinístico

Entrega final do teste técnico de Data & AI Engineering descrito em [docs/technical-test-data-ai-engineering.md](/home/lucas/projects/lucas54neves/namastex-test/docs/technical-test-data-ai-engineering.md). O projeto implementa um pipeline em Python para transformar conversas transacionais de WhatsApp em uma arquitetura Bronze -> Silver -> Gold, com atualização automática da camada analítica quando a fonte cresce e um agente operacional determinístico para diagnóstico, alerta, fallback e remediação segura.

## Objetivo da entrega

O enunciado pede mais do que uma análise pontual: pede uma infraestrutura persistente que permaneça "viva", gerencie o pipeline, monitore falhas e mantenha a camada Gold atualizada. A solução entregue atende esse objetivo com os seguintes blocos:

- Pipeline em Python puro, organizado em camadas claras.
- Bronze como réplica controlada da fonte original.
- Silver como camada de limpeza, normalização, deduplicação, mascaramento e organização por lead.
- Gold como camada analítica para segmentação, personas, audiência, sinais comerciais e sentimento.
- Agente operacional determinístico para planejar ajustes de contrato, classificar falhas, acionar playbooks seguros, registrar decisões e preservar o último estado íntegro.
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
- O agente operacional observa o ciclo, valida contratos, registra relatórios, aciona alertas e executa fallback quando necessário.

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
- `reports/monitoring/latest_agent_report.json`
- `reports/monitoring/latest_alert_report.json`
- `state/pipeline_state.json`

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
- `plan_pipeline.py` roda o planner estrutural que detecta drift de schema e propostas de atualização de contrato.

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

O agente deste projeto não governa o pipeline por LLM. Ele é determinístico, auditável e restrito. Suas responsabilidades são:

- compilar e aplicar a `pipeline_spec.json`
- detectar drift de schema e metadados
- gerar propostas estruturais sujeitas a aprovação
- classificar falhas conhecidas em diagnósticos operacionais
- executar apenas playbooks seguros permitidos
- isolar registros inválidos em quarentena quando aplicável
- emitir relatórios e alertas
- restaurar o último estado íntegro em falhas inesperadas

Principais artefatos operacionais:

- `reports/monitoring/latest_run_report.json`
- `reports/monitoring/latest_agent_report.json`
- `reports/monitoring/latest_plan_report.json`
- `reports/alerts/`
- `reports/agent_decisions/latest_agent_decision.json`
- `state/pipeline_state.json`
- `state/approval_state.json`
- `state/pipeline_spec_history.json`

## Decisões técnicas do projeto

### 1. Agente determinístico em vez de agente autônomo generativo

O enunciado exige um agente que crie e mantenha o pipeline, mas não exige que ele tome decisões estruturais livres. A escolha foi usar um agente determinístico com regras explícitas, diagnósticos mapeados e playbooks seguros. Isso reduz risco operacional, melhora auditabilidade e facilita defesa técnica da solução.

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
- O planner estrutural propõe mudanças e exige aprovação para alterações de contrato, em vez de modificar tudo automaticamente.
- O projeto foi otimizado para o dataset e o escopo do teste, não como plataforma multi-tenant completa.

## Referências

- [Enunciado técnico](/home/lucas/projects/lucas54neves/namastex-test/docs/technical-test-data-ai-engineering.md)
- [Data dictionary](/home/lucas/projects/lucas54neves/namastex-test/docs/data-dictionary-data-ai-engineering.md)
