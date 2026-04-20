# Pipeline Medalhão com Agente Operacional Determinístico

Este repositório implementa uma entrega do teste técnico de Data & AI Engineering a partir da base transacional de conversas WhatsApp. A solução foi estruturada como um pipeline medalhão em Python com atualização incremental, validações de qualidade, relatórios operacionais e uma camada agêntica determinística para diagnóstico, remediação segura e fallback.

O projeto não usa LLM. O termo `agente` aqui significa um supervisor operacional baseado em regras explícitas, responsável por classificar falhas, sugerir ação corretiva, tentar reconstruções seguras de Silver e Gold e preservar o último estado íntegro quando ocorre erro inesperado.

Em termos de aderência ao teste, o projeto implementa um agente operacional determinístico que gerencia a execução do pipeline, diagnostica falhas, aplica auto-remediações seguras e preserva o último estado íntegro, mas não cria ou reconfigura o pipeline autonomamente.

## Fontes

- Enunciado: `docs/Teste Técnico de Data & AI Engineering.docx`
- Dicionário de dados: `docs/Dicionário de Dados - Teste Técnico de Data & AI Engineering.docx`
- Bronze de entrada: `docs/conversations_bronze.parquet`

## Arquitetura

```text
src/pipeline/
  agent.py
  alerts.py
  config.py
  io.py
  jobs.py
  quality.py
  state.py
  transforms.py

scripts/
  monitor_pipeline.py
  profile_bronze.py
  run_pipeline.py
  run_pipeline_daemon.py

data/
  bronze/
  silver/
  gold/

reports/
  alerts/
  bronze_profile.json
  monitoring/

state/
  pipeline_state.json
```

## Aderência ao teste

| Requisito | Como a solução atende |
|---|---|
| Python puro | Pipeline, runner, validações e monitoramento implementados em Python |
| Bronze, Silver e Gold | Materialização em parquet com transformações distintas por camada |
| Pipeline vivo | Runner incremental por fingerprint e modo contínuo com polling |
| Agente que gerencia o pipeline | Camada operacional determinística para diagnóstico, remediação e fallback |
| Mascaramento de dados sensíveis | Mascaramento de nome, telefone, CPF, CEP, e-mail e placa preservando formato |
| Tabela analítica com classificações | Gold agora inclui segmentações, audiências e perfis de lead baseados em regras |

## O que foi implementado

- cópia controlada da fonte na Bronze
- parsing do JSON de `metadata`
- conversão defensiva de `timestamp`
- normalização de `sender_name`
- mascaramento de PII em `sender_name`, `sender_phone` e `message_body`
- deduplicação semântica de eventos com retenção do `status` mais informativo
- extração de sinais e entidades de domínio:
  - e-mail, telefone, CPF, CEP e placa
  - marca, modelo e ano do veículo
  - concorrente citado
  - valor cotado
  - tipo de sinistro
- Gold agregada por `conversation_id`
- segmentação analítica da Gold por persona, audiência, temperatura do lead, estágio de intenção, prontidão de contato, sensibilidade a preço e sinal de risco
- validações automatizadas para Bronze, Silver e Gold
- atualização incremental por fingerprint da fonte
- modo contínuo com polling para manter a Gold atualizada
- relatórios de monitoramento, alertas e estado persistido
- fallback operacional para o último estado bem-sucedido
- testes automatizados cobrindo transforms, qualidade, runner, alertas e daemon

## Camadas do pipeline

### Bronze

Replica o arquivo de origem em parquet para a área controlada do pipeline, preservando o schema bruto para reprocessamento.

### Silver

Camada de limpeza e enriquecimento. Contém:

- `metadata_*` expandido da coluna JSON
- flags `is_inbound` e `is_outbound`
- colunas mascaradas:
  - `sender_name_masked`
  - `sender_phone_masked`
  - `message_body_masked`
- colunas de deduplicação:
  - `duplicate_event_group_size`
  - `had_status_duplication`
  - `dropped_duplicate_events`
- colunas de extração:
  - `vehicle_make`
  - `vehicle_model`
  - `vehicle_year`
  - `competitor_mentioned`
  - `quoted_price`
  - `sinistro_type`

### Gold

Camada analítica agregada por conversa. Mantém métricas de engajamento e sinais transacionais, além de classificações derivadas por regra.

Principais métricas:

- duração da conversa
- total de mensagens inbound e outbound
- score de compartilhamento de dados
- total de duplicidades removidas
- presença de veículo, concorrente e sinistro
- cidade, estado, campanha, agente e origem do lead

Novas colunas de segmentação:

- `persona_profile`
- `audience_segment`
- `lead_temperature`
- `price_sensitivity`
- `intent_stage`
- `contact_readiness`
- `risk_signal`

Perfis e audiências atuais:

- `cotador_comparador` para leads que comparam oferta e já citaram preço
- `cliente_pos_sinistro` para leads com contexto de sinistro
- `lead_engajado_com_dados` para conversas mais quentes com maior compartilhamento de dados
- `lead_frio` para conversas pouco engajadas
- `oferta_competitiva`, `retencao_pos_sinistro`, `close_comercial` e `nutricao_basica` como audiências operacionais

## Política de mascaramento

O masking preserva a forma visível do dado para manter utilidade analítica sem expor o valor original.

Exemplos:

- `Ana Paula` -> `XXX XXXXX`
- `123.456.789-00` -> `XXX.XXX.XXX-XX`
- `04567-123` -> `XXXXX-XXX`
- `ana.paula@gmail.com` -> `xxx.xxxxx@xxxxx.xxx`
- `ABC1D23` -> `XXX9X99`

## Qualidade e agente operacional

As validações atuais cobrem:

- Bronze:
  - colunas obrigatórias
  - unicidade de `message_id`
  - canal restrito a `whatsapp`
- Silver:
  - colunas críticas presentes
  - `timestamp` não nulo
  - ausência de duplicidade pós-deduplicação
  - checagem de vazamento de CPF mascarado
  - consistência de `mentions_vehicle`
- Gold:
  - colunas analíticas obrigatórias
  - unicidade de `conversation_id`
  - métricas não negativas
  - buckets válidos
  - validade de `persona_profile`, `audience_segment` e `lead_temperature`

A camada agêntica:

- classifica falhas por tipo
- sugere ação corretiva
- tenta auto-remediação segura quando a falha é reconstruível
- registra relatório operacional separado do relatório de validação
- aplica fallback para o último estado íntegro em falhas inesperadas

Status operacionais possíveis:

- `healthy`
- `auto_remediated`
- `degraded_validation_failed`
- `fallback_applied`
- `manual_intervention_required`
- `idle_no_source_change`

## Execução

Com o ambiente virtual criado e as dependências instaladas:

```bash
venv/bin/python scripts/profile_bronze.py
venv/bin/python scripts/run_pipeline.py
venv/bin/python scripts/run_pipeline.py --force
venv/bin/python scripts/monitor_pipeline.py
venv/bin/python scripts/run_pipeline_daemon.py --force-first-run --poll-interval-seconds 60
venv/bin/pytest -q
```

Variáveis de ambiente operacionais:

```bash
export PIPELINE_POLL_INTERVAL_SECONDS="60"
export PIPELINE_ALERT_WEBHOOK_URL="https://seu-endpoint-de-alerta"
export PIPELINE_ALERT_SUPPRESSION_MINUTES="30"
```

O modo contínuo:

- executa o pipeline em loop
- respeita o fingerprint incremental da fonte
- evita reprocessamento quando a Bronze não mudou
- pode rodar indefinidamente ou com número fixo de ciclos via `--max-cycles`

Exemplo de execução controlada:

```bash
venv/bin/python scripts/run_pipeline_daemon.py \
  --force-first-run \
  --poll-interval-seconds 30 \
  --max-cycles 5
```

## Artefatos gerados

- Bronze: `data/bronze/conversations.parquet`
- Silver: `data/silver/conversations_silver.parquet`
- Gold: `data/gold/conversations_gold.parquet`
- Profiling: `reports/bronze_profile.json`
- Monitoramento: `reports/monitoring/latest_run_report.json`
- Relatório agêntico: `reports/monitoring/latest_agent_report.json`
- Relatório de alerta: `reports/monitoring/latest_alert_report.json`
- Incidentes persistidos: `reports/alerts/*.json`
- Histórico de alertas: `reports/alerts/alert_history.json`
- Estado: `state/pipeline_state.json`

## Limitações atuais

- o agente atual é determinístico e baseado em regras, não em LLM
- a operação contínua é por polling local, não por orquestrador externo como Databricks Jobs, Airflow ou systemd
- a segmentação da Gold é explicável e reproduzível, mas ainda pode evoluir com mais sinais do domínio

## Testes automatizados

A suíte atual cobre:

- mascaramento de PII preservando formato
- deduplicação com retenção do `status` mais informativo
- contexto de conversa
- segmentação da Gold
- validações de qualidade
- execução incremental
- fallback em erro inesperado
- runner contínuo com polling e ciclos controlados
