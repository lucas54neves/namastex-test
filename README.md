# Pipeline Medalhão com Agente Operacional Determinístico

Este repositório implementa uma entrega do teste técnico de Data & AI Engineering a partir da base transacional de conversas WhatsApp. A solução foi estruturada como um pipeline medalhão em Python com atualização incremental, validações de qualidade, relatórios operacionais e uma camada agêntica determinística para planejamento, diagnóstico, remediação segura e fallback.

O projeto não usa LLM na execução principal. O termo `agente` aqui significa um conjunto de módulos determinísticos e auditáveis que cria e mantém uma `pipeline_spec.json`, compila essa spec para o runtime, monitora a execução, aplica remediações seguras, isola registros inválidos em quarentena e preserva o último estado íntegro quando ocorre erro inesperado.

Em termos de aderência ao teste, o projeto agora trata a criação do pipeline como criação e evolução de especificação versionada, enquanto a execução continua determinística e controlada.

## Fontes

- Enunciado: `docs/Teste Técnico de Data & AI Engineering.docx`
- Dicionário de dados: `docs/Dicionário de Dados - Teste Técnico de Data & AI Engineering.docx`
- Bronze de entrada: `docs/conversations_bronze.parquet`

## Arquitetura

```text
src/pipeline/
  approval.py
  agent.py
  alerts.py
  compiler.py
  config.py
  io.py
  jobs.py
  llm_advisor.py
  operator.py
  planner.py
  playbooks.py
  quality.py
  quarantine.py
  spec.py
  state.py
  transforms.py

scripts/
  plan_pipeline.py
  monitor_pipeline.py
  profile_bronze.py
  run_pipeline.py
  run_pipeline_daemon.py

config/
  pipeline_spec.json

data/
  bronze/
  silver/
  gold/
  quarantine/

reports/
  alerts/
  agent_decisions/
  bronze_profile.json
  monitoring/

state/
  approval_state.json
  pipeline_state.json
  pipeline_spec_history.json
```

## Aderência ao teste

| Requisito | Como a solução atende |
|---|---|
| Python puro | Pipeline, runner, validações e monitoramento implementados em Python |
| Bronze, Silver e Gold | Materialização em parquet com transformações distintas por camada |
| Pipeline vivo | Runner incremental por fingerprint e modo contínuo com polling |
| Agente que cria e gerencia o pipeline | Planner gera e evolui `pipeline_spec.json`; operator executa, valida, remedia e faz fallback |
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
- Gold agregada por `lead_key`
- segmentação analítica da Gold por persona, audiência, temperatura do lead, estágio de intenção, prontidão de contato, sensibilidade a preço e sinal de risco
- `pipeline_spec.json` como contrato versionado do pipeline
- compiler para traduzir spec em plano executável
- operator explícito para gerenciar o ciclo de execução
- playbooks auditáveis para remediação
- quarentena de registros inválidos
- planner para detectar drift e propor evolução da spec
- aprovação humana para mudanças estruturais
- interface opcional de LLM advisor, desligada por padrão
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

Camada de limpeza e enriquecimento remodelada para ter granularidade principal por lead. O runtime continua usando colunas cruas apenas em memória para derivação, deduplicação e agregação da Gold, mas a publicação agora materializa dois artefatos:

- `data/silver/silver_leads.parquet`
  contrato principal da Silver, com uma linha por `lead_key`
- `data/silver/silver_messages.parquet`
  tabela auxiliar por mensagem deduplicada para rastreabilidade `lead_key -> conversation_id -> message_id`

Ambos os parquets aplicam uma política explícita de persistência sem PII e não gravam:

- `sender_name`
- `sender_phone`
- `message_body`
- `conversation_lead_name`
- `conversation_lead_phone`
- `conversation_agent_name`
- `sender_name_normalized`
- `lead_name_raw`
- `lead_phone_raw`

O artefato principal `silver_leads.parquet` contém, entre outras, as colunas:

- `lead_key`
- `canonical_lead_name_masked`
- `lead_contact_ref`
- `city`
- `state`
- `first_seen_at`
- `last_seen_at`
- `conversation_count`
- `message_count`
- `observed_campaign_ids`
- `observed_lead_sources`
- `observed_outcomes`
- flags agregadas de sinal:
  - `has_vehicle_signal`
  - `has_competitor_signal`
  - `has_sinistro_signal`
  - `has_email_signal`
  - `has_phone_signal`
  - `has_cpf_signal`
  - `has_cep_signal`
  - `has_plate_signal`

As coleções observadas são persistidas como listas ordenadas serializadas em JSON para manter determinismo e facilidade de revisão.

O artefato auxiliar `silver_messages.parquet` contém:

- `metadata_*` expandido da coluna JSON
- `lead_key`
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

Camada analítica agregada por lead. A Gold passa a consumir explicitamente o contrato remodelado da Silver:

- `silver_leads.parquet` como fonte primária com uma linha por `lead_key`
- `silver_messages.parquet` como artefato auxiliar para recompor métricas dependentes do histórico de mensagens

Cada linha publicada em `data/gold/conversations_gold.parquet` representa um único `lead_key` consolidando todas as conversas conhecidas do lead. A agregação usa regras determinísticas:

- `first_seen_at` e `last_seen_at` por mínimo e máximo do histórico do lead
- `conversation_count` por contagem distinta de `conversation_id`
- `total_messages`, `inbound_messages`, `outbound_messages` e `duplicate_events_removed` por soma no histórico deduplicado
- sinais booleanos como `contains_email`, `contains_phone`, `mentioned_vehicle`, `mentioned_competitor` e `mentioned_sinistro` por OR lógico
- coleções observadas como campanhas, origens e outcomes preservadas a partir da Silver principal em JSON ordenado e determinístico
- contexto de preço, concorrente, veículo e sinistro escolhido de forma determinística a partir do último valor não nulo observado no histórico do lead

Principais métricas:

- primeira e última atividade do lead
- total de conversas distintas por lead
- total de mensagens inbound e outbound
- score de compartilhamento de dados
- total de duplicidades removidas
- presença de veículo, concorrente e sinistro
- cidade, estado, campanhas, outcomes e origens observadas do lead

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

Validações da Gold agora verificam explicitamente:

- unicidade de `lead_key`
- `conversation_count` e `total_messages` não negativos
- vocabulários válidos para persona, audiência, temperatura, sensibilidade a preço, estágio de intenção, prontidão de contato e sinal de risco

## Política de mascaramento

O masking preserva a forma visível do dado para manter utilidade analítica sem expor o valor original.

Além do masking, a persistência de `Silver` e `Gold` remove colunas cruas de identidade e texto livre antes da escrita em parquet. A validação do pipeline passa a checar o contrato do frame final publicado, não apenas o frame intermediário em memória.

Exemplos:

- `Ana Paula` -> `XXX XXXXX`
- `123.456.789-00` -> `XXX.XXX.XXX-XX`
- `04567-123` -> `XXXXX-XXX`
- `ana.paula@gmail.com` -> `xxx.xxxxx@xxxxx.xxx`
- `ABC1D23` -> `XXX9X99`

## Qualidade e agente operacional

O pipeline passou a ser dirigido por spec:

- `config/pipeline_spec.json` descreve colunas obrigatórias, deduplicação, validações, segmentações e playbooks seguros
- a spec agora diferencia o contrato da Silver principal por lead e o contrato auxiliar de `silver_messages`
- `compiler.py` compila a spec para um plano executável
- `planner.py` inspeciona a Bronze, detecta drift e propõe mudanças na spec
- `approval.py` controla aprovação humana para mudanças estruturais
- `operator.py` executa o ciclo operacional
- `llm_advisor.py` existe apenas como interface opcional e desabilitada por padrão

As validações atuais cobrem:

- Bronze:
  - colunas obrigatórias
  - unicidade de `message_id`
  - canal restrito a `whatsapp`
- Silver:
  - `silver_leads`:
    - ausência de colunas cruas proibidas no artefato publicado
    - presença de `canonical_lead_name_masked` e `lead_contact_ref`
    - unicidade de `lead_key`
    - `first_seen_at` e `last_seen_at` não nulos
    - contagens agregadas não negativas
    - varredura anti-vazamento em campos textuais publicados
  - `silver_messages`:
    - ausência de colunas cruas proibidas no artefato publicado
    - presença das colunas mascaradas obrigatórias
    - `timestamp` não nulo
    - ausência de duplicidade pós-deduplicação
    - contratos anti-vazamento por classe sensível em `message_body_masked`:
    - e-mail
    - telefone
    - CPF
    - CEP
    - placa
    - consistência de `mentions_vehicle`
- Gold:
  - ausência de colunas cruas proibidas no artefato publicado
  - varredura anti-vazamento em qualquer coluna textual publicada não excluída explicitamente
  - colunas analíticas obrigatórias
  - unicidade de `conversation_id`
  - métricas não negativas
  - buckets válidos
  - validade de `persona_profile`, `audience_segment` e `lead_temperature`

A camada agêntica:

- cria e mantém uma spec versionada do pipeline
- compila a spec para o runtime
- classifica falhas por tipo
- sugere ação corretiva
- seleciona playbooks auditáveis
- tenta auto-remediação segura quando a falha é reconstruível
- isola registros inválidos em quarentena
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
venv/bin/python scripts/plan_pipeline.py
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
- Silver principal: `data/silver/silver_leads.parquet`
- Silver auxiliar: `data/silver/silver_messages.parquet`
- Gold: `data/gold/conversations_gold.parquet`
- Quarentena: `data/quarantine/quarantined_bronze_rows.parquet`
- Spec do pipeline: `config/pipeline_spec.json`
- Profiling: `reports/bronze_profile.json`
- Monitoramento: `reports/monitoring/latest_run_report.json`
- Relatório de planejamento: `reports/monitoring/latest_plan_report.json`
- Relatório agêntico: `reports/monitoring/latest_agent_report.json`
- Relatório de alerta: `reports/monitoring/latest_alert_report.json`
- Decisão agêntica: `reports/agent_decisions/latest_agent_decision.json`
- Incidentes persistidos: `reports/alerts/*.json`
- Histórico de alertas: `reports/alerts/alert_history.json`
- Estado: `state/pipeline_state.json`
- Aprovações: `state/approval_state.json`
- Histórico de mudanças da spec: `state/pipeline_spec_history.json`

## Limitações atuais

- o agente atual é determinístico e baseado em regras; o LLM advisor é apenas opcional e está desligado
- a operação contínua é por polling local, não por orquestrador externo como Databricks Jobs, Airflow ou systemd
- a segmentação da Gold é explicável e reproduzível, mas ainda pode evoluir com mais sinais do domínio
- o planner atual evolui a spec com foco em drift estrutural e metadata, não reescreve código Python livremente

## Testes automatizados

A suíte atual cobre:

- mascaramento de PII preservando formato
- deduplicação com retenção do `status` mais informativo
- contexto de conversa
- segmentação da Gold
- validações de qualidade
- spec, planner e aprovação
- quarentena de registros inválidos
- execução incremental
- fallback em erro inesperado
- runner contínuo com polling e ciclos controlados
