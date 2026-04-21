# Pipeline Medalhao com Agente Operacional Deterministico

Este repositorio implementa a entrega do teste tecnico de Data & AI Engineering a partir de uma base transacional de conversas WhatsApp. A solucao foi estruturada como um pipeline medalhao em Python com execucao incremental, contratos de qualidade, artefatos operacionais persistidos e uma camada agentica deterministica para planejamento, diagnostico, remediacao segura, alerta e fallback.

O projeto nao usa LLM no caminho principal de execucao. O termo `agente` aqui significa um conjunto de modulos auditaveis que:

- mantem uma `pipeline_spec.json` versionada
- compila essa spec para o runtime
- executa validacoes por camada
- classifica falhas conhecidas
- tenta auto-remediacao segura via playbooks permitidos
- isola registros invalidos em quarentena
- registra relatorios operacionais e de validacao
- preserva o ultimo estado integro quando ocorre erro inesperado

## Fontes

- Enunciado: `docs/Teste Tecnico de Data & AI Engineering.docx`
- Dicionario de dados: `docs/Dicionario de Dados - Teste Tecnico de Data & AI Engineering.docx`
- Bronze de entrada: `docs/conversations_bronze.parquet`

## Visao geral

O pipeline responde a quatro perguntas centrais do teste:

1. O que o sistema faz?
   Copia a Bronze para uma area controlada, normaliza e enriquece os eventos, publica uma `Silver` organizada por lead e uma `Gold` analitica por lead, e mantem relatorios operacionais para execucao, planejamento, diagnostico e alertas.
2. O que e persistido em cada camada?
   `Bronze` replica a fonte bruta controlada, `Silver` publica um artefato principal por `lead_key` e um artefato auxiliar por mensagem, e `Gold` publica uma visao analitica tambem por `lead_key`.
3. Como os dados sensiveis sao protegidos?
   O runtime pode usar colunas cruas apenas em memoria quando necessario para derivacao. Os artefatos publicados em `Silver` e `Gold` removem colunas proibidas e as validacoes varrem vazamento em campos textuais.
4. Como isso atende ao teste?
   Ha um pipeline medalhao em Python, atualizacao automatica da `Gold` por fingerprint e polling, `Silver` principal organizada por lead, `Gold` util com segmentacoes deterministicas e um agente operacional limitado, auditavel e reproduzivel.

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
  publication.py
  quality.py
  quarantine.py
  spec.py
  state.py
  transforms.py

scripts/
  profile_bronze.py
  plan_pipeline.py
  monitor_pipeline.py
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

## Aderencia ao teste

| Requisito do teste | Evidencia principal | Implementacao |
|---|---|---|
| Pipeline em Python | scripts de execucao e modulos em `src/pipeline/` | `scripts/run_pipeline.py`, `scripts/run_pipeline_daemon.py`, `src/pipeline/jobs.py` |
| Bronze, Silver e Gold | artefatos parquet distintos por camada | `data/bronze/conversations.parquet`, `data/silver/silver_leads.parquet`, `data/silver/silver_messages.parquet`, `data/gold/conversations_gold.parquet` |
| Atualizacao automatica da Gold | reexecucao por fingerprint e modo continuo com polling | `src/pipeline/state.py`, `src/pipeline/operator.py`, `scripts/run_pipeline_daemon.py` |
| Silver organizada por lead | tabela principal com uma linha por `lead_key` e tabela auxiliar de rastreabilidade | `src/pipeline/transforms.py`, `src/pipeline/publication.py`, `tests/test_jobs.py` |
| Gold analitica util | agregacao por lead com segmentacoes reproduziveis | `src/pipeline/transforms.py`, `src/pipeline/quality.py` |
| Protecao de dados sensiveis | policy de publicacao sem PII e checks anti-vazamento | `src/pipeline/publication.py`, `src/pipeline/quality.py`, `tests/test_quality.py` |
| Agente operacional | planejamento, diagnostico, playbooks seguros, fallback e aprovacao humana para mudancas estruturais | `src/pipeline/planner.py`, `src/pipeline/agent.py`, `src/pipeline/approval.py`, `src/pipeline/operator.py` |

## Camadas do pipeline

### Bronze

`Bronze` replica `docs/conversations_bronze.parquet` para `data/bronze/conversations.parquet`, preservando o schema bruto em uma area controlada para reproducao e reprocessamento.

### Silver

`Silver` e a camada de limpeza e enriquecimento. O contrato publicado tem dois artefatos:

- `data/silver/silver_leads.parquet`
  Artefato principal da `Silver`, com uma linha por `lead_key`.
- `data/silver/silver_messages.parquet`
  Artefato auxiliar por mensagem deduplicada para preservar a rastreabilidade `lead_key -> conversation_id -> message_id`.

O runtime pode usar colunas cruas em memoria para deduplicacao, extracao e agregacao, mas os artefatos publicados removem colunas proibidas como:

- `sender_name`
- `sender_phone`
- `message_body`
- `conversation_lead_name`
- `conversation_lead_phone`
- `conversation_agent_name`
- `sender_name_normalized`
- `lead_name_raw`
- `lead_phone_raw`

O contrato principal de `silver_leads.parquet` inclui, entre outras:

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
- `has_vehicle_signal`
- `has_competitor_signal`
- `has_sinistro_signal`
- `has_email_signal`
- `has_phone_signal`
- `has_cpf_signal`
- `has_cep_signal`
- `has_plate_signal`

Colecoes observadas sao persistidas em formato deterministico para revisao e reprocessamento consistente.

O contrato auxiliar de `silver_messages.parquet` inclui, entre outras:

- `lead_key`
- `conversation_id`
- `message_id`
- `timestamp`
- `direction`
- `message_type`
- `sender_name_masked`
- `sender_phone_masked`
- `message_body_masked`
- `duplicate_event_group_size`
- `had_status_duplication`
- `dropped_duplicate_events`
- `mentions_vehicle`
- `mentions_competitor`
- `mentions_sinistro`
- `contains_email`
- `contains_phone`
- `contains_cpf`
- `contains_cep`
- `contains_plate`
- `vehicle_make`
- `vehicle_model`
- `vehicle_year`
- `quoted_price`
- `sinistro_type`
- `metadata_*` expandido a partir do JSON de origem

### Gold

`Gold` e a camada analitica principal publicada em `data/gold/conversations_gold.parquet`. Cada linha representa um unico `lead_key` consolidando todas as conversas conhecidas do lead.

O build da `Gold` consome explicitamente:

- `silver_leads.parquet` como resumo primario por lead
- `silver_messages.parquet` como historico auxiliar para metricas dependentes do nivel de mensagem

Regras centrais de agregacao:

- `first_seen_at` e `last_seen_at` por minimo e maximo do historico do lead
- `conversation_count` por contagem distinta de `conversation_id`
- `total_messages`, `inbound_messages`, `outbound_messages` e `duplicate_events_removed` por soma no historico deduplicado
- sinais booleanos como `contains_email`, `contains_phone`, `mentioned_vehicle`, `mentioned_competitor` e `mentioned_sinistro` por OR logico
- contexto de preco, concorrente, veiculo e sinistro por selecao deterministica do ultimo valor nao nulo observado

Principais colunas analiticas:

- `lead_key`
- `conversation_count`
- `total_messages`
- `inbound_messages`
- `outbound_messages`
- `duplicate_events_removed`
- `contains_email`
- `contains_phone`
- `contains_cpf`
- `contains_cep`
- `contains_plate`
- `mentioned_vehicle`
- `mentioned_competitor`
- `mentioned_sinistro`
- `avg_response_time_sec`
- `city`
- `state`
- `observed_lead_sources`
- `observed_campaign_ids`
- `observed_outcomes`
- `engagement_bucket`
- `data_shared_score`
- `persona_profile`
- `audience_segment`
- `lead_temperature`
- `price_sensitivity`
- `intent_stage`
- `contact_readiness`
- `risk_signal`

Segmentacoes atuais:

- `persona_profile`: `cotador_comparador`, `cliente_pos_sinistro`, `lead_engajado_com_dados`, `lead_frio`
- `audience_segment`: `oferta_competitiva`, `retencao_pos_sinistro`, `close_comercial`, `nutricao_basica`
- `lead_temperature`, `price_sensitivity`, `intent_stage`, `contact_readiness` e `risk_signal` com vocabularios controlados validados em runtime

## Politica de protecao de dados

O projeto protege dados sensiveis em duas etapas complementares:

1. Mascaramento
   O pipeline mascara nome, telefone e texto livre, preservando parte do formato visivel para manter utilidade analitica sem expor o valor original.
2. Publicacao segura
   Antes de escrever `Silver` e `Gold`, a camada de publicacao remove colunas cruas proibidas e as validacoes verificam o frame final publicado.

Exemplos de mascaramento:

- `Ana Paula` -> `XXX XXXXX`
- `123.456.789-00` -> `XXX.XXX.XXX-XX`
- `04567-123` -> `XXXXX-XXX`
- `ana.paula@gmail.com` -> `xxx.xxxxx@xxxxx.xxx`
- `ABC1D23` -> `XXX9X99`

Dados que nao sao publicados em artefatos `Silver` e `Gold`:

- nome cru
- telefone cru
- texto livre cru
- nomes crus normalizados ou auxiliares usados para derivacao

## Qualidade e agente operacional

O pipeline e dirigido por spec:

- `config/pipeline_spec.json` descreve colunas obrigatorias, deduplicacao, validacoes, segmentacoes e playbooks seguros
- `src/pipeline/compiler.py` compila a spec para um plano executavel
- `src/pipeline/planner.py` inspeciona a Bronze e propoe evolucoes estruturadas da spec
- `src/pipeline/approval.py` controla aprovacao humana para mudancas que nao podem ser autoaplicadas com seguranca
- `src/pipeline/operator.py` executa o ciclo operacional completo
- `src/pipeline/agent.py` diagnostica falhas de validacao e escolhe playbooks seguros
- `src/pipeline/llm_advisor.py` existe apenas como interface opcional; ele nao participa do caminho principal e pode estar desabilitado sem afetar a execucao

Checks atuais de validacao:

- Bronze:
  - colunas obrigatorias
  - unicidade de `message_id`
  - canal restrito a `whatsapp`
- Silver principal:
  - ausencia de colunas cruas proibidas
  - presenca de `canonical_lead_name_masked` e `lead_contact_ref`
  - unicidade de `lead_key`
  - `first_seen_at` e `last_seen_at` nao nulos
  - contagens agregadas nao negativas
  - varredura anti-vazamento em campos publicados
- Silver auxiliar:
  - ausencia de colunas cruas proibidas
  - presenca de `sender_name_masked`, `sender_phone_masked` e `message_body_masked`
  - `timestamp` nao nulo
  - unicidade pelas chaves de deduplicacao publicadas
  - checks anti-vazamento por classe sensivel: e-mail, telefone, CPF, CEP e placa
  - consistencia de `mentions_vehicle`
- Gold:
  - ausencia de colunas cruas proibidas
  - varredura anti-vazamento em qualquer coluna textual publicada nao excluida explicitamente
  - colunas obrigatorias
  - unicidade de `lead_key`
  - `conversation_count`, `total_messages` e `duplicate_events_removed` nao negativos
  - vocabularios validos para `engagement_bucket`, `persona_profile`, `audience_segment`, `lead_temperature`, `price_sensitivity`, `intent_stage`, `contact_readiness` e `risk_signal`

Status operacionais observaveis nos relatorios:

- `success`
- `success_after_auto_remediation`
- `fallback_to_last_successful`
- `skipped_no_source_change`
- `idle_no_source_change` no relatorio agencial quando nao ha mudanca na fonte

Importante: o agente e deterministico e limitado ao escopo implementado no repositorio. Ele nao reescreve o codigo Python livremente nem executa evolucao autonoma irrestrita do sistema.

## Execucao

Com o ambiente virtual criado e as dependencias instaladas:

```bash
venv/bin/python scripts/profile_bronze.py
venv/bin/python scripts/plan_pipeline.py
venv/bin/python scripts/run_pipeline.py
venv/bin/python scripts/run_pipeline.py --force
venv/bin/python scripts/monitor_pipeline.py
venv/bin/python scripts/run_pipeline_daemon.py --force-first-run --poll-interval-seconds 60
venv/bin/python -m pytest -q
venv/bin/python -m pytest tests/test_jobs.py -q
```

Variaveis de ambiente operacionais:

```bash
export PIPELINE_POLL_INTERVAL_SECONDS="60"
export PIPELINE_ALERT_WEBHOOK_URL="https://seu-endpoint-de-alerta"
export PIPELINE_ALERT_SUPPRESSION_MINUTES="30"
```

O modo continuo:

- executa o pipeline em loop
- recalcula a execucao quando o fingerprint da fonte muda
- evita reprocessamento quando a Bronze nao mudou
- pode rodar indefinidamente ou com numero fixo de ciclos via `--max-cycles`

Exemplo de execucao controlada:

```bash
venv/bin/python scripts/run_pipeline_daemon.py \
  --force-first-run \
  --poll-interval-seconds 30 \
  --max-cycles 5
```

## Artefatos gerados

### Dados

| Caminho | Papel operacional |
|---|---|
| `data/bronze/conversations.parquet` | copia controlada da Bronze para reproducao e reprocessamento |
| `data/silver/silver_leads.parquet` | contrato principal da `Silver`, uma linha por `lead_key` |
| `data/silver/silver_messages.parquet` | artefato auxiliar de rastreabilidade e suporte a agregacoes |
| `data/gold/conversations_gold.parquet` | tabela analitica principal por `lead_key` |
| `data/quarantine/` | isolamento de registros invalidos ou inseguros quando necessario |

Observacao: arquivos legados, como `data/silver/conversations_silver.parquet`, podem existir de execucoes anteriores, mas nao representam o contrato publicado atual do runtime.

### Relatorios

| Caminho | Papel operacional |
|---|---|
| `reports/bronze_profile.json` | perfil exploratorio da Bronze |
| `reports/monitoring/latest_run_report.json` | resumo da ultima execucao e das validacoes |
| `reports/monitoring/latest_plan_report.json` | contexto detectado e propostas estruturadas do planner para evolucao da spec |
| `reports/monitoring/latest_agent_report.json` | diagnostico, decisoes e fallback da camada agentica |
| `reports/monitoring/latest_alert_report.json` | consolidado do ultimo evento de alerta |
| `reports/agent_decisions/latest_agent_decision.json` | resumo auditavel das decisoes do agente |
| `reports/alerts/alert_history.json` | historico consolidado de eventos de alerta |
| `reports/alerts/*.json` | incidentes persistidos por evento |

### Estado

| Caminho | Papel operacional |
|---|---|
| `state/pipeline_state.json` | estado de execucao, fingerprint e historico de runs |
| `state/approval_state.json` | aprovacoes humanas persistidas para mudancas estruturais |
| `state/pipeline_spec_history.json` | historico de propostas/aplicacoes de mudanca da spec |

## Decisoes e trade-offs

- `Silver` principal por lead
  Esta modelagem atende melhor ao enunciado de dados organizados por usuario/lead. O trade-off e manter um segundo artefato de mensagens para nao perder rastreabilidade e suporte a agregacoes.
- `Gold` por lead, nao por conversa
  A camada analitica fica mais defensavel para segmentacao comercial e consolidacao de sinais. O trade-off e que metricas por conversa deixam de ser a entidade principal e passam a ser insumo auxiliar.
- Persistencia sem PII crua
  Reduz o risco de exposicao e alinha documentacao e artefatos. O trade-off e que investigacao detalhada depende das versoes mascaradas e dos sinais derivados, nao de texto livre cru.
- Agente deterministico e limitado
  O comportamento e mais auditavel e seguro para o teste tecnico. O trade-off e menor flexibilidade do que um sistema autonomo irrestrito.

## Limitacoes atuais

- o agente atual e deterministico e baseado em regras; o `llm_advisor` e opcional
- a operacao continua e por polling local, nao por orquestrador externo como Airflow ou systemd
- o planner agora emite propostas estruturadas e auditaveis para schema, validacoes, derivacoes, segmentacoes e regras de transformacao; ele nao altera o codigo Python por conta propria
- a `Gold` e explicavel e reproduzivel, mas ainda pode evoluir com enriquecimentos adicionais de dominio

## Testes automatizados

A suite atual cobre:

- mascaramento de PII preservando formato
- deduplicacao com retencao do `status` mais informativo
- contratos de publicacao sem PII
- checks anti-vazamento em `Silver` e `Gold`
- modelagem `Silver` principal por lead e `Gold` por lead
- planner, spec e aprovacao
- quarentena de registros invalidos
- execucao incremental por fingerprint
- fallback em erro inesperado
- modo continuo com polling e ciclos controlados
