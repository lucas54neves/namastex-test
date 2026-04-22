# Pipeline Medalhao com Agente Operacional Deterministico

Este repositorio implementa a entrega do teste tecnico de Data & AI Engineering a partir de uma base transacional de conversas WhatsApp. A solucao foi estruturada como um pipeline medalhao em Python com execucao incremental, contratos de qualidade, artefatos operacionais persistidos, uma camada agentica deterministica para planejamento estrutural, diagnostico operacional, remediacao segura, alerta e fallback, e uma etapa opcional de enriquecimento semantico por conversa.

O projeto nao usa LLM para governar o pipeline. O termo `agente` aqui significa um conjunto de modulos auditaveis que:

- mantem uma `pipeline_spec.json` versionada
- compila essa spec para o runtime
- executa validacoes por camada
- classifica falhas conhecidas
- tenta auto-remediacao segura apenas para playbooks operacionais aprovados
- isola registros invalidos em quarentena
- registra relatorios operacionais e de validacao
- preserva o ultimo estado integro quando ocorre erro inesperado

## Fontes

- Enunciado: `docs/technical-test-data-ai-engineering.md`
- Dicionario de dados: `docs/data-dictionary-data-ai-engineering.md`
- Bronze de entrada: `docs/conversations_bronze.parquet`

## Visao geral

O pipeline responde a quatro perguntas centrais do teste:

1. O que o sistema faz?
   Copia a Bronze para uma area controlada, normaliza e enriquece os eventos, publica uma `Silver` organizada por lead, um artefato intermediario de enriquecimento semantico por `conversation_id` e uma `Gold` analitica por lead, e mantem relatorios separados para execucao operacional, planejamento estrutural, diagnostico e alertas.
2. O que e persistido em cada camada?
   `Bronze` replica a fonte bruta controlada, `Silver` publica um artefato principal por `lead_key`, um artefato auxiliar por mensagem e um artefato de enrichment por conversa, e `Gold` publica uma visao analitica tambem por `lead_key`.
3. Como os dados sensiveis sao protegidos?
   O runtime pode usar colunas cruas apenas em memoria quando necessario para derivacao. Os artefatos publicados em `Silver` e `Gold` removem colunas proibidas e as validacoes varrem vazamento em campos textuais.
4. Como isso atende ao teste?
   Ha um pipeline medalhao em Python, atualizacao automatica da `Gold` por fingerprint e polling, `Silver` principal organizada por lead, enriquecimento semantico por conversa com fallback deterministico, `Gold` util com segmentacoes reproduziveis e um agente operacional limitado, auditavel e reproduzivel.

## Arquitetura

```text
src/pipeline/
  approval.py
  agent.py
  alerts.py
  compiler.py
  conversation_enrichment.py
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
| Bronze, Silver e Gold | artefatos parquet distintos por camada | `data/bronze/conversations.parquet`, `data/silver/silver_leads.parquet`, `data/silver/silver_messages.parquet`, `data/silver/silver_conversations_llm.parquet`, `data/gold/conversations_gold.parquet` |
| Atualizacao automatica da Gold | reexecucao por fingerprint e modo continuo com polling | `src/pipeline/state.py`, `src/pipeline/operator.py`, `scripts/run_pipeline_daemon.py` |
| Silver organizada por lead | tabela principal com uma linha por `lead_key` e tabela auxiliar de rastreabilidade | `src/pipeline/transforms.py`, `src/pipeline/publication.py`, `tests/test_jobs.py` |
| Gold analitica util | agregacao por lead com segmentacoes reproduziveis | `src/pipeline/transforms.py`, `src/pipeline/quality.py` |
| Protecao de dados sensiveis | policy de publicacao sem PII e checks anti-vazamento | `src/pipeline/publication.py`, `src/pipeline/quality.py`, `tests/test_quality.py` |
| Agente operacional | planejamento, diagnostico, playbooks seguros, fallback e aprovacao humana para mudancas estruturais | `src/pipeline/planner.py`, `src/pipeline/agent.py`, `src/pipeline/approval.py`, `src/pipeline/operator.py` |

## Suite de aderencia

Existe uma suite dedicada e avaliavel para os requisitos centrais do enunciado em `tests/test_requirements_adherence.py`.

Ela funciona como a entrada principal para revisar, em poucos testes, as evidencias automatizadas de que o repositorio atende aos pontos mais cobrados do teste:

- `Silver` principal com granularidade por lead e unicidade por `lead_key`
- ausencia de campos crus de PII publicados em `Silver` e `Gold`
- persistencia auditavel do enrichment semantico por `conversation_id`
- atualizacao automatica de `Gold` quando a `Bronze` cresce
- persistencia de estado operacional em artefato sob `state/`
- geracao de diagnostico e alerta em falha simulada

Execucao recomendada:

```bash
PIPELINE_ENABLE_LLM_ENRICHMENT=0 venv/bin/python scripts/run_pipeline.py --force
venv/bin/python -m pytest -q
venv/bin/python -m pytest tests/test_requirements_adherence.py -q
```

Baseline validado localmente:

- o caminho minimo suportado para validacao do repositorio desabilita o enrichment LLM com `PIPELINE_ENABLE_LLM_ENRICHMENT=0`
- esse modo nao depende de rede nem de credenciais externas e e o mesmo caminho coberto pelos testes de paridade de runtime
- a execucao com providers habilitados continua opcional e deve ser tratada como modo de integracao, nao como requisito do baseline local

## Camadas do pipeline

### Bronze

`Bronze` replica `docs/conversations_bronze.parquet` para `data/bronze/conversations.parquet`, preservando o schema bruto em uma area controlada para reproducao e reprocessamento.

### Silver

`Silver` e a camada de limpeza e enriquecimento. O contrato publicado tem tres artefatos:

- `data/silver/silver_leads.parquet`
  Artefato principal da `Silver`, com uma linha por `lead_key`.
- `data/silver/silver_messages.parquet`
  Artefato auxiliar por mensagem deduplicada para preservar a rastreabilidade `lead_key -> conversation_id -> message_id`.
- `data/silver/silver_conversations_llm.parquet`
  Artefato intermediario por `conversation_id`, com classificacoes semanticas estruturadas, metadata de inferencia, cache por hash de entrada, normalizacao canonica de drift lexical do provider antes da validacao estrita e fallback deterministico quando o enrichment LLM esta desabilitado, indisponivel ou invalido.

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

O contrato de `silver_conversations_llm.parquet` inclui, entre outras:

- `conversation_id`
- `lead_key`
- `conversation_started_at`
- `conversation_last_message_at`
- `llm_input_hash`
- `prompt_version`
- `llm_model`
- `provider_name`
- `provider_attempt_count`
- `provider_error_summary`
- `inference_status`
- `processed_at_utc`
- `sentiment_label`
- `sentiment_confidence_band`
- `intent_stage`
- `persona_profile`
- `audience_segment`
- `price_objection_intensity`
- `competitor_pressure_level`
- `commercial_urgency_signal`
- `recommended_next_action`
- `explanation_short`
- `fallback_reason`
- `validation_error`

No caminho opcional de provider, o runtime aplica uma etapa deterministica de normalizacao antes da validacao estrita do vocabulario controlado. Isso permite aceitar drift lexical seguro, como `negative -> negativo`, `high -> forte` e `strong -> forte`, sem relaxar o contrato publicado nem aceitar termos ambiguos como `skeptical`.

Essa escolha nao foi feita apenas no prompt do provider. O prompt agora tambem envia os vocabularios aceitos e instrui a LLM a nao inventar labels, mas isso sozinho nao e suficiente como contrato de runtime. Providers ainda podem retornar sinonimos, labels em ingles ou estagios intermediarios mesmo quando a instrucao e clara. Por isso a arquitetura usa tres camadas complementares: o prompt reduz drift na origem, a normalizacao corrige apenas drift lexical seguro de forma deterministica e testavel, e a validacao estrita continua rejeitando qualquer valor ambiguo ou fora do contrato final.

### Gold

`Gold` e a camada analitica principal publicada em `data/gold/conversations_gold.parquet`. Cada linha representa um unico `lead_key` consolidando todas as conversas conhecidas do lead.

O build da `Gold` consome explicitamente:

- `silver_leads.parquet` como resumo primario por lead
- `silver_messages.parquet` como historico auxiliar para metricas dependentes do nivel de mensagem
- `silver_conversations_llm.parquet` como fonte principal dos campos semanticos por conversa, consolidada deterministicamente em nivel de lead

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
- `dominant_email_provider`
- `engagement_bucket`
- `data_shared_score`
- `persona_profile`
- `audience_segment`
- `lead_temperature`
- `price_sensitivity`
- `intent_stage`
- `contact_readiness`
- `risk_signal`
- `response_latency_band`
- `closure_outcome_group`
- `has_closed_outcome`
- `price_objection_intensity`
- `commercial_urgency_signal`
- `competitor_pressure_level`
- `conversation_sentiment_label`
- `conversation_sentiment_support`
- `positive_tone_hits`
- `negative_tone_hits`

Segmentacoes atuais:

- `persona_profile`: `cotador_comparador`, `cliente_pos_sinistro`, `lead_engajado_com_dados`, `lead_frio`
- `audience_segment`: `oferta_competitiva`, `retencao_pos_sinistro`, `close_comercial`, `nutricao_basica`
- `lead_temperature`, `price_sensitivity`, `intent_stage`, `contact_readiness` e `risk_signal` com vocabularios controlados validados em runtime

Enriquecimentos analiticos adicionais:

- `dominant_email_provider`: familia normalizada inferida deterministicamente de e-mails observados no historico, com vocabulario controlado e `null` quando nao ha evidencia
- `response_latency_band`: `sem_evidencia`, `rapida`, `moderada` ou `lenta`, derivada de `avg_response_time_sec`
- `closure_outcome_group` e `has_closed_outcome`: normalizacao de desfechos observados para suportar analise de taxa de fechamento por perfil
- `price_objection_intensity`: `nenhuma`, `leve` ou `forte`, derivada de sinais de cotacao, pressao de preco e contexto competitivo
- `commercial_urgency_signal`: `nenhuma`, `moderada` ou `alta`, derivada de sinais linguísticos de urgencia e do ciclo observado do lead
- `competitor_pressure_level`: `nenhuma`, `leve` ou `alta`, derivada de mencoes a concorrentes e sinais de comparacao comercial
- `conversation_sentiment_label`: `positivo`, `neutro`, `negativo` ou `sem_evidencia`, consolidado da classificacao por conversa
- `conversation_sentiment_support`: `fraco`, `moderado`, `forte` ou `sem_evidencia`, consolidado do suporte por conversa
- `positive_tone_hits` e `negative_tone_hits`: contagens agregadas de familias de pistas lexicais positivas e negativas encontradas no historico inbound

Exemplos de grouped analysis habilitados diretamente pela `Gold`:

- fechamento por `persona_profile` usando `has_closed_outcome` ou `closure_outcome_group`
- latencia media e distribuicao de `response_latency_band` por `audience_segment`, `city` ou `state`
- intensidade de objecao de preco por canal de origem usando `price_objection_intensity`
- pressao competitiva por perfil usando `competitor_pressure_level` e `primary_competitor`
- leads com maior friccao comercial usando `conversation_sentiment_label = negativo` e `conversation_sentiment_support`
- interacoes favoraveis para priorizacao comercial usando `conversation_sentiment_label = positivo` combinado com `contact_readiness`

Observacoes sobre enrichment semantico:

- o enrichment por conversa e opcional e fica fora do caminho de controle operacional do agente
- quando `PIPELINE_ENABLE_LLM_ENRICHMENT=1`, o pipeline monta um runtime dedicado em `src/pipeline/llm_runtime.py` para executar OpenAI como provider primario e Anthropic como fallback por conversa
- o runtime registra `provider_name`, `provider_attempt_count` e `provider_error_summary` para auditoria sem expor payload cru nem credenciais
- sem credenciais, com erro de provider ou com output invalido fora do vocabulario controlado, o pipeline persiste fallback deterministico e continua a publicacao
- o cache evita recomputar conversas quando `llm_input_hash`, `prompt_version` e a identidade efetiva de modelos do runtime (`llm_model`) nao mudam
- sinais de preco, urgencia e concorrencia continuam auditaveis e o `Gold` sempre permanece publicavel mesmo sem LLM

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
- `src/pipeline/planner.py` inspeciona a Bronze e propoe evolucoes estruturadas da spec em modo recomendacao por padrao
- `src/pipeline/approval.py` controla aprovacao humana explicita para qualquer aplicacao estrutural na spec
- `src/pipeline/operator.py` executa o ciclo operacional completo
- `src/pipeline/agent.py` diagnostica falhas de validacao e escolhe playbooks seguros
- `src/pipeline/llm_advisor.py` existe apenas como interface opcional do agente operacional; ele nao participa do caminho principal e pode estar desabilitado sem afetar a execucao
- `src/pipeline/conversation_enrichment.py` monta payloads sanitizados por conversa, controla cache por hash, integra o runtime de providers, valida outputs estruturados e persiste fallback seguro
- `src/pipeline/llm_runtime.py` concentra resolucao de configuracao, roteamento OpenAI -> Anthropic, metadata de tentativas e integracao opcional com LangChain/LangGraph

Checks atuais de validacao:

- Bronze:
  - colunas obrigatorias
  - unicidade de `message_id`
  - canal restrito a `whatsapp`
  - regra semantica de que a primeira mensagem por `conversation_id` deve ser `outbound`, com diagnostico por conversa violadora
- Silver principal:
  - ausencia de colunas cruas proibidas
  - presenca de `canonical_lead_name_masked` e `lead_contact_ref`
  - unicidade de `lead_key`
  - `first_seen_at` e `last_seen_at` nao nulos
  - contagens agregadas nao negativas
  - varredura anti-vazamento em campos publicados
  - consistencia semantica com `silver_messages` para `first_seen_at`, `last_seen_at`, `conversation_count`, `message_count` e propagacao de sinais booleanos
- Silver auxiliar:
  - ausencia de colunas cruas proibidas
  - presenca de `sender_name_masked`, `sender_phone_masked` e `message_body_masked`
  - `timestamp` nao nulo
  - unicidade pelas chaves de deduplicacao publicadas
  - checks anti-vazamento por classe sensivel: e-mail, telefone, CPF, CEP e placa
  - consistencia de `mentions_vehicle`
- Silver enrichment por conversa:
  - contrato obrigatorio por `conversation_id`
  - status de inferencia validos e schema controlado
  - vocabularios validos para classificacoes semanticas e `recommended_next_action`
  - varredura anti-vazamento em `explanation_short`
- Gold:
  - ausencia de colunas cruas proibidas
  - varredura anti-vazamento em qualquer coluna textual publicada nao excluida explicitamente
  - colunas obrigatorias
  - unicidade de `lead_key`
  - `conversation_count`, `total_messages` e `duplicate_events_removed` nao negativos
  - vocabularios validos para `engagement_bucket`, `persona_profile`, `audience_segment`, `lead_temperature`, `price_sensitivity`, `intent_stage`, `contact_readiness`, `risk_signal`, `dominant_email_provider`, `response_latency_band`, `closure_outcome_group`, `price_objection_intensity`, `commercial_urgency_signal` e `competitor_pressure_level`
  - consistencia semantica com `silver` e `silver_messages` para chaves, intervalos de tempo, totais agregados e sinais observados
  - coerencia deterministica de `engagement_bucket`, `lead_temperature`, `contact_readiness`, `risk_signal`, `dominant_email_provider`, `response_latency_band`, `closure_outcome_group`, `has_closed_outcome`, `price_objection_intensity`, `commercial_urgency_signal` e `competitor_pressure_level` com os fatos publicados na propria linha

As validacoes semanticas usam apenas identificadores tecnicos e amostras limitadas de `lead_key` ou `conversation_id` nos diagnosticos. O runtime nao publica corpo de mensagem, nome cru, telefone cru ou outros valores sensiveis nos payloads de falha.

Status operacionais observaveis nos relatorios:

- `success`
- `success_after_auto_remediation`
- `fallback_to_last_successful`
- `skipped_no_source_change`
- `idle_no_source_change` no relatorio agencial quando nao ha mudanca na fonte
- `auto_remediated`, `not_auto_remediable`, `manual_intervention_required` e `fallback_applied` no `latest_agent_report.json` para classificar o tratamento operacional

Importante: o agente e deterministico e limitado ao escopo implementado no repositorio. Ele nao reescreve o codigo Python livremente, nao trata toda falha como auto-remediavel e nao executa evolucao estrutural autonoma sem aprovacao.

## Execucao

Com o ambiente virtual criado e as dependencias instaladas:

```bash
venv/bin/python scripts/profile_bronze.py
venv/bin/python scripts/plan_pipeline.py
PIPELINE_ENABLE_LLM_ENRICHMENT=0 venv/bin/python scripts/run_pipeline.py --force
venv/bin/python scripts/run_pipeline.py
venv/bin/python scripts/run_pipeline.py --force
venv/bin/python scripts/monitor_pipeline.py
venv/bin/python scripts/run_pipeline_daemon.py --force-first-run --poll-interval-seconds 60
venv/bin/python -m pytest -q
venv/bin/python -m pytest tests/test_jobs.py -q
```

Configuracao por `.env`:

```bash
cp .env.example .env
```

O runtime carrega automaticamente o arquivo `.env` na raiz do repositorio. Variaveis ja exportadas no shell continuam tendo precedencia.

Para validar o repositorio de forma deterministica, prefira sobrescrever `PIPELINE_ENABLE_LLM_ENRICHMENT=0` no comando, mesmo se o `.env` local habilitar providers opcionais por padrao.

Variaveis de ambiente operacionais e de LLM:

```bash
PIPELINE_POLL_INTERVAL_SECONDS="60"
PIPELINE_ALERT_WEBHOOK_URL="https://seu-endpoint-de-alerta"
PIPELINE_ALERT_SUPPRESSION_MINUTES="30"
PIPELINE_ENABLE_LLM_ENRICHMENT="0"
PIPELINE_ENABLE_LLM_ADVISOR="0"
OPENAI_API_KEY="sua-chave-openai"
ANTHROPIC_API_KEY="sua-chave-anthropic"
PIPELINE_LLM_OPENAI_MODEL="gpt-5-mini"
PIPELINE_LLM_ANTHROPIC_MODEL="claude-sonnet"
PIPELINE_LLM_TIMEOUT_SECONDS="20"
PIPELINE_LLM_MAX_RETRIES="1"
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
| `data/silver/silver_conversations_llm.parquet` | enrichment semantico por `conversation_id`, com cache, status e fallback auditavel |
| `data/gold/conversations_gold.parquet` | tabela analitica principal por `lead_key` |
| `data/quarantine/` | isolamento de registros invalidos ou inseguros quando necessario |

Observacao: arquivos legados, como `data/silver/conversations_silver.parquet`, podem existir de execucoes anteriores, mas nao representam o contrato publicado atual do runtime.

### Relatorios

| Caminho | Papel operacional |
|---|---|
| `reports/bronze_profile.json` | perfil exploratorio da Bronze |
| `reports/monitoring/latest_run_report.json` | resumo da ultima execucao e das validacoes |
| `reports/monitoring/latest_plan_report.json` | contexto detectado, propostas estruturais, estados de aprovacao e aplicacoes aprovadas |
| `reports/monitoring/latest_agent_report.json` | diagnostico operacional, classificacao de auto-remediacao, decisoes, fallback e referencia ao planner |
| `reports/monitoring/latest_alert_report.json` | consolidado do ultimo evento de alerta |
| `reports/agent_decisions/latest_agent_decision.json` | resumo auditavel das decisoes do agente |
| `reports/alerts/alert_history.json` | historico consolidado de eventos de alerta |
| `reports/alerts/*.json` | incidentes persistidos por evento |

### Estado

| Caminho | Papel operacional |
|---|---|
| `state/pipeline_state.json` | estado de execucao, fingerprint e historico de runs |
| `state/approval_state.json` | decisoes humanas persistidas para propostas estruturais identificadas por `proposal_id` |
| `state/pipeline_spec_history.json` | historico de propostas/aplicacoes de mudanca da spec |

## Decisoes e trade-offs

- `Silver` principal por lead
  Esta modelagem atende melhor ao enunciado de dados organizados por usuario/lead. O trade-off e manter um segundo artefato de mensagens para nao perder rastreabilidade e suporte a agregacoes.
- `Gold` por lead, nao por conversa
  A camada analitica fica mais defensavel para segmentacao comercial e consolidacao de sinais. O trade-off e que metricas por conversa deixam de ser a entidade principal e passam a ser insumo auxiliar, inclusive quando o enrichment semantico e produzido por `conversation_id`.
- Persistencia sem PII crua
  Reduz o risco de exposicao e alinha documentacao e artefatos. O trade-off e que investigacao detalhada depende das versoes mascaradas e dos sinais derivados, nao de texto livre cru.
- Agente deterministico e limitado
  O comportamento e mais auditavel e seguro para o teste tecnico. O trade-off e menor flexibilidade do que um sistema autonomo irrestrito.

## Limitacoes atuais

- o agente atual e deterministico e baseado em regras; o `llm_advisor` e opcional
- o enrichment LLM por conversa depende de habilitacao explicita e de um provedor conectado; sem isso, o pipeline permanece funcional com fallback deterministico
- a operacao continua e por polling local, nao por orquestrador externo como Airflow ou systemd
- o planner agora emite propostas estruturadas e auditaveis para schema, validacoes, derivacoes, segmentacoes e regras de transformacao; ele nao altera o codigo Python por conta propria
- a `Gold` e explicavel e reproduzivel, mas ainda pode evoluir com taxonomias semanticas de dominio e conectores reais de inferencia

## Testes automatizados

A suite atual cobre:

- mascaramento de PII preservando formato
- deduplicacao com retencao do `status` mais informativo
- contratos de publicacao sem PII
- enrichment semantico por conversa com cache, validacao e fallback
- checks anti-vazamento em `Silver` e `Gold`
- modelagem `Silver` principal por lead e `Gold` por lead
- planner, spec e aprovacao
- quarentena de registros invalidos
- execucao incremental por fingerprint
- fallback em erro inesperado
- modo continuo com polling e ciclos controlados
