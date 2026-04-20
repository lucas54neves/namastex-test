# Pipeline de Transformação Agêntica de Dados

Este repositório contém a base inicial do teste técnico de Data & AI Engineering. O foco atual é construir um pipeline medalhão em Python a partir da base transacional de conversas WhatsApp, preparando o terreno para uma operação contínua e agêntica.

## Fontes

- Enunciado: `docs/Teste Técnico de Data & AI Engineering.docx`
- Dicionário de dados: `docs/Dicionário de Dados - Teste Técnico de Data & AI Engineering.docx`
- Bronze de entrada: `docs/conversations_bronze.parquet`

## O que já foi implementado

- Estrutura inicial do projeto em `src/pipeline` e `scripts/`
- Configuração central de caminhos e criação automática de diretórios
- Script de profiling da Bronze
- Materialização inicial das camadas Bronze, Silver e Gold em parquet
- Parsing do JSON de `metadata`
- Conversão de `timestamp` para datetime
- Normalização de `sender_name`
- Sinais básicos de mensagem:
  - comprimento da mensagem
  - contagem de palavras
  - presença de e-mail, telefone, CPF, CEP e placa
- Endurecimento inicial da Silver:
  - mascaramento de PII preservando formato em `sender_name`, `sender_phone` e `message_body`
  - deduplicação semântica de eventos por conversa, timestamp, direção, remetente, tipo e corpo da mensagem
  - priorização de `status` mais informativo na deduplicação
  - extração de entidades de domínio: veículo, concorrente citado, valor cotado e tipo de sinistro
- Gold agregada por `conversation_id`, com features de engajamento, compartilhamento de dados, veículo, concorrência e sinistro
- Silver deduplicada de `153228` para `145928` mensagens após remoção de `7300` eventos semânticos duplicados
- Validações automatizadas de qualidade para Bronze, Silver e Gold
- Execução incremental por fingerprint da fonte
- Estado persistido do pipeline com histórico de execuções
- Snapshot de monitoramento do último run
- Camada agêntica para classificar falhas, sugerir correção e aplicar remediação segura quando possível
- Fallback operacional para o último estado bem-sucedido em caso de erro inesperado
- Alertas ativos com geração de incidente local e entrega opcional por webhook
- Testes automatizados para mascaramento, deduplicação, qualidade e runner incremental

## Estrutura atual

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

tests/
  test_agent.py
  test_alerts.py
  test_jobs.py
  test_quality.py
  test_transforms.py
```

## Como executar

Com o ambiente virtual já criado e as dependências instaladas:

```bash
venv/bin/python scripts/profile_bronze.py
venv/bin/python scripts/run_pipeline.py
venv/bin/python scripts/run_pipeline.py --force
venv/bin/python scripts/monitor_pipeline.py
venv/bin/pytest -q
```

Para habilitar entrega externa por webhook:

```bash
export PIPELINE_ALERT_WEBHOOK_URL="https://seu-endpoint-de-alerta"
```

Para instalar os hooks locais:

```bash
venv/bin/pre-commit install
venv/bin/pre-commit install --hook-type pre-push
```

Os hooks de `mypy` e `pytest` usam ambientes Python isolados gerenciados pelo próprio `pre-commit`, reduzindo dependência do `PATH` local e evitando falhas quando o `venv` do projeto não estiver ativado no shell.

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

## Estado atual das camadas

### Bronze

Cópia da fonte original em parquet para a área controlada do pipeline.

### Silver

Camada de limpeza e enriquecimento. Hoje contém:

- `metadata_*` expandido da coluna JSON
- flags de inbound e outbound
- campos mascarados:
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

Resultado atual:

- `145928` mensagens após deduplicação
- `7300` eventos duplicados removidos
- cerca de `13k` mensagens com sinal de veículo
- `7721` mensagens com concorrente citado
- `1891` mensagens com menção classificada como sinistro

### Gold

Camada analítica por conversa. Hoje contém agregações como:

- duração da conversa
- bucket de engajamento
- score de compartilhamento de dados
- total de duplicidades removidas
- presença de veículo, concorrente e sinistro
- cidade, estado, campanha, agente e origem do lead

## Qualidade e monitoramento

O runner principal agora funciona de forma incremental.

- Cada execução calcula um fingerprint da fonte com caminho, tamanho e `mtime`
- Se a Bronze não mudou, o pipeline pula o reprocessamento e registra o evento como `skipped_no_source_change`
- Se a Bronze mudou, o pipeline reprocessa Bronze, Silver e Gold e escreve um relatório de validação
- Em paralelo, a camada agêntica registra um relatório operacional separado com diagnóstico, remediação e fallback

As validações atuais cobrem:

- Bronze:
  - presença das colunas obrigatórias
  - unicidade de `message_id`
  - canal restrito a `whatsapp`
- Silver:
  - colunas críticas presentes
  - `timestamp` não nulo
  - ausência de duplicidade nas chaves semânticas pós-deduplicação
  - verificação básica de vazamento de CPF mascarado
  - consistência da flag `mentions_vehicle`
- Gold:
  - colunas analíticas obrigatórias
  - unicidade de `conversation_id`
  - métricas não negativas
  - buckets de engajamento válidos

Estado validado no último run completo:

- status do relatório: `passed`
- checks com falha: `0`
- contagens: Bronze `153228`, Silver `145928`, Gold `15000`
- segundo run sem mudança na fonte: `skipped_no_source_change`

## Camada agêntica

O pipeline agora possui uma camada operacional em cima do monitoramento:

- classifica falhas por tipo, como:
  - `source_schema_drift`
  - `silver_deduplication_failure`
  - `pii_masking_leak`
  - `gold_bucket_invalid`
  - `unexpected_runtime_error`
- sugere correção objetiva para cada classe de falha
- tenta auto-remediação apenas em casos conservadores, como:
  - reconstrução da Silver a partir da Bronze
  - reconstrução da Gold a partir da Silver
  - reaplicação de transformações quando uma validação estrutural ou derivada falha
- aplica fallback para o último estado bem-sucedido quando ocorre erro inesperado e não há reparo seguro no mesmo ciclo

Status operacionais possíveis no relatório agêntico:

- `healthy`
- `auto_remediated`
- `degraded_validation_failed`
- `fallback_applied`
- `manual_intervention_required`
- `idle_no_source_change`

## Alertas ativos

O pipeline agora transforma o resultado agêntico em um evento de alerta a cada execução.

- Sempre persiste um incidente local em `reports/alerts/`
- Mantém histórico append-only em `reports/alerts/alert_history.json`
- Calcula severidade por status agêntico:
  - `info` para `healthy` e `idle_no_source_change`
  - `warning` para `auto_remediated`
  - `high` para `degraded_validation_failed` e `fallback_applied`
  - `critical` para `manual_intervention_required`
- Só tenta entrega externa quando o status agêntico exige atenção operacional
- A entrega externa é opcional e usa `PIPELINE_ALERT_WEBHOOK_URL`

Estado validado no último run real:

- severidade do alerta: `info`
- `should_alert`: `false`
- entrega externa: não tentada, porque o status agêntico foi `healthy`
- incidentes persistidos até agora: `1`

## Testes automatizados

A suíte atual cobre:

- mascaramento de PII preservando formato
- deduplicação com retenção do `status` mais informativo
- extração de contexto da conversa
- detecção de falha em validação da Silver
- validação básica da Gold
- execução incremental do runner
- geração do relatório de validação
- diagnóstico agêntico de falhas conhecidas
- auto-remediação da Gold após validação inválida
- fallback para último estado bem-sucedido após erro em runtime
- geração de alertas com severidade correta
- persistência de incidente e histórico de alertas

## Observações técnicas

- A Bronze real possui `153228` mensagens e `15000` conversas.
- O dataset contém tipos de mensagem além dos citados no dicionário, como `contact`, `video` e `location`.
- Há duplicidades semânticas relevantes causadas por múltiplos `status` para o mesmo evento.
- O mascaramento atual preserva formato para os principais identificadores estruturados, mas ainda pode evoluir para cobrir mais casos de nomes citados livremente.
- A extração de veículo é heurística e ainda pode crescer para cobrir mais marcas e modelos.
- A classificação de sinistro está intencionalmente conservadora para evitar confundir histórico do lead com texto de cobertura comercial.
- O fingerprint incremental usa metadados do arquivo-fonte. Para produção, o ideal é evoluir para controle por partição, watermark ou checksum por lote.
- As validações atuais priorizam confiabilidade estrutural e riscos óbvios; ainda há espaço para checks de distribuição, drift e anomalias de negócio.
- A camada agêntica ainda usa um conjunto fechado de diagnósticos e remediações seguras. Ela não reescreve código nem altera regras do pipeline de forma autônoma.
- A entrega externa por webhook é best-effort. Em caso de falha de rede ou endpoint, o incidente continua preservado localmente.

## Próximos passos

- enriquecer a extração de veículo com maior cobertura de marcas e modelos
- melhorar a identificação de histórico de sinistro versus menção a cobertura
- adicionar alertas ativos a partir do resultado de monitoramento
- evoluir de incremental por arquivo para incremental por lote ou watermark
- ampliar a taxonomia de falhas e as remediações seguras por camada
- adicionar deduplicação/supressão para evitar alert storm em falhas repetidas
- integrar com canal externo real, como Slack webhook ou sistema de incidentes
- registrar métricas históricas de saúde para detectar regressão e drift
- ampliar a suíte de testes com cenários de regressão e dados sintéticos mais variados
