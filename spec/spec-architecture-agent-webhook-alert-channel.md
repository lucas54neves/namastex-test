---
title: Agent Webhook Alert Channel — Canal Real de Notificação Externa para Alertas do Agente
version: 1.0
date_created: 2026-04-28
owner: Data Engineering
tags: [architecture, agent, alerts, webhook, observability]
---

# Introduction

O sistema de alertas atual do agente produz relatórios JSON locais (`reports/alerts/`) e histórico de incidentes, mas não notifica nenhum canal externo. Quando o pipeline falha em produção, o único mecanismo de descoberta é a leitura manual dos relatórios ou a observação de logs. Esta spec define um canal de notificação via HTTP webhook — configurável, opcional e não-bloqueante — que entrega alertas do agente para Slack, Discord, Microsoft Teams, PagerDuty ou qualquer endpoint que aceite POST JSON.

## 1. Purpose & Scope

**Propósito:** Definir o mecanismo de entrega de alertas do agente para canais externos via HTTP POST, tornando o sistema de monitoramento do pipeline observável além do filesystem local.

**Escopo:**

- Novo módulo `src/pipeline/agent/alert_channels.py` com a lógica de entrega HTTP
- Atualização de `src/pipeline/agent/alerts.py` para chamar o canal após persistência local
- Novas variáveis de ambiente para configuração do webhook
- Atualização de `config/agent_autonomy_policy.json` para incluir configuração de canal
- Atualização do `.env.example` com as novas variáveis
- Atualização do `README.md` com instruções de configuração do webhook
- Testes unitários cobrindo o módulo `alert_channels.py`

**Fora de escopo:**

- Integração com email (SMTP)
- Integração com SMS
- Deduplicação de alertas idênticos em janela de tempo
- Retry com backoff exponencial além de N tentativas fixas
- Autenticação OAuth — apenas Bearer token via header

**Público-alvo:** Engenheiros implementando a feature e operadores configurando o pipeline em produção.

## 2. Definitions

| Termo | Definição |
|---|---|
| Webhook | Endpoint HTTP que recebe dados via POST quando um evento ocorre; responsabilidade de processamento é do receptor |
| Alert Event | Estrutura de dados produzida por `handle_alerting()` quando `should_alert = True` |
| Delivery Attempt | Uma chamada HTTP POST ao endpoint configurado com o payload do alerta |
| Delivery Result | Resultado de uma tentativa: `delivered`, `failed`, `skipped` |
| `PIPELINE_ALERT_WEBHOOK_URL` | Variável de ambiente com a URL do endpoint receptor |
| `PIPELINE_ALERT_WEBHOOK_TOKEN` | Variável de ambiente opcional com Bearer token para autenticação |
| `PIPELINE_ALERT_WEBHOOK_TIMEOUT_SECONDS` | Tempo máximo de espera por resposta HTTP (default: 5s) |
| `PIPELINE_ALERT_WEBHOOK_MAX_RETRIES` | Número máximo de tentativas antes de desistir (default: 2) |
| `PIPELINE_ALERT_MIN_SEVERITY` | Severidade mínima para disparar entrega (`low`, `medium`, `high`, `critical`) |
| Non-blocking | A falha na entrega do webhook NÃO deve interromper ou afetar o ciclo do pipeline |
| Payload | Corpo JSON enviado no POST ao webhook |

## 3. Requirements, Constraints & Guidelines

### Ativação e configuração

- **REQ-001**: O canal de webhook DEVE ser ativado apenas quando `PIPELINE_ALERT_WEBHOOK_URL` estiver definido e não vazio. Na ausência da variável, o comportamento atual (apenas relatório JSON local) DEVE ser preservado sem nenhuma alteração.
- **REQ-002**: A URL configurada em `PIPELINE_ALERT_WEBHOOK_URL` DEVE ser validada sintaticamente antes de qualquer tentativa de entrega. Se inválida, o sistema DEVE registrar um log de aviso e pular a entrega sem falhar.
- **REQ-003**: O token de autenticação configurado em `PIPELINE_ALERT_WEBHOOK_TOKEN` DEVE ser enviado como header `Authorization: Bearer <token>`. Se ausente, o header não deve ser incluído.
- **REQ-004**: O timeout de cada tentativa DEVE ser controlado por `PIPELINE_ALERT_WEBHOOK_TIMEOUT_SECONDS` (default: `5`, tipo: int). O valor DEVE ser clampado no intervalo `[1, 30]`.
- **REQ-005**: O número máximo de tentativas DEVE ser controlado por `PIPELINE_ALERT_WEBHOOK_MAX_RETRIES` (default: `2`, tipo: int). O valor DEVE ser clampado no intervalo `[1, 5]`. Entre tentativas, o intervalo de espera é fixo em 1 segundo.
- **REQ-006**: A severidade mínima para entrega DEVE ser controlada por `PIPELINE_ALERT_MIN_SEVERITY` (default: `"medium"`). Alertas com severidade abaixo do mínimo DEVEM ser marcados como `skipped` no relatório de entrega.

### Payload

- **REQ-010**: O payload enviado ao webhook DEVE ser um objeto JSON com os campos:

```json
{
  "source": "namastex-pipeline-agent",
  "incident_id": "<string>",
  "severity": "<low|medium|high|critical>",
  "status": "<string>",
  "summary": "<string>",
  "pipeline_status": "<string>",
  "failed_checks": ["<string>", "..."],
  "auto_remediation_applied": "<bool>",
  "timestamp_utc": "<ISO-8601>",
  "details_url": "<string|null>"
}
```

- **REQ-011**: O campo `details_url` DEVE ser `null` quando não configurado. Se `PIPELINE_ALERT_DETAILS_URL` estiver definido, seu valor DEVE ser incluído.
- **REQ-012**: O payload NÃO DEVE conter `lead_key`, dados mascarados, corpo de mensagens ou qualquer campo do schema Silver/Gold.
- **REQ-013**: O Content-Type do POST DEVE ser `application/json`.
- **REQ-014**: O encoding do payload DEVE ser UTF-8.

### Comportamento de entrega

- **REQ-020**: A função de entrega DEVE ser chamada APÓS a persistência local dos relatórios JSON (`_write_reports`). A falha na entrega NÃO deve afetar a persistência local.
- **REQ-021**: Se todas as tentativas falharem, o sistema DEVE registrar um log de erro (`logging.ERROR`) com o código HTTP ou a exceção, e continuar a execução normalmente.
- **REQ-022**: Uma resposta HTTP com código `2xx` DEVE ser considerada entrega bem-sucedida. Qualquer outro código DEVE ser considerado falha e causar nova tentativa (até o limite de `PIPELINE_ALERT_WEBHOOK_MAX_RETRIES`).
- **REQ-023**: O resultado da entrega DEVE ser incluído no relatório de alerta local em `reports/alerts/latest_alert_report.json` no campo `delivery`, com os subcampos: `channel`, `status` (`delivered`/`failed`/`skipped`), `attempts`, `http_status_code` (int ou null), `error` (string ou null).
- **REQ-024**: Se `should_alert = False` no evento de alerta, o webhook NÃO deve ser chamado.

### Segurança

- **SEC-001**: O valor de `PIPELINE_ALERT_WEBHOOK_TOKEN` NUNCA deve aparecer em logs, relatórios JSON ou mensagens de erro. Se logar a URL, truncar após o domínio.
- **SEC-002**: A URL DEVE usar HTTPS. Se `http://` for fornecido, o sistema DEVE registrar um aviso e pular a entrega em produção. Em ambiente de teste, `http://localhost` é permitido.
- **SEC-003**: Nenhuma credencial do pipeline (OPENAI_API_KEY, ANTHROPIC_API_KEY, DATABRICKS_TOKEN) DEVE ser incluída no payload.

### Compatibilidade com plataformas comuns

- **GUD-001**: O payload deve ser compatível com Slack Incoming Webhooks sem pré-processamento pelo receptor (campos mapeáveis para `text` ou `blocks`).
- **GUD-002**: O payload deve ser compatível com PagerDuty Events API v2 via adaptador externo ao pipeline.
- **GUD-003**: O payload deve ser compatível com Microsoft Teams Incoming Webhook (Adaptive Cards v1).

## 4. Interfaces & Data Contracts

### Módulo `src/pipeline/agent/alert_channels.py`

```python
from dataclasses import dataclass
from typing import Literal

DeliveryStatus = Literal["delivered", "failed", "skipped"]

@dataclass(frozen=True)
class DeliveryResult:
    channel: str          # "webhook" ou "none"
    status: DeliveryStatus
    attempts: int
    http_status_code: int | None
    error: str | None

    def as_dict(self) -> dict: ...

def deliver_alert(event: dict) -> DeliveryResult:
    """
    Entrega o evento de alerta ao canal externo configurado.
    Retorna DeliveryResult independente de sucesso ou falha.
    Nunca lança exceção.
    """
```

### Atualização em `src/pipeline/agent/alerts.py`

A função `handle_alerting()` DEVE:
1. Persistir relatórios locais (comportamento atual, sem alteração)
2. Chamar `deliver_alert(event)` quando `event["should_alert"] is True`
3. Incorporar o resultado `DeliveryResult.as_dict()` no campo `delivery` do relatório retornado

### Variáveis de ambiente

| Variável | Tipo | Default | Descrição |
|---|---|---|---|
| `PIPELINE_ALERT_WEBHOOK_URL` | string | `""` | URL do endpoint receptor. Vazio = desabilitado |
| `PIPELINE_ALERT_WEBHOOK_TOKEN` | string | `""` | Bearer token opcional para autenticação |
| `PIPELINE_ALERT_WEBHOOK_TIMEOUT_SECONDS` | int | `5` | Timeout por tentativa em segundos |
| `PIPELINE_ALERT_WEBHOOK_MAX_RETRIES` | int | `2` | Máximo de tentativas |
| `PIPELINE_ALERT_MIN_SEVERITY` | string | `"medium"` | Severidade mínima para entrega |
| `PIPELINE_ALERT_DETAILS_URL` | string | `""` | URL de detalhes incluída no payload |

## 5. Acceptance Criteria

- **AC-001**: Dado `PIPELINE_ALERT_WEBHOOK_URL` não definido, quando o pipeline executa e gera alerta, então nenhuma chamada HTTP é feita e o comportamento local é idêntico ao atual.
- **AC-002**: Dado `PIPELINE_ALERT_WEBHOOK_URL` válido e servidor mock respondendo `200`, quando um alerta `should_alert=True` é gerado, então `latest_alert_report.json` contém `delivery.status = "delivered"` e `delivery.attempts = 1`.
- **AC-003**: Dado servidor mock sempre respondendo `500`, quando um alerta é gerado, então o sistema tenta `PIPELINE_ALERT_WEBHOOK_MAX_RETRIES` vezes, registra `delivery.status = "failed"`, e o pipeline conclui normalmente sem exceção.
- **AC-004**: Dado `PIPELINE_ALERT_MIN_SEVERITY = "high"` e evento com `severity = "low"`, quando o alerta é gerado, então `delivery.status = "skipped"` e nenhuma chamada HTTP é feita.
- **AC-005**: Dado `PIPELINE_ALERT_WEBHOOK_TOKEN = "secret"`, quando o POST é enviado, então o header `Authorization: Bearer secret` está presente e a string `"secret"` não aparece em nenhum log.
- **AC-006**: Dado URL com `http://` (não HTTPS) em ambiente não-teste, quando configurado, então o sistema loga aviso e `delivery.status = "skipped"`.
- **AC-007**: Dado timeout configurado em 1 segundo e servidor mock demorando 5 segundos, quando o alerta é enviado, então a tentativa expira, é registrada como `failed`, e o ciclo do pipeline não é bloqueado.
- **AC-008**: Dado que `should_alert = False`, quando `handle_alerting()` é chamado, então `deliver_alert()` não é chamado.

## 6. Test Automation Strategy

- **Test Levels**: Unit (lógica de retry, payload, validação de URL, filtragem por severidade), Integration (mock HTTP server via `pytest-httpserver` ou `responses`)
- **Frameworks**: pytest, `unittest.mock.patch`, `responses` ou `pytest-httpserver`
- **Arquivo de teste**: `tests/test_alerts.py` (expandir) e/ou `tests/test_alert_channels.py` (novo)
- **Test Data**: Fixtures de evento de alerta — `severity=low/medium/high/critical`, `should_alert=True/False`
- **Segurança**: Testar que `PIPELINE_ALERT_WEBHOOK_TOKEN` não aparece em `caplog` nem no dict retornado
- **CI/CD**: Os testes de alerta DEVEM rodar sem rede real — usar mocks ou servidor local

## 7. Rationale & Context

Alertas persistidos apenas como JSON local são invisíveis para times de operação. Em ambientes Databricks, os arquivos de output ficam em Unity Catalog Volumes e requerem acesso ao workspace para leitura. Um webhook transforma o pipeline em um sistema observável: qualquer falha crítica pode ser enviada ao Slack de operações ou ao PagerDuty sem dependência de infraestrutura adicional. O design non-blocking garante que a entrega do alerta nunca compromete a estabilidade do pipeline em si.

## 8. Dependencies & External Integrations

### External Systems
- **EXT-001**: Qualquer endpoint HTTP/HTTPS que aceite POST JSON — Slack, Discord, Teams, PagerDuty, n8n, Zapier, endpoint próprio.

### Technology Platform Dependencies
- **PLT-001**: Python 3.11 `urllib.request` ou `http.client` (stdlib) — sem dependência nova obrigatória. Se `requests` já estiver em `requirements.txt`, pode ser usado.

### Compliance Dependencies
- **COM-001**: O payload não deve conter dados pessoais — garantido por REQ-012 e SEC-003.

## 9. Examples & Edge Cases

```python
# Exemplo de payload enviado ao webhook
{
  "source": "namastex-pipeline-agent",
  "incident_id": "incident_20260428T143022",
  "severity": "high",
  "status": "manual_intervention_required",
  "summary": "Falha de validação em silver.masked_text_fields_not_leaking",
  "pipeline_status": "validation_failed",
  "failed_checks": ["silver.masked_text_fields_not_leaking"],
  "auto_remediation_applied": false,
  "timestamp_utc": "2026-04-28T14:30:22.000000+00:00",
  "details_url": null
}

# Edge case: webhook URL com trailing slash
# "https://hooks.slack.com/services/T000/B000/xxx/"
# Deve ser aceito sem modificação.

# Edge case: payload com lista failed_checks vazia
# (alerta de healthy após auto-remediação)
# Válido — envia payload com failed_checks = []
```

## 10. Validation Criteria

- `deliver_alert()` nunca lança exceção em nenhum cenário
- `DeliveryResult.status` é sempre um de `"delivered"`, `"failed"`, `"skipped"`
- `delivery.attempts` está no intervalo `[0, PIPELINE_ALERT_WEBHOOK_MAX_RETRIES]`
- Nenhuma referência a `PIPELINE_ALERT_WEBHOOK_TOKEN` aparece em `logging` output capturado
- O campo `delivery` existe em `latest_alert_report.json` após qualquer ciclo com alerta

## 11. Related Specifications / Further Reading

- `spec-architecture-terminal-runtime-logging.md` — padrão de logging estruturado do pipeline
- `spec-architecture-agent-autonomy-governed-by-impact.md` — ciclo de autonomia que gera os eventos de alerta
- `src/pipeline/agent/alerts.py` — implementação atual do sistema de alertas locais
