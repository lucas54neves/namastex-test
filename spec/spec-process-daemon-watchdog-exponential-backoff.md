---
title: Daemon Watchdog with Exponential Backoff Auto-Restart (GAP-A01)
version: 1.0
date_created: 2026-04-29
owner: Data Engineering
tags: [process, infrastructure, daemon, resilience, autonomy, gap-a01]
---

# Introduction

Esta especificação define os requisitos para adicionar um watchdog com retry e backoff exponencial ao daemon do pipeline (`scripts/run_pipeline_daemon.py`). O objetivo é garantir que qualquer exceção não tratada fora do bloco `try/except` interno de `run_cycle()` não cause parada silenciosa e permanente do processo — o daemon deve se recuperar automaticamente com espera crescente entre tentativas.

## 1. Purpose & Scope

**Propósito:** Fechar o GAP-A01 identificado em `docs/agent-autonomy-gaps.md`: o loop de `run_daemon()` não possui guarda de exceção externa. Qualquer exceção lançada por `run_pipeline()` (erro de escrita em arquivo de relatório, falha de import, erro transitório de OS) termina o processo permanentemente e silenciosamente.

**Escopo:** Apenas `scripts/run_pipeline_daemon.py`. Nenhuma alteração em módulos de orquestração, transformação, agente ou schema.

**Arquivos afetados:**
- `scripts/run_pipeline_daemon.py` — adição do loop de retry com backoff

**Público-alvo:** Engenheiros implementando a correção e revisores de PR.

## 2. Definitions

| Termo | Definição |
|---|---|
| Watchdog | Mecanismo que detecta falhas no processo principal e aciona recuperação automática |
| Backoff exponencial | Estratégia de espera onde o intervalo entre tentativas cresce como `min(2^n, cap)` segundos |
| Ciclo | Uma execução completa de `run_pipeline()` dentro de `run_daemon()` |
| Falha de ciclo | Qualquer exceção não tratada propagada por `run_pipeline()` para fora do seu `try/except` interno |
| Tentativa (attempt) | Contador de falhas consecutivas sem um ciclo bem-sucedido; resetado a zero após sucesso |
| Cap de backoff | Limite máximo de espera entre tentativas, configurável via env var ou argumento |
| `run_cycle()` | Função em `src/pipeline/orchestration/operator.py` que executa bronze → silver → gold e já possui `try/except` interno para falhas de estágio |

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: `run_daemon()` DEVE envolver a chamada a `run_pipeline()` em um bloco `try/except Exception` externo que capture todas as exceções não tratadas que escapem de `run_pipeline()`.
- **REQ-002**: Ao capturar uma exceção, o daemon DEVE logar o erro via `log_event(logging.ERROR, "daemon_cycle_failed", ...)` incluindo: `cycle`, `attempt`, `backoff_seconds`, `error_type` e `error_message`.
- **REQ-003**: Após capturar uma exceção, o daemon DEVE aguardar `min(2 ** attempt, max_backoff_seconds)` segundos antes de tentar novamente, onde `attempt` é o número de falhas consecutivas acumuladas (iniciando em 0).
- **REQ-004**: O campo `max_backoff_seconds` DEVE ser configurável via argumento CLI `--max-backoff-seconds` com valor padrão lido de `os.getenv("PIPELINE_MAX_BACKOFF_SECONDS", 300)`.
- **REQ-005**: Após um ciclo bem-sucedido (nenhuma exceção propagada por `run_pipeline()`), o contador `attempt` DEVE ser resetado para `0`.
- **REQ-006**: `log_event(logging.WARNING, "daemon_backoff", ...)` DEVE ser emitido antes de cada espera de backoff, incluindo `backoff_seconds` calculado e `attempt`.
- **REQ-007**: O comportamento do `finally: shutdown_langfuse_client()` DEVE ser preservado — ainda executado quando o daemon encerra normalmente (por `max_cycles`) ou por `KeyboardInterrupt`.
- **REQ-008**: `KeyboardInterrupt` NÃO DEVE ser capturado pelo watchdog; DEVE propagar normalmente para encerrar o processo, passando pelo `finally`.
- **REQ-009**: A lógica de `--max-cycles` DEVE continuar funcionando: se `args.max_cycles > 0` e `cycle >= args.max_cycles`, o daemon DEVE encerrar mesmo que a última execução tenha sido uma falha (sem entrar em backoff).
- **CON-001**: A interface pública de `run_daemon(args)` NÃO DEVE mudar.
- **CON-002**: O argumento `--max-backoff-seconds` DEVE aceitar apenas inteiros positivos; valor `0` ou negativo DEVE ser rejeitado com erro de parse.
- **CON-003**: O backoff DEVE usar `time.sleep()` — sem dependências externas adicionais.
- **GUD-001**: O valor padrão de `max_backoff_seconds` (300 s) DEVE ser definido como constante nomeada `DEFAULT_MAX_BACKOFF_SECONDS` no módulo, análoga a `DEFAULT_POLL_INTERVAL_SECONDS`.
- **GUD-002**: O `attempt` DEVE ser uma variável local dentro de `run_daemon()`, não um estado global, para garantir que reinícios normais (via `--max-cycles`) não herdem contagens de falha anteriores.

## 4. Interfaces & Data Contracts

### 4.1 Assinatura de `run_daemon()` — sem alteração

```python
def run_daemon(args: argparse.Namespace) -> None:
    ...
```

### 4.2 Novo argumento CLI

```
--max-backoff-seconds INT
    Tempo máximo de espera entre tentativas após falha (segundos).
    Default: PIPELINE_MAX_BACKOFF_SECONDS env var ou 300.
```

### 4.3 Estrutura do loop com watchdog

```python
DEFAULT_MAX_BACKOFF_SECONDS = 300

def run_daemon(args: argparse.Namespace) -> None:
    configure_terminal_logging()
    cycle = 0
    attempt = 0
    try:
        while True:
            cycle += 1
            force = bool(args.force_first_run and cycle == 1)
            log_event(logging.INFO, "daemon_cycle_started", cycle=cycle, force=force,
                      poll_interval_seconds=args.poll_interval_seconds)
            try:
                artifacts = run_pipeline(build_paths(ROOT), force=force)
                attempt = 0  # reset após sucesso
                output = {"cycle": cycle, "poll_interval_seconds": args.poll_interval_seconds}
                output.update(artifacts_as_dict(artifacts))
                print(json.dumps(output, indent=2, ensure_ascii=False), flush=True)
            except Exception as exc:
                backoff = min(2 ** attempt, args.max_backoff_seconds)
                log_event(logging.ERROR, "daemon_cycle_failed", cycle=cycle, attempt=attempt,
                          backoff_seconds=backoff, error_type=type(exc).__name__,
                          error_message=str(exc))
                attempt += 1
                if args.max_cycles and cycle >= args.max_cycles:
                    log_event(logging.INFO, "daemon_stopped", cycle=cycle, reason="max_cycles_reached")
                    break
                log_event(logging.WARNING, "daemon_backoff", backoff_seconds=backoff, attempt=attempt)
                time.sleep(backoff)
                continue

            if args.max_cycles and cycle >= args.max_cycles:
                log_event(logging.INFO, "daemon_stopped", cycle=cycle, reason="max_cycles_reached")
                break

            time.sleep(max(args.poll_interval_seconds, 1))
    finally:
        shutdown_langfuse_client()
```

### 4.4 Eventos de log emitidos pelo watchdog

| Evento | Nível | Campos obrigatórios |
|---|---|---|
| `daemon_cycle_failed` | ERROR | `cycle`, `attempt`, `backoff_seconds`, `error_type`, `error_message` |
| `daemon_backoff` | WARNING | `backoff_seconds`, `attempt` |

### 4.5 Tabela de backoff esperado por tentativa

| `attempt` (0-indexed) | `backoff_seconds` (cap=300) |
|---|---|
| 0 | 1 |
| 1 | 2 |
| 2 | 4 |
| 3 | 8 |
| 4 | 16 |
| 5 | 32 |
| 6 | 64 |
| 7 | 128 |
| 8 | 256 |
| 9+ | 300 (cap) |

## 5. Acceptance Criteria

- **AC-001**: Dado que `run_pipeline()` lança `RuntimeError` em um ciclo, quando o daemon está rodando, então `log_event("daemon_cycle_failed")` DEVE ser emitido com `error_type="RuntimeError"` e `backoff_seconds=1` (primeira falha, `attempt=0`).
- **AC-002**: Dado que `run_pipeline()` falha em 4 ciclos consecutivos, quando o quinto ciclo é iniciado, então o `backoff_seconds` calculado DEVE ser `min(2**3, max_backoff_seconds) = 8` (na quarta falha, `attempt=3`).
- **AC-003**: Dado que após N falhas consecutivas um ciclo conclui com sucesso, quando o próximo ciclo falha, então `backoff_seconds` DEVE ser `1` (reset do `attempt` para `0`).
- **AC-004**: Dado `--max-backoff-seconds 10` e `attempt=9`, quando a backoff é calculada, então `backoff_seconds` DEVE ser `10` (cap aplicado).
- **AC-005**: Dado que `KeyboardInterrupt` é lançado durante `run_pipeline()`, quando o daemon recebe o sinal, então o processo DEVE encerrar normalmente passando pelo `finally: shutdown_langfuse_client()` sem entrar em backoff.
- **AC-006**: Dado `--max-cycles 3` e falha nos 3 primeiros ciclos, quando `cycle >= max_cycles` é atingido mesmo em estado de falha, então o daemon DEVE encerrar sem entrar em backoff.
- **AC-007**: Dado `--max-backoff-seconds 0` ou valor negativo passado via CLI, quando o argumento é parseado, então `argparse` DEVE rejeitar o valor com mensagem de erro antes de iniciar o daemon.

## 6. Test Automation Strategy

- **Test Levels**: Unitário — testar `run_daemon()` com mock de `run_pipeline`, `time.sleep`, e `log_event`.
- **Frameworks**: `pytest`, `unittest.mock.patch`.
- **Arquivo**: `tests/test_daemon_watchdog.py` (novo).

### Cenários obrigatórios

| Teste | Descrição |
|---|---|
| `test_backoff_resets_on_success` | N falhas seguidas de sucesso → `attempt` volta a 0, próximo backoff é 1s |
| `test_backoff_cap_applied` | `attempt` alto com cap baixo → `backoff_seconds == max_backoff_seconds` |
| `test_keyboard_interrupt_not_swallowed` | `KeyboardInterrupt` propaga; `shutdown_langfuse_client` chamado via `finally` |
| `test_max_cycles_exits_on_failure` | `max_cycles=2`, falha nos 2 ciclos → sai sem backoff adicional |
| `test_successful_cycle_no_backoff` | Ciclo sem exceção → `time.sleep` chamado com `poll_interval_seconds`, não com backoff |
| `test_daemon_cycle_failed_event_fields` | Exceção capturada → `log_event` chamado com `error_type` e `backoff_seconds` corretos |

### CI/CD

- `venv/bin/python -m pytest tests/test_daemon_watchdog.py -q` DEVE passar sem modificações adicionais.
- Não requer providers externos, acesso a disco ou ambiente real do pipeline.

## 7. Rationale & Context

O `run_daemon()` atual envolve o loop em `try/finally` exclusivamente para garantir `shutdown_langfuse_client()` no encerramento. Nenhuma exceção propagada por `run_pipeline()` é capturada antes de terminar o processo. `run_cycle()` possui seu próprio `try/except Exception` para falhas de estágio (bronze/silver/gold), mas erros que ocorrem fora desse escopo — como falha de escrita de relatório JSON, erro de I/O no `pipeline_state.json`, ou import dinâmico com falha — propagam para `run_daemon()` sem interceptação e matam o processo.

O resultado é uma parada silenciosa: nenhum alerta é gerado (o sistema de alertas só roda dentro de `run_cycle()`), o fingerprint de fonte para de ser atualizado, e a Gold fica obsoleta indefinidamente. O backoff exponencial com cap é o padrão da indústria para esse padrão porque: (1) evita busy-loop em falhas transitórias, (2) dá tempo ao ambiente para se recuperar em falhas de recurso, e (3) o cap evita esperas excessivas em falhas persistentes.

## 8. Dependencies & External Integrations

### External Systems
- **EXT-001**: Nenhum novo sistema externo. O watchdog usa apenas `time.sleep()` e `logging` da stdlib.

### Infrastructure Dependencies
- **INF-001**: `time` stdlib — para `time.sleep()` no backoff.
- **INF-002**: `os.getenv` — para leitura do env var `PIPELINE_MAX_BACKOFF_SECONDS`; já usado no módulo.

### Technology Platform Dependencies
- **PLT-001**: Python 3.11+ — já obrigatório pelo repositório.

## 9. Examples & Edge Cases

### Sequência de backoff com falhas consecutivas

```
Ciclo 1 → falha → attempt=0 → backoff=1s → attempt vira 1
Ciclo 2 → falha → attempt=1 → backoff=2s → attempt vira 2
Ciclo 3 → falha → attempt=2 → backoff=4s → attempt vira 3
Ciclo 4 → SUCESSO → attempt reset=0
Ciclo 5 → falha → attempt=0 → backoff=1s → attempt vira 1
```

### Variável de ambiente para cap de backoff

```bash
# Reduz o cap para ambientes de desenvolvimento
PIPELINE_MAX_BACKOFF_SECONDS=30 python scripts/run_pipeline_daemon.py

# Usa o default de 300s
python scripts/run_pipeline_daemon.py

# Sobrepõe via CLI (tem precedência sobre env var)
python scripts/run_pipeline_daemon.py --max-backoff-seconds 60
```

### Edge case: `max_cycles` atingido durante estado de falha

```
--max-cycles 3
Ciclo 1 → falha → entra em backoff
Ciclo 2 → falha → entra em backoff
Ciclo 3 → falha → cycle >= max_cycles → encerra sem backoff adicional
```

### Edge case: `attempt=0` — primeiro backoff é 1s (não 0s)

`2 ** 0 = 1` — a primeira tentativa de recuperação espera 1 segundo, não zero. Isso evita loop imediato em casos de erro persistente na inicialização.

## 10. Validation Criteria

- **VAL-001**: `venv/bin/python -m pytest tests/test_daemon_watchdog.py -q` DEVE passar com todos os cenários listados na seção 6.
- **VAL-002**: `venv/bin/python -m pytest -q` (suíte completa) DEVE continuar passando sem regressões.
- **VAL-003**: `grep -n "DEFAULT_MAX_BACKOFF_SECONDS" scripts/run_pipeline_daemon.py` DEVE retornar a definição da constante.
- **VAL-004**: `grep -n "daemon_cycle_failed\|daemon_backoff" scripts/run_pipeline_daemon.py` DEVE retornar as duas emissões de `log_event`.
- **VAL-005**: `grep -n "KeyboardInterrupt" scripts/run_pipeline_daemon.py` DEVE retornar zero resultados — confirmar que `KeyboardInterrupt` não é capturado pelo watchdog.
- **VAL-006**: A tabela de backoff da seção 4.5 DEVE ser reproduzível via: `[min(2**n, 300) for n in range(10)] == [1, 2, 4, 8, 16, 32, 64, 128, 256, 300]`.

## 11. Related Specifications / Further Reading

- [docs/agent-autonomy-gaps.md](../docs/agent-autonomy-gaps.md) — Catálogo completo dos gaps de autonomia; GAP-A01 é a fonte desta spec
- [spec-architecture-agentic-layer-gap-remediation.md](./spec-architecture-agentic-layer-gap-remediation.md) — GAP-01 a GAP-04 originais já implementados
- [spec-design-agentic-layer-quality-gaps.md](./spec-design-agentic-layer-quality-gaps.md) — Lacunas de qualidade da camada agêntica (QUAL-01 a QUAL-04)
