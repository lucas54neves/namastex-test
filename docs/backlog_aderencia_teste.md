# Backlog Prático de Aderência ao Teste

Este documento transforma a avaliação do projeto em um backlog executável, priorizado e verificável para elevar a entrega à aderência máxima ao enunciado do teste técnico.

## Objetivo

Levar o projeto ao ponto em que um avaliador consiga confirmar, por código, artefatos e documentação, que a solução:

- implementa um pipeline medalhão claro em Python
- mantém a Gold atualizada automaticamente
- protege dados sensíveis de forma efetiva
- organiza a Silver por lead/usuário
- entrega uma Gold analítica útil e defensável
- demonstra um agente que monitora, diagnostica, alerta e corrige ou sugere correção

## Ordem de Implementação

Implementar os tickets nesta sequência:

1. `P0-01` Política de persistência sem PII
2. `P0-02` Validações anti-vazamento de dados sensíveis
3. `P0-03` Remodelagem da Silver principal por lead
4. `P0-04` Adequação da Gold à nova Silver
5. `P0-05` Atualização da documentação de arquitetura e aderência
6. `P1-01` Fortalecimento do planner para propostas estruturadas
7. `P1-02` Separação clara entre remediação operacional e evolução estrutural
8. `P1-03` Testes de aderência ao enunciado
9. `P1-04` Validações semânticas adicionais
10. `P2-01` Enriquecimento analítico da Gold
11. `P2-02` Insight de sentimento ou proxy determinístico
12. `P2-03` Relatório executivo de aderência

## P0

### P0-01: Política de persistência sem PII

**Objetivo**

Eliminar exposição indevida de PII nos artefatos persistidos de `Silver` e `Gold`.

**Escopo**

- revisar quais campos crus podem existir apenas em memória
- remover da persistência em `Silver`:
  - `sender_name`
  - `sender_phone`
  - `message_body`
- garantir que apenas versões seguras sejam publicadas quando necessário:
  - `sender_name_masked`
  - `sender_phone_masked`
  - `message_body_masked`
- revisar se há outros campos sensíveis derivados que precisem ser mascarados ou removidos
- revisar a persistência da `Gold` para evitar vazamento indireto

**Arquivos candidatos**

- `src/pipeline/transforms.py`
- `src/pipeline/io.py`
- `src/pipeline/jobs.py`
- `src/pipeline/operator.py`

**Critérios de aceite**

- nenhum parquet publicado em `data/silver` contém `sender_name`, `sender_phone` ou `message_body` crus
- nenhum parquet publicado em `data/gold` contém PII crua evitável
- a execução completa do pipeline continua funcionando após a mudança

**Dependências**

- nenhuma

---

### P0-02: Validações anti-vazamento de dados sensíveis

**Objetivo**

Transformar a política de masking em contratos automatizados.

**Escopo**

- expandir validações da `Silver` e, se necessário, da `Gold`
- detectar vazamento de:
  - e-mail
  - telefone
  - CPF
  - CEP
  - placa
- adicionar checks para garantir ausência de colunas cruas proibidas nos artefatos publicados
- cobrir cenários com dados mistos em texto livre

**Arquivos candidatos**

- `src/pipeline/quality.py`
- `tests/test_quality.py`
- `tests/test_transforms.py`

**Critérios de aceite**

- existe ao menos um check automatizado para cada tipo principal de PII sensível
- os testes falham se colunas cruas proibidas reaparecerem
- os relatórios de validação passam a refletir explicitamente a política anti-vazamento

**Dependências**

- `P0-01`

---

### P0-03: Remodelagem da Silver principal por lead

**Objetivo**

Fazer a `Silver` atender ao enunciado como camada limpa e organizada por usuário/lead.

**Escopo**

- definir uma chave estável de lead, por exemplo:
  - `lead_id`
  - `lead_key`
- consolidar atributos por lead:
  - nome canônico mascarado
  - telefone mascarado ou hash/chave estável
  - cidade e estado
  - origem do lead
  - campanhas observadas
  - outcomes observados
  - sinais extraídos acumulados
- decidir a modelagem final:
  - `silver` principal por lead
  - opcionalmente uma tabela auxiliar de mensagens, como `silver_messages`
- garantir rastreabilidade entre lead, conversa e mensagem

**Arquivos candidatos**

- `src/pipeline/transforms.py`
- `src/pipeline/spec.py`
- `src/pipeline/compiler.py`
- `config/pipeline_spec.json`

**Critérios de aceite**

- existe uma tabela `Silver` principal com unicidade por lead
- a granularidade principal da `Silver` está documentada e coerente com o enunciado
- a execução do pipeline continua gerando artefatos íntegros

**Dependências**

- `P0-01`
- `P0-02`

---

### P0-04: Adequação da Gold à nova Silver

**Objetivo**

Recalibrar a `Gold` para operar corretamente sobre a nova modelagem da `Silver`.

**Escopo**

- redefinir entradas da agregação analítica considerando a nova camada por lead
- decidir se a `Gold` será:
  - por lead
  - por conversa
  - por lead com métricas consolidadas de múltiplas conversas
- preservar ou revisar segmentações atuais:
  - `persona_profile`
  - `audience_segment`
  - `lead_temperature`
  - `price_sensitivity`
  - `intent_stage`
  - `contact_readiness`
  - `risk_signal`
- garantir que a Gold continue sendo atualizada automaticamente quando a fonte crescer

**Arquivos candidatos**

- `src/pipeline/transforms.py`
- `src/pipeline/quality.py`
- `tests/test_transforms.py`
- `tests/test_jobs.py`

**Critérios de aceite**

- a `Gold` continua sendo produzida automaticamente pelo pipeline
- a granularidade da `Gold` está explícita e é defensável para o caso de uso
- classificações e agregações permanecem consistentes após a remodelagem

**Dependências**

- `P0-03`

---

### P0-05: Atualização da documentação de arquitetura e aderência

**Objetivo**

Alinhar a documentação ao comportamento real do sistema após a remodelagem.

**Escopo**

- atualizar `README.md`
- explicar a granularidade de cada camada
- descrever a política de masking e persistência
- incluir seção objetiva de aderência ao enunciado
- incluir seção de decisões de modelagem e trade-offs
- alinhar a documentação com o contrato vigente da `Silver`:
  - artefato principal `data/silver/silver_leads.parquet`
  - artefato auxiliar `data/silver/silver_messages.parquet`
- alinhar a documentação com a `Gold` publicada em `data/gold/conversations_gold.parquet` com granularidade por `lead_key`
- documentar artefatos gerados em:
  - `data/`
  - `reports/`
  - `state/`

**Arquivos candidatos**

- `README.md`
- `docs/backlog_aderencia_teste.md`

**Critérios de aceite**

- o `README.md` descreve exatamente o comportamento atual do pipeline
- a documentação não promete capacidades que o código não entrega
- um avaliador consegue mapear rapidamente requisito -> implementação

**Dependências**

- `P0-03`
- `P0-04`

## P1

### P1-01: Fortalecimento do planner para propostas estruturadas

**Objetivo**

Evidenciar melhor a capacidade do agente de manter e evoluir o pipeline.

**Escopo**

- ampliar o planner para propor mudanças além de schema
- incluir propostas para:
  - novas validações
  - novas colunas derivadas
  - ajustes de segmentação
  - alterações em regras de transformação
- registrar para cada proposta:
  - contexto detectado
  - impacto esperado
  - risco
  - necessidade de aprovação

**Arquivos candidatos**

- `src/pipeline/planner.py`
- `src/pipeline/spec.py`
- `config/pipeline_spec.json`

**Critérios de aceite**

- o planner gera propostas estruturadas além de novas colunas na Bronze
- cada proposta tem rastreabilidade e classificação de risco
- o relatório do planner é auditável e útil operacionalmente

**Dependências**

- `P0-05`

---

### P1-02: Separação clara entre remediação operacional e evolução estrutural

**Objetivo**

Deixar explícito o que o agente corrige automaticamente e o que ele apenas recomenda.

**Escopo**

- separar, nos relatórios e no código, dois fluxos:
  - remediação operacional
  - evolução estrutural
- ajustar nomenclatura e payloads dos relatórios do agente
- evitar que o projeto pareça prometer autoevolução irrestrita

**Arquivos candidatos**

- `src/pipeline/agent.py`
- `src/pipeline/operator.py`
- `reports/monitoring/` gerados
- `README.md`

**Critérios de aceite**

- os relatórios distinguem remediação de operação e evolução de pipeline
- o comportamento do agente fica tecnicamente preciso e defensável
- a documentação descreve a autonomia real do sistema

**Dependências**

- `P1-01`

---

### P1-03: Testes de aderência ao enunciado

**Objetivo**

Traduzir os requisitos do teste em uma suíte explícita de conformidade.

**Escopo**

- criar testes cobrindo:
  - `Silver` principal por lead
  - ausência de PII crua em artefatos publicados
  - atualização da `Gold` após crescimento da Bronze
  - persistência do estado operacional
  - geração de alertas e diagnósticos em falhas simuladas
- agrupar esses testes de forma identificável

**Arquivos candidatos**

- `tests/test_jobs.py`
- `tests/test_quality.py`
- `tests/test_agent.py`
- `tests/test_daemon.py`
- novo arquivo como `tests/test_requirements_adherence.py`

**Critérios de aceite**

- existe uma suíte dedicada ou claramente identificável para aderência ao teste
- a suíte falha quando qualquer requisito principal deixa de ser atendido
- os testes rodam no fluxo padrão do projeto

**Dependências**

- `P0-05`
- `P1-02`

---

### P1-04: Validações semânticas adicionais

**Objetivo**

Cobrir regras do domínio descritas no dicionário de dados e no enunciado.

**Escopo**

- validar que a primeira mensagem de cada conversa é `outbound`
- validar consistência entre lead, conversa e agregações da `Gold`
- validar coerência de buckets e classificações
- revisar checks atuais para evitar checks “informativos” marcados sempre como `passed`

**Arquivos candidatos**

- `src/pipeline/quality.py`
- `tests/test_quality.py`

**Critérios de aceite**

- regras de domínio relevantes do enunciado passam a ser validadas de fato
- checks meramente informativos são reclassificados ou substituídos por validações reais
- relatórios de qualidade ficam mais úteis para avaliação técnica

**Dependências**

- `P1-03`

## P2

### P2-01: Enriquecimento analítico da Gold

**Objetivo**

Elevar criatividade analítica e valor de negócio da camada `Gold`.

**Escopo**

- adicionar ou formalizar métricas e agrupamentos como:
  - provedor de e-mail dominante
  - tempo médio de resposta por segmento
  - taxa de fechamento por perfil
  - intensidade de objeção por preço
  - sinais de urgência comercial
  - comparação com concorrentes por perfil
- priorizar insights não triviais e defensáveis

**Arquivos candidatos**

- `src/pipeline/transforms.py`
- `src/pipeline/quality.py`
- `tests/test_transforms.py`
- `README.md`

**Critérios de aceite**

- a Gold contém insights analíticos além do mínimo esperado
- os novos campos têm definição documentada e regra verificável
- as classificações continuam consistentes com os dados disponíveis

**Dependências**

- `P0-04`

---

### P2-02: Insight de sentimento ou proxy determinístico

**Objetivo**

Cobrir o espaço analítico sugerido pelo enunciado sem depender de LLM.

**Escopo**

- implementar análise de sentimento heurística ou proxy determinístico
- documentar claramente limitações e sinais usados
- integrar o resultado à Gold se ele agregar valor real

**Arquivos candidatos**

- `src/pipeline/transforms.py`
- `tests/test_transforms.py`
- `README.md`

**Critérios de aceite**

- existe ao menos um indicador de sentimento ou humor conversacional documentado
- a regra é reproduzível e testável
- o indicador não compromete a clareza do modelo analítico

**Dependências**

- `P2-01`

---

### P2-03: Relatório executivo de aderência

**Objetivo**

Facilitar a avaliação final do projeto sem depender de leitura completa do código.

**Escopo**

- criar um artefato resumindo:
  - requisitos do enunciado
  - como cada um foi atendido
  - evidência em código, artefatos e testes
- pode ser um documento em `docs/` ou seção adicional do `README`

**Arquivos candidatos**

- novo arquivo em `docs/`
- `README.md`

**Critérios de aceite**

- um avaliador consegue comprovar aderência sem percorrer o repositório inteiro
- há evidência objetiva por requisito

**Dependências**

- `P1-03`
- `P2-01`

## Quadro Resumido

| Ticket | Prioridade | Resultado esperado |
|---|---|---|
| `P0-01` | P0 | Artefatos sem PII crua indevida |
| `P0-02` | P0 | Validações anti-vazamento automatizadas |
| `P0-03` | P0 | Silver principal por lead |
| `P0-04` | P0 | Gold adaptada à nova modelagem |
| `P0-05` | P0 | Documentação coerente com a implementação |
| `P1-01` | P1 | Planner com propostas estruturadas |
| `P1-02` | P1 | Separação entre remediação e evolução |
| `P1-03` | P1 | Suíte de aderência ao enunciado |
| `P1-04` | P1 | Validações semânticas mais fortes |
| `P2-01` | P2 | Gold mais criativa e analítica |
| `P2-02` | P2 | Sentimento ou proxy determinístico |
| `P2-03` | P2 | Relatório executivo de aderência |

## Definição de Pronto

O backlog será considerado concluído quando:

- os tickets `P0` estiverem implementados e validados
- a documentação estiver atualizada no mesmo ciclo das mudanças
- a suíte de testes passar integralmente
- a aderência ao enunciado puder ser demonstrada por código, artefatos e documentação
