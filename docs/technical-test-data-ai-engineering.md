# Teste Técnico

_Pipeline de Transformação Agêntica de Dados_

### Contexto

O objetivo é avaliar a capacidade de construir um agente que cria, gerencia e mantém um pipeline de transformação de dados, não apenas executa análises pontuais.

### Desafio

Construir um agente que cria e gerencia um pipeline de transformação de dados em 3 camadas, utilizando dados transacionais de conversas no canal de WhatsApp entre lead e vendedor humano.

O agente deve:

- Criar o pipeline de transformação em código Python
- Gerenciar o pipeline de forma autônoma (se quebrar, identifica, alerta e corrige ou ao menos sugere correção)
- Manter o pipeline "vivo", atualizando automaticamente a camada Gold conforme novos dados chegam na fonte

Importante: O agente NÃO deve fazer a análise pontualmente a cada execução. Ele deve construir o pipeline como infraestrutura persistente, similar a um job no Databricks.

## Pipeline em Camada Medalhão

### Camada Bronze

- Todas as conversas transacionais de um chatbot no formato original
- Dados não estruturados: mensagens de texto dos leads, metadados de conversas
- Inclui coluna de mensagem com dados não estruturados

### Camada Silver

- Dados limpos e organizados por usuário/lead
- Extração de informações relevantes das mensagens (e-mails, dados de contato, etc.)
- Transformação e limpeza dos dados brutos
- Remoção de ruído e normalização

### Camada Gold

- Tabela analítica com classificações e agrupamentos
- Criação de audiências e personas a partir dos dados transformados
- Esta camada atualiza automaticamente quando novos dados chegam na Bronze
- Exemplos de insights esperados:
  - Provedores de e-mail mais usados pelos leads
  - Classificação de personas/perfis de usuários
  - Segmentação por audiência
  - Análise de sentimento do cliente

## Requisitos

### Obrigatórios

- Python puro: o pipeline deve ser implementado em código Python
- AI Agent: o agente cria E gerencia o pipeline (auto-correção em caso de falha)
- Pipeline vivo: atualização automática da Gold quando a fonte de dados cresce
- 3 camadas: Bronze → Silver → Gold com transformações claras em cada etapa
- Código disponível em repositório público no GitHub

### Diferenciais (espaço para criatividade)

- Destaque para pipelines que utilizam o Databricks (free tier atende)
- Tipos de classificação e insights gerados na camada Gold
- Abordagem de limpeza e transformação dos dados
- Criatividade nos agrupamentos e segmentações
- Robustez do mecanismo de auto-correção do agente
- É recomendado fugir dos exemplos de insights

### Base de Dados

- Fonte: dados transacionais de conversas (`conversations_bronze.parquet`)
- Formato: mensagens de texto de leads com metadados
- Volume estimado: ~15 mil conversas transcritas
- Dados sensíveis (nomes, e-mails) devem ser mascarados com as mesmas dimensões
- [Data Dictionary](./data-dictionary-data-ai-engineering.md)

## Avaliação

### Engenharia de dados e criatividade analítica

Conseguiu projetar um pipeline robusto com camadas claras de transformação? Entendeu a diferença entre análise pontual e infraestrutura de dados? Que insights conseguiu extrair? Como classificou e segmentou os dados?

### Capacidade de construir agentes

O agente é autônomo? Se auto-corrige? Gerencia o pipeline como um processo contínuo?

### Qualidade de código

O código é limpo, bem estruturado, documentado? Segue boas práticas?
