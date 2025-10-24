# FREEPORT

Plataforma:
- gerenciamento de dados
- entrega de analytics
- camada de orquestração

*

## 1. Gerenciamento de Dados:
- versionamento
- materialização
- distribuição (release)
- rastreabilidade
- automação

### 1.1 Benefícios do Freeporte
✅ Imutabilidade: Materializations nunca são modificadas
✅ Rastreabilidade: Sabe exatamente o que cada cliente recebeu e quando
✅ Versionamento Automático: Gerencia semantic versioning automaticamente
✅ Rollback: Pode apontar prod views para versões antigas
✅ Auditoria: Audit catalog mantém histórico permanente
✅ Integração com Dispatch: Orquestra entregas para centenas de clientes
✅ Schema Validation: Previne breaking changes acidentais

### 1.2 Limitaçoes e Trade Offs
⚠️ API-driven: Tudo via API, não há UI completa
⚠️ Curva de aprendizado: Django + Airflow + Databricks + Unity Catalog
⚠️ Debugging complexo: Precisa checar Django Admin + Airflow + Databricks
⚠️ Imutabilidade: Não pode "editar" uma materialization, precisa criar nova
⚠️ Dependência de Dispatch: Releases são orquestrados por sistema externo
⚠️ Resource limits: Databricks Jobs API tem limite de tamanho de parâmetros (por isso configs vão no Unity Catalog)

### 1.1.1 Entendendo os benefícios

### 1.1.1.1 Entendendo a imutabilidade


**O que significa "Imutabilidade"?
Imutabilidade no Freeport significa que:**
❌ Você NUNCA edita uma Materialization existente
❌ Você NUNCA sobrescreve uma versão de tabela já criada (Delta version)
✅ Você SEMPRE cria NOVA Materialization para qualquer mudança
✅ Cada Materialization gera uma NOVA versão Delta da tabela
Analogia: Como commits do Git - cada commit é imutável, você cria novos commits, não edita os antigos.

Me fala mais sobre a imutabilidade (✅ Imutabilidade: Materializations nunca são modificadas)

Eu tenho alguns cenários.
Cenário 1 : Eu entrego um relatório mensal, mês que vem, iremos acrescentar mais linhas, correspondentes ao novo mês o que irá acontecer com a tabela?

Cenário 2 : Descobrimos que algumas estimativas estavam erradas, não vamos acrescentar linhas, mas os valores de algumas colunas serão re-calculados. O que irá acontecer?

Cenário 3 : Decidimos acrescentar uma coluna com o nome do país na tabela. O que irá acontecer?
