# Databricks notebook source
# MAGIC %md
# MAGIC # Table vs View: Entendendo as Diferenças
# MAGIC
# MAGIC ## O que é uma **Table** (Tabela)?
# MAGIC
# MAGIC - **Armazena dados fisicamente** no storage (Delta Lake, Parquet, etc.)
# MAGIC - **Ocupa espaço em disco**
# MAGIC - Dados persistem independentemente da query
# MAGIC - Pode ter partições, índices, e otimizações
# MAGIC - **Performance**: Consultas são rápidas pois os dados já estão materializados
# MAGIC - **Uso**: Quando você precisa armazenar dados para uso recorrente
# MAGIC
# MAGIC ## O que é uma **View** (Visão)?
# MAGIC
# MAGIC - **Não armazena dados** - é apenas uma query salva
# MAGIC - **Não ocupa espaço** (apenas a definição SQL)
# MAGIC - Executa a query toda vez que é consultada
# MAGIC - **Performance**: Depende da complexidade da query subjacente
# MAGIC - **Uso**: Para criar abstrações, simplificar queries complexas, ou aplicar filtros/transformações sem duplicar dados

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exemplo Prático: Criando uma Table baseada na tabela fonte

# COMMAND ----------

# Primeiro, vamos ver os dados da tabela original
df_source = spark.table("ydx_internal_analysts_gold.testblueprints_v38_market_share_for_column_null")

# Visualizar schema
print("Schema da tabela fonte:")
df_source.printSchema()

# Visualizar primeiras linhas
display(df_source.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Criando uma TABLE (Materialized)

# COMMAND ----------

# Criar uma nova table com dados materializados
# Esta table irá COPIAR os dados e ocupar espaço em disco

spark.sql("""
    CREATE OR REPLACE TABLE ydx_internal_analysts_gold.example_materialized_table
    USING DELTA
    COMMENT 'Exemplo de tabela materializada - cópia física dos dados'
    AS
    SELECT
        *,
        CURRENT_TIMESTAMP() as created_at
    FROM ydx_internal_analysts_gold.testblueprints_v38_market_share_for_column_null
""")

print("✅ Table criada com sucesso!")

# COMMAND ----------

# Verificar propriedades da table
spark.sql("DESCRIBE EXTENDED ydx_internal_analysts_gold.example_materialized_table").show(100, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Criando uma VIEW (Virtual)

# COMMAND ----------

# Criar uma view - apenas salva a query, não copia dados
# Não ocupa espaço adicional em disco

spark.sql("""
    CREATE OR REPLACE VIEW ydx_internal_analysts_gold.example_virtual_view
    COMMENT 'Exemplo de view virtual - apenas definição SQL'
    AS
    SELECT
        *,
        CURRENT_TIMESTAMP() as viewed_at
    FROM ydx_internal_analysts_gold.testblueprints_v38_market_share_for_column_null
""")

print("✅ View criada com sucesso!")

# COMMAND ----------

# Verificar propriedades da view
spark.sql("DESCRIBE EXTENDED ydx_internal_analysts_gold.example_virtual_view").show(100, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparação Prática

# COMMAND ----------

# Consultar a TABLE
print("Consultando a TABLE:")
df_table = spark.table("ydx_internal_analysts_gold.example_materialized_table")
display(df_table.limit(5))

# COMMAND ----------

# Consultar a VIEW
print("Consultando a VIEW:")
df_view = spark.table("ydx_internal_analysts_gold.example_virtual_view")
display(df_view.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diferenças Principais Observadas
# MAGIC
# MAGIC | Característica | TABLE | VIEW |
# MAGIC |---------------|-------|------|
# MAGIC | **Storage** | Armazena dados fisicamente | Apenas definição SQL |
# MAGIC | **Espaço em disco** | Ocupa espaço | Não ocupa (mínimo) |
# MAGIC | **Performance** | Rápida (dados já materializados) | Depende da query fonte |
# MAGIC | **Atualização** | Dados ficam estáticos (snapshot) | Sempre reflete dados atuais |
# MAGIC | **Timestamp** | `created_at` é fixo | `viewed_at` muda a cada query |
# MAGIC | **Caso de uso** | Dados que mudam pouco, agregações custosas | Abstrações, filtros, dados sempre atuais |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quando usar cada um?
# MAGIC
# MAGIC ### Use **TABLE** quando:
# MAGIC - Você precisa de performance consistente
# MAGIC - Os dados não mudam frequentemente
# MAGIC - Você tem queries complexas/custosas que são executadas repetidamente
# MAGIC - Você precisa particionar ou otimizar os dados
# MAGIC - Exemplo: Tabelas agregadas diárias, snapshots históricos
# MAGIC
# MAGIC ### Use **VIEW** quando:
# MAGIC - Você quer sempre dados atualizados da fonte
# MAGIC - Você quer economizar espaço de storage
# MAGIC - Você quer criar uma abstração/interface sobre tabelas complexas
# MAGIC - A query subjacente é simples e rápida
# MAGIC - Exemplo: Filtros sobre tabelas, junções simples, renomeação de colunas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza (opcional)

# COMMAND ----------

# Descomentar para deletar os objetos criados
# spark.sql("DROP TABLE IF EXISTS ydx_internal_analysts_gold.example_materialized_table")
# spark.sql("DROP VIEW IF EXISTS ydx_internal_analysts_gold.example_virtual_view")
# print("✅ Objetos removidos")
