# 🚀 FreePort Optimization Guide

## ⚠️ Problema de Custo Identificado

A função original `filter_non_null_columns` tem **ALTO CUSTO** porque executa **N queries separadas** (uma por coluna):

```python
# ❌ PROBLEMA: Loop com N queries
for column in all_columns:  # 50 colunas = 50 queries!
    df = spark.sql(f"""
        SELECT COUNT({column})
        FROM {prod_schema}.{demo_name}{table_suffix}
        WHERE {column} IS NOT NULL
    """)
    # Cada query faz FULL TABLE SCAN da tabela inteira!
```

### 💰 Impacto Real:

**Exemplo:** Tabela com 50 colunas e 1 bilhão de linhas

| Abordagem | Queries | Scans | Custo Relativo |
|-----------|---------|-------|----------------|
| ❌ Original (N queries) | 50 | 50 full scans | **100% (baseline)** |
| ✅ Otimizada (1 query) | 1 | 1 full scan | **2% (98% economia!)** |
| ✅✅ Sampling (1 query + sample) | 1 | 1% dos dados | **0.02% (99.98% economia!)** |

---

## ✅ Solução 1: Single Query Optimization (RECOMENDADO)

Ao invés de N queries, executa **1 query única** que verifica todas as colunas:

```python
def filter_non_null_columns_optimized(prod_schema, demo_name, table_suffix, exclude_columns=None):
    """
    ✅ OPTIMIZED: Uses SINGLE query with COUNT(*) FILTER
    """
    # Get column names
    all_columns = get_columns(...)

    # Build single query checking ALL columns at once
    count_expressions = []
    for column in all_columns:
        count_expr = f"COUNT(*) FILTER (WHERE {column} IS NOT NULL) as {column}_count"
        count_expressions.append(count_expr)

    # ONE query to rule them all!
    count_query = f"""
        SELECT
            {','.join(count_expressions)}
        FROM {prod_schema}.{demo_name}{table_suffix}
    """

    counts_df = spark.sql(count_query)
    counts_row = counts_df.collect()[0]

    # Filter columns with count > 0
    accepted_columns = [col for i, col in enumerate(all_columns) if counts_row[i] > 0]

    return accepted_columns
```

### 📊 Comparação:

```
❌ ORIGINAL (50 colunas):
Query 1: SELECT COUNT(col1) FROM table WHERE col1 IS NOT NULL  → Full scan
Query 2: SELECT COUNT(col2) FROM table WHERE col2 IS NOT NULL  → Full scan
...
Query 50: SELECT COUNT(col50) FROM table WHERE col50 IS NOT NULL → Full scan
TOTAL: 50 full table scans

✅ OTIMIZADA (50 colunas):
Query 1: SELECT
    COUNT(*) FILTER (WHERE col1 IS NOT NULL) as col1_count,
    COUNT(*) FILTER (WHERE col2 IS NOT NULL) as col2_count,
    ...
    COUNT(*) FILTER (WHERE col50 IS NOT NULL) as col50_count
FROM table
TOTAL: 1 full table scan

ECONOMIA: 98% de redução de custo!
```

---

## ✅✅ Solução 2: Sampling Optimization (MÁXIMA ECONOMIA)

Se ainda for muito caro, usa **amostragem** da tabela:

```python
def filter_non_null_columns_sampling(
    prod_schema, demo_name, table_suffix,
    exclude_columns=None,
    sample_fraction=0.01  # 1% dos dados
):
    """
    ✅✅ MOST OPTIMIZED: Uses sampling to reduce cost even further
    """
    count_query = f"""
        SELECT
            {','.join(count_expressions)}
        FROM {prod_schema}.{demo_name}{table_suffix}
        TABLESAMPLE ({sample_fraction * 100} PERCENT)  -- Only scans 1% of data!
    """
    # ... rest of logic
```

### Trade-off:
- ✅ **Vantagem**: Custo **99.98% menor**
- ⚠️ **Desvantagem**: Pode perder colunas com poucos valores não-nulos

---

## 🎯 Qual Usar?

### Use **Single Query Optimization** quando:
- ✅ Precisa de **100% precisão**
- ✅ Tabelas até **alguns bilhões de linhas**
- ✅ **98% de economia já é suficiente**
- ✅ **RECOMENDADO para maioria dos casos**

### Use **Sampling** quando:
- ✅ Tabelas **muito grandes** (trilhões de linhas)
- ✅ Pode tolerar perder colunas com **valores raros**
- ✅ Precisa de **máxima economia** de custo
- ⚠️ Entende o trade-off de precisão

---

## 📝 Como Usar as Versões Otimizadas

### Opção 1: Single Query (padrão, recomendado)

```python
from freeport_module_template_optimized import freeport_module

# Usa otimização de single query automaticamente
deliverable = freeport_module(
    sandbox_schema,
    prod_schema,
    demo_name,
    "pro_insights"  # Qualquer módulo DYNAMIC
)
```

### Opção 2: Sampling (máxima economia)

```python
# Usa apenas 1% dos dados para verificar colunas
deliverable = freeport_module(
    sandbox_schema,
    prod_schema,
    demo_name,
    "pro_insights",
    use_sampling=True,      # ✅ Ativa sampling
    sample_fraction=0.01    # 1% dos dados
)
```

### Opção 3: Batch com otimização

```python
from freeport_module_template_optimized import run_all_modules

# Roda todos os módulos com single query optimization
results = run_all_modules(sandbox_schema, prod_schema, demo_name)

# Ou com sampling para máxima economia
results = run_all_modules(
    sandbox_schema,
    prod_schema,
    demo_name,
    use_sampling=True,
    sample_fraction=0.01
)
```

---

## 🔍 Comparação Técnica Detalhada

### Código Original (❌ Caro)

```python
def filter_non_null_columns(prod_schema, demo_name, table_suffix, exclude_columns=None):
    df = spark.sql(f"SELECT *{exclude_clause} FROM {prod_schema}.{demo_name}{table_suffix}")
    all_columns = df.columns
    accepted_columns = []

    # ❌ LOOP: Executa N queries
    for i in range(0, len(all_columns)):
        df = spark.sql(f"""
            with filtered as (
                SELECT {all_columns[i]} as important_column
                FROM {prod_schema}.{demo_name}{table_suffix}
                where {all_columns[i]} is not null
            )
            select count(important_column) as count_rows from filtered
        """)

        if df.collect()[0][0] > 0:
            accepted_columns.append(all_columns[i])

    return accepted_columns
```

**Problemas:**
1. Loop executa `N` queries separadas
2. Cada query faz full table scan
3. Overhead de múltiplas conexões/execuções
4. **Custo proporcional ao número de colunas**

### Código Otimizado (✅ Barato)

```python
def filter_non_null_columns_optimized(prod_schema, demo_name, table_suffix, exclude_columns=None):
    # Get columns (LIMIT 1 = quase zero custo)
    df = spark.sql(f"SELECT *{exclude_clause} FROM {prod_schema}.{demo_name}{table_suffix} LIMIT 1")
    all_columns = df.columns

    # ✅ SINGLE QUERY: Verifica todas as colunas de uma vez
    count_expressions = []
    for column in all_columns:
        count_expr = f"COUNT(*) FILTER (WHERE {column} IS NOT NULL) as {column}_count"
        count_expressions.append(count_expr)

    count_query = f"""
        SELECT {',\n            '.join(count_expressions)}
        FROM {prod_schema}.{demo_name}{table_suffix}
    """

    counts_df = spark.sql(count_query)
    counts_row = counts_df.collect()[0]

    accepted_columns = [col for i, col in enumerate(all_columns) if counts_row[i] > 0]
    return accepted_columns
```

**Vantagens:**
1. ✅ **Apenas 1 query** para todas as colunas
2. ✅ **1 full table scan** ao invés de N
3. ✅ Usa `COUNT(*) FILTER` nativo do Spark (otimizado)
4. ✅ **Custo fixo independente do número de colunas**

---

## 📊 Benchmark Estimado

### Cenário: Tabela com 1 bilhão de linhas, 50 colunas

| Métrica | Original (N queries) | Otimizada (1 query) | Sampling (0.01) |
|---------|---------------------|---------------------|----------------|
| **Queries executadas** | 50 | 1 | 1 |
| **Linhas processadas** | 50B (50×1B) | 1B | 10M (0.01×1B) |
| **Tempo estimado** | ~500 segundos | ~10 segundos | ~0.1 segundo |
| **DBU consumido** | ~50 DBU | ~1 DBU | ~0.01 DBU |
| **Custo estimado** | $50 | $1 | $0.01 |
| **Economia** | Baseline | **98%** 💰 | **99.98%** 💰💰💰 |

*Valores aproximados para ilustração. Custos reais dependem do cluster e região.*

---

## ⚡ Recomendações Finais

1. **✅ Use `freeport_module_template_optimized.py`** ao invés do original
2. **✅ Padrão recomendado**: Single Query Optimization (já 98% mais barato)
3. **⚠️ Para tabelas gigantes**: Considere Sampling com `sample_fraction=0.01`
4. **📊 Monitore custos**: Adicione logging para acompanhar execuções
5. **🧪 Teste primeiro**: Rode em subset pequeno antes de produção

---

## 🔄 Migração do Código Existente

### Antes (❌ Código antigo)

```python
from freeport_module_template import freeport_module

deliverable = freeport_module(sandbox_schema, prod_schema, demo_name, "pro_insights")
# ❌ Usa N queries (caro!)
```

### Depois (✅ Código otimizado)

```python
from freeport_module_template_optimized import freeport_module

deliverable = freeport_module(sandbox_schema, prod_schema, demo_name, "pro_insights")
# ✅ Usa 1 query (98% mais barato!)
```

**Mudança mínima, economia máxima!** 🎉

---

## 📚 Referências

- **Arquivo original**: `freeport_module_template.py`
- **Arquivo otimizado**: `freeport_module_template_optimized.py`
- **Este guia**: `OPTIMIZATION_GUIDE.md`
- **Spark COUNT FILTER docs**: [Spark SQL Reference](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html)
