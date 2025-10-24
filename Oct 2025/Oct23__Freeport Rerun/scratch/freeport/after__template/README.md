# FreePort Module Template

Este diretório contém o template padronizado e a documentação para todos os módulos FreePort.

## 📁 Estrutura

```
after__template/
├── README.md                      # Esta documentação
├── freeport_module_template.py    # Template principal com lógica unificada
└── module_config.py                # Configurações de todos os módulos
```

## 🎯 Objetivo

Padronizar a criação de deliverables FreePort eliminando duplicação de código através de:

1. **Template único** que funciona para todos os módulos
2. **Configuração centralizada** com personalização via dicionário
3. **Dois padrões claros**: SIMPLE e DYNAMIC

## 📊 Resumo das Versões Analisadas

### Ready__1Geo (Geographic Analysis)
- **Melhor versão**: `after__geo3.py`
- **Padrão**: SIMPLE com `SELECT *`
- **Evolução**: Começou sem coluna `state`, depois adicionou, testou mudanças de ordem/adição/remoção, finalizou com `SELECT *`

### Ready__2PRO (PRO Insights)
- **Arquivo**: `after__pro.py`
- **Padrão**: DYNAMIC com filtragem de colunas não-nulas
- **Característica**: Usa Jinja2 template para gerar SELECT dinâmico

### Ready__4leakage (Leakage Modules)
5 módulos com padrão idêntico:
- `category_closure` (cclos)
- `leakage_retailer` (lret)
- `leakage_users` (luser)
- `leakage_product` (lprod)
- `market_share` (msha)

**Padrão**: DYNAMIC com filtragem de colunas não-nulas

### Ready__5Product A (SKU Modules)
3 módulos com exclusão de colunas:
- `sku_analysis` (sana) - exclui `product_attributes`
- `sku_detail` (sdet) - exclui `ASP`
- `sku_time_series` (stim) - exclui `product_attributes`

**Padrão**: DYNAMIC com exclusão de colunas ANTES da filtragem

## 🔀 Dois Padrões Identificados

### Pattern 1: SIMPLE
```python
# Usado por: geographic_analysis
query_string = "SELECT * FROM {{ sources[0].full_name }}"
# Não precisa de query_parameters
```

### Pattern 2: DYNAMIC
```python
# Usado por: todos os outros módulos
# 1. Filtra colunas com valores não-nulos
accepted_columns = filter_non_null_columns(...)

# 2. Usa template Jinja2
query_string = """
    SELECT
    {% for column in parameters.columns %}
    {{ column }}{{ ',' if not loop.last else '' }}
    {% endfor %}
    FROM {{ sources[0].full_name }}
"""

# 3. Passa colunas como parâmetro
query_parameters = {"columns": accepted_columns}
```

### Pattern 2b: DYNAMIC com Exclusões
```python
# Usado por: sku_analysis, sku_detail, sku_time_series
# Primeiro exclui colunas específicas, DEPOIS filtra não-nulos
exclude_columns = ["product_attributes"]  # ou ["ASP"]
accepted_columns = filter_non_null_columns(..., exclude_columns)
```

## 🛠️ Como Usar o Template

### Opção 1: Executar módulo individual

```python
from freeport_module_template import freeport_module

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testesteelauder_v38"

# Executar módulo específico
deliverable = freeport_module(
    sandbox_schema,
    prod_schema,
    demo_name,
    "geographic_analysis"  # ou qualquer outro módulo
)
```

### Opção 2: Executar múltiplos módulos

```python
from freeport_module_template import run_all_modules

# Executar todos os módulos
results = run_all_modules(sandbox_schema, prod_schema, demo_name)

# Ou executar apenas alguns
results = run_all_modules(
    sandbox_schema,
    prod_schema,
    demo_name,
    module_list=["geographic_analysis", "pro_insights", "sku_analysis"]
)
```

## 📋 Módulos Disponíveis

| Módulo | Nickname | Table Suffix | Pattern | Exclusões |
|--------|----------|--------------|---------|-----------|
| geographic_analysis | geo | _geographic_analysis | SIMPLE | - |
| pro_insights | pro | _pro_insights | DYNAMIC | - |
| category_closure | cclos | _category_closure | DYNAMIC | - |
| leakage_retailer | lret | _leakage_retailer | DYNAMIC | - |
| leakage_users | luser | _leakage_users | DYNAMIC | - |
| leakage_product | lprod | _leakage_product | DYNAMIC | - |
| market_share | msha | _market_share | DYNAMIC | - |
| sku_analysis | sana | _sku_analysis | DYNAMIC | product_attributes |
| sku_detail | sdet | _sku_detail | DYNAMIC | ASP |
| sku_time_series | stim | _sku_time_series | DYNAMIC | product_attributes |

## 🔧 Personalização via Config

### Estrutura do Config

```python
{
    "table_suffix": "_geographic_analysis",    # Sufixo da tabela
    "table_nickname": "geo",                   # Apelido curto
    "pattern": "SIMPLE",                       # SIMPLE ou DYNAMIC
    "query_type": "SELECT_ALL",                # Tipo de query
    "exclude_columns": [],                     # Colunas a excluir
    "description": "Geographic analysis..."    # Descrição do módulo
}
```

### Lógica IF/ELSE para Personalização

```python
def freeport_module(sandbox_schema, prod_schema, demo_name, module_type):
    config = get_module_config(module_type)

    if config["pattern"] == "SIMPLE":
        # Usa SELECT *
        query_string = "SELECT * FROM {{ sources[0].full_name }}"
        query_parameters = None

    elif config["pattern"] == "DYNAMIC":
        # Filtra colunas não-nulas
        if config["exclude_columns"]:
            # Se tem exclusões, aplica primeiro
            accepted_columns = filter_non_null_columns(
                prod_schema, demo_name,
                config["table_suffix"],
                config["exclude_columns"]
            )
        else:
            # Sem exclusões
            accepted_columns = filter_non_null_columns(
                prod_schema, demo_name,
                config["table_suffix"]
            )

        query_string = """
            SELECT
            {% for column in parameters.columns %}
            {{ column }}{{ ',' if not loop.last else '' }}
            {% endfor %}
            FROM {{ sources[0].full_name }}
        """
        query_parameters = {"columns": accepted_columns}
```

## 🎯 Principais Diferenças Entre Módulos

### 1. Geographic Analysis (ÚNICO SIMPLE)
- ✅ Usa `SELECT *`
- ❌ Não filtra colunas
- ❌ Não verifica nulos

### 2. PRO Insights + Leakage Modules (DYNAMIC Puro)
- ✅ Filtra colunas não-nulas
- ✅ Usa Jinja2 template
- ❌ Não exclui colunas

### 3. SKU Modules (DYNAMIC com Exclusões)
- ✅ Filtra colunas não-nulas
- ✅ Usa Jinja2 template
- ✅ **Exclui colunas específicas ANTES da filtragem**

## 📝 Naming Convention

**Formato do module_name:**
```
corporate_{nickname}_{demo_name}
```

**Exemplos:**
- `corporate_geo_testesteelauder_v38`
- `corporate_pro_testgraco_v38`
- `corporate_luser_testesteelauder_v38`
- `corporate_sana_testgraco_v38`

## 🔍 Função de Filtragem de Colunas

```python
def filter_non_null_columns(prod_schema, demo_name, table_suffix, exclude_columns=None):
    """
    Filtra colunas que têm pelo menos um valor não-nulo.

    Args:
        prod_schema: Schema de produção
        demo_name: Nome da demo
        table_suffix: Sufixo da tabela
        exclude_columns: Lista de colunas para excluir

    Returns:
        Lista de nomes de colunas com valores não-nulos
    """
    # 1. Monta cláusula de exclusão se necessário
    exclude_clause = ""
    if exclude_columns:
        exclude_list = ", ".join(exclude_columns)
        exclude_clause = f" EXCEPT ({exclude_list})"

    # 2. Busca todas as colunas (exceto as excluídas)
    df = spark.sql(f"SELECT *{exclude_clause} FROM {prod_schema}.{demo_name}{table_suffix}")

    # 3. Para cada coluna, verifica se tem valores não-nulos
    accepted_columns = []
    for column in df.columns:
        count = spark.sql(f"""
            SELECT COUNT({column})
            FROM {prod_schema}.{demo_name}{table_suffix}
            WHERE {column} IS NOT NULL
        """).collect()[0][0]

        if count > 0:
            accepted_columns.append(column)

    return accepted_columns
```

## 📚 Referências

- **Geographic Analysis**: `Ready__1Geo/after__geo3.py`
- **PRO Insights**: `Ready__2PRO/after__pro.py`
- **Category Closure**: `Ready__4leakage/(CClos) after__c clos.py`
- **Leakage Retailer**: `Ready__4leakage/(LRetailer) after__l ret.py`
- **Leakage Users**: `Ready__4leakage/(LUser) after__l user.py`
- **Leakage Product**: `Ready__4leakage/(Lprod) after__l prod.py`
- **Market Share**: `Ready__4leakage/(MShar) after__m share.py`
- **SKU Analysis**: `Ready__5Product A/(S Ana) after__s ana.py`
- **SKU Detail**: `Ready__5Product A/(S DET) after__s DET.py`
- **SKU Time Series**: `Ready__5Product A/(S Tim) after__s tim.py`

## ✅ Benefícios do Template

1. **Eliminação de duplicação**: Um arquivo ao invés de 10+
2. **Manutenção centralizada**: Mudanças em um lugar só
3. **Configuração clara**: Diferenças explícitas no config
4. **Execução em lote**: Função para rodar múltiplos módulos
5. **Tratamento de erros**: Try/except para continuar em caso de falha
6. **Documentação**: Config serve como documentação viva

## 🚀 Próximos Passos

1. Testar template com todos os módulos
2. Validar output tables geradas
3. Comparar com versões antigas
4. Migrar código de produção para usar template
5. Adicionar logging e métricas
