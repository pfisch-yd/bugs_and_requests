-- Databricks notebook source
Conhecendo as sequencia de transformacoes da pasta blueprints

market_share_for_column_NULL
=>
filter_items
=>
source_table

E sabendo que a. source_Table é ydx_ove_analysts_silver.items_final

Quero recriar esse trecho  em sql para reprioduzir em todas as tabelas até a inicial


SELECT parent_brand, brand, sub_brand, merchant, SUM(gmv)
FROM yd_sensitive_corporate.ydx_ove_analysts_gold.ove_v38_market_share_for_column_NULL
WHERE parent_brand = "ALLEN + ROTH"
   OR brand = "ALLEN + ROTH"
   OR sub_brand = "ALLEN + ROTH"
GROUP BY 1,2,3,4;

-- COMMAND ----------

SELECT parent_brand, brand, sub_brand, merchant, SUM(gmv) as gmv
  FROM ydx_ove_analysts_silver.items_final
  WHERE parent_brand = "ALLEN + ROTH"
     OR brand = "ALLEN + ROTH"
     OR sub_brand = "ALLEN + ROTH"
  GROUP BY parent_brand, brand, sub_brand, merchant;

-- COMMAND ----------

SELECT parent_brand, brand, sub_brand, merchant, SUM(gmv) as gmv
  FROM yd_sensitive_corporate.ydx_ove_analysts_gold.ove_v38_filter_items
  WHERE parent_brand = "ALLEN + ROTH"
     OR brand = "ALLEN + ROTH"
     OR sub_brand = "ALLEN + ROTH"
  GROUP BY parent_brand, brand, sub_brand, merchant

-- COMMAND ----------

SELECT parent_brand, brand, sub_brand, merchant, SUM(gmv) as gmv
  FROM yd_sensitive_corporate.ydx_ove_analysts_gold.ove_v38_market_share_for_column_NULL
  WHERE parent_brand = "ALLEN + ROTH"
     OR brand = "ALLEN + ROTH"
     OR sub_brand = "ALLEN + ROTH"
  GROUP BY parent_brand, brand, sub_brand, merchant

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 1. Consulta na tabela inicial (source_table):
-- MAGIC   SELECT parent_brand, brand, sub_brand, merchant, SUM(gmv) as gmv
-- MAGIC   FROM ydx_ove_analysts_silver.items_final
-- MAGIC   WHERE parent_brand = "ALLEN + ROTH"
-- MAGIC      OR brand = "ALLEN + ROTH"
-- MAGIC      OR sub_brand = "ALLEN + ROTH"
-- MAGIC   GROUP BY parent_brand, brand, sub_brand, merchant;
-- MAGIC
-- MAGIC   2. Consulta na tabela filter_items:
-- MAGIC   SELECT parent_brand, brand, sub_brand, merchant, SUM(gmv) as gmv
-- MAGIC   FROM yd_sensitive_corporate.ydx_ove_analysts_gold.ove_v38_filter_items
-- MAGIC   WHERE parent_brand = "ALLEN + ROTH"
-- MAGIC      OR brand = "ALLEN + ROTH"
-- MAGIC      OR sub_brand = "ALLEN + ROTH"
-- MAGIC   GROUP BY parent_brand, brand, sub_brand, merchant;
-- MAGIC
-- MAGIC   3. Consulta na tabela final (market_share_for_column_NULL):
-- MAGIC   SELECT parent_brand, brand, sub_brand, merchant, SUM(gmv) as gmv
-- MAGIC   FROM yd_sensitive_corporate.ydx_ove_analysts_gold.ove_v38_market_share_for_column_NULL
-- MAGIC   WHERE parent_brand = "ALLEN + ROTH"
-- MAGIC      OR brand = "ALLEN + ROTH"
-- MAGIC      OR sub_brand = "ALLEN + ROTH"
-- MAGIC   GROUP BY parent_brand, brand, sub_brand, merchant;
