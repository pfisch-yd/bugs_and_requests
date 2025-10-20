# Databricks notebook source
# MAGIC %sql
# MAGIC select *
# MAGIC FROM yd_sensitive_corporate.ydx_estee_lauder_analysts_gold.estee_lauder_v38_sku_analysis
# MAGIC where
# MAGIC new_sku_flag = 1
# MAGIC and
# MAGIC web_description = "Shape Tape Ultra Creamy Concealer"
# MAGIC and display_interval = "Last Year"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with appearances as (
# MAGIC select
# MAGIC   order_date,
# MAGIC   md5(concat(
# MAGIC   coalesce(merchant),
# MAGIC   coalesce(brand, ""),
# MAGIC   web_description,
# MAGIC   coalesce(sku, '')
# MAGIC   )) as product_hash,
# MAGIC   merchant,
# MAGIC   brand,
# MAGIC   web_description,
# MAGIC   sku
# MAGIC from ydx_estee_lauder_analysts_silver.source_table
# MAGIC where
# MAGIC web_description = "Shape Tape Ultra Creamy Concealer"
# MAGIC and
# MAGIC brand = "tarte"
# MAGIC )
# MAGIC
# MAGIC select
# MAGIC   sku,
# MAGIC   min(order_date) as first_appearance
# MAGIC from appearances
# MAGIC group by 1
# MAGIC order by 1
