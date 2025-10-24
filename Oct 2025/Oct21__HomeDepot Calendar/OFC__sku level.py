# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       web_description,
# MAGIC       quarter,
# MAGIC       r4q_gmv,
# MAGIC       r4q_sample_size,
# MAGIC       md5 (concat (web_description, brand, merchant)) as hash
# MAGIC     FROM
# MAGIC       yd_sensitive_corporate.ydx_thd_analysts_gold.sigma_product_insights_v3
# MAGIC     WHERE
# MAGIC       web_description = {{product-control}}
# MAGIC   )
# MAGIC WHERE
# MAGIC   hash = {{Hash}}

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct quarter from 
# MAGIC yd_sensitive_corporate.ydx_thd_analysts_gold.sigma_product_insights_v3

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC [yd_sensitive_corporate.ydx_thd_analysts_gold.sigma_product_insights_v3](yipitdata-corporate.cloud.databricks.com/editor/notebooks/1323528892883924?o=3092962415911490)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       web_description,
# MAGIC       quarter,
# MAGIC       r4q_gmv,
# MAGIC       r4q_sample_size,
# MAGIC       md5 (concat (web_description, brand, merchant)) as hash
# MAGIC     FROM
# MAGIC       yd_sensitive_corporate.ydx_thd_analysts_gold.sigma_product_insights_v3
# MAGIC     WHERE
# MAGIC       web_description = {{product-control}}
# MAGIC   )
# MAGIC WHERE
# MAGIC   hash = {{Hash}}
