# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC     date,
# MAGIC     month_start,
# MAGIC     month_end,
# MAGIC     quarter,
# MAGIC     month
# MAGIC FROM yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_nrf_calendar;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC     date,
# MAGIC     month_start,
# MAGIC     month_end,
# MAGIC     quarter,
# MAGIC     month
# MAGIC FROM yd_sensitive_corporate.ydx_target_analysts_gold.target_home_v38_market_share_for_column_null_nrf_calendar;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE yd_sensitive_corporate.ydx_target_analysts_gold.target_home_v38_market_share_for_column_null_nrf_calendar;
# MAGIC
