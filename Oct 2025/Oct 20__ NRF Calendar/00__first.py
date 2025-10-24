# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT DISTINCT date, month_start, month_end, quarter, month
# MAGIC FROM yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_nrf_calendar
# MAGIC order by date desc limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT date, month_start, month_end, quarter, month
# MAGIC FROM yd_sensitive_corporate.ydx_target_analysts_gold.target_home_v38_market_share_for_column_null_nrf_calendar
# MAGIC order by date desc
# MAGIC limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE yd_sensitive_corporate.ydx_target_analysts_gold.target_home_v38_market_share_for_column_null_nrf_calendar;
# MAGIC
