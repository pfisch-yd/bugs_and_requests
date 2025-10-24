# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/3347229604844412?o=3092962415911490#command/3347229604844418

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH hd_fy AS (
# MAGIC SELECT year, qtr, start_date, end_date
# MAGIC FROM ydx_thd_analysts_silver.hd_fiscal_calendar
# MAGIC )
# MAGIC
# MAGIC SELECT a.*, b.qtr AS fiscal_qtr, b.year AS fiscal_year
# MAGIC FROM ydx_thd_analysts_silver.thd_adj_filtered_items a
# MAGIC LEFT JOIN hd_fy b ON a.order_date BETWEEN b.start_date AND b.end_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT year, qtr, start_date, end_date
# MAGIC FROM ydx_thd_analysts_silver.hd_fiscal_calendar
# MAGIC order by start_date

# COMMAND ----------

"4Q18" = "4Q18"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC with old as (
# MAGIC SELECT year, qtr, start_date, end_date
# MAGIC FROM ydx_thd_analysts_silver.hd_fiscal_calendar
# MAGIC order by start_date
# MAGIC ),
# MAGIC
# MAGIC new as (select * from 
# MAGIC yd_sensitive_corporate.ydx_thd_analysts_silver.thd_calendar_quarters_fiscal_months
# MAGIC order by start_date
# MAGIC )
# MAGIC
# MAGIC select old.*, new.start_date, new.end_date  from old
# MAGIC left join new
# MAGIC using (year, qtr)
# MAGIC
