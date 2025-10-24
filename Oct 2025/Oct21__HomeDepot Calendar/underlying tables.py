# Databricks notebook source
# MAGIC %md
# MAGIC - ydx_thd_analysts_silver.items_table
# MAGIC
# MAGIC [items](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/3978516224037562?o=3092962415911490#command/7154780925998295)
# MAGIC
# MAGIC
# MAGIC - ydx_thd_analysts_gold.quarterly_reports
# MAGIC
# MAGIC [quarterly reports](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/1205520871642399?o=3092962415911490#command/1205520871642402)
# MAGIC -- internal tables
# MAGIC
# MAGIC - ydx_thd_analysts_silver.retailer_segment_mapping
# MAGIC [link](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/1214896439294806?o=3092962415911490#command/1214896439294809)
# MAGIC
# MAGIC no quarters

# COMMAND ----------


tab = spark.sql("select order_date, day, week, month, year, fiscal_qtr, fiscal_year from ydx_thd_analysts_silver.items_table")
tab.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_date, day, week, month, year, fiscal_qtr, fiscal_year
# MAGIC FROM
# MAGIC --- yd_sensitive_corporate.ydx_thd_analysts_silver.pro_adj_items_final
# MAGIC --- ydx_thd_analysts_silver.home_depot_pro_adj_items_mapped
# MAGIC --- ydx_thd_analysts_silver.thd_adj_items_final
# MAGIC --- ydx_thd_analysts_silver.thd_adj_app_adj_items
# MAGIC --- ydx_thd_analysts_silver.thd_adj_cat_adj_items
# MAGIC --- ydx_thd_analysts_silver.thd_adj_cat_map_items
# MAGIC --- ydx_thd_analysts_silver.thd_adj_cat_map_items_prelim
# MAGIC --- ydx_thd_analysts_silver.thd_adj_cat_map_items_initial
# MAGIC --- ydx_thd_analysts_silver.thd_adj_geo_reweight_items
# MAGIC ydx_thd_analysts_silver.thd_adj_filtered_items
# MAGIC
# MAGIC limit 250

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## items final
# MAGIC
# MAGIC very complicated, and very granular
# MAGIC
# MAGIC yd_sensitive_corporate.ydx_thd_analysts_silver.pro_adj_items_final
# MAGIC
# MAGIC [pro adj](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/1941792040337489?o=3092962415911490#command/1941792040337492)
# MAGIC
# MAGIC ydx_thd_analysts_silver.thd_adj_items_final
# MAGIC
# MAGIC
# MAGIC ydx_thd_analysts_gold.items_table_export_2025_09_01
# MAGIC
# MAGIC
# MAGIC [thd adj](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/3347229604844401?o=3092962415911490)
# MAGIC

# COMMAND ----------


tab = spark.sql("select * from ydx_thd_analysts_gold.quarterly_reports")
tab.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ydx_thd_analysts_silver.cat_geo_quarterly
# MAGIC
# MAGIC [ydx_thd_analysts_silver.cat_geo_quarterly](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/1205520871642239?o=3092962415911490#command/1205520871642240)
# MAGIC
# MAGIC ydx_thd_analysts_silver.flooring_companies_qtrly
# MAGIC
# MAGIC ydx_thd_analysts_silver.demos_quarterly
# MAGIC
# MAGIC ydx_thd_analysts_silver.pro_diy_quarterly
# MAGIC
# MAGIC
# MAGIC ...
# MAGIC
# MAGIC
# MAGIC Okay,
# MAGIC actually master items is feed by something else https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/3347229604844412?o=3092962415911490#command/3347229604844418

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ydx_thd_analysts_silver.hd_fiscal_calendar
# MAGIC --- WHERE start_date <= CURRENT_DATE() AND end_date >= CURRENT_DATE()
# MAGIC
# MAGIC limit 250

# COMMAND ----------

https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/3347229604844412?o=3092962415911490#command/3347229604844418
