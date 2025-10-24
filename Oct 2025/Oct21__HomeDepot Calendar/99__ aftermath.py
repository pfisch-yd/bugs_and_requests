# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM telemetry.ydbu_function_usage
# MAGIC WHERE function_name = 'create_table'
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 100;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   args as oi,
# MAGIC   args like "%_test%" as testing,
# MAGIC   *
# MAGIC FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
# MAGIC where name = "create_table"
# MAGIC and args like "%ydx_thd_analysts_gold%"
# MAGIC
# MAGIC AND end_timestamp > CURRENT_DATE() - INTERVAL 2 DAY

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC brand insigths
# MAGIC
# MAGIC [brand_view_dash_v3_test_portal](yipitdata-corporate.cloud.databricks.com/editor/notebooks/1214896439294806?o=3092962415911490)
# MAGIC - ydx_thd_analysts_gold.quarterly_reports
# MAGIC - ydx_thd_analysts_silver.retailer_segment_mapping
# MAGIC [link to cell](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/1214896439294806?o=3092962415911490#command/1214896439294826)
# MAGIC
# MAGIC
# MAGIC
# MAGIC [brand_view_dash_v3](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/3183149375076733?o=3092962415911490)
# MAGIC
# MAGIC exports table
# MAGIC
# MAGIC [sigma_data_export_test_portal](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/1214896439294806?o=3092962415911490#command/1214896439294841)
# MAGIC - ydx_thd_analysts_silver.retailer_segment_mapping
# MAGIC - ydx_thd_analysts_gold.quarterly_reports
# MAGIC
# MAGIC generation
# MAGIC
# MAGIC [sigma_quarterly_demo_v2](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/1214896439294806?o=3092962415911490#command/1214896439294807)
# MAGIC
# MAGIC sku level
# MAGIC
# MAGIC [yd_sensitive_corporate.ydx_thd_analysts_gold.sigma_product_insights_v3](yipitdata-corporate.cloud.databricks.com/editor/notebooks/1323528892883924?o=3092962415911490)
# MAGIC
# MAGIC - ydx_thd_analysts_silver.items_table
# MAGIC - ydx_thd_analysts_gold.quarterly_reports
# MAGIC - ydx_thd_analysts_silver.retailer_segment_mapping
# MAGIC
# MAGIC competitor
# MAGIC
# MAGIC [sigma_competitor_share_v2_test_portal](yipitdata-corporate.cloud.databricks.com/editor/notebooks/1214896439294806?o=3092962415911490)
# MAGIC
# MAGIC
# MAGIC [sigma_competitor_share_v2](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/3183149375076733?o=3092962415911490#command/3183149375076744)
# MAGIC
# MAGIC 2 tabs
# MAGIC
# MAGIC price bands
# MAGIC
# MAGIC
# MAGIC [ydx_internal_analysts_gold.market_size](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/1292773254552225?o=3092962415911490#command/8889646404517165)
# MAGIC - 2 tables
# MAGIC
# MAGIC
# MAGIC [thd_price_bands_charts_vf](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/3485706836239686?o=3092962415911490)
# MAGIC - ydx_thd_analysts_silver.items_table
# MAGIC
# MAGIC [quarterly_reports](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/2654141369252334?o=3092962415911490)
