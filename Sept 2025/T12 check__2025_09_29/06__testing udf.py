# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC I want to see the result for the new Market Share
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/Users/pfisch@yipitdata.com/Sept 2025/T12 check__2025_09_29/(2) market_share"

# COMMAND ----------

# DBTITLE 1,FILTER ITEMS
# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_filter_items
# MAGIC WHERE brand = 'samsung'
# MAGIC AND merchant_clean = 'Home Depot'
# MAGIC AND major_cat = 'Laundry Appliances'
# MAGIC AND sub_cat = 'Combination Washer-Dryers'
# MAGIC AND minor_cat = 'All Combination Washer-Dryers'
# MAGIC --- AND special_attribute = 'Laundry Appliances'
# MAGIC AND channel = 'ONLINE'
# MAGIC AND parent_brand = 'SAMSUNG'
# MAGIC and sub_brand = 'samsung'
# MAGIC --- and month_start = '2024-09-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC count(*) over () as rows,
# MAGIC sum(month(date)) OVER (
# MAGIC     partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
# MAGIC     order by month_start ROWS BETWEEN 11 PRECEDING and current row) as partition_factor_trailing_12m
# MAGIC FROM yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_standard_calendar
# MAGIC WHERE brand = 'samsung'
# MAGIC AND merchant = 'Home Depot'
# MAGIC AND major_cat = 'Laundry Appliances'
# MAGIC AND sub_cat = 'Combination Washer-Dryers'
# MAGIC AND minor_cat = 'All Combination Washer-Dryers'
# MAGIC --- AND special_attribute = 'Laundry Appliances'
# MAGIC AND channel = 'ONLINE'
# MAGIC AND parent_brand = 'SAMSUNG'
# MAGIC and sub_brand = 'samsung'
# MAGIC --- and month_start = '2024-09-01'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### What am I gonna do?
# MAGIC
# MAGIC - clone _filter_items to create a internal lowe's filter items
# MAGIC - apply the new market share logic

# COMMAND ----------

clone_filter_items = spark.sql(f"""
    SELECT *
    FROM yd_sensitive_corporate.ydx_lowes_analysts_silver.lowes_v38_filter_items
""")

sandbox_schema = "ydx_internal_analysts_sandbox"
demo_name = 'lowes_v38'

create_table(sandbox_schema, demo_name+'_filter_items',clone_filter_items, overwrite=True)

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = 'lowes_v38'
special_attribute_column_original = []
start_date_of_data = "2021-01-01"

run_export_market_share_module(
        sandbox_schema,
        prod_schema,
        demo_name,
        special_attribute_column_original,
        start_date_of_data
    )

# COMMAND ----------

start_date_of_data = "2021-01-01"
start_date_of_data = "date('" + start_date_of_data + "')"
print(start_date_of_data)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC count(*) over () as rows,
# MAGIC sum(month(date)) OVER (
# MAGIC     partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
# MAGIC     order by month_start ROWS BETWEEN 11 PRECEDING and current row) as partition_factor_trailing_12m
# MAGIC FROM yd_sensitive_corporate.ydx_internal_analysts_gold.lowes_v38_market_share_for_column_null_standard_calendar
# MAGIC WHERE brand = 'samsung'
# MAGIC AND merchant = 'Home Depot'
# MAGIC AND major_cat = 'Laundry Appliances'
# MAGIC AND sub_cat = 'Combination Washer-Dryers'
# MAGIC AND minor_cat = 'All Combination Washer-Dryers'
# MAGIC --- AND special_attribute = 'Laundry Appliances'
# MAGIC AND channel = 'ONLINE'
# MAGIC AND parent_brand = 'SAMSUNG'
# MAGIC and sub_brand = 'samsung'
# MAGIC --- and month_start = '2024-09-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC *
# MAGIC FROM yd_sensitive_corporate.ydx_internal_analysts_gold.lowes_v38_market_share_for_column_null_standard_calendar
# MAGIC WHERE brand = 'samsung'
# MAGIC AND merchant = 'Home Depot'
# MAGIC AND major_cat = 'Laundry Appliances'
# MAGIC AND sub_cat = 'Combination Washer-Dryers'
# MAGIC AND minor_cat = 'All Combination Washer-Dryers'
# MAGIC --- AND special_attribute = 'Laundry Appliances'
# MAGIC AND channel = 'ONLINE'
# MAGIC AND parent_brand = 'SAMSUNG'
# MAGIC and sub_brand = 'samsung'
# MAGIC and month_start = '2024-09-01'
