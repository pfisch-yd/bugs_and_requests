# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC select 
# MAGIC date,
# MAGIC channel,
# MAGIC parent_brand,
# MAGIC brand,
# MAGIC sub_brand,
# MAGIC merchant, major_cat, sub_cat, minor_cat, special_attribute,
# MAGIC sum(gmv) OVER (
# MAGIC     partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
# MAGIC     order by month_start ROWS BETWEEN 11 PRECEDING and current row) as gmv_trailing_12m,
# MAGIC sum(units) OVER (
# MAGIC     partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
# MAGIC     order by month_start ROWS BETWEEN 11 PRECEDING and current row) as units_trailing_12m,
# MAGIC sum(sample_size) OVER (
# MAGIC     partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
# MAGIC     order by month_start ROWS BETWEEN 11 PRECEDING and current row) as sample_size_trailing_12m,
# MAGIC sum(month(date)) OVER (
# MAGIC     partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
# MAGIC     order by month_start ROWS BETWEEN 11 PRECEDING and current row) as partition_factor_trailing_12m
# MAGIC FROM yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_standard_calendar
# MAGIC WHERE brand = "samsung"
# MAGIC )
# MAGIC WHERE date >= "2025-08-01"
# MAGIC AND partition_factor_trailing_12m < 78;
# MAGIC
# MAGIC
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC SELECT
# MAGIC   date,
# MAGIC   month_start,
# MAGIC   month(date),
# MAGIC   sum(month(date)) OVER (
# MAGIC     partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
# MAGIC     order by month_start ROWS BETWEEN 11 PRECEDING and current row) as partition_factor_trailing_12m,
# MAGIC   sum(gmv) OVER (
# MAGIC     partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
# MAGIC     order by month_start ROWS BETWEEN 11 PRECEDING and current row) as gmv
# MAGIC FROM yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_standard_calendar
# MAGIC WHERE brand = 'samsung'
# MAGIC AND merchant = 'Home Depot'
# MAGIC AND major_cat = 'Laundry Appliances'
# MAGIC AND sub_cat = 'Combination Washer-Dryers'
# MAGIC AND minor_cat = 'All Combination Washer-Dryers'
# MAGIC AND special_attribute = 'Laundry Appliances'
# MAGIC AND channel = 'ONLINE'
# MAGIC AND parent_brand = 'SAMSUNG'
# MAGIC and sub_brand = 'samsung'
# MAGIC )
# MAGIC WHERE month_start >= '2024-08-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_standard_calendar
# MAGIC WHERE brand = 'samsung'
# MAGIC AND merchant = 'Home Depot'
# MAGIC AND major_cat = 'Laundry Appliances'
# MAGIC AND sub_cat = 'Combination Washer-Dryers'
# MAGIC AND minor_cat = 'All Combination Washer-Dryers'
# MAGIC AND special_attribute = 'Laundry Appliances'
# MAGIC AND channel = 'ONLINE'
# MAGIC AND parent_brand = 'SAMSUNG'
# MAGIC and sub_brand = 'samsung'
# MAGIC and month_start = '2024-09-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_filter_items
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
