# Databricks notebook source
# MAGIC %sql
# MAGIC
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

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null
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
# MAGIC ### My guess is that we've added an Arg in the group by and forgot to add the same arg to the TRAILING LOGIC (sum_over)
