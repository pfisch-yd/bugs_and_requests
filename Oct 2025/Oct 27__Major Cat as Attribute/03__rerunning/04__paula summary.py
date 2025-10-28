# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select product_attributes from ydx_weber_analysts_gold.weber_v38_sku_time_series
# MAGIC limit 15
# MAGIC
# MAGIC /*
# MAGIC
# MAGIC all rows have {"major_cat":"gas grills"} on it
# MAGIC
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select product_attributes from ydx_weber_analysts_silver.weber_v38_filter_items
# MAGIC limit 15
# MAGIC
# MAGIC /*
# MAGIC We have a lot of nulls,
# MAGIC but a few correct attributes like
# MAGIC
# MAGIC {"Backing":"PVC","Brand/Model Compatibility":"PBV4PS1","CA Residents: Prop 65 Warning(s)":"Yes","Closure Type":"Zipper","Color/Finish Family":"Black","Depth (Inches)":"27","Fits Grill Type":"Vertical smoker","Height (Inches)":"48","Manufacturer Color/Finish":"Black","Number of Handles":"0","Number of Vents":"0","Primary Material":"Polyester","Reversible":"No","Series Name":"Pro Series 4","UNSPSC":"48101500","UV Protection":"Yes","Warranty":"1-year limited","Water Resistant":"Yes","Width (Inches)":"28"}
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select product_attributes from ydx_internal_analysts_sandbox.weber_v38_filter_items
# MAGIC limit 15
# MAGIC
# MAGIC /*
# MAGIC We have a lot of nulls,
# MAGIC but a few correct attributes like
# MAGIC
# MAGIC {"Backing":"PVC","Brand/Model Compatibility":"PBV4PS1","CA Residents: Prop 65 Warning(s)":"Yes","Closure Type":"Zipper","Color/Finish Family":"Black","Depth (Inches)":"27","Fits Grill Type":"Vertical smoker","Height (Inches)":"48","Manufacturer Color/Finish":"Black","Number of Handles":"0","Number of Vents":"0","Primary Material":"Polyester","Reversible":"No","Series Name":"Pro Series 4","UNSPSC":"48101500","UV Protection":"Yes","Warranty":"1-year limited","Water Resistant":"Yes","Width (Inches)":"28"}
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select product_attributes from ydx_internal_analysts_gold.weber_v38_filter_items
# MAGIC limit 15
# MAGIC
# MAGIC /*
# MAGIC everything is null
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY ydx_weber_analysts_gold.weber_v38_sku_time_series

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY ydx_internal_analysts_gold.weber_v38_sku_time_series
