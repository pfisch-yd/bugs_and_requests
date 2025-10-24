# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This the definition of normal proceedure.
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/Users/pfisch@yipitdata.com/corporate_transformation_blueprints/corporate_transformation_blueprints/scratch_pfisch/freeport/Ready__1Geo/after__geo2"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis
# MAGIC
# MAGIC limit 10

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testgraco"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

freeport_geo_analysis(sandbox_schema, prod_schema, demo_name)

# COMMAND ----------

# MAGIC %md
# MAGIC yd_fp_corporate_staging.ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis5
# MAGIC
# MAGIC __dmv__000
# MAGIC
# MAGIC okay
# MAGIC
