# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC What has happend?
# MAGIC
# MAGIC The first trial,
# MAGIC client 1: testesteelauder got dmv__000
# MAGIC client 2: testgraco got dmv__001
# MAGIC We'd like to get the 000 attached to the version in the blueprints.

# COMMAND ----------

# MAGIC %run "/Workspace/Users/pfisch@yipitdata.com/corporate_transformation_blueprints/corporate_transformation_blueprints/scratch_pfisch/freeport/Ready__1Geo/after__geo2"

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testesteelauder"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

freeport_geo_analysis(sandbox_schema, prod_schema, demo_name)

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testgraco"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

freeport_geo_analysis(sandbox_schema, prod_schema, demo_name)
