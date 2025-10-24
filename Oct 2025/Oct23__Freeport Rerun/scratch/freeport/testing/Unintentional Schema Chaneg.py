# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Let's simulate 

# COMMAND ----------

# MAGIC %run "/Workspace/Users/pfisch@yipitdata.com/corporate_transformation_blueprints/corporate_transformation_blueprints/scratch_pfisch/freeport/Ready__1Geo/after__geo2"

# COMMAND ----------

from yipit_databricks_utils.future import create_table

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testgraco"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

geo_schema = spark.sql(f"""
    SELECT
    *
    FROM {prod_schema}.{demo_name}_geographic_analysis
""")

#create_table(prod_schema, demo_name+'_geographic_analysis', geo_schema, overwrite=True)

geo_schema.display()

# COMMAND ----------

geo_schema.dtypes

# COMMAND ----------

# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/Brands Dashboard Templates/__centralized_udfs_for_client_dashboard_exports"

# COMMAND ----------

#run_everything("testgraco")
