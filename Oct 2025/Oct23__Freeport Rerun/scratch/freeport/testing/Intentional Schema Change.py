# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Let's simulate that we changed how the blueprints are set and see what happens to the FP table

# COMMAND ----------

# MAGIC %run "/Workspace/Users/pfisch@yipitdata.com/corporate_transformation_blueprints/corporate_transformation_blueprints/scratch_pfisch/freeport/Ready__1Geo/after__geo2"

# COMMAND ----------

from yipit_databricks_utils.future import create_table

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testesteelauder"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

geo_schema = spark.sql(f"""
    SELECT
    * except(major_cat, brand),
    1 as major_cat,
    concat(major_cat, brand) as super_brand
    FROM {prod_schema}.{demo_name}_geographic_analysis
""")

create_table(prod_schema, demo_name+'_geographic_analysis', geo_schema, overwrite=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from
# MAGIC ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testesteelauder"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

freeport_geo_analysis(sandbox_schema, prod_schema, demo_name)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC mmm.
# MAGIC
# MAGIC in a couple of seconds,
# MAGIC I got an error message:
# MAGIC
# MAGIC A column, variable, or function parameter with name `brand` cannot be resolved. Did you mean one of the following? [`gmv`, `month`, `state`, `sub_brand`, `merchant`]. SQLSTATE: 42703
# MAGIC
# MAGIC I will add select *

# COMMAND ----------

# MAGIC %run "/Workspace/Users/pfisch@yipitdata.com/corporate_transformation_blueprints/corporate_transformation_blueprints/scratch_pfisch/freeport/Ready__1Geo/after__geo3"

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testesteelauder"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

freeport_geo_analysis(sandbox_schema, prod_schema, demo_name)

# COMMAND ----------

# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/Brands Dashboard Templates/__centralized_udfs_for_client_dashboard_exports"

# COMMAND ----------

run_everything("testesteelauder")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC If we have an intentional schema change.
# MAGIC
# MAGIC I'd like to add +1 to the end of the table.
# MAGIC that means.
# MAGIC I 

# COMMAND ----------


