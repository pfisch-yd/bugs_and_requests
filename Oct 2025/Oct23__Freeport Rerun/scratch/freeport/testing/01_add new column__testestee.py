# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This the definition of normal proceedure.
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/Users/pfisch@yipitdata.com/corporate_transformation_blueprints/corporate_transformation_blueprints/scratch_pfisch/freeport/Ready__1Geo/after__geo2__add column test"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis
# MAGIC
# MAGIC limit 10

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testesteelauder"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

# COMMAND ----------

add_column = spark.sql(f"""
    SELECT
    * EXCEPT (new_column),
    "Paula is the best" as new_column
    FROM {prod_schema}.{demo_name}_geographic_analysis
""")

create_table(prod_schema, demo_name+'_geographic_analysis', add_column, overwrite=True)

# COMMAND ----------


freeport_geo_analysis(sandbox_schema, prod_schema, demo_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select new_column from
# MAGIC yd_fp_corporate_staging.ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis5__dmv__000

# COMMAND ----------

store_table = spark.sql(f"""
    SELECT
    *
    fROM {prod_schema}.{demo_name}_geographic_analysis
""")

store_table.display()

# COMMAND ----------

# RE STORE TABLE
create_table(prod_schema, demo_name+'_geographic_analysis', store_table, overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC yd_fp_corporate_staging.ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis5
# MAGIC
# MAGIC __dmv__000
# MAGIC
# MAGIC
# MAGIC We didnt'add a number
# MAGIC
# MAGIC but I dont think I will see the new column

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC *
# MAGIC
# MAGIC from
# MAGIC yd_fp_corporate_staging.ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis5__dmv__000
# MAGIC limit 15

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC *
# MAGIC
# MAGIC from
# MAGIC ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis
# MAGIC limit 15

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # let's duplicate and change the schema
