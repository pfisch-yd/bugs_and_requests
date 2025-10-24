# Databricks notebook source
# MAGIC %run "/Workspace/Users/pfisch@yipitdata.com/corporate_transformation_blueprints/corporate_transformation_blueprints/scratch_pfisch/freeport/Ready__1Geo/after__geo2"

# COMMAND ----------

prod_schema = "ydx_werner_analysts_gold"
demo_name = "werner" + "_v38"

store_table = spark.sql(f"""
    SELECT
    *
    fROM {prod_schema}.{demo_name}_geographic_analysis
""")

create_table("ydx_internal_analysts_gold", demo_name+'_geographic_analysis', store_table, overwrite=True)

# COMMAND ----------

prod_schema = "ydx_ove_analysts_gold"
demo_name = "ove" + "_v38"

store_table = spark.sql(f"""
    SELECT
    *
    fROM {prod_schema}.{demo_name}_geographic_analysis
""")

create_table("ydx_internal_analysts_gold", demo_name+'_geographic_analysis', store_table, overwrite=True)

# COMMAND ----------

prod_schema = "ydx_renin_analysts_gold"
demo_name = "renin" + "_v38"

store_table = spark.sql(f"""
    SELECT
    *
    fROM {prod_schema}.{demo_name}_geographic_analysis
""")

create_table("ydx_internal_analysts_gold", demo_name+'_geographic_analysis', store_table, overwrite=True)

# COMMAND ----------

prod_schema = "ydx_generac_analysts_gold"
demo_name = "generac" + "_v38"

store_table = spark.sql(f"""
    SELECT
    *
    fROM {prod_schema}.{demo_name}_geographic_analysis
""")

create_table("ydx_internal_analysts_gold", demo_name+'_geographic_analysis', store_table, overwrite=True)
