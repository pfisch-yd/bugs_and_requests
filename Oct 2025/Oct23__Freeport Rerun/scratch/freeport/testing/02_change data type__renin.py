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
# MAGIC select * from ydx_internal_analysts_gold.renin_v38_geographic_analysis
# MAGIC
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT column_name, data_type
# MAGIC FROM information_schema.columns
# MAGIC WHERE table_name = 'renin_v38_geographic_analysis'
# MAGIC and column_name = "state"

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "renin"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

# COMMAND ----------

add_column = spark.sql(f"""
    SELECT
    * except (state),
    2025 as state
    FROM {prod_schema}.{demo_name}_geographic_analysis
""")

add_column.display()

# COMMAND ----------

store_table = spark.sql(f"""
    SELECT
    *
    fROM {prod_schema}.{demo_name}_geographic_analysis
""")

store_table.display()

# COMMAND ----------

create_table(prod_schema, demo_name+'_geographic_analysis', add_column, overwrite=True)

# COMMAND ----------


freeport_geo_analysis(sandbox_schema, prod_schema, demo_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT column_name, data_type
# MAGIC FROM information_schema.columns
# MAGIC WHERE table_name = 'renin_v38_geographic_analysis'
# MAGIC and column_name = "state"

# COMMAND ----------

# yd_fp_corporate_staging.ydx_internal_analysts_gold.renin_v38_geographic_analysis5__dmv__001

tab1 = spark.sql(f"""
    SELECT
    *
    FROM yd_fp_corporate_staging.ydx_internal_analysts_gold.renin_v38_geographic_analysis5__dmv__001
""")

tab1.printSchema()

# COMMAND ----------

# RE STORE TABLE
create_table(prod_schema, demo_name+'_geographic_analysis', store_table, overwrite=True)

# COMMAND ----------

# yd_fp_corporate_staging.ydx_internal_analysts_gold.renin_v38_geographic_analysis5__dmv__001

tab2 = spark.sql(f"""
    SELECT
    *
    FROM yd_sensitive_corporate.ydx_internal_analysts_gold.renin_v38_geographic_analysis
""")

tab2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC yd_fp_corporate_staging.ydx_internal_analysts_gold.testgraco_v38_geographic_analysis5
# MAGIC
# MAGIC __dmv__002

# COMMAND ----------

df_000 = spark.sql(f"""
    SELECT
    *
    FROM yd_fp_corporate_staging.ydx_internal_analysts_gold.testgraco_v38_geographic_analysis5__dmv__001
""")

df_001 = spark.sql(f"""
    SELECT
    *
    FROM yd_fp_corporate_staging.ydx_internal_analysts_gold.testgraco_v38_geographic_analysis5__dmv__001
""")

df_002 = spark.sql(f"""
    SELECT
    *
    FROM yd_fp_corporate_staging.ydx_internal_analysts_gold.testgraco_v38_geographic_analysis5__dmv__002
""")

# COMMAND ----------


print(df_000.columns)
print(df_001.columns)
print(df_002.columns)

# COMMAND ----------

df_000.display()   

# COMMAND ----------

df_001.display()  

# COMMAND ----------

df_002.display()
