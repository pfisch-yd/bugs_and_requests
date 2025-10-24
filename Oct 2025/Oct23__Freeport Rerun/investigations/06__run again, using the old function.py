# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Let's re run
# MAGIC I want 1 client, internal, 1 table
# MAGIC
# MAGIC I will stick to the classics
# MAGIC
# MAGIC internal triplecrown
# MAGIC module geo
# MAGIC

# COMMAND ----------

# MAGIC %run "./01__create email function and body"

# COMMAND ----------

# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/blueprints__other branches/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/freeport/module_template_function"

# COMMAND ----------

demo_name = "triplecrowncentral"
sol_owner = "pfisch+freeport@yipitdata.com"
module = "geo"
total_tasks = 10
successful_tasks = 10

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "triplecrowncentral" + "_v38"
module_type = "geographic_analysis"
table_suffix = "_geographic_analysis"
use_sampling =False
sample_fraction =0.01
column = None

# COMMAND ----------

df = spark.table(f"{prod_schema}.{demo_name}{table_suffix}")

df_play = spark.sql(f"""
select max(month) as month
    from ydx_internal_analysts_gold.{demo_name}{table_suffix}
""")

df_play.display()

# COMMAND ----------

from yipit_databricks_utils.future import create_table

#create_table("ydx_internal_analysts_gold", f"{demo_name}{table_suffix}", df_play, overwrite=True)

# COMMAND ----------

freeport_module(sandbox_schema, prod_schema, demo_name, module_type, use_sampling=False, sample_fraction=0.01, column=None)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC FROM yd_sensitive_corporate.ydx_internal_analysts_gold.triplecrowncentral_v38_geographic_analysis
# MAGIC order by month desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from yd_fp_corporate_staging.ydx_internal_analysts_gold.triplecrowncentral_v38_geographic_analysis2a__dmv__001
# MAGIC order by month desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select max(month) from yd_fp_corporate_prod.ydx_internal_analysts_gold.triplecrowncentral_v38_geographic_analysis2a__dmv__001
