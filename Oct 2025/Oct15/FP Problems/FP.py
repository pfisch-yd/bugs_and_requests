# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC O que eu quero.
# MAGIC Acompanhar o FP das tabelas
# MAGIC
# MAGIC 1. Quem está disponível => todo mundo que na outra tabelas
# MAGIC
# MAGIC 2. Quem eu considero Freeportado ?
# MAGIC
# MAGIC Acho que todos que tiverem a tabela de ✅ Successful: - geographic_analysis
# MAGIC
# MAGIC

# COMMAND ----------

import pandas as pd

# COMMAND ----------



sandbox_schema = "ydx_kiko_analysts_silver"
prod_schema = "ydx_kiko_analysts_gold"
demo_name = "kiko" + "_v38"

#geo = spark.sql(f"select * from yd_fp_corporate_staging.{prod_schema}.{demo_name}_geographic_analysis_fp")
#spark.catalog.tableExists()

def is_freeported(sandbox_schema, prod_schema, demo_name):
    table_name = f"yd_fp_corporate_staging.{prod_schema}.{demo_name}_geographic_analysis_fp"
    return spark.catalog.tableExists(table_name)


def last_freeported_at(sandbox_schema, prod_schema, demo_name):
    table_name = f"yd_fp_corporate_staging.{prod_schema}.{demo_name}_geographic_analysis_fp"
    if spark.catalog.tableExists(table_name):
        t = spark.sql(f"""select max(month) as last_month from yd_fp_corporate_staging.{prod_schema}.{demo_name}_geographic_analysis_fp""")
        max_date = t.collect()[0]['last_month']
    else:
        max_date = None
    return max_date



# COMMAND ----------

table_name = f"yd_fp_corporate_staging.{prod_schema}.{demo_name}_geographic_analysis_fp"
if spark.catalog.tableExists(table_name):
    t = spark.sql(f"""select max(month) as last_month from yd_fp_corporate_staging.{prod_schema}.{demo_name}_geographic_analysis_fp""")
    max_date = t.collect()[0]['last_month']
else:
    max_date = None

print(max_date)

# COMMAND ----------

is_freeported(sandbox_schema, prod_schema, demo_name)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Validate something for all clients

# COMMAND ----------

client_info = spark.sql(f"""
select * from data_solutions_sandbox.corporate_clients_info
where is_active
""")

client_info_pd = client_info.toPandas()
demo_name_list = client_info_pd["demo_name"]
prod_schema_list = client_info_pd["prod_schema"]
sandbox_schema_list = client_info_pd["sandbox_schema"]

is_freeported_list = []
last_freeported_at_list = []

# COMMAND ----------

for i in range(0,len(client_info_pd)):
    sandbox_schema = sandbox_schema_list[i]
    prod_schema = prod_schema_list[i]
    demo_name = demo_name_list[i]+ "_v38"

    loop = is_freeported(sandbox_schema, prod_schema, demo_name)
    is_freeported_list.append(loop)

    loop_at = last_freeported_at(sandbox_schema, prod_schema, demo_name)
    last_freeported_at_list.append(loop_at)

# COMMAND ----------

client_info_pd["is_freeported"] = is_freeported_list
client_info_pd["last_freeported_at"] = last_freeported_at_list
client_info_pd

client_info_spark = spark.createDataFrame(client_info_pd)

# COMMAND ----------

client_info_spark.display()

from yipit_databricks_utils.future import create_table

create_table("data_solutions_sandbox", 'corporate_clients_info__freeport', client_info_spark, overwrite=True)
