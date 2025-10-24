# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC [pro insights module](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/346905085678972?o=3092962415911490#command/8328037515090554)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/Users/pfisch@yipitdata.com/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/analysis/pro_insights"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/pfisch@yipitdata.com/corporate_transformation_blueprints/corporate_transformation_blueprints/scratch_pfisch/freeport/run_pro_insights_fp"

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testgraco"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

# COMMAND ----------

from datetime import datetime

timeseries = []
# Pega a hora atual formatada
data_hora = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
titulo = f"==>{data_hora}"
print(titulo)

# COMMAND ----------

timeseries.append("original")
data_hora = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
titulo = f"==>{data_hora}"
timeseries.append(titulo)

export_pro_insights(
    sandbox_schema,
    prod_schema,
    demo_name,
    pro_source_table,
    start_date_of_data
)

timeseries.append("original __ finalizado")
data_hora = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
titulo = f"==>{data_hora}"
timeseries.append(titulo)

# COMMAND ----------

print(titulo)

# COMMAND ----------

timeseries.append("fp")
data_hora = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
titulo = f"==>{data_hora}"
timeseries.append(titulo)

export_pro_insights__fp(
    sandbox_schema,
    prod_schema,
    demo_name,
    pro_source_table,
    start_date_of_data
)

timeseries.append("fp __ finalizado")
data_hora = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
titulo = f"==>{data_hora}"
timeseries.append(titulo)

# COMMAND ----------

print(titulo)

# COMMAND ----------

timeseries
