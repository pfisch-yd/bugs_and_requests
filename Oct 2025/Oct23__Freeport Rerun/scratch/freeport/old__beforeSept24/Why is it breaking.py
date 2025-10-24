# Databricks notebook source
import json

import jmespath
from pyspark.sql import functions as F

from yipit_databricks_utils.helpers.delta import _table_exists

# from freeport_databricks.helpers.logs import get_logger
# from freeport_databricks.helpers.constants import MaterializationType
#from freeport_databricks.helpers.meta import exit_with_notebook_output
#from freeport_databricks.helpers.cloud_storage import load_json_object_from_s3
#from freeport_databricks.helpers.transformations import parse_individual_queries

# COMMAND ----------

# dbutils.widgets.get("MODEL_VERSION_CONFIGURATION")

# COMMAND ----------

# model_version_input_config

model_version_input_config = dbutils.widgets.get("MODEL_VERSION_CONFIGURATION")

# COMMAND ----------

# model_version_input_data

model_version_input_data = json.loads(model_version_input_config)

# COMMAND ----------

# model_version_data["query"]

model_version_data = load_json_object_from_s3(
    model_version_input_data["model_version_configuration_path"]
)

# COMMAND ----------

# query_parts

query = model_version_data["query"]

query_parts = parse_individual_queries(query)

# COMMAND ----------

query_df = spark.sql(f"{query_parts[0]}")
query_columns = query_df.columns
