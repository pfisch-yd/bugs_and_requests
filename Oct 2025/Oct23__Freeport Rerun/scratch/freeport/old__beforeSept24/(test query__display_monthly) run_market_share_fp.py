# Databricks notebook source
import sys

sys.path.append("/Workspace/Repos/ETL_Production/freeport_service/")

from freeport_databricks.client.v1 import (
    get_or_create_deliverable,
    materialize_deliverable,
    sql_from_query_template,
    df_from_query_template,
    get_or_create_query_template
)

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from yipit_databricks_utils.future import create_table

# COMMAND ----------

data_hora = "2025_09_19__13_13"

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testgraco"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

# COMMAND ----------

# Parameters embedded directly in query string
sources = [
    {
        'database_name': sandbox_schema,
        'table_name': demo_name+"_filter_items",
        'catalog_name': "yd_sensitive_corporate",
    }
]

# Create display interval mapping first
query_string = """
    select "Annually" as display_interval union all
    select "Quarterly" as display_interval union all
    select "Monthly" as display_interval union all
    select "Year-to-Date" as display_interval union all
    select "Trailing 12 Months" as display_interval union all
    select "Trailing 6 Months" as display_interval union all
    select "Trailing 3 Months" as display_interval
"""

query_parameters = {
    "metrics": {
        "column": "vin",
    },
}

sql = sql_from_query_template(
    query_parameters=query_parameters,
    sources=sources, 
    query_string=query_string,
)

spark.sql(sql)

# COMMAND ----------

df = spark.sql(sql)

df.display()

# COMMAND ----------

# MAGIC %run "/Workspace/Users/pfisch@yipitdata.com/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/analysis/market_share"

# COMMAND ----------

#run_export_market_share_module(
#        sandbox_schema,
#        prod_schema,
#        demo_name,
#        special_attribute_column_original
#    )

# COMMAND ----------

original_table = spark.sql(f"""
    SELECT * FROM {prod_schema}.{demo_name}_display_interval_monthly
""")

original_table.display()

# COMMAND ----------

# MAGIC %run "/Workspace/Users/pfisch@yipitdata.com/corporate_transformation_blueprints/corporate_transformation_blueprints/scratch_pfisch/freeport/testing/compare_df_function"

# COMMAND ----------

original = original_table
freeport_df = df
compare_df(original, freeport_df, show_sample_differences=True, verbose=True)
