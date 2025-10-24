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

data_hora = "2025_09_22__14_00"

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testesteelauder"
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
mapping_display_interval_query = """
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
        "column": "display_interval",
    },
}

sql = sql_from_query_template(
    query_parameters=query_parameters,
    sources=sources, 
    query_string=mapping_display_interval_query,
)

df = spark.sql(sql)
df.display

# COMMAND ----------

df.display()

# COMMAND ----------

len("pfisch__test_display_interval_mapping__2025_09_221")

# COMMAND ----------

# Create display interval deliverable
display_interval_template = get_or_create_query_template(
    slug="pfisch__test_display_interval_mapping__2025_09_222",
    query_string=mapping_display_interval_query,
    template_description="Display interval mapping for market share analysis",
    version_description="Static mapping table for display intervals",
)

# COMMAND ----------

#print(f"pfisch__test_display_interval_mapping__{demo_name}__2025_09_22__14_00")
#len("pfisch__display_inter_ma__{demo_name}__2025_09_221")

fp_catalog = "yd_fp_corporate_staging"
ss_catalog = "yd_sensitive_corporate"

print(f"{fp_catalog}.{prod_schema}.{demo_name}_display_interval_monthly")

# yd_fp_corporate_staging.ydx_internal_analysts_gold.testesteelauder_v38_display_interval_monthly__dmv__000


# COMMAND ----------

fp_catalog = "yd_fp_corporate_staging"
ss_catalog = "yd_sensitive_corporate"

display_deliverable = get_or_create_deliverable(
    f"pfisch__display_inter_ma__{demo_name}__2025_09_222",
    query_template=display_interval_template["id"],
    input_tables=[],  # No input tables needed for static data
    output_table=f"{fp_catalog}.{prod_schema}.{demo_name}_display_interval_monthly2",
    description="Display interval mapping table for market share analysis",
    product_org="corporate",
    allow_major_version=True,
    allow_minor_version=True,
    staged_retention_days=100
)



# COMMAND ----------

# Materialize display interval table
materialize_deliverable(
    display_deliverable["id"],
    release_on_success=False,
    wait_for_completion=True,
)
