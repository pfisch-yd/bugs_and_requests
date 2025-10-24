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

def freeport_geo_analysis(sandbox_schema, prod_schema, demo_name):
    sources = [
        {
            'database_name': sandbox_schema,
            'table_name': demo_name+"_geographic_analysis",
            'catalog_name': "yd_sensitive_corporate",
        }
    ]

    query_string = """
        SELECT
            *
        FROM {{ sources[0].full_name }}
    """

    module_name = "pfisch__geob"+demo_name

    # Create display interval deliverable
    query_template = get_or_create_query_template(
        slug=module_name,
        query_string=query_string,
        template_description="Display interval mapping for market share analysis",
        version_description="Static mapping table for display intervals",
    )

    fp_catalog = "yd_fp_corporate_staging"
    ss_catalog = "yd_sensitive_corporate"

    deliverable = get_or_create_deliverable(
        module_name+"dd",
        query_template=query_template["id"],
        input_tables=[
                f"{ss_catalog}.{prod_schema}.{demo_name}_geographic_analysis"
            ],
        output_table=fp_catalog+"."+prod_schema + "."+demo_name+'_geographic_analysis4',
        description="test Geo 20250923b",
        product_org="corporate",
        allow_major_version=True,
        allow_minor_version=True,
        staged_retention_days=100
    )

    # Materialize display interval table
    materialize_deliverable(
        deliverable["id"],
        release_on_success=False,
        wait_for_completion=True,
    )

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testesteelauder"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

#freeport_geo_analysis(sandbox_schema, prod_schema, demo_name)

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testgraco"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

#freeport_geo_analysis(sandbox_schema, prod_schema, demo_name)

# COMMAND ----------

# MAGIC %md
# MAGIC yd_fp_corporate_staging.ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis2__dmv__000
# MAGIC
# MAGIC yd_fp_corporate_staging.ydx_internal_analysts_gold.testgraco_v38_geographic_analysis2__dmv__001
