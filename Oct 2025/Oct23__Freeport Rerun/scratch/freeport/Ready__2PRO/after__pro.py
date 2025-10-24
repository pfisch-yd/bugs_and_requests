# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC what I am going to do
# MAGIC
# MAGIC take a look on how pro looks
# MAGIC
# MAGIC see the columns
# MAGIC
# MAGIC check the data type
# MAGIC
# MAGIC and then try to create

# COMMAND ----------

import sys

sys.path.append("/Workspace/Repos/ETL_Production/freeport_service/")

from freeport_databricks.client.v1 import (
    get_or_create_deliverable,
    materialize_deliverable,
    sql_from_query_template,
    df_from_query_template,
    get_or_create_query_template
)

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from yipit_databricks_utils.future import create_table

# COMMAND ----------

def freeport_pro_insights(sandbox_schema, prod_schema, demo_name):
    df = spark.sql(f"""
        SELECT * FROM {prod_schema}.{demo_name}_pro_insights
    """)

    all_columns = df.columns
    accepted_columns = []

    for i in range(0, len(all_columns)):
        df = spark.sql(f"""
            with filtered as (
            SELECT
                {all_columns[i]} as important_column
            FROM {prod_schema}.{demo_name}_pro_insights
            where
                {all_columns[i]} is not null
            )

            select count(important_column) as count_rows from filtered
        """)

        if df.collect()[0][0] > 0:
            #print("accepted column {}".format(all_columns[i]))
            accepted_columns.append(all_columns[i])

    sources = [
        {
            'database_name': prod_schema,
            'table_name': demo_name+"_pro_insights",
            'catalog_name': "yd_sensitive_corporate",
        }
    ]

    query_string="""
        SELECT
        {% for column in parameters.columns %}
        {{ column }}{{ ',' if not loop.last else '' }}
        {% endfor %}
        FROM {{ sources[0].full_name }}
        """  

    sources = [
        {
            'database_name': prod_schema,
            'table_name': demo_name+"_pro_insights",
            'catalog_name': "yd_sensitive_corporate",
        }
    ]

    query_string = """
        SELECT
        {% for column in parameters.columns %}
        {{ column }}{{ ',' if not loop.last else '' }}
        {% endfor %}
        FROM {{ sources[0].full_name }}
    """

    module_name = "pfisch__pro_20250923b"

    # Create display interval deliverable
    query_template = get_or_create_query_template(
        slug=module_name,
        query_string=query_string,
        template_description="pfisch test pro",
        version_description="pfisch test pro",
    )

    fp_catalog = "yd_fp_corporate_staging"
    ss_catalog = "yd_sensitive_corporate"

    deliverable = get_or_create_deliverable(
        module_name+"dd",
        query_template=query_template["id"],
        input_tables=[
                f"{ss_catalog}.{prod_schema}.{demo_name}_pro_insights"
            ],
        output_table=fp_catalog+"."+prod_schema + "."+demo_name+'_pro_insights2',
        query_parameters={"columns": accepted_columns},
        description="test Pro 20250923b",
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
demo_name = "testgraco"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

freeport_pro_insights(sandbox_schema, prod_schema, demo_name)

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testesteelauder"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

freeport_pro_insights(sandbox_schema, prod_schema, demo_name)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC yd_fp_corporate_staging.ydx_internal_analysts_gold.testgraco_v38_pro_insights1__dmv__000
