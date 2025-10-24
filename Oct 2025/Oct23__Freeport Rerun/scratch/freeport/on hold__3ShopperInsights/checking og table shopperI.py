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

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from yipit_databricks_utils.future import create_table

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testesteelauder"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

# COMMAND ----------

df = spark.sql(f"""
    SELECT * FROM {prod_schema}.{demo_name}_filter_items
""")

df.display()

# COMMAND ----------

all_columns = df.columns
print(all_columns[0])

# COMMAND ----------

accepted_columns = []

# COMMAND ----------

for i in range(0, len(all_columns)):
    df = spark.sql(f"""
        with filtered as (
        SELECT
            {all_columns[i]} as important_column
        FROM {prod_schema}.{demo_name}_filter_items
        where
            {all_columns[i]} is not null
        )

        select count(important_column) as count_rows from filtered
    """)

    if df.collect()[0][0] > 0:
        print("accepted column {}".format(all_columns[i]))
        accepted_columns.append(all_columns[i])
    else:
        print("unnaccepted")

# COMMAND ----------

accepted_columns

# COMMAND ----------

sources = [
    {
        'database_name': prod_schema,
        'table_name': demo_name+"_filter_items",
        'catalog_name': "yd_sensitive_corporate",
    }
]

df = df_from_query_template(
    {"columns": accepted_columns},
    sources,
    query_string="""
    SELECT
    {% for column in parameters.columns %}
    {{ column }}{{ ',' if not loop.last else '' }}
    {% endfor %}
    FROM {{ sources[0].full_name }}
    """   
)

df.display()

# COMMAND ----------

sources = [
    {
        'database_name': prod_schema,
        'table_name': demo_name+"_filter_items",
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

module_name = "pfisch__shopi_20250923v"

# COMMAND ----------

# Create display interval deliverable
query_template = get_or_create_query_template(
    slug=module_name,
    query_string=query_string,
    template_description="pfisch test shopI",
    version_description="pfisch test shopI",
)

# COMMAND ----------

fp_catalog = "yd_fp_corporate_staging"
ss_catalog = "yd_sensitive_corporate"

deliverable = get_or_create_deliverable(
    module_name+"dd",
    query_template=query_template["id"],
    input_tables=[
            f"{ss_catalog}.{prod_schema}.{demo_name}_filter_items"
        ],
    output_table=fp_catalog+"."+prod_schema + "."+demo_name+'_filter_items2',
    query_parameters={"columns": accepted_columns},
    description="test ShopI 20250923",
    product_org="corporate",
    allow_major_version=True,
    allow_minor_version=True,
    staged_retention_days=100,
    slack_notifications= True,
    enable_variants = True,
    variant_field_names = ["product_attributes"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC I have been getting a data type issue
# MAGIC https://yipitdata-freeport.cloud.databricks.com/?o=3387028208381496#job/152628281355371/run/739484902812427

# COMMAND ----------

# Materialize display interval table
materialize_deliverable(
    deliverable["id"],
    release_on_success=False,
    wait_for_completion=True,
)
