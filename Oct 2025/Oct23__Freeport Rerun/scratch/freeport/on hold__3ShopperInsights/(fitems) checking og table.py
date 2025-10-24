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

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testesteelauder"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

# COMMAND ----------

table_suffix = "_filter_items"
table_nickname = "fite"

# COMMAND ----------

df = spark.sql(f"""
    SELECT * except (product_attributes) FROM {prod_schema}.{demo_name}{table_suffix}
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
        FROM {prod_schema}.{demo_name}{table_suffix}
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
        'table_name': demo_name+table_suffix,
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
        'table_name': demo_name+table_suffix,
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

module_name = "pfisch__"+table_nickname+"_20250923a"

# COMMAND ----------

# Create display interval deliverable
query_template = get_or_create_query_template(
    slug=module_name,
    query_string=query_string,
    template_description="pfisch test "+table_nickname,
    version_description="pfisch test "+table_nickname,
)

# COMMAND ----------

fp_catalog = "yd_fp_corporate_staging"
ss_catalog = "yd_sensitive_corporate"

deliverable = get_or_create_deliverable(
    module_name+"dd",
    query_template=query_template["id"],
    input_tables=[
            f"{ss_catalog}.{prod_schema}.{demo_name}{table_suffix}"
        ],
    output_table=fp_catalog+"."+prod_schema + "."+demo_name+table_suffix+'1b',
    query_parameters={"columns": accepted_columns},
    description="test "+table_nickname+" 20250923",
    product_org="corporate",
    allow_major_version=True,
    allow_minor_version=True,
    staged_retention_days=100
)

# COMMAND ----------

# Materialize display interval table
materialize_deliverable(
    deliverable["id"],
    release_on_success=False,
    wait_for_completion=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC yd_fp_corporate_staging.ydx_internal_analysts_gold.testgraco_v38_pro_insights1__dmv__000
