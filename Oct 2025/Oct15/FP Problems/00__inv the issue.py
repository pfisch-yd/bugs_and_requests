# Databricks notebook source
# MAGIC %md
# MAGIC # Solved
# MAGIC
# MAGIC A lot of clients had issues w _silver schema

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC I think the issue is about casing in the column names
# MAGIC
# MAGIC I am going to:
# MAGIC
# MAGIC 1 - check col names in source table
# MAGIC 2 - I will see the casing in the name
# MAGIC 3 - 

# COMMAND ----------

# tab = spark.sql(f"""" select * from ydx_petsmart_analysts_silver.petsmart_v38_filter_items """)

demo_name = "petsmart"

def lower_case_column(demo_name):
    tab = spark.table(f"ydx_{demo_name}_analysts_silver.{demo_name}_v38_filter_items")
    listt = tab.columns
    print(listt)
    not_lower = [item for item in listt if item != item.lower()]
    print(not_lower)
    return not_lower

# COMMAND ----------

issue_list = ["klein", "renin", "mayzon",  "on", "petsmart", "ideal_electric", "msi", "generac"]
# "tti_open", "champion"
for demo in issue_list:
    lower_case_column(demo)

# COMMAND ----------

# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/blueprints__other branches/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/freeport/module_template_function"

# COMMAND ----------

# freeport_module(sandbox_schema, prod_schema, demo_name, module_type, use_sampling=False, sample_fraction=0.01, column=None)

# "shopper_filter_items" and "filter_items"



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC I will have to clone the schema to internal
# MAGIC so I can do it again.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col
import ast
from datetime import datetime
from yipit_databricks_utils.helpers.gsheets import read_gsheet
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit
from pyspark.sql.functions import lower
import pandas as pd

from datetime import datetime, timedelta
import calendar

import pandas as pd
from pyspark.sql import SparkSession

from pyspark.sql.functions import to_timestamp

from yipit_databricks_utils.future import create_table

# COMMAND ----------

internal_sandbox = "ydx_internal_analysts_sandbox"
internal_gold = "ydx_internal_analysts_gold"

df = read_gsheet(
        "1F8bvKrBdlY0GIm9xAU5v3IM7ERLM6kUdi_wYph9JJds",
        1374540499
    )

client_row = df.filter(df[1] == demo_name) \
            .orderBy(col("timestamp").desc()) \
            .limit(1)
client_row_data = client_row.collect()[0]


external_sandbox= client_row_data[3]
external_gold = client_row_data[4]


internal_filter_items = spark.sql(f"""
    SELECT * FROM {external_sandbox}.{demo_name}_v38_filter_items
""")

create_table(internal_sandbox, demo_name+"_v38_filter_items", internal_filter_items, overwrite=True)

internal_shopper_filter_items = spark.sql(f"""
    SELECT * FROM {external_gold}.{demo_name}_v38_filter_items
""")

create_table(internal_gold, demo_name+"_v38_filter_items", internal_shopper_filter_items, overwrite=True)


# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "petsmart_v38"
module_type = "filter_items"

freeport_module(sandbox_schema, prod_schema, demo_name, module_type, use_sampling=False, sample_fraction=0.01, column=None)

# "shopper_filter_items" and "filter_items"

# COMMAND ----------

sandbox_schema = external_sandbox # "ydx_internal_analysts_sandbox"
prod_schema = external_gold # "ydx_internal_analysts_gold"
demo_name = "petsmart_v38"
module_type = "filter_items"


# ===============================================

use_sampling=False
sample_fraction=0.01
column=None

# ===============================================
# Some folders conventions
fp_catalog = "yd_fp_corporate_staging"
ss_catalog = "yd_sensitive_corporate"
view_suffix = "_fp"

# Get module configuration
config = get_module_config(module_type)

if not config:
    raise ValueError(f"Unknown module type: {module_type}")

table_suffix = config["table_suffix"]
table_nickname = config["table_nickname"]
pattern = config["pattern"]
query_type = config["query_type"]
exclude_columns = config["exclude_columns"]
description = config["description"]
schema_type = config["schema_type"]

if schema_type == "sandbox":
    schema = sandbox_schema
else:
    schema = prod_schema

if module_type in ("market_share_standard_calendar", "market_share_nrf_calendar", "market_share_for_column"):
    if module_type == "market_share_for_column":
        table_suffix = table_suffix + "_" + column
        table_nickname = table_nickname + column
    elif module_type == "market_share_standard_calendar":
        table_suffix = table_suffix + "_" + column +'_standard_calendar'
        table_nickname = table_nickname + column + '_std'
    elif module_type == "market_share_nrf_calendar":
        table_suffix = table_suffix + "_" + column +'_nrf_calendar'
        table_nickname = table_nickname + column +'_nrf'

# Create module name
module_name = f"corporate_{table_nickname}_{demo_name}"
# @@ SANDBOX NOTATION
#module_name = f"pfi6_{table_nickname}_{demo_name}"

# @@ SANDBOX NOTATION
#output_table_name = f"{fp_catalog}.{schema}.{demo_name}{table_suffix}"
input_tables_name = f"{ss_catalog}.{schema}.{demo_name}{table_suffix}"
output_table_name = f"{fp_catalog}.{schema}.{demo_name}{table_suffix}"

# COMMAND ----------

# Define sources
sources = [{
    'database_name': schema,
    'table_name': demo_name + table_suffix,
    'catalog_name': "yd_sensitive_corporate",
}]

# Apply pattern-specific logic
if pattern == "SIMPLE":
    # SIMPLE pattern: Use SELECT * or explicit column list
    if query_type == "SELECT_ALL":
        query_string = """
            SELECT *
            FROM {{ sources[0].full_name }}
        """
        query_parameters = None
    else:
        raise NotImplementedError("Explicit column selection not implemented for SIMPLE pattern")
else:
    raise ValueError(f"Unknown pattern: {pattern}")    

# Apply pattern-specific logic
if module_type == "sku_detail":
    query_string = """
        SELECT * except (ASP),
        ASP as asp
        FROM {{ sources[0].full_name }}
    """
    query_parameters = None  

# COMMAND ----------

# Create query template
query_template = get_or_create_query_template(
    slug=module_name,
    query_string=query_string,
    template_description=f"Corporate {description}",
    version_description=f"Production {description} table",
)

# COMMAND ----------

print(output_table_name)

# COMMAND ----------

# Create deliverable
deliverable = get_or_create_deliverable(
    module_name+"_deliverable",
    query_template=query_template["id"],
    input_tables=[input_tables_name],
    output_table=output_table_name,
    query_parameters=query_parameters,
    description=f"{description} for {demo_name}",
    product_org="corporate",
    allow_major_version=True,
    allow_minor_version=True,
    staged_retention_days=100
)

# COMMAND ----------

# Materialize deliverable
materialization = materialize_deliverable(
    deliverable["id"],
    release_on_success=False,
    wait_for_completion=True,
)

materialization_id = materialization['id']
release_response = release_materialization(materialization_id)

# Assess latest version
print(f"Waiting for release to complete...")
while True:
    response = get(f"api/v1/data_model/fp_materialization/{materialization_id}")
    print(response)

    last_fp_release = response.json()["last_fp_release"]
    if last_fp_release:
        if last_fp_release['airflow_status'] == "success":
            print(f"✅ FP Release completed successfully!")
            break
        elif last_fp_release['airflow_status'] == "failed":
            print("❌ FP Release failed!")
            break

        print("Release still running. Waiting 45 seconds...")
        time.sleep(45)
    else:
        print("No releases found. Waiting 45 seconds...")
        time.sleep(45)

latest_table_name = last_fp_release['view_details']['table_name']
latest_catalog = last_fp_release['view_details']['catalog_name']
latest_database = last_fp_release['view_details']['database_name']

latest_table = f"{latest_catalog}.{latest_database}.{latest_table_name}"


view_name =  f"{fp_catalog}.{schema}.{demo_name}{table_suffix}{view_suffix}"
view_comment = f"{description} for {demo_name}"

query_view = f"""
    CREATE OR REPLACE VIEW {view_name}
    COMMENT '{view_comment}'
    AS
    SELECT
        *
    FROM {latest_table}
"""
spark.sql(query_view)

# return deliverable
