# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC context
# MAGIC (jira)[https://yipitdata5.atlassian.net/browse/ENG-1394?focusedCommentId=409774&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-409774]
# MAGIC
# MAGIC helpful links
# MAGIC (clients)[https://docs.google.com/spreadsheets/d/1yMjwt2RAvaedrC_KSHwE9af1UsPioJv_siIdGlFWnnA/edit?gid=477748809#gid=477748809]

# COMMAND ----------

# MAGIC %run /setup_serverless

# COMMAND ----------

# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/Brands Dashboard Templates/__centralized_udfs_for_client_dashboard_exports"

# COMMAND ----------

from yipit_databricks_utils.future import create_table

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from yipit_databricks_utils.future import create_table
from pyspark.sql.functions import current_timestamp

from yipit_databricks_utils.helpers.gsheets import read_gsheet

from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import col

import ast

# COMMAND ----------

version = "_v38"

# COMMAND ----------

client = dbutils.widgets.get("client_name")
demo_name = client

# COMMAND ----------

df = read_gsheet(
    "1F8bvKrBdlY0GIm9xAU5v3IM7ERLM6kUdi_wYph9JJds",
    1374540499
)

df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "M/d/yyyy H:mm:ss"))
client_row = df.filter(df[1] == demo_name) \
            .orderBy(col("timestamp").desc()) \
            .limit(1)
client_row_data = client_row.collect()[0]

demo_name = client_row_data[1] + version
dash_display_title = client_row_data[2]
sandbox_schema = client_row_data[3]
prod_schema = client_row_data[4]
source_table = client_row_data[5]
sample_size_guardrail_threshold = client_row_data[7]

if pd.isna(client_row_data[6]) is True: #is null
    pro_source_table = "ydx_retail_silver.edison_pro_items"
else:
    pro_source_table = client_row_data[6]

#brands_display_list = normalize_brand_list(client_row_data[8])
parent_brand = client_row_data[9]
product_id = client_row_data[10]
client_email_distro = client_row_data[11]

raw_date = client_row_data[12]  # '2/24/2025'
start_date_of_data = parse_date(raw_date)

category_cols = [
    client_row_data[13],
    client_row_data[14],
    client_row_data[15]]

if pd.isna(client_row_data[16]) is True:
    special_attribute_column = []
    special_attribute_display = []
else:
    special_attribute_column = ast.literal_eval(client_row_data[16])
    special_attribute_display = ast.literal_eval(client_row_data[16])

if pd.isna(client_row_data[17]) is False:
    market_share = "Market Share" in [x.strip() for x in client_row_data[17].split(",")]
    shopper_insights = 'Shopper Insights' in [x.strip() for x in client_row_data[17].split(",")]
    pro_module = "Pro" in [x.strip() for x in client_row_data[17].split(",")]
    pricing_n_promo = 'Pricing & Promo' in [x.strip() for x in client_row_data[17].split(",")]
else:
    market_share = True
    shopper_insights = True
    pro_module = True
    pricing_n_promo = True

timestamp = client_row_data[0]

# COMMAND ----------

start_date_of_data = parse_date(raw_date)

# COMMAND ----------


print(sandbox_schema)
print(prod_schema)
print(demo_name)
print(special_attribute_column)
print(type(special_attribute_column))

# COMMAND ----------

table = spark.sql(f"""
    SELECT * FROM {prod_schema}.{demo_name}_market_share_for_column_null_nrf_calendar
""")

before = table.count()

# COMMAND ----------

special_attribute_column_original = special_attribute_column

run_export_market_share_module(
    sandbox_schema,
    prod_schema,
    demo_name,
    special_attribute_column,
    start_date_of_data
)

# COMMAND ----------

table = spark.sql(f"""
    SELECT * FROM {prod_schema}.{demo_name}_market_share_for_column_null_nrf_calendar
""")

after = table.count()

# COMMAND ----------

after/before
