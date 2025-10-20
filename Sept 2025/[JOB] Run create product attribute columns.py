# Databricks notebook source
# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/all"

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
from yipit_databricks_client.helpers.telemetry import track_usage

version = "_v38" 

def normalize_brand_list(value):
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        value = value.strip()
        # Case 1: Looks like a list string (e.g., "['Column A', 'Brand B']")
        if value.startswith("[") and value.endswith("]"):
            try:
                return ast.literal_eval(value)  # Safe conversion from string to list
            except Exception:
                return [value]  # Fallback: wrap as single item
        else:
            return [value]  # Not a list string, just a single brand
    return []  # Handle unexpected cases

def clean_last_asterisk(text):
    if text is None:
        return text
    lenn = len(text)
    if text[lenn-1:lenn] == "'":
        ans = text[:-1]
    else:
        ans = text
    return ans

def parse_date(raw_date: str) -> str:
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw_date, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"Date format not recognized: {raw_date}")



# COMMAND ----------

def run_everything_product (demo_name):
    og_demo_name = demo_name
    df = read_gsheet(
        "1F8bvKrBdlY0GIm9xAU5v3IM7ERLM6kUdi_wYph9JJds",
        1374540499
    )
    df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "M/d/yyyy H:mm:ss"))
    client_row = df.filter(df[1] == demo_name) \
               .orderBy(col("timestamp").desc()) \
               .limit(1)
    client_row_data = client_row.collect()[0]

    demo_name = client_row_data[1] + "_v38"
    dash_display_title = client_row_data[2]
    sandbox_schema = client_row_data[3]
    prod_schema = client_row_data[4]
    source_table = client_row_data[5]
    sample_size_guardrail_threshold = client_row_data[7]

    if pd.isna(client_row_data[6]) is True: #is null
        pro_source_table = "ydx_retail_silver.edison_pro_items"
    else:
        pro_source_table = client_row_data[6]

    brands_display_list = normalize_brand_list(client_row_data[8])
    parent_brand = clean_last_asterisk(client_row_data[9])
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

    market_share = True
    shopper_insights = True
    pro_module = True
    pricing_n_promo = True

    export_sku_time_series(
        sandbox_schema,
        prod_schema,
        demo_name,
        product_id
    )

# COMMAND ----------

LIST = ['KIK', 'Renin', 'athome',  'bosch', 'cabinetworks', 'crescent', 'daye', 'duraflame', 'echo_ope', 'ecolab', 'elanco', 'electrolux', 'electrolux_cdi', 'fiskars_crafts', 'generac', 'glossier', 'good_earth', 'hft', 'james_hardie', 'jbweld','keter', 'kidde', 'kiko', 'klein', 'kohler', 'lasko', 'lowes', 'msi', 'nest', 'onesize', 'ove', 'petsmart',  'rain_bird', 'renin', 'sbm', 'target_home', 'traeger', 'wayfair', 'wd40'] 

# COMMAND ----------

demo_name = "testblueprints"
# run_everything_snippet(demo_name)

# COMMAND ----------

client = dbutils.widgets.get("client_name")
demo_name = client

# COMMAND ----------

print(demo_name)

# COMMAND ----------

run_everything_product (demo_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from ydx_sherwin_williams_analysts_gold.sherwin_williams_v38_sku_time_series
