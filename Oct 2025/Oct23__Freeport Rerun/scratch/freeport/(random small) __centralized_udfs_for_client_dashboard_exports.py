# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Retail Analytics Platform - Client Dashboard Export Functions
# MAGIC
# MAGIC ## 🚀 Quick Start
# MAGIC
# MAGIC This notebook provides centralized functions for generating client dashboard exports.
# MAGIC
# MAGIC **Main Functions:**
# MAGIC - `run_everything(demo_name)` - Complete dashboard generation for production
# MAGIC - `run_everything_internally(demo_name)` - Internal development version
# MAGIC - `check_your_parameters(demo_name)` - View client configuration
# MAGIC - `search_demo_name(search_word)` - Find client names
# MAGIC - `ask_for_help()` - Show available functions
# MAGIC - `display_version_info()` - Show current version information
# MAGIC
# MAGIC **Supported Modules:** Market Share, Shopper Insights, Pro, Geographic Analysis, Product Analysis, Retailer Leakage, Tariffs, Pricing & Promo
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %run "./retail_analytics_platform/all"

# COMMAND ----------

import ast
import calendar
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, lit, lower, to_timestamp
from pyspark.sql.window import Window
from yipit_databricks_utils.helpers.gsheets import read_gsheet

# COMMAND ----------

# MAGIC %run "./version_utils"

# COMMAND ----------

# Display current version information
print("🏷️  Corporate Transformation Blueprints - Loaded Successfully!")
print(f"📦 Version: {version_info['semantic']} (Suffix: {version_info['version_suffix']})")
print(f"📅 Release Date: {version_info['release_date']}")
print(f"🏷️  Release: {version_info['release_name']}")
print("=" * 60)

# COMMAND ----------

# Note: version variable is imported via %run "./retail_analytics_platform/all" above

# COMMAND ----------

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

# COMMAND ----------

def parse_date(raw_date: str) -> str:
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw_date, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"Date format not recognized: {raw_date}")

# COMMAND ----------

def check_your_parameters(demo_name):
    df = get_default_gsheet_client().read_sheet(
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

    brands_display_list = normalize_brand_list(client_row_data[8])
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
        special_attribute_column = client_row_data[16]
        special_attribute_display = client_row_data[16]

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

    text = """
    If there is something wrong,
    please right a new form here

    https://docs.google.com/forms/d/e/1FAIpQLSetveoNR1i_9nWWebCOYbSdWYxqZkacJsCMhgt5-r0XuiTOWw/viewform?usp=dialog 

    demo_name: {0}
    dash_display_title: {1}
    sandbox_schema: {2}
    prod_schema: {3}
    source_table: {4}
    pro_source_table: {5}
    sample_size_guardrail_threshold: {6}
    brands_display_list: {7}
    parent_brand: {8}
    product_id: {9}
    client_email_distro: {10}
    start_date_of_data: {11}
    category_cols: {12},
    special_attribute_column: {13}

    BOUGHT PACKAGES:
    Market Share = {14}
    Shopper Insights = {15}
    Pro = {16}
    Pricing n Promo = {17}

    TIMESTAMP = {18}
    """
    print(text.format(
        demo_name, dash_display_title, sandbox_schema,
        prod_schema, source_table, pro_source_table, sample_size_guardrail_threshold,
        brands_display_list, parent_brand, product_id, client_email_distro,
        start_date_of_data, category_cols, special_attribute_column,
        market_share, shopper_insights, pro_module, pricing_n_promo,
        timestamp
    ))

# COMMAND ----------

def search_demo_name(search_word):
    df = get_default_gsheet_client().read_sheet(
    "1F8bvKrBdlY0GIm9xAU5v3IM7ERLM6kUdi_wYph9JJds",
    1374540499
    )
    
    # Define a dummy window to assign reverse order (biggest first)
    window_spec = Window.partitionBy("demo_namelowercasenospacenoversionegtriplecrowncentral").orderBy(lit(1).desc())

    # Add a rank within each group and filter for the first (i.e., "best")
    clients = df.select("demo_namelowercasenospacenoversionegtriplecrowncentral") \
                .withColumn("rank", row_number().over(window_spec)) \
                .filter("rank = 1") \
                .select("demo_namelowercasenospacenoversionegtriplecrowncentral")

    filtered_clients = clients.filter(
        lower(clients["demo_namelowercasenospacenoversionegtriplecrowncentral"]).like("%"+search_word+"%")
    )

    if search_word is None:
        final_table = clients
    else:
        final_table = filtered_clients

    final_table = final_table.withColumnRenamed("demo_namelowercasenospacenoversionegtriplecrowncentral", "demo_name2")

    final_table.display()
    return final_table

# COMMAND ----------

# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/blueprints__other branches/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/freeport/freeport_all_tables"

# COMMAND ----------

def  freeport2(demo_name):
    df = get_default_gsheet_client().read_sheet(
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

    brands_display_list = normalize_brand_list(client_row_data[8])
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

    # READY FOR OFFICIAL ROLL OUT!!!
    sandbox_schema = "ydx_internal_analysts_sandbox"
    prod_schema = "ydx_internal_analysts_gold"

    sol_owner = client_row_data[18]
    print(sol_owner)

    # READY FOR OFFICIAL ROLL OUT!!!
    sol_owner = "pfisch+solwoner@yipitdata.com"

    freeport_all_tables(demo_name, sandbox_schema, prod_schema, sol_owner, special_attribute_column, market_share, shopper_insights, pro_module, pricing_n_promo)

# COMMAND ----------

freeport2("testblueprints")

