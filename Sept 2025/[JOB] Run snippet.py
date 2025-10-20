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

def run_everything_snippet (demo_name):
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

    demo_name = client_row_data[1] + "_snippetv38"
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

    # READY FOR OFFICIAL ROLL OUT!!!
    sandbox_schema = "ydx_internal_analysts_sandbox"
    prod_schema = "ydx_internal_analysts_gold"

    print("run_export_schema_check")

    sku_detail = spark.sql(f"""
        SELECT * FROM {source_table}
        limit 2500
    """)

    create_table(sandbox_schema, demo_name + "_sourcetable", sku_detail, overwrite=True)

    print(sandbox_schema + "." + demo_name + "_sourcetable")

    source_table = sandbox_schema + "." + demo_name + "_sourcetable"

    module_check = []

    try:
        print("prep_filter_items")
        prep_filter_items(
                sandbox_schema,
                demo_name,
                source_table,
                category_cols,
                start_date_of_data,
                special_attribute_column,
                special_attribute_display,
                product_id
        )
        module_check.append("prep_filter_items")
    except:
        print("fail : prep_filter_items")
        module_check.append("fail : prep_filter_items")

    try:
        print("run_export_client_specs")
        run_export_client_specs(
            sandbox_schema,
            prod_schema,
            demo_name,
            brands_display_list, 
            parent_brand, 
            dash_display_title,
            client_email_distro,
            sample_size_guardrail_threshold
        )
        module_check.append("run_export_client_specs")
    except:
        print("fail : run_export_client_specs")
        module_check.append("fail : run_export_client_specs")

    try:
        print("run_export_shopper_insights_module")
        run_export_shopper_insights_module(
            sandbox_schema,
            prod_schema,
            demo_name
        )
        module_check.append("run_export_shopper_insights_module")
    except:
        print("fail : run_export_shopper_insights_module")
        module_check.append("fail : run_export_shopper_insights_module")
        
    if market_share:
        try:
            print("run_export_market_share_module")
            run_export_market_share_module(
                sandbox_schema,
                prod_schema,
                demo_name,
                special_attribute_column
            )
            module_check.append("run_export_market_share_module")
        except:
            print("fail : run_export_market_share_module")
            module_check.append("fail : run_export_market_share_module")
        
        try:
            print("run_export_geo_analysis_module")
            run_export_geo_analysis_module(
                sandbox_schema,
                prod_schema,
                demo_name
            )
            module_check.append("run_export_geo_analysis_module")
        except:
            print("fail : run_export_geo_analysis_module")
            module_check.append("fail : run_export_geo_analysis_module")
        
        try:
            print("run_export_product_analysis_module")
            run_export_product_analysis_module(
                sandbox_schema,
                prod_schema,
                demo_name,
                product_id
            )
            module_check.append("run_export_product_analysis_module")
        except:
            print("fail : run_export_product_analysis_module")
            module_check.append("fail : run_export_product_analysis_module")
        
    try:    
        print("run tariffs")
        run_tariffs_module(
            sandbox_schema,
            prod_schema,
            demo_name,
            product_id
            )
        module_check.append("run_tariffs_module")
    except:
        print("fail : run_tariffs_module")
        module_check.append("fail : run_tariffs_module")
    
    if shopper_insights:
        try:
            print("run_export_retailer_leakage_module")
            run_export_retailer_leakage_module(
                sandbox_schema,
                prod_schema,
                demo_name,
                start_date_of_data
            )
            module_check.append("run_export_retailer_leakage_module")
        except:
            print("fail : run_export_retailer_leakage_module")
            module_check.append("fail : run_export_retailer_leakage_module")

    if  pro_module:
        try:
            print("run_export_pro_insights")
            run_export_pro_insights(
                sandbox_schema,
                prod_schema,
                demo_name,
                pro_source_table,
                start_date_of_data
            )
            module_check.append("run_export_pro_insights")
        except:
            print("fail : run_export_pro_insights")
            module_check.append("fail : run_export_pro_insights")
    
    try:
        print("run metric save")
        # https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/4485801655516586?o=3092962415911490#command/6710077027376437
        # Function needs to run after client variables are defined
        run_metric_save(demo_name, sandbox_schema, brands_display_list)
        module_check.append("metric_save")  
    except:
        print("fail : run_metric_save")  
        module_check.append("fail : run_metric_save")

# COMMAND ----------

LIST = ['amazon', 'amorepacific', 'athome', 'cargill', 'cecred', 'ecolab', 'estee_lauder', 'glossier', 'hart', 'hft', 'homedepot', 'homedepot_v38', 'libman', 'lowes', 'mayzon', 'michaels', 'newell', 'odele', 'onesize', 'osea', 'pbb_v1', 'petsmart', 'scotts', 'shiseido', 'summerfridays', 'supergoop', 'target_home', 'tractorsupply', 'wayfair', 'worthington']

# COMMAND ----------

demo_name = "testblueprints"
# run_everything_snippet(demo_name)

# COMMAND ----------

client = dbutils.widgets.get("client_name")
demo_name = client

# COMMAND ----------

print(demo_name)

# COMMAND ----------

run_everything_snippet(demo_name)
