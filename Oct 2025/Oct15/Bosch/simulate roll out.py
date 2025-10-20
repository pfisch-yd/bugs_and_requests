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

# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/all"

# COMMAND ----------

import ast
import calendar
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, lit, lower, to_timestamp
from pyspark.sql.window import Window
from yipit_databricks_utils.helpers.gsheets import read_gsheet

from yipit_databricks_client.helpers.telemetry import track_usage

# COMMAND ----------

# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/corporate_transformation_blueprints/corporate_transformation_blueprints/version_utils"

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

@track_usage
def run_everything (demo_name, mode="NORMAL"):
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

    if mode == "NORMAL":
        print("==> READY FOR OFFICIAL ROLL OUT!!!")
    elif mode == "INTERNAL":
        print("===> TEST MODE")
        sandbox_schema = "ydx_internal_analysts_sandbox"
        prod_schema = "ydx_internal_analysts_gold"
    else:
        print("==>")
    
    print("run_export_schema_check")
    
    run_export_schema_check(
        source_table,
        sandbox_schema,
        demo_name,
        category_cols,
        product_id)
     
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
#
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

    print("run_export_shopper_insights_module")
    run_export_shopper_insights_module(
        sandbox_schema,
        prod_schema,
        demo_name
    )
        
    if market_share:
        print("run_export_market_share_module")
        run_export_market_share_module(
            sandbox_schema,
            prod_schema,
            demo_name,
            special_attribute_column,
            start_date_of_data
        )
        print("run_export_geo_analysis_module")
        run_export_geo_analysis_module(
            sandbox_schema,
            prod_schema,
            demo_name
        )
        print("run_export_product_analysis_module")
        run_export_product_analysis_module(
            sandbox_schema,
            prod_schema,
            demo_name,
            product_id,
            special_attribute_column
        )
        
    print("run tariffs")
    run_tariffs_module(
        sandbox_schema,
        prod_schema,
        demo_name,
        product_id
        )
    
    if shopper_insights:
        print("run_export_retailer_leakage_module")
        run_export_retailer_leakage_module(
            sandbox_schema,
            prod_schema,
            demo_name,
            start_date_of_data
        )

    if  pro_module:
        print("run_export_pro_insights")
        run_export_pro_insights(
            sandbox_schema,
            prod_schema,
            demo_name,
            pro_source_table,
            start_date_of_data
        )
    
    print("run metric save")
    # https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/4485801655516586?o=3092962415911490#command/6710077027376437
    # Function needs to run after client variables are defined
    run_metric_save(demo_name, sandbox_schema, brands_display_list)

# COMMAND ----------

def ask_for_help():
    print("Hello! a list of functions that you can use")

    print("=> check_your_parameters(demo_name) ")
    print("Here you can retrieve the parameters that is used for this client.")

    print("=> run_everything(demo_name) ")
    print("Here we create ALL possible tables. This should be used when we do our routinely data refresh.")

    print("=> run_internally_everything(demo_name) ")
    print("This is CDP dev's team internal environment, where we replicate the conditions of client's tables without impacting final users.")

    print("=> search_demo_name(search_word) ")
    print("If you forgot the name of a client, this function can help you search for its key.")

    print("=> to run module by module")
    print(" go to")
    print(" https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/3573370221974421?o=3092962415911490#command/5534654816609620 ")

# COMMAND ----------

demo_name = "bosch"
mode="INTERNAL"

# COMMAND ----------


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

if mode == "NORMAL":
    print("==> READY FOR OFFICIAL ROLL OUT!!!")
elif mode == "INTERNAL":
    print("===> TEST MODE")
    sandbox_schema = "ydx_internal_analysts_sandbox"
    prod_schema = "ydx_internal_analysts_gold"
else:
    print("==>")

print("run_export_schema_check")

# COMMAND ----------

run_export_schema_check(
    source_table,
    sandbox_schema,
    demo_name,
    category_cols,
    product_id)

# COMMAND ----------

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

# COMMAND ----------

print("run_export_product_analysis_module")
run_export_product_analysis_module(
    sandbox_schema,
    prod_schema,
    demo_name,
    product_id,
    special_attribute_column
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Okay, the issue is in Product Attributes, not anywhere in the code**

# COMMAND ----------

print(
    sandbox_schema,
    prod_schema,
    demo_name,
    product_id
)

# COMMAND ----------

module_name = '_sku_analysis'
# Check which optional columns exist
optional_cols = get_optional_columns_availability(sandbox_schema, demo_name)
has_product_attrs = optional_cols['product_attributes']

# Conditionally include optional columns in queries
product_attrs_select = "first(to_json(product_attributes)) as product_attributes," if optional_cols['product_attributes'] else "CAST(null as STRING) as product_attributes,"
product_hash_select = "min(product_hash) as product_hash," if optional_cols['product_hash'] else "CAST(null as STRING) as product_hash,"
parent_sku_select = "min(parent_sku) as parent_sku," if optional_cols['parent_sku'] else "CAST(null as STRING) as parent_sku,"
parent_sku_name_select = "min(parent_sku_name) as parent_sku_name," if optional_cols['parent_sku_name'] else "CAST(null as STRING) as parent_sku_name,"
parent_brand_select = "min(parent_brand) as parent_brand," if optional_cols['parent_brand'] else "CAST(null as STRING) as parent_brand,"
product_url_select = "min(product_url) as product_url," if optional_cols['product_url'] else "CAST(null as STRING) as product_url,"

sku_analysis = spark.sql(f"""
    -- SET use_cached_result = false;
                        
    with 
    max_month as (
        SELECT month(max(month)) as max_month, MAX(month) as max_date
        FROM {sandbox_schema}.{demo_name}_filter_items
    ),

    annual as (
        SELECT *
        FROM (
            SELECT 'Last Year' as display_interval, *, 
            lag(observations) OVER (partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id order by period_starting) as last_obs,
            lag(gmv) OVER (partition by 
            channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id
            order by period_starting) as last_gmv,
            CASE WHEN row_number() OVER (partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id order by period_starting) = 1 then 1 else 0 end as new_sku_flag
            FROM (
                SELECT channel, date_trunc('year', max_date) - interval 1 month as max_date, major_cat, sub_cat, minor_cat, web_description, merchant_clean as merchant, brand, sub_brand, year as period_starting, 
                {product_id} as product_id,
                {product_attrs_select}
                {product_hash_select}
                {parent_sku_select}
                {parent_sku_name_select}
                {parent_brand_select}
                {product_url_select}
                SUM(item_price * item_quantity) as total_spend, SUM(item_quantity) as total_units, SUM(gmv) as gmv, count(*) as observations
                FROM {sandbox_schema}.{demo_name}_filter_items
                CROSS JOIN max_month
                WHERE web_description is not null
                AND date_trunc('year', month) < date_trunc('year', max_date)
                GROUP BY ALL
            )
        )
        WHERE period_starting = date_trunc('year', max_date)
    ),

    union_all as (
        SELECT *
        FROM annual
    )

    select
        "{product_id}" as product_id_type,
        {f'*except(product_attributes), parse_json(product_attributes) as product_attributes' if has_product_attrs else '*'}
    from
    union_all
""")

# COMMAND ----------

def get_optional_columns_availability(sandbox_schema, demo_name):
    """
    Check which optional columns exist in the filter_items table
    """
    try:
        columns = spark.table(f"{sandbox_schema}.{demo_name}_filter_items").columns
        optional_columns = {
            'product_attributes': 'product_attributes' in columns,
            'product_hash': 'product_hash' in columns,
            'parent_sku': 'parent_sku' in columns,
            'parent_sku_name': 'parent_sku_name' in columns,
            'variation_sku': 'variation_sku' in columns,
            'variation_sku_name': 'variation_sku_name' in columns,
            'parent_brand': 'parent_brand' in columns,
            'product_url': 'product_url' in columns
        }
        return optional_columns
    except Exception:
        return {col: False for col in ['product_attributes', 'product_hash', 'parent_sku', 'parent_sku_name', 'variation_sku', 'variation_sku_name', 'parent_brand', 'product_url']}

# COMMAND ----------

spark.sql(f"select distinct product_attributes from {sandbox_schema}.{demo_name}_filter_items").display()

# COMMAND ----------

optional_cols['product_attributes']

# COMMAND ----------

optional_cols = get_optional_columns_availability(sandbox_schema, demo_name)
has_product_attrs = optional_cols['product_attributes']

# Conditionally include optional columns in queries
product_attrs_select = "first(to_json(product_attributes)) as product_attributes," if optional_cols['product_attributes'] else "CAST(null as STRING) as product_attributes,"

# COMMAND ----------

module_name = '_sku_analysis'
# Check which optional columns exist
optional_cols = get_optional_columns_availability(sandbox_schema, demo_name)
has_product_attrs = optional_cols['product_attributes']

# Conditionally include optional columns in queries
product_attrs_select = "first(to_json(product_attributes)) as product_attributes," if optional_cols['product_attributes'] else "CAST(null as STRING) as product_attributes,"
product_hash_select = "min(product_hash) as product_hash," if optional_cols['product_hash'] else "CAST(null as STRING) as product_hash,"
parent_sku_select = "min(parent_sku) as parent_sku," if optional_cols['parent_sku'] else "CAST(null as STRING) as parent_sku,"
parent_sku_name_select = "min(parent_sku_name) as parent_sku_name," if optional_cols['parent_sku_name'] else "CAST(null as STRING) as parent_sku_name,"
parent_brand_select = "min(parent_brand) as parent_brand," if optional_cols['parent_brand'] else "CAST(null as STRING) as parent_brand,"
product_url_select = "min(product_url) as product_url," if optional_cols['product_url'] else "CAST(null as STRING) as product_url,"

sku_analysis = spark.sql(f"""
    -- SET use_cached_result = false;
                        
    with 
    max_month as (
        SELECT month(max(month)) as max_month, MAX(month) as max_date
        FROM {sandbox_schema}.{demo_name}_filter_items
    ),

    change_type as (
        select * except (product_attributes),
        CAST(null as STRING) as product_attributes
        FROM {sandbox_schema}.{demo_name}_filter_items
    ),

    annual as (
        SELECT {product_id} as product_id,
                --- {product_attrs_select}
                {product_hash_select}
                {parent_sku_select}
                {parent_sku_name_select}
                {parent_brand_select}
                {product_url_select}
                count(*) as observations
                FROM change_type
        group by all
    ),

    union_all as (
        SELECT *
        FROM annual
    )

    select
        *
    from
    union_all
""")

# COMMAND ----------

sku_analysis = spark.sql(f"""
SELECT  TYPEOF(product_attributes) as dat,product_attributes
FROM {sandbox_schema}.{demo_name}_filter_items""")


sku_analysis.display()

# COMMAND ----------

export_sku_analysis(
    sandbox_schema,
    prod_schema,
    demo_name,
    product_id
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------



export_sku_time_series(
    sandbox_schema,
    prod_schema,
    demo_name,
    product_id,
    special_attribute_column_original
)

export_sku_detail(
    sandbox_schema,
    prod_schema,
    demo_name,
    product_id
)

# Run QA checks
qa_results = run_sku_analysis_qa(
    sandbox_schema,
    prod_schema,
    demo_name
)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC --------

# COMMAND ----------




    

#
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

print("run_export_shopper_insights_module")
run_export_shopper_insights_module(
    sandbox_schema,
    prod_schema,
    demo_name
)
    
if market_share:
    print("run_export_market_share_module")
    run_export_market_share_module(
        sandbox_schema,
        prod_schema,
        demo_name,
        special_attribute_column,
        start_date_of_data
    )
    print("run_export_geo_analysis_module")
    run_export_geo_analysis_module(
        sandbox_schema,
        prod_schema,
        demo_name
    )
    
    
print("run tariffs")
run_tariffs_module(
    sandbox_schema,
    prod_schema,
    demo_name,
    product_id
    )

if shopper_insights:
    print("run_export_retailer_leakage_module")
    run_export_retailer_leakage_module(
        sandbox_schema,
        prod_schema,
        demo_name,
        start_date_of_data
    )

if  pro_module:
    print("run_export_pro_insights")
    run_export_pro_insights(
        sandbox_schema,
        prod_schema,
        demo_name,
        pro_source_table,
        start_date_of_data
    )

print("run metric save")
# https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/4485801655516586?o=3092962415911490#command/6710077027376437
# Function needs to run after client variables are defined
run_metric_save(demo_name, sandbox_schema, brands_display_list)
