%run /setup_serverless

%run "/Workspace/Corporate Sensitive - Dashboard Templates/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/all"

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

def check_your_parameters(demo_name):
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

def search_demo_name(search_word):
    df = read_gsheet(
        "1F8bvKrBdlY0GIm9xAU5v3IM7ERLM6kUdi_wYph9JJds",
        1374540499
    )
    
    window_spec = (
        Window
        .partitionBy(F.col("demo_namelowercasenospacenoversionegtriplecrowncentral"))
        .orderBy(F.monotonically_increasing_id().desc())
    )

    out = (
        df.withColumn("rank", F.row_number().over(window_spec))
        .select("demo_namelowercasenospacenoversionegtriplecrowncentral", "rank")
    )

    out = out.filter("rank = 1")
    clients = out.select("demo_namelowercasenospacenoversionegtriplecrowncentral")

    filtered_clients = clients.filter(
        lower(clients["demo_namelowercasenospacenoversionegtriplecrowncentral"]).like("%"+search_word+"%")
    )

    if search_word is None:
        final_table = clients
    else:
        final_table = filtered_clients

    final_table = final_table.withColumnRenamed("demo_namelowercasenospacenoversionegtriplecrowncentral", "demo_name")

    final_table.display()
    return final_table

@track_usage
def run_everything (demo_name):
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
    # sandbox_schema = "ydx_internal_analysts_sandbox"
    # prod_schema = "ydx_internal_analysts_gold"

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
            special_attribute_column
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
            product_id
        )
        
    try:    
        print("run tariffs")
        run_tariffs_module(
            sandbox_schema,
            prod_schema,
            demo_name,
            product_id
            )
    except:
        print("didnt work")
    
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

    #if demo_name=="estee_lauder"+version or demo_name=="testesteelauder"+version:
    #    print("This is a Estee Lauder account.")
    #    run_shopper_insights_for_estee_lauder(og_demo_name)