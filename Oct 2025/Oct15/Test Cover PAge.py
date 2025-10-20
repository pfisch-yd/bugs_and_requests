# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC avoid issues in cover page.
# MAGIC
# MAGIC Let's test if your cover page is going to be null.
# MAGIC
# MAGIC We need to see if if the parent_brand is listed on parent_brand
# MAGIC
# MAGIC .
# MAGIC do we use parent or brand

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select parent_brand from ydx_thd_analysts_silver.home_depot_filter_items_dash
# MAGIC limit 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select parent_brand from ydx_thd_analysts_silver.home_depot_filter_items_dash
# MAGIC where
# MAGIC parent_brand = "Home Depot"

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

demo_name = "testgraco"

# COMMAND ----------

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

# COMMAND ----------




sku_detail = spark.sql(f"""
      select parent_brand, sum(gmv) as gmv
      from {source_table}
    where
    parent_brand = "Home Depot"
    group by 1
""")

sku_detail.display()

# COMMAND ----------

def cover_page_ready(source_table, parent_brand):
    module_name = '_filter_items'

    table_name_var = source_table.split(".")[1]
    table_schema_var = source_table.split(".")[0]

    schema_check_sub_brand = spark.sql(f"""
        SELECT 
            column_name
        FROM 
            information_schema.columns
        WHERE 
            table_name = '{table_name_var}'
            AND table_schema = '{table_schema_var}'
            and column_name = 'sub_brand'
    """)

    schema_check_parent_brand = spark.sql(f"""
        SELECT 
            column_name
        FROM 
            information_schema.columns
        WHERE 
            table_name = '{table_name_var}'
            AND table_schema = '{table_schema_var}'
            and column_name = 'parent_brand'
    """)
    schema_check_leia_panel_flag_source = spark.sql(f"""
        SELECT 
            column_name
        FROM 
            information_schema.columns
        WHERE 
            table_name = '{table_name_var}'
            AND table_schema = '{table_schema_var}'
            and column_name = 'leia_panel_flag_source'
    """)

    if schema_check_sub_brand.isEmpty() and schema_check_parent_brand.isEmpty():
        column_except = ''
        column_parent_brand = 'brand'
        column_sub_brand = 'brand'
    elif schema_check_sub_brand.isEmpty() and not(schema_check_parent_brand.isEmpty()):
        column_except = 'except(parent_brand)'
        column_parent_brand = 'parent_brand'
        column_sub_brand = 'brand'
    elif not(schema_check_sub_brand.isEmpty()) and schema_check_parent_brand.isEmpty():
        column_except = 'except(sub_brand)'
        column_parent_brand = 'brand'
        column_sub_brand = 'sub_brand'
    else:
        column_except = 'except(sub_brand, parent_brand)'
        column_parent_brand = 'parent_brand'
        column_sub_brand = 'sub_brand'

    if schema_check_leia_panel_flag_source.isEmpty(): #THIS SECTION NEEDS TO BE REMOVED BY 5/10
        column_leia = '1'
    else:
        column_leia = 'leia_panel_flag_source'

    filter_items = spark.sql(f"""
        with new_columns as (               
        SELECT 
            a.* {column_except},
            coalesce({column_parent_brand}, brand) as parent_brand
        FROM {source_table} a
        )

        select
        parent_brand,
        sum(gmv) as gmv,
        '{parent_brand}' as truee
        from new_columns
        where
        parent_brand = '{parent_brand}'
        group by 1
        
        limit 100
    """)

    return filter_items

# COMMAND ----------

cover_page_ready(source_table, parent_brand).display()

table = cover_page_ready(source_table, parent_brand)

table_is_empty = table.isEmpty()

#print("{} ===> {} =>> {}".format(client_name, prod_schema, table.collect()[0][0]))

# COMMAND ----------


table.collect()[0][0]

# COMMAND ----------

## https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/1648027874099560?o=3092962415911490#command/1648027874099573

#for client_name in distinct_values:
df = read_gsheet(
        "1F8bvKrBdlY0GIm9xAU5v3IM7ERLM6kUdi_wYph9JJds",
        1374540499
)
df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "M/d/yyyy H:mm:ss"))
distinct_values = df.select(df.columns[1]).distinct().toPandas().values.flatten()

len_clients = len(distinct_values)

should_have_tariffs = []
take_action = []
#for client_name in distinct_values:
for i in range(0,len_clients):
    client_name = distinct_values[i]
    demo_name = client_name
    client_row = df.filter(df[1] == demo_name) \
               .orderBy(col("timestamp").desc()) \
               .limit(1)
    client_row_data = client_row.collect()[0]
    prod_schema = client_row_data[4]
    demo_name = demo_name+ "_v38"
    source_table = client_row_data[5]
    parent_brand = clean_last_asterisk(client_row_data[9])
    if prod_schema == "ydx_prospect_analysts_gold":
        print("{} ===> jumpp".format(client_name, prod_schema))
    else:
        try:
            table = cover_page_ready(source_table, parent_brand)

            table_is_empty = table.isEmpty()
            if table_is_empty == False:
                should_have_tariffs.append(client_name)
                message = table.collect()[0][0]
            else:
                take_action.append(client_name)
                message = "CORRECT"
            print("{} ===> {} =>> {}".format(client_name, prod_schema, message))
        except:
            pass

print(should_have_tariffs)

# COMMAND ----------

take_action

# COMMAND ----------

# MAGIC %md
# MAGIC 'sherwin_williams', => no cover
# MAGIC 'bestbuy', => no cover page
# MAGIC 'hart', ===> ISSUE!!!
# MAGIC 'beautyexample',
# MAGIC 'jh_demo',
# MAGIC 'sherwin_williams_limited'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select parent_brand, count(*) as dd from ydx_tti_analysts_silver.hart_final_items
# MAGIC where
# MAGIC lower(parent_brand) like "h%"
# MAGIC group by 1
# MAGIC order by 1
# MAGIC
