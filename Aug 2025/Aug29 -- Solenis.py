# Databricks notebook source
# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/Brands Dashboard Templates/__centralized_udfs_for_client_dashboard_exports"

# COMMAND ----------

demo_name = "testsolenis"
check_your_parameters(demo_name)

# COMMAND ----------

demo_name = "testsolenis"
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

def clean_last_asterisk(text):
    lenn = len(text)
    if text[lenn-1:lenn] == "'":
        ans = text[:-1]
    else:
        ans = text
    return ans

# COMMAND ----------

text = client_row_data[9]

lenn = len(text)
if text[lenn-1:lenn] == "'":
    print(text[:-1])
else:
    print(text)

# COMMAND ----------

parent_brand = clean_last_asterisk(client_row_data[9])
parent_brand 
