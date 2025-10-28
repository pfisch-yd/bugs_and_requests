# Databricks notebook source
# MAGIC %md
# MAGIC # Step-by-Step Investigation: export_sku_time_series Function
# MAGIC
# MAGIC **Case Study**: Weber
# MAGIC
# MAGIC **Objective**: Break down the export_sku_time_series function into small steps and verify product_attributes integrity at each stage
# MAGIC
# MAGIC **Catalog & Schemas**:
# MAGIC - Catalog: `yd_sensitive_corporate`
# MAGIC - Filter Items Schema: `ydx_internal_analysts_sandbox`
# MAGIC - SKU Time Series Schema: `ydx_internal_analysts_gold`

# COMMAND ----------

# DBTITLE 1,Setup and Imports
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from yipit_databricks_utils.future import create_table

spark = SparkSession.builder.getOrCreate()

# Load setup functions
%run "/Workspace/Corporate Sensitive - Dashboard Templates/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/core/setup"

# COMMAND ----------

# DBTITLE 1,Configuration
# Case study client
DEMO_NAME = "weber"
CATALOG = "yd_sensitive_corporate"
FILTER_ITEMS_SCHEMA = "ydx_internal_analysts_sandbox"
SKU_TIME_SERIES_SCHEMA = "ydx_internal_analysts_gold"
PRODUCT_ID = "web_description"

# Full table names
FILTER_ITEMS_TABLE = f"{CATALOG}.{FILTER_ITEMS_SCHEMA}.{DEMO_NAME}_v38_filter_items"
OUTPUT_TABLE = f"{CATALOG}.{SKU_TIME_SERIES_SCHEMA}.{DEMO_NAME}_v38_sku_time_series_debug"

print(f"Configuration:")
print(f"  Demo Name: {DEMO_NAME}")
print(f"  Catalog: {CATALOG}")
print(f"  Filter Items: {FILTER_ITEMS_TABLE}")
print(f"  Output: {OUTPUT_TABLE}")

# COMMAND ----------

# DBTITLE 1,Function: Get Client Config
def get_client_config(demo_name):
    """
    Retrieve client configuration from data_solutions.client_info table
    """
    try:
        client_info_df = spark.sql(f"""
            SELECT *
            FROM data_solutions.client_info
            WHERE LOWER(demo_name) = LOWER('{demo_name}')
            OR LOWER(client_name) = LOWER('{demo_name}')
        """)

        if client_info_df.count() > 0:
            config = client_info_df.first().asDict()
            print(f"✓ Configuration found for {demo_name}")
            return config
        else:
            print(f"⚠️  No configuration found for {demo_name}")
            return None

    except Exception as e:
        print(f"❌ Error retrieving config: {str(e)}")
        return None

# Test the function
config = get_client_config(DEMO_NAME)
if config:
    for key, value in config.items():
        print(f"  {key}: {value}")

# COMMAND ----------

# DBTITLE 1,Function: Check Product Attributes Integrity
def check_product_attributes_integrity(df, stage_name, show_samples=True):
    """
    Comprehensive integrity check for product_attributes column

    Returns:
    - Data type
    - Fill rate (% non-null)
    - Top value
    - Sample records
    """
    print(f"\n{'='*80}")
    print(f"INTEGRITY CHECK: {stage_name}")
    print(f"{'='*80}\n")

    total_records = df.count()
    print(f"Total records: {total_records:,}")

    # Check if product_attributes column exists
    if 'product_attributes' not in df.columns:
        print("⚠️  WARNING: product_attributes column does NOT exist")
        return {
            'stage': stage_name,
            'column_exists': False,
            'total_records': total_records
        }

    print("✓ product_attributes column exists")

    # Get data type
    schema_field = [f for f in df.schema.fields if f.name == 'product_attributes'][0]
    data_type = str(schema_field.dataType)
    print(f"\n1. DATA TYPE: {data_type}")

    # Calculate fill rate
    null_count = df.filter(F.col('product_attributes').isNull()).count()
    non_null_count = total_records - null_count
    fill_rate = (non_null_count / total_records * 100) if total_records > 0 else 0

    print(f"\n2. FILL RATE:")
    print(f"   Non-null: {non_null_count:,} ({fill_rate:.2f}%)")
    print(f"   Null: {null_count:,} ({100-fill_rate:.2f}%)")

    # Get top value
    if non_null_count > 0:
        print(f"\n3. TOP VALUES:")

        # Convert to JSON string for comparison
        attrs_json_df = df.filter(F.col('product_attributes').isNotNull()) \
            .select(F.to_json('product_attributes').alias('attrs_json'))

        top_values = attrs_json_df.groupBy('attrs_json') \
            .agg(F.count('*').alias('count')) \
            .orderBy(F.desc('count')) \
            .limit(5)

        top_values_pd = top_values.toPandas()
        top_values_pd['percentage'] = (top_values_pd['count'] / non_null_count * 100).round(2)

        print(top_values_pd.to_string(index=False))

        # Check for "major_cat" or category keywords
        print(f"\n4. CATEGORY KEYWORD CHECK:")
        category_keywords = ['major_cat', 'Major Cat', 'major cat', 'category']

        for keyword in category_keywords:
            count_with_keyword = attrs_json_df.filter(F.col('attrs_json').contains(keyword)).count()

            if count_with_keyword > 0:
                pct = (count_with_keyword / non_null_count * 100) if non_null_count > 0 else 0
                print(f"   ⚠️  '{keyword}' found in {count_with_keyword:,} records ({pct:.2f}%)")
            else:
                print(f"   ✓ '{keyword}' not found")

        # Show samples
        if show_samples:
            print(f"\n5. SAMPLE RECORDS:")
            sample_df = df.filter(F.col('product_attributes').isNotNull()) \
                .select('major_cat', 'web_description', 'product_attributes') \
                .limit(3)
            sample_df.show(truncate=False)
    else:
        print("\n   No non-null values to analyze")

    print(f"\n{'='*80}\n")

    return {
        'stage': stage_name,
        'column_exists': True,
        'data_type': data_type,
        'total_records': total_records,
        'non_null_count': non_null_count,
        'fill_rate': fill_rate
    }

# COMMAND ----------

# DBTITLE 1,STEP 0: Check Source Table (_filter_items)
print("=" * 100)
print("STEP 0: CHECKING SOURCE TABLE")
print("=" * 100)

source_df = spark.table(FILTER_ITEMS_TABLE)

step0_integrity = check_product_attributes_integrity(
    source_df,
    "STEP 0: Source _filter_items table"
)

# Store for later comparison
source_df.createOrReplaceTempView("source_filter_items")

# COMMAND ----------

# DBTITLE 1,STEP 1: Check Optional Columns Availability
print("=" * 100)
print("STEP 1: CHECKING OPTIONAL COLUMNS AVAILABILITY")
print("=" * 100)

def get_optional_columns_availability(table_name):
    """
    Check which optional columns exist in the filter_items table
    """
    try:
        columns = spark.table(table_name).columns
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

optional_cols = get_optional_columns_availability(FILTER_ITEMS_TABLE)
has_product_attrs = optional_cols['product_attributes']

print("\nOptional columns availability:")
for col, exists in optional_cols.items():
    status = "✓" if exists else "✗"
    print(f"  {status} {col}")

print(f"\nhas_product_attrs = {has_product_attrs}")

# COMMAND ----------

# DBTITLE 1,STEP 2: Build SELECT Statement for Product Attributes
print("=" * 100)
print("STEP 2: BUILDING SELECT STATEMENT FOR PRODUCT ATTRIBUTES")
print("=" * 100)

# This is the logic from the original function
product_attrs_select = "to_json(product_attributes) as product_attributes_json," if optional_cols['product_attributes'] else "CAST(null as STRING) as product_attributes_json,"
product_hash_select = "min(product_hash) as product_hash," if optional_cols['product_hash'] else "CAST(null as STRING) as product_hash,"
parent_sku_select = "min(parent_sku) as parent_sku," if optional_cols['parent_sku'] else "CAST(null as STRING) as parent_sku,"
parent_sku_name_select = "min(parent_sku_name) as parent_sku_name," if optional_cols['parent_sku_name'] else "CAST(null as STRING) as parent_sku_name,"
variation_sku_select = "min(variation_sku) as variation_sku," if optional_cols['variation_sku'] else "CAST(null as STRING) as variation_sku,"
variation_sku_name_select = "min(variation_sku_name) as variation_sku_name," if optional_cols['variation_sku_name'] else "CAST(null as STRING) as variation_sku_name,"
parent_brand_select = "min(parent_brand) as parent_brand," if optional_cols['parent_brand'] else "CAST(null as STRING) as parent_brand,"
product_url_select = "min(product_url) as product_url," if optional_cols['product_url'] else "CAST(null as STRING) as product_url,"

print("Generated SELECT statements:")
print(f"\nproduct_attrs_select = {product_attrs_select}")
print(f"\nOther optional columns:")
print(f"  product_hash_select = {product_hash_select}")
print(f"  parent_sku_select = {parent_sku_select}")
print(f"  parent_sku_name_select = {parent_sku_name_select}")
print(f"  variation_sku_select = {variation_sku_select}")
print(f"  variation_sku_name_select = {variation_sku_name_select}")
print(f"  parent_brand_select = {parent_brand_select}")
print(f"  product_url_select = {product_url_select}")

# COMMAND ----------

# DBTITLE 1,STEP 3: Create max_month CTE
print("=" * 100)
print("STEP 3: CREATING max_month CTE")
print("=" * 100)

max_month_df = spark.sql(f"""
    SELECT
        month(max(month)) as max_month,
        MAX(month) as max_date
    FROM {FILTER_ITEMS_TABLE}
""")

max_month_df.show()
max_month_df.createOrReplaceTempView("max_month")

max_month_values = max_month_df.first()
print(f"\nmax_month: {max_month_values['max_month']}")
print(f"max_date: {max_month_values['max_date']}")

# COMMAND ----------

# DBTITLE 1,STEP 4: Create base CTE (with aggregations)
print("=" * 100)
print("STEP 4: CREATING base CTE")
print("=" * 100)

base_query = f"""
    SELECT
        "{PRODUCT_ID}" as product_id_type,
        'Monthly Time Series' as display_interval,
        channel,
        major_cat,
        sub_cat,
        minor_cat,
        web_description,
        merchant_clean as merchant,
        brand,
        sub_brand,
        {PRODUCT_ID} as product_id,
        {product_attrs_select}
        {product_hash_select}
        {parent_sku_select}
        {parent_sku_name_select}
        {variation_sku_select}
        {variation_sku_name_select}
        {parent_brand_select}
        {product_url_select}
        month as period_month,
        month as period_starting,
        max_date,
        SUM(item_price * item_quantity) as total_spend,
        SUM(item_quantity) as total_units,
        SUM(gmv) as gmv,
        count(*) as observations,
        min(order_date) as first_observation_in_month,
        max(order_date) as last_observation_in_month
    FROM {FILTER_ITEMS_TABLE}
    CROSS JOIN max_month
    WHERE web_description is not null
    GROUP BY ALL
"""

print("Base query:")
print(base_query)

base_df = spark.sql(base_query)

print(f"\nBase DataFrame created with {base_df.count():,} records")

# Check integrity at this stage
step4_integrity = check_product_attributes_integrity(
    base_df,
    "STEP 4: After base CTE (aggregation)"
)

base_df.createOrReplaceTempView("base")

# COMMAND ----------

# DBTITLE 1,STEP 4.5: Investigate product_attributes_json
print("=" * 100)
print("STEP 4.5: INVESTIGATING product_attributes_json (BEFORE parse_json)")
print("=" * 100)

# Check the JSON string version before parsing
if 'product_attributes_json' in base_df.columns:
    print("\n✓ product_attributes_json column exists")

    json_check_df = base_df.filter(F.col('product_attributes_json').isNotNull())
    json_count = json_check_df.count()

    print(f"Records with non-null product_attributes_json: {json_count:,}")

    if json_count > 0:
        print("\nSample product_attributes_json values:")
        json_check_df.select('major_cat', 'web_description', 'product_attributes_json').show(5, truncate=False)

        # Check for major_cat in JSON
        major_cat_in_json = json_check_df.filter(
            F.col('product_attributes_json').contains('major_cat')
        ).count()

        if major_cat_in_json > 0:
            print(f"\n⚠️  WARNING: Found 'major_cat' in {major_cat_in_json:,} JSON strings")
            print("\nSample records with major_cat in JSON:")
            json_check_df.filter(
                F.col('product_attributes_json').contains('major_cat')
            ).select('major_cat', 'web_description', 'product_attributes_json').show(5, truncate=False)
        else:
            print("\n✓ No 'major_cat' found in product_attributes_json")
else:
    print("⚠️  product_attributes_json column does NOT exist")

# COMMAND ----------

# DBTITLE 1,STEP 5: Add Lag and New SKU Flag
print("=" * 100)
print("STEP 5: ADDING LAG AND NEW SKU FLAG")
print("=" * 100)

with_lag_query = """
    SELECT
        *,
        lag(observations) OVER (
            partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id
            order by period_starting
        ) as last_obs,
        lag(gmv) OVER (
            partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id
            order by period_starting
        ) as last_gmv,
        CASE WHEN row_number() OVER (
            partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id
            order by period_starting
        ) = 1 then 1 else 0 end as new_sku_flag
    FROM base
"""

with_lag_df = spark.sql(with_lag_query)

print(f"\nDataFrame with lag created with {with_lag_df.count():,} records")

# Check integrity at this stage (product_attributes_json should still be there)
print("\nChecking product_attributes_json at this stage:")
if 'product_attributes_json' in with_lag_df.columns:
    print("✓ product_attributes_json still exists")
    sample_json = with_lag_df.filter(F.col('product_attributes_json').isNotNull()).limit(3)
    sample_json.select('major_cat', 'product_attributes_json').show(truncate=False)
else:
    print("⚠️  product_attributes_json column lost")

with_lag_df.createOrReplaceTempView("with_lag_and_new_sku_flag")

# COMMAND ----------

# DBTITLE 1,STEP 6: Parse JSON to Create Final product_attributes
print("=" * 100)
print("STEP 6: PARSING JSON TO CREATE FINAL product_attributes")
print("=" * 100)

final_query = """
    SELECT
        *except(product_attributes_json),
        parse_json(product_attributes_json) as product_attributes
    FROM with_lag_and_new_sku_flag
    ORDER BY channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id, period_starting
"""

final_df = spark.sql(final_query)

print(f"\nFinal DataFrame created with {final_df.count():,} records")

# Check integrity of final product_attributes
step6_integrity = check_product_attributes_integrity(
    final_df,
    "STEP 6: Final output (after parse_json)"
)

# COMMAND ----------

# DBTITLE 1,STEP 7: Save to Table (Optional - Uncomment to Save)
print("=" * 100)
print("STEP 7: SAVING TO TABLE (DEBUG VERSION)")
print("=" * 100)

# Uncomment to save the table
# create_table_with_variant_support(
#     "_sku_time_series_debug",
#     SKU_TIME_SERIES_SCHEMA,
#     f"{DEMO_NAME}_v38_sku_time_series_debug",
#     final_df,
#     overwrite=True,
#     variant_columns=['product_attributes']
# )
#
# print(f"✓ Table saved to {OUTPUT_TABLE}")

print("Skipping table save (uncomment code above to save)")

# COMMAND ----------

# DBTITLE 1,SUMMARY: Integrity Check Comparison
print("\n\n")
print("=" * 100)
print("SUMMARY: INTEGRITY CHECK COMPARISON")
print("=" * 100)

summary_data = []

for step_name, integrity_result in [
    ('STEP 0: Source', step0_integrity),
    ('STEP 4: After Aggregation', step4_integrity),
    ('STEP 6: Final Output', step6_integrity)
]:
    if integrity_result and integrity_result.get('column_exists'):
        summary_data.append({
            'Step': step_name,
            'Total Records': f"{integrity_result.get('total_records', 0):,}",
            'Non-Null Count': f"{integrity_result.get('non_null_count', 0):,}",
            'Fill Rate': f"{integrity_result.get('fill_rate', 0):.2f}%",
            'Data Type': integrity_result.get('data_type', 'N/A')
        })
    else:
        summary_data.append({
            'Step': step_name,
            'Total Records': f"{integrity_result.get('total_records', 0):,}" if integrity_result else 'N/A',
            'Non-Null Count': 'N/A',
            'Fill Rate': 'N/A',
            'Data Type': 'Column Missing'
        })

summary_df = pd.DataFrame(summary_data)
print("\n")
print(summary_df.to_string(index=False))

print("\n" + "=" * 100)

# COMMAND ----------

# DBTITLE 1,DIAGNOSTIC: Find Where Major Cat Gets Added
print("\n\n")
print("=" * 100)
print("DIAGNOSTIC: SEARCHING FOR major_cat INJECTION POINT")
print("=" * 100)

# Compare source vs final
print("\n1. CHECKING SOURCE (_filter_items):")
source_with_major_cat = spark.sql(f"""
    SELECT COUNT(*) as count
    FROM {FILTER_ITEMS_TABLE}
    WHERE to_json(product_attributes) LIKE '%major_cat%'
""").first()['count']

print(f"   Records with 'major_cat' in product_attributes: {source_with_major_cat:,}")

print("\n2. CHECKING FINAL OUTPUT:")
final_with_major_cat = final_df.filter(
    F.to_json('product_attributes').contains('major_cat')
).count()

print(f"   Records with 'major_cat' in product_attributes: {final_with_major_cat:,}")

print("\n3. DIAGNOSIS:")
if source_with_major_cat == 0 and final_with_major_cat > 0:
    print("   ❌ CONFIRMED: major_cat is being ADDED during transformation")
    print("   Root cause: The function is creating product_attributes incorrectly")
elif source_with_major_cat > 0 and final_with_major_cat > 0:
    print("   ⚠️  major_cat exists in BOTH source and output")
    print("   Root cause: Upstream data issue (already in _filter_items)")
else:
    print("   ✓ No major_cat found in either source or output")

print("\n" + "=" * 100)
