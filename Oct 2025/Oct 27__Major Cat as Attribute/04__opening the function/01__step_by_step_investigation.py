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



# COMMAND ----------

# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/core/setup"

# COMMAND ----------

# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/analysis/product_analysis"

# COMMAND ----------

# DBTITLE 1,Configuration
# Case study client
DEMO_NAME = "weber_v38"
CATALOG = "yd_sensitive_corporate"
FILTER_ITEMS_SCHEMA = "ydx_internal_analysts_sandbox"
SKU_TIME_SERIES_SCHEMA = "ydx_internal_analysts_gold"
PRODUCT_ID = "sku"

# Full table names
FILTER_ITEMS_TABLE = f"{CATALOG}.{FILTER_ITEMS_SCHEMA}.{DEMO_NAME}_filter_items"
OUTPUT_TABLE = f"{CATALOG}.{SKU_TIME_SERIES_SCHEMA}.{DEMO_NAME}_sku_time_series"

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
    Returns a dictionary with sandbox_schema and other relevant config
    """
    try:
        # Try to query the client_info table
        client_info_df = spark.sql(f"""
            SELECT *
            FROM data_solutions_sandbox.corporate_clients_info
            WHERE demo_name = '{demo_name}'
            OR LOWER(demo_name) = LOWER('{demo_name}')
        """)

        # Convert to dictionary
        config = client_info_df.first().asDict()
        print(f"✓ Configuration found for {demo_name}")
        print(f"  Sandbox Schema: {config.get('sandbox_schema', 'N/A')}")

        return config

    except Exception as e:
        print(f"❌ Error retrieving config for {demo_name}: {str(e)}")
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



# COMMAND ----------

# Case study client
DEMO_NAME = "weber_v38"
CATALOG = "yd_sensitive_corporate"
FILTER_ITEMS_SCHEMA = "ydx_internal_analysts_sandbox"
SKU_TIME_SERIES_SCHEMA = "ydx_internal_analysts_gold"
PRODUCT_ID = "sku"

# Full table names
FILTER_ITEMS_TABLE = f"{CATALOG}.{FILTER_ITEMS_SCHEMA}.{DEMO_NAME}_filter_items"
OUTPUT_TABLE = f"{CATALOG}.{SKU_TIME_SERIES_SCHEMA}.{DEMO_NAME}_sku_time_series"

print(f"Configuration:")
print(f"  Demo Name: {DEMO_NAME}")
print(f"  Catalog: {CATALOG}")
print(f"  Filter Items: {FILTER_ITEMS_TABLE}")
print(f"  Output: {OUTPUT_TABLE}")

# COMMAND ----------

# DBTITLE 1,check parameters
DEMO_NAME = "weber"
sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = get_client_config(DEMO_NAME)['demo_name'] + "_v38"
product_id = "sku" #get_client_config(DEMO_NAME)['product_id']
special_attribute_column_original = get_client_config(DEMO_NAME)['special_attribute_column']

print(f"sandbox_schema = {sandbox_schema}") 
print(f"prod_schema = {prod_schema}") 
print(f"demo_name = {demo_name}") 
print(f"product_id = {product_id}") 
print(f"special_attribute_column_original = {special_attribute_column_original}") 

# COMMAND ----------

module_name = '_sku_time_series'

# Check which optional columns exist
optional_cols = get_optional_columns_availability(sandbox_schema, demo_name)
print(optional_cols)
has_product_attrs = optional_cols['product_attributes']
print(has_product_attrs)

# Build product attributes selection logic
# This will include both existing product_attributes (if any) and special_attribute_column_original columns



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

# MAGIC %md
# MAGIC
# MAGIC ====================================================================================================
# MAGIC STEP 2: BUILDING SELECT STATEMENT FOR PRODUCT ATTRIBUTES
# MAGIC ====================================================================================================
# MAGIC Generated SELECT statements:
# MAGIC
# MAGIC product_attrs_select = to_json(product_attributes) as product_attributes_json,
# MAGIC
# MAGIC Other optional columns:
# MAGIC   product_hash_select = min(product_hash) as product_hash,
# MAGIC   parent_sku_select = min(parent_sku) as parent_sku,
# MAGIC   parent_sku_name_select = min(parent_sku_name) as parent_sku_name,
# MAGIC   variation_sku_select = min(variation_sku) as variation_sku,
# MAGIC   variation_sku_name_select = min(variation_sku_name) as variation_sku_name,
# MAGIC   parent_brand_select = min(parent_brand) as parent_brand,
# MAGIC   product_url_select = min(product_url) as product_url,

# COMMAND ----------

# DBTITLE 1,OPENING INTERNAL FUNCTIONS
def has_product_attributes_column(sandbox_schema, demo_name):
    """
    Check if the product_attributes column exists in the filter_items table
    """
    try:
        columns = spark.table(f"{sandbox_schema}.{demo_name}_filter_items").columns
        return 'product_attributes' in columns
    except Exception:
        return False

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

try:
    print("1.")
    columns = spark.table(f"{sandbox_schema}.{demo_name}_filter_items").columns
    print(f"2. {sandbox_schema}.{demo_name}_filter_items")
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
    print(f"3. {optional_columns}")
    res = optional_columns
    print("4.")
except Exception:
    res = {col: False for col in ['product_attributes', 'product_hash', 'parent_sku', 'parent_sku_name', 'variation_sku', 'variation_sku_name', 'parent_brand', 'product_url']}

print(res)

# COMMAND ----------

sku_time_series = spark.sql(f"""
        -- SET use_cached_result = false;
        with 
        max_month as (
            SELECT month(max(month)) as max_month, MAX(month) as max_date
            FROM {sandbox_schema}.{demo_name}_filter_items
        ),
        
        base as (
        SELECT
            "{product_id}" as product_id_type,
            'Monthly Time Series' as display_interval,
            channel,
            major_cat,
            sub_cat,
            minor_cat,
            web_description,
            merchant_clean as merchant,
            brand,
            sub_brand,
            {product_id} as product_id,
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
        FROM {sandbox_schema}.{demo_name}_filter_items
        CROSS JOIN max_month
        WHERE web_description is not null
        GROUP BY ALL
        ),
            
        with_lag_and_new_sku_flag as (
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
        )
        
        SELECT 
            *except(product_attributes_json), parse_json(product_attributes_json) as product_attributes
        FROM with_lag_and_new_sku_flag
        ORDER BY channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id, period_starting
    """)

# COMMAND ----------

sku_time_series = spark.sql(f"""
        -- SET use_cached_result = false;
        with 
        base as (
        SELECT
            {product_attrs_select}
            max(order_date) as last_observation_in_month
        FROM {sandbox_schema}.{demo_name}_filter_items
        WHERE web_description is not null
        GROUP BY ALL
        ),
            
        with_lag_and_new_sku_flag as (
        SELECT 
            *
        FROM base
        )
        
        SELECT 
            *except(product_attributes_json), parse_json(product_attributes_json) as product_attributes
        FROM with_lag_and_new_sku_flag
    """)

sku_time_series.display()


# COMMAND ----------

sku_time_series = spark.sql(f"""
        -- SET use_cached_result = false;
        with 
        max_month as (
            SELECT month(max(month)) as max_month, MAX(month) as max_date
            FROM {sandbox_schema}.{demo_name}_filter_items
        ),
        
        base as (
        SELECT
            "{product_id}" as product_id_type,
            'Monthly Time Series' as display_interval,
            channel,
            major_cat,
            sub_cat,
            minor_cat,
            web_description,
            merchant_clean as merchant,
            brand,
            sub_brand,
            {product_id} as product_id,
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
        FROM {sandbox_schema}.{demo_name}_filter_items
        CROSS JOIN max_month
        WHERE web_description is not null
        GROUP BY ALL
        ),
            
        with_lag_and_new_sku_flag as (
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
        )
        
        SELECT 
            product_attributes_json as old, parse_json(product_attributes_json) as product_attributes
        FROM with_lag_and_new_sku_flag
        ORDER BY channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id, period_starting
    """)

sku_time_series.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY ydx_weber_analysts_gold.weber_v38_sku_time_series

# COMMAND ----------

create_table_with_variant_support(module_name,prod_schema, demo_name+'_sku_time_series', sku_time_series, overwrite=True, variant_columns=['product_attributes'])

print(module_name,"ydx_internal_analysts_gold", demo_name+'_sku_time_series', sku_time_series)

# COMMAND ----------

schema= prod_schema
table_name =  demo_name+'_sku_time_series'
dataframe = sku_time_series
overwrite=True
variant_columns=['product_attributes']

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC opening the create functions
# MAGIC

# COMMAND ----------


from pyspark.sql.functions import col, lit, expr
from pyspark.sql.types import StringType, NullType
import pyspark.sql.functions as F

# COMMAND ----------

# --- blueprints
blueprints_version = get_version_info().get("tag_name")
github_commit_hash = get_version_info().get("commit_hash")

metadata = f"""
'blueprints.created_by' = "Blueprints : export{module_name}",
'blueprints.version' = '{blueprints_version}',
'blueprints.commit_hash' = '{github_commit_hash}'
"""

full_table_name = f"{schema}.{table_name}"
print(full_table_name)

# COMMAND ----------

def table_exists_and_has_variant_support(schema_name, table_name):
    """
    Check if table exists and has variant type support enabled.
    Returns tuple: (table_exists, has_variant_support)
    """
    try:
        # Check if table exists
        tables = spark.sql(f"SHOW TABLES IN {schema_name}").collect()
        table_exists = any(row.tableName == table_name for row in tables)
        
        if not table_exists:
            return False, False
        
        # Check if table has variant type support property
        properties = spark.sql(f"SHOW TBLPROPERTIES {schema_name}.{table_name}").collect()
        has_variant_support = any(
            row.key == 'delta.feature.variantType-preview' and row.value == 'supported' 
            for row in properties
        )
        
        return True, has_variant_support
        
    except Exception as e:
        print(f"⚠️  Error checking table existence/properties: {str(e)}")
        return False, False

# COMMAND ----------

def validate_variant_columns(df, variant_cols):
    """
    Validate that specified variant columns exist in the DataFrame.
    Returns validated list of variant columns that actually exist.
    """
    if not variant_cols:
        return []
    
    df_columns = set(df.columns)
    validated_variant_columns = []
    
    for col_name in variant_cols:
        if col_name in df_columns:
            validated_variant_columns.append(col_name)
            print(f"✅ Validated variant column: '{col_name}'")
        else:
            print(f"⚠️  Warning: Variant column '{col_name}' not found in DataFrame. Skipping.")
    
    return validated_variant_columns

# COMMAND ----------





def fix_null_columns(df, validated_variant_cols):
    """
    Fix columns that are fully NULL with no defined type by casting them appropriately.
    Variant columns are cast to VARIANT, others to STRING.
    This prevents PySpark Connect UNSUPPORTED_OPERATION errors.
    """
    try:
        # Get the schema to identify problematic columns
        schema_fields = df.schema.fields
        select_exprs = []
        
        for field in schema_fields:
            column_name = field.name
            data_type = field.dataType
            
            # Check if the column has NullType or causes schema issues
            if isinstance(data_type, NullType):
                if validated_variant_cols and column_name in validated_variant_cols:
                    print(f"🔄 Casting NULL variant column '{column_name}' to VARIANT type")
                    # Cast to VARIANT using parse_json(null) to create proper VARIANT null
                    select_exprs.append(F.expr("parse_json(null)").alias(column_name))
                else:
                    print(f"⚠️  Casting NULL column '{column_name}' to STRING type")
                    select_exprs.append(col(column_name).cast(StringType()).alias(column_name))
            else:
                select_exprs.append(col(column_name))
        
        # Return the DataFrame with fixed columns
        return df.select(*select_exprs)
        
    except Exception as e:
        print(f"⚠️  Schema analysis failed: {str(e)}")
        print("🔧 Attempting alternative fix by casting problematic columns...")
        
        # Fallback: Try to identify and fix problematic columns by testing each one
        try:
            # Get column names
            column_names = df.columns
            select_exprs = []
            
            for col_name in column_names:
                try:
                    # Test if we can get the data type safely
                    sample_df = df.select(col_name).limit(1)
                    sample_df.collect()  # Force evaluation
                    select_exprs.append(col(col_name))
                except Exception as col_error:
                    if validated_variant_cols and col_name in validated_variant_cols:
                        print(f"🔄 Column '{col_name}' causing issues, casting to VARIANT: {str(col_error)[:100]}")
                        select_exprs.append(F.expr("parse_json(null)").alias(col_name))
                    else:
                        print(f"⚠️  Column '{col_name}' causing issues, casting to STRING: {str(col_error)[:100]}")
                        select_exprs.append(col(col_name).cast(StringType()).alias(col_name))
            
            return df.select(*select_exprs)
            
        except Exception as fallback_error:
            print(f"❌ Fallback fix failed: {str(fallback_error)}")
            print("🔧 Applying blanket casting to all columns...")
            
            # Last resort: cast all columns to appropriate types
            column_names = df.columns
            select_exprs = []
            
            for col_name in column_names:
                # Try to preserve known good column types, cast others appropriately
                if col_name in ['order_date', 'month', 'created_date', 'updated_date']:
                    select_exprs.append(col(col_name))  # Keep date columns as-is
                elif col_name in ['item_price', 'item_quantity', 'gmv']:
                    select_exprs.append(col(col_name))  # Keep numeric columns as-is
                elif validated_variant_cols and col_name in validated_variant_cols:
                    print(f"🔄 Casting problematic variant column '{col_name}' to VARIANT")
                    select_exprs.append(F.expr("parse_json(null)").alias(col_name))
                else:
                    # Cast potentially problematic columns to STRING
                    select_exprs.append(col(col_name).cast(StringType()).alias(col_name))
            
            return df.select(*select_exprs)



# COMMAND ----------

# Validate variant columns first
validated_variant_columns = validate_variant_columns(dataframe, variant_columns)

# COMMAND ----------

# Check if table exists and has variant support
table_exists, has_variant_support = table_exists_and_has_variant_support(schema, table_name)
print(table_exists, has_variant_support)

# COMMAND ----------

print(f"🔧 Processing table with {len(validated_variant_columns)} variant column(s): {validated_variant_columns}")

# Fix NULL columns before creating empty DataFrame
print("🔧 Preprocessing DataFrame to handle NULL columns...")
fixed_dataframe = fix_null_columns(dataframe, validated_variant_columns)

# COMMAND ----------

if table_exists and has_variant_support:
    # Table exists with variant support - just write the data
    print(f"✅ Table {full_table_name} exists with variant support. Writing data directly...")
    # create_table(schema, table_name, fixed_dataframe, overwrite=overwrite, spark_options={"userMetadata" : f"{metadata}"})


# COMMAND ----------

fixed_dataframe.display()

# COMMAND ----------

if table_exists and has_variant_support:
    # Table exists with variant support - just write the data
    print(f"✅ Table {full_table_name} exists with variant support. Writing data directly...")
    create_table(schema, table_name, fixed_dataframe, overwrite=overwrite, spark_options={"userMetadata" : f"{metadata}"})

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from yd_sensitive_corporate.ydx_internal_analysts_gold.weber_v38_sku_time_series

# COMMAND ----------





if validated_variant_columns:
    
else:
    # For non-variant tables, still fix NULL columns to prevent issues
    print("🔧 Preprocessing DataFrame to handle NULL columns...")
    fixed_dataframe = fix_null_columns(dataframe, [])
    
    # Use the standard create_table function for non-variant tables
    print("💾 Creating standard table...")
    create_table(schema, table_name, fixed_dataframe, overwrite=overwrite, spark_options={"userMetadata" : f"{metadata}"})

spark.sql(f"""
ALTER TABLE {schema}.{table_name}
SET TBLPROPERTIES (
{metadata}
)
""")

# COMMAND ----------

def create_table_with_variant_support(module_name, schema, table_name, dataframe, overwrite=True, variant_columns=None):
    """
    Create a table with support for variant columns.
    Handles NULL columns by casting them to STRING type to avoid PySpark Connect issues.
    
    Args:
        schema: Database schema name
        table_name: Name of the table to create
        dataframe: Spark DataFrame to save
        overwrite: Whether to overwrite existing table
        variant_columns: List of column names that contain variant data
    """
    from pyspark.sql.functions import col, lit, expr
    from pyspark.sql.types import StringType, NullType
    import pyspark.sql.functions as F
    
    # --- blueprints
    blueprints_version = get_version_info().get("tag_name")
    github_commit_hash = get_version_info().get("commit_hash")

    metadata = f"""
    'blueprints.created_by' = "Blueprints : export{module_name}",
    'blueprints.version' = '{blueprints_version}',
    'blueprints.commit_hash' = '{github_commit_hash}'
    """

    full_table_name = f"{schema}.{table_name}"
    
    def table_exists_and_has_variant_support(schema_name, table_name):
        """
        Check if table exists and has variant type support enabled.
        Returns tuple: (table_exists, has_variant_support)
        """
        try:
            # Check if table exists
            tables = spark.sql(f"SHOW TABLES IN {schema_name}").collect()
            table_exists = any(row.tableName == table_name for row in tables)
            
            if not table_exists:
                return False, False
            
            # Check if table has variant type support property
            properties = spark.sql(f"SHOW TBLPROPERTIES {schema_name}.{table_name}").collect()
            has_variant_support = any(
                row.key == 'delta.feature.variantType-preview' and row.value == 'supported' 
                for row in properties
            )
            
            return True, has_variant_support
            
        except Exception as e:
            print(f"⚠️  Error checking table existence/properties: {str(e)}")
            return False, False
    
    def validate_variant_columns(df, variant_cols):
        """
        Validate that specified variant columns exist in the DataFrame.
        Returns validated list of variant columns that actually exist.
        """
        if not variant_cols:
            return []
        
        df_columns = set(df.columns)
        validated_variant_columns = []
        
        for col_name in variant_cols:
            if col_name in df_columns:
                validated_variant_columns.append(col_name)
                print(f"✅ Validated variant column: '{col_name}'")
            else:
                print(f"⚠️  Warning: Variant column '{col_name}' not found in DataFrame. Skipping.")
        
        return validated_variant_columns
    
    def fix_null_columns(df, validated_variant_cols):
        """
        Fix columns that are fully NULL with no defined type by casting them appropriately.
        Variant columns are cast to VARIANT, others to STRING.
        This prevents PySpark Connect UNSUPPORTED_OPERATION errors.
        """
        try:
            # Get the schema to identify problematic columns
            schema_fields = df.schema.fields
            select_exprs = []
            
            for field in schema_fields:
                column_name = field.name
                data_type = field.dataType
                
                # Check if the column has NullType or causes schema issues
                if isinstance(data_type, NullType):
                    if validated_variant_cols and column_name in validated_variant_cols:
                        print(f"🔄 Casting NULL variant column '{column_name}' to VARIANT type")
                        # Cast to VARIANT using parse_json(null) to create proper VARIANT null
                        select_exprs.append(F.expr("parse_json(null)").alias(column_name))
                    else:
                        print(f"⚠️  Casting NULL column '{column_name}' to STRING type")
                        select_exprs.append(col(column_name).cast(StringType()).alias(column_name))
                else:
                    select_exprs.append(col(column_name))
            
            # Return the DataFrame with fixed columns
            return df.select(*select_exprs)
            
        except Exception as e:
            print(f"⚠️  Schema analysis failed: {str(e)}")
            print("🔧 Attempting alternative fix by casting problematic columns...")
            
            # Fallback: Try to identify and fix problematic columns by testing each one
            try:
                # Get column names
                column_names = df.columns
                select_exprs = []
                
                for col_name in column_names:
                    try:
                        # Test if we can get the data type safely
                        sample_df = df.select(col_name).limit(1)
                        sample_df.collect()  # Force evaluation
                        select_exprs.append(col(col_name))
                    except Exception as col_error:
                        if validated_variant_cols and col_name in validated_variant_cols:
                            print(f"🔄 Column '{col_name}' causing issues, casting to VARIANT: {str(col_error)[:100]}")
                            select_exprs.append(F.expr("parse_json(null)").alias(col_name))
                        else:
                            print(f"⚠️  Column '{col_name}' causing issues, casting to STRING: {str(col_error)[:100]}")
                            select_exprs.append(col(col_name).cast(StringType()).alias(col_name))
                
                return df.select(*select_exprs)
                
            except Exception as fallback_error:
                print(f"❌ Fallback fix failed: {str(fallback_error)}")
                print("🔧 Applying blanket casting to all columns...")
                
                # Last resort: cast all columns to appropriate types
                column_names = df.columns
                select_exprs = []
                
                for col_name in column_names:
                    # Try to preserve known good column types, cast others appropriately
                    if col_name in ['order_date', 'month', 'created_date', 'updated_date']:
                        select_exprs.append(col(col_name))  # Keep date columns as-is
                    elif col_name in ['item_price', 'item_quantity', 'gmv']:
                        select_exprs.append(col(col_name))  # Keep numeric columns as-is
                    elif validated_variant_cols and col_name in validated_variant_cols:
                        print(f"🔄 Casting problematic variant column '{col_name}' to VARIANT")
                        select_exprs.append(F.expr("parse_json(null)").alias(col_name))
                    else:
                        # Cast potentially problematic columns to STRING
                        select_exprs.append(col(col_name).cast(StringType()).alias(col_name))
                
                return df.select(*select_exprs)
    
    # Validate variant columns first
    validated_variant_columns = validate_variant_columns(dataframe, variant_columns)
    
    # Check if table exists and has variant support
    table_exists, has_variant_support = table_exists_and_has_variant_support(schema, table_name)
    
    if validated_variant_columns:
        print(f"🔧 Processing table with {len(validated_variant_columns)} variant column(s): {validated_variant_columns}")
        
        # Fix NULL columns before creating empty DataFrame
        print("🔧 Preprocessing DataFrame to handle NULL columns...")
        fixed_dataframe = fix_null_columns(dataframe, validated_variant_columns)
        
        if table_exists and has_variant_support:
            # Table exists with variant support - just write the data
            print(f"✅ Table {full_table_name} exists with variant support. Writing data directly...")
            create_table(schema, table_name, fixed_dataframe, overwrite=overwrite, spark_options={"userMetadata" : f"{metadata}"})
        else:
            # Table doesn't exist or doesn't have variant support
            if table_exists:
                print(f"⚠️  Table {full_table_name} exists but lacks variant support. Recreating with support...")
            else:
                print(f"📋 Table {full_table_name} doesn't exist. Creating with variant support...")
            
            # First create an empty table structure to enable variant properties
            print("📋 Creating empty table structure for variant support...")
            empty_df = fixed_dataframe.limit(0)
            create_table(schema, table_name, empty_df, overwrite=True, spark_options={"userMetadata" : f"{metadata}"})
            
            # Set the variant type properties for the table
            print("🔧 Enabling variant type support...")
            spark.sql(f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ('delta.feature.variantType-preview' = 'supported')")
            
            # Now insert the actual data using create_table function
            print("💾 Inserting actual data...")
            create_table(schema, table_name, fixed_dataframe, overwrite=overwrite, spark_options={"userMetadata" : f"{metadata}"})
    else:
        # For non-variant tables, still fix NULL columns to prevent issues
        print("🔧 Preprocessing DataFrame to handle NULL columns...")
        fixed_dataframe = fix_null_columns(dataframe, [])
        
        # Use the standard create_table function for non-variant tables
        print("💾 Creating standard table...")
        create_table(schema, table_name, fixed_dataframe, overwrite=overwrite, spark_options={"userMetadata" : f"{metadata}"})
    
    spark.sql(f"""
    ALTER TABLE {schema}.{table_name}
    SET TBLPROPERTIES (
    {metadata}
    )
    """)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select product_attributes from ydx_internal_analysts_gold.weber_v38_sku_time_series
# MAGIC limit 15

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
