# Databricks notebook source
# MAGIC %md
# MAGIC # Run SKU Time Series Function on Test Data
# MAGIC
# MAGIC **Purpose**: Apply the `export_sku_time_series` function to copied test tables
# MAGIC
# MAGIC **Process**:
# MAGIC 1. Load the product_analysis.py functions
# MAGIC 2. Run export_sku_time_series on test catalog tables
# MAGIC 3. Create output tables in test catalog for analysis

# COMMAND ----------

# DBTITLE 1,Setup and Imports
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pandas as pd

spark = SparkSession.builder.getOrCreate()

# Test catalog and schema
TEST_CATALOG = "ydx_internal_analysts_sandbox"
TEST_SCHEMA = "default"  # Adjust if needed

# COMMAND ----------

# DBTITLE 1,Import Blueprint Functions
# Import the product_analysis module from blueprints
# Adjust the path based on where your blueprints are installed

# Option 1: If blueprints are installed as a package
try:
    from corporate_transformation_blueprints.retail_analytics_platform.analysis.product_analysis import (
        export_sku_time_series,
        get_optional_columns_availability
    )
    print("✓ Successfully imported from installed package")
except ImportError:
    print("⚠️  Could not import from package, will define function inline")

# COMMAND ----------

# DBTITLE 1,Helper Function: Create Table with Variant Support
def create_table_with_variant_support(module_name, schema, table_name, dataframe, overwrite=True, variant_columns=None):
    """
    Helper function to create tables with VARIANT columns (like product_attributes)
    """
    full_table_name = f"{schema}.{table_name}"

    print(f"Creating table: {full_table_name}")

    try:
        if overwrite:
            dataframe.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(full_table_name)
        else:
            dataframe.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(full_table_name)

        print(f"✓ Table created successfully: {full_table_name}")

    except Exception as e:
        print(f"❌ Error creating table: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,Define export_sku_time_series Function (Inline Copy)
def export_sku_time_series_test(
        sandbox_schema,
        prod_schema,
        demo_name,
        product_id='web_description'
        ):
    """
    Modified version of export_sku_time_series for testing
    This is a copy from product_analysis.py
    """

    module_name = '_sku_time_series'

    # Check which optional columns exist
    filter_items_table = f"{sandbox_schema}.{demo_name}_v38_filter_items"

    try:
        columns = spark.table(filter_items_table).columns
        optional_cols = {
            'product_attributes': 'product_attributes' in columns,
            'product_hash': 'product_hash' in columns,
            'parent_sku': 'parent_sku' in columns,
            'parent_sku_name': 'parent_sku_name' in columns,
            'variation_sku': 'variation_sku' in columns,
            'variation_sku_name': 'variation_sku_name' in columns,
            'parent_brand': 'parent_brand' in columns,
            'product_url': 'product_url' in columns
        }
    except Exception as e:
        print(f"❌ Error checking columns: {str(e)}")
        raise

    has_product_attrs = optional_cols['product_attributes']

    print(f"Optional columns availability:")
    for col, exists in optional_cols.items():
        status = "✓" if exists else "✗"
        print(f"  {status} {col}")

    # Conditionally include optional columns in queries
    product_attrs_select = "to_json(product_attributes) as product_attributes_json," if optional_cols['product_attributes'] else "CAST(null as STRING) as product_attributes_json,"
    product_hash_select = "min(product_hash) as product_hash," if optional_cols['product_hash'] else "CAST(null as STRING) as product_hash,"
    parent_sku_select = "min(parent_sku) as parent_sku," if optional_cols['parent_sku'] else "CAST(null as STRING) as parent_sku,"
    parent_sku_name_select = "min(parent_sku_name) as parent_sku_name," if optional_cols['parent_sku_name'] else "CAST(null as STRING) as parent_sku_name,"
    variation_sku_select = "min(variation_sku) as variation_sku," if optional_cols['variation_sku'] else "CAST(null as STRING) as variation_sku,"
    variation_sku_name_select = "min(variation_sku_name) as variation_sku_name," if optional_cols['variation_sku_name'] else "CAST(null as STRING) as variation_sku_name,"
    parent_brand_select = "min(parent_brand) as parent_brand," if optional_cols['parent_brand'] else "CAST(null as STRING) as parent_brand,"
    product_url_select = "min(product_url) as product_url," if optional_cols['product_url'] else "CAST(null as STRING) as product_url,"

    sku_time_series = spark.sql(f"""
        -- SET use_cached_result = false;
        with
        max_month as (
            SELECT month(max(month)) as max_month, MAX(month) as max_date
            FROM {filter_items_table}
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
        FROM {filter_items_table}
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

    output_table = f"{prod_schema}.{demo_name}_sku_time_series_test"

    create_table_with_variant_support(
        module_name,
        prod_schema,
        f"{demo_name}_sku_time_series_test",
        sku_time_series,
        overwrite=True,
        variant_columns=['product_attributes']
    )

    print(f"✓ Created output table: {output_table}")

    return spark.table(output_table)

# COMMAND ----------

# DBTITLE 1,Client Configuration
clients_config = {
    'weber': {'demo_name': 'weber'},
    'werner': {'demo_name': 'werner'},
    'odele': {'demo_name': 'odele'}
}

# COMMAND ----------

# DBTITLE 1,Run export_sku_time_series for Each Client
print("=" * 80)
print("RUNNING export_sku_time_series ON TEST DATA")
print("=" * 80)

results = {}

for client_name, config in clients_config.items():
    demo_name = config['demo_name']

    print(f"\n{'#'*80}")
    print(f"# PROCESSING: {client_name.upper()}")
    print(f"{'#'*80}\n")

    try:
        # Run the function
        result_df = export_sku_time_series_test(
            sandbox_schema=f"{TEST_CATALOG}.{TEST_SCHEMA}",
            prod_schema=f"{TEST_CATALOG}.{TEST_SCHEMA}",
            demo_name=demo_name,
            product_id='web_description'
        )

        record_count = result_df.count()
        print(f"\n✓ Successfully created _sku_time_series_test table")
        print(f"  Records: {record_count:,}")

        results[client_name] = {
            'success': True,
            'record_count': record_count,
            'table_name': f"{TEST_CATALOG}.{TEST_SCHEMA}.{demo_name}_sku_time_series_test"
        }

    except Exception as e:
        print(f"\n❌ Error processing {client_name}: {str(e)}")
        results[client_name] = {
            'success': False,
            'error': str(e)
        }

# COMMAND ----------

# DBTITLE 1,Summary
print("\n\n")
print("=" * 80)
print("PROCESSING SUMMARY")
print("=" * 80)

for client_name, result in results.items():
    if result['success']:
        print(f"\n{client_name.upper()}:")
        print(f"  ✓ Success")
        print(f"  Records: {result['record_count']:,}")
        print(f"  Table: {result['table_name']}")
    else:
        print(f"\n{client_name.upper()}:")
        print(f"  ❌ Failed")
        print(f"  Error: {result['error']}")

print("\n" + "=" * 80)

# COMMAND ----------

# DBTITLE 1,Quick Check: Product Attributes in Output
print("\n\n")
print("=" * 80)
print("CHECKING PRODUCT_ATTRIBUTES IN OUTPUT TABLES")
print("=" * 80)

for client_name, result in results.items():
    if result['success']:
        table_name = result['table_name']

        try:
            df = spark.table(table_name)

            # Check if product_attributes column exists
            if 'product_attributes' in df.columns:
                # Get sample records with product_attributes
                attrs_json = df.filter(F.col('product_attributes').isNotNull()) \
                    .select(F.to_json('product_attributes').alias('attrs_json'))

                total_with_attrs = attrs_json.count()

                print(f"\n{client_name.upper()}:")
                print(f"  Total records with product_attributes: {total_with_attrs:,}")

                # Check for category-related keywords
                keywords = ['major_cat', 'Major Cat', 'major cat', 'category']
                for keyword in keywords:
                    count_with_keyword = attrs_json.filter(F.col('attrs_json').contains(keyword)).count()
                    if count_with_keyword > 0:
                        pct = (count_with_keyword / total_with_attrs * 100) if total_with_attrs > 0 else 0
                        print(f"  ⚠️  '{keyword}' found in {count_with_keyword:,} records ({pct:.2f}%)")

                # Show sample records
                print(f"\n  Sample records:")
                sample_df = df.filter(F.col('product_attributes').isNotNull()) \
                    .select('major_cat', 'web_description', 'product_attributes') \
                    .limit(3)
                sample_df.show(truncate=False)

        except Exception as e:
            print(f"\n{client_name.upper()}: ❌ Error - {str(e)}")

print("=" * 80)
