# Databricks notebook source
# MAGIC %md
# MAGIC # Copy Filter Items Tables to Test Catalog
# MAGIC
# MAGIC **Purpose**: Copy `_v38_filter_items` tables from production to test catalog for investigation
# MAGIC
# MAGIC **Source**: `(sandbox_schema).(demo_name)_v38_filter_items`
# MAGIC **Destination**: `ydx_internal_analysts_sandbox.(demo_name)_v38_filter_items`

# COMMAND ----------

# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/core/setup"

# COMMAND ----------

# DBTITLE 1,Setup and Imports
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from yipit_databricks_utils.future import create_table

spark = SparkSession.builder.getOrCreate()

# Test catalog and schema
TEST_CATALOG = "ydx_internal_analysts_sandbox"
TEST_SCHEMA = "ydx_internal_analysts_sandbox"  # Adjust if needed

# COMMAND ----------

# DBTITLE 1,Function: Get Client Config from Database
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

# COMMAND ----------

# DBTITLE 1,Client Configuration
# Define clients to investigate
clients_config = {
    'weber': {
        'demo_name': 'weber',
        'sandbox_schema': get_client_config('weber')['sandbox_schema']  # Update based on actual config
    },
    'werner': {
        'demo_name': 'werner',
        'sandbox_schema': get_client_config('werner')['sandbox_schema']  # Update based on actual config
    },
    'odele': {
        'demo_name': 'odele',
        'sandbox_schema': get_client_config('odele')['sandbox_schema']  # Update based on actual config
    }
}

# COMMAND ----------

# DBTITLE 1,Function: Copy Table to Test Catalog
def copy_filter_items_to_test(demo_name, sandbox_schema, test_catalog=TEST_CATALOG, test_schema=TEST_SCHEMA):
    """
    Copy _v38_filter_items table from production to test catalog
    """
    source_table = f"{sandbox_schema}.{demo_name}_v38_filter_items"
    dest_table = f"ydx_internal_analysts_sandbox.{demo_name}_v38_filter_items"

    print(f"\n{'='*80}")
    print(f"COPYING TABLE FOR: {demo_name.upper()}")
    print(f"{'='*80}")
    print(f"Source: {source_table}")
    print(f"Destination: {dest_table}")

    try:
        # Check if source table exists
        source_df = spark.table(source_table)
        record_count = source_df.count()
        print(f"✓ Source table found with {record_count:,} records")

        # Show schema
        print(f"\nTable schema:")
        source_df.printSchema()

        # Check if product_attributes column exists
        has_product_attrs = 'product_attributes' in source_df.columns
        print(f"\nHas product_attributes column: {has_product_attrs}")

        # Create or replace table in test catalog
        # 
        print(f"\nCopying table...")
        create_table_with_variant_support("prep_filter_items", "ydx_internal_analysts_sandbox", f"{demo_name}_v38_filter_items", source_df, overwrite=True, variant_columns=['product_attributes'])

        # Verify copy
        dest_df = spark.table(dest_table)
        dest_count = dest_df.count()

        if dest_count == record_count:
            print(f"✓ Table copied successfully! {dest_count:,} records")
        else:
            print(f"⚠️  Warning: Record count mismatch!")
            print(f"   Source: {record_count:,}")
            print(f"   Destination: {dest_count:,}")

        return True

    except Exception as e:
        print(f"❌ Error copying table: {str(e)}")
        return False

# COMMAND ----------

# DBTITLE 1,Copy All Client Tables
print("\n\n")
print("=" * 80)
print("STARTING TABLE COPY PROCESS")
print("=" * 80)

results = {}

for client_name, config in clients_config.items():
    success = copy_filter_items_to_test(
        demo_name=config['demo_name'],
        sandbox_schema=config['sandbox_schema']
    )
    results[client_name] = success

    print(config['demo_name'])
    print(config['sandbox_schema'])

# COMMAND ----------

# DBTITLE 1,Summary
print("\n\n")
print("=" * 80)
print("COPY SUMMARY")
print("=" * 80)

for client_name, success in results.items():
    status = "✓ SUCCESS" if success else "❌ FAILED"
    print(f"{client_name:20s} {status}")

print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Verify Copied Tables
print("\n\n")
print("=" * 80)
print("VERIFICATION: CHECKING PRODUCT_ATTRIBUTES IN COPIED TABLES")
print("=" * 80)

for client_name, config in clients_config.items():
    dest_table = f"{TEST_SCHEMA}.{config['demo_name']}_v38_filter_items"

    try:
        print(dest_table)
        
        df = spark.sql(f"select * from {dest_table}")
        if 'product_attributes' in df.columns:
            # Check for category-related terms in product_attributes
            attrs_json = df.filter(F.col('product_attributes').isNotNull()) \
                .select(F.to_json('product_attributes').alias('attrs_json'))

            total_with_attrs = attrs_json.count()

            # Check for "major_cat" or similar
            keywords = ['major_cat', 'Major Cat', 'major cat', 'category']
            print(f"\n{client_name.upper()}:")
            print(f"  Total records with product_attributes: {total_with_attrs:,}")

            for keyword in keywords:
                count_with_keyword = attrs_json.filter(F.col('attrs_json').contains(keyword)).count()
                if count_with_keyword > 0:
                    pct = (count_with_keyword / total_with_attrs * 100) if total_with_attrs > 0 else 0
                    print(f"  ⚠️  '{keyword}' found in {count_with_keyword:,} records ({pct:.2f}%)")
    except Exception as e:
        print(f"\n{client_name.upper()}: ❌ Error - {str(e)}")

print("\n" + "=" * 80)

# COMMAND ----------

"yd_sensitive_corporate.ydx_internal_analysts_sandbox.odele_v38_filter_items"

# COMMAND ----------

# DBTITLE 1,Sample Records with Product Attributes
print("\n\n")
print("=" * 80)
print("SAMPLE RECORDS WITH PRODUCT_ATTRIBUTES")
print("=" * 80)

for client_name, config in clients_config.items():
    dest_table = f"ydx_internal_analysts_sandbox.{config['demo_name']}_v38_filter_items"

    try:
        df = spark.table(dest_table)

        if 'product_attributes' in df.columns:
            print(f"\n{'-'*80}")
            print(f"CLIENT: {client_name.upper()}")
            print(f"{'-'*80}")

            sample_df = df.filter(F.col('product_attributes').isNotNull()) \
                .select('major_cat', 'sub_cat', 'web_description', 'product_attributes') \
                .limit(3)

            sample_df.show(truncate=False)

    except Exception as e:
        print(f"\n{client_name.upper()}: ❌ Error - {str(e)}")

print("=" * 80)
