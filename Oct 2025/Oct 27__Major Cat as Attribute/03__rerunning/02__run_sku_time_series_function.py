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
TEST_CATALOG = "yd_sensitive_corporate"
TEST_SCHEMA = "ydx_internal_analysts_sandbox"  # Adjust if needed

# COMMAND ----------

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

# MAGIC %sql
# MAGIC
# MAGIC select * FROM data_solutions_sandbox.corporate_clients_info

# COMMAND ----------

# DBTITLE 1,Import Blueprint Functions
# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/analysis/product_analysis"

# COMMAND ----------

# DBTITLE 1,Client Configuration
clients_config = {
    'weber': {'demo_name': 'weber'},
    'werner': {'demo_name': 'werner'},
    'odele': {'demo_name': 'odele'}
}

# COMMAND ----------



# COMMAND ----------

#    'werner': {'demo_name': 'werner'},
    'odele': {'demo_name': 'odele'}

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
        special_attribute_column = get_client_config(demo_name)['special_attribute_column']
        # Run the function
        result_df = export_sku_time_series(
            sandbox_schema=f"ydx_internal_analysts_sandbox",
            prod_schema=f"ydx_internal_analysts_gold",
            demo_name=f"{demo_name}_v38",
            product_id='web_description',
            special_attribute_column_original=special_attribute_column
        )
        

        print(f"sandbox_schema={TEST_CATALOG}.{TEST_SCHEMA}")
        print(f"prod_schema={TEST_CATALOG}.{TEST_SCHEMA},")
        print(f"demo_name={demo_name},")
        print(f"special_attribute_column={special_attribute_column},")

        record_count = result_df.count()
        print(f"\n✓ Successfully created _sku_time_series_test table")
        print(f"  Records: {record_count:,}")

        results[client_name] = {
            'success': True,
            'record_count': record_count,
            'table_name': f"{TEST_CATALOG}.{TEST_SCHEMA}.{demo_name}_v38_sku_time_series"
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

print("1.")
for client_name, result in results.items():
    print("2.")
    if result['success']:
        print("3.")
        table_name = result['table_name']
        print(table_name )
        try:
            print("4.")
            df = spark.table(table_name)
            print("4a.")
            # Check if product_attributes column exists
            if 'product_attributes' in df.columns:
                print("5.")
                # Get sample records with product_attributes
                attrs_json = df.filter(F.col('product_attributes').isNotNull()) \
                    .select(F.to_json('product_attributes').alias('attrs_json'))
                print("5b.")
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
                print("5c.")
                # Show sample records
                print(f"\n  Sample records:")
                sample_df = df.filter(F.col('product_attributes').isNotNull()) \
                    .select('major_cat', 'web_description', 'product_attributes') \
                    .limit(3)
                sample_df.show(truncate=False)

        except Exception as e:
            print("5.")
            print(f"\n{client_name.upper()}: ❌ Error - {str(e)}")

print("=" * 80)
print("6.")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select  product_attributes from 
# MAGIC yd_sensitive_corporate.ydx_internal_analysts_sandbox.weber_v38_sku_time_series
# MAGIC
# MAGIC where product_attributes is not null

# COMMAND ----------


