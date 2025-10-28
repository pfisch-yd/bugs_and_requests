# Databricks notebook source
# MAGIC %md
# MAGIC # Identify Affected Clients
# MAGIC
# MAGIC **Objective**: Find all clients that have:
# MAGIC 1. `special_attribute_column` is NULL, [], or empty
# MAGIC 2. BUT have a valid `product_attributes` column (variant type, NOT 100% null)
# MAGIC
# MAGIC **Data Source**: `data_solutions_sandbox.corporate_clients_info`

# COMMAND ----------

# DBTITLE 1,Setup and Imports
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# DBTITLE 1,Load All Clients from corporate_clients_info
print("=" * 100)
print("LOADING ALL CLIENTS FROM corporate_clients_info")
print("=" * 100)

clients_df = spark.sql("""
    SELECT *
    FROM data_solutions_sandbox.corporate_clients_info
""")

total_clients = clients_df.count()
print(f"\nTotal clients in table: {total_clients:,}")

# Show schema to understand available columns
print("\nTable schema:")
clients_df.printSchema()

# Show sample records
print("\nSample records:")
clients_df.select('demo_name', 'client_name', 'sandbox_schema', 'special_attribute_column').show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Filter Clients with Empty special_attribute_column
print("=" * 100)
print("FILTERING CLIENTS WITH EMPTY special_attribute_column")
print("=" * 100)

# Filter for clients where special_attribute_column is null, empty array, or None
clients_with_empty_special_attrs = clients_df.filter(
    F.col('special_attribute_column').isNull() |
    (F.size(F.col('special_attribute_column')) == 0)
)

count_empty = clients_with_empty_special_attrs.count()
print(f"\nClients with NULL or empty special_attribute_column: {count_empty:,}")

print("\nSample clients with empty special_attribute_column:")
clients_with_empty_special_attrs.select(
    'demo_name',
    'client_name',
    'sandbox_schema',
    'special_attribute_column'
).show(20, truncate=False)

# Store for next step
clients_with_empty_special_attrs.createOrReplaceTempView("clients_empty_special_attrs")

# COMMAND ----------

# DBTITLE 1,Function: Check product_attributes Column Validity
def check_product_attributes_validity(demo_name, sandbox_schema):
    """
    Check if a client has a valid product_attributes column:
    1. Column exists
    2. Is variant type
    3. Is NOT 100% null

    Returns dict with status information
    """
    try:
        table_name = f"{sandbox_schema}.{demo_name}_v38_filter_items"

        # Check if table exists
        try:
            df = spark.table(table_name)
        except Exception as e:
            return {
                'demo_name': demo_name,
                'table_exists': False,
                'error': 'Table not found'
            }

        total_records = df.count()

        # Check if product_attributes column exists
        if 'product_attributes' not in df.columns:
            return {
                'demo_name': demo_name,
                'table_exists': True,
                'column_exists': False,
                'total_records': total_records
            }

        # Get data type
        schema_field = [f for f in df.schema.fields if f.name == 'product_attributes'][0]
        data_type = str(schema_field.dataType)

        # Check if it's variant type
        is_variant = 'variant' in data_type.lower()

        # Calculate null percentage
        null_count = df.filter(F.col('product_attributes').isNull()).count()
        null_percentage = (null_count / total_records * 100) if total_records > 0 else 100

        # Is NOT 100% null?
        has_non_null_values = null_percentage < 100

        return {
            'demo_name': demo_name,
            'table_exists': True,
            'column_exists': True,
            'data_type': data_type,
            'is_variant': is_variant,
            'total_records': total_records,
            'null_count': null_count,
            'non_null_count': total_records - null_count,
            'null_percentage': round(null_percentage, 2),
            'has_non_null_values': has_non_null_values,
            'is_valid': is_variant and has_non_null_values
        }

    except Exception as e:
        return {
            'demo_name': demo_name,
            'table_exists': False,
            'error': str(e)
        }

# COMMAND ----------

# DBTITLE 1,Check All Clients for Valid product_attributes
print("=" * 100)
print("CHECKING PRODUCT_ATTRIBUTES VALIDITY FOR ALL CLIENTS")
print("=" * 100)

# Get list of clients with empty special_attribute_column
clients_to_check = clients_with_empty_special_attrs.select('demo_name', 'sandbox_schema').collect()

print(f"\nChecking {len(clients_to_check)} clients...")
print("This may take a few minutes...\n")

# Check each client
results = []
for i, row in enumerate(clients_to_check, 1):
    demo_name = row['demo_name']
    sandbox_schema = row['sandbox_schema']

    if i % 10 == 0:
        print(f"  Progress: {i}/{len(clients_to_check)} clients checked...")

    result = check_product_attributes_validity(demo_name, sandbox_schema)
    results.append(result)

print(f"\n✓ Completed checking {len(results)} clients")

# Convert results to DataFrame
results_df = spark.createDataFrame(results)
results_df.createOrReplaceTempView("product_attrs_check_results")

# COMMAND ----------

# DBTITLE 1,Identify Affected Clients
print("=" * 100)
print("IDENTIFYING AFFECTED CLIENTS")
print("=" * 100)

# Affected clients are those with:
# 1. Empty special_attribute_column (already filtered)
# 2. Valid product_attributes column (variant type, not 100% null)

affected_clients_df = results_df.filter(
    (F.col('is_valid') == True)
)

affected_count = affected_clients_df.count()
print(f"\n🚨 AFFECTED CLIENTS: {affected_count:,}")

if affected_count > 0:
    print("\nList of affected clients:")
    affected_clients_df.select(
        'demo_name',
        'total_records',
        'non_null_count',
        'null_percentage',
        'data_type'
    ).orderBy('demo_name').show(100, truncate=False)

    # Save list for later use
    affected_clients_df.createOrReplaceTempView("affected_clients")
else:
    print("\n✓ No affected clients found")

# COMMAND ----------

# DBTITLE 1,Summary Statistics
print("=" * 100)
print("SUMMARY STATISTICS")
print("=" * 100)

# Overall breakdown
summary_query = """
    SELECT
        CASE
            WHEN table_exists = false THEN 'Table Not Found'
            WHEN column_exists = false THEN 'Column Not Found'
            WHEN is_valid = true THEN 'AFFECTED (Valid product_attributes)'
            WHEN null_percentage = 100 THEN 'product_attributes 100% Null'
            WHEN is_variant = false THEN 'Not Variant Type'
            ELSE 'Other'
        END as status,
        COUNT(*) as client_count
    FROM product_attrs_check_results
    GROUP BY status
    ORDER BY client_count DESC
"""

summary_df = spark.sql(summary_query)

print("\nBreakdown of all clients:")
summary_df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Detailed View: Affected Clients with Sample Data
if affected_count > 0:
    print("=" * 100)
    print("DETAILED VIEW: AFFECTED CLIENTS WITH SAMPLE DATA")
    print("=" * 100)

    affected_list = affected_clients_df.select('demo_name', 'sandbox_schema').collect()

    for i, row in enumerate(affected_list[:5], 1):  # Show first 5 for brevity
        demo_name = row['demo_name']
        sandbox_schema = row['sandbox_schema'] if row['sandbox_schema'] else 'yd_sensitive_corporate'
        table_name = f"{sandbox_schema}.{demo_name}_v38_filter_items"

        print(f"\n{'-'*100}")
        print(f"CLIENT {i}: {demo_name.upper()}")
        print(f"Table: {table_name}")
        print(f"{'-'*100}")

        try:
            df = spark.table(table_name)

            # Show sample product_attributes
            sample_df = df.filter(F.col('product_attributes').isNotNull()) \
                .select('major_cat', 'web_description', 'product_attributes') \
                .limit(3)

            print("\nSample records with product_attributes:")
            sample_df.show(truncate=False)

        except Exception as e:
            print(f"Error: {str(e)}")

    if affected_count > 5:
        print(f"\n... and {affected_count - 5} more affected clients")

# COMMAND ----------

# DBTITLE 1,Export Affected Clients List
print("=" * 100)
print("EXPORTING AFFECTED CLIENTS LIST")
print("=" * 100)

if affected_count > 0:
    # Join with original client info to get full details
    full_affected_info = spark.sql("""
        SELECT
            c.demo_name,
            c.client_name,
            c.sandbox_schema,
            c.special_attribute_column,
            r.total_records,
            r.non_null_count,
            r.null_percentage,
            r.data_type
        FROM clients_empty_special_attrs c
        JOIN affected_clients r ON c.demo_name = r.demo_name
        ORDER BY c.demo_name
    """)

    print(f"\nFull list of {affected_count} affected clients:")
    full_affected_info.show(100, truncate=False)

    # Convert to pandas for export
    affected_pd = full_affected_info.toPandas()

    print("\n✓ Affected clients data ready for export")
    print(f"  Total affected: {len(affected_pd)}")

    # Display summary
    print("\nSummary:")
    print(f"  Total clients checked: {len(clients_to_check)}")
    print(f"  Affected clients: {affected_count}")
    print(f"  Percentage affected: {(affected_count / len(clients_to_check) * 100):.1f}%")

else:
    print("\nNo affected clients to export")

# COMMAND ----------

# DBTITLE 1,Save Results to Table (Optional)
# Uncomment to save results to a table

# output_table = "ydx_internal_analysts_sandbox.major_cat_issue_affected_clients"
#
# full_affected_info.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .saveAsTable(output_table)
#
# print(f"✓ Results saved to {output_table}")

print("\nSkipping table save (uncomment code above to save)")

# COMMAND ----------

# DBTITLE 1,Generate Action Report
if affected_count > 0:
    print("=" * 100)
    print("ACTION REPORT")
    print("=" * 100)

    print(f"""
    📋 AFFECTED CLIENTS IDENTIFIED

    Total Affected: {affected_count}

    These clients have:
    ✓ special_attribute_column is NULL or empty
    ✓ product_attributes column exists (variant type)
    ✓ product_attributes has non-null values

    🚨 IMPACT:
    When export_sku_time_series runs for these clients, it will:
    - Ignore the existing valid product_attributes from _filter_items
    - Create empty/null product_attributes in _sku_time_series
    - OR incorrectly populate product_attributes from other sources

    ⚠️  SUSPECTED ISSUE:
    If these clients show {"major_cat":"..."} in their _sku_time_series tables,
    it confirms the blueprint code is incorrectly creating product_attributes
    instead of using the existing column.

    📝 RECOMMENDED ACTIONS:
    1. Verify _sku_time_series tables for these clients
    2. Check if they have {"major_cat":"..."} in product_attributes
    3. Fix the blueprint code to use existing product_attributes
    4. Rerun export_sku_time_series for all affected clients

    📊 NEXT STEPS:
    - Run notebook: 06__verify_sku_time_series_tables.py
    - Check actual _sku_time_series content for affected clients
    - Confirm major_cat injection
    """)

    print("\n" + "=" * 100)

# COMMAND ----------

# DBTITLE 1,Export Affected Clients List to CSV (for local analysis)
if affected_count > 0:
    print("=" * 100)
    print("PREPARING CSV EXPORT")
    print("=" * 100)

    # Get affected clients list
    affected_clients_list = full_affected_info.toPandas()

    # Display for copying
    print("\nAffected clients (CSV format):")
    print("\ndemo_name,client_name,sandbox_schema,total_records,non_null_product_attrs")

    for _, row in affected_clients_list.iterrows():
        print(f"{row['demo_name']},{row['client_name']},{row['sandbox_schema']},{row['total_records']},{row['non_null_count']}")

    print("\n✓ Copy the above list for use in other notebooks or documentation")
    print("=" * 100)
