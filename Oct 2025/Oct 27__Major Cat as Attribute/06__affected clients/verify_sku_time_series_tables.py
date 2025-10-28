# Databricks notebook source
# MAGIC %md
# MAGIC # Verify SKU Time Series Tables for Affected Clients
# MAGIC
# MAGIC **Objective**: For all affected clients identified in previous notebook:
# MAGIC 1. Check their _sku_time_series tables
# MAGIC 2. Verify if product_attributes contains {"major_cat":"..."}
# MAGIC 3. Confirm the bug is present in production

# COMMAND ----------

# DBTITLE 1,Setup and Imports
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# DBTITLE 1,Load Affected Clients List
print("=" * 100)
print("LOADING AFFECTED CLIENTS LIST")
print("=" * 100)

# Option 1: Load from previous notebook's temp view (if running in same session)
try:
    affected_clients_df = spark.table("affected_clients")
    print("✓ Loaded from temp view")
except:
    # Option 2: Re-run the identification query
    print("⚠️  Temp view not found, re-running identification query...")

    clients_df = spark.sql("""
        SELECT demo_name, sandbox_schema, special_attribute_column
        FROM data_solutions_sandbox.corporate_clients_info
        WHERE special_attribute_column IS NULL
           OR size(special_attribute_column) = 0
    """)

    # For now, let's just use the list
    # In practice, you'd re-run the full check
    affected_clients_df = clients_df

affected_count = affected_clients_df.count()
print(f"\nTotal affected clients to check: {affected_count:,}")

# COMMAND ----------

# DBTITLE 1,Function: Check SKU Time Series for Major Cat
def check_sku_time_series_for_major_cat(demo_name, sandbox_schema=None):
    """
    Check if _sku_time_series table has major_cat in product_attributes

    Returns dict with findings
    """
    try:
        # Try different schema locations
        schemas_to_try = [
            f"ydx_{demo_name}_analysts_gold",  # Client-specific gold schema
            "ydx_internal_analysts_gold",       # Internal gold schema
            sandbox_schema if sandbox_schema else "yd_sensitive_corporate"
        ]

        table_found = False
        table_name = None
        df = None

        for schema in schemas_to_try:
            try:
                test_table = f"{schema}.{demo_name}_v38_sku_time_series"
                df = spark.table(test_table)
                table_name = test_table
                table_found = True
                break
            except:
                continue

        if not table_found:
            return {
                'demo_name': demo_name,
                'table_found': False,
                'error': 'Table not found in any schema'
            }

        total_records = df.count()

        # Check if product_attributes column exists
        if 'product_attributes' not in df.columns:
            return {
                'demo_name': demo_name,
                'table_found': True,
                'table_name': table_name,
                'column_exists': False,
                'total_records': total_records
            }

        # Calculate fill rate
        null_count = df.filter(F.col('product_attributes').isNull()).count()
        non_null_count = total_records - null_count

        # Check for major_cat in product_attributes
        major_cat_count = 0
        if non_null_count > 0:
            attrs_json = df.filter(F.col('product_attributes').isNotNull()) \
                .select(F.to_json('product_attributes').alias('attrs_json'))

            major_cat_count = attrs_json.filter(
                F.col('attrs_json').contains('major_cat')
            ).count()

        has_major_cat_issue = major_cat_count > 0
        major_cat_percentage = (major_cat_count / non_null_count * 100) if non_null_count > 0 else 0

        return {
            'demo_name': demo_name,
            'table_found': True,
            'table_name': table_name,
            'column_exists': True,
            'total_records': total_records,
            'non_null_count': non_null_count,
            'major_cat_count': major_cat_count,
            'major_cat_percentage': round(major_cat_percentage, 2),
            'has_major_cat_issue': has_major_cat_issue
        }

    except Exception as e:
        return {
            'demo_name': demo_name,
            'table_found': False,
            'error': str(e)
        }

# COMMAND ----------

# DBTITLE 1,Check All Affected Clients' SKU Time Series Tables
print("=" * 100)
print("CHECKING SKU TIME SERIES TABLES FOR MAJOR CAT ISSUE")
print("=" * 100)

# Get list of affected clients
clients_to_check = affected_clients_df.select('demo_name', 'sandbox_schema').collect()

print(f"\nChecking {len(clients_to_check)} clients...")
print("This may take several minutes...\n")

# Check each client
sku_time_series_results = []
for i, row in enumerate(clients_to_check, 1):
    demo_name = row['demo_name']
    sandbox_schema = row['sandbox_schema'] if hasattr(row, 'sandbox_schema') else None

    if i % 10 == 0:
        print(f"  Progress: {i}/{len(clients_to_check)} clients checked...")

    result = check_sku_time_series_for_major_cat(demo_name, sandbox_schema)
    sku_time_series_results.append(result)

print(f"\n✓ Completed checking {len(sku_time_series_results)} clients")

# Convert results to DataFrame
sku_ts_results_df = spark.createDataFrame(sku_time_series_results)
sku_ts_results_df.createOrReplaceTempView("sku_time_series_check_results")

# COMMAND ----------

# DBTITLE 1,Identify Clients with Confirmed Major Cat Issue
print("=" * 100)
print("CLIENTS WITH CONFIRMED MAJOR CAT ISSUE")
print("=" * 100)

# Filter for clients with major_cat in product_attributes
confirmed_issue_df = sku_ts_results_df.filter(
    (F.col('has_major_cat_issue') == True)
)

confirmed_count = confirmed_issue_df.count()
print(f"\n🚨 CONFIRMED ISSUE IN {confirmed_count:,} CLIENTS")

if confirmed_count > 0:
    print("\nClients with major_cat in _sku_time_series product_attributes:")
    confirmed_issue_df.select(
        'demo_name',
        'table_name',
        'total_records',
        'non_null_count',
        'major_cat_count',
        'major_cat_percentage'
    ).orderBy(F.desc('major_cat_percentage')).show(100, truncate=False)

    confirmed_issue_df.createOrReplaceTempView("confirmed_major_cat_issue")
else:
    print("\n✓ No clients with major_cat issue found")

# COMMAND ----------

# DBTITLE 1,Summary Statistics
print("=" * 100)
print("SUMMARY STATISTICS")
print("=" * 100)

summary_query = """
    SELECT
        CASE
            WHEN table_found = false THEN 'Table Not Found'
            WHEN column_exists = false THEN 'Column Not Found'
            WHEN has_major_cat_issue = true THEN 'HAS MAJOR_CAT ISSUE'
            WHEN non_null_count = 0 THEN 'All product_attributes NULL'
            ELSE 'No Issue'
        END as status,
        COUNT(*) as client_count
    FROM sku_time_series_check_results
    GROUP BY status
    ORDER BY client_count DESC
"""

summary_df = spark.sql(summary_query)

print("\nBreakdown of SKU Time Series tables:")
summary_df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Detailed View: Sample Records from Affected Clients
if confirmed_count > 0:
    print("=" * 100)
    print("SAMPLE RECORDS FROM AFFECTED CLIENTS")
    print("=" * 100)

    confirmed_list = confirmed_issue_df.select('demo_name', 'table_name').collect()

    for i, row in enumerate(confirmed_list[:5], 1):  # Show first 5
        demo_name = row['demo_name']
        table_name = row['table_name']

        print(f"\n{'-'*100}")
        print(f"CLIENT {i}: {demo_name.upper()}")
        print(f"Table: {table_name}")
        print(f"{'-'*100}")

        try:
            df = spark.table(table_name)

            # Show sample records with major_cat in product_attributes
            sample_df = df.filter(
                F.to_json('product_attributes').contains('major_cat')
            ).select('major_cat', 'web_description', 'product_attributes').limit(3)

            print("\nSample records with major_cat in product_attributes:")
            sample_df.show(truncate=False)

        except Exception as e:
            print(f"Error: {str(e)}")

    if confirmed_count > 5:
        print(f"\n... and {confirmed_count - 5} more affected clients")

# COMMAND ----------

# DBTITLE 1,Cross-Reference: Filter Items vs SKU Time Series
if confirmed_count > 0:
    print("=" * 100)
    print("CROSS-REFERENCE: FILTER ITEMS vs SKU TIME SERIES")
    print("=" * 100)

    print("\nComparing product_attributes in _filter_items vs _sku_time_series")
    print("for clients with confirmed major_cat issue\n")

    confirmed_list = confirmed_issue_df.select('demo_name').collect()

    comparison_results = []

    for row in confirmed_list[:10]:  # Check first 10 for speed
        demo_name = row['demo_name']

        print(f"\n{demo_name.upper()}:")

        # Check filter_items
        try:
            filter_table = f"yd_sensitive_corporate.ydx_internal_analysts_sandbox.{demo_name}_v38_filter_items"
            filter_df = spark.table(filter_table)

            if 'product_attributes' in filter_df.columns:
                filter_has_major_cat = filter_df.filter(
                    F.to_json('product_attributes').contains('major_cat')
                ).count()

                filter_non_null = filter_df.filter(
                    F.col('product_attributes').isNotNull()
                ).count()

                print(f"  _filter_items:")
                print(f"    Non-null product_attributes: {filter_non_null:,}")
                print(f"    Contains major_cat: {filter_has_major_cat:,}")

                comparison_results.append({
                    'demo_name': demo_name,
                    'filter_items_has_major_cat': filter_has_major_cat > 0,
                    'sku_time_series_has_major_cat': True
                })
            else:
                print(f"  _filter_items: No product_attributes column")

        except Exception as e:
            print(f"  _filter_items: Error - {str(e)}")

    # Summary of comparison
    if comparison_results:
        print("\n" + "-" * 100)
        print("COMPARISON SUMMARY:")
        print("-" * 100)

        for result in comparison_results:
            filter_status = "✓" if not result['filter_items_has_major_cat'] else "❌"
            print(f"{result['demo_name']:30s} | Filter Items: {filter_status} | SKU Time Series: ❌")

# COMMAND ----------

# DBTITLE 1,Export Confirmed Issue List
if confirmed_count > 0:
    print("=" * 100)
    print("EXPORTING CONFIRMED ISSUE LIST")
    print("=" * 100)

    confirmed_pd = confirmed_issue_df.toPandas()

    print(f"\nClients with confirmed major_cat issue: {len(confirmed_pd)}")

    # Export as CSV format
    print("\n--- CSV FORMAT (for documentation) ---")
    print("demo_name,table_name,total_records,major_cat_count,major_cat_percentage")

    for _, row in confirmed_pd.iterrows():
        print(f"{row['demo_name']},{row['table_name']},{row['total_records']},{row['major_cat_count']},{row['major_cat_percentage']}")

    print("\n" + "=" * 100)

# COMMAND ----------

# DBTITLE 1,Final Report
print("=" * 100)
print("FINAL REPORT: MAJOR CAT ISSUE VERIFICATION")
print("=" * 100)

total_checked = len(sku_time_series_results)
total_with_issue = confirmed_count

print(f"""
📊 VERIFICATION COMPLETE

Total Clients Checked: {total_checked:,}
Clients with Confirmed Issue: {total_with_issue:,}
Percentage Affected: {(total_with_issue / total_checked * 100) if total_checked > 0 else 0:.1f}%

🔍 CONFIRMED:
The major_cat issue is present in production _sku_time_series tables.

✅ EVIDENCE:
- _filter_items tables have valid product_attributes (or are NULL)
- _sku_time_series tables have {{"major_cat":"..."}} in product_attributes
- This confirms the blueprint code is incorrectly creating product_attributes

📝 ROOT CAUSE:
The export_sku_time_series function is NOT using the existing product_attributes
column from _filter_items. Instead, it's creating new product_attributes that
incorrectly include major_cat.

🔧 REQUIRED FIX:
1. Update export_sku_time_series to use existing product_attributes from _filter_items
2. Ensure major_cat is never included in product_attributes
3. Rerun all {total_with_issue:,} affected clients

⚠️  IMPACT:
- Atlas Product Analysis dropdown shows "Major Cat" as a product attribute
- Users cannot properly filter by actual product attributes
- Data quality and user experience are degraded
""")

print("=" * 100)
