# Databricks notebook source
# MAGIC %md
# MAGIC # Investigation: Major Cat as Product Attribute
# MAGIC
# MAGIC **Problem**: Atlas Product Analysis dropdown showing "Major Cat" as an attribute for Weber, Werner, and Odele
# MAGIC
# MAGIC **Objective**: Investigate the `product_attributes` column in `_filter_items` tables to identify the root cause

# COMMAND ----------

# DBTITLE 1,Setup and Imports
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# DBTITLE 1,Function: Get Client Configuration
def get_client_config(demo_name):
    """
    Retrieve client configuration from data_solutions.client_info table
    Returns a dictionary with sandbox_schema and other relevant config
    """
    try:
        # Try to query the client_info table
        client_info_df = spark.sql(f"""
            SELECT *
            FROM data_solutions.client_info
            WHERE demo_name = '{demo_name}'
            OR client_name = '{demo_name}'
            OR LOWER(demo_name) = LOWER('{demo_name}')
            OR LOWER(client_name) = LOWER('{demo_name}')
        """)

        if client_info_df.count() == 0:
            print(f"⚠️  No configuration found for demo_name: {demo_name}")
            print("   Trying alternative table names...")

            # Try alternative table names
            for table_name in ['data_solutions.client_config', 'data_solutions.clients', 'corporate.client_info']:
                try:
                    alt_df = spark.sql(f"SELECT * FROM {table_name} WHERE LOWER(demo_name) = LOWER('{demo_name}') OR LOWER(client_name) = LOWER('{demo_name}')")
                    if alt_df.count() > 0:
                        print(f"✓ Found configuration in {table_name}")
                        client_info_df = alt_df
                        break
                except Exception as e:
                    continue

            if client_info_df.count() == 0:
                return None

        # Convert to dictionary
        config = client_info_df.first().asDict()
        print(f"✓ Configuration found for {demo_name}")
        print(f"  Sandbox Schema: {config.get('sandbox_schema', 'N/A')}")

        return config

    except Exception as e:
        print(f"❌ Error retrieving config for {demo_name}: {str(e)}")
        return None

# COMMAND ----------

# DBTITLE 1,Function: Analyze Product Attributes Column
def analyze_product_attributes(demo_name, config=None):
    """
    Analyze the product_attributes column in _filter_items table

    Returns:
    - Data type
    - Fill rate (% non-null)
    - Distinct value count
    - Top 10 most frequent values with distribution
    """

    # Get config if not provided
    if config is None:
        config = get_client_config(demo_name)
        if config is None:
            print(f"Cannot proceed without configuration for {demo_name}")
            return None

    # Construct table name
    sandbox_schema = config.get('sandbox_schema', 'yd_sensitive_corporate')
    table_name = f"{sandbox_schema}.{demo_name}_v38_filter_items"

    print(f"\n{'='*80}")
    print(f"ANALYZING: {table_name}")
    print(f"{'='*80}\n")

    try:
        # Check if table exists
        table_df = spark.table(table_name)
        total_records = table_df.count()

        print(f"✓ Table found: {table_name}")
        print(f"  Total records: {total_records:,}")

    except Exception as e:
        print(f"❌ Table not found: {table_name}")
        print(f"   Error: {str(e)}")
        return None

    # 1. Check if product_attributes column exists
    columns = table_df.columns
    has_product_attrs = 'product_attributes' in columns

    print(f"\n{'─'*80}")
    print("1. COLUMN EXISTENCE")
    print(f"{'─'*80}")
    print(f"  product_attributes column exists: {has_product_attrs}")

    if not has_product_attrs:
        print("  ⚠️  product_attributes column does not exist in this table")
        return {
            'demo_name': demo_name,
            'table_name': table_name,
            'column_exists': False
        }

    # 2. Get data type
    print(f"\n{'─'*80}")
    print("2. DATA TYPE")
    print(f"{'─'*80}")

    schema_field = [f for f in table_df.schema.fields if f.name == 'product_attributes'][0]
    data_type = str(schema_field.dataType)
    print(f"  Data Type: {data_type}")

    # 3. Calculate fill rate
    print(f"\n{'─'*80}")
    print("3. FILL RATE")
    print(f"{'─'*80}")

    null_count = table_df.filter(F.col('product_attributes').isNull()).count()
    non_null_count = total_records - null_count
    fill_rate = (non_null_count / total_records * 100) if total_records > 0 else 0

    print(f"  Non-null records: {non_null_count:,}")
    print(f"  Null records: {null_count:,}")
    print(f"  Fill rate: {fill_rate:.2f}%")

    # 4. Convert to JSON string for analysis
    print(f"\n{'─'*80}")
    print("4. DISTINCT VALUES COUNT")
    print(f"{'─'*80}")

    # Convert product_attributes to JSON string for comparison
    attrs_as_json = table_df.filter(F.col('product_attributes').isNotNull()) \
        .select(F.to_json('product_attributes').alias('attrs_json'))

    distinct_count = attrs_as_json.select('attrs_json').distinct().count()
    print(f"  Distinct product_attributes values: {distinct_count:,}")

    # 5. Get top 10 most frequent values
    print(f"\n{'─'*80}")
    print("5. TOP 10 MOST FREQUENT VALUES")
    print(f"{'─'*80}")

    top_values = attrs_as_json.groupBy('attrs_json') \
        .agg(F.count('*').alias('count')) \
        .orderBy(F.desc('count')) \
        .limit(10)

    top_values_pd = top_values.toPandas()
    top_values_pd['percentage'] = (top_values_pd['count'] / non_null_count * 100).round(2)

    print(top_values_pd.to_string(index=False))

    # 6. Check for "Major Cat" or similar category fields in the JSON
    print(f"\n{'─'*80}")
    print("6. CHECKING FOR CATEGORY FIELDS IN PRODUCT ATTRIBUTES")
    print(f"{'─'*80}")

    # Look for common category-related keywords in the JSON
    category_keywords = ['major_cat', 'Major Cat', 'major cat', 'category', 'Category']

    for keyword in category_keywords:
        count_with_keyword = attrs_as_json.filter(
            F.col('attrs_json').contains(keyword)
        ).count()

        if count_with_keyword > 0:
            pct = (count_with_keyword / non_null_count * 100) if non_null_count > 0 else 0
            print(f"  ⚠️  Found '{keyword}' in {count_with_keyword:,} records ({pct:.2f}%)")

            # Show sample records with this keyword
            print(f"\n  Sample records containing '{keyword}':")
            sample_records = table_df.filter(F.to_json('product_attributes').contains(keyword)) \
                .select('major_cat', 'web_description', 'product_attributes') \
                .limit(3)
            sample_records.show(truncate=False)
        else:
            print(f"  ✓ No records contain '{keyword}'")

    # 7. Analyze structure of product_attributes
    print(f"\n{'─'*80}")
    print("7. PRODUCT ATTRIBUTES STRUCTURE ANALYSIS")
    print(f"{'─'*80}")

    # Get sample records to understand structure
    sample_df = table_df.filter(F.col('product_attributes').isNotNull()) \
        .select('major_cat', 'sub_cat', 'minor_cat', 'web_description', 'product_attributes') \
        .limit(5)

    print("\n  Sample records:")
    sample_df.show(truncate=False)

    # Try to extract keys from the product_attributes if it's a map/struct
    try:
        if 'map' in data_type.lower() or 'struct' in data_type.lower():
            print("\n  Attempting to extract attribute keys...")

            # For map type, get all keys
            keys_df = table_df.filter(F.col('product_attributes').isNotNull()) \
                .select(F.explode(F.map_keys('product_attributes')).alias('attr_key')) \
                .groupBy('attr_key') \
                .agg(F.count('*').alias('count')) \
                .orderBy(F.desc('count'))

            print("\n  All attribute keys found:")
            keys_df.show(50, truncate=False)
    except Exception as e:
        print(f"  Could not extract keys: {str(e)}")

    print(f"\n{'='*80}")
    print(f"ANALYSIS COMPLETE: {demo_name}")
    print(f"{'='*80}\n")

    return {
        'demo_name': demo_name,
        'table_name': table_name,
        'column_exists': True,
        'data_type': data_type,
        'total_records': total_records,
        'non_null_count': non_null_count,
        'fill_rate': fill_rate,
        'distinct_count': distinct_count,
        'top_values': top_values_pd
    }

# COMMAND ----------

# DBTITLE 1,Investigate Affected Clients
affected_clients = ['weber', 'werner', 'odele']

print("=" * 100)
print("INVESTIGATING AFFECTED CLIENTS")
print("=" * 100)

results = {}

for client in affected_clients:
    print(f"\n\n{'#'*100}")
    print(f"# CLIENT: {client.upper()}")
    print(f"{'#'*100}\n")

    result = analyze_product_attributes(client)
    results[client] = result

# COMMAND ----------

# DBTITLE 1,Investigate Control Group (Unaffected Clients)
# Select some clients that are NOT affected to compare
control_clients = ['homedepot', 'lowes', 'target']  # Adjust based on available clients

print("\n\n")
print("=" * 100)
print("INVESTIGATING CONTROL GROUP (UNAFFECTED CLIENTS)")
print("=" * 100)

control_results = {}

for client in control_clients:
    print(f"\n\n{'#'*100}")
    print(f"# CLIENT: {client.upper()}")
    print(f"{'#'*100}\n")

    result = analyze_product_attributes(client)
    control_results[client] = result

# COMMAND ----------

# DBTITLE 1,Comparison Summary
print("\n\n")
print("=" * 100)
print("COMPARISON SUMMARY: AFFECTED vs CONTROL")
print("=" * 100)

# Create comparison dataframe
comparison_data = []

for client_name, result in {**results, **control_results}.items():
    if result and result.get('column_exists'):
        comparison_data.append({
            'client': client_name,
            'group': 'affected' if client_name in affected_clients else 'control',
            'total_records': result.get('total_records'),
            'fill_rate': f"{result.get('fill_rate', 0):.2f}%",
            'distinct_values': result.get('distinct_count'),
            'data_type': result.get('data_type')
        })

if comparison_data:
    comparison_df = pd.DataFrame(comparison_data)
    print("\n")
    print(comparison_df.to_string(index=False))
else:
    print("No data available for comparison")

# COMMAND ----------

# DBTITLE 1,Next Steps
print("\n\n")
print("=" * 100)
print("RECOMMENDED NEXT STEPS")
print("=" * 100)
print("""
Based on the analysis above:

1. If "Major Cat" or category fields are found in product_attributes:
   - Identify WHERE in the data pipeline this is being added
   - Check the core/setup notebook to see how product_attributes is populated
   - Review source data to see if it's coming from upstream

2. If there are differences between affected and control clients:
   - Compare their configurations in client_info
   - Check if there are different data transformation rules
   - Review any client-specific customizations

3. To fix the issue:
   - Filter out category fields from product_attributes during _filter_items creation
   - Update the data pipeline to exclude major_cat from attribute collections
   - Rerun the affected clients' data exports

4. Document the root cause and solution in analysis.md
""")

print("\n" + "=" * 100)
