# Databricks notebook source
# MAGIC %md
# MAGIC # Verify Results: Analyze Output Tables
# MAGIC
# MAGIC **Purpose**: Use investigation functions from 02__investigating to analyze the output tables
# MAGIC
# MAGIC **Process**:
# MAGIC 1. Import analysis functions from 02__investigating
# MAGIC 2. Run comprehensive analysis on _sku_time_series_test tables
# MAGIC 3. Compare input (_filter_items) vs output (_sku_time_series_test)
# MAGIC 4. Determine root cause

# COMMAND ----------

# DBTITLE 1,Setup and Imports
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.getOrCreate()

# Test catalog and schema
TEST_CATALOG = "ydx_internal_analysts_sandbox"
TEST_SCHEMA = "default"  # Adjust if needed

# COMMAND ----------

# DBTITLE 1,Function: Analyze Product Attributes (From 02__investigating)
def analyze_product_attributes_in_table(table_name, table_label="Table"):
    """
    Comprehensive analysis of product_attributes column in any table
    """
    print(f"\n{'='*80}")
    print(f"ANALYZING: {table_label}")
    print(f"Table: {table_name}")
    print(f"{'='*80}\n")

    try:
        # Load table
        df = spark.table(table_name)
        total_records = df.count()

        print(f"✓ Table found: {table_name}")
        print(f"  Total records: {total_records:,}")

    except Exception as e:
        print(f"❌ Table not found: {table_name}")
        print(f"   Error: {str(e)}")
        return None

    # Check if product_attributes column exists
    columns = df.columns
    has_product_attrs = 'product_attributes' in columns

    print(f"\n{'─'*80}")
    print("1. COLUMN EXISTENCE")
    print(f"{'─'*80}")
    print(f"  product_attributes column exists: {has_product_attrs}")

    if not has_product_attrs:
        print("  ⚠️  product_attributes column does not exist in this table")
        return {
            'table_name': table_name,
            'column_exists': False
        }

    # Get data type
    print(f"\n{'─'*80}")
    print("2. DATA TYPE")
    print(f"{'─'*80}")

    schema_field = [f for f in df.schema.fields if f.name == 'product_attributes'][0]
    data_type = str(schema_field.dataType)
    print(f"  Data Type: {data_type}")

    # Calculate fill rate
    print(f"\n{'─'*80}")
    print("3. FILL RATE")
    print(f"{'─'*80}")

    null_count = df.filter(F.col('product_attributes').isNull()).count()
    non_null_count = total_records - null_count
    fill_rate = (non_null_count / total_records * 100) if total_records > 0 else 0

    print(f"  Non-null records: {non_null_count:,}")
    print(f"  Null records: {null_count:,}")
    print(f"  Fill rate: {fill_rate:.2f}%")

    # Convert to JSON string for analysis
    print(f"\n{'─'*80}")
    print("4. DISTINCT VALUES COUNT")
    print(f"{'─'*80}")

    attrs_as_json = df.filter(F.col('product_attributes').isNotNull()) \
        .select(F.to_json('product_attributes').alias('attrs_json'))

    distinct_count = attrs_as_json.select('attrs_json').distinct().count()
    print(f"  Distinct product_attributes values: {distinct_count:,}")

    # Get top 10 most frequent values
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

    # Check for "Major Cat" or similar category fields
    print(f"\n{'─'*80}")
    print("6. CHECKING FOR CATEGORY FIELDS IN PRODUCT ATTRIBUTES")
    print(f"{'─'*80}")

    category_keywords = ['major_cat', 'Major Cat', 'major cat', 'category', 'Category']

    keywords_found = {}

    for keyword in category_keywords:
        count_with_keyword = attrs_as_json.filter(
            F.col('attrs_json').contains(keyword)
        ).count()

        keywords_found[keyword] = count_with_keyword

        if count_with_keyword > 0:
            pct = (count_with_keyword / non_null_count * 100) if non_null_count > 0 else 0
            print(f"  ⚠️  Found '{keyword}' in {count_with_keyword:,} records ({pct:.2f}%)")
        else:
            print(f"  ✓ No records contain '{keyword}'")

    # Show sample records with category keywords
    if any(count > 0 for count in keywords_found.values()):
        print(f"\n{'─'*80}")
        print("7. SAMPLE RECORDS WITH CATEGORY KEYWORDS")
        print(f"{'─'*80}")

        for keyword in category_keywords:
            if keywords_found[keyword] > 0:
                print(f"\n  Sample records containing '{keyword}':")
                sample_records = df.filter(F.to_json('product_attributes').contains(keyword)) \
                    .select('major_cat', 'sub_cat', 'web_description', 'product_attributes') \
                    .limit(3)
                sample_records.show(truncate=False)

    # Try to extract keys from product_attributes if it's a map/struct
    print(f"\n{'─'*80}")
    print("8. PRODUCT ATTRIBUTES KEYS (if extractable)")
    print(f"{'─'*80}")

    try:
        if 'map' in data_type.lower() or 'variant' in data_type.lower():
            # For variant type, we need to parse differently
            keys_sample = df.filter(F.col('product_attributes').isNotNull()) \
                .select('product_attributes') \
                .limit(5)

            print("\n  Sample product_attributes structures:")
            keys_sample.show(truncate=False)

    except Exception as e:
        print(f"  Could not extract structure: {str(e)}")

    print(f"\n{'='*80}")
    print(f"ANALYSIS COMPLETE: {table_label}")
    print(f"{'='*80}\n")

    return {
        'table_name': table_name,
        'column_exists': True,
        'data_type': data_type,
        'total_records': total_records,
        'non_null_count': non_null_count,
        'fill_rate': fill_rate,
        'distinct_count': distinct_count,
        'keywords_found': keywords_found
    }

# COMMAND ----------

# DBTITLE 1,Client Configuration
clients_config = {
    'weber': {'demo_name': 'weber'},
    'werner': {'demo_name': 'werner'},
    'odele': {'demo_name': 'odele'}
}

# COMMAND ----------

# DBTITLE 1,Analyze Input Tables (_filter_items)
print("\n\n")
print("#" * 100)
print("# PART 1: ANALYZING INPUT TABLES (_filter_items)")
print("#" * 100)

input_results = {}

for client_name, config in clients_config.items():
    demo_name = config['demo_name']
    input_table = f"{TEST_CATALOG}.{TEST_SCHEMA}.{demo_name}_v38_filter_items"

    result = analyze_product_attributes_in_table(
        input_table,
        table_label=f"{client_name.upper()} - INPUT (_filter_items)"
    )

    input_results[client_name] = result

# COMMAND ----------

# DBTITLE 1,Analyze Output Tables (_sku_time_series_test)
print("\n\n")
print("#" * 100)
print("# PART 2: ANALYZING OUTPUT TABLES (_sku_time_series_test)")
print("#" * 100)

output_results = {}

for client_name, config in clients_config.items():
    demo_name = config['demo_name']
    output_table = f"{TEST_CATALOG}.{TEST_SCHEMA}.{demo_name}_sku_time_series_test"

    result = analyze_product_attributes_in_table(
        output_table,
        table_label=f"{client_name.upper()} - OUTPUT (_sku_time_series_test)"
    )

    output_results[client_name] = result

# COMMAND ----------

# DBTITLE 1,Comparison: Input vs Output
print("\n\n")
print("=" * 100)
print("COMPARISON: INPUT (_filter_items) vs OUTPUT (_sku_time_series_test)")
print("=" * 100)

for client_name in clients_config.keys():
    input_result = input_results.get(client_name)
    output_result = output_results.get(client_name)

    print(f"\n{'-'*100}")
    print(f"CLIENT: {client_name.upper()}")
    print(f"{'-'*100}")

    if input_result and output_result:
        if input_result.get('column_exists') and output_result.get('column_exists'):
            # Compare keywords found
            input_keywords = input_result.get('keywords_found', {})
            output_keywords = output_result.get('keywords_found', {})

            print("\nCategory Keywords Comparison:")
            print(f"{'Keyword':<20s} {'Input (_filter_items)':<25s} {'Output (_sku_time_series)':<25s} {'Status':<15s}")
            print("-" * 85)

            all_keywords = set(list(input_keywords.keys()) + list(output_keywords.keys()))

            for keyword in all_keywords:
                input_count = input_keywords.get(keyword, 0)
                output_count = output_keywords.get(keyword, 0)

                if input_count > 0 and output_count > 0:
                    status = "⚠️  PERSISTED"
                elif input_count > 0 and output_count == 0:
                    status = "✓ REMOVED"
                elif input_count == 0 and output_count > 0:
                    status = "❌ ADDED"
                else:
                    status = "✓ CLEAN"

                print(f"{keyword:<20s} {input_count:<25,} {output_count:<25,} {status:<15s}")

            # Fill rate comparison
            print(f"\nFill Rate Comparison:")
            print(f"  Input:  {input_result.get('fill_rate', 0):.2f}%")
            print(f"  Output: {output_result.get('fill_rate', 0):.2f}%")

    else:
        print("  ⚠️  Unable to compare - missing data")

print("\n" + "=" * 100)

# COMMAND ----------

# DBTITLE 1,Root Cause Determination
print("\n\n")
print("=" * 100)
print("ROOT CAUSE ANALYSIS")
print("=" * 100)

conclusions = []

for client_name in clients_config.keys():
    input_result = input_results.get(client_name)
    output_result = output_results.get(client_name)

    if input_result and output_result:
        if input_result.get('column_exists') and output_result.get('column_exists'):
            input_keywords = input_result.get('keywords_found', {})
            output_keywords = output_result.get('keywords_found', {})

            # Check if Major Cat appears in input
            input_has_major_cat = any(
                count > 0 for keyword, count in input_keywords.items()
                if 'major' in keyword.lower() or 'cat' in keyword.lower()
            )

            # Check if Major Cat appears in output
            output_has_major_cat = any(
                count > 0 for keyword, count in output_keywords.items()
                if 'major' in keyword.lower() or 'cat' in keyword.lower()
            )

            if input_has_major_cat and output_has_major_cat:
                conclusion = f"❌ {client_name.upper()}: Major Cat is ALREADY in _filter_items and PERSISTED to output"
                root_cause = "UPSTREAM DATA ISSUE"
            elif not input_has_major_cat and output_has_major_cat:
                conclusion = f"❌ {client_name.upper()}: Major Cat is NOT in _filter_items but ADDED in output"
                root_cause = "BLUEPRINT CODE ISSUE"
            elif input_has_major_cat and not output_has_major_cat:
                conclusion = f"✓ {client_name.upper()}: Major Cat was in _filter_items but REMOVED in output"
                root_cause = "FIXED BY BLUEPRINT"
            else:
                conclusion = f"✓ {client_name.upper()}: No Major Cat in input or output"
                root_cause = "NO ISSUE"

            conclusions.append({
                'client': client_name,
                'conclusion': conclusion,
                'root_cause': root_cause
            })

print("\nFINDINGS:")
print("-" * 100)

for item in conclusions:
    print(f"\n{item['conclusion']}")
    print(f"   Root Cause: {item['root_cause']}")

print("\n" + "=" * 100)

# Determine overall root cause
if conclusions:
    root_causes = [item['root_cause'] for item in conclusions]

    if 'BLUEPRINT CODE ISSUE' in root_causes:
        print("\n🔍 OVERALL ROOT CAUSE: BLUEPRINT CODE ISSUE")
        print("   The product_analysis.py code is ADDING Major Cat to product_attributes")
        print("   Action: Fix the blueprint code to exclude category fields")

    elif 'UPSTREAM DATA ISSUE' in root_causes:
        print("\n🔍 OVERALL ROOT CAUSE: UPSTREAM DATA ISSUE")
        print("   The _filter_items table ALREADY contains Major Cat in product_attributes")
        print("   Action: Fix the core/setup process or manual data modification")

    else:
        print("\n✓ NO SYSTEMATIC ISSUE FOUND")
        print("   Either the issue was already fixed or doesn't exist in test data")

print("\n" + "=" * 100)

# COMMAND ----------

# DBTITLE 1,Detailed Investigation: Extract Actual Attribute Keys
print("\n\n")
print("=" * 100)
print("DETAILED INVESTIGATION: EXTRACTING ACTUAL ATTRIBUTE KEYS")
print("=" * 100)

for client_name, config in clients_config.items():
    demo_name = config['demo_name']

    print(f"\n{'-'*100}")
    print(f"CLIENT: {client_name.upper()}")
    print(f"{'-'*100}")

    # Check both input and output tables
    for table_type, table_suffix in [('INPUT', '_v38_filter_items'), ('OUTPUT', '_sku_time_series_test')]:
        table_name = f"{TEST_CATALOG}.{TEST_SCHEMA}.{demo_name}{table_suffix}"

        print(f"\n{table_type}: {table_name}")

        try:
            df = spark.table(table_name)

            if 'product_attributes' in df.columns:
                # Get records where product_attributes is not null
                sample_df = df.filter(F.col('product_attributes').isNotNull()) \
                    .select(
                        'major_cat',
                        'web_description',
                        F.to_json('product_attributes').alias('product_attributes_json')
                    ) \
                    .limit(5)

                print("\n  Sample records with product_attributes:")
                sample_df.show(truncate=False)

        except Exception as e:
            print(f"  ❌ Error: {str(e)}")

print("\n" + "=" * 100)
