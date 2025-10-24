# Databricks notebook source
# MAGIC %md
# MAGIC # Compare Geo Analysis Results
# MAGIC
# MAGIC This notebook compares the DataFrames created by:
# MAGIC - `export_geo_analysis()` (original method)
# MAGIC - `export_geo_analysis__fp()` (Freeport method)
# MAGIC
# MAGIC To ensure both methods produce identical results.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Parameters

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testesteelauder_v38"

# Table names for comparison
original_table = f"{prod_schema}.{demo_name}_geographic_analysis"
freeport_table = f"yd_fp_corporate_staging.{prod_schema}.{demo_name}_geographic_analysis"

print(f"Original table: {original_table}")
print(f"Freeport table: {freeport_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load DataFrames

# COMMAND ----------

# Load original results
try:
    df_original = spark.table(original_table)
    print(f"✅ Original table loaded successfully")
    print(f"   Rows: {df_original.count():,}")
    print(f"   Columns: {len(df_original.columns)}")
except Exception as e:
    print(f"❌ Error loading original table: {e}")
    df_original = None

# COMMAND ----------

# Load Freeport results
try:
    df_freeport = spark.table(freeport_table)
    print(f"✅ Freeport table loaded successfully")
    print(f"   Rows: {df_freeport.count():,}")
    print(f"   Columns: {len(df_freeport.columns)}")
except Exception as e:
    print(f"❌ Error loading Freeport table: {e}")
    df_freeport = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Comparison

# COMMAND ----------

if df_original and df_freeport:
    print("=== SCHEMA COMPARISON ===\n")

    # Get schemas
    schema_original = df_original.schema
    schema_freeport = df_freeport.schema

    # Compare column names
    cols_original = set(df_original.columns)
    cols_freeport = set(df_freeport.columns)

    if cols_original == cols_freeport:
        print("✅ Column names match perfectly!")
        print(f"   Both have {len(cols_original)} columns")
    else:
        print("❌ Column names differ:")
        only_original = cols_original - cols_freeport
        only_freeport = cols_freeport - cols_original

        if only_original:
            print(f"   Only in original: {only_original}")
        if only_freeport:
            print(f"   Only in Freeport: {only_freeport}")

    # Compare data types
    print("\n=== DATA TYPE COMPARISON ===")
    type_differences = []

    for field_orig in schema_original.fields:
        field_fp = next((f for f in schema_freeport.fields if f.name == field_orig.name), None)
        if field_fp:
            if field_orig.dataType != field_fp.dataType:
                type_differences.append((field_orig.name, str(field_orig.dataType), str(field_fp.dataType)))

    if not type_differences:
        print("✅ All data types match!")
    else:
        print("❌ Data type differences found:")
        for col, orig_type, fp_type in type_differences:
            print(f"   {col}: {orig_type} vs {fp_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row Count Comparison

# COMMAND ----------

if df_original and df_freeport:
    print("=== ROW COUNT COMPARISON ===\n")

    count_original = df_original.count()
    count_freeport = df_freeport.count()

    if count_original == count_freeport:
        print(f"✅ Row counts match: {count_original:,} rows")
    else:
        print(f"❌ Row counts differ:")
        print(f"   Original: {count_original:,}")
        print(f"   Freeport: {count_freeport:,}")
        print(f"   Difference: {abs(count_original - count_freeport):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Content Comparison

# COMMAND ----------

if df_original and df_freeport and df_original.count() == df_freeport.count():
    print("=== DATA CONTENT COMPARISON ===\n")

    # Ensure same column order for comparison
    common_cols = sorted(list(set(df_original.columns) & set(df_freeport.columns)))

    if common_cols:
        df_orig_sorted = df_original.select(*common_cols).orderBy(*common_cols)
        df_fp_sorted = df_freeport.select(*common_cols).orderBy(*common_cols)

        # Method 1: Use except to find differences
        diff_orig_minus_fp = df_orig_sorted.exceptAll(df_fp_sorted)
        diff_fp_minus_orig = df_fp_sorted.exceptAll(df_orig_sorted)

        count_diff_1 = diff_orig_minus_fp.count()
        count_diff_2 = diff_fp_minus_orig.count()

        if count_diff_1 == 0 and count_diff_2 == 0:
            print("✅ DATA CONTENT MATCHES PERFECTLY!")
            print("   All rows and values are identical between both tables")
        else:
            print("❌ Data content differences found:")
            print(f"   Rows only in original: {count_diff_1}")
            print(f"   Rows only in Freeport: {count_diff_2}")

            # Show sample differences
            if count_diff_1 > 0:
                print("\n   Sample rows only in original:")
                diff_orig_minus_fp.show(5, truncate=False)

            if count_diff_2 > 0:
                print("\n   Sample rows only in Freeport:")
                diff_fp_minus_orig.show(5, truncate=False)
    else:
        print("❌ No common columns found for comparison")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics Comparison

# COMMAND ----------

if df_original and df_freeport:
    print("=== SUMMARY STATISTICS COMPARISON ===\n")

    # Find numeric columns
    numeric_cols = []
    for col in df_original.columns:
        if col in df_freeport.columns:
            col_type = dict(df_original.dtypes)[col]
            if col_type in ['int', 'bigint', 'float', 'double', 'decimal']:
                numeric_cols.append(col)

    print(f"Comparing statistics for numeric columns: {numeric_cols}\n")

    for col in numeric_cols[:5]:  # Limit to first 5 numeric columns
        print(f"--- {col} ---")

        # Original stats
        orig_stats = df_original.select(
            F.sum(col).alias('sum'),
            F.avg(col).alias('avg'),
            F.min(col).alias('min'),
            F.max(col).alias('max'),
            F.stddev(col).alias('stddev')
        ).collect()[0]

        # Freeport stats
        fp_stats = df_freeport.select(
            F.sum(col).alias('sum'),
            F.avg(col).alias('avg'),
            F.min(col).alias('min'),
            F.max(col).alias('max'),
            F.stddev(col).alias('stddev')
        ).collect()[0]

        # Compare
        stats_match = True
        for stat_name in ['sum', 'avg', 'min', 'max', 'stddev']:
            orig_val = getattr(orig_stats, stat_name)
            fp_val = getattr(fp_stats, stat_name)

            if orig_val != fp_val:
                print(f"   ❌ {stat_name}: {orig_val} vs {fp_val}")
                stats_match = False

        if stats_match:
            print(f"   ✅ All statistics match for {col}")

        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Validation Summary

# COMMAND ----------

print("=" * 50)
print("FINAL VALIDATION SUMMARY")
print("=" * 50)

if not df_original:
    print("❌ CRITICAL: Could not load original table")
elif not df_freeport:
    print("❌ CRITICAL: Could not load Freeport table")
else:
    # Overall assessment
    issues = []

    # Check schemas
    if set(df_original.columns) != set(df_freeport.columns):
        issues.append("Column names differ")

    # Check row counts
    if df_original.count() != df_freeport.count():
        issues.append("Row counts differ")

    if not issues:
        print("🎉 SUCCESS: Both methods produce identical results!")
        print("   ✅ Schema matches")
        print("   ✅ Row counts match")
        print("   ✅ Data content matches")
        print("\n   Migration to Freeport is SAFE to proceed!")
    else:
        print("⚠️  ISSUES FOUND:")
        for issue in issues:
            print(f"   ❌ {issue}")
        print("\n   Review differences before proceeding with migration.")

print("=" * 50)
