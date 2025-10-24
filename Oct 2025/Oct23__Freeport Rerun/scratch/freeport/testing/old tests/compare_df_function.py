# Databricks notebook source
# MAGIC %md
# MAGIC # DataFrame Comparison Function
# MAGIC
# MAGIC Comprehensive function to compare two DataFrames and ensure they are identical:
# MAGIC - Same schema (columns and data types)
# MAGIC - Same row count
# MAGIC - Same data content (every cell value)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

def compare_df(original, freeport_df, show_sample_differences=True, verbose=True):
    """
    Comprehensive DataFrame comparison function

    Parameters:
    -----------
    original : DataFrame
        Original DataFrame to compare against
    freeport_df : DataFrame
        Freeport DataFrame to compare
    show_sample_differences : bool
        Whether to show sample rows that differ (default: True)
    verbose : bool
        Whether to print detailed comparison results (default: True)

    Returns:
    --------
    dict: Comparison results with the following keys:
        - 'identical': bool - True if DataFrames are completely identical
        - 'schema_match': bool - True if schemas match
        - 'row_count_match': bool - True if row counts match
        - 'content_match': bool - True if all cell values match
        - 'column_differences': list - Columns that differ
        - 'type_differences': list - Data type differences
        - 'row_count_original': int - Row count in original
        - 'row_count_freeport': int - Row count in freeport
        - 'content_diff_count': int - Number of differing rows
    """

    results = {
        'identical': False,
        'schema_match': False,
        'row_count_match': False,
        'content_match': False,
        'column_differences': [],
        'type_differences': [],
        'row_count_original': 0,
        'row_count_freeport': 0,
        'content_diff_count': 0
    }

    if verbose:
        print("=" * 60)
        print("DATAFRAME COMPARISON ANALYSIS")
        print("=" * 60)

    # === 1. SCHEMA COMPARISON ===
    if verbose:
        print("\n1. SCHEMA COMPARISON")
        print("-" * 30)

    # Get column names
    original_cols = set(original.columns)
    freeport_cols = set(freeport_df.columns)

    # Check column differences
    only_in_original = original_cols - freeport_cols
    only_in_freeport = freeport_cols - original_cols
    common_cols = original_cols & freeport_cols

    results['column_differences'] = list(only_in_original | only_in_freeport)

    if only_in_original or only_in_freeport:
        results['schema_match'] = False
        if verbose:
            print("❌ Column names differ:")
            if only_in_original:
                print(f"   Only in original: {sorted(only_in_original)}")
            if only_in_freeport:
                print(f"   Only in freeport: {sorted(only_in_freeport)}")
    else:
        if verbose:
            print(f"✅ Column names match ({len(original_cols)} columns)")

    # Check data types for common columns
    original_schema = {field.name: field.dataType for field in original.schema.fields}
    freeport_schema = {field.name: field.dataType for field in freeport_df.schema.fields}

    type_diffs = []
    for col in common_cols:
        if original_schema[col] != freeport_schema[col]:
            type_diffs.append((col, str(original_schema[col]), str(freeport_schema[col])))

    results['type_differences'] = type_diffs

    if type_diffs:
        results['schema_match'] = False
        if verbose:
            print("❌ Data type differences:")
            for col, orig_type, fp_type in type_diffs:
                print(f"   {col}: {orig_type} vs {fp_type}")
    else:
        if not results['column_differences']:
            results['schema_match'] = True
            if verbose:
                print("✅ Data types match for all columns")

    # === 2. ROW COUNT COMPARISON ===
    if verbose:
        print("\n2. ROW COUNT COMPARISON")
        print("-" * 30)

    original_count = original.count()
    freeport_count = freeport_df.count()

    results['row_count_original'] = original_count
    results['row_count_freeport'] = freeport_count

    if original_count == freeport_count:
        results['row_count_match'] = True
        if verbose:
            print(f"✅ Row counts match: {original_count:,} rows")
    else:
        results['row_count_match'] = False
        if verbose:
            print(f"❌ Row counts differ:")
            print(f"   Original: {original_count:,}")
            print(f"   Freeport: {freeport_count:,}")
            print(f"   Difference: {abs(original_count - freeport_count):,}")

    # === 3. CONTENT COMPARISON ===
    if verbose:
        print("\n3. CONTENT COMPARISON")
        print("-" * 30)

    if not common_cols:
        if verbose:
            print("❌ No common columns for content comparison")
        results['content_match'] = False
    elif not results['row_count_match']:
        if verbose:
            print("❌ Cannot compare content - row counts differ")
        results['content_match'] = False
    else:
        # Sort columns for consistent comparison
        sorted_common_cols = sorted(list(common_cols))

        # Ensure same column order and sort by all columns for deterministic comparison
        try:
            original_sorted = original.select(*sorted_common_cols).orderBy(*sorted_common_cols)
            freeport_sorted = freeport_df.select(*sorted_common_cols).orderBy(*sorted_common_cols)

            # Use except to find differences (rows in original but not in freeport)
            diff_original_minus_freeport = original_sorted.exceptAll(freeport_sorted)
            diff_freeport_minus_original = freeport_sorted.exceptAll(original_sorted)

            count_diff_1 = diff_original_minus_freeport.count()
            count_diff_2 = diff_freeport_minus_original.count()

            total_diff_count = count_diff_1 + count_diff_2
            results['content_diff_count'] = total_diff_count

            if total_diff_count == 0:
                results['content_match'] = True
                if verbose:
                    print("✅ All cell values match perfectly!")
            else:
                results['content_match'] = False
                if verbose:
                    print(f"❌ Content differences found:")
                    print(f"   Rows only in original: {count_diff_1:,}")
                    print(f"   Rows only in freeport: {count_diff_2:,}")
                    print(f"   Total differing rows: {total_diff_count:,}")

                    if show_sample_differences:
                        if count_diff_1 > 0:
                            print(f"\n   Sample rows only in original (max 3):")
                            diff_original_minus_freeport.show(3, truncate=True)

                        if count_diff_2 > 0:
                            print(f"\n   Sample rows only in freeport (max 3):")
                            diff_freeport_minus_original.show(3, truncate=True)

        except Exception as e:
            results['content_match'] = False
            if verbose:
                print(f"❌ Error during content comparison: {str(e)}")

    # === 4. FINAL ASSESSMENT ===
    results['identical'] = (
        results['schema_match'] and
        results['row_count_match'] and
        results['content_match']
    )

    if verbose:
        print("\n" + "=" * 60)
        print("FINAL ASSESSMENT")
        print("=" * 60)

        if results['identical']:
            print("🎉 DATAFRAMES ARE IDENTICAL!")
            print("   ✅ Schema matches")
            print("   ✅ Row counts match")
            print("   ✅ All cell values match")
            print("\n   Migration to Freeport is SAFE!")
        else:
            print("⚠️  DATAFRAMES DIFFER:")
            if not results['schema_match']:
                print("   ❌ Schema differences found")
            if not results['row_count_match']:
                print("   ❌ Row count differences found")
            if not results['content_match']:
                print("   ❌ Content differences found")
            print("\n   Review differences before proceeding!")

        print("=" * 60)

    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples

# COMMAND ----------

# Example 1: Basic usage
# results = compare_df(original_df, freeport_df)

# Example 2: Quiet mode (only return results, no printing)
# results = compare_df(original_df, freeport_df, verbose=False)

# Example 3: Don't show sample differences
# results = compare_df(original_df, freeport_df, show_sample_differences=False)

# Example 4: Check if identical programmatically
# results = compare_df(original_df, freeport_df)
# if results['identical']:
#     print("DataFrames are identical - migration is safe!")
# else:
#     print(f"Found {results['content_diff_count']} differing rows")
