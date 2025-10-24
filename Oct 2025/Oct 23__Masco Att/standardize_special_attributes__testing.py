# Databricks notebook source
# MAGIC %md
# MAGIC # Standardize Special Attributes - Fix NULL Values
# MAGIC
# MAGIC This notebook standardizes special attribute columns to prevent NULL values from being
# MAGIC incorrectly displayed as L1 category names in the portal.
# MAGIC
# MAGIC **Issue**: When special attribute columns have NULL values, the portal was displaying
# MAGIC the L1 category name instead of handling the NULL properly.
# MAGIC
# MAGIC **Solution**: Apply COALESCE to replace NULL values with 'N/A' for all special attribute columns.
# MAGIC
# MAGIC **Related**: Slack conversation - Oct 23, 2025

# COMMAND ----------

# MAGIC %run /setup_serverless

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import col
import ast
from datetime import datetime
from yipit_databricks_utils.helpers.gsheets import read_gsheet
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit
from pyspark.sql.functions import lower
import pandas as pd

from datetime import datetime, timedelta
import calendar

import pandas as pd
from pyspark.sql import SparkSession

from pyspark.sql.functions import to_timestamp

from yipit_databricks_utils.future import create_table

# COMMAND ----------

# DBTITLE 1,Standardize Special Attributes Function
def standardize_special_attributes(
    df: DataFrame,
    special_attribute_columns: list = None,
    null_replacement: str = 'N/A'
) -> DataFrame:
    """
    Standardize special attribute columns by replacing NULL values with a specified value.

    This function prevents the issue where NULL attribute values get incorrectly filled
    with L1 category names in the portal display.

    Args:
        df: Input Spark DataFrame
        special_attribute_columns: List of column names to standardize. If None, will attempt
                                   to identify columns based on common patterns.
        null_replacement: Value to use instead of NULL (default: 'N/A')

    Returns:
        DataFrame with standardized special attribute columns

    Example:
        >>> df_standardized = standardize_special_attributes(
        ...     df=filter_items_df,
        ...     special_attribute_columns=['shower_door_type', 'faucet_finish', 'cabinet_style'],
        ...     null_replacement='N/A'
        ... )
    """

    # If no columns specified, try to identify special attribute columns
    if special_attribute_columns is None:
        return df

    # Apply COALESCE to each special attribute column
    print(f"Standardizing {len(special_attribute_columns)} special attribute column(s)...")

    for col in special_attribute_columns:
        if col in df.columns:
            df = df.withColumn(
                col,
                F.coalesce(F.col(col), F.lit(null_replacement))
            )
            print(f"  ✓ Standardized column: {col}")
        else:
            print(f"  ⚠ Warning: Column '{col}' not found in DataFrame. Skipping.")

    return df

# COMMAND ----------

# DBTITLE 1,Validate Standardization
def validate_special_attributes_standardization(
    df: DataFrame,
    special_attribute_columns: list
) -> dict:
    """
    Validate that special attribute columns have been properly standardized.

    Args:
        df: DataFrame to validate
        special_attribute_columns: List of columns to check

    Returns:
        Dictionary with validation results for each column
    """

    validation_results = {}

    print("Validating special attribute standardization...")
    print("=" * 60)

    for col in special_attribute_columns:
        if col not in df.columns:
            validation_results[col] = {"status": "MISSING", "null_count": None}
            print(f"❌ Column '{col}' not found in DataFrame")
            continue

        # Count NULL values
        null_count = df.filter(F.col(col).isNull()).count()
        total_count = df.count()

        if null_count == 0:
            validation_results[col] = {
                "status": "PASS",
                "null_count": 0,
                "total_count": total_count
            }
            print(f"✅ Column '{col}': No NULL values (total rows: {total_count})")
        else:
            validation_results[col] = {
                "status": "FAIL",
                "null_count": null_count,
                "total_count": total_count,
                "null_percentage": round((null_count / total_count) * 100, 2)
            }
            print(f"❌ Column '{col}': {null_count} NULL values found "
                  f"({validation_results[col]['null_percentage']}% of total)")

    print("=" * 60)

    return validation_results

# COMMAND ----------

# DBTITLE 1,Apply Standardization to Filter Items Table
def apply_standardization_to_filter_items(
    schema_name: str,
    table_name: str,
    special_attribute_columns: list = None,
    null_replacement: str = 'N/A',
    overwrite: bool = True,
    validate: bool = True
) -> DataFrame:
    """
    Apply special attribute standardization to an existing filter_items table.

    This function can be run as a post-processing step after the main setup.

    Args:
        schema_name: Schema containing the table
        table_name: Name of the filter_items table
        special_attribute_columns: List of columns to standardize
        null_replacement: Value to replace NULLs with
        overwrite: Whether to overwrite the original table
        validate: Whether to run validation after standardization

    Returns:
        Standardized DataFrame
    """

    print(f"Loading table: {schema_name}.{table_name}")
    df = spark.table(f"{schema_name}.{table_name}")

    print(f"Original row count: {df.count()}")

    # Apply standardization
    df_standardized = standardize_special_attributes(
        df=df,
        special_attribute_columns=special_attribute_columns,
        null_replacement=null_replacement
    )

    # Validate if requested
    if validate and special_attribute_columns:
        validation_results = validate_special_attributes_standardization(
            df=df_standardized,
            special_attribute_columns=special_attribute_columns
        )

    # Overwrite table if requested
    if overwrite:
        print(f"\nOverwriting table: {schema_name}.{table_name}")
        df_standardized.write.mode("overwrite").saveAsTable(f"{schema_name}.{table_name}")
        print("✅ Table successfully updated")

    return df_standardized

# COMMAND ----------

# DBTITLE 1,Example Usage - Standalone Fix
# MAGIC %md
# MAGIC ### Example 1: Fix an existing table
# MAGIC
# MAGIC ```python
# MAGIC # Apply standardization to existing Masco table
# MAGIC df_fixed = apply_standardization_to_filter_items(
# MAGIC     schema_name='ydx_masco_analysts_silver',
# MAGIC     table_name='masco_v38_filter_items',
# MAGIC     special_attribute_columns=['shower_door_type', 'faucet_finish', 'cabinet_hardware'],
# MAGIC     null_replacement='N/A',
# MAGIC     overwrite=True,
# MAGIC     validate=True
# MAGIC )
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Example Usage - Integration with Setup
# MAGIC %md
# MAGIC ### Example 2: Integrate into setup.py workflow
# MAGIC
# MAGIC Add this function call in `prep_filter_items()` after line 393:
# MAGIC
# MAGIC ```python
# MAGIC # In setup.py, after creating filter_items table (line 393)
# MAGIC
# MAGIC # Import the standardization function
# MAGIC from standardize_special_attributes import standardize_special_attributes
# MAGIC
# MAGIC # Apply standardization before returning
# MAGIC adding_special_attribute_column = standardize_special_attributes(
# MAGIC     df=adding_special_attribute_column,
# MAGIC     special_attribute_columns=special_attribute_column,
# MAGIC     null_replacement='N/A'
# MAGIC )
# MAGIC
# MAGIC create_table_with_variant_support(
# MAGIC     module_name,
# MAGIC     sandbox_schema,
# MAGIC     demo_name+'_filter_items',
# MAGIC     adding_special_attribute_column,
# MAGIC     overwrite=True,
# MAGIC     variant_columns=['product_attributes']
# MAGIC )
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Batch Fix for All Client Tables
def fix_all_client_special_attributes(
    schema_pattern: str = 'ydx_%_analysts_silver',
    table_pattern: str = '%_filter_items',
    special_attribute_columns: list = None,
    null_replacement: str = 'N/A',
    dry_run: bool = True
):
    """
    Apply standardization to all client filter_items tables matching a pattern.

    This addresses the concern from the Slack conversation that other clients
    might have the same issue.

    Args:
        schema_pattern: SQL LIKE pattern for schemas to process
        table_pattern: SQL LIKE pattern for tables to process
        special_attribute_columns: List of columns to standardize (if None, auto-detect)
        null_replacement: Value to replace NULLs with
        dry_run: If True, only show what would be fixed without making changes
    """

    print("Searching for tables to fix...")
    print("=" * 80)

    # Find all matching tables
    tables_query = f"""
        SELECT
            table_schema,
            table_name,
            table_catalog
        FROM information_schema.tables
        WHERE
            table_schema LIKE '{schema_pattern}'
            AND table_name LIKE '{table_pattern}'
        ORDER BY table_schema, table_name
    """

    tables_to_fix = spark.sql(tables_query).collect()

    print(f"Found {len(tables_to_fix)} table(s) matching pattern:")
    for row in tables_to_fix:
        print(f"  - {row.table_catalog}.{row.table_schema}.{row.table_name}")

    if dry_run:
        print("\n⚠️  DRY RUN MODE - No changes will be made")
        print("Set dry_run=False to apply changes")
        return

    print("\n" + "=" * 80)
    print("Processing tables...")
    print("=" * 80)

    results = []

    for row in tables_to_fix:
        schema_name = row.table_schema
        table_name = row.table_name
        full_table_name = f"{schema_name}.{table_name}"

        try:
            print(f"\n📋 Processing: {full_table_name}")

            df_fixed = apply_standardization_to_filter_items(
                schema_name=schema_name,
                table_name=table_name,
                special_attribute_columns=special_attribute_columns,
                null_replacement=null_replacement,
                overwrite=True,
                validate=True
            )

            results.append({
                "table": full_table_name,
                "status": "SUCCESS",
                "error": None
            })

        except Exception as e:
            print(f"❌ Error processing {full_table_name}: {str(e)}")
            results.append({
                "table": full_table_name,
                "status": "FAILED",
                "error": str(e)
            })

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    success_count = sum(1 for r in results if r["status"] == "SUCCESS")
    failed_count = sum(1 for r in results if r["status"] == "FAILED")

    print(f"✅ Successfully processed: {success_count}")
    print(f"❌ Failed: {failed_count}")

    if failed_count > 0:
        print("\nFailed tables:")
        for r in results:
            if r["status"] == "FAILED":
                print(f"  - {r['table']}: {r['error']}")

    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Instructions
# MAGIC
# MAGIC ### Option 1: Fix Masco table immediately
# MAGIC ```python
# MAGIC df_fixed = apply_standardization_to_filter_items(
# MAGIC     schema_name='ydx_masco_analysts_silver',
# MAGIC     table_name='masco_v38_filter_items',
# MAGIC     special_attribute_columns=['shower_door_type'],  # Add other columns as needed
# MAGIC     null_replacement='N/A',
# MAGIC     overwrite=True,
# MAGIC     validate=True
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Option 2: Fix all client tables (as suggested by Daniel)
# MAGIC ```python
# MAGIC # First run in dry_run mode to see what would be affected
# MAGIC fix_all_client_special_attributes(
# MAGIC     schema_pattern='ydx_%_analysts_silver',
# MAGIC     table_pattern='%_filter_items',
# MAGIC     null_replacement='N/A',
# MAGIC     dry_run=True
# MAGIC )
# MAGIC
# MAGIC # Then apply changes
# MAGIC results = fix_all_client_special_attributes(
# MAGIC     schema_pattern='ydx_%_analysts_silver',
# MAGIC     table_pattern='%_filter_items',
# MAGIC     null_replacement='N/A',
# MAGIC     dry_run=False
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Option 3: Integrate into setup.py for future pipelines
# MAGIC See "Example Usage - Integration with Setup" section above

# COMMAND ----------

# MAGIC %md
# MAGIC # Let's test this out

# COMMAND ----------

schema_name = "ydx_internal_analysts_sandbox"
demo_name = "masco_v38"
table_suffix = "_filter_items"
table_name=f"{demo_name}{table_suffix}"
special_attribute_columns=['shower_door_type']
null_replacement='N/A'
overwrite=True
validate=True

# COMMAND ----------

apply_standardization_to_filter_items(
    schema_name=schema_name,
    table_name=f"{demo_name}{table_name}",
    special_attribute_columns=['shower_door_type'],  # Add other columns as needed
    null_replacement='N/A',
    overwrite=True,
    validate=True
)

# COMMAND ----------

print(f"Loading table: {schema_name}.{table_name}")
df = spark.table(f"{schema_name}.{table_name}")


# COMMAND ----------

# If no columns specified, try to identify special attribute columns
if special_attribute_columns is None:
    # Common special attribute column patterns
    attribute_patterns = [
        '_type', '_finish', '_style', '_material', '_color',
        '_size', '_feature', '_category', 'attribute'
    ]

    special_attribute_columns = [
        col for col in df.columns
        if any(pattern in col.lower() for pattern in attribute_patterns)
    ]

    if special_attribute_columns:
        print(f"Auto-detected special attribute columns: {special_attribute_columns}")
    else:
        print("No special attribute columns detected. Returning original DataFrame.")
else:
    print("Yes special attribute columns were detected. Continueing...")

# COMMAND ----------





# Apply COALESCE to each special attribute column
print(f"Standardizing {len(special_attribute_columns)} special attribute column(s)...")

for col in special_attribute_columns:
    if col in df.columns:
        df = df.withColumn(
            col,
            F.coalesce(F.col(col), F.lit(null_replacement))
        )
        print(f"  ✓ Standardized column: {col}")
    else:
        print(f"  ⚠ Warning: Column '{col}' not found in DataFrame. Skipping.")

# COMMAND ----------

df_standardized = df

# COMMAND ----------

print(validate, special_attribute_columns)

# COMMAND ----------


validation_results = {}

print("Validating special attribute standardization...")
print("=" * 60)

# COMMAND ----------

for col in special_attribute_columns:
    if col not in df.columns:
        validation_results[col] = {"status": "MISSING", "null_count": None}
        print(f"❌ Column '{col}' not found in DataFrame")
        continue

    # Count NULL values
    null_count = df.filter(F.col(col).isNull()).count()
    total_count = df.count()

    if null_count == 0:
        validation_results[col] = {
            "status": "PASS",
            "null_count": 0,
            "total_count": total_count
        }
        print(f"✅ Column '{col}': No NULL values (total rows: {total_count})")
    else:
        validation_results[col] = {
            "status": "FAIL",
            "null_count": null_count,
            "total_count": total_count,
            "null_percentage": round((null_count / total_count) * 100, 2)
        }
        print(f"❌ Column '{col}': {null_count} NULL values found "
                f"({validation_results[col]['null_percentage']}% of total)")

print("=" * 60)

# COMMAND ----------

for col in special_attribute_columns:
    if col not in df.columns:
        validation_results[col] = {"status": "MISSING", "null_count": None}
        print(f"❌ Column '{col}' not found in DataFrame")
        continue

    # Count NULL values
    null_count = df.filter(F.col(col).isNull()).count()
    total_count = df.count()

    if null_count == 0:
        validation_results[col] = {
            "status": "PASS",
            "null_count": 0,
            "total_count": total_count
        }
        print(f"✅ Column '{col}': No NULL values (total rows: {total_count})")
    else:
        validation_results[col] = {
            "status": "FAIL",
            "null_count": null_count,
            "total_count": total_count,
            "null_percentage": round((null_count / total_count) * 100, 2)
        }
        print(f"❌ Column '{col}': {null_count} NULL values found "
                f"({validation_results[col]['null_percentage']}% of total)")

print("=" * 60)

return validation_results

# COMMAND ----------

# Overwrite table if requested
if overwrite:
    print(f"\nOverwriting table: {schema_name}.{table_name}")
    create_table(schema_name, table_name, df_standardized, overwrite=True)
    print("✅ Table successfully updated")
