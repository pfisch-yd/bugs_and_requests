# Databricks notebook source
"""
FreePort Module Template - OPTIMIZED VERSION
=============================================
This template provides a standardized structure for creating FreePort deliverables.

OPTIMIZATION: Uses a SINGLE query to check all columns instead of N queries (one per column).
This dramatically reduces costs and execution time.
"""

import sys
sys.path.append("/Workspace/Repos/ETL_Production/freeport_service/")

from freeport_databricks.client.v1 import (
    get_or_create_deliverable,
    materialize_deliverable,
    release_materialization,
    sql_from_query_template,
    df_from_query_template,
    get_or_create_query_template
)

from freeport_databricks.client.api_client import (
    get,
    FREEPORT_DOMAIN
)

import json
import time
from yipit_databricks_utils.future import create_table

print(f"Freeport Domain: {FREEPORT_DOMAIN}")

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from yipit_databricks_utils.future import create_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module Configuration

# COMMAND ----------

def get_module_config(module_type):
    """
    Returns configuration for each module type.

    Args:
        module_type (str): Type of module to configure

    Returns:
        dict: Configuration dictionary with module settings
    """

    configs = {
        "geographic_analysis": {
            "table_suffix": "_geographic_analysis",
            "table_nickname": "geo",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "description": "Geographic analysis for market share"
        },

        "pro_insights": {
            "table_suffix": "_pro_insights",
            "table_nickname": "pro",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "description": "PRO insights analysis"
        },

        "category_closure": {
            "table_suffix": "_category_closure",
            "table_nickname": "cclos",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "description": "Category closure analysis"
        },

        "leakage_retailer": {
            "table_suffix": "_leakage_retailer",
            "table_nickname": "lret",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "description": "Leakage retailer analysis"
        },

        "leakage_users": {
            "table_suffix": "_leakage_users",
            "table_nickname": "luser",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "description": "Leakage users analysis"
        },

        "leakage_product": {
            "table_suffix": "_leakage_product",
            "table_nickname": "lprod",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "description": "Leakage product analysis"
        },

        "market_share": {
            "table_suffix": "_market_share",
            "table_nickname": "msha",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "description": "Market share analysis"
        },

        "sku_analysis": {
            "table_suffix": "_sku_analysis",
            "table_nickname": "sana",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "description": "SKU analysis"
        },

        "sku_detail": {
            "table_suffix": "_sku_detail",
            "table_nickname": "sdet",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "description": "SKU detail analysis"
        },

        "sku_time_series": {
            "table_suffix": "_sku_time_series",
            "table_nickname": "stim",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "description": "SKU time series analysis"
        }
    }

    return configs.get(module_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## OPTIMIZED Column Filtering
# MAGIC
# MAGIC ✅ **OPTIMIZATION**: Uses a SINGLE query with COUNT(*) FILTER to check all columns at once
# MAGIC
# MAGIC ### Cost Comparison:
# MAGIC - ❌ **OLD**: N queries (one per column) = N full table scans
# MAGIC - ✅ **NEW**: 1 query checking all columns = 1 full table scan
# MAGIC
# MAGIC ### Example:
# MAGIC - Table with 50 columns and 1B rows
# MAGIC - OLD: 50 full scans = **50x cost**
# MAGIC - NEW: 1 full scan = **1x cost**
# MAGIC - **Savings: 98% reduction in cost!**

# COMMAND ----------

def filter_non_null_columns_optimized(prod_schema, demo_name, table_suffix, exclude_columns=None):
    """
    OPTIMIZED: Filter columns that have at least one non-null value using a SINGLE query.

    Performance improvement:
    - OLD: Executes N queries (one per column) = N full table scans
    - NEW: Executes 1 query checking all columns = 1 full table scan

    Args:
        prod_schema (str): Production schema name
        demo_name (str): Demo name
        table_suffix (str): Table suffix
        exclude_columns (list): Columns to exclude from the selection

    Returns:
        list: List of column names with non-null values
    """
    exclude_columns = exclude_columns or []

    # Build exclude clause for SQL
    exclude_clause = ""
    if exclude_columns:
        exclude_list = ", ".join(exclude_columns)
        exclude_clause = f" EXCEPT ({exclude_list})"

    # Get all columns (excluding specified ones)
    df = spark.sql(f"""
        SELECT *{exclude_clause} FROM {prod_schema}.{demo_name}{table_suffix}
        LIMIT 1
    """)

    all_columns = df.columns

    # ✅ OPTIMIZATION: Single query with COUNT(*) FILTER for all columns
    # Instead of N queries (one per column), we execute ONE query that checks all columns
    count_expressions = []
    for column in all_columns:
        # Use COUNT(*) FILTER for each column
        count_expr = f"COUNT(*) FILTER (WHERE {column} IS NOT NULL) as {column}_count"
        count_expressions.append(count_expr)

    # Build single query with all counts
    count_query = f"""
        SELECT
            {',\n            '.join(count_expressions)}
        FROM {prod_schema}.{demo_name}{table_suffix}
    """

    # Execute single query
    counts_df = spark.sql(count_query)
    counts_row = counts_df.collect()[0]

    # Filter columns with at least one non-null value
    accepted_columns = []
    for i, column in enumerate(all_columns):
        count_value = counts_row[i]
        if count_value > 0:
            accepted_columns.append(column)

    return accepted_columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative Optimization Strategies
# MAGIC
# MAGIC If the single query approach is still too expensive, consider these alternatives:

# COMMAND ----------

def filter_non_null_columns_sampling(prod_schema, demo_name, table_suffix, exclude_columns=None, sample_fraction=0.01):
    """
    ALTERNATIVE OPTIMIZATION: Use table sampling to reduce cost even further.

    This checks non-null values on a SAMPLE of the data instead of the full table.
    Trade-off: Might miss columns that have very few non-null values.

    Args:
        prod_schema (str): Production schema name
        demo_name (str): Demo name
        table_suffix (str): Table suffix
        exclude_columns (list): Columns to exclude from the selection
        sample_fraction (float): Fraction of data to sample (default: 0.01 = 1%)

    Returns:
        list: List of column names with non-null values in the sample
    """
    exclude_columns = exclude_columns or []

    # Build exclude clause for SQL
    exclude_clause = ""
    if exclude_columns:
        exclude_list = ", ".join(exclude_columns)
        exclude_clause = f" EXCEPT ({exclude_list})"

    # Get all columns (excluding specified ones)
    df = spark.sql(f"""
        SELECT *{exclude_clause} FROM {prod_schema}.{demo_name}{table_suffix}
        LIMIT 1
    """)

    all_columns = df.columns

    # Build count expressions with TABLESAMPLE
    count_expressions = []
    for column in all_columns:
        count_expr = f"COUNT(*) FILTER (WHERE {column} IS NOT NULL) as {column}_count"
        count_expressions.append(count_expr)

    # Single query with sampling
    count_query = f"""
        SELECT
            {',\n            '.join(count_expressions)}
        FROM {prod_schema}.{demo_name}{table_suffix}
        TABLESAMPLE ({sample_fraction * 100} PERCENT)
    """

    # Execute single query on sample
    counts_df = spark.sql(count_query)
    counts_row = counts_df.collect()[0]

    # Filter columns with at least one non-null value
    accepted_columns = []
    for i, column in enumerate(all_columns):
        count_value = counts_row[i]
        if count_value > 0:
            accepted_columns.append(column)

    return accepted_columns

# COMMAND ----------

def filter_non_null_columns_metadata(prod_schema, demo_name, table_suffix, exclude_columns=None):
    """
    MOST OPTIMIZED: Use table statistics/metadata if available (zero cost).

    This approach uses Databricks table statistics instead of scanning data.
    Trade-off: Only works if ANALYZE TABLE has been run on the source table.

    Args:
        prod_schema (str): Production schema name
        demo_name (str): Demo name
        table_suffix (str): Table suffix
        exclude_columns (list): Columns to exclude from the selection

    Returns:
        list: List of column names with non-null values based on statistics
    """
    exclude_columns = exclude_columns or []

    full_table_name = f"{prod_schema}.{demo_name}{table_suffix}"

    try:
        # Get column statistics
        stats_df = spark.sql(f"DESCRIBE EXTENDED {full_table_name}")

        # Parse column statistics to find columns with null counts
        # Note: This requires that ANALYZE TABLE has been run
        # ANALYZE TABLE {full_table_name} COMPUTE STATISTICS FOR ALL COLUMNS

        # For now, fall back to optimized method if stats are not available
        print("Warning: Metadata approach requires ANALYZE TABLE. Falling back to optimized method.")
        return filter_non_null_columns_optimized(prod_schema, demo_name, table_suffix, exclude_columns)

    except Exception as e:
        print(f"Could not use metadata approach: {e}. Falling back to optimized method.")
        return filter_non_null_columns_optimized(prod_schema, demo_name, table_suffix, exclude_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main FreePort Module Function

# COMMAND ----------

def freeport_module(sandbox_schema, prod_schema, demo_name, module_type, use_sampling=False, sample_fraction=0.01):
    """
    Generic FreePort module function that handles both SIMPLE and DYNAMIC patterns.

    Args:
        sandbox_schema (str): Sandbox schema name
        prod_schema (str): Production schema name
        demo_name (str): Demo name (with version)
        module_type (str): Type of module (e.g., 'geographic_analysis', 'pro_insights')
        use_sampling (bool): If True, uses sampling optimization (default: False)
        sample_fraction (float): Fraction for sampling if use_sampling=True (default: 0.01)

    Returns:
        dict: Deliverable information
    """

    # Get module configuration
    config = get_module_config(module_type)

    if not config:
        raise ValueError(f"Unknown module type: {module_type}")

    table_suffix = config["table_suffix"]
    table_nickname = config["table_nickname"]
    pattern = config["pattern"]
    query_type = config["query_type"]
    exclude_columns = config["exclude_columns"]
    description = config["description"]

    # Define sources
    sources = [{
        'database_name': prod_schema,
        'table_name': demo_name + table_suffix,
        'catalog_name': "yd_sensitive_corporate",
    }]

    # Apply pattern-specific logic
    if pattern == "SIMPLE":
        # SIMPLE pattern: Use SELECT * or explicit column list
        if query_type == "SELECT_ALL":
            query_string = """
                SELECT *
                FROM {{ sources[0].full_name }}
            """
            query_parameters = None
        else:
            raise NotImplementedError("Explicit column selection not implemented for SIMPLE pattern")

    elif pattern == "DYNAMIC":
        # DYNAMIC pattern: Filter columns with non-null values

        # Choose optimization strategy
        if use_sampling:
            print(f"Using SAMPLING optimization (sample_fraction={sample_fraction})")
            accepted_columns = filter_non_null_columns_sampling(
                prod_schema,
                demo_name,
                table_suffix,
                exclude_columns,
                sample_fraction
            )
        else:
            print(f"Using OPTIMIZED single-query approach")
            accepted_columns = filter_non_null_columns_optimized(
                prod_schema,
                demo_name,
                table_suffix,
                exclude_columns
            )

        print(f"Found {len(accepted_columns)} columns with non-null values")

        query_string = """
            SELECT
            {% for column in parameters.columns %}
            {{ column }}{{ ',' if not loop.last else '' }}
            {% endfor %}
            FROM {{ sources[0].full_name }}
        """

        query_parameters = {"columns": accepted_columns}

    else:
        raise ValueError(f"Unknown pattern: {pattern}")

    # Create module name
    #module_name = f"corporate_{table_nickname}_{demo_name}"
    # @@ SANDBOX NOTATION
    module_name = f"pfis_{table_nickname}_{demo_name}"

    # Create query template
    query_template = get_or_create_query_template(
        slug=module_name,
        query_string=query_string,
        template_description=f"Corporate {description}",
        version_description=f"Production {description} table",
    )

    # Define catalogs
    fp_catalog = "yd_fp_corporate_staging"
    ss_catalog = "yd_sensitive_corporate"

    # @@ SANDBOX NOTATION
    #output_table_name = f"{fp_catalog}.{prod_schema}.{demo_name}{table_suffix}"
    output_table_name = f"{fp_catalog}.{prod_schema}.{demo_name}{table_suffix}1a"

    # Create deliverable
    deliverable_config = {
        "query_template": query_template["id"],
        "input_tables": [f"{ss_catalog}.{prod_schema}.{demo_name}{table_suffix}"],
        "output_table": output_table_name,
        "description": f"{description} for {demo_name}",
        "product_org": "corporate",
        "allow_major_version": True,
        "allow_minor_version": True,
        "staged_retention_days": 100
    }

    #------
    deliverable = get_or_create_deliverable(
        module_name+"_deliverable",
        query_template=query_template["id"],
        input_tables=[f"{ss_catalog}.{prod_schema}.{demo_name}{table_suffix}"],
        output_table=output_table_name,
        query_parameters=query_parameters,
        description=f"{description} for {demo_name}",
        product_org="corporate",
        allow_major_version=True,
        allow_minor_version=True,
        staged_retention_days=100
    )
    #------

    # Materialize deliverable
    materialization = materialize_deliverable(
        deliverable["id"],
        release_on_success=False,
        wait_for_completion=True,
    )

    materialization_id = materialization['id']
    release_response = release_materialization(materialization_id)

    # Assess latest version
    print(f"Waiting for release to complete...")
    while True:
        response = get(f"api/v1/data_model/fp_materialization/{materialization_id}")
        print(response)

        last_fp_release = response.json()["last_fp_release"]
        if last_fp_release:
            if last_fp_release['airflow_status'] == "success":
                print(f"✅ FP Release completed successfully!")
                break
            elif last_fp_release['airflow_status'] == "failed":
                print("❌ FP Release failed!")
                break

            print("Release still running. Waiting 45 seconds...")
            time.sleep(45)
        else:
            print("No releases found. Waiting 45 seconds...")
            time.sleep(45)

    latest_table_name = last_fp_release['view_details']['table_name']
    latest_catalog = last_fp_release['view_details']['catalog_name']
    latest_database = last_fp_release['view_details']['database_name']

    latest_table = f"{latest_catalog}.{latest_database}.{latest_table_name}"
    # print(f"✅ Latest table: {latest_table}")

    view_suffix = "_fp"
    view_name =  f"{ss_catalog}.{prod_schema}.{demo_name}{table_suffix}{view_suffix}"
    view_comment = f"{description} for {demo_name}"

    query_view = f"""
        CREATE OR REPLACE VIEW {view_name}
        COMMENT '{view_comment}'
        AS
        SELECT
            *
        FROM {latest_table}
    """
    spark.sql(query_view)
    # print("✅ Successfully created a view")

    return deliverable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples

# COMMAND ----------

# Example configuration
sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testesteelauder_v38"

module_type = "geographic_analysis"
use_sampling = True
sample_fraction = 0.01

# COMMAND ----------

# Example 1: Geographic Analysis (SIMPLE pattern - no optimization needed)
freeport_module(sandbox_schema, prod_schema, demo_name, "geographic_analysis")

# COMMAND ----------

ss_catalog = "yd_sensitive_corporate"
table_suffix = get_module_config("geographic_analysis")["table_suffix"]
view_suffix = "_fp"

view_name =  f"{ss_catalog}.{prod_schema}.{demo_name}{table_suffix}{view_suffix}"
print(view_name)

# COMMAND ----------

get_module_config("geographic_analysis")["table_suffix"]

# COMMAND ----------

# Example 2: PRO Insights with OPTIMIZED single-query approach (default)
freeport_module(sandbox_schema, prod_schema, demo_name, "pro_insights")

# COMMAND ----------

# Example 3: PRO Insights with SAMPLING optimization (even cheaper, but less precise)
# freeport_module(sandbox_schema, prod_schema, demo_name, "pro_insights", use_sampling=True, sample_fraction=0.01)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Execution

# COMMAND ----------

# Example: Run all modules with optimized approach
# results = run_all_modules(sandbox_schema, prod_schema, demo_name)

# Example: Run all modules with sampling (cheapest option)
# results = run_all_modules(sandbox_schema, prod_schema, demo_name, use_sampling=True, sample_fraction=0.01)
