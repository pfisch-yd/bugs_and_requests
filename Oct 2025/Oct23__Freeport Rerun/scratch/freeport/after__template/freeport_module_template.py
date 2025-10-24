# Databricks notebook source
"""
FreePort Module Template
========================
This template provides a standardized structure for creating FreePort deliverables.

Two patterns are supported:
1. SIMPLE: Static column selection (e.g., geographic_analysis)
2. DYNAMIC: Dynamic column filtering based on null values (e.g., pro_insights, leakages)
"""

import sys
sys.path.append("/Workspace/Repos/ETL_Production/freeport_service/")

from freeport_databricks.client.v1 import (
    get_or_create_deliverable,
    materialize_deliverable,
    sql_from_query_template,
    df_from_query_template,
    get_or_create_query_template
)

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from yipit_databricks_utils.future import create_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module Configuration
# MAGIC
# MAGIC Define module-specific configurations here

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
            "pattern": "DYNAMIC",
            "query_type": "PARAMETRIZED",
            "exclude_columns": [],
            "description": "PRO insights analysis"
        },

        "category_closure": {
            "table_suffix": "_category_closure",
            "table_nickname": "cclos",
            "pattern": "DYNAMIC",
            "query_type": "PARAMETRIZED",
            "exclude_columns": [],
            "description": "Category closure analysis"
        },

        "leakage_retailer": {
            "table_suffix": "_leakage_retailer",
            "table_nickname": "lret",
            "pattern": "DYNAMIC",
            "query_type": "PARAMETRIZED",
            "exclude_columns": [],
            "description": "Leakage retailer analysis"
        },

        "leakage_users": {
            "table_suffix": "_leakage_users",
            "table_nickname": "luser",
            "pattern": "DYNAMIC",
            "query_type": "PARAMETRIZED",
            "exclude_columns": [],
            "description": "Leakage users analysis"
        },

        "leakage_product": {
            "table_suffix": "_leakage_product",
            "table_nickname": "lprod",
            "pattern": "DYNAMIC",
            "query_type": "PARAMETRIZED",
            "exclude_columns": [],
            "description": "Leakage product analysis"
        },

        "market_share": {
            "table_suffix": "_market_share",
            "table_nickname": "msha",
            "pattern": "DYNAMIC",
            "query_type": "PARAMETRIZED",
            "exclude_columns": [],
            "description": "Market share analysis"
        },

        "sku_analysis": {
            "table_suffix": "_sku_analysis",
            "table_nickname": "sana",
            "pattern": "DYNAMIC",
            "query_type": "PARAMETRIZED",
            "exclude_columns": ["product_attributes"],
            "description": "SKU analysis"
        },

        "sku_detail": {
            "table_suffix": "_sku_detail",
            "table_nickname": "sdet",
            "pattern": "DYNAMIC",
            "query_type": "PARAMETRIZED",
            "exclude_columns": ["ASP"],
            "description": "SKU detail analysis"
        },

        "sku_time_series": {
            "table_suffix": "_sku_time_series",
            "table_nickname": "stim",
            "pattern": "DYNAMIC",
            "query_type": "PARAMETRIZED",
            "exclude_columns": ["product_attributes"],
            "description": "SKU time series analysis"
        }
    }

    return configs.get(module_type)

# COMMAND ----------

def filter_non_null_columns(prod_schema, demo_name, table_suffix, exclude_columns=None):
    """
    Filter columns that have at least one non-null value.

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

    df = spark.sql(f"""
        SELECT *{exclude_clause} FROM {prod_schema}.{demo_name}{table_suffix}
    """)

    all_columns = df.columns
    accepted_columns = []

    for column in all_columns:
        df_check = spark.sql(f"""
            WITH filtered AS (
                SELECT
                    {column} AS important_column
                FROM {prod_schema}.{demo_name}{table_suffix}
                WHERE {column} IS NOT NULL
            )
            SELECT COUNT(important_column) AS count_rows FROM filtered
        """)

        if df_check.collect()[0][0] > 0:
            accepted_columns.append(column)

    return accepted_columns

# COMMAND ----------

def freeport_module(sandbox_schema, prod_schema, demo_name, module_type):
    """
    Generic FreePort module function that handles both SIMPLE and DYNAMIC patterns.

    Args:
        sandbox_schema (str): Sandbox schema name
        prod_schema (str): Production schema name
        demo_name (str): Demo name (with version)
        module_type (str): Type of module (e.g., 'geographic_analysis', 'pro_insights')

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
            # If explicit columns needed, define them here
            raise NotImplementedError("Explicit column selection not implemented for SIMPLE pattern")

    elif pattern == "DYNAMIC":
        # DYNAMIC pattern: Filter columns with non-null values
        accepted_columns = filter_non_null_columns(
            prod_schema,
            demo_name,
            table_suffix,
            exclude_columns
        )

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
    module_name = f"corporate_{table_nickname}_{demo_name}"

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

    # Create deliverable
    deliverable_config = {
        "query_template": query_template["id"],
        "input_tables": [f"{ss_catalog}.{prod_schema}.{demo_name}{table_suffix}"],
        "output_table": f"{fp_catalog}.{prod_schema}.{demo_name}{table_suffix}",
        "description": f"{description} for {demo_name}",
        "product_org": "corporate",
        "allow_major_version": True,
        "allow_minor_version": True,
        "staged_retention_days": 100
    }

    if query_parameters:
        deliverable_config["query_parameters"] = query_parameters

    deliverable = get_or_create_deliverable(
        f"{module_name}_deliverable",
        **deliverable_config
    )

    # Materialize deliverable
    materialize_deliverable(
        deliverable["id"],
        release_on_success=False,
        wait_for_completion=True,
    )

    return deliverable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples

# COMMAND ----------

# Example configuration
sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testesteelauder_v38"

# COMMAND ----------

# Example 1: Geographic Analysis (SIMPLE pattern)
# freeport_module(sandbox_schema, prod_schema, demo_name, "geographic_analysis")

# COMMAND ----------

# Example 2: PRO Insights (DYNAMIC pattern)
# freeport_module(sandbox_schema, prod_schema, demo_name, "pro_insights")

# COMMAND ----------

# Example 3: Category Closure (DYNAMIC pattern)
# freeport_module(sandbox_schema, prod_schema, demo_name, "category_closure")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Execution
# MAGIC
# MAGIC Run multiple modules for a single demo

# COMMAND ----------

def run_all_modules(sandbox_schema, prod_schema, demo_name, module_list=None):
    """
    Run multiple FreePort modules for a demo.

    Args:
        sandbox_schema (str): Sandbox schema name
        prod_schema (str): Production schema name
        demo_name (str): Demo name (with version)
        module_list (list): List of module types to run. If None, runs all.
    """

    all_modules = [
        "geographic_analysis",
        "pro_insights",
        "category_closure",
        "leakage_retailer",
        "leakage_users",
        "leakage_product",
        "market_share",
        "sku_analysis",
        "sku_detail",
        "sku_time_series"
    ]

    modules_to_run = module_list if module_list else all_modules

    results = {}
    for module_type in modules_to_run:
        try:
            print(f"Running {module_type}...")
            deliverable = freeport_module(sandbox_schema, prod_schema, demo_name, module_type)
            results[module_type] = {"status": "success", "deliverable": deliverable}
            print(f"✓ {module_type} completed successfully")
        except Exception as e:
            results[module_type] = {"status": "failed", "error": str(e)}
            print(f"✗ {module_type} failed: {str(e)}")

    return results

# COMMAND ----------

# Example: Run all modules for a demo
# results = run_all_modules(sandbox_schema, prod_schema, demo_name)
