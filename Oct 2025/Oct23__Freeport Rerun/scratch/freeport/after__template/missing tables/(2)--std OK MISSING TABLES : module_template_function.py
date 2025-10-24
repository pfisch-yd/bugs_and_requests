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

        # ============================================================================
        # CORE TABLES
        # ============================================================================
        # Foundation tables used across all analysis modules

        "filter_items": {
            "table_suffix": "_filter_items",
            "table_nickname": "fitems",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "sandbox",  # Uses sandbox_schema
            "description": "Filtered items dataset for analysis"
        },

        "client_specs": {
            "table_suffix": "_client_specs",
            "table_nickname": "cspecs",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "Client specifications and configuration"
        },

        "panel_stats": {
            "table_suffix": "_panel_stats",
            "table_nickname": "pstats",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "Panel statistics and metrics"
        },

        "sample_size_guardrail": {
            "table_suffix": "_sample_size_guardrail",
            "table_nickname": "sguard",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "Sample size guardrails and thresholds"
        },


        # ============================================================================
        # ANALYSIS MODULES
        # ============================================================================

        # ----------------------------------------------------------------------------
        # GEO - Geographic Analysis
        # ----------------------------------------------------------------------------

        "geographic_analysis": {
            "table_suffix": "_geographic_analysis",
            "table_nickname": "geo",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "Geographic analysis by state and region"
        },


        # ----------------------------------------------------------------------------
        # MARKET SHARE
        # ----------------------------------------------------------------------------
        # Note: Market Share tables with LOOP are excluded from this config
        # They require special handling for dynamic column generation:
        #   - market_share_for_column (with LOOP)
        # # demo_name+'_market_share_for_column_'+column+'_standard_calendar'
        # demo_name+'_market_share_for_column_'+column+'_nrf_calendar'

        "market_share_for_column": {
            "table_suffix": "_market_share_for_column",
            "table_nickname": "masha",  # Different nickname to avoid collision with core filter_items
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "Market Share for a column"
        },

        "market_share_standard_calendar": {
            "table_suffix": "_market_share_for_column",
            "table_nickname": "mastd",  # Different nickname to avoid collision with core filter_items
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "Market Share for a column"
        },

        "market_share_nrf_calendar": {
            "table_suffix": "_market_share_for_column",
            "table_nickname": "manrf",  # Different nickname to avoid collision with core filter_items
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "Market Share for a column"
        },

        #   - market_share_for_column_nrf (with LOOP)
        #   - market_share_for_column_std (with LOOP)


        # ----------------------------------------------------------------------------
        # PRO INSIGHTS
        # ----------------------------------------------------------------------------

        "pro_insights": {
            "table_suffix": "_pro_insights",
            "table_nickname": "pro",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "PRO insights and advanced analytics"
        },


        # ----------------------------------------------------------------------------
        # PRODUCT ANALYSIS (SKU)
        # ----------------------------------------------------------------------------

        "sku_analysis": {
            "table_suffix": "_sku_analysis",
            "table_nickname": "sana",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "SKU-level product analysis and metrics"
        },

        "sku_time_series": {
            "table_suffix": "_sku_time_series",
            "table_nickname": "stim",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "SKU time series data and trends"
        },

        "sku_detail": {
            "table_suffix": "_sku_detail",
            "table_nickname": "sdet",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "Detailed SKU attributes and information"
        },


        # ----------------------------------------------------------------------------
        # RET LEAKAGE (Retail Leakage Analysis)
        # ----------------------------------------------------------------------------

        "leakage_users": {
            "table_suffix": "_leakage_users",
            "table_nickname": "luser",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "User-level leakage analysis"
        },

        "leakage_retailer": {
            "table_suffix": "_leakage_retailer",
            "table_nickname": "lret",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "Retailer-level leakage metrics"
        },

        "category_closure": {
            "table_suffix": "_category_closure",
            "table_nickname": "cclos",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "Category closure and hierarchy mapping"
        },

        "leakage_product": {
            "table_suffix": "_leakage_product",
            "table_nickname": "lprod",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "Product-level leakage analysis"
        },

        "market_share": {
            "table_suffix": "_market_share",
            "table_nickname": "msha",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "Market share calculations and metrics"
        },


        # ----------------------------------------------------------------------------
        # SHOPPER INSIGHTS
        # ----------------------------------------------------------------------------

        "shopper_filter_items": {
            "table_suffix": "_filter_items",
            "table_nickname": "shopfit",  # Different nickname to avoid collision with core filter_items
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "Shopper insights filtered items dataset"
        },


        # ----------------------------------------------------------------------------
        # TARIFFS
        # ----------------------------------------------------------------------------

        "tariffs_month_grouping": {
            "table_suffix": "_tariffs_month_grouping",
            "table_nickname": "tgroup",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "Monthly tariff groupings and aggregations"
        },

        "tariffs_month_stable_products_list": {
            "table_suffix": "_tariffs_month_stable_products_list",
            "table_nickname": "tstable",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "List of products with stable prices for tariff analysis"
        },

        "tariffs_month_product_filled_prices": {
            "table_suffix": "_tariffs_month_product_filled_prices",
            "table_nickname": "tprices",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "Product prices with filled values for tariff calculations"
        },

        "tariffs_week_grouping": {
            "table_suffix": "_tariffs_week_grouping",
            "table_nickname": "tgroupw",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "Monthly tariff groupings and aggregations"
        },

        "tariffs_week_stable_products_list": {
            "table_suffix": "_tariffs_week_stable_products_list",
            "table_nickname": "tstablew",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "List of products with stable prices for tariff analysis"
        },

        "tariffs_week_product_filled_prices": {
            "table_suffix": "_tariffs_week_product_filled_prices",
            "table_nickname": "tpricesw",
            "pattern": "SIMPLE",
            "query_type": "SELECT_ALL",
            "exclude_columns": [],
            "schema_type": "prod",  # Uses prod_schema
            "description": "Product prices with filled values for tariff calculations"
        },

    }


    return configs.get(module_type)

# COMMAND ----------

def freeport_module(sandbox_schema, prod_schema, demo_name, module_type, use_sampling=False, sample_fraction=0.01, column=None):
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

    # Some folders conventions
    fp_catalog = "yd_fp_corporate_staging"
    ss_catalog = "yd_sensitive_corporate"
    view_suffix = "_fp"

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
    schema_type = config["schema_type"]
    
    if schema_type == "sandbox":
        schema = sandbox_schema
    else:
        schema = prod_schema

    if module_type in ("market_share_standard_calendar", "market_share_nrf_calendar", "market_share_for_column"):
        if module_type == "market_share_for_column_null":
            table_suffix = table_suffix + "_" + column
            table_nickname = table_nickname + column
        elif module_type == "market_share_standard_calendar":
            table_suffix = table_suffix + "_" + column +'_standard_calendar'
            table_nickname = table_nickname + column + '_std'
        elif module_type == "market_share_nrf_calendar":
            table_suffix = table_suffix + "_" + column +'_nrf_calendar'
            table_nickname = table_nickname + column +'_nrf'

    # Create module name
    #module_name = f"corporate_{table_nickname}_{demo_name}"
    # @@ SANDBOX NOTATION
    module_name = f"pfi3_{table_nickname}_{demo_name}"

    # @@ SANDBOX NOTATION
    #output_table_name = f"{fp_catalog}.{schema}.{demo_name}{table_suffix}"
    input_tables_name = f"{ss_catalog}.{schema}.{demo_name}{table_suffix}"
    output_table_name = f"{fp_catalog}.{schema}.{demo_name}{table_suffix}1e"

    # Define sources
    sources = [{
        'database_name': schema,
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
    else:
        raise ValueError(f"Unknown pattern: {pattern}")

    # Create query template
    query_template = get_or_create_query_template(
        slug=module_name,
        query_string=query_string,
        template_description=f"Corporate {description}",
        version_description=f"Production {description} table",
    )

    

    # Create deliverable
    deliverable = get_or_create_deliverable(
        module_name+"_deliverable",
        query_template=query_template["id"],
        input_tables=[input_tables_name],
        output_table=output_table_name,
        query_parameters=query_parameters,
        description=f"{description} for {demo_name}",
        product_org="corporate",
        allow_major_version=True,
        allow_minor_version=True,
        staged_retention_days=100
    )

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

    
    view_name =  f"{fp_catalog}.{schema}.{demo_name}{table_suffix}{view_suffix}"
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

    return deliverable

# COMMAND ----------

demo_name = "testesteelauder_v38"
sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
sol_owner = "pfisch+solowner@gmail.com"
special_attribute_column_original = []
market_share=True
shopper_insights=True
pro_module=True
pricing_n_promo=True
max_workers=4

module_type =  "market_share_standard_calendar"

column = "null"

# COMMAND ----------

freeport_module(sandbox_schema, prod_schema, demo_name, module_type, use_sampling=False, sample_fraction=0.01, column=column)
