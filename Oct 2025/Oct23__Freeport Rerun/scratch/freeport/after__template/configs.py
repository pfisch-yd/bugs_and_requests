"""
FreePort Complete Configuration
================================
Configuration for all FreePort tables and modules.

Note: Tables with LOOP (Market Share variants) are excluded as they require special handling.
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

}



# ============================================================================
# Module Organization Reference
# ============================================================================

"""
FOLDER STRUCTURE:
├── CORE (4 modules)
│   ├── filter_items (sandbox_schema)
│   ├── client_specs (prod_schema)
│   ├── panel_stats (prod_schema)
│   └── sample_size_guardrail (prod_schema)
│
└── ANALYSIS (17 modules)
    ├── GEO (1 module)
    │   └── geographic_analysis
    │
    ├── MARKET SHARE (excluded - requires LOOP handling)
    │   └── Note: market_share_for_column variants not in this config
    │
    ├── PRO INSIGHTS (1 module)
    │   └── pro_insights
    │
    ├── PRODUCT ANALYSIS (3 modules)
    │   ├── sku_analysis
    │   ├── sku_time_series
    │   └── sku_detail
    │
    ├── RET LEAKAGE (5 modules)
    │   ├── leakage_users
    │   ├── leakage_retailer
    │   ├── category_closure
    │   ├── leakage_product
    │   └── market_share
    │
    ├── SHOPPER INSIGHTS (1 module)
    │   └── shopper_filter_items
    │
    └── TARIFFS (3 modules)
        ├── tariffs_month_grouping
        ├── tariffs_month_stable_products_list
        └── tariffs_month_product_filled_prices

TOTAL: 21 modules (4 CORE + 17 ANALYSIS)
Note: Market Share LOOP variants excluded (require special handling)
"""
