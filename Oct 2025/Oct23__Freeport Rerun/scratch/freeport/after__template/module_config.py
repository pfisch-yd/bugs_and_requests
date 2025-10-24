"""
FreePort Module Configuration
==============================
This file contains all module-specific configurations and customizations.
Use this as a reference to understand the differences between modules.
"""

MODULE_CONFIGURATIONS = {
    # ============================================================================
    # PATTERN: SIMPLE - Static column selection using SELECT *
    # ============================================================================

    "geographic_analysis": {
        "table_suffix": "_geographic_analysis",
        "table_nickname": "geo",
        "pattern": "SIMPLE",
        "query_type": "SELECT_ALL",
        "exclude_columns": [],
        "description": "Geographic analysis for market share",
        "notes": "Uses SELECT * to get all columns from geographic_analysis table",
        "example_file": "Ready__1Geo/after__geo3.py"
    },

    # ============================================================================
    # PATTERN: DYNAMIC - Filters columns with non-null values
    # ============================================================================

    "pro_insights": {
        "table_suffix": "_pro_insights",
        "table_nickname": "pro",
        "pattern": "DYNAMIC",
        "query_type": "PARAMETRIZED",
        "exclude_columns": [],
        "description": "PRO insights analysis",
        "notes": "Dynamically filters columns that have at least one non-null value",
        "example_file": "Ready__2PRO/after__pro.py"
    },

    # ============================================================================
    # LEAKAGE MODULES - All use DYNAMIC pattern with column filtering
    # ============================================================================

    "category_closure": {
        "table_suffix": "_category_closure",
        "table_nickname": "cclos",
        "pattern": "DYNAMIC",
        "query_type": "PARAMETRIZED",
        "exclude_columns": [],
        "description": "Category closure analysis",
        "notes": "Leakage module - filters non-null columns from category_closure table",
        "example_file": "Ready__4leakage/(CClos) after__c clos.py"
    },

    "leakage_retailer": {
        "table_suffix": "_leakage_retailer",
        "table_nickname": "lret",
        "pattern": "DYNAMIC",
        "query_type": "PARAMETRIZED",
        "exclude_columns": [],
        "description": "Leakage retailer analysis",
        "notes": "Leakage module - filters non-null columns from leakage_retailer table",
        "example_file": "Ready__4leakage/(LRetailer) after__l ret.py"
    },

    "leakage_users": {
        "table_suffix": "_leakage_users",
        "table_nickname": "luser",
        "pattern": "DYNAMIC",
        "query_type": "PARAMETRIZED",
        "exclude_columns": [],
        "description": "Leakage users analysis",
        "notes": "Leakage module - filters non-null columns from leakage_users table",
        "example_file": "Ready__4leakage/(LUser) after__l user.py"
    },

    "leakage_product": {
        "table_suffix": "_leakage_product",
        "table_nickname": "lprod",
        "pattern": "DYNAMIC",
        "query_type": "PARAMETRIZED",
        "exclude_columns": [],
        "description": "Leakage product analysis",
        "notes": "Leakage module - filters non-null columns from leakage_product table",
        "example_file": "Ready__4leakage/(Lprod) after__l prod.py"
    },

    "market_share": {
        "table_suffix": "_market_share",
        "table_nickname": "msha",
        "pattern": "DYNAMIC",
        "query_type": "PARAMETRIZED",
        "exclude_columns": [],
        "description": "Market share analysis",
        "notes": "Leakage module - filters non-null columns from market_share table",
        "example_file": "Ready__4leakage/(MShar) after__m share.py"
    },

    # ============================================================================
    # SKU/PRODUCT MODULES - Use DYNAMIC pattern with excluded columns
    # ============================================================================

    "sku_analysis": {
        "table_suffix": "_sku_analysis",
        "table_nickname": "sana",
        "pattern": "DYNAMIC",
        "query_type": "PARAMETRIZED",
        "exclude_columns": ["product_attributes"],
        "description": "SKU analysis",
        "notes": "Excludes 'product_attributes' column, then filters non-null columns",
        "example_file": "Ready__5Product A/(S Ana) after__s ana.py",
        "sql_pattern": "SELECT * EXCEPT (product_attributes)"
    },

    "sku_detail": {
        "table_suffix": "_sku_detail",
        "table_nickname": "sdet",
        "pattern": "DYNAMIC",
        "query_type": "PARAMETRIZED",
        "exclude_columns": ["ASP"],
        "description": "SKU detail analysis",
        "notes": "Excludes 'ASP' column, then filters non-null columns",
        "example_file": "Ready__5Product A/(S DET) after__s DET.py",
        "sql_pattern": "SELECT * EXCEPT (ASP)"
    },

    "sku_time_series": {
        "table_suffix": "_sku_time_series",
        "table_nickname": "stim",
        "pattern": "DYNAMIC",
        "query_type": "PARAMETRIZED",
        "exclude_columns": ["product_attributes"],
        "description": "SKU time series analysis",
        "notes": "Excludes 'product_attributes' column, then filters non-null columns",
        "example_file": "Ready__5Product A/(S Tim) after__s tim.py",
        "sql_pattern": "SELECT * EXCEPT (product_attributes)"
    }
}


def get_module_config(module_type):
    """
    Get configuration for a specific module type.

    Args:
        module_type (str): Module type identifier

    Returns:
        dict: Module configuration or None if not found
    """
    return MODULE_CONFIGURATIONS.get(module_type)


def list_all_modules():
    """
    List all available module types.

    Returns:
        list: List of module type names
    """
    return list(MODULE_CONFIGURATIONS.keys())


def get_modules_by_pattern(pattern):
    """
    Get all modules that use a specific pattern.

    Args:
        pattern (str): Pattern type ('SIMPLE' or 'DYNAMIC')

    Returns:
        list: List of module names using the specified pattern
    """
    return [
        module_name
        for module_name, config in MODULE_CONFIGURATIONS.items()
        if config["pattern"] == pattern
    ]


def print_module_summary():
    """
    Print a summary of all module configurations.
    """
    print("=" * 80)
    print("FREEPORT MODULE CONFIGURATIONS SUMMARY")
    print("=" * 80)
    print()

    patterns = {}
    for module_name, config in MODULE_CONFIGURATIONS.items():
        pattern = config["pattern"]
        if pattern not in patterns:
            patterns[pattern] = []
        patterns[pattern].append(module_name)

    for pattern, modules in patterns.items():
        print(f"\n{pattern} PATTERN ({len(modules)} modules)")
        print("-" * 80)
        for module_name in modules:
            config = MODULE_CONFIGURATIONS[module_name]
            print(f"  • {module_name}")
            print(f"    - Suffix: {config['table_suffix']}")
            print(f"    - Nickname: {config['table_nickname']}")
            if config['exclude_columns']:
                print(f"    - Excludes: {', '.join(config['exclude_columns'])}")
            print(f"    - Description: {config['description']}")
            print()


# ============================================================================
# KEY DIFFERENCES BETWEEN MODULES
# ============================================================================

"""
┌──────────────────────────────────────────────────────────────────────────┐
│ PATTERN 1: SIMPLE (SELECT *)                                              │
├──────────────────────────────────────────────────────────────────────────┤
│ Module: geographic_analysis                                               │
│ - Uses: SELECT * FROM table                                               │
│ - No column filtering                                                     │
│ - No null checking                                                        │
│ - Example: after__geo3.py                                                 │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│ PATTERN 2: DYNAMIC (Column Filtering)                                     │
├──────────────────────────────────────────────────────────────────────────┤
│ Modules: pro_insights, all leakage modules, all SKU modules               │
│ - Step 1: Get all columns from table                                      │
│ - Step 2: Check each column for non-null values                           │
│ - Step 3: Keep only columns with at least 1 non-null value                │
│ - Step 4: Use Jinja2 template to SELECT filtered columns                  │
│ - Query: SELECT {% for column in parameters.columns %}...                 │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│ PATTERN 2b: DYNAMIC with Exclusions                                       │
├──────────────────────────────────────────────────────────────────────────┤
│ Modules: sku_analysis, sku_detail, sku_time_series                        │
│ - Same as PATTERN 2, but excludes specific columns FIRST                  │
│ - sku_analysis: EXCEPT (product_attributes)                               │
│ - sku_detail: EXCEPT (ASP)                                                │
│ - sku_time_series: EXCEPT (product_attributes)                            │
│ - Then applies column filtering logic                                     │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│ NAMING CONVENTIONS                                                         │
├──────────────────────────────────────────────────────────────────────────┤
│ module_name format: corporate_{nickname}_{demo_name}                      │
│                                                                            │
│ Examples:                                                                  │
│   - corporate_geo_testesteelauder_v38                                     │
│   - corporate_pro_testgraco_v38                                           │
│   - corporate_luser_testesteelauder_v38                                   │
│   - corporate_sana_testgraco_v38                                          │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│ IF/ELSE LOGIC FOR PERSONALIZATION                                         │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│ if module == "geographic_analysis":                                       │
│     # Use SIMPLE pattern with SELECT *                                    │
│     query_string = "SELECT * FROM {{ sources[0].full_name }}"             │
│                                                                            │
│ elif module in ["sku_analysis", "sku_time_series"]:                       │
│     # Exclude product_attributes first, then filter columns               │
│     exclude_columns = ["product_attributes"]                              │
│     filtered_columns = filter_non_null_columns(..., exclude_columns)      │
│                                                                            │
│ elif module == "sku_detail":                                              │
│     # Exclude ASP first, then filter columns                              │
│     exclude_columns = ["ASP"]                                             │
│     filtered_columns = filter_non_null_columns(..., exclude_columns)      │
│                                                                            │
│ else:                                                                      │
│     # All other modules: Use DYNAMIC pattern with column filtering        │
│     filtered_columns = filter_non_null_columns(...)                       │
│                                                                            │
└──────────────────────────────────────────────────────────────────────────┘
"""
