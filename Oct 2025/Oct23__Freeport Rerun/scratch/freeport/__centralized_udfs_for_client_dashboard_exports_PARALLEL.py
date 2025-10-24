# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Retail Analytics Platform - Client Dashboard Export Functions (PARALLELIZED VERSION)
# MAGIC
# MAGIC ## 🚀 Quick Start
# MAGIC
# MAGIC This notebook provides centralized functions for generating client dashboard exports WITH PARALLELIZATION.
# MAGIC
# MAGIC **Main Functions:**
# MAGIC - `run_everything_parallel(demo_name)` - Complete dashboard generation with parallel execution
# MAGIC - `run_everything_parallel_internally(demo_name)` - Internal development version with parallelization
# MAGIC
# MAGIC **Key Improvements:**
# MAGIC - ✅ Parallel execution of independent modules
# MAGIC - ✅ Faster overall execution time
# MAGIC - ✅ Thread-safe operations
# MAGIC - ✅ Error handling for individual module failures
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %run "./retail_analytics_platform/all"

# COMMAND ----------

import ast
import calendar
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, lit, lower, to_timestamp
from pyspark.sql.window import Window
from yipit_databricks_utils.helpers.gsheets import read_gsheet
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback

# COMMAND ----------

# MAGIC %run "./version_utils"

# COMMAND ----------

# Display current version information
print("🏷️  Corporate Transformation Blueprints - PARALLEL VERSION - Loaded Successfully!")
print(f"📦 Version: {version_info['semantic']} (Suffix: {version_info['version_suffix']})")
print(f"📅 Release Date: {version_info['release_date']}")
print(f"🏷️  Release: {version_info['release_name']}")
print("=" * 60)

# COMMAND ----------

def normalize_brand_list(value):
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        value = value.strip()
        # Case 1: Looks like a list string (e.g., "['Column A', 'Brand B']")
        if value.startswith("[") and value.endswith("]"):
            try:
                return ast.literal_eval(value)  # Safe conversion from string to list
            except Exception:
                return [value]  # Fallback: wrap as single item
        else:
            return [value]  # Not a list string, just a single brand
    return []  # Handle unexpected cases

# COMMAND ----------

def parse_date(raw_date: str) -> str:
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw_date, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"Date format not recognized: {raw_date}")

# COMMAND ----------

def execute_with_error_handling(func, func_name, *args, **kwargs):
    """
    Wrapper function to execute a function with error handling

    Args:
        func: Function to execute
        func_name: Name of the function (for logging)
        *args: Positional arguments for the function
        **kwargs: Keyword arguments for the function

    Returns:
        tuple: (success: bool, result: any, error: str)
    """
    try:
        print(f"🔄 Starting: {func_name}")
        result = func(*args, **kwargs)
        print(f"✅ Completed: {func_name}")
        return (True, result, None)
    except Exception as e:
        error_msg = f"❌ Error in {func_name}: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        return (False, None, error_msg)

# COMMAND ----------

def run_everything_parallel(demo_name, max_workers=4, mode="NORMAL"):
    """
    Parallelized version of run_everything

    Args:
        demo_name: Client demo name
        max_workers: Maximum number of parallel threads (default: 4)

    Returns:
        dict: Results summary with success/failure status for each module
    """
    df = get_default_gsheet_client().read_sheet(
        "1F8bvKrBdlY0GIm9xAU5v3IM7ERLM6kUdi_wYph9JJds",
        1374540499
    )
    df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "M/d/yyyy H:mm:ss"))
    client_row = df.filter(df[1] == demo_name) \
               .orderBy(col("timestamp").desc()) \
               .limit(1)
    client_row_data = client_row.collect()[0]

    demo_name = client_row_data[1] + version
    dash_display_title = client_row_data[2]
    sandbox_schema = client_row_data[3]
    prod_schema = client_row_data[4]
    source_table = client_row_data[5]
    sample_size_guardrail_threshold = client_row_data[7]

    if pd.isna(client_row_data[6]) is True:
        pro_source_table = "ydx_retail_silver.edison_pro_items"
    else:
        pro_source_table = client_row_data[6]

    brands_display_list = normalize_brand_list(client_row_data[8])
    parent_brand = client_row_data[9]
    product_id = client_row_data[10]
    client_email_distro = client_row_data[11]

    raw_date = client_row_data[12]
    start_date_of_data = parse_date(raw_date)

    category_cols = [
        client_row_data[13],
        client_row_data[14],
        client_row_data[15]]

    if pd.isna(client_row_data[16]) is True:
        special_attribute_column = []
        special_attribute_display = []
    else:
        special_attribute_column = ast.literal_eval(client_row_data[16])
        special_attribute_display = ast.literal_eval(client_row_data[16])

    if pd.isna(client_row_data[17]) is False:
        market_share = "Market Share" in [x.strip() for x in client_row_data[17].split(",")]
        shopper_insights = 'Shopper Insights' in [x.strip() for x in client_row_data[17].split(",")]
        pro_module = "Pro" in [x.strip() for x in client_row_data[17].split(",")]
        pricing_n_promo = 'Pricing & Promo' in [x.strip() for x in client_row_data[17].split(",")]
    else:
        market_share = True
        shopper_insights = True
        pro_module = True
        pricing_n_promo = True

    if mode == "NORMAL":
        print("==> READY FOR OFFICIAL ROLL OUT!!!")
    elif mode == "INTERNAL":
        print("===> TEST MODE")
        sandbox_schema = "ydx_internal_analysts_sandbox"
        prod_schema = "ydx_internal_analysts_gold"
    else:
        print("==>")

    results = {}

    # ============================================
    # PHASE 1: Sequential Prerequisites
    # These must run BEFORE parallelization
    # ============================================
    print("=" * 80)
    print("📋 PHASE 1: Running Sequential Prerequisites")
    print("=" * 80)

    # Step 1: Schema Check (must run first)
    success, _, error = execute_with_error_handling(
        run_export_schema_check,
        "run_export_schema_check",
        source_table,
        sandbox_schema,
        demo_name,
        category_cols,
        product_id
    )
    results["schema_check"] = {"success": success, "error": error}

    if not success:
        print("❌ Schema check failed. Aborting execution.")
        return results

    # Step 2: Prep Filter Items (must run after schema check)
    success, _, error = execute_with_error_handling(
        prep_filter_items,
        "prep_filter_items",
        sandbox_schema,
        demo_name,
        source_table,
        category_cols,
        start_date_of_data,
        special_attribute_column,
        special_attribute_display,
        product_id
    )
    results["prep_filter_items"] = {"success": success, "error": error}

    if not success:
        print("❌ Prep filter items failed. Aborting execution.")
        return results

    # Step 3: Client Specs (must run before other modules)
    success, _, error = execute_with_error_handling(
        run_export_client_specs,
        "run_export_client_specs",
        sandbox_schema,
        prod_schema,
        demo_name,
        brands_display_list,
        parent_brand,
        dash_display_title,
        client_email_distro,
        sample_size_guardrail_threshold
    )
    results["client_specs"] = {"success": success, "error": error}

    # Step 4: Shopper Insights Module (foundational for other modules)
    success, _, error = execute_with_error_handling(
        run_export_shopper_insights_module,
        "run_export_shopper_insights_module",
        sandbox_schema,
        prod_schema,
        demo_name
    )
    results["shopper_insights_module"] = {"success": success, "error": error}

    # ============================================
    # PHASE 2: Parallel Execution of Independent Modules
    # These can run in parallel
    # ============================================
    print("=" * 80)
    print("🚀 PHASE 2: Running Parallel Modules")
    print("=" * 80)

    parallel_tasks = []

    # Collect tasks that can run in parallel
    if market_share:
        parallel_tasks.append({
            "func": run_export_market_share_module,
            "name": "run_export_market_share_module",
            "args": (sandbox_schema, prod_schema, demo_name, special_attribute_column)
        })
        parallel_tasks.append({
            "func": run_export_geo_analysis_module,
            "name": "run_export_geo_analysis_module",
            "args": (sandbox_schema, prod_schema, demo_name)
        })
        parallel_tasks.append({
            "func": run_export_product_analysis_module,
            "name": "run_export_product_analysis_module",
            "args": (sandbox_schema, prod_schema, demo_name, product_id)
        })

    parallel_tasks.append({
        "func": run_tariffs_module,
        "name": "run_tariffs_module",
        "args": (sandbox_schema, prod_schema, demo_name, product_id)
    })

    if shopper_insights:
        parallel_tasks.append({
            "func": run_export_retailer_leakage_module,
            "name": "run_export_retailer_leakage_module",
            "args": (sandbox_schema, prod_schema, demo_name, start_date_of_data)
        })

    if pro_module:
        parallel_tasks.append({
            "func": run_export_pro_insights,
            "name": "run_export_pro_insights",
            "args": (sandbox_schema, prod_schema, demo_name, pro_source_table, start_date_of_data)
        })

    # Execute tasks in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(
                execute_with_error_handling,
                task["func"],
                task["name"],
                *task["args"]
            ): task["name"]
            for task in parallel_tasks
        }

        for future in as_completed(future_to_task):
            task_name = future_to_task[future]
            success, result, error = future.result()
            results[task_name] = {"success": success, "error": error}

    # ============================================
    # PHASE 3: Final Sequential Step
    # ============================================
    print("=" * 80)
    print("📊 PHASE 3: Running Final Metric Save")
    print("=" * 80)

    success, _, error = execute_with_error_handling(
        run_metric_save,
        "run_metric_save",
        demo_name,
        sandbox_schema,
        brands_display_list
    )
    results["metric_save"] = {"success": success, "error": error}

    # ============================================
    # Summary Report
    # ============================================
    print("=" * 80)
    print("📈 EXECUTION SUMMARY")
    print("=" * 80)

    total_tasks = len(results)
    successful_tasks = sum(1 for r in results.values() if r["success"])
    failed_tasks = total_tasks - successful_tasks

    print(f"✅ Successful: {successful_tasks}/{total_tasks}")
    print(f"❌ Failed: {failed_tasks}/{total_tasks}")
    print()

    if failed_tasks > 0:
        print("❌ Failed Modules:")
        for module, result in results.items():
            if not result["success"]:
                print(f"  - {module}")
                if result["error"]:
                    print(f"    Error: {result['error'][:200]}...")

    print("=" * 80)

    return results

# COMMAND ----------

def run_everything_parallel_internally(demo_name, max_workers=4):
    """
    Parallelized version of run_everything_internally (for internal dev environment)

    Args:
        demo_name: Client demo name
        max_workers: Maximum number of parallel threads (default: 4)

    Returns:
        dict: Results summary with success/failure status for each module
    """
    df = get_default_gsheet_client().read_sheet(
        "1F8bvKrBdlY0GIm9xAU5v3IM7ERLM6kUdi_wYph9JJds",
        1374540499
    )
    df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "M/d/yyyy H:mm:ss"))
    client_row = df.filter(df[1] == demo_name) \
               .orderBy(col("timestamp").desc()) \
               .limit(1)
    client_row_data = client_row.collect()[0]

    demo_name = client_row_data[1] + version
    dash_display_title = client_row_data[2]
    sandbox_schema = client_row_data[3]
    prod_schema = client_row_data[4]
    source_table = client_row_data[5]
    sample_size_guardrail_threshold = client_row_data[7]

    if pd.isna(client_row_data[6]) is True:
        pro_source_table = "ydx_retail_silver.edison_pro_items"
    else:
        pro_source_table = client_row_data[6]

    brands_display_list = normalize_brand_list(client_row_data[8])
    parent_brand = client_row_data[9]
    product_id = client_row_data[10]
    client_email_distro = client_row_data[11]

    raw_date = client_row_data[12]
    start_date_of_data = parse_date(raw_date)

    category_cols = [
        client_row_data[13],
        client_row_data[14],
        client_row_data[15]]

    if pd.isna(client_row_data[16]) is True:
        special_attribute_column = []
        special_attribute_display = []
    else:
        special_attribute_column = ast.literal_eval(client_row_data[16])
        special_attribute_display = ast.literal_eval(client_row_data[16])

    if pd.isna(client_row_data[17]) is False:
        market_share = "Market Share" in [x.strip() for x in client_row_data[17].split(",")]
        shopper_insights = 'Shopper Insights' in [x.strip() for x in client_row_data[17].split(",")]
        pro_module = "Pro" in [x.strip() for x in client_row_data[17].split(",")]
        pricing_n_promo = 'Pricing & Promo' in [x.strip() for x in client_row_data[17].split(",")]
    else:
        market_share = True
        shopper_insights = True
        pro_module = True
        pricing_n_promo = True

    # Override for internal dev
    sandbox_schema = "ydx_internal_analysts_sandbox"
    prod_schema = "ydx_internal_analysts_gold"

    results = {}

    # ============================================
    # PHASE 1: Sequential Prerequisites
    # ============================================
    print("=" * 80)
    print("📋 PHASE 1: Running Sequential Prerequisites (Internal Dev)")
    print("=" * 80)

    success, _, error = execute_with_error_handling(
        run_export_schema_check,
        "run_export_schema_check",
        source_table,
        sandbox_schema,
        demo_name,
        category_cols,
        product_id
    )
    results["schema_check"] = {"success": success, "error": error}

    if not success:
        print("❌ Schema check failed. Aborting execution.")
        return results

    success, _, error = execute_with_error_handling(
        prep_filter_items,
        "prep_filter_items",
        sandbox_schema,
        demo_name,
        source_table,
        category_cols,
        start_date_of_data,
        special_attribute_column,
        special_attribute_display,
        product_id
    )
    results["prep_filter_items"] = {"success": success, "error": error}

    if not success:
        print("❌ Prep filter items failed. Aborting execution.")
        return results

    success, _, error = execute_with_error_handling(
        run_export_client_specs,
        "run_export_client_specs",
        sandbox_schema,
        prod_schema,
        demo_name,
        brands_display_list,
        parent_brand,
        dash_display_title,
        client_email_distro,
        sample_size_guardrail_threshold
    )
    results["client_specs"] = {"success": success, "error": error}

    success, _, error = execute_with_error_handling(
        run_export_shopper_insights_module,
        "run_export_shopper_insights_module",
        sandbox_schema,
        prod_schema,
        demo_name
    )
    results["shopper_insights_module"] = {"success": success, "error": error}

    # ============================================
    # PHASE 2: Parallel Execution
    # ============================================
    print("=" * 80)
    print("🚀 PHASE 2: Running Parallel Modules (Internal Dev)")
    print("=" * 80)

    parallel_tasks = []

    if market_share:
        parallel_tasks.append({
            "func": run_export_market_share_module,
            "name": "run_export_market_share_module",
            "args": (sandbox_schema, prod_schema, demo_name, special_attribute_column)
        })
        parallel_tasks.append({
            "func": run_export_geo_analysis_module,
            "name": "run_export_geo_analysis_module",
            "args": (sandbox_schema, prod_schema, demo_name)
        })
        parallel_tasks.append({
            "func": run_export_product_analysis_module,
            "name": "run_export_product_analysis_module",
            "args": (sandbox_schema, prod_schema, demo_name, product_id)
        })

    if shopper_insights:
        parallel_tasks.append({
            "func": run_export_retailer_leakage_module,
            "name": "run_export_retailer_leakage_module",
            "args": (sandbox_schema, prod_schema, demo_name, start_date_of_data)
        })

    if pro_module:
        parallel_tasks.append({
            "func": run_export_pro_insights,
            "name": "run_export_pro_insights",
            "args": (sandbox_schema, prod_schema, demo_name, pro_source_table, start_date_of_data)
        })

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(
                execute_with_error_handling,
                task["func"],
                task["name"],
                *task["args"]
            ): task["name"]
            for task in parallel_tasks
        }

        for future in as_completed(future_to_task):
            task_name = future_to_task[future]
            success, result, error = future.result()
            results[task_name] = {"success": success, "error": error}

    # ============================================
    # PHASE 3: Final Step (Internal uses different metric save)
    # ============================================
    print("=" * 80)
    print("📊 PHASE 3: Running Final Metric Save (Internal Dev)")
    print("=" * 80)

    success, _, error = execute_with_error_handling(
        run_dash_metric_save,
        "run_dash_metric_save",
        demo_name
    )
    results["metric_save"] = {"success": success, "error": error}

    # ============================================
    # Summary Report
    # ============================================
    print("=" * 80)
    print("📈 EXECUTION SUMMARY (Internal Dev)")
    print("=" * 80)

    total_tasks = len(results)
    successful_tasks = sum(1 for r in results.values() if r["success"])
    failed_tasks = total_tasks - successful_tasks

    print(f"✅ Successful: {successful_tasks}/{total_tasks}")
    print(f"❌ Failed: {failed_tasks}/{total_tasks}")
    print()

    if failed_tasks > 0:
        print("❌ Failed Modules:")
        for module, result in results.items():
            if not result["success"]:
                print(f"  - {module}")
                if result["error"]:
                    print(f"    Error: {result['error'][:200]}...")

    print("=" * 80)

    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 1: Run everything in parallel (Production)
# MAGIC ```python
# MAGIC results = run_everything_parallel("clientname")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2: Run everything in parallel (Internal Dev)
# MAGIC ```python
# MAGIC results = run_everything_parallel_internally("clientname")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 3: Run with custom max workers
# MAGIC ```python
# MAGIC # Use 8 parallel threads instead of default 4
# MAGIC results = run_everything_parallel("clientname", max_workers=8)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Comparison
# MAGIC
# MAGIC ### Sequential Execution (Original):
# MAGIC - **Total Time**: ~Sum of all individual module times
# MAGIC - Example: If each of 6 modules takes 5 minutes = **30 minutes total**
# MAGIC
# MAGIC ### Parallel Execution (This Version):
# MAGIC - **Total Time**: ~Time of longest module + overhead
# MAGIC - Example: If longest module takes 8 minutes = **~10 minutes total** (3x faster!)
# MAGIC
# MAGIC ### Best Practices:
# MAGIC 1. **max_workers=4** (default): Good balance for most cases
# MAGIC 2. **max_workers=6-8**: For high-resource clusters with many modules
# MAGIC 3. **max_workers=2-3**: For smaller clusters or testing
# MAGIC
# MAGIC ### Important Notes:
# MAGIC - Sequential prerequisites (schema check, prep_filter_items) still run first
# MAGIC - Only independent modules are parallelized
# MAGIC - Error in one module won't stop others from running
# MAGIC - Full execution summary provided at the end

# COMMAND ----------

def confirm_version():
    print("pll internal freeport")
