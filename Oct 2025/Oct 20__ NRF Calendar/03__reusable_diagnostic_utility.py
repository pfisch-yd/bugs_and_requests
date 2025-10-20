# Databricks notebook source
# MAGIC %md
# MAGIC # Reusable Diagnostic Utilities for NRF Calendar Issues
# MAGIC
# MAGIC This notebook contains reusable functions to diagnose NRF calendar date mismatches
# MAGIC between client tables.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, lit, when, coalesce
from datetime import datetime
import pandas as pd

# COMMAND ----------

def get_table_metadata(schema, table_name):
    """
    Get blueprints metadata for a given table.

    Args:
        schema: Database schema name
        table_name: Table name

    Returns:
        dict with version, commit_hash, created_by
    """
    try:
        props_df = spark.sql(f"SHOW TBLPROPERTIES {schema}.{table_name}")
        props_dict = {row['key']: row['value'] for row in props_df.collect()}

        metadata = {
            'schema': schema,
            'table_name': table_name,
            'blueprints_version': props_dict.get('blueprints.version'),
            'commit_hash': props_dict.get('blueprints.commit_hash'),
            'created_by': props_dict.get('blueprints.created_by'),
            'has_metadata': 'blueprints.commit_hash' in props_dict
        }

        return metadata
    except Exception as e:
        return {
            'schema': schema,
            'table_name': table_name,
            'error': str(e),
            'has_metadata': False
        }

# COMMAND ----------

def get_table_update_timestamp(schema, table_name):
    """
    Get the last update timestamp from a table's _updated_timestamp column.

    Args:
        schema: Database schema name
        table_name: Table name

    Returns:
        datetime or None
    """
    try:
        query = f"""
        SELECT MAX(_updated_timestamp) as last_update
        FROM {schema}.{table_name}
        """
        result = spark.sql(query).collect()[0]
        return result['last_update']
    except Exception as e:
        print(f"Error getting timestamp for {schema}.{table_name}: {e}")
        return None

# COMMAND ----------

def get_execution_history(client_name, days_back=90):
    """
    Get run_everything() execution history for a client.

    Args:
        client_name: Client demo_name
        days_back: How many days back to look

    Returns:
        DataFrame with execution history
    """
    query = f"""
    SELECT
        get_json_object(args, '$[0]') AS client,
        end_timestamp,
        user,
        notebook_path,
        error,
        CASE WHEN error IS NULL THEN 'SUCCESS' ELSE 'FAILED' END as status
    FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
    WHERE name LIKE 'run_everything%'
        AND get_json_object(args, '$[0]') = '{client_name}'
        AND end_timestamp > CURRENT_DATE() - INTERVAL {days_back} DAY
    ORDER BY end_timestamp DESC
    """
    return spark.sql(query)

# COMMAND ----------

def get_nrf_calendar_history(limit=20):
    """
    Get update history of the NRF calendar table.

    Args:
        limit: Number of versions to retrieve

    Returns:
        DataFrame with version history
    """
    query = f"""
    DESCRIBE HISTORY yd_sensitive_corporate.ydx_internal_analysts_gold.454cal_added_columns
    ORDER BY version DESC
    LIMIT {limit}
    """
    return spark.sql(query)

# COMMAND ----------

def get_nrf_dates_for_period(nrf_month_start):
    """
    Get NRF calendar mapping for a specific month period.

    Args:
        nrf_month_start: Start date of NRF month (e.g., '2025-08-31')

    Returns:
        DataFrame with calendar mapping
    """
    query = f"""
    SELECT
        cal_day,
        nrf_month_for_trailing,
        nrf_month_start,
        nrf_month_end,
        nrf_month_number,
        nrf_quarter_rank,
        nrf_year
    FROM yd_sensitive_corporate.ydx_internal_analysts_gold.454cal_added_columns
    WHERE nrf_month_start = '{nrf_month_start}'
    ORDER BY cal_day
    """
    return spark.sql(query)

# COMMAND ----------

def get_table_dates_for_period(schema, table_name, month_start):
    """
    Get distinct date values from a market_share table for a specific month.

    Args:
        schema: Database schema
        table_name: Table name
        month_start: Month start date to filter on

    Returns:
        DataFrame with date, month_start, month_end, month
    """
    query = f"""
    SELECT DISTINCT
        date,
        month_start,
        month_end,
        month,
        quarter,
        year
    FROM {schema}.{table_name}
    WHERE month_start = '{month_start}'
    ORDER BY date
    """
    return spark.sql(query)

# COMMAND ----------

def compare_client_metadata(client1_info, client2_info):
    """
    Compare metadata between two clients.

    Args:
        client1_info: dict from get_table_metadata()
        client2_info: dict from get_table_metadata()

    Returns:
        dict with comparison results
    """
    comparison = {
        'clients': f"{client1_info.get('table_name', 'client1')} vs {client2_info.get('table_name', 'client2')}",
        'same_version': client1_info.get('blueprints_version') == client2_info.get('blueprints_version'),
        'same_commit': client1_info.get('commit_hash') == client2_info.get('commit_hash'),
        'both_have_metadata': client1_info.get('has_metadata') and client2_info.get('has_metadata'),
        'client1_version': client1_info.get('blueprints_version'),
        'client2_version': client2_info.get('blueprints_version'),
        'client1_commit': client1_info.get('commit_hash'),
        'client2_commit': client2_info.get('commit_hash')
    }

    # Determine likely cause
    if not comparison['both_have_metadata']:
        comparison['likely_cause'] = 'One table not created by blueprints (Hypothesis 1)'
    elif not comparison['same_commit']:
        comparison['likely_cause'] = 'Different Git commits used (Hypothesis 2)'
    else:
        comparison['likely_cause'] = 'NRF calendar updated between runs (Hypothesis 3)'

    return comparison

# COMMAND ----------

def diagnose_nrf_calendar_mismatch(
    client1_name,
    client1_schema,
    client1_table,
    client2_name,
    client2_schema,
    client2_table,
    nrf_month_start,
    save_to_csv=True,
    output_path="/dbfs/tmp/nrf_calendar_diagnosis.csv"
):
    """
    Complete diagnostic function to investigate NRF calendar date mismatches.

    Args:
        client1_name: First client name
        client1_schema: First client schema
        client1_table: First client table name
        client2_name: Second client name
        client2_schema: Second client schema
        client2_table: Second client table name
        nrf_month_start: NRF month start date to investigate (e.g., '2025-08-31')
        save_to_csv: Whether to save results to CSV
        output_path: Path to save CSV file

    Returns:
        dict with diagnosis results
    """
    print(f"🔍 Diagnosing NRF calendar mismatch between {client1_name} and {client2_name}...")
    print("=" * 80)

    diagnosis = {
        'timestamp': datetime.now(),
        'client1': client1_name,
        'client2': client2_name,
        'nrf_month_start': nrf_month_start
    }

    # Phase 1: Get metadata
    print("\n📋 Phase 1: Checking table metadata...")
    client1_metadata = get_table_metadata(client1_schema, client1_table)
    client2_metadata = get_table_metadata(client2_schema, client2_table)

    comparison = compare_client_metadata(client1_metadata, client2_metadata)
    diagnosis.update(comparison)

    print(f"  Client 1 version: {client1_metadata.get('blueprints_version')}")
    print(f"  Client 2 version: {client2_metadata.get('blueprints_version')}")
    print(f"  Client 1 commit: {client1_metadata.get('commit_hash')}")
    print(f"  Client 2 commit: {client2_metadata.get('commit_hash')}")
    print(f"  Commits match: {comparison['same_commit']}")

    # Phase 2: Get execution times
    print("\n⏰ Phase 2: Checking execution times...")
    client1_timestamp = get_table_update_timestamp(client1_schema, client1_table)
    client2_timestamp = get_table_update_timestamp(client2_schema, client2_table)

    diagnosis['client1_updated'] = client1_timestamp
    diagnosis['client2_updated'] = client2_timestamp

    if client1_timestamp and client2_timestamp:
        time_diff = abs((client1_timestamp - client2_timestamp).total_seconds() / 3600)
        diagnosis['hours_between_updates'] = time_diff
        print(f"  Client 1 last updated: {client1_timestamp}")
        print(f"  Client 2 last updated: {client2_timestamp}")
        print(f"  Time difference: {time_diff:.1f} hours")

    # Phase 3: Check NRF calendar
    print("\n📅 Phase 3: Checking NRF calendar mapping...")
    nrf_dates = get_nrf_dates_for_period(nrf_month_start)
    nrf_dates_pd = nrf_dates.toPandas()

    unique_trailing_months = nrf_dates_pd['nrf_month_for_trailing'].nunique()
    diagnosis['unique_nrf_trailing_months'] = unique_trailing_months

    print(f"  Days in NRF month: {len(nrf_dates_pd)}")
    print(f"  Unique nrf_month_for_trailing values: {unique_trailing_months}")

    if unique_trailing_months > 1:
        print(f"  ⚠️  WARNING: Multiple trailing month values found!")
        print(f"     Values: {nrf_dates_pd['nrf_month_for_trailing'].unique()}")
        diagnosis['calendar_issue'] = True
    else:
        print(f"  ✓ Calendar mapping consistent")
        diagnosis['calendar_issue'] = False

    # Phase 4: Check actual table dates
    print("\n📊 Phase 4: Comparing table date values...")
    client1_dates = get_table_dates_for_period(client1_schema, client1_table, nrf_month_start)
    client2_dates = get_table_dates_for_period(client2_schema, client2_table, nrf_month_start)

    client1_dates_pd = client1_dates.toPandas()
    client2_dates_pd = client2_dates.toPandas()

    if not client1_dates_pd.empty and not client2_dates_pd.empty:
        client1_date = client1_dates_pd['date'].iloc[0]
        client2_date = client2_dates_pd['date'].iloc[0]

        diagnosis['client1_date'] = str(client1_date)
        diagnosis['client2_date'] = str(client2_date)
        diagnosis['dates_match'] = client1_date == client2_date

        print(f"  Client 1 date: {client1_date}")
        print(f"  Client 2 date: {client2_date}")
        print(f"  Dates match: {diagnosis['dates_match']}")

        if not diagnosis['dates_match']:
            print(f"  ❌ MISMATCH CONFIRMED")

    # Final diagnosis
    print("\n" + "=" * 80)
    print("🎯 DIAGNOSIS:")
    print("=" * 80)
    print(f"Likely cause: {diagnosis['likely_cause']}")
    print()

    # Recommendations
    print("💡 RECOMMENDED ACTIONS:")
    if not comparison['both_have_metadata']:
        print("  1. One table not created by blueprints")
        print("  2. Re-run both clients using current blueprints code")
        print(f"     run_everything('{client1_name}')")
        print(f"     run_everything('{client2_name}')")
    elif not comparison['same_commit']:
        print("  1. Tables created with different code versions")
        print("  2. Re-run both clients with same blueprints version")
        print(f"     run_everything('{client1_name}')")
        print(f"     run_everything('{client2_name}')")
    else:
        print("  1. Likely NRF calendar updated between runs")
        if client1_timestamp and client2_timestamp:
            older_client = client1_name if client1_timestamp < client2_timestamp else client2_name
            print(f"  2. Re-run older client: run_everything('{older_client}')")

    # Save to CSV if requested
    if save_to_csv:
        diagnosis_df = pd.DataFrame([diagnosis])
        diagnosis_df.to_csv(output_path, index=False)
        print(f"\n💾 Diagnosis saved to: {output_path}")

    return diagnosis

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example Usage

# COMMAND ----------

# Example: Diagnose Lowes vs Target issue
diagnosis_result = diagnose_nrf_calendar_mismatch(
    client1_name='lowes',
    client1_schema='yd_sensitive_corporate.ydx_lowes_analysts_gold',
    client1_table='lowes_v38_market_share_for_column_null_nrf_calendar',
    client2_name='target_home',
    client2_schema='yd_sensitive_corporate.ydx_target_analysts_gold',
    client2_table='target_home_v38_market_share_for_column_null_nrf_calendar',
    nrf_month_start='2025-08-31',
    save_to_csv=True,
    output_path='/dbfs/tmp/lowes_vs_target_nrf_diagnosis.csv'
)

# COMMAND ----------

# You can also use individual functions for specific checks

# Check metadata only
lowes_metadata = get_table_metadata(
    'yd_sensitive_corporate.ydx_lowes_analysts_gold',
    'lowes_v38_market_share_for_column_null_nrf_calendar'
)
print(lowes_metadata)

# COMMAND ----------

# Check execution history
lowes_history = get_execution_history('lowes', days_back=90)
display(lowes_history)

# COMMAND ----------

# Check NRF calendar for specific period
nrf_calendar = get_nrf_dates_for_period('2025-08-31')
display(nrf_calendar)

# COMMAND ----------

# Check what dates exist in a table for a specific month
table_dates = get_table_dates_for_period(
    'yd_sensitive_corporate.ydx_lowes_analysts_gold',
    'lowes_v38_market_share_for_column_null_nrf_calendar',
    '2025-08-31'
)
display(table_dates)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Diagnosis Across Multiple Clients

# COMMAND ----------

def batch_diagnose_clients(
    client_list,
    base_schema_pattern='yd_sensitive_corporate.ydx_{client}_analysts_gold',
    table_suffix='_v38_market_share_for_column_null_nrf_calendar',
    nrf_month_start='2025-08-31',
    output_path='/dbfs/tmp/batch_nrf_diagnosis.csv'
):
    """
    Run diagnosis across multiple clients and save consolidated results.

    Args:
        client_list: List of client names
        base_schema_pattern: Schema pattern with {client} placeholder
        table_suffix: Table name suffix
        nrf_month_start: NRF month to check
        output_path: Where to save results

    Returns:
        DataFrame with all diagnosis results
    """
    results = []

    for i, client in enumerate(client_list):
        print(f"\n{'='*80}")
        print(f"Checking client {i+1}/{len(client_list)}: {client}")
        print(f"{'='*80}")

        schema = base_schema_pattern.format(client=client)
        table = f"{client}{table_suffix}"

        try:
            # Get metadata
            metadata = get_table_metadata(schema, table)
            timestamp = get_table_update_timestamp(schema, table)

            # Get dates
            dates_df = get_table_dates_for_period(schema, table, nrf_month_start)
            dates_pd = dates_df.toPandas()

            result = {
                'client': client,
                'schema': schema,
                'table': table,
                'blueprints_version': metadata.get('blueprints_version'),
                'commit_hash': metadata.get('commit_hash'),
                'has_metadata': metadata.get('has_metadata'),
                'last_updated': timestamp,
                'date_value': str(dates_pd['date'].iloc[0]) if not dates_pd.empty else None,
                'month_start': nrf_month_start
            }

            results.append(result)
            print(f"✓ {client}: date={result['date_value']}, commit={result['commit_hash'][:8] if result['commit_hash'] else 'N/A'}")

        except Exception as e:
            print(f"✗ {client}: Error - {str(e)}")
            results.append({
                'client': client,
                'error': str(e)
            })

    # Convert to DataFrame and save
    results_df = pd.DataFrame(results)
    results_df.to_csv(output_path, index=False)
    print(f"\n💾 Batch diagnosis saved to: {output_path}")

    return results_df

# COMMAND ----------

# Example: Check all major clients
major_clients = [
    'lowes',
    'target_home',
    'bosch',
    'kohler',
    'traeger',
    'wayfair'
]

batch_results = batch_diagnose_clients(
    client_list=major_clients,
    nrf_month_start='2025-08-31',
    output_path='/dbfs/tmp/all_clients_nrf_diagnosis.csv'
)

display(batch_results)
