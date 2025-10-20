# Databricks notebook source
# MAGIC %md
# MAGIC # NRF Calendar Date Mismatch Investigation
# MAGIC
# MAGIC **Issue**: Lowes and Target tables show different `date` values for the same NRF month period
# MAGIC
# MAGIC **Tables affected**:
# MAGIC - `yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_nrf_calendar`
# MAGIC - `yd_sensitive_corporate.ydx_target_analysts_gold.target_home_v38_market_share_for_column_null_nrf_calendar`
# MAGIC
# MAGIC **Hypothesis**:
# MAGIC 1. One table created outside blueprints
# MAGIC 2. Different Git commits used
# MAGIC 3. NRF calendar updated between runs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1: Compare Table Metadata
# MAGIC Check Git commits, versions, and creation timestamps

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check Lowes table properties
# MAGIC SHOW TBLPROPERTIES yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_nrf_calendar;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check Target table properties
# MAGIC SHOW TBLPROPERTIES yd_sensitive_corporate.ydx_target_analysts_gold.target_home_v38_market_share_for_column_null_nrf_calendar;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare metadata side by side
# MAGIC WITH lowes_props AS (
# MAGIC   SELECT key, value as lowes_value
# MAGIC   FROM (
# MAGIC     SHOW TBLPROPERTIES yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_nrf_calendar
# MAGIC   )
# MAGIC   WHERE key LIKE 'blueprints.%' OR key LIKE 'delta.%'
# MAGIC ),
# MAGIC target_props AS (
# MAGIC   SELECT key, value as target_value
# MAGIC   FROM (
# MAGIC     SHOW TBLPROPERTIES yd_sensitive_corporate.ydx_target_analysts_gold.target_home_v38_market_share_for_column_null_nrf_calendar
# MAGIC   )
# MAGIC   WHERE key LIKE 'blueprints.%' OR key LIKE 'delta.%'
# MAGIC )
# MAGIC SELECT
# MAGIC   COALESCE(lowes_props.key, target_props.key) as property_key,
# MAGIC   lowes_props.lowes_value,
# MAGIC   target_props.target_value,
# MAGIC   CASE
# MAGIC     WHEN lowes_props.lowes_value = target_props.target_value THEN 'MATCH'
# MAGIC     WHEN lowes_props.lowes_value IS NULL OR target_props.target_value IS NULL THEN 'MISSING'
# MAGIC     ELSE 'DIFFERENT'
# MAGIC   END as comparison
# MAGIC FROM lowes_props
# MAGIC FULL OUTER JOIN target_props ON lowes_props.key = target_props.key
# MAGIC ORDER BY property_key;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2: Check Execution Timeline
# MAGIC When was each client's dashboard last generated?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get execution history for both clients
# MAGIC SELECT
# MAGIC   get_json_object(args, '$[0]') AS client_name,
# MAGIC   end_timestamp,
# MAGIC   user,
# MAGIC   notebook_path,
# MAGIC   error,
# MAGIC   CASE WHEN error IS NULL THEN 'SUCCESS' ELSE 'FAILED' END as status
# MAGIC FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
# MAGIC WHERE name LIKE 'run_everything%'
# MAGIC   AND get_json_object(args, '$[0]') IN ('lowes', 'target_home')
# MAGIC   AND end_timestamp > CURRENT_DATE() - INTERVAL 3 MONTH
# MAGIC ORDER BY end_timestamp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get most recent successful runs
# MAGIC WITH recent_runs AS (
# MAGIC   SELECT
# MAGIC     get_json_object(args, '$[0]') AS client_name,
# MAGIC     end_timestamp,
# MAGIC     user,
# MAGIC     error,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY get_json_object(args, '$[0]') ORDER BY end_timestamp DESC) as rn
# MAGIC   FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
# MAGIC   WHERE name LIKE 'run_everything%'
# MAGIC     AND get_json_object(args, '$[0]') IN ('lowes', 'target_home')
# MAGIC     AND error IS NULL
# MAGIC     AND end_timestamp > CURRENT_DATE() - INTERVAL 3 MONTH
# MAGIC )
# MAGIC SELECT
# MAGIC   client_name,
# MAGIC   end_timestamp as last_successful_run,
# MAGIC   user,
# MAGIC   DATEDIFF(CURRENT_TIMESTAMP(), end_timestamp) as days_ago
# MAGIC FROM recent_runs
# MAGIC WHERE rn = 1
# MAGIC ORDER BY client_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 3: Check Table Update Timestamps
# MAGIC Compare _updated_timestamp from the actual tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check Lowes table update timestamp
# MAGIC SELECT
# MAGIC   'lowes' as client,
# MAGIC   MIN(_updated_timestamp) as first_update,
# MAGIC   MAX(_updated_timestamp) as last_update,
# MAGIC   COUNT(DISTINCT _updated_timestamp) as unique_timestamps
# MAGIC FROM yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_nrf_calendar;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check Target table update timestamp
# MAGIC SELECT
# MAGIC   'target_home' as client,
# MAGIC   MIN(_updated_timestamp) as first_update,
# MAGIC   MAX(_updated_timestamp) as last_update,
# MAGIC   COUNT(DISTINCT _updated_timestamp) as unique_timestamps
# MAGIC FROM yd_sensitive_corporate.ydx_target_analysts_gold.target_home_v38_market_share_for_column_null_nrf_calendar;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare side by side
# MAGIC WITH lowes_ts AS (
# MAGIC   SELECT
# MAGIC     'lowes' as client,
# MAGIC     MAX(_updated_timestamp) as last_update
# MAGIC   FROM yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_nrf_calendar
# MAGIC ),
# MAGIC target_ts AS (
# MAGIC   SELECT
# MAGIC     'target_home' as client,
# MAGIC     MAX(_updated_timestamp) as last_update
# MAGIC   FROM yd_sensitive_corporate.ydx_target_analysts_gold.target_home_v38_market_share_for_column_null_nrf_calendar
# MAGIC )
# MAGIC SELECT * FROM lowes_ts
# MAGIC UNION ALL
# MAGIC SELECT * FROM target_ts;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 4: Check NRF Calendar History
# MAGIC Was the NRF calendar updated between the two runs?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check history of 454cal_added_columns
# MAGIC DESCRIBE HISTORY yd_sensitive_corporate.ydx_internal_analysts_gold.454cal_added_columns
# MAGIC ORDER BY version DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check history of 454cal_by_month
# MAGIC DESCRIBE HISTORY yd_sensitive_corporate.ydx_internal_analysts_gold.454cal_by_month
# MAGIC ORDER BY version DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 5: Inspect NRF Calendar Data
# MAGIC Check the actual calendar mapping for the problematic period

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect calendar for August-September period
# MAGIC SELECT
# MAGIC     cal_day,
# MAGIC     nrf_month_for_trailing,
# MAGIC     nrf_month_start,
# MAGIC     nrf_month_end,
# MAGIC     nrf_month_number,
# MAGIC     nrf_quarter_rank,
# MAGIC     nrf_year
# MAGIC FROM yd_sensitive_corporate.ydx_internal_analysts_gold.454cal_added_columns
# MAGIC WHERE nrf_month_start = '2025-08-31'
# MAGIC ORDER BY cal_day;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check what nrf_month_for_trailing values exist for this period
# MAGIC SELECT
# MAGIC     DISTINCT nrf_month_for_trailing,
# MAGIC     MIN(cal_day) as first_day,
# MAGIC     MAX(cal_day) as last_day,
# MAGIC     COUNT(*) as days_in_month
# MAGIC FROM yd_sensitive_corporate.ydx_internal_analysts_gold.454cal_added_columns
# MAGIC WHERE nrf_month_start = '2025-08-31'
# MAGIC GROUP BY nrf_month_for_trailing
# MAGIC ORDER BY nrf_month_for_trailing;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 6: Compare Actual Table Data
# MAGIC Look at the problematic rows in both tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lowes data for the problematic period
# MAGIC SELECT DISTINCT
# MAGIC     date,
# MAGIC     month_start,
# MAGIC     month_end,
# MAGIC     quarter,
# MAGIC     month,
# MAGIC     year
# MAGIC FROM yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_nrf_calendar
# MAGIC WHERE month_start = '2025-08-31'
# MAGIC ORDER BY date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Target data for the problematic period
# MAGIC SELECT DISTINCT
# MAGIC     date,
# MAGIC     month_start,
# MAGIC     month_end,
# MAGIC     quarter,
# MAGIC     month,
# MAGIC     year
# MAGIC FROM yd_sensitive_corporate.ydx_target_analysts_gold.target_home_v38_market_share_for_column_null_nrf_calendar
# MAGIC WHERE month_start = '2025-08-31'
# MAGIC ORDER BY date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare side by side
# MAGIC WITH lowes_dates AS (
# MAGIC   SELECT DISTINCT
# MAGIC       date as lowes_date,
# MAGIC       month_start,
# MAGIC       month_end,
# MAGIC       month
# MAGIC   FROM yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_nrf_calendar
# MAGIC   WHERE month_start = '2025-08-31'
# MAGIC ),
# MAGIC target_dates AS (
# MAGIC   SELECT DISTINCT
# MAGIC       date as target_date,
# MAGIC       month_start,
# MAGIC       month_end,
# MAGIC       month
# MAGIC   FROM yd_sensitive_corporate.ydx_target_analysts_gold.target_home_v38_market_share_for_column_null_nrf_calendar
# MAGIC   WHERE month_start = '2025-08-31'
# MAGIC )
# MAGIC SELECT
# MAGIC   COALESCE(l.month_start, t.month_start) as month_start,
# MAGIC   COALESCE(l.month_end, t.month_end) as month_end,
# MAGIC   l.lowes_date,
# MAGIC   t.target_date,
# MAGIC   DATEDIFF(l.lowes_date, t.target_date) as date_difference_days
# MAGIC FROM lowes_dates l
# MAGIC FULL OUTER JOIN target_dates t
# MAGIC   ON l.month_start = t.month_start AND l.month_end = t.month_end;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 7: Check Source Data
# MAGIC Verify the _filter_items tables that feed into market_share

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check Lowes source data date range
# MAGIC SELECT
# MAGIC   'lowes' as client,
# MAGIC   MIN(order_date) as min_order_date,
# MAGIC   MAX(order_date) as max_order_date,
# MAGIC   COUNT(*) as total_rows
# MAGIC FROM ydx_lowes_analysts_gold.lowes_v38_filter_items
# MAGIC WHERE order_date >= '2025-08-01';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check Target source data date range
# MAGIC SELECT
# MAGIC   'target_home' as client,
# MAGIC   MIN(order_date) as min_order_date,
# MAGIC   MAX(order_date) as max_order_date,
# MAGIC   COUNT(*) as total_rows
# MAGIC FROM ydx_target_analysts_gold.target_home_v38_filter_items
# MAGIC WHERE order_date >= '2025-08-01';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 8: Diagnosis Summary
# MAGIC Consolidate findings to determine root cause

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create summary table for analysis
# MAGIC WITH
# MAGIC -- Get table metadata
# MAGIC lowes_metadata AS (
# MAGIC   SELECT
# MAGIC     'lowes' as client,
# MAGIC     MAX(CASE WHEN key = 'blueprints.version' THEN value END) as blueprints_version,
# MAGIC     MAX(CASE WHEN key = 'blueprints.commit_hash' THEN value END) as commit_hash,
# MAGIC     MAX(CASE WHEN key = 'blueprints.created_by' THEN value END) as created_by
# MAGIC   FROM (
# MAGIC     SHOW TBLPROPERTIES yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_nrf_calendar
# MAGIC   )
# MAGIC   WHERE key LIKE 'blueprints.%'
# MAGIC ),
# MAGIC target_metadata AS (
# MAGIC   SELECT
# MAGIC     'target_home' as client,
# MAGIC     MAX(CASE WHEN key = 'blueprints.version' THEN value END) as blueprints_version,
# MAGIC     MAX(CASE WHEN key = 'blueprints.commit_hash' THEN value END) as commit_hash,
# MAGIC     MAX(CASE WHEN key = 'blueprints.created_by' THEN value END) as created_by
# MAGIC   FROM (
# MAGIC     SHOW TBLPROPERTIES yd_sensitive_corporate.ydx_target_analysts_gold.target_home_v38_market_share_for_column_null_nrf_calendar
# MAGIC   )
# MAGIC   WHERE key LIKE 'blueprints.%'
# MAGIC ),
# MAGIC -- Get execution times
# MAGIC execution_times AS (
# MAGIC   SELECT
# MAGIC     get_json_object(args, '$[0]') AS client,
# MAGIC     MAX(end_timestamp) as last_run_timestamp
# MAGIC   FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
# MAGIC   WHERE name LIKE 'run_everything%'
# MAGIC     AND get_json_object(args, '$[0]') IN ('lowes', 'target_home')
# MAGIC     AND error IS NULL
# MAGIC     AND end_timestamp > CURRENT_DATE() - INTERVAL 3 MONTH
# MAGIC   GROUP BY get_json_object(args, '$[0]')
# MAGIC ),
# MAGIC -- Get table update times
# MAGIC lowes_update AS (
# MAGIC   SELECT 'lowes' as client, MAX(_updated_timestamp) as table_updated
# MAGIC   FROM yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_nrf_calendar
# MAGIC ),
# MAGIC target_update AS (
# MAGIC   SELECT 'target_home' as client, MAX(_updated_timestamp) as table_updated
# MAGIC   FROM yd_sensitive_corporate.ydx_target_analysts_gold.target_home_v38_market_share_for_column_null_nrf_calendar
# MAGIC ),
# MAGIC all_updates AS (
# MAGIC   SELECT * FROM lowes_update
# MAGIC   UNION ALL
# MAGIC   SELECT * FROM target_update
# MAGIC ),
# MAGIC -- Combine all info
# MAGIC combined AS (
# MAGIC   SELECT * FROM lowes_metadata
# MAGIC   UNION ALL
# MAGIC   SELECT * FROM target_metadata
# MAGIC )
# MAGIC SELECT
# MAGIC   c.client,
# MAGIC   c.blueprints_version,
# MAGIC   c.commit_hash,
# MAGIC   et.last_run_timestamp,
# MAGIC   au.table_updated,
# MAGIC   CASE
# MAGIC     WHEN c.commit_hash IS NULL THEN 'NOT CREATED BY BLUEPRINTS'
# MAGIC     ELSE 'CREATED BY BLUEPRINTS'
# MAGIC   END as creation_source
# MAGIC FROM combined c
# MAGIC LEFT JOIN execution_times et ON c.client = et.client
# MAGIC LEFT JOIN all_updates au ON c.client = au.client
# MAGIC ORDER BY c.client;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnosis Report
# MAGIC
# MAGIC Based on the queries above:
# MAGIC
# MAGIC ### Check these key findings:
# MAGIC
# MAGIC 1. **Different Git Commits?** (Phase 1)
# MAGIC    - If commit hashes differ → Tables created with different code versions
# MAGIC    - If one is NULL → Table not created by blueprints
# MAGIC
# MAGIC 2. **Different Execution Times?** (Phase 2-3)
# MAGIC    - Compare telemetry timestamps vs table update timestamps
# MAGIC    - Large gap suggests they were created at different times
# MAGIC
# MAGIC 3. **NRF Calendar Changed?** (Phase 4)
# MAGIC    - Check if calendar was updated between the two table creation times
# MAGIC    - If yes → Calendar change caused different mappings
# MAGIC
# MAGIC 4. **Date Mapping Issue?** (Phase 5)
# MAGIC    - All days in NRF month should map to same `nrf_month_for_trailing`
# MAGIC    - If multiple values exist → Calendar definition problem
# MAGIC
# MAGIC ### Root Cause Priority:
# MAGIC - **Most Likely**: Different execution times with calendar update in between (Hypothesis 3)
# MAGIC - **Second Most Likely**: Different Git commits (Hypothesis 2)
# MAGIC - **Least Likely**: One table created outside blueprints (Hypothesis 1)
# MAGIC
# MAGIC ### Recommended Fix:
# MAGIC Re-run both clients with the same blueprints version to ensure consistency.
