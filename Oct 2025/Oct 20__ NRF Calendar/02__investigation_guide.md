# NRF Calendar Date Mismatch - Investigation Guide

## Problem Summary

**Issue**: Lowes and Target market_share tables show different `date` values for the same NRF calendar month period.

**Example:**
| Client | date | month_start | month_end | month |
|--------|------------|-------------|-----------|-------|
| Lowes | 2025-09-01 | 2025-08-31 | 2025-10-04 | 8 |
| Target | 2025-08-01 | 2025-08-31 | 2025-10-04 | 8 |

**Problem**: Same NRF month (Aug 31 - Oct 4), but `date` differs by 1 month.

---

## Investigation Hypotheses

### Hypothesis 1: One table created outside blueprints
**How to verify**: Check for `blueprints.commit_hash` in table properties
- If missing → table not created by blueprints code

### Hypothesis 2: Different Git commits used
**How to verify**: Compare `blueprints.commit_hash` between tables
- If different → tables created with different code versions
- Need to check what changed in the code between commits

### Hypothesis 3: NRF calendar updated between runs
**How to verify**: Compare table creation times vs NRF calendar update times
- If calendar updated between Lowes and Target runs → different mappings used

---

## Investigation Phases

### Phase 1: Quick Metadata Check
**Run this first** to quickly identify the most likely cause.

**Query**: Check table properties for both clients
```sql
SHOW TBLPROPERTIES yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_nrf_calendar;
SHOW TBLPROPERTIES yd_sensitive_corporate.ydx_target_analysts_gold.target_home_v38_market_share_for_column_null_nrf_calendar;
```

**Look for**:
- `blueprints.version` - Should match if same code version
- `blueprints.commit_hash` - Should match if same code version
- `blueprints.created_by` - Should be "Blueprints : export_market_share_for_column_null_nrf_calendar"

**Decision tree**:
- **If both have commit hashes AND they match** → Go to Phase 4 (calendar changed)
- **If both have commit hashes BUT they differ** → Go to Phase 2 (different commits)
- **If one is missing commit hash** → Hypothesis 1 confirmed (created outside blueprints)

---

### Phase 2: Execution Timeline
**When were the tables created?**

**Query**: Check run_everything() execution history
```sql
SELECT
  get_json_object(args, '$[0]') AS client_name,
  end_timestamp,
  user,
  error
FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
WHERE name LIKE 'run_everything%'
  AND get_json_object(args, '$[0]') IN ('lowes', 'target_home')
  AND error IS NULL
  AND end_timestamp > CURRENT_DATE() - INTERVAL 3 MONTH
ORDER BY end_timestamp DESC;
```

**Also check**: Table `_updated_timestamp`
```sql
SELECT MAX(_updated_timestamp) FROM yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_nrf_calendar;
SELECT MAX(_updated_timestamp) FROM yd_sensitive_corporate.ydx_target_analysts_gold.target_home_v38_market_share_for_column_null_nrf_calendar;
```

**What to look for**:
- Time gap between executions (hours? days? weeks?)
- Timestamps should match telemetry execution times

---

### Phase 3: Git Commit Comparison
**If commits differ, what changed?**

**Steps**:
1. Get commit hashes from Phase 1
2. Check Git history for `market_share.py`:
   ```bash
   git log --oneline corporate_transformation_blueprints/retail_analytics_platform/analysis/market_share.py
   ```
3. Look for changes between commits in:
   - Lines 586-608: `part1_nrf` (the join to NRF calendar)
   - Lines 373-376: `part4_nrf` (the calendar data mapping)

**Key code section** (lines 600-608):
```python
left join
yd_sensitive_corporate.ydx_internal_analysts_gold.454cal_added_columns as fiscal
on data_table.date = fiscal.cal_day
```

This join determines how `nrf_month_for_trailing` gets assigned.

---

### Phase 4: NRF Calendar History
**Was the calendar updated between the runs?**

**Query**: Check calendar table history
```sql
DESCRIBE HISTORY yd_sensitive_corporate.ydx_internal_analysts_gold.454cal_added_columns
ORDER BY version DESC LIMIT 20;
```

**Compare**:
- Calendar update timestamps
- Table creation timestamps (from Phase 2)

**Red flag**: If calendar was updated between Lowes run and Target run

---

### Phase 5: Inspect Calendar Data
**Check the actual calendar mapping**

**Query**: See how dates map to NRF months
```sql
SELECT
    cal_day,
    nrf_month_for_trailing,
    nrf_month_start,
    nrf_month_end,
    nrf_month_number
FROM yd_sensitive_corporate.ydx_internal_analysts_gold.454cal_added_columns
WHERE nrf_month_start = '2025-08-31'
ORDER BY cal_day;
```

**Expected behavior**: All days in the period (Aug 31 - Oct 4) should map to the **same** `nrf_month_for_trailing` value.

**Problem scenario**: If multiple different `nrf_month_for_trailing` values exist, the calendar definition itself has an issue.

---

### Phase 6: Compare Actual Table Data
**Verify the problem exists in the tables**

**Query**: Compare date values side by side
```sql
-- Lowes
SELECT DISTINCT date, month_start, month_end, month
FROM yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_nrf_calendar
WHERE month_start = '2025-08-31';

-- Target
SELECT DISTINCT date, month_start, month_end, month
FROM yd_sensitive_corporate.ydx_target_analysts_gold.target_home_v38_market_share_for_column_null_nrf_calendar
WHERE month_start = '2025-08-31';
```

**What you should see**: Same `month_start` and `month_end`, but different `date` values.

---

## Root Cause Determination

### Scenario A: Different Commit Hashes
**Root cause**: Tables created with different code versions
**Fix**: Re-run both clients with the same blueprints version

**Action**:
```python
# Make sure both use the same code
run_everything("lowes")
run_everything("target_home")
```

---

### Scenario B: Same Commits, Different Times + Calendar Update
**Root cause**: NRF calendar was updated between the two runs
**Fix**: Re-run the older table to use the updated calendar

**To determine which is older**:
```sql
-- Compare timestamps
SELECT
  'lowes' as client,
  MAX(_updated_timestamp) as updated
FROM yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_nrf_calendar
UNION ALL
SELECT
  'target_home' as client,
  MAX(_updated_timestamp) as updated
FROM yd_sensitive_corporate.ydx_target_analysts_gold.target_home_v38_market_share_for_column_null_nrf_calendar;
```

**Action**: Re-run the client with the older timestamp.

---

### Scenario C: One Created Outside Blueprints
**Root cause**: One table was manually created or used old code path
**Fix**: Re-create the table using blueprints

**How to identify**: Missing `blueprints.commit_hash` in table properties

**Action**:
```python
# Delete the old table and recreate
spark.sql("DROP TABLE IF EXISTS schema.table_name")
run_everything("client_name")
```

---

## How the NRF Calendar Join Works

**Code flow** (from `market_share.py`):

1. **Line 592**: Convert `order_date` to `date` (day-level)
   ```python
   cast(date_trunc('day', order_date) as date) as date
   ```

2. **Lines 604-607**: Join to NRF calendar
   ```python
   left join
   yd_sensitive_corporate.ydx_internal_analysts_gold.454cal_added_columns as fiscal
   on data_table.date = fiscal.cal_day
   ```

3. **Line 602**: Get the "trailing month" reference
   ```python
   nrf_month_for_trailing as date
   ```

4. **Lines 373-376**: Later, join again to get month metadata
   ```python
   left join
   yd_sensitive_corporate.ydx_internal_analysts_gold.454cal_by_month as fiscal
   on data_table.date = fiscal.nrf_month_for_trailing
   ```

**The key**: `nrf_month_for_trailing` determines the `date` column value in the final table.

---

## Expected vs Actual Behavior

### Expected (correct):
For NRF month Aug 31 - Oct 4:
- All calendar days in this period should map to the **same** `nrf_month_for_trailing` value
- This becomes the `date` in the market_share table
- Should be consistent across all clients

### Actual (problem):
- Lowes shows `date = 2025-09-01`
- Target shows `date = 2025-08-01`
- Same NRF month, different reference dates

### Why this matters:
The `date` column is used as the **reference label** for the NRF month in dashboards. If it's inconsistent:
- Charts will misalign
- Time series comparisons break
- Month labels show wrong dates

---

## Quick Diagnostic Script

Run the full diagnostic notebook:
```
/Users/paulafisch/Documents/Projects/bugs_and_requests/Oct 2025/Oct 20__ NRF Calendar/01__diagnostic_investigation.py
```

This will execute all phases automatically and generate a summary report.

---

## Prevention for Future

### Recommendations:

1. **Add schema validation** to detect this issue:
   ```python
   def validate_nrf_calendar_consistency(client_list):
       """Check that all clients have same date mapping for NRF months"""
   ```

2. **Log table metadata** to CSV for easy comparison:
   ```python
   def log_table_creation_metadata(client, commit_hash, timestamp):
       """Save to execution_logs.csv"""
   ```

3. **Version lock the NRF calendar**:
   - Store calendar version in table metadata
   - Alert if calendar changes
   - Re-run all clients if calendar updates

4. **Automated testing**:
   - Compare date mappings across clients
   - Flag discrepancies automatically
   - Part of CI/CD pipeline

---

## Contact

For questions about this investigation:
- See JIRA: [ENG-2404](https://yipitdata5.atlassian.net/browse/ENG-2404)
- Code: `/corporate_transformation_blueprints/retail_analytics_platform/analysis/market_share.py`
- NRF Calendar: `yd_sensitive_corporate.ydx_internal_analysts_gold.454cal_added_columns`
