# Affected Clients Investigation

## Purpose
Identify ALL clients affected by the "Major Cat as Attribute" issue across the entire client base.

## Data Source
**Table**: `data_solutions_sandbox.corporate_clients_info`

## Selection Criteria

### Clients are considered AFFECTED if:

1. **`special_attribute_column` is NULL, empty array, or None**
   - Indicates no special handling for product attributes
   - Should use existing product_attributes from _filter_items

2. **`product_attributes` column exists and is valid**
   - Data type: VARIANT
   - NOT 100% null (has some non-null values)
   - Contains actual product attributes from source data

3. **Confirmed issue in _sku_time_series**
   - product_attributes contains `{"major_cat":"..."}`
   - Instead of actual product attributes from _filter_items

## Notebooks

### 1. identify_affected_clients.py
**Purpose**: Scan all clients and identify those potentially affected

**Process**:
1. Load all clients from `corporate_clients_info`
2. Filter for clients with empty `special_attribute_column`
3. Check each client's `_filter_items` table for valid `product_attributes`
4. Generate list of affected clients

**Output**:
- Count of affected clients
- Detailed list with statistics
- Sample data from affected clients

**Key Metrics**:
- Total records in _filter_items
- Non-null product_attributes count
- Null percentage
- Data type verification

---

### 2. verify_sku_time_series_tables.py
**Purpose**: Verify the issue exists in production _sku_time_series tables

**Process**:
1. Load affected clients list from previous notebook
2. Check each client's `_sku_time_series` table
3. Search for "major_cat" in product_attributes
4. Cross-reference with _filter_items to confirm issue

**Output**:
- Confirmed affected clients with major_cat issue
- Percentage of records with major_cat
- Sample records showing the issue
- Comparison: _filter_items vs _sku_time_series

**Verification Logic**:
```
IF major_cat in _sku_time_series
AND (major_cat NOT in _filter_items OR _filter_items has valid attrs)
THEN: Confirmed Issue (blueprint code problem)
```

---

## Expected Findings

### Scenario A: Confirmed Blueprint Issue
```
_filter_items:
  ✓ Valid product_attributes (e.g., {"Color":"Black", "Size":"Large"})
  ✓ OR NULL (no product attributes)

_sku_time_series:
  ❌ product_attributes = {"major_cat":"..."}

Diagnosis: Blueprint code is REPLACING valid attributes with major_cat
```

### Scenario B: No Issue
```
_filter_items:
  ✓ Valid product_attributes OR NULL

_sku_time_series:
  ✓ Same as _filter_items (copied correctly)

Diagnosis: No issue, working as expected
```

---

## Tables Checked

### Source Tables (Input)
- Schema: `yd_sensitive_corporate.ydx_internal_analysts_sandbox`
- Table: `{demo_name}_v38_filter_items`

### Output Tables (Production)
- Schema: `ydx_{demo_name}_analysts_gold` (client-specific)
- OR Schema: `ydx_internal_analysts_gold` (shared)
- Table: `{demo_name}_v38_sku_time_series`

---

## Key Statistics Collected

### For Each Client:
1. **Configuration**
   - demo_name
   - client_name
   - sandbox_schema
   - special_attribute_column value

2. **_filter_items Analysis**
   - Table exists?
   - product_attributes column exists?
   - Data type (should be VARIANT)
   - Total records
   - Non-null count
   - Null percentage

3. **_sku_time_series Analysis**
   - Table exists?
   - product_attributes column exists?
   - Total records with product_attributes
   - Count with "major_cat"
   - Percentage affected

---

## Results Format

### CSV Export Format
```csv
demo_name,client_name,sandbox_schema,total_records,major_cat_count,major_cat_percentage,status
weber,Weber,ydx_internal_analysts_sandbox,150000,150000,100.0,AFFECTED
werner,Werner,ydx_internal_analysts_sandbox,80000,80000,100.0,AFFECTED
odele,Odele,ydx_internal_analysts_sandbox,45000,45000,100.0,AFFECTED
```

---

## Execution Instructions

### Step 1: Run Identification
```python
# Run notebook: identify_affected_clients.py
# Expected runtime: 5-15 minutes (depends on # of clients)
```

**Output**:
- Temp view: `affected_clients`
- Console output with affected clients list

### Step 2: Run Verification
```python
# Run notebook: verify_sku_time_series_tables.py
# Expected runtime: 10-30 minutes (depends on # of clients)
```

**Output**:
- Temp view: `confirmed_major_cat_issue`
- Detailed report of confirmed issues
- CSV-formatted list for documentation

### Step 3: Document Findings
- Copy CSV output to action_plan.md
- Update analysis.md with affected clients list
- Prepare rerun list for fixing the issue

---

## Known Clients (Confirmed Affected)
Based on previous investigation:
- ✓ Weber
- ✓ Werner
- ✓ Odele

This investigation will identify ALL other affected clients.

---

## Next Steps After Identification

1. **Document all affected clients**
   - Add to action_plan.md
   - Include in bug report

2. **Prioritize clients for fix**
   - By usage/importance
   - By data volume
   - By last refresh date

3. **Prepare rerun plan**
   - Order of execution
   - Dependencies
   - Validation steps

4. **Fix blueprint code**
   - Update product_analysis.py
   - Test on one client first
   - Roll out to all affected clients

---

## Notes

- Run during off-peak hours if checking many clients
- Some clients may have missing tables (expected)
- Empty special_attribute_column is the KEY indicator
- Variant type product_attributes is expected/correct
- major_cat should NEVER appear in product_attributes

---

## Questions This Investigation Answers

1. ✅ How many clients are affected?
2. ✅ Which specific clients have the issue?
3. ✅ Is the issue in _filter_items or _sku_time_series?
4. ✅ What percentage of records are affected per client?
5. ✅ Is this a widespread issue or isolated to a few clients?

---

## Contact
For questions about this investigation, refer to:
- Main issue: `details.md`
- Root cause analysis: `03__rerunning/SUMMARY.md`
- Step-by-step debug: `04__opening the function/`
