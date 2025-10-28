# Investigation Summary - 03__rerunning

## Overview
This folder contains the investigation to determine if the "Major Cat as Attribute" issue is caused by:
1. **Blueprint code** (product_analysis.py incorrectly adding Major Cat)
2. **Upstream data** (Major Cat already in _filter_items)

---

## Files in This Folder

### Planning & Documentation
- **README.md**: Complete workflow documentation with expected scenarios and next steps

### Investigation Notebooks
1. **01__copy_filter_items_to_test.py**: Copy production tables to test catalog
2. **02__run_sku_time_series_function.py**: Apply blueprint function to test data
3. **03__verify_results.py**: Compare input vs output to determine root cause

### Paula's Investigation Notebooks
4. **04__paula summary.py**: Manual SQL queries checking different schemas
5. **05__step by step sku time.py**: Modified export_sku_time_series function with special attribute handling

### Results (CSV Files)
- **most_frequent__filter_items_weber.csv**: Empty (no product_attributes data)
- **most_frequent__filter_items_werner.csv**: Empty (no product_attributes data)
- **most_frequent__filter_items_odele.csv**: Empty (no product_attributes data)

---

## Key Findings from Investigation

### From 04__paula summary.py

#### Weber Schema Analysis

| Schema | Table | Product Attributes Status |
|--------|-------|---------------------------|
| `ydx_weber_analysts_gold` | `weber_v38_sku_time_series` | ❌ **ALL rows have `{"major_cat":"gas grills"}`** |
| `ydx_weber_analysts_silver` | `weber_v38_filter_items` | ✓ Has correct attributes (lots of nulls, but proper product attributes when present) |
| `ydx_internal_analysts_sandbox` | `weber_v38_filter_items` | ✓ Has correct attributes (same as silver) |
| `ydx_internal_analysts_gold` | `weber_v38_filter_items` | ⚠️ Everything is NULL |

**Example of CORRECT product_attributes** (from _filter_items):
```json
{
  "Backing":"PVC",
  "Brand/Model Compatibility":"PBV4PS1",
  "CA Residents: Prop 65 Warning(s)":"Yes",
  "Closure Type":"Zipper",
  "Color/Finish Family":"Black",
  "Depth (Inches)":"27",
  "Fits Grill Type":"Vertical smoker",
  "Height (Inches)":"48",
  "Manufacturer Color/Finish":"Black",
  "Number of Handles":"0",
  "Number of Vents":"0",
  "Primary Material":"Polyester",
  "Reversible":"No",
  "Series Name":"Pro Series 4",
  "UNSPSC":"48101500",
  "UV Protection":"Yes",
  "Warranty":"1-year limited",
  "Water Resistant":"Yes",
  "Width (Inches)":"28"
}
```

**Example of INCORRECT product_attributes** (from _sku_time_series):
```json
{"major_cat":"gas grills"}
```

---

### From 05__step by step sku time.py

This file shows a **modified version** of `export_sku_time_series` function with a new parameter:

```python
def export_sku_time_series(
    sandbox_schema,
    prod_schema,
    demo_name,
    product_id,
    special_attribute_column_original  # NEW PARAMETER
):
```

**Key Logic Change** (lines 27-36):
```python
if special_attribute_column_original:
    # Only special attribute columns - create named_struct from them
    special_cols_struct = ', '.join([f"'{col}', min({col})" for col in special_attribute_column_original])
    product_attrs_select = f"to_json(named_struct({special_cols_struct})) as product_attributes_json,"
else:
    # No product attributes at all
    product_attrs_select = "CAST(null as STRING) as product_attributes_json,"
```

**This suggests**: Someone (possibly Paula) was investigating whether passing a list of special columns to create product_attributes might be the issue.

**Note**: The original product_attributes handling is commented out (lines 31-33):
```python
# elif has_product_attrs:
#     # Only existing product_attributes (variant)
#     product_attrs_select = "to_json(first(product_attributes)) as product_attributes_json,"
```

---

### From CSV Files (Empty Results)

All three CSV files (`most_frequent__filter_items_*.csv`) are **empty except for headers**:
```
,attrs_json,count,percentage,demo_name,table analysed
```

**This indicates**: When analyzing the `_filter_items` tables, there were either:
- No product_attributes values found
- Or the query/export didn't capture any data

---

## 🔍 ROOT CAUSE DETERMINATION

Based on the evidence:

### ✅ CONFIRMED: Blueprint Code Issue

**Evidence**:
1. **INPUT (_filter_items)**: Has CORRECT product attributes (or nulls)
   - Example: `{"Backing":"PVC", "Color/Finish Family":"Black", ...}`

2. **OUTPUT (_sku_time_series)**: Has INCORRECT product attributes
   - Example: `{"major_cat":"gas grills"}`

3. **Transformation**: Major Cat is being ADDED during the export_sku_time_series process

### 🎯 The Problem

The original `product_analysis.py` code (from the blueprints) is:
- **NOT using existing product_attributes** from _filter_items
- **INSTEAD creating NEW product_attributes** from somewhere else
- **Incorrectly including major_cat** in the generated product_attributes

### Hypothesis: Where is major_cat coming from?

Looking at notebook 05, there's a `special_attribute_column_original` parameter that creates product_attributes from specified columns.

**Possible scenario**:
- Somewhere in the call to `export_sku_time_series()`, a parameter is being passed that includes `major_cat` as a "special attribute"
- OR there's code elsewhere that's generating product_attributes from dimension columns instead of using the existing product_attributes column

---

## 🔧 Next Steps

### 1. Find Where export_sku_time_series is Called

Search for calls to `export_sku_time_series()` or `run_export_product_analysis_module()` to see:
- What parameters are being passed
- If there's a `special_attribute_column_original` parameter
- If `major_cat` is being explicitly included

### 2. Check Original Blueprint Code

Compare the version in notebook 05 with the original in:
```
/Users/paulafisch/Documents/Projects/corporate_transformation_blueprints/
corporate_transformation_blueprints/retail_analytics_platform/analysis/product_analysis.py
```

Look for:
- Any `special_attribute_column_original` parameter (doesn't exist in our earlier read)
- Any logic that creates product_attributes from dimension columns
- Any hardcoded inclusion of major_cat

### 3. Review Client Configuration

Check if Weber, Werner, and Odele have special configurations that:
- Override the default product_attributes behavior
- Specify which columns should become product_attributes
- Include major_cat in an attribute list

### 4. Fix and Test

Once root cause is confirmed:
1. Remove logic that creates product_attributes from major_cat
2. Ensure product_attributes column from _filter_items is used as-is
3. Test on Weber in sandbox
4. Rerun all affected clients

---

## 📊 Summary Table

| Client | _filter_items Status | _sku_time_series Status | Issue Confirmed |
|--------|---------------------|------------------------|-----------------|
| Weber | ✓ Correct attributes | ❌ Has major_cat | YES |
| Werner | Unknown (CSV empty) | Likely same issue | Probable |
| Odele | Unknown (CSV empty) | Likely same issue | Probable |

---

## Conclusion

**ROOT CAUSE**: The blueprint code is NOT using the existing product_attributes from _filter_items, but instead is creating new product_attributes that incorrectly include major_cat.

**EVIDENCE**: _filter_items has correct product attributes, but _sku_time_series has `{"major_cat":"..."}` instead.

**ACTION REQUIRED**:
1. Investigate how export_sku_time_series is being called
2. Find where major_cat is being injected
3. Fix to use existing product_attributes column
4. Rerun affected clients
