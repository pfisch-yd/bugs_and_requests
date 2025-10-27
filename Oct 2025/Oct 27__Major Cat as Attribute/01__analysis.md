# Analysis: Major Cat as Product Attribute

## Code Review Summary

After reviewing [product_analysis.py](../../../corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/analysis/product_analysis.py), here's my analysis of how Product Attributes are handled:

## Key Functions Analyzed

### 1. `export_sku_time_series()` (Lines 279-372)
This is the function that creates the `_sku_time_series` table used by the Atlas Product Analysis dropdown.

### 2. Product Attributes Field Handling

The code checks if `product_attributes` column exists in `_filter_items`:

```python
optional_cols = get_optional_columns_availability(sandbox_schema, demo_name)
has_product_attrs = optional_cols['product_attributes']
```

**Line 293**: If the column exists, it's included as:
```sql
product_attrs_select = "to_json(product_attributes) as product_attributes_json,"
```

**Line 365**: The final output parses it back:
```sql
*except(product_attributes_json), parse_json(product_attributes_json) as product_attributes
```

## Critical Finding: The Source is `_filter_items`

**The `product_analysis.py` code does NOT create or modify the product_attributes values**. It simply:
1. Checks if the `product_attributes` column exists in `_filter_items`
2. If it exists, copies it as-is using `to_json()` and `parse_json()`
3. If it doesn't exist, sets it to `NULL`

## Data Flow

```
_filter_items (from core/setup)
    ↓
    product_attributes column (if exists)
    ↓
to_json(product_attributes) → product_attributes_json
    ↓
parse_json(product_attributes_json) → product_attributes
    ↓
_sku_time_series table
    ↓
Atlas Product Analysis Dropdown
```

## Where Major Cat Could Be Injected

**The problem originates BEFORE this code**, specifically in:

### Option 1: The `_filter_items` table creation in `core/setup`
- The `product_attributes` column is populated when `_filter_items` is created
- **This is where Major Cat is likely being added as an attribute**

### Option 2: Source data structure
- If the source data has a `product_attributes` field/column that includes "Major Cat"
- This would be passed through unchanged

## Product Attributes Data Type

Based on the code:
- **Source type in `_filter_items`**: Likely a `STRUCT` or `MAP` type (since it uses `to_json()`)
- **Intermediate type**: `STRING` (JSON string)
- **Final type in `_sku_time_series`**: `VARIANT` type (parsed JSON)

The code uses:
```python
create_table_with_variant_support(module_name, prod_schema, demo_name+'_sku_time_series',
                                  sku_time_series, overwrite=True,
                                  variant_columns=['product_attributes'])
```

## Expected Product Attributes Content

Product attributes should contain things like:
- Color
- Size
- Material
- Style
- Flavor
- Scent
- etc.

**Major Cat (Major Category) should NOT be in product attributes** because it's already a separate dimension column used for grouping in the SQL queries (see lines 315, 350, etc.).

## Hypothesis

The bug likely occurs when:
1. **`core/setup` incorrectly includes `major_cat` in the `product_attributes` column** for certain clients (Weber, Werner, Odele)
2. This could be due to:
   - A data transformation error in setup
   - Source data inconsistency for these clients
   - A configuration that treats category fields differently for these clients

## Next Steps to Confirm

1. **Query the `_filter_items` table** for affected clients:
   ```sql
   SELECT
       demo_name,
       product_attributes,
       major_cat,
       COUNT(*) as record_count
   FROM sandbox_schema.weber_filter_items
   WHERE product_attributes IS NOT NULL
   GROUP BY 1, 2, 3
   LIMIT 100
   ```

2. **Check if product_attributes contains "Major Cat"** in the JSON structure

3. **Compare with unaffected clients** to see the difference in `product_attributes` structure

4. **Review `core/setup` notebook** to understand how `product_attributes` column is created

## Conclusion

**The `product_analysis.py` code is NOT injecting Major Cat as an attribute.** It's simply passing through whatever is in the `product_attributes` column from `_filter_items`. The root cause is in the `core/setup` process that creates the `_filter_items` table.
