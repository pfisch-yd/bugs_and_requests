# Action Plan: Major Cat Appearing as Attribute in Product Analysis

## Problem Summary
The Product Attribute dropdown menu in Atlas (Product Analysis page) is incorrectly showing "Major Cat" as an attribute for several clients (Weber, Werner, and Odele). This dropdown uses `_sku_time_series` as its data source.

## Investigation Steps

### 1. Examine the Source Code
- **File to Review**: `/Users/paulafisch/Documents/Projects/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/analysis/product_analysis.py`
- **Goal**: Understand how `_sku_time_series` tables are created and populated
- **Focus**: Identify where product attributes are defined and how they are filtered

### 2. Database Investigation
- **Query `_sku_time_series` tables** for affected clients:
  - Weber
  - Werner
  - Odele
- **Check for**:
  - Presence of "Major Cat" in attribute columns
  - Schema differences compared to unaffected clients
  - Data quality issues or unexpected column values

### 3. Root Cause Analysis
- **Determine why "Major Cat" is appearing**:
  - Is it a configuration issue?
  - Is it a data pipeline bug?
  - Is it a schema mismatch?
  - Is there a hardcoded list of attributes that needs updating?

### 4. Compare Affected vs Unaffected Clients
- **Select a control group**: Choose 2-3 clients that don't have this issue
- **Compare**:
  - Table schemas
  - Data transformation logic
  - Configuration parameters
  - Source data structure

### 5. Develop Solution
Based on findings, potential solutions could include:
- Filtering out "Major Cat" from attribute lists
- Fixing data pipeline to exclude "Major Cat" during table creation
- Updating configuration to properly categorize "Major Cat"
- Correcting source data classification

### 6. Testing Plan
- Test fix on one affected client first
- Verify dropdown no longer shows "Major Cat"
- Ensure no other attributes were inadvertently removed
- Check that all expected attributes still appear

### 7. Rollout
- Apply fix to all affected clients (Weber, Werner, Odele)
- Monitor for any side effects
- Document the issue and resolution

## Next Steps
1. Start by reading and analyzing `product_analysis.py`
2. Query the database to examine `_sku_time_series` structure for affected clients
3. Proceed based on initial findings

## Notes
- Major Cat (Major Category) should likely be a dimension/filter, not a product attribute
- This may be a classification issue in the data model
