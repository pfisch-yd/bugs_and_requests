# Investigation Summary - Attribute Filters Disappearing on Retailer Selection

**Date**: October 24, 2025
**Issue**: Attribute filters disappear in Masco portal when retailer is selected
**Reported by**: Diego
**Link**: [Slack Thread](https://yipitdata.slack.com/archives/C06QMHHAQE8/p1761228788604509)

---

## Problem Statement

When users select a **retailer** in the Masco portal, the **attribute filter options disappear**. However, when selecting **brand** or **category**, the attribute filters remain populated.

---

## Root Cause Analysis

### Current Architecture Issue

The portal uses **different gold tables** for different filter types:

| Filter Type | Source Table | Merchant Column Used |
|------------|--------------|---------------------|
| **Attribute Filters** | `masco_v38_filter_items` | `merchant_clean` |
| **Other Top-Level Filters** (retailer, brand, category) | `masco_v38_market_share_for_column_null` | `merchant` |

### The Inconsistency

**In `filter_items` table:**
- Raw merchant: `"amazon_leia"`
- Cleaned merchant: `"Amazon"`
- Row count: 3,141,927 rows

**In `market_share_for_column_null` table:**
- Merchant: `"Amazon"` (only this format)
- Row count: 774,035 rows

**Result:** When a user selects "Amazon" from the retailer filter (sourced from `market_share_for_column_null`), the attribute filter tries to match it against `filter_items`, but **finds no match** because:
- Portal sends filter: `merchant = "Amazon"`
- `filter_items` has: `merchant = "amazon_leia"`
- Even though `merchant_clean = "Amazon"`, the join/filter logic is using the wrong column

---

## Investigation Results

### 1. Merchant Name Format Comparison

```sql
-- filter_items (raw merchant)
merchant: "amazon_leia"
merchant_clean: "Amazon"
Count: 3,141,927

-- filter_items (clean merchant)
merchant_clean: "Amazon"
Count: 3,141,927

-- market_share_for_column_null
merchant: "Amazon"
Count: 774,035

-- sku_time_series (proposed solution)
merchant: "Amazon"
Count: 956,097
```

### 2. Table Structure Analysis

**`masco_v38_filter_items` (gold):**
- Has both `merchant` and `merchant_clean` columns
- Contains attribute columns: `shower_door_type`, `Mounting`, `Type`, `pack_size`, `finish`
- Has `special_attribute_column` and `special_attribute_display`
- Has `product_attributes` (VARIANT type)
- **Total distinct merchants**: 9

**`masco_v38_market_share_for_column_null` (gold):**
- Has only `merchant` column (NO `merchant_clean`)
- Does NOT have individual attribute columns
- Does have `special_attribute` column (aggregated)
- **Total distinct merchants**: 9

**`masco_v38_sku_time_series` (gold):**
- Has only `merchant` column (clean format like "Amazon")
- Has `product_attributes` (VARIANT type)
- Compatible with `market_share_for_column_null` format
- **Total distinct merchants**: 9

### 3. All Merchants in Portal (Standardized Format)

Both `market_share_for_column_null` and `sku_time_series` use the same clean merchant names:
- Ace Hardware
- Amazon
- Costco
- Do It Best
- Home Depot
- Lowe's
- Menards
- Tractor Supply
- Walmart

---

## Proposed Solutions (from Slack)

### Option 1: Use `_sku_time_series` for Attribute Filters (Juan's Preference)
**Pros:**
- More compatible with Share Breakdown tables
- Uses same merchant format as other filters
- Already used in PA (Product Analysis)
- Has `product_attributes` VARIANT column

**Cons:**
- Requires portal code changes
- Need to verify attribute data completeness
- More complex implementation

**Status:** Juan and Daniel agreed to defer this until React migration

### Option 2: Make Attribute Filter Independent (Quick Fix)
**Pros:**
- Simple implementation
- No data pipeline changes needed
- Immediate fix

**Cons:**
- Doesn't solve root cause
- Creates inconsistent UX (other filters affect each other, but not attributes)
- Temporary solution

**Status:** Discussed but decided to wait

### Option 3: Leave As-Is Until React Migration (DECIDED)
**Pros:**
- Avoids technical debt from quick fixes
- Will be addressed properly in React migration
- Part of larger attribute filter UI redesign

**Cons:**
- Users experience the bug until then
- Timeline tied to Price Bands Phase 2 completion

**Status:** ✅ **AGREED - Current decision by Daniel and Juan**

---

## Technical Details for Future Implementation

### When Implementing Fix (React Migration):

1. **Standardize merchant names upstream** in data pipeline:
   - Ensure all gold tables use `merchant_clean` format
   - OR ensure `filter_items` uses same `merchant` format as other tables

2. **Update attribute filter query** to use `_sku_time_series`:
   ```sql
   -- Current (problematic)
   SELECT DISTINCT shower_door_type
   FROM masco_v38_filter_items
   WHERE merchant_clean = <selected_retailer>

   -- Proposed (compatible)
   SELECT DISTINCT product_attributes:shower_door_type
   FROM masco_v38_sku_time_series
   WHERE merchant = <selected_retailer>
   ```

3. **New React UI for attributes**:
   - Single select with both attribute keys AND values
   - Proper handling of VARIANT type `product_attributes`

### Data Pipeline Fix (setup.py)

The root issue is in [setup.py:336-361](../../corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/core/setup.py#L336-L361):

```python
# Current implementation keeps raw merchant name
filter_items = spark.sql(f"""
    SELECT
        a.* {column_except},
        coalesce(b.merchant_clean, a.merchant) as merchant_clean,  # Adds clean version
        # ... but keeps original 'merchant' column with "amazon_leia" format
```

**Fix:** Use `merchant_clean` as the primary `merchant` column:
```python
filter_items = spark.sql(f"""
    SELECT
        a.* except(merchant) {column_except},
        coalesce(b.merchant_clean, a.merchant) as merchant,  # Replace merchant with clean version
```

---

## Questions to Answer Before Responding

### 1. ✅ What are the exact merchant name differences between tables?
**Answer:**
- `filter_items` uses raw names like "amazon_leia"
- `market_share_for_column_null` and `sku_time_series` use clean names like "Amazon"

### 2. ✅ Which table has attribute data?
**Answer:**
- `filter_items`: Has individual attribute columns (shower_door_type, etc.)
- `sku_time_series`: Has VARIANT `product_attributes` column
- `market_share_for_column_null`: Has aggregated `special_attribute` only

### 3. ✅ Is the proposed `_sku_time_series` solution viable?
**Answer:**
- YES - It has compatible merchant names and attribute data
- Used successfully in PA module
- Same format as Share Breakdown tables

### 4. ✅ What is the timeline for fix?
**Answer:**
- NOT immediate
- After Price Bands Phase 2 completion
- During React migration for new attribute filter UI

### 5. ✅ Should we implement a quick fix now?
**Answer:**
- NO - Team decided to wait for proper fix during React migration
- Avoids technical debt from temporary solutions

### 6. ✅ Is there an upstream data fix needed?
**Answer:**
- YES - `filter_items` should use `merchant_clean` as primary merchant column
- This would make it compatible with other tables
- Fix can be implemented in `setup.py` `prep_filter_items()` function

---

## Recommended Response to Diego

**Key Points to Communicate:**

1. **Root cause identified**: Different tables use different merchant name formats ("amazon_leia" vs "Amazon")

2. **Decision made**: Team will address this properly during React migration (not implementing quick fix)

3. **Timeline**: After Price Bands Phase 2, during new attribute filter UI implementation

4. **Temporary workaround**: None recommended - users will need to work around this limitation

5. **Upstream fix**: Recommend standardizing merchant names in `filter_items` table creation

---

## Related Issues

- First issue (Oct 23): NULL attributes showing L1 category names → [Fixed with coalesce](slack_messages.md)
- Second issue (Oct 24): Attribute filters disappear on retailer selection → **This investigation**

---

## Action Items

- [ ] Confirm with Juan and Daniel the React migration timeline
- [ ] Verify `sku_time_series` has complete attribute data coverage
- [ ] Plan upstream fix for `filter_items` merchant name standardization
- [ ] Document new attribute filter query logic for React implementation
- [ ] Add this to React migration planning docs

---

## Files Referenced

- Setup script: `/Users/paulafisch/Documents/Projects/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/core/setup.py`
- Slack messages: [slack_messages.md](slack_messages.md), [slack_messages2.md](slack_messages2.md)
- NULL fix script: [standardize_special_attributes.py](standardize_special_attributes.py)
