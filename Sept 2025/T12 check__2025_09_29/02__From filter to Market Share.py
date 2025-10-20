# Databricks notebook source
# DBTITLE 1,OG
# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM yd_sensitive_corporate.ydx_lowes_analysts_silver.lowes_v38_filter_items
# MAGIC WHERE brand = 'samsung'
# MAGIC AND merchant = 'Home Depot'
# MAGIC AND major_cat = 'Laundry Appliances'
# MAGIC AND sub_cat = 'Combination Washer-Dryers'
# MAGIC AND minor_cat = 'All Combination Washer-Dryers'
# MAGIC --- AND special_attribute = 'Laundry Appliances'
# MAGIC AND channel = 'ONLINE'
# MAGIC AND parent_brand = 'SAMSUNG'
# MAGIC and sub_brand = 'samsung'
# MAGIC --- and month_start = '2024-09-01'

# COMMAND ----------

# DBTITLE 1,PART 1
sandbox_schema = "ydx_lowes_analysts_silver"
demo_name = "lowes_v38"
column = "major_cat"

part1_std = f"""
    with 
    clean_special_attribute_and_columns as (
    select
        * except (merchant, parent_brand, brand, sub_brand),
        coalesce(cast({column} as string), major_cat) as special_attribute,
        cast(date_trunc('month', order_date) as date) as date,
        coalesce(parent_brand, "unbranded") as parent_brand,
        coalesce(brand, "unbranded") as brand,
        coalesce(sub_brand, "unbranded") as sub_brand,
        coalesce(merchant_clean, "no merchant") as merchant
        from {sandbox_schema}.{demo_name}_filter_items
    ),

    grouping_by_all_possible_controls as (
    SELECT
        date,
        channel,
        parent_brand, 
        brand,
        sub_brand,
        merchant, 
        major_cat as major_category,
        sub_cat as sub_category,
        minor_cat as minor_category,
        special_attribute, -- Notice: If no special attribute, then major
        -- and this is redundant. So this grouping goes from 10 args to 9 args 

        sum(gmv) as gmv, 
        count(*) as sample_size, 
        sum(item_price * item_quantity) as item_subtotal_observed, 
        sum(item_quantity) as item_quantity_observed
    from clean_special_attribute_and_columns
    group by 1,2,3,4,5,6,7,8,9,10
    ),
"""

end_table = f"""
    last_table as (select * from grouping_by_all_possible_controls)

    select * from last_table
    WHERE brand = 'samsung'
    AND merchant = 'Home Depot'
    AND major_category = 'Laundry Appliances'
    AND sub_category = 'Combination Washer-Dryers'
    AND minor_category = 'All Combination Washer-Dryers'
    --- AND special_attribute = 'Laundry Appliances'
    AND channel = 'ONLINE'
    AND parent_brand = 'SAMSUNG'
    and sub_brand = 'samsung'
    --- and month_start = '2024-09-01'

    order by date
"""

full_query_std = f"""
{part1_std}
{end_table}
"""


market_share_for_column_std = spark.sql(full_query_std)
market_share_for_column_std.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## FOUND AN ISSUE IN PART 1

# COMMAND ----------

sandbox_schema = "ydx_lowes_analysts_silver"
demo_name = "lowes_v38"
column = "major_cat"

part1_std = f"""
    with 
    grouping_by_all_possible_controls as (
    select
        * except (merchant, parent_brand, brand, sub_brand),
        coalesce(cast({column} as string), major_cat) as special_attribute,
        cast(date_trunc('month', order_date) as date) as date,
        coalesce(parent_brand, "unbranded") as parent_brand,
        coalesce(brand, "unbranded") as brand,
        coalesce(sub_brand, "unbranded") as sub_brand,
        coalesce(merchant_clean, "no merchant") as merchant
        from {sandbox_schema}.{demo_name}_filter_items
    ),
"""

end_table = f"""
    last_table as (select * from grouping_by_all_possible_controls)

    select
    month,
    brand,
    merchant,
    major_cat,
    sub_cat,
    minor_cat,
    channel,
    parent_brand,
    sub_brand,

    gmv
    from last_table
    WHERE brand = 'samsung'
    AND merchant = 'Home Depot'
    AND major_cat = 'Laundry Appliances'
    AND sub_cat = 'Combination Washer-Dryers'
    AND minor_cat = 'All Combination Washer-Dryers'
    --- AND special_attribute = 'Laundry Appliances'
    AND channel = 'ONLINE'
    AND parent_brand = 'SAMSUNG'
    and sub_brand = 'samsung'
    --- and month = '2024-09-01'

    order by month
"""

full_query_std = f"""
{part1_std}
{end_table}
"""


market_share_for_column_std = spark.sql(full_query_std)
market_share_for_column_std.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC brand,
# MAGIC merchant,
# MAGIC major_cat,
# MAGIC sub_cat,
# MAGIC minor_cat,
# MAGIC channel,
# MAGIC parent_brand,
# MAGIC sub_brand
# MAGIC
# MAGIC FROM yd_sensitive_corporate.ydx_lowes_analysts_silver.lowes_v38_filter_items
# MAGIC WHERE brand = 'samsung'
# MAGIC AND merchant_clean = 'Home Depot'
# MAGIC AND major_cat = 'Laundry Appliances'
# MAGIC AND sub_cat = 'Combination Washer-Dryers'
# MAGIC AND minor_cat = 'All Combination Washer-Dryers'
# MAGIC --- AND special_attribute = 'Laundry Appliances'
# MAGIC AND channel = 'ONLINE'
# MAGIC AND parent_brand = 'SAMSUNG'
# MAGIC and sub_brand = 'samsung'
# MAGIC --- and month_start = '2024-09-01'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # I think Melissa has mixed up Merchant x Merchan_clean
