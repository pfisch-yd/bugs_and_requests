# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC The solution is to add a few more rows here
# MAGIC
# MAGIC `        create_a_sequence_array as (
# MAGIC         select
# MAGIC             sequence(0, CAST(months_between(
# MAGIC                 least(
# MAGIC                 add_months(max_day, 12),
# MAGIC                 date_trunc('month', MAX(date) over ())
# MAGIC                 )
# MAGIC                 , min_day) AS INT) ) as month_sequence,
# MAGIC             *
# MAGIC         from calculate_date_range
# MAGIC         ),`
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,BEFORE
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

part2 = f"""
    --- Y : getting out of the main road
    calculate_date_range AS (
    SELECT
        *,
        min(date) over (partition by
            channel,
            parent_brand, 
            brand,
            sub_brand,
            merchant, 
            major_category,
            sub_category,
            minor_category,
            special_attribute
        ) AS min_day,
        max(date) over (partition by
            channel,
            parent_brand, 
            brand,
            sub_brand,
            merchant, 
            major_category,
            sub_category,
            minor_category,
            special_attribute
        ) AS max_day
    FROM grouping_by_all_possible_controls
    ),
    
    create_a_sequence_array as (
    select
        sequence(0, CAST(months_between(
            least(
            add_months(max_day, 12),
            date_trunc('month', MAX(date) over ())
            )
            , min_day) AS INT) ) as month_sequence,
        *
    from calculate_date_range
    ),

    unnest_all_months as (
    SELECT
        create_a_sequence_array.*,
        min_day,
        max_day,
        n AS month_offset  -- Position in the array
    FROM create_a_sequence_array
    LATERAL VIEW posexplode(month_sequence) AS n, month_offset
    ),
"""



part3 = f"""
    organize_data as (
    select
    --- if it is a non existing row, it will be used the filler row
    cast(add_months(min_day, month_offset) as date) as date,

    --- clean some rows
    * except (
        date,
        month_sequence, min_day, max_day, month_offset,
        sample_size,gmv,item_subtotal_observed, item_quantity_observed
        ),

    --- if it is a filler row, than metrics are 0 (not null)
    case when date = add_months(min_day, month_offset) then sample_size else 0 end as sample_size,
    case when date = add_months(min_day, month_offset) then gmv else 0 end as gmv,
    case when date = add_months(min_day, month_offset) then item_subtotal_observed else 0 end as item_subtotal_observed,
    case when date = add_months(min_day, month_offset) then item_quantity_observed else 0 end as item_quantity_observed
    from unnest_all_months
    ),

    deduplicate as (
    select
        date,
        channel,
        parent_brand, 
        brand,
        sub_brand,
        merchant, 
        major_category,
        sub_category,
        minor_category,
        special_attribute,


        sum(gmv) as gmv, 
        sum(sample_size) as sample_size, 
        sum(item_subtotal_observed) as item_subtotal_observed, 
        sum(item_quantity_observed) as item_quantity_observed,
        try_divide(
            sum(gmv) ,
            try_divide(
            sum(item_subtotal_observed) ,
            sum(item_quantity_observed))
        ) as units
    from
    organize_data
    group by all
    ),
"""

part4_std = f"""
    adding_columns as (
    select
        date,

        cast(date_trunc('month', date) as date) as month_start,
        date_add(date_trunc('month', date_add(date_trunc('month', date),35)),-1) as month_end,
        month(date) as month,

        cast(date_trunc('quarter', date) as date) as quarter_start, 
        date_add(date_trunc('quarter', date_add(date_trunc('quarter', date),95)),-1) as quarter_end,
        quarter(date) as quarter,

        cast(date_trunc('year', date) as date) as year_start, 
        date_add(date_trunc('year', date_add(date_trunc('year', date),370)),-1) as year_end,
        year(date) as year,

        max(date) over () as max_date,

        *

    from deduplicate
    ),

    add_max_date_columns as (
    select
        * except (max_date),
        month(max_date) as max_month, 
        quarter(max_date) as max_quarter, 
        max_date
    from
    adding_columns
    ),
"""

part4_nrf = f"""
    adding_columns as (
    select
        cast(data_table.date as date) as date,
        cast(nrf_month_start as date) as month_start, 
        nrf_month_end as month_end, 
        nrf_month_number as month,
        cast(nrf_quarter_start as date) as quarter_start, 
        nrf_quarter_end as quarter_end, 
        nrf_quarter_rank as quarter,
        cast(nrf_year_start as date) as year_start, 
        nrf_year_end as year_end, 
        nrf_year as year,
        data_table.* except (date)
        
    from deduplicate as data_table
    left join
    yd_sensitive_corporate.ydx_internal_analysts_gold.454cal_by_month as fiscal
    on data_table.date = fiscal.nrf_month_for_trailing
    ),

    add_max_date_columns as (
    select
        *,
        max(month) over () as max_month,
        max(quarter) over () as max_quarter, 
        max(date) over () as max_date
    from
    adding_columns
    ),
"""

part5 = f"""
    add_sumover as(
    select
    *,
    --- t12
    sum(gmv) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 11 PRECEDING and current row) as gmv_trailing_12m,
    sum(units) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 11 PRECEDING and current row) as units_trailing_12m,
    sum(sample_size) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 11 PRECEDING and current row) as sample_size_trailing_12m,
    sum(month(date)) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 11 PRECEDING and current row) as partition_factor_trailing_12m,
    --- t6
    sum(gmv) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 5 PRECEDING and current row) as gmv_trailing_6m,
    sum(units) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 5 PRECEDING and current row) as units_trailing_6m,
    sum(sample_size) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 5 PRECEDING and current row) as sample_size_trailing_6m,
    sum(
        case when month(date) <= 6 then month(date) else month(date) - 6 end
    ) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 5 PRECEDING and current row) as partition_factor_trailing_6m,
    -- t3
    sum(gmv) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 2 PRECEDING and current row) as gmv_trailing_3m,
    sum(units) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 2 PRECEDING and current row) as units_trailing_3m,
    sum(sample_size) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 2 PRECEDING and current row) as sample_size_trailing_3m,
    sum(
        month(date) - month(date_trunc("quarter", date)) + 1
    ) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 2 PRECEDING and current row) as partition_factor_trailing_3m

    from add_max_date_columns
    ),

    filter_rows as (
    select
    * except (
        partition_factor_trailing_12m,
        gmv_trailing_12m,
        units_trailing_12m,
        sample_size_trailing_12m,
        partition_factor_trailing_6m,
        gmv_trailing_6m,
        units_trailing_6m,
        sample_size_trailing_6m,
        partition_factor_trailing_3m,
        gmv_trailing_3m,
        units_trailing_3m,
        sample_size_trailing_3m
        ),
    case when partition_factor_trailing_12m = 78 then gmv_trailing_12m end as gmv_trailing_12m,
    case when partition_factor_trailing_12m = 78 then units_trailing_12m end as units_trailing_12m,
    case when partition_factor_trailing_12m = 78 then sample_size_trailing_12m end as sample_size_trailing_12m,

    case when partition_factor_trailing_6m = 21 then gmv_trailing_6m end as gmv_trailing_6m,
    case when partition_factor_trailing_6m = 21 then units_trailing_6m end as units_trailing_6m,
    case when partition_factor_trailing_6m = 21 then sample_size_trailing_6m end as sample_size_trailing_6m,

    case when partition_factor_trailing_3m = 6 then gmv_trailing_3m end as gmv_trailing_3m,
    case when partition_factor_trailing_3m = 6 then units_trailing_3m end as units_trailing_3m,
    case when partition_factor_trailing_3m = 6 then sample_size_trailing_3m end as sample_size_trailing_3m
    from add_sumover
    ),

    fix_metric_data_types as (
    select
    * except (
        gmv,
        gmv_trailing_3m,
        gmv_trailing_6m,
        gmv_trailing_12m,
        units,
        units_trailing_3m,
        units_trailing_6m,
        units_trailing_12m,
        sample_size,
        sample_size_trailing_3m,
        sample_size_trailing_6m,
        sample_size_trailing_12m,
        item_subtotal_observed,
        item_quantity_observed
    ),
    CAST(gmv AS DECIMAL(36,8)) as gmv,
    CAST(gmv_trailing_3m AS DECIMAL(36,8)) as gmv_trailing_3m,
    CAST(gmv_trailing_6m AS DECIMAL(36,8)) as gmv_trailing_6m,
    CAST(gmv_trailing_12m AS DECIMAL(36,8)) as gmv_trailing_12m,
    CAST(units AS DECIMAL(36,8)) as units,
    CAST(units_trailing_3m AS DECIMAL(36,8)) as units_trailing_3m,
    CAST(units_trailing_6m AS DECIMAL(36,8)) as units_trailing_6m,
    CAST(units_trailing_12m AS DECIMAL(36,8)) as units_trailing_12m,
    CAST(sample_size AS int) as sample_size,
    CAST(sample_size_trailing_3m AS int) as sample_size_trailing_3m,
    CAST(sample_size_trailing_6m AS int) as sample_size_trailing_6m,
    CAST(sample_size_trailing_12m AS int) as sample_size_trailing_12m,
    CAST(item_subtotal_observed AS int) as item_subtotal_observed,
    CAST(item_quantity_observed AS int) as item_quantity_observed
    from filter_rows
    )
"""



end_table = f"""
    --- last_table as (select * from unnest_all_months)

    select
    date as month,
    brand,
    merchant,
    major_category,
    sub_category,
    minor_category,
    channel,
    parent_brand,
    sub_brand,

    gmv
    from fix_metric_data_types
    WHERE brand = 'samsung'
    AND merchant = 'Home Depot'
    AND major_category = 'Laundry Appliances'
    AND sub_category = 'Combination Washer-Dryers'
    AND minor_category = 'All Combination Washer-Dryers'
    AND special_attribute = 'Laundry Appliances'
    AND channel = 'ONLINE'
    AND parent_brand = 'SAMSUNG'
    and sub_brand = 'samsung'
    and month_start = '2024-09-01'

    order by month
"""

full_query_std = f"""
{part1_std}
{part2}
{part3}
{part4_std}
{part5}
{end_table}
"""


market_share_for_column_std = spark.sql(full_query_std)
market_share_for_column_std.display()

# COMMAND ----------

# DBTITLE 1,first attempt
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

part2 = f"""
    --- Y : getting out of the main road
    calculate_date_range AS (
    SELECT
        *,
        min(date) over (partition by
            channel,
            parent_brand, 
            brand,
            sub_brand,
            merchant, 
            major_category,
            sub_category,
            minor_category,
            special_attribute
        ) AS min_day,
        max(date) over (partition by
            channel,
            parent_brand, 
            brand,
            sub_brand,
            merchant, 
            major_category,
            sub_category,
            minor_category,
            special_attribute
        ) AS max_day
    FROM grouping_by_all_possible_controls
    ),
    
    create_a_sequence_array as (
    select
        sequence(0, CAST(months_between(
            least(
            add_months(max_day, 12),
            date_trunc('month', MAX(date) over ())
            )
            , 
            greatest(
            add_months(min_day, -14),
            date('2023-01-01')
            )
            ) AS INT) ) as month_sequence,
        *
    from calculate_date_range
    ),

    unnest_all_months as (
    SELECT
        create_a_sequence_array.*,
        min_day,
        max_day,
        n AS month_offset  -- Position in the array
    FROM create_a_sequence_array
    LATERAL VIEW posexplode(month_sequence) AS n, month_offset
    ),
"""



part3 = f"""
    organize_data as (
    select
    --- if it is a non existing row, it will be used the filler row
    cast(add_months(min_day, month_offset) as date) as date,

    --- clean some rows
    * except (
        date,
        month_sequence, min_day, max_day, month_offset,
        sample_size,gmv,item_subtotal_observed, item_quantity_observed
        ),

    --- if it is a filler row, than metrics are 0 (not null)
    case when date = add_months(min_day, month_offset) then sample_size else 0 end as sample_size,
    case when date = add_months(min_day, month_offset) then gmv else 0 end as gmv,
    case when date = add_months(min_day, month_offset) then item_subtotal_observed else 0 end as item_subtotal_observed,
    case when date = add_months(min_day, month_offset) then item_quantity_observed else 0 end as item_quantity_observed
    from unnest_all_months
    ),

    deduplicate as (
    select
        date,
        channel,
        parent_brand, 
        brand,
        sub_brand,
        merchant, 
        major_category,
        sub_category,
        minor_category,
        special_attribute,


        sum(gmv) as gmv, 
        sum(sample_size) as sample_size, 
        sum(item_subtotal_observed) as item_subtotal_observed, 
        sum(item_quantity_observed) as item_quantity_observed,
        try_divide(
            sum(gmv) ,
            try_divide(
            sum(item_subtotal_observed) ,
            sum(item_quantity_observed))
        ) as units
    from
    organize_data
    group by all
    ),
"""

part4_std = f"""
    adding_columns as (
    select
        date,

        cast(date_trunc('month', date) as date) as month_start,
        date_add(date_trunc('month', date_add(date_trunc('month', date),35)),-1) as month_end,
        month(date) as month,

        cast(date_trunc('quarter', date) as date) as quarter_start, 
        date_add(date_trunc('quarter', date_add(date_trunc('quarter', date),95)),-1) as quarter_end,
        quarter(date) as quarter,

        cast(date_trunc('year', date) as date) as year_start, 
        date_add(date_trunc('year', date_add(date_trunc('year', date),370)),-1) as year_end,
        year(date) as year,

        max(date) over () as max_date,

        *

    from deduplicate
    ),

    add_max_date_columns as (
    select
        * except (max_date),
        month(max_date) as max_month, 
        quarter(max_date) as max_quarter, 
        max_date
    from
    adding_columns
    ),
"""

part4_nrf = f"""
    adding_columns as (
    select
        cast(data_table.date as date) as date,
        cast(nrf_month_start as date) as month_start, 
        nrf_month_end as month_end, 
        nrf_month_number as month,
        cast(nrf_quarter_start as date) as quarter_start, 
        nrf_quarter_end as quarter_end, 
        nrf_quarter_rank as quarter,
        cast(nrf_year_start as date) as year_start, 
        nrf_year_end as year_end, 
        nrf_year as year,
        data_table.* except (date)
        
    from deduplicate as data_table
    left join
    yd_sensitive_corporate.ydx_internal_analysts_gold.454cal_by_month as fiscal
    on data_table.date = fiscal.nrf_month_for_trailing
    ),

    add_max_date_columns as (
    select
        *,
        max(month) over () as max_month,
        max(quarter) over () as max_quarter, 
        max(date) over () as max_date
    from
    adding_columns
    ),
"""

part5 = f"""
    add_sumover as(
    select
    *,
    --- t12
    sum(gmv) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 11 PRECEDING and current row) as gmv_trailing_12m,
    sum(units) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 11 PRECEDING and current row) as units_trailing_12m,
    sum(sample_size) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 11 PRECEDING and current row) as sample_size_trailing_12m,
    sum(month(date)) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 11 PRECEDING and current row) as partition_factor_trailing_12m,
    --- t6
    sum(gmv) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 5 PRECEDING and current row) as gmv_trailing_6m,
    sum(units) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 5 PRECEDING and current row) as units_trailing_6m,
    sum(sample_size) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 5 PRECEDING and current row) as sample_size_trailing_6m,
    sum(
        case when month(date) <= 6 then month(date) else month(date) - 6 end
    ) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 5 PRECEDING and current row) as partition_factor_trailing_6m,
    -- t3
    sum(gmv) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 2 PRECEDING and current row) as gmv_trailing_3m,
    sum(units) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 2 PRECEDING and current row) as units_trailing_3m,
    sum(sample_size) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 2 PRECEDING and current row) as sample_size_trailing_3m,
    sum(
        month(date) - month(date_trunc("quarter", date)) + 1
    ) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 2 PRECEDING and current row) as partition_factor_trailing_3m

    from add_max_date_columns
    ),

    filter_rows as (
    select
    * except (
        partition_factor_trailing_12m,
        gmv_trailing_12m,
        units_trailing_12m,
        sample_size_trailing_12m,
        partition_factor_trailing_6m,
        gmv_trailing_6m,
        units_trailing_6m,
        sample_size_trailing_6m,
        partition_factor_trailing_3m,
        gmv_trailing_3m,
        units_trailing_3m,
        sample_size_trailing_3m
        ),
    case when partition_factor_trailing_12m = 78 then gmv_trailing_12m end as gmv_trailing_12m,
    case when partition_factor_trailing_12m = 78 then units_trailing_12m end as units_trailing_12m,
    case when partition_factor_trailing_12m = 78 then sample_size_trailing_12m end as sample_size_trailing_12m,

    case when partition_factor_trailing_6m = 21 then gmv_trailing_6m end as gmv_trailing_6m,
    case when partition_factor_trailing_6m = 21 then units_trailing_6m end as units_trailing_6m,
    case when partition_factor_trailing_6m = 21 then sample_size_trailing_6m end as sample_size_trailing_6m,

    case when partition_factor_trailing_3m = 6 then gmv_trailing_3m end as gmv_trailing_3m,
    case when partition_factor_trailing_3m = 6 then units_trailing_3m end as units_trailing_3m,
    case when partition_factor_trailing_3m = 6 then sample_size_trailing_3m end as sample_size_trailing_3m
    from add_sumover
    ),

    fix_metric_data_types as (
    select
    * except (
        gmv,
        gmv_trailing_3m,
        gmv_trailing_6m,
        gmv_trailing_12m,
        units,
        units_trailing_3m,
        units_trailing_6m,
        units_trailing_12m,
        sample_size,
        sample_size_trailing_3m,
        sample_size_trailing_6m,
        sample_size_trailing_12m,
        item_subtotal_observed,
        item_quantity_observed
    ),
    CAST(gmv AS DECIMAL(36,8)) as gmv,
    CAST(gmv_trailing_3m AS DECIMAL(36,8)) as gmv_trailing_3m,
    CAST(gmv_trailing_6m AS DECIMAL(36,8)) as gmv_trailing_6m,
    CAST(gmv_trailing_12m AS DECIMAL(36,8)) as gmv_trailing_12m,
    CAST(units AS DECIMAL(36,8)) as units,
    CAST(units_trailing_3m AS DECIMAL(36,8)) as units_trailing_3m,
    CAST(units_trailing_6m AS DECIMAL(36,8)) as units_trailing_6m,
    CAST(units_trailing_12m AS DECIMAL(36,8)) as units_trailing_12m,
    CAST(sample_size AS int) as sample_size,
    CAST(sample_size_trailing_3m AS int) as sample_size_trailing_3m,
    CAST(sample_size_trailing_6m AS int) as sample_size_trailing_6m,
    CAST(sample_size_trailing_12m AS int) as sample_size_trailing_12m,
    CAST(item_subtotal_observed AS int) as item_subtotal_observed,
    CAST(item_quantity_observed AS int) as item_quantity_observed
    from filter_rows
    )
"""



end_table = f"""
    --- last_table as (select * from unnest_all_months)

    select
    date as month,
    brand,

    gmv
    from fix_metric_data_types
    WHERE brand = 'samsung'
    AND merchant = 'Home Depot'
    AND major_category = 'Laundry Appliances'
    AND sub_category = 'Combination Washer-Dryers'
    AND minor_category = 'All Combination Washer-Dryers'
    AND special_attribute = 'Laundry Appliances'
    AND channel = 'ONLINE'
    AND parent_brand = 'SAMSUNG'
    and sub_brand = 'samsung'
    --- and month_start = '2024-09-01'

    order by month
"""

full_query_std = f"""
{part1_std}
{part2}
{part3}
{part4_std}
{part5}
{end_table}
"""


market_share_for_column_std = spark.sql(full_query_std)
market_share_for_column_std.display()

# COMMAND ----------

# DBTITLE 1,taking complexity off pt1
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

            WHERE brand = 'samsung'
            AND merchant_clean = 'Home Depot'
            AND major_cat = 'Laundry Appliances'
            AND sub_cat = 'Combination Washer-Dryers'
            AND minor_cat = 'All Combination Washer-Dryers'
            AND coalesce(cast({column} as string), major_cat)= 'Laundry Appliances'
            AND channel = 'ONLINE'
            AND parent_brand = 'SAMSUNG'
            and sub_brand = 'samsung'
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

part2 = f"""
    --- Y : getting out of the main road
    calculate_date_range AS (
    SELECT
        *,
        min(date) over (partition by
            channel,
            parent_brand, 
            brand,
            sub_brand,
            merchant, 
            major_category,
            sub_category,
            minor_category,
            special_attribute
        ) AS min_day,
        max(date) over (partition by
            channel,
            parent_brand, 
            brand,
            sub_brand,
            merchant, 
            major_category,
            sub_category,
            minor_category,
            special_attribute
        ) AS max_day
    FROM grouping_by_all_possible_controls
    ),
    
    create_a_sequence_array as (
    select
        sequence(0, 24) as month_sequence,
        *
    from calculate_date_range
    where
    date = "2024-10-01"
    ),

    unnest_all_months as (
    SELECT
        create_a_sequence_array.*,
        min_day,
        max_day,
        n AS month_offset  -- Position in the array
    FROM create_a_sequence_array
    LATERAL VIEW posexplode(month_sequence) AS n, month_offset
    ),
"""

end_table = f"""
    last_table as (select * from unnest_all_months)

    select
    date as month,
    brand,

    gmv,

    min_day,
    max_day,
    month_sequence

    from last_table


    order by month
"""

full_query_std = f"""
{part1_std}
{part2}
{end_table}
"""


market_share_for_column_std = spark.sql(full_query_std)
market_share_for_column_std.display()

# COMMAND ----------

# DBTITLE 1,Investigating Pt3
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

            WHERE brand = 'samsung'
            AND merchant_clean = 'Home Depot'
            AND major_cat = 'Laundry Appliances'
            AND sub_cat = 'Combination Washer-Dryers'
            AND minor_cat = 'All Combination Washer-Dryers'
            AND coalesce(cast({column} as string), major_cat)= 'Laundry Appliances'
            AND channel = 'ONLINE'
            AND parent_brand = 'SAMSUNG'
            and sub_brand = 'samsung'
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

part2 = f"""
    --- Y : getting out of the main road
    calculate_date_range AS (
    SELECT
        *,
        min(date) over (partition by
            channel,
            parent_brand, 
            brand,
            sub_brand,
            merchant, 
            major_category,
            sub_category,
            minor_category,
            special_attribute
        ) AS min_day,
        max(date) over (partition by
            channel,
            parent_brand, 
            brand,
            sub_brand,
            merchant, 
            major_category,
            sub_category,
            minor_category,
            special_attribute
        ) AS max_day
    FROM grouping_by_all_possible_controls
    ),
    
    create_a_sequence_array as (
    select
        sequence(0, 30) as month_sequence,
        *
    from calculate_date_range
    where
    date = "2024-10-01"
    ),

    unnest_all_months as (
    SELECT
        create_a_sequence_array.*,
        min_day,
        greatest(add_months(min_day, -12), date("2023-01-01")) as ref_day,
        max_day,
        n AS month_offset  -- Position in the array
    FROM create_a_sequence_array
    LATERAL VIEW posexplode(month_sequence) AS n, month_offset
    ),
"""


part3 = f"""
    organize_data as (
    select
    --- if it is a non existing row, it will be used the filler row
    cast(add_months(ref_day, month_offset) as date) as date,

    --- clean some rows
    * except (
        date,
        month_sequence,min_day, max_day, month_offset,
        sample_size,gmv,item_subtotal_observed, item_quantity_observed
        ),

    --- if it is a filler row, than metrics are 0 (not null)
    case when date = add_months(ref_day, month_offset) then sample_size else 0 end as sample_size,
    case when date = add_months(ref_day, month_offset) then gmv else 0 end as gmv,
    case when date = add_months(ref_day, month_offset) then item_subtotal_observed else 0 end as item_subtotal_observed,
    case when date = add_months(ref_day, month_offset) then item_quantity_observed else 0 end as item_quantity_observed
    from unnest_all_months
    ),

    deduplicate as (
    select
        date,
        channel,
        parent_brand, 
        brand,
        sub_brand,
        merchant, 
        major_category,
        sub_category,
        minor_category,
        special_attribute,


        sum(gmv) as gmv, 
        sum(sample_size) as sample_size, 
        sum(item_subtotal_observed) as item_subtotal_observed, 
        sum(item_quantity_observed) as item_quantity_observed,
        try_divide(
            sum(gmv) ,
            try_divide(
            sum(item_subtotal_observed) ,
            sum(item_quantity_observed))
        ) as units
    from
    organize_data
    group by all
    ),
"""

end_table = f"""
    last_table as (select * from organize_data)

    select
    date as month,
    *



    from last_table


    order by month
"""

full_query_std = f"""
{part1_std}
{part2}
{part3}
{end_table}
"""


market_share_for_column_std = spark.sql(full_query_std)
market_share_for_column_std.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## I found out that I have to change a few things in PT3 to make it work.

# COMMAND ----------

# DBTITLE 1,second attempt
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

part2 = f"""
    --- Y : getting out of the main road
    calculate_date_range AS (
    SELECT
        *,
        min(date) over (partition by
            channel,
            parent_brand, 
            brand,
            sub_brand,
            merchant, 
            major_category,
            sub_category,
            minor_category,
            special_attribute
        ) AS min_day,
        max(date) over (partition by
            channel,
            parent_brand, 
            brand,
            sub_brand,
            merchant, 
            major_category,
            sub_category,
            minor_category,
            special_attribute
        ) AS max_day
    FROM grouping_by_all_possible_controls
    ),
    
    create_a_sequence_array as (
    select
        sequence(0, CAST(months_between(
            least(
            add_months(max_day, 12),
            date_trunc('month', MAX(date) over ())
            )
            , 
            greatest(
            add_months(min_day, -12),
            date('2023-01-01')
            )
            ) AS INT) ) as month_sequence,
        *
    from calculate_date_range
    ),

    unnest_all_months as (
    SELECT
        create_a_sequence_array.*,
        min_day,
        greatest(
            add_months(min_day, -12),
            date('2023-01-01')
            ) as ref_day, -- This is the date we are looking for
        max_day,
        n AS month_offset  -- Position in the array
    FROM create_a_sequence_array
    LATERAL VIEW posexplode(month_sequence) AS n, month_offset
    ),
"""



part3 = f"""
    organize_data as (
    select
    --- if it is a non existing row, it will be used the filler row
    cast(add_months(ref_day, month_offset) as date) as date,

    --- clean some rows
    * except (
        date,
        month_sequence, ref_day, min_day, max_day, month_offset,
        sample_size,gmv,item_subtotal_observed, item_quantity_observed
        ),

    --- if it is a filler row, than metrics are 0 (not null)
    case when date = add_months(ref_day, month_offset) then sample_size else 0 end as sample_size,
    case when date = add_months(ref_day, month_offset) then gmv else 0 end as gmv,
    case when date = add_months(ref_day, month_offset) then item_subtotal_observed else 0 end as item_subtotal_observed,
    case when date = add_months(ref_day, month_offset) then item_quantity_observed else 0 end as item_quantity_observed
    from unnest_all_months
    ),

    deduplicate as (
    select
        date,
        channel,
        parent_brand, 
        brand,
        sub_brand,
        merchant, 
        major_category,
        sub_category,
        minor_category,
        special_attribute,


        sum(gmv) as gmv, 
        sum(sample_size) as sample_size, 
        sum(item_subtotal_observed) as item_subtotal_observed, 
        sum(item_quantity_observed) as item_quantity_observed,
        try_divide(
            sum(gmv) ,
            try_divide(
            sum(item_subtotal_observed) ,
            sum(item_quantity_observed))
        ) as units
    from
    organize_data
    group by all
    ),
"""

part4_std = f"""
    adding_columns as (
    select
        date,

        cast(date_trunc('month', date) as date) as month_start,
        date_add(date_trunc('month', date_add(date_trunc('month', date),35)),-1) as month_end,
        month(date) as month,

        cast(date_trunc('quarter', date) as date) as quarter_start, 
        date_add(date_trunc('quarter', date_add(date_trunc('quarter', date),95)),-1) as quarter_end,
        quarter(date) as quarter,

        cast(date_trunc('year', date) as date) as year_start, 
        date_add(date_trunc('year', date_add(date_trunc('year', date),370)),-1) as year_end,
        year(date) as year,

        max(date) over () as max_date,

        *

    from deduplicate
    ),

    add_max_date_columns as (
    select
        * except (max_date),
        month(max_date) as max_month, 
        quarter(max_date) as max_quarter, 
        max_date
    from
    adding_columns
    ),
"""

part4_nrf = f"""
    adding_columns as (
    select
        cast(data_table.date as date) as date,
        cast(nrf_month_start as date) as month_start, 
        nrf_month_end as month_end, 
        nrf_month_number as month,
        cast(nrf_quarter_start as date) as quarter_start, 
        nrf_quarter_end as quarter_end, 
        nrf_quarter_rank as quarter,
        cast(nrf_year_start as date) as year_start, 
        nrf_year_end as year_end, 
        nrf_year as year,
        data_table.* except (date)
        
    from deduplicate as data_table
    left join
    yd_sensitive_corporate.ydx_internal_analysts_gold.454cal_by_month as fiscal
    on data_table.date = fiscal.nrf_month_for_trailing
    ),

    add_max_date_columns as (
    select
        *,
        max(month) over () as max_month,
        max(quarter) over () as max_quarter, 
        max(date) over () as max_date
    from
    adding_columns
    ),
"""

part5 = f"""
    add_sumover as(
    select
    *,
    --- t12
    sum(gmv) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 11 PRECEDING and current row) as gmv_trailing_12m,
    sum(units) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 11 PRECEDING and current row) as units_trailing_12m,
    sum(sample_size) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 11 PRECEDING and current row) as sample_size_trailing_12m,
    sum(month(date)) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 11 PRECEDING and current row) as partition_factor_trailing_12m,
    --- t6
    sum(gmv) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 5 PRECEDING and current row) as gmv_trailing_6m,
    sum(units) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 5 PRECEDING and current row) as units_trailing_6m,
    sum(sample_size) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 5 PRECEDING and current row) as sample_size_trailing_6m,
    sum(
        case when month(date) <= 6 then month(date) else month(date) - 6 end
    ) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 5 PRECEDING and current row) as partition_factor_trailing_6m,
    -- t3
    sum(gmv) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 2 PRECEDING and current row) as gmv_trailing_3m,
    sum(units) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 2 PRECEDING and current row) as units_trailing_3m,
    sum(sample_size) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 2 PRECEDING and current row) as sample_size_trailing_3m,
    sum(
        month(date) - month(date_trunc("quarter", date)) + 1
    ) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 2 PRECEDING and current row) as partition_factor_trailing_3m

    from add_max_date_columns
    ),

    filter_rows as (
    select
    * except (
        partition_factor_trailing_12m,
        gmv_trailing_12m,
        units_trailing_12m,
        sample_size_trailing_12m,
        partition_factor_trailing_6m,
        gmv_trailing_6m,
        units_trailing_6m,
        sample_size_trailing_6m,
        partition_factor_trailing_3m,
        gmv_trailing_3m,
        units_trailing_3m,
        sample_size_trailing_3m
        ),
    case when partition_factor_trailing_12m = 78 then gmv_trailing_12m end as gmv_trailing_12m,
    case when partition_factor_trailing_12m = 78 then units_trailing_12m end as units_trailing_12m,
    case when partition_factor_trailing_12m = 78 then sample_size_trailing_12m end as sample_size_trailing_12m,

    case when partition_factor_trailing_6m = 21 then gmv_trailing_6m end as gmv_trailing_6m,
    case when partition_factor_trailing_6m = 21 then units_trailing_6m end as units_trailing_6m,
    case when partition_factor_trailing_6m = 21 then sample_size_trailing_6m end as sample_size_trailing_6m,

    case when partition_factor_trailing_3m = 6 then gmv_trailing_3m end as gmv_trailing_3m,
    case when partition_factor_trailing_3m = 6 then units_trailing_3m end as units_trailing_3m,
    case when partition_factor_trailing_3m = 6 then sample_size_trailing_3m end as sample_size_trailing_3m
    from add_sumover
    ),

    fix_metric_data_types as (
    select
    * except (
        gmv,
        gmv_trailing_3m,
        gmv_trailing_6m,
        gmv_trailing_12m,
        units,
        units_trailing_3m,
        units_trailing_6m,
        units_trailing_12m,
        sample_size,
        sample_size_trailing_3m,
        sample_size_trailing_6m,
        sample_size_trailing_12m,
        item_subtotal_observed,
        item_quantity_observed
    ),
    CAST(gmv AS DECIMAL(36,8)) as gmv,
    CAST(gmv_trailing_3m AS DECIMAL(36,8)) as gmv_trailing_3m,
    CAST(gmv_trailing_6m AS DECIMAL(36,8)) as gmv_trailing_6m,
    CAST(gmv_trailing_12m AS DECIMAL(36,8)) as gmv_trailing_12m,
    CAST(units AS DECIMAL(36,8)) as units,
    CAST(units_trailing_3m AS DECIMAL(36,8)) as units_trailing_3m,
    CAST(units_trailing_6m AS DECIMAL(36,8)) as units_trailing_6m,
    CAST(units_trailing_12m AS DECIMAL(36,8)) as units_trailing_12m,
    CAST(sample_size AS int) as sample_size,
    CAST(sample_size_trailing_3m AS int) as sample_size_trailing_3m,
    CAST(sample_size_trailing_6m AS int) as sample_size_trailing_6m,
    CAST(sample_size_trailing_12m AS int) as sample_size_trailing_12m,
    CAST(item_subtotal_observed AS int) as item_subtotal_observed,
    CAST(item_quantity_observed AS int) as item_quantity_observed
    from filter_rows
    )
"""



end_table = f"""
    --- last_table as (select * from unnest_all_months)

    select
    date as month,
    brand,

    gmv
    from fix_metric_data_types
    WHERE brand = 'samsung'
    AND merchant = 'Home Depot'
    AND major_category = 'Laundry Appliances'
    AND sub_category = 'Combination Washer-Dryers'
    AND minor_category = 'All Combination Washer-Dryers'
    AND special_attribute = 'Laundry Appliances'
    AND channel = 'ONLINE'
    AND parent_brand = 'SAMSUNG'
    and sub_brand = 'samsung'
    --- and month_start = '2024-09-01'

    order by month
"""

full_query_std = f"""
{part1_std}
{part2}
{part3}
{part4_std}
{part5}
{end_table}
"""


market_share_for_column_std = spark.sql(full_query_std)
market_share_for_column_std.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## problems with date column

# COMMAND ----------

start_date_of_data = "2021-01-01"

query = f"""
    select
    order_date,
    {start_date_of_data} as hey,
    '{start_date_of_data}' as hey2,
    date('{start_date_of_data}') as hey3,
    months_between(order_date, date('{start_date_of_data}') ) as hey4
    from {sandbox_schema}.{demo_name}_filter_items

    limit 20

"""

market_share_for_column_std = spark.sql(query)
market_share_for_column_std.display()

# COMMAND ----------

# DBTITLE 1,thrid attempt
sandbox_schema = "ydx_lowes_analysts_silver"
demo_name = "lowes_v38"
column = "major_cat"
start_date_of_data = "2021-01-01"

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

part2 = f"""
    --- Y : getting out of the main road
    calculate_date_range AS (
    SELECT
        *,
        min(date) over (partition by
            channel,
            parent_brand, 
            brand,
            sub_brand,
            merchant, 
            major_category,
            sub_category,
            minor_category,
            special_attribute
        ) AS min_day,
        max(date) over (partition by
            channel,
            parent_brand, 
            brand,
            sub_brand,
            merchant, 
            major_category,
            sub_category,
            minor_category,
            special_attribute
        ) AS max_day
    FROM grouping_by_all_possible_controls
    ),
    
    create_a_sequence_array as (
    select
        sequence(0, CAST(months_between(
            least(
            add_months(max_day, 12),
            date_trunc('month', MAX(date) over ())
            )
            , 
            greatest(
            add_months(min_day, -12),
            date('2023-01-01')
            )
            ) AS INT) ) as month_sequence,
        *
    from calculate_date_range
    ),

    unnest_all_months as (
    SELECT
        create_a_sequence_array.*,
        min_day,
        greatest(
            add_months(min_day, -12),
            date('2023-01-01')
            ) as ref_day, -- This is the date we are looking for
        max_day,
        n AS month_offset  -- Position in the array
    FROM create_a_sequence_array
    LATERAL VIEW posexplode(month_sequence) AS n, month_offset
    ),
"""



part3 = f"""
    organize_data as (
    select
    --- if it is a non existing row, it will be used the filler row
    cast(add_months(ref_day, month_offset) as date) as date,

    --- clean some rows
    * except (
        date,
        month_sequence, ref_day, min_day, max_day, month_offset,
        sample_size,gmv,item_subtotal_observed, item_quantity_observed
        ),

    --- if it is a filler row, than metrics are 0 (not null)
    case when date = add_months(ref_day, month_offset) then sample_size else 0 end as sample_size,
    case when date = add_months(ref_day, month_offset) then gmv else 0 end as gmv,
    case when date = add_months(ref_day, month_offset) then item_subtotal_observed else 0 end as item_subtotal_observed,
    case when date = add_months(ref_day, month_offset) then item_quantity_observed else 0 end as item_quantity_observed
    from unnest_all_months
    ),

    deduplicate as (
    select
        date,
        channel,
        parent_brand, 
        brand,
        sub_brand,
        merchant, 
        major_category,
        sub_category,
        minor_category,
        special_attribute,


        sum(gmv) as gmv, 
        sum(sample_size) as sample_size, 
        sum(item_subtotal_observed) as item_subtotal_observed, 
        sum(item_quantity_observed) as item_quantity_observed,
        try_divide(
            sum(gmv) ,
            try_divide(
            sum(item_subtotal_observed) ,
            sum(item_quantity_observed))
        ) as units
    from
    organize_data
    group by all
    ),
"""

part4_std = f"""
    adding_columns as (
    select
        date,

        cast(date_trunc('month', date) as date) as month_start,
        date_add(date_trunc('month', date_add(date_trunc('month', date),35)),-1) as month_end,
        month(date) as month,

        cast(date_trunc('quarter', date) as date) as quarter_start, 
        date_add(date_trunc('quarter', date_add(date_trunc('quarter', date),95)),-1) as quarter_end,
        quarter(date) as quarter,

        cast(date_trunc('year', date) as date) as year_start, 
        date_add(date_trunc('year', date_add(date_trunc('year', date),370)),-1) as year_end,
        year(date) as year,

        max(date) over () as max_date,

        *

    from deduplicate
    ),

    add_max_date_columns as (
    select
        * except (max_date),
        month(max_date) as max_month, 
        quarter(max_date) as max_quarter, 
        max_date
    from
    adding_columns
    ),
"""

part4_nrf = f"""
    adding_columns as (
    select
        cast(data_table.date as date) as date,
        cast(nrf_month_start as date) as month_start, 
        nrf_month_end as month_end, 
        nrf_month_number as month,
        cast(nrf_quarter_start as date) as quarter_start, 
        nrf_quarter_end as quarter_end, 
        nrf_quarter_rank as quarter,
        cast(nrf_year_start as date) as year_start, 
        nrf_year_end as year_end, 
        nrf_year as year,
        data_table.* except (date)
        
    from deduplicate as data_table
    left join
    yd_sensitive_corporate.ydx_internal_analysts_gold.454cal_by_month as fiscal
    on data_table.date = fiscal.nrf_month_for_trailing
    ),

    add_max_date_columns as (
    select
        *,
        max(month) over () as max_month,
        max(quarter) over () as max_quarter, 
        max(date) over () as max_date
    from
    adding_columns
    ),
"""

part5 = f"""
    add_sumover as(
    select
    *,
    --- t12
    sum(gmv) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 11 PRECEDING and current row) as gmv_trailing_12m,
    sum(units) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 11 PRECEDING and current row) as units_trailing_12m,
    sum(sample_size) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 11 PRECEDING and current row) as sample_size_trailing_12m,
    sum(month(date)) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 11 PRECEDING and current row) as partition_factor_trailing_12m,
    --- t6
    sum(gmv) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 5 PRECEDING and current row) as gmv_trailing_6m,
    sum(units) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 5 PRECEDING and current row) as units_trailing_6m,
    sum(sample_size) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 5 PRECEDING and current row) as sample_size_trailing_6m,
    sum(
        case when month(date) <= 6 then month(date) else month(date) - 6 end
    ) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 5 PRECEDING and current row) as partition_factor_trailing_6m,
    -- t3
    sum(gmv) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 2 PRECEDING and current row) as gmv_trailing_3m,
    sum(units) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 2 PRECEDING and current row) as units_trailing_3m,
    sum(sample_size) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 2 PRECEDING and current row) as sample_size_trailing_3m,
    sum(
        month(date) - month(date_trunc("quarter", date)) + 1
    ) OVER (
        partition by channel, parent_brand, brand, sub_brand, merchant, major_category, sub_category, minor_category, special_attribute
        order by month_start ROWS BETWEEN 2 PRECEDING and current row) as partition_factor_trailing_3m

    from add_max_date_columns
    ),

    filter_rows as (
    select
    * except (
        partition_factor_trailing_12m,
        gmv_trailing_12m,
        units_trailing_12m,
        sample_size_trailing_12m,
        partition_factor_trailing_6m,
        gmv_trailing_6m,
        units_trailing_6m,
        sample_size_trailing_6m,
        partition_factor_trailing_3m,
        gmv_trailing_3m,
        units_trailing_3m,
        sample_size_trailing_3m
        ),
    case when partition_factor_trailing_12m = 78 then gmv_trailing_12m end as gmv_trailing_12m,
    case when partition_factor_trailing_12m = 78 then units_trailing_12m end as units_trailing_12m,
    case when partition_factor_trailing_12m = 78 then sample_size_trailing_12m end as sample_size_trailing_12m,

    case when partition_factor_trailing_6m = 21 then gmv_trailing_6m end as gmv_trailing_6m,
    case when partition_factor_trailing_6m = 21 then units_trailing_6m end as units_trailing_6m,
    case when partition_factor_trailing_6m = 21 then sample_size_trailing_6m end as sample_size_trailing_6m,

    case when partition_factor_trailing_3m = 6 then gmv_trailing_3m end as gmv_trailing_3m,
    case when partition_factor_trailing_3m = 6 then units_trailing_3m end as units_trailing_3m,
    case when partition_factor_trailing_3m = 6 then sample_size_trailing_3m end as sample_size_trailing_3m
    from add_sumover
    ),

    fix_metric_data_types as (
    select
    * except (
        gmv,
        gmv_trailing_3m,
        gmv_trailing_6m,
        gmv_trailing_12m,
        units,
        units_trailing_3m,
        units_trailing_6m,
        units_trailing_12m,
        sample_size,
        sample_size_trailing_3m,
        sample_size_trailing_6m,
        sample_size_trailing_12m,
        item_subtotal_observed,
        item_quantity_observed
    ),
    CAST(gmv AS DECIMAL(36,8)) as gmv,
    CAST(gmv_trailing_3m AS DECIMAL(36,8)) as gmv_trailing_3m,
    CAST(gmv_trailing_6m AS DECIMAL(36,8)) as gmv_trailing_6m,
    CAST(gmv_trailing_12m AS DECIMAL(36,8)) as gmv_trailing_12m,
    CAST(units AS DECIMAL(36,8)) as units,
    CAST(units_trailing_3m AS DECIMAL(36,8)) as units_trailing_3m,
    CAST(units_trailing_6m AS DECIMAL(36,8)) as units_trailing_6m,
    CAST(units_trailing_12m AS DECIMAL(36,8)) as units_trailing_12m,
    CAST(sample_size AS int) as sample_size,
    CAST(sample_size_trailing_3m AS int) as sample_size_trailing_3m,
    CAST(sample_size_trailing_6m AS int) as sample_size_trailing_6m,
    CAST(sample_size_trailing_12m AS int) as sample_size_trailing_12m,
    CAST(item_subtotal_observed AS int) as item_subtotal_observed,
    CAST(item_quantity_observed AS int) as item_quantity_observed
    from filter_rows
    )
"""



end_table = f"""
    --- last_table as (select * from unnest_all_months)

    select
    date as month,
    brand,

    gmv
    from fix_metric_data_types
    WHERE brand = 'samsung'
    AND merchant = 'Home Depot'
    AND major_category = 'Laundry Appliances'
    AND sub_category = 'Combination Washer-Dryers'
    AND minor_category = 'All Combination Washer-Dryers'
    AND special_attribute = 'Laundry Appliances'
    AND channel = 'ONLINE'
    AND parent_brand = 'SAMSUNG'
    and sub_brand = 'samsung'
    --- and month_start = '2024-09-01'

    order by month
"""

full_query_std = f"""
{part1_std}
{part2}
{part3}
{part4_std}
{part5}
{end_table}
"""


market_share_for_column_std = spark.sql(full_query_std)
market_share_for_column_std.display()
