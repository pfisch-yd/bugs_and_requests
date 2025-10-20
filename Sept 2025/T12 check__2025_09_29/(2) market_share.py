# Databricks notebook source
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from yipit_databricks_utils.future import create_table
from pyspark.sql.functions import current_timestamp

from pyspark.sql.functions import to_timestamp

# COMMAND ----------

# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/core/setup"

# COMMAND ----------

# DBTITLE 1,Market Share Package
def export_market_share(
        sandbox_schema,
        prod_schema,
        demo_name,
        special_attribute_column_original,
        start_date_of_data
    ): 
    special_attribute_column = special_attribute_column_original
    
    # lets create a loop for each item in special att list
    special_attribute_column.append("major_cat")

    mapping_display_interval_monthly = spark.sql(f"""
    select "Annually" as display_interval union all
    select "Quarterly" as display_interval union all
    select "Monthly" as display_interval union all
    select "Year-to-Date" as display_interval union all
    select "Trailing 12 Months" as display_interval union all
    select "Trailing 6 Months" as display_interval union all
    select "Trailing 3 Months" as display_interval
    """)

    create_table(prod_schema, demo_name+'_display_interval_monthly', mapping_display_interval_monthly, overwrite=True)

    for column in special_attribute_column:        
        market_share_for_column = spark.sql(f"""
            -- SET use_cached_result = false;
                         
            with 
            clean_special_attribute_and_columns as (
            select
                * except (merchant),
                coalesce(cast({column} as string), major_cat) as special_attribute,
                date_trunc('month', order_date) as trunc_day,
                brand,
                merchant_clean as merchant
                from {sandbox_schema}.{demo_name}_filter_items
            ),

            grouping_by_all_possible_controls as (
            SELECT
                trunc_day,
                channel,
                parent_brand, 
                brand,
                sub_brand,
                merchant, 
                major_cat,
                sub_cat,
                minor_cat,
                special_attribute, -- Notice: If no special attribute, then major
                -- and this is redundant. So this grouping goes from 10 args to 9 args 

                sum(gmv) as gmv, 
                count(*) as sample_size, 
                sum(item_price * item_quantity) as item_subtotal_observed, 
                sum(item_quantity) as item_quantity_observed
            from clean_special_attribute_and_columns
            group by 1,2,3,4,5,6,7,8,9,10
            ),

            --- Y : getting out of the main road
            calculate_date_range AS (
            SELECT
                *,
                min(trunc_day) over (partition by
                    channel,
                    parent_brand, 
                    brand,
                    sub_brand,
                    merchant, 
                    major_cat,
                    sub_cat,
                    minor_cat,
                    special_attribute
                ) AS min_day,
                max(trunc_day) over (partition by
                    channel,
                    parent_brand, 
                    brand,
                    sub_brand,
                    merchant, 
                    major_cat,
                    sub_cat,
                    minor_cat,
                    special_attribute
                ) AS max_day
            FROM grouping_by_all_possible_controls
            ),

            create_a_sequence_array as (
            select
                sequence(
                    0,
                    CAST(
                        months_between(
                            /* capped_end_day */
                            least(
                                add_months(max_day, 12),          -- allow up to +12 m
                                date_trunc('month', MAX(trunc_day) over ()) -- but never beyond max trunc_day of the entire table
                            ),
                            
                            /*min_day*/
                            greatest(
                            add_months(min_day, -12),
                            date('{start_date_of_data}')
                            )
                        ) AS INT
                    )
                )           AS month_sequence,
                *
            from calculate_date_range
            ),

            unnest_all_months as (
            SELECT
                create_a_sequence_array.*,
                min_day,
                greatest(add_months(min_day, -12), date('{start_date_of_data}')) as ref_day,
                max_day,
                n AS month_offset  -- Position in the array
            FROM create_a_sequence_array
            LATERAL VIEW posexplode(month_sequence) AS n, month_offset
            ),

            organize_data as (
                select
                --- if it is a non existing row, it will be used the filler row
                add_months(ref_day, month_offset) as trunc_day,

                --- clean some rows
                * except (
                    trunc_day,
                    month_sequence, ref_day, min_day, max_day, month_offset,
                    sample_size,gmv,item_subtotal_observed, item_quantity_observed
                    ),
                
                --- if it is a filler row, than metrics are 0 (not null)
                case when trunc_day = add_months(ref_day, month_offset) then sample_size else 0 end as sample_size,
                case when trunc_day = add_months(ref_day, month_offset) then gmv else 0 end as gmv,
                case when trunc_day = add_months(ref_day, month_offset) then item_subtotal_observed else 0 end as item_subtotal_observed,
                case when trunc_day = add_months(ref_day, month_offset) then item_quantity_observed else 0 end as item_quantity_observed
                from unnest_all_months
            ),

            deduplicate as (
                select
                    trunc_day,
                    channel,
                    parent_brand, 
                    brand,
                    sub_brand,
                    merchant, 
                    major_cat,
                    sub_cat,
                    minor_cat,
                    special_attribute,


                    sum(gmv) as gmv, 
                    sum(sample_size) as sample_size, 
                    sum(item_subtotal_observed) as item_subtotal_observed, 
                    sum(item_quantity_observed) as item_quantity_observed
                from
                organize_data
                group by all
            )

            select * from deduplicate
                """)
        
        module_name = '_market_share_for_column_'+column
        table_name = demo_name+'_market_share_for_column_'+column
        if column == "major_cat":
            table_name = demo_name+'_market_share_for_column_null'
            module_name = '_market_share_for_column_null'
        
        create_blueprints_table(module_name,
            prod_schema,
            table_name,
            market_share_for_column
            )

# COMMAND ----------

def export_market_share__new_schema(
        sandbox_schema,
        prod_schema,
        demo_name,
        special_attribute_column_original,
        start_date_of_data
    ): 
    special_attribute_column = special_attribute_column_original
    
    # lets create a loop for each item in special att list
    special_attribute_column.append("major_cat")

    mapping_display_interval_monthly = spark.sql(f"""
        select "Annually" as display_interval union all
        select "Quarterly" as display_interval union all
        select "Monthly" as display_interval union all
        select "Year-to-Date" as display_interval union all
        select "Trailing 12 Months" as display_interval union all
        select "Trailing 6 Months" as display_interval union all
        select "Trailing 3 Months" as display_interval
    """)

    create_table(prod_schema, demo_name+'_display_interval_monthly', mapping_display_interval_monthly, overwrite=True)

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
                major_cat,
                sub_cat,
                minor_cat,
                special_attribute
            ) AS min_day,
            max(date) over (partition by
                channel,
                parent_brand, 
                brand,
                sub_brand,
                merchant, 
                major_cat,
                sub_cat,
                minor_cat,
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
                /*min_day*/
                greatest(
                    add_months(min_day, -12),
                    date('{start_date_of_data}')
                )) AS INT) ) as month_sequence,
            *
        from calculate_date_range
        ),

        unnest_all_months as (
        SELECT
            create_a_sequence_array.*,
            min_day,
            greatest(add_months(min_day, -12), date('{start_date_of_data}')) as ref_day,
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
            major_cat,
            sub_cat,
            minor_cat,
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
            partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
            order by month_start ROWS BETWEEN 11 PRECEDING and current row) as gmv_trailing_12m,
        sum(units) OVER (
            partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
            order by month_start ROWS BETWEEN 11 PRECEDING and current row) as units_trailing_12m,
        sum(sample_size) OVER (
            partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
            order by month_start ROWS BETWEEN 11 PRECEDING and current row) as sample_size_trailing_12m,
        sum(month(date)) OVER (
            partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
            order by month_start ROWS BETWEEN 11 PRECEDING and current row) as partition_factor_trailing_12m,
        --- t6
        sum(gmv) OVER (
            partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
            order by month_start ROWS BETWEEN 5 PRECEDING and current row) as gmv_trailing_6m,
        sum(units) OVER (
            partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
            order by month_start ROWS BETWEEN 5 PRECEDING and current row) as units_trailing_6m,
        sum(sample_size) OVER (
            partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
            order by month_start ROWS BETWEEN 5 PRECEDING and current row) as sample_size_trailing_6m,
        sum(
            case when month(date) <= 6 then month(date) else month(date) - 6 end
        ) OVER (
            partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
            order by month_start ROWS BETWEEN 5 PRECEDING and current row) as partition_factor_trailing_6m,
        -- t3
        sum(gmv) OVER (
            partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
            order by month_start ROWS BETWEEN 2 PRECEDING and current row) as gmv_trailing_3m,
        sum(units) OVER (
            partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
            order by month_start ROWS BETWEEN 2 PRECEDING and current row) as units_trailing_3m,
        sum(sample_size) OVER (
            partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
            order by month_start ROWS BETWEEN 2 PRECEDING and current row) as sample_size_trailing_3m,
        sum(
            month(date) - month(date_trunc("quarter", date)) + 1
        ) OVER (
            partition by channel, parent_brand, brand, sub_brand, merchant, major_cat, sub_cat, minor_cat, special_attribute
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
        CAST(item_subtotal_observed AS DECIMAL(36,8)) as item_subtotal_observed,
        CAST(item_quantity_observed AS int) as item_quantity_observed
        from filter_rows
        )

        select
            date, 
            month_start, 
            month_end, 
            month, 
            quarter_start, 
            quarter_end, 
            quarter, 
            year_start, 
            year_end, 
            year, 
            max_month, 
            max_quarter, 
            max_date, 
            parent_brand, 
            brand, 
            sub_brand, 
            major_cat, 
            sub_cat, 
            minor_cat, 
            merchant, 
            channel, 
            special_attribute, 
            gmv, 
            units,
            sample_size,
            item_subtotal_observed,
            item_quantity_observed,
            gmv_trailing_3m,
            gmv_trailing_6m,
            gmv_trailing_12m,
            units_trailing_3m,
            units_trailing_6m,
            units_trailing_12m,
            sample_size_trailing_3m,
            sample_size_trailing_6m,
            sample_size_trailing_12m
        from fix_metric_data_types
    """


    for column in special_attribute_column:        
        # part1 receives column as input
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
                major_cat as major_cat,
                sub_cat as sub_cat,
                minor_cat as minor_cat,
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
        
        part1_nrf = f"""
            with 
            0clean_special_attribute_and_columns as (
            select
                * except (merchant, parent_brand, brand, sub_brand),
                coalesce(cast({column} as string), major_cat) as special_attribute,
                cast(date_trunc('day', order_date) as date) as date,
                coalesce(parent_brand, "unbranded") as parent_brand,
                coalesce(brand, "unbranded") as brand,
                coalesce(sub_brand, "unbranded") as sub_brand,
                merchant_clean as merchant
                from {sandbox_schema}.{demo_name}_filter_items
            ),

            clean_special_attribute_and_columns as (
            select
                nrf_month_for_trailing as date, 
                data_table.* except (date)
            from 0clean_special_attribute_and_columns as data_table
            left join
            yd_sensitive_corporate.ydx_internal_analysts_gold.454cal_added_columns as fiscal
            on data_table.date = fiscal.cal_day
            ),

            grouping_by_all_possible_controls as (
            SELECT
                date,
                channel,
                parent_brand, 
                brand,
                sub_brand,
                merchant, 
                major_cat as major_cat,
                sub_cat as sub_cat,
                minor_cat as minor_cat,
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
        
        full_query_std = f"""
        {part1_std}
        {part2}
        {part3}
        {part4_std}
        {part5}
        """
                
        full_query_nrf = f"""
        {part1_nrf}
        {part2}
        {part3}
        {part4_nrf}
        {part5}
        """
        
        # spark tables
        market_share_for_column_std = spark.sql(full_query_std)
        market_share_for_column_nrf = spark.sql(full_query_nrf)
        
        # add updated at
        market_share_for_column_std = market_share_for_column_std.withColumn("_updated_timestamp", current_timestamp())
        market_share_for_column_nrf = market_share_for_column_nrf.withColumn("_updated_timestamp", current_timestamp())

        # define name
        module_name = '_market_share_for_column_'+column+'_nrf_calendar'
        table_name_std = demo_name+'_market_share_for_column_'+column+'_standard_calendar'
        table_name_nrf = demo_name+'_market_share_for_column_'+column+'_nrf_calendar'
        if column == "major_cat":
            table_name_nrf = demo_name+'_market_share_for_column_null'+'_nrf_calendar'
            table_name_std = demo_name+'_market_share_for_column_null'+'_standard_calendar'
        
        # create table
        module_name = '_market_share_for_column_'+column+'_standard_calendar'
        create_blueprints_table(module_name,
        prod_schema,
        table_name_std,
        market_share_for_column_std
        )
        
        module_name = '_market_share_for_column_'+column+'_nrf_calendar'
        create_blueprints_table(module_name,
        prod_schema,
        table_name_nrf,
        market_share_for_column_nrf
        )

# COMMAND ----------

# DBTITLE 1,Module Wrapper
def run_export_market_share_module(
        sandbox_schema,
        prod_schema,
        demo_name,
        special_attribute_column_original,
        start_date_of_data
    ):

    export_market_share__new_schema(
        sandbox_schema,
        prod_schema,
        demo_name,
        special_attribute_column_original,
        start_date_of_data
    )

    export_market_share(
        sandbox_schema,
        prod_schema,
        demo_name,
        special_attribute_column_original,
        start_date_of_data
    )

    print('Succesfully exported Market Share module.')
