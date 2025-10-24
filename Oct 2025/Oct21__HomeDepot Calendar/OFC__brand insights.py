# Databricks notebook source
yd_sensitive_corporate.ydx_thd_analysts_gold.brand_view_dash_v3_test_portal
yd_sensitive_corporate.ydx_thd_analysts_gold.brand_view_dash_v3

# COMMAND ----------

# MAGIC %md
# MAGIC [brand_view_dash_v3_test_portal](yipitdata-corporate.cloud.databricks.com/editor/notebooks/1214896439294806?o=3092962415911490)
# MAGIC
# MAGIC [brand_view_dash_v3](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/3183149375076733?o=3092962415911490)

# COMMAND ----------

# MAGIC %sql
# MAGIC with
# MAGIC   -- Step 1: Load source data with base filters
# MAGIC   source_data as (
# MAGIC     select
# MAGIC       *
# MAGIC     from
# MAGIC       yd_sensitive_corporate.ydx_thd_analysts_gold.brand_view_dash_v3
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and (
# MAGIC         case
# MAGIC           when length (array_join (array {{Merchant-1}}, ',')) = 0 then true
# MAGIC           else merchant in {{Merchant-1}}
# MAGIC         end
# MAGIC       )
# MAGIC       and (
# MAGIC         case
# MAGIC           when length (array_join (array {{Department-5}}, ',')) = 0 then true
# MAGIC           else department in {{Department-5}}
# MAGIC         end
# MAGIC       )
# MAGIC       and (
# MAGIC         case
# MAGIC           when length (array_join (array {{Sub-Class-2}}, ',')) = 0 then true
# MAGIC           else sub_class in {{Sub-Class-2}}
# MAGIC         end
# MAGIC       )
# MAGIC       and (
# MAGIC         case
# MAGIC           when length (array_join (array {{Class-1}}, ',')) = 0 then true
# MAGIC           else class in {{Class-1}}
# MAGIC         end
# MAGIC       )
# MAGIC       and (
# MAGIC         case
# MAGIC           when length (array_join (array {{Channel-7}}, ',')) = 0 then true
# MAGIC           else channel in {{Channel-7}}
# MAGIC         end
# MAGIC       )
# MAGIC       and (
# MAGIC         case
# MAGIC           when length (array_join (array {{Clean-Brand}}, ',')) = 0 then true
# MAGIC           else clean_brand in {{Clean-Brand}}
# MAGIC         end
# MAGIC       )
# MAGIC       and (
# MAGIC         case
# MAGIC           when length (array_join (array {{Quarter-2}}, ',')) = 0 then true
# MAGIC           else quarter in {{Quarter-2}}
# MAGIC         end
# MAGIC       )
# MAGIC   ),
# MAGIC   -- Step 2: Calculate total rolling merchant GMV by brand
# MAGIC   brand_total_gmv as (
# MAGIC     select
# MAGIC       clean_brand,
# MAGIC       sum(rolling_merchant_gmv) as total_rolling_gmv
# MAGIC     from
# MAGIC       source_data
# MAGIC     group by
# MAGIC       clean_brand
# MAGIC   ),
# MAGIC   -- Step 3: Filter out null GMV values
# MAGIC   brand_gmv_filtered as (
# MAGIC     select
# MAGIC       clean_brand,
# MAGIC       total_rolling_gmv
# MAGIC     from
# MAGIC       brand_total_gmv
# MAGIC     where
# MAGIC       total_rolling_gmv is not null
# MAGIC   ),
# MAGIC   -- Step 4: Rank brands by total rolling GMV
# MAGIC   brand_ranked as (
# MAGIC     select
# MAGIC       clean_brand,
# MAGIC       total_rolling_gmv,
# MAGIC       rank() over (
# MAGIC         order by
# MAGIC           total_rolling_gmv desc
# MAGIC       ) as brand_rank
# MAGIC     from
# MAGIC       brand_gmv_filtered
# MAGIC   ),
# MAGIC   -- Step 5: Keep only top 15 brands
# MAGIC   top_brands as (
# MAGIC     select
# MAGIC       clean_brand,
# MAGIC       total_rolling_gmv,
# MAGIC       brand_rank
# MAGIC     from
# MAGIC       brand_ranked
# MAGIC     where
# MAGIC       brand_rank <= 15
# MAGIC   ),
# MAGIC   -- Step 6: Get distinct quarters from source data
# MAGIC   distinct_quarters as (
# MAGIC     select distinct
# MAGIC       quarter
# MAGIC     from
# MAGIC       source_data
# MAGIC   ),
# MAGIC   -- Step 7: Rank quarters by descending order
# MAGIC   quarters_ranked as (
# MAGIC     select
# MAGIC       quarter,
# MAGIC       rank() over (
# MAGIC         order by
# MAGIC           quarter desc
# MAGIC       ) as quarter_rank
# MAGIC     from
# MAGIC       distinct_quarters
# MAGIC   ),
# MAGIC   -- Step 8: Get the latest quarter from the filter
# MAGIC   latest_quarter as (
# MAGIC     select
# MAGIC       max(quarter) as max_quarter
# MAGIC     from
# MAGIC       source_data
# MAGIC   ),
# MAGIC   -- Step 9: Join source data with quarter rankings and latest quarter
# MAGIC   data_with_quarter_ranks as (
# MAGIC     select
# MAGIC       sd.quarter,
# MAGIC       sd.department,
# MAGIC       sd.class,
# MAGIC       sd.sub_class,
# MAGIC       sd.channel,
# MAGIC       sd.merchant,
# MAGIC       sd.clean_brand,
# MAGIC       sd.rolling_merchant_gmv,
# MAGIC       qr.quarter_rank,
# MAGIC       lq.max_quarter
# MAGIC     from
# MAGIC       source_data sd
# MAGIC       left join quarters_ranked qr on sd.quarter = qr.quarter
# MAGIC       cross join latest_quarter lq
# MAGIC   ),
# MAGIC   -- Step 10: Apply page filters for scorecard (latest quarter only)
# MAGIC   filtered_data as (
# MAGIC     select
# MAGIC       quarter,
# MAGIC       department,
# MAGIC       class,
# MAGIC       sub_class,
# MAGIC       channel,
# MAGIC       merchant,
# MAGIC       clean_brand,
# MAGIC       rolling_merchant_gmv,
# MAGIC       quarter_rank
# MAGIC     from
# MAGIC       data_with_quarter_ranks
# MAGIC     where
# MAGIC       quarter_rank <= 17
# MAGIC       and quarter = max_quarter
# MAGIC   ),
# MAGIC   -- Step 11: Calculate total market size (sum of all rolling merchant GMV for latest quarter)
# MAGIC   market_size as (
# MAGIC     select
# MAGIC       sum(rolling_merchant_gmv) as total_market_size
# MAGIC     from
# MAGIC       filtered_data
# MAGIC   ),
# MAGIC   last_table as (
# MAGIC     select
# MAGIC       total_market_size as market_size
# MAGIC     from
# MAGIC       market_size
# MAGIC   )
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   last_table

# COMMAND ----------

# MAGIC %md
# MAGIC # INV

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC yd_sensitive_corporate.ydx_thd_analysts_silver.thd_calendar_quarters_fiscal_months
# MAGIC
# MAGIC limit 10

# COMMAND ----------


# ydx_thd_analysts_gold.quarterly_reports
# ydx_thd_analysts_silver.retailer_segment_mapping


brand_view_test = spark.sql(f"""
with quarters AS (
SELECT concat(substr(fiscal_year,1,4)," ", substr(fiscal_qtr,1,2)) as quarter
FROM ydx_thd_analysts_gold.quarterly_reports
GROUP BY 1
),

sub_classes AS (
SELECT department, class, case when sub_class='N/A' then "OTHER PRODUCTS" else sub_class end as sub_class, COALESCE(upper(brand), "UNBRANDED") as clean_brand 
FROM ydx_thd_analysts_gold.quarterly_reports
GROUP BY 1, 2, 3, 4
),

channels AS (
SELECT merchant, channel
FROM ydx_thd_analysts_gold.quarterly_reports
GROUP BY 1, 2
),

full_list AS (
SELECT a.*, b.quarter, c.merchant, c.channel
FROM sub_classes a
LEFT JOIN quarters b
LEFT JOIN channels c
),

        prep as
        (select * except(quarter), 
        concat(substr(fiscal_year,1,4)," ", substr(fiscal_qtr,1,2)) as quarter,
        COALESCE(upper(brand), "UNBRANDED") as clean_brand 
        from ydx_thd_analysts_gold.quarterly_reports a
        where source IN ('cat_geo', 'flooring')
        ),

        merchant_level as
        (select quarter, department, class, sub_class, channel, merchant, clean_brand, sum(gmv) as gmv
        from prep
        group by 1,2,3,4,5,6,7
        order by quarter desc),

        joined aS (
SELECT a.quarter, a.department, a.class, a.sub_class, a.merchant, a.channel, a.clean_brand, b.gmv
FROM full_list a
LEFT JOIN merchant_level b ON a.quarter = b.quarter AND a.department = b.department AND COALESCE(a.class, '') = COALESCE(b.class, '') AND COALESCE(a.sub_class, '') = COALESCE(b.sub_class, '') AND a.merchant = b.merchant AND a.channel = b.channel AND a.clean_brand = b.clean_brand
),

        final as
        (select quarter, a.department, a.class, a.sub_class, a.channel, b.final_name as merchant, clean_brand, sum(gmv) over (partition by a.department, a.class, a.sub_class, a.channel, a.merchant, a.clean_brand order by quarter rows between 3 preceding and current row) as rolling_merchant_gmv
        from joined a
        left join ydx_thd_analysts_silver.retailer_segment_mapping b
        on a.merchant = b.merchant
        order by quarter desc)

        select quarter, department, class, sub_class, case when channel='B&M' then "In-Store" when channel='ONLINE' then 'Online' end as channel, merchant, clean_brand, rolling_merchant_gmv
        from final
        where quarter <= '2025 2Q' --  and class not in ('C08 LIVE GOODS')   
        AND rolling_merchant_gmv IS NOT NULL 
        """)
