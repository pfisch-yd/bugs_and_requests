# Databricks notebook source
yd_sensitive_corporate.ydx_thd_analysts_gold.sigma_competitor_share_v2_test_portal
yd_sensitive_corporate.ydx_thd_analysts_gold.sigma_competitor_share_v2

# COMMAND ----------

# MAGIC %md
# MAGIC [sigma_competitor_share_v2_test_portal](yipitdata-corporate.cloud.databricks.com/editor/notebooks/1214896439294806?o=3092962415911490)
# MAGIC
# MAGIC
# MAGIC [sigma_competitor_share_v2](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/3183149375076733?o=3092962415911490#command/3183149375076744)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC with
# MAGIC   -- Step 1: Load source data with department filter
# MAGIC   source_data as (
# MAGIC     select
# MAGIC       *
# MAGIC     from
# MAGIC       yd_sensitive_corporate.ydx_thd_analysts_gold.sigma_competitor_share_v2
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and (
# MAGIC         case
# MAGIC           when length (array_join (array {{Department-6}}, ',')) = 0 then true
# MAGIC           else department in {{Department-6}}
# MAGIC         end
# MAGIC       )
# MAGIC       and (
# MAGIC         case
# MAGIC           when length (array_join (array {{Quarter-1}}, ',')) = 0 then true
# MAGIC           else quarter in {{Quarter-1}}
# MAGIC         end
# MAGIC       )
# MAGIC       and (
# MAGIC         case
# MAGIC           when length (array_join (array {{Class-4}}, ',')) = 0 then true
# MAGIC           else class in {{Class-4}}
# MAGIC         end
# MAGIC       )
# MAGIC       and (
# MAGIC         case
# MAGIC           when length (array_join (array {{Sub-Class}}, ',')) = 0 then true
# MAGIC           else sub_class in {{Sub-Class}}
# MAGIC         end
# MAGIC       )
# MAGIC       and (
# MAGIC         case
# MAGIC           when length (array_join (array {{Merchant-3}}, ',')) = 0 then true
# MAGIC           else merchant in {{Merchant-3}}
# MAGIC         end
# MAGIC       )
# MAGIC       and (
# MAGIC         case
# MAGIC           when length (array_join (array {{Channel-5}}, ',')) = 0 then true
# MAGIC           else channel in {{Channel-5}}
# MAGIC         end
# MAGIC       )
# MAGIC   ),
# MAGIC   -- Step 2: Calculate total GMV by merchant
# MAGIC   merchant_total_gmv as (
# MAGIC     select
# MAGIC       merchant,
# MAGIC       sum(gmv) as total_gmv
# MAGIC     from
# MAGIC       source_data
# MAGIC     group by
# MAGIC       merchant
# MAGIC   ),
# MAGIC   -- Step 3: Boost Home Depot's GMV to prioritize it in ranking
# MAGIC   merchant_weighted_gmv as (
# MAGIC     select
# MAGIC       merchant,
# MAGIC       if (
# MAGIC         merchant = 'Home Depot',
# MAGIC         total_gmv * 10000000,
# MAGIC         total_gmv
# MAGIC       ) as weighted_gmv
# MAGIC     from
# MAGIC       merchant_total_gmv
# MAGIC   ),
# MAGIC   -- Step 4: Filter out null weighted GMV values
# MAGIC   merchant_gmv_filtered as (
# MAGIC     select
# MAGIC       merchant,
# MAGIC       weighted_gmv
# MAGIC     from
# MAGIC       merchant_weighted_gmv
# MAGIC     where
# MAGIC       weighted_gmv is not null
# MAGIC   ),
# MAGIC   -- Step 5: Rank merchants by weighted GMV
# MAGIC   merchant_ranked as (
# MAGIC     select
# MAGIC       merchant,
# MAGIC       weighted_gmv,
# MAGIC       rank() over (
# MAGIC         order by
# MAGIC           weighted_gmv desc
# MAGIC       ) as merchant_rank
# MAGIC     from
# MAGIC       merchant_gmv_filtered
# MAGIC   ),
# MAGIC   -- Step 6: Keep only top 15 merchants
# MAGIC   top_merchants as (
# MAGIC     select
# MAGIC       merchant,
# MAGIC       weighted_gmv,
# MAGIC       merchant_rank
# MAGIC     from
# MAGIC       merchant_ranked
# MAGIC     where
# MAGIC       merchant_rank <= 15
# MAGIC   ),
# MAGIC   -- Step 7: Get distinct quarters from source data
# MAGIC   distinct_quarters as (
# MAGIC     select distinct
# MAGIC       quarter
# MAGIC     from
# MAGIC       source_data
# MAGIC   ),
# MAGIC   -- Step 8: Rank quarters by descending order
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
# MAGIC   -- Step 9: Join source data with top merchants and quarter rankings
# MAGIC   data_with_merchant_and_quarter_ranks as (
# MAGIC     select
# MAGIC       sd.quarter,
# MAGIC       sd.merchant,
# MAGIC       sd.channel,
# MAGIC       sd.department,
# MAGIC       sd.class,
# MAGIC       sd.gmv,
# MAGIC       tm.merchant as top_merchant,
# MAGIC       tm.weighted_gmv,
# MAGIC       tm.merchant_rank,
# MAGIC       qr.quarter as ranked_quarter,
# MAGIC       qr.quarter_rank
# MAGIC     from
# MAGIC       source_data sd
# MAGIC       left join top_merchants tm on sd.merchant = tm.merchant
# MAGIC       left join quarters_ranked qr on sd.quarter = qr.quarter
# MAGIC   ),
# MAGIC   -- Step 10: Apply page filters (channel, class, quarter, and top merchants/quarters)
# MAGIC   filtered_data as (
# MAGIC     select
# MAGIC       quarter,
# MAGIC       merchant,
# MAGIC       channel,
# MAGIC       department,
# MAGIC       class,
# MAGIC       gmv,
# MAGIC       top_merchant,
# MAGIC       weighted_gmv,
# MAGIC       merchant_rank,
# MAGIC       ranked_quarter,
# MAGIC       quarter_rank
# MAGIC     from
# MAGIC       data_with_merchant_and_quarter_ranks
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and quarter_rank <= 17
# MAGIC       and quarter_rank = 1
# MAGIC   ),
# MAGIC   -- Step 11: Calculate total GMV sum
# MAGIC   final_result as (
# MAGIC     select
# MAGIC       sum(gmv) as sum_of_gmv
# MAGIC     from
# MAGIC       filtered_data
# MAGIC   )
# MAGIC select
# MAGIC   sum_of_gmv
# MAGIC from
# MAGIC   final_result
