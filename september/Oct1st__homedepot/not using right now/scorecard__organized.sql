with
-- Step 1: Load source data with department filter
source_data as (
  select
    *
  from
    yd_sensitive_corporate.ydx_thd_analysts_gold.sigma_competitor_share_v2
  where
    department in {{Department-6}}
),

-- Step 2: Calculate total GMV by merchant
merchant_total_gmv as (
  select
    merchant,
    sum(gmv) as total_gmv
  from
    source_data
  group by
    merchant
),

-- Step 3: Boost Home Depot's GMV to prioritize it in ranking
merchant_weighted_gmv as (
  select
    merchant,
    if (merchant = {{Merchant-3}}, total_gmv * 10000000, total_gmv) as weighted_gmv
  from
    merchant_total_gmv
),

-- Step 4: Filter out null weighted GMV values
merchant_gmv_filtered as (
  select
    merchant,
    weighted_gmv
  from
    merchant_weighted_gmv
  where
    weighted_gmv is not null
),

-- Step 5: Rank merchants by weighted GMV
merchant_ranked as (
  select
    merchant,
    weighted_gmv,
    rank() over (order by weighted_gmv desc) as merchant_rank
  from
    merchant_gmv_filtered
),

-- Step 6: Keep only top 15 merchants
top_merchants as (
  select
    merchant,
    weighted_gmv,
    merchant_rank
  from
    merchant_ranked
  where
    merchant_rank <= 15
),

-- Step 7: Get distinct quarters from source data
distinct_quarters as (
  select distinct
    quarter
  from
    source_data
),

-- Step 8: Rank quarters by descending order
quarters_ranked as (
  select
    quarter,
    rank() over (order by quarter desc) as quarter_rank
  from
    distinct_quarters
),

-- Step 9: Join source data with top merchants and quarter rankings
data_with_merchant_and_quarter_ranks as (
  select
    sd.quarter,
    sd.merchant,
    sd.channel,
    sd.department,
    sd.class,
    sd.gmv,
    tm.merchant as top_merchant,
    tm.weighted_gmv,
    tm.merchant_rank,
    qr.quarter as ranked_quarter,
    qr.quarter_rank
  from
    source_data sd
    left join top_merchants tm on sd.merchant = tm.merchant
    left join quarters_ranked qr on sd.quarter = qr.quarter
),

-- Step 10: Apply page filters (channel, class, quarter, and top merchants/quarters)
filtered_data as (
  select
    quarter,
    merchant,
    channel,
    department,
    class,
    gmv,
    top_merchant,
    weighted_gmv,
    merchant_rank,
    ranked_quarter,
    quarter_rank
  from
    data_with_merchant_and_quarter_ranks
  where
    channel = {{Channel-5}}
    and class = {{Class-4}}
    and quarter_rank <= 17
    and (
      lower(quarter) = lower('2025 1Q')
      or lower(quarter) is null
    )
),

-- Step 11: Calculate total GMV sum
final_result as (
  select
    sum(gmv) as sum_of_gmv
  from
    filtered_data
)

select
  sum_of_gmv
from
  final_result
