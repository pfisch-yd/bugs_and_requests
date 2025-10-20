with
-- Step 1: Load source data with filters
source_data as (
  select
    *
  from
    yd_sensitive_corporate.ydx_thd_analysts_gold.brand_view_dash_v3
  where
    merchant = {{Merchant-3}}
    and department = {{Department-6}}
    and sub_class = {{SubClass-7}}
    and class = {{Class-4}}
),

-- Step 2: Calculate total rolling merchant GMV by brand
brand_total_gmv as (
  select
    clean_brand,
    sum(rolling_merchant_gmv) as total_rolling_gmv
  from
    source_data
  group by
    clean_brand
),

-- Step 3: Filter out null GMV values
brand_gmv_filtered as (
  select
    clean_brand,
    total_rolling_gmv
  from
    brand_total_gmv
  where
    total_rolling_gmv is not null
),

-- Step 4: Rank brands by total rolling GMV
brand_ranked as (
  select
    clean_brand,
    total_rolling_gmv,
    rank() over (order by total_rolling_gmv desc) as brand_rank
  from
    brand_gmv_filtered
),

-- Step 5: Keep only top 15 brands
top_brands as (
  select
    clean_brand,
    total_rolling_gmv,
    brand_rank
  from
    brand_ranked
  where
    brand_rank <= 15
),

-- Step 6: Get distinct quarters from source data
distinct_quarters as (
  select distinct
    quarter
  from
    source_data
),

-- Step 7: Rank quarters by descending order
quarters_ranked as (
  select
    quarter,
    rank() over (order by quarter desc) as quarter_rank
  from
    distinct_quarters
),

-- Step 8: Join source data with top brands and quarter rankings
data_with_brand_and_quarter_ranks as (
  select
    sd.quarter,
    sd.clean_brand,
    sd.channel,
    sd.department,
    sd.class,
    sd.sub_class,
    sd.merchant,
    sd.rolling_merchant_gmv,
    tb.clean_brand as top_brand,
    tb.total_rolling_gmv,
    tb.brand_rank,
    qr.quarter as ranked_quarter,
    qr.quarter_rank
  from
    source_data sd
    left join top_brands tb on sd.clean_brand = tb.clean_brand
    left join quarters_ranked qr on sd.quarter = qr.quarter
),

-- Step 9: Apply page filters (channel, quarter)
filtered_data as (
  select
    quarter,
    clean_brand,
    channel,
    department,
    class,
    sub_class,
    merchant,
    rolling_merchant_gmv,
    top_brand,
    total_rolling_gmv,
    brand_rank,
    ranked_quarter,
    quarter_rank
  from
    data_with_brand_and_quarter_ranks
  where
    channel = {{Channel-5}}
    and quarter_rank <= 17
    and (
      lower(quarter) = lower('2024 3Q')
      or lower(quarter) is null
    )
),

-- Step 10: Calculate total rolling merchant GMV sum
final_result as (
  select
    sum(rolling_merchant_gmv) as sum_of_rolling_merchant_gmv
  from
    filtered_data
)

select
  sum_of_rolling_merchant_gmv
from
  final_result
