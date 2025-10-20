with
-- Step 1: Load source data with base filters
source_data as (
  select
    *
  from
    yd_sensitive_corporate.ydx_thd_analysts_gold.brand_view_dash_v3
  where
    1=1
    and merchant in {{Merchant-1}}

    and department = {{Department-5}}

    and sub_class = {{Sub-Class-2}}

    and class = {{Class-1}}

    and channel = {{Channel-7}}
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

-- Step 8: Get the latest quarter from the filter
latest_quarter as (
  select
    max(quarter) as max_quarter
  from
    source_data
  where
    quarter in {{Quarter-2}}
),

-- Step 9: Join source data with quarter rankings and latest quarter
data_with_quarter_ranks as (
  select
    sd.quarter,
    sd.department,
    sd.class,
    sd.sub_class,
    sd.channel,
    sd.merchant,
    sd.clean_brand,
    sd.rolling_merchant_gmv,
    qr.quarter_rank,
    lq.max_quarter
  from
    source_data sd
    left join quarters_ranked qr on sd.quarter = qr.quarter
    cross join latest_quarter lq
),

-- Step 10: Apply page filters for scorecard (latest quarter only)
filtered_data as (
  select
    quarter,
    department,
    class,
    sub_class,
    channel,
    merchant,
    clean_brand,
    rolling_merchant_gmv,
    quarter_rank
  from
    data_with_quarter_ranks
  where
    quarter_rank <= 17
    and quarter = max_quarter
    and clean_brand in {{Clean-Brand}}
),

-- Step 11: Calculate total market size (sum of all rolling merchant GMV for latest quarter)
market_size as (
  select
    sum(rolling_merchant_gmv) as total_market_size
  from
    filtered_data
),

last_table as (
  select
    total_market_size as market_size
  from
    market_size
)

select * from last_table
