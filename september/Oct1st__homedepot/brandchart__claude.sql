select
  brand_name_5 Brand_Name,
  quarter_6 Quarter,
  SUM_8 `Sum_of_%_of_total_for_sum_of_rolling_merchant_gmv__by_x_axis_`
from
  (
    select
      brand_name brand_name_5,
      quarter quarter_6,
      sum(
        `%_of_total_for_sum_of_rolling_merchant_gmv__by_x_axis_`
      ) SUM_8
    from
      (
        with
          -- Step 1: Load source data with base filters
          source_data as (
            select
              *
            from
              yd_sensitive_corporate.ydx_thd_analysts_gold.brand_view_dash_v3
            where
              1 = 1
              and merchant in ('Home Depot', 'Walmart')
              and department = ('D23F WALL/FLOOR COVERING')
              and sub_class = ('SC09 CARPET TILE')
              and class = ('C02 STOCK CARPET/MODULAR/UTI')
              and channel = ('Online')
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
              rank() over (
                order by
                  total_rolling_gmv desc
              ) as brand_rank
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
              rank() over (
                order by
                  quarter desc
              ) as quarter_rank
            from
              distinct_quarters
          ),
          -- Step 8: Create comma-separated list of top brand names
          top_brands_list as (
            select
              array_join(
                array_compact (
                  transform
                    (
                      array_sort(
                        array_agg (
                          distinct struct (clean_brand as s0, clean_brand as target)
                        ),
                        (l, r) -> (
                          case
                            when (l) ['s0'] is null
                            and (r) ['s0'] is null then 0
                            when (l) ['s0'] is null then -1
                            when (r) ['s0'] is null then 1
                            when (l) ['s0'] < (r) ['s0'] then -1
                            when (l) ['s0'] > (r) ['s0'] then 1
                            else 0
                          end
                        )
                      ),
                      st -> (st) ['target']
                    )
                ),
                ', '
              ) as top_brands_string
            from
              top_brands
          ),
          -- Step 9: Join source data with quarter rankings
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
              qr.quarter_rank
            from
              source_data sd
              left join quarters_ranked qr on sd.quarter = qr.quarter
          ),
          -- Step 10: Apply page filters (channel, quarter, quarter rank)
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
              and clean_brand in ('TRAFFICMASTER', 'FOSS FLOORS')
              and quarter in ('2025 1Q', '2024 4Q', '2024 3Q')
          ),
          -- Step 11: Categorize brands (top brands or "Other Brands")
          data_with_brand_category as (
            select
              fd.quarter,
              fd.rolling_merchant_gmv,
              if (
                position(fd.clean_brand in tbl.top_brands_string) > 0,
                fd.clean_brand,
                'Other Brands'
              ) as brand_name
            from
              filtered_data fd
              cross join top_brands_list tbl
          ),
          -- Step 12: Calculate rolling merchant GMV by brand and quarter
          gmv_by_brand_and_quarter as (
            select
              brand_name,
              quarter,
              sum(rolling_merchant_gmv) as total_rolling_gmv
            from
              data_with_brand_category
            group by
              brand_name,
              quarter
          ),
          -- Step 13: Calculate total rolling merchant GMV by quarter (for share calculation)
          total_gmv_by_quarter as (
            select
              quarter,
              sum(rolling_merchant_gmv) as quarter_total_gmv
            from
              data_with_brand_category
            group by
              quarter
          ),
          -- Step 14: Calculate total rolling merchant GMV by brand (for sorting)
          total_gmv_by_brand as (
            select
              brand_name,
              sum(rolling_merchant_gmv) as brand_total_gmv
            from
              data_with_brand_category
            group by
              brand_name
          ),
          -- Step 15: Join all metrics together
          final_data as (
            select
              gb.brand_name,
              gb.quarter,
              gb.total_rolling_gmv,
              gb.total_rolling_gmv / nullif(tq.quarter_total_gmv, 0) as brand_share,
              tb.brand_total_gmv
            from
              gmv_by_brand_and_quarter gb
              left join total_gmv_by_quarter tq on gb.quarter = tq.quarter
              left join total_gmv_by_brand tb on gb.brand_name = tb.brand_name
          ),
          last_table as (
            select
              brand_name,
              brand_total_gmv as `sort`,
              quarter,
              brand_share as `%_of_total_for_sum_of_rolling_merchant_gmv__by_x_axis_`
            from
              final_data
            order by
              brand_total_gmv asc,
              brand_name asc,
              quarter asc
          )
        select
          *
        from
          last_table
      ) sql1
    group by
      brand_name,
      quarter
  ) Q1
order by
  brand_name_5 asc,
  quarter_6 asc
limit
  25001
  -- Sigma Σ {"sourceUrl":"https://app.sigmacomputing.com/yipit/workbook/Home-Depot-Merchant-Access-greater-than-greater-than-Oct1st-1o8XXhApQiLPhs78JjbZKY?:displayNodeId=vQcHQo0C-6","kind":"adhoc","request-id":"g0199a125725a7329a660ce0b083c0b2c","email":"pfisch@yipitdata.com"}