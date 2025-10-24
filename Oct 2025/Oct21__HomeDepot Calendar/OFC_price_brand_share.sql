-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # MAKES A. TRANSFORMATION!!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC yd_sensitive_corporate.ydx_internal_analysts_gold.market_size
-- MAGIC
-- MAGIC ydx_internal_analysts_gold.thd_price_bands_charts_vf
-- MAGIC
-- MAGIC ydx_thd_analysts_gold.quarterly_reports
-- MAGIC
-- MAGIC [ydx_internal_analysts_gold.market_size](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/1292773254552225?o=3092962415911490#command/8889646404517165)
-- MAGIC
-- MAGIC [thd_price_bands_charts_vf](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/3485706836239686?o=3092962415911490)
-- MAGIC
-- MAGIC [quarterly_reports](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/2654141369252334?o=3092962415911490)

-- COMMAND ----------

select
  "A" as label,
  {{price_step_starting_with}} as min,
  {{price_step_starting_with}} + {{price_step}} as max,
  concat (
    "A. (",
    cast({{price_step_starting_with}} as string),
    " - ",
    cast(
      (
        {{price_step_starting_with}} + {{price_step}}
      ) as string
    ),
    ")"
  ) as descr
union all
select
  "B" as label,
  {{price_step_starting_with}} + {{price_step}} as min,
  {{price_step_starting_with}} + 2 * {{price_step}} as max,
  concat (
    "B. (",
    cast(
      {{price_step_starting_with}} + 1 * {{price_step}} as string
    ),
    " - ",
    cast(
      (
        {{price_step_starting_with}} + 2 * {{price_step}}
      ) as string
    ),
    ")"
  )
union all
select
  "C" as label,
  {{price_step_starting_with}} + 2 * {{price_step}} as min,
  {{price_step_starting_with}} + 3 * {{price_step}} as max,
  concat (
    "C. (",
    cast(
      {{price_step_starting_with}} + 2 * {{price_step}} as string
    ),
    " - ",
    cast(
      (
        {{price_step_starting_with}} + 3 * {{price_step}}
      ) as string
    ),
    ")"
  )
union all
select
  "D" as label,
  {{price_step_starting_with}} + 3 * {{price_step}} as min,
  {{price_step_starting_with}} + 4 * {{price_step}} as max,
  concat (
    "D. (",
    cast(
      {{price_step_starting_with}} + 3 * {{price_step}} as string
    ),
    " - ",
    cast(
      (
        {{price_step_starting_with}} + 4 * {{price_step}}
      ) as string
    ),
    ")"
  )
union all
select
  "E" as label,
  {{price_step_starting_with}} + 4 * {{price_step}} as min,
  "+" as max,
  concat (
    "E. (",
    cast(
      {{price_step_starting_with}} + 4 * {{price_step}} as string
    ),
    " +",
    ")"
  )
  /*
  
  case
  when item_price_avg < {{price_step}} then concat("A. (0 - ", cast({{price_step}} as string), ")")
  when item_price_avg < 2*{{price_step}} then concat("B. (", cast({{price_step}} as string), " - ", cast(2*{{price_step}} as string), ")")
  when item_price_avg < 3*{{price_step}}  then concat("C. (", cast(2*{{price_step}} as string), " - ", cast(3*{{price_step}} as string), ")")
  when item_price_avg < 4*{{price_step}} then concat("D. (", cast(3*{{price_step}} as string), " - ", cast(4*{{price_step}} as string), ")")
  else concat("E. (", cast(4*{{price_step}} as string), " - ", cast(4*{{price_step}} as string), ")")
  end as price_band,
   */

-- COMMAND ----------

 ---- test
with
  prep as (
    select
      *
    from
      yd_sensitive_corporate.ydx_internal_analysts_gold.market_size
    WHERE
      -- (case when length(array_join(array{{pricing__page__brand-display}},',') ) = 0 then true else brand in {{pricing__page__brand-display}} end)
      -- and
      (
        case
          when length (array_join (array {{Department_pb}}, ',')) = 0 then true
          else department in {{Department_pb}}
        end
      )
      and (
        case
          when length (array_join (array {{Class_pb}}, ',')) = 0 then true
          else class in {{Class_pb}}
        end
      )
      and (
        case
          when length (array_join (array {{Sub-Class_pb}}, ',')) = 0 then true
          else sub_class in {{Sub-Class_pb}}
        end
      )
      and (
        case
          when length (array_join (array {{Merchant_pb}}, ',')) = 0 then true
          else merchant in {{Merchant_pb}}
        end
      )
      and (
        case
          when length (array_join (array {{Channel_pb}}, ',')) = 0 then true
          else channel in {{Channel_pb}}
        end
      )
      -- and
      -- (case when length(array_join(array{{Quarter}},',') ) = 0 then true else quarter in {{Quarter}} end)
  )
select
  sum(gmv)
from
  prep
where
  quarter = '2025 2Q'
  or quarter is null

-- COMMAND ----------



-- COMMAND ----------

with
  filters as (
    select
      *
    from
      ydx_internal_analysts_gold.thd_price_bands_charts_vf
    WHERE
      -- (case when length(array_join(array{{pricing__page__brand-display}},',') ) = 0 then true else brand in {{pricing__page__brand-display}} end)
      -- and
      (
        case
          when length (array_join (array {{Department_pb}}, ',')) = 0 then true
          else department in {{Department_pb}}
        end
      )
      and (
        case
          when length (array_join (array {{Class_pb}}, ',')) = 0 then true
          else class in {{Class_pb}}
        end
      )
      and (
        case
          when length (array_join (array {{Sub-Class_pb}}, ',')) = 0 then true
          else sub_class in {{Sub-Class_pb}}
        end
      )
      and (
        case
          when length (array_join (array {{Merchant_pb}}, ',')) = 0 then true
          else merchant in {{Merchant_pb}}
        end
      )
      and (
        case
          when length (array_join (array {{Channel_pb}}, ',')) = 0 then true
          else channel in {{Channel_pb}}
        end
      )
  ),
  sample_guardail_calc as (
    SELECT
      *,
      sum(count) OVER (
        partition by
          department
      ) as major_sample,
      sum(count) OVER (
        partition by
          class
      ) as sub_sample,
      sum(count) OVER (
        partition by
          sub_class
      ) as minor_sample,
      sum(count) OVER (
        partition by
          department,
          merchant
      ) as major_merchant_sample
    from
      filters
  ),
  clean_base as (
    select
      *
    from
      sample_guardail_calc
    where
      major_sample > 1500
  ),
  base_plus_price_band as (
    select
      quarter,
      department,
      class,
      sub_class,
      merchant,
      channel,
      case
        when {{Descr}} is null then "all_market"
        when item_price < {{price_step_starting_with}} + {{price_step}} then concat (
          "A. (",
          cast({{price_step_starting_with}} as string),
          " - ",
          cast(
            (
              {{price_step_starting_with}} + {{price_step}}
            ) as string
          ),
          ")"
        )
        when item_price < {{price_step_starting_with}} + 2 * {{price_step}} then concat (
          "B. (",
          cast(
            {{price_step_starting_with}} + 1 * {{price_step}} as string
          ),
          " - ",
          cast(
            (
              {{price_step_starting_with}} + 2 * {{price_step}}
            ) as string
          ),
          ")"
        )
        when item_price < {{price_step_starting_with}} + 3 * {{price_step}} then concat (
          "C. (",
          cast(
            {{price_step_starting_with}} + 2 * {{price_step}} as string
          ),
          " - ",
          cast(
            (
              {{price_step_starting_with}} + 3 * {{price_step}}
            ) as string
          ),
          ")"
        )
        when item_price < {{price_step_starting_with}} + 4 * {{price_step}} then concat (
          "D. (",
          cast(
            {{price_step_starting_with}} + 3 * {{price_step}} as string
          ),
          " - ",
          cast(
            (
              {{price_step_starting_with}} + 4 * {{price_step}}
            ) as string
          ),
          ")"
        )
        else concat (
          "E. (",
          cast(
            {{price_step_starting_with}} + 4 * {{price_step}} as string
          ),
          " +",
          ")"
        )
      end as price_band,
      sum(gmv) as gmv,
      count(*) as sample_size
    from
      clean_base
    group by
      1,
      2,
      3,
      4,
      5,
      6,
      7
  ),
  quarters AS (
    SELECT
      concat (
        substr (fiscal_year, 1, 4),
        " ",
        substr (fiscal_qtr, 1, 2)
      ) as quarter
    FROM
      ydx_thd_analysts_gold.quarterly_reports
    WHERE
      source IN ('cat_geo', 'flooring')
    GROUP BY
      1
  ),
  sub_classes AS (
    SELECT
      department,
      class,
      sub_class,
      price_band,
      merchant,
      channel
    FROM
      base_plus_price_band
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6
  ),
  full_list AS (
    SELECT
      a.*,
      b.quarter
    FROM
      sub_classes a
      LEFT JOIN quarters b
  ),
  joined as (
    SELECT
      a.quarter,
      a.department,
      a.class,
      a.sub_class,
      a.merchant,
      a.channel,
      a.price_band,
      b.gmv,
      b.sample_size
    FROM
      full_list a
      LEFT JOIN base_plus_price_band b ON a.quarter = b.quarter
      AND a.department = b.department
      AND COALESCE(a.class, '') = COALESCE(b.class, '')
      AND COALESCE(a.sub_class, '') = COALESCE(b.sub_class, '')
      AND a.merchant = b.merchant
      AND a.channel = b.channel
      and a.price_band = b.price_band
  ),
  agg_two AS (
    select
      quarter,
      department,
      class,
      sub_class,
      channel,
      merchant as final_name,
      price_band,
      sum(gmv) over (
        partition by
          a.department,
          a.class,
          a.sub_class,
          a.merchant,
          a.channel,
          a.price_band
        order by
          quarter rows between 3 preceding
          and current row
      ) as rolling_merchant_gmv,
      sum(sample_size) over (
        partition by
          a.department,
          a.class,
          a.merchant,
          a.channel,
          a.price_band
        order by
          quarter rows between 3 preceding
          and current row
      ) as rolling_sample_size
    from
      joined a
      -- left join ydx_internal_analysts_gold.retailer_segment_mapping_20250412 b  ----- UPDATE ONCE TABLE COMPLETE
      --   on a.merchant = b.merchant
  ),
  final_agg as (
    select
      *
    from
      agg_two
    WHERE
      rolling_merchant_gmv IS NOT NULL
      and (
        case
          when {{Descr}} is null then price_band = "all_market"
          when left ({{Descr}}, 1) = "A" then price_band = concat (
            "A. (",
            cast({{price_step_starting_with}} as string),
            " - ",
            cast(
              (
                {{price_step_starting_with}} + {{price_step}}
              ) as string
            ),
            ")"
          )
          when left ({{Descr}}, 1) = "B" then price_band = concat (
            "B. (",
            cast(
              {{price_step_starting_with}} + 1 * {{price_step}} as string
            ),
            " - ",
            cast(
              (
                {{price_step_starting_with}} + 2 * {{price_step}}
              ) as string
            ),
            ")"
          )
          when left ({{Descr}}, 1) = "C" then price_band = concat (
            "C. (",
            cast(
              {{price_step_starting_with}} + 2 * {{price_step}} as string
            ),
            " - ",
            cast(
              (
                {{price_step_starting_with}} + 3 * {{price_step}}
              ) as string
            ),
            ")"
          )
          when left ({{Descr}}, 1) = "D" then price_band = concat (
            "D. (",
            cast(
              {{price_step_starting_with}} + 3 * {{price_step}} as string
            ),
            " - ",
            cast(
              (
                {{price_step_starting_with}} + 4 * {{price_step}}
              ) as string
            ),
            ")"
          )
          when left ({{Descr}}, 1) = "E" then price_band = concat (
            "E. (",
            cast(
              {{price_step_starting_with}} + 4 * {{price_step}} as string
            ),
            " +",
            ")"
          )
          else price_band = concat (
            "E. (",
            cast(
              {{price_step_starting_with}} + 4 * {{price_step}} as string
            ),
            " +",
            ")"
          )
        end
      )
  ),
  total as (
    SELECT
      quarter,
      sum(rolling_merchant_gmv) as total_gmv
    FROM
      final_agg
    group by
      1
  ),
  segment as (
    SELECT
      quarter,
      final_name,
      sum(rolling_merchant_gmv) as segment_gmv
    FROM
      final_agg
    group by
      1,
      2
  ),
  top_brands as (
    SELECT
      final_name,
      sum(segment_gmv) as total_segment_gmv
    FROM
      segment
    GROUP BY
      1
    ORDER BY
      2 DESC
    LIMIT
      10
  ),
  final as (
    select
      a.quarter,
      a.final_name,
      a.segment_gmv / b.total_gmv as market_share
    from
      segment a
      cross join total b on a.quarter = b.quarter
    where
      a.quarter >= '2022 1Q'
      and a.quarter <= '2025 2Q'
      and
      -- (case when {{Descr}} is null then false else true end)
      -- and
      (
        case
          when length (array_join (array {{Quarter_pb}}, ',')) = 0 then true
          else a.quarter in {{Quarter_pb}}
        end
      )
  ),
  ------- ADDING PB LEVEL sample size constraints
  price_band_sample_guardail_calc as (
    select distinct
      price_band,
      avg(rolling_sample_size) as avg_rolling_sample_size,
      median (rolling_sample_size) as median_rolling_sample_size
    from
      agg_two
    group by
      1
  ),
  create_pb_sample_flag as (
    select
      *,
      case
        when avg_rolling_sample_size < 20 then 1
        else 0
      end as sample_size_flag_using_avg,
      case
        when median_rolling_sample_size < 8 then 1
        else 0
      end as sample_size_flag_using_median
    from
      price_band_sample_guardail_calc
  ),
  pb_sample_flag as (
    select
      sum(int (sample_size_flag_using_avg)) as sample_size_flag_avg,
      sum(int (sample_size_flag_using_median)) as sample_size_flag_median
    from
      create_pb_sample_flag
      -- group by all
  ),
  prep_last_cte as (
    select
      a.quarter,
      case
        when b.final_name is null then 'Other Retailers'
        else a.final_name
      end as final_name,
      sum(a.market_share) as market_share
    from
      final a
      left join top_brands b on a.final_name = b.final_name
    group by
      1,
      2
    order by
      2,
      3
  ),
  last_cte as (
    select
      a.*,
      b.sample_size_flag_avg,
      b.sample_size_flag_median
    from
      prep_last_cte a
      left join pb_sample_flag b
  )
select
  *,
  {{price_step_starting_with}} as y,
  {{price_step}} as yy
from
  last_cte
where
  case
    when {{price_step_starting_with}} + {{price_step}} = 0 --- and {{price_step}} = 0
    then false
    when {{price_step}} = 0 then false
    when {{Descr}} is not null
    AND (
      sample_size_flag_avg = 0
      OR sample_size_flag_median = 0
    ) then true
    else false
  end
  -- when {{price_step_starting_with}} + {{price_step}} = 0 and {{Descr}} is null
  -- then true
  -- when {{price_step_starting_with}} + {{price_step}} = 0 --- and {{price_step}} = 0
  -- then false
  -- when {{price_step}} = 0--- and {{price_step}} = 0
  -- then false
  -- -- when sample_size_flag > 0 then false
  -- else true end

-- COMMAND ----------

with
  filters as (
    select
      *
    from
      ydx_internal_analysts_gold.thd_price_bands_charts_vf
    WHERE
      -- (case when length(array_join(array{{pricing__page__brand-display}},',') ) = 0 then true else brand in {{pricing__page__brand-display}} end)
      -- and
      (
        case
          when length (array_join (array {{Department_pb}}, ',')) = 0 then true
          else department in {{Department_pb}}
        end
      )
      and (
        case
          when length (array_join (array {{Class_pb}}, ',')) = 0 then true
          else class in {{Class_pb}}
        end
      )
      and (
        case
          when length (array_join (array {{Sub-Class_pb}}, ',')) = 0 then true
          else sub_class in {{Sub-Class_pb}}
        end
      )
      and (
        case
          when length (array_join (array {{Merchant_pb}}, ',')) = 0 then true
          else merchant in {{Merchant_pb}}
        end
      )
      and (
        case
          when length (array_join (array {{Channel_pb}}, ',')) = 0 then true
          else channel in {{Channel_pb}}
        end
      )
  ),
  sample_guardail_calc as (
    SELECT
      *,
      sum(count) OVER (
        partition by
          department
      ) as major_sample,
      sum(count) OVER (
        partition by
          class
      ) as sub_sample,
      sum(count) OVER (
        partition by
          sub_class
      ) as minor_sample,
      sum(count) OVER (
        partition by
          department,
          merchant
      ) as major_merchant_sample
    from
      filters
  ),
  clean_base as (
    select
      *
    from
      sample_guardail_calc
    where
      major_sample > 1500
  ),
  base_plus_price_band as (
    select
      quarter,
      department,
      class,
      sub_class,
      merchant,
      channel,
      case
        when item_price < {{price_step_starting_with}} + {{price_step}} then concat (
          "A. (",
          cast({{price_step_starting_with}} as string),
          " - ",
          cast(
            (
              {{price_step_starting_with}} + {{price_step}}
            ) as string
          ),
          ")"
        )
        when item_price < {{price_step_starting_with}} + 2 * {{price_step}} then concat (
          "B. (",
          cast(
            {{price_step_starting_with}} + 1 * {{price_step}} as string
          ),
          " - ",
          cast(
            (
              {{price_step_starting_with}} + 2 * {{price_step}}
            ) as string
          ),
          ")"
        )
        when item_price < {{price_step_starting_with}} + 3 * {{price_step}} then concat (
          "C. (",
          cast(
            {{price_step_starting_with}} + 2 * {{price_step}} as string
          ),
          " - ",
          cast(
            (
              {{price_step_starting_with}} + 3 * {{price_step}}
            ) as string
          ),
          ")"
        )
        when item_price < {{price_step_starting_with}} + 4 * {{price_step}} then concat (
          "D. (",
          cast(
            {{price_step_starting_with}} + 3 * {{price_step}} as string
          ),
          " - ",
          cast(
            (
              {{price_step_starting_with}} + 4 * {{price_step}}
            ) as string
          ),
          ")"
        )
        else concat (
          "E. (",
          cast(
            {{price_step_starting_with}} + 4 * {{price_step}} as string
          ),
          " +",
          ")"
        )
      end as price_band,
      sum(gmv) as gmv,
      count(*) as sample_size
    from
      clean_base
    group by
      1,
      2,
      3,
      4,
      5,
      6,
      7
  ),
  quarters AS (
    SELECT
      concat (
        substr (fiscal_year, 1, 4),
        " ",
        substr (fiscal_qtr, 1, 2)
      ) as quarter
    FROM
      ydx_thd_analysts_gold.quarterly_reports
    WHERE
      source IN ('cat_geo', 'flooring')
    GROUP BY
      1
  ),
  sub_classes AS (
    SELECT
      department,
      class,
      sub_class,
      price_band,
      merchant,
      channel
    FROM
      base_plus_price_band
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6
  ),
  full_list AS (
    SELECT
      a.*,
      b.quarter
    FROM
      sub_classes a
      LEFT JOIN quarters b
  ),
  joined as (
    SELECT
      a.quarter,
      a.department,
      a.class,
      a.sub_class,
      a.merchant,
      a.channel,
      a.price_band,
      b.gmv,
      b.sample_size
    FROM
      full_list a
      LEFT JOIN base_plus_price_band b ON a.quarter = b.quarter
      AND a.department = b.department
      AND COALESCE(a.class, '') = COALESCE(b.class, '')
      AND COALESCE(a.sub_class, '') = COALESCE(b.sub_class, '')
      AND a.merchant = b.merchant
      AND a.channel = b.channel
      and a.price_band = b.price_band
  ),
  agg_two AS (
    select
      quarter,
      department,
      class,
      sub_class,
      channel,
      merchant as final_name,
      -- b.final_name, ----- UPDATE ONCE TABLE COMPLETE
      price_band,
      sum(gmv) over (
        partition by
          a.department,
          a.class,
          a.sub_class,
          a.merchant,
          a.channel,
          a.price_band
        order by
          quarter rows between 3 preceding
          and current row
      ) as rolling_merchant_gmv,
      sum(sample_size) over (
        partition by
          a.department,
          a.class,
          a.merchant,
          a.channel,
          a.price_band
        order by
          quarter rows between 3 preceding
          and current row
      ) as rolling_sample_size
    from
      joined a
      -- left join ydx_thd_analysts_silver.retailer_segment_mapping b  ----- UPDATE ONCE TABLE COMPLETE
      --   on a.merchant = b.merchant
  ),
  final as (
    select
      *
    from
      agg_two
    WHERE
      rolling_merchant_gmv IS NOT NULL
      -- and
      -- (
      --     case
      --     when left({{Descr}}, 1) = "A" then price_band =
      --     concat("A. (",  cast({{price_step_starting_with}} as string)," - ", cast( ({{price_step_starting_with}} + {{price_step}}) as string), ")")
      --     when left({{Descr}}, 1) = "B" then price_band = concat("B. (", cast({{price_step_starting_with}} + 1*{{price_step}} as string), " - ", cast( ({{price_step_starting_with}} + 2*{{price_step}}) as string), ")")
      --     when left({{Descr}}, 1) = "C"  then price_band = concat("C. (", cast({{price_step_starting_with}} + 2*{{price_step}} as string), " - ", cast( ({{price_step_starting_with}} + 3*{{price_step}}) as string), ")")
      --     when left({{Descr}}, 1) = "D" then price_band = concat("D. (", cast({{price_step_starting_with}} + 3*{{price_step}} as string), " - ", cast( ({{price_step_starting_with}} + 4*{{price_step}}) as string), ")")
      --     when left({{Descr}}, 1) = "E" then price_band = concat("E. (", cast({{price_step_starting_with}} + 4*{{price_step}} as string), " +", ")")
      --     else price_band = concat("E. (", cast({{price_step_starting_with}} + 4*{{price_step}} as string), " +", ")")
      --     end)
  ),
  total as (
    SELECT
      quarter,
      sum(rolling_merchant_gmv) as total_gmv
    FROM
      final
    group by
      1
  ),
  segment as (
    SELECT
      quarter,
      price_band,
      sum(rolling_merchant_gmv) as segment_gmv
    FROM
      final
    group by
      1,
      2
  ),
  last_cte_prep as (
    select
      a.quarter,
      a.price_band,
      a.segment_gmv / b.total_gmv as market_share
    from
      segment a
      cross join total b on a.quarter = b.quarter
    where
      a.quarter >= '2022 1Q'
      and a.quarter <= '2025 2Q'
  ),
  ------- ADDING PB LEVEL sample size constraints
  price_band_sample_guardail_calc as (
    select distinct
      price_band,
      avg(rolling_sample_size) as avg_rolling_sample_size,
      median (rolling_sample_size) as median_rolling_sample_size
    from
      agg_two
    group by
      1
  ),
  create_pb_sample_flag as (
    select
      *,
      case
        when avg_rolling_sample_size < 20 then 1
        else 0
      end as sample_size_flag_using_avg,
      case
        when median_rolling_sample_size < 8 then 1
        else 0
      end as sample_size_flag_using_median
    from
      price_band_sample_guardail_calc
  ),
  pb_sample_flag as (
    select
      sum(int (sample_size_flag_using_avg)) as sample_size_flag_avg,
      sum(int (sample_size_flag_using_median)) as sample_size_flag_median
    from
      create_pb_sample_flag
      -- group by all
  ),
  last_cte as (
    select
      a.*,
      b.sample_size_flag_avg,
      b.sample_size_flag_median
    from
      last_cte_prep a
      left join pb_sample_flag b
  )
select
  *
  -- {{price_step_starting_with}} as y,
  -- {{price_step}} as yy
from
  last_cte
where
  case
    when {{price_step_starting_with}} + {{price_step}} = 0 --- and {{price_step}} = 0
    then false
    when {{price_step}} = 0 then false
    when sample_size_flag_avg = 0
    OR sample_size_flag_median = 0 then true
    else false
  end
  -- when {{price_step_starting_with}} + {{price_step}} = 0 and {{Descr}} is null
  -- then true
  -- when {{price_step_starting_with}} + {{price_step}} = 0 --- and {{price_step}} = 0
  -- then false
  -- when {{price_step}} = 0--- and {{price_step}} = 0
  -- then false
  -- -- when sample_size_flag > 0 then false
  -- else true end
  and
  -- (case when {{Descr}} is null then false else true end)
  --   and
  (
    case
      when length (array_join (array {{Quarter_pb}}, ',')) = 0 then true
      else quarter in {{Quarter_pb}}
    end
  )

-- COMMAND ----------

with
  filters as (
    select
      *
    from
      ydx_internal_analysts_gold.thd_price_bands_charts_vf
    WHERE
      -- (case when length(array_join(array{{pricing__page__brand-display}},',') ) = 0 then true else brand in {{pricing__page__brand-display}} end)
      -- and
      (
        case
          when length (array_join (array {{Department_pb}}, ',')) = 0 then true
          else department in {{Department_pb}}
        end
      )
      and (
        case
          when length (array_join (array {{Class_pb}}, ',')) = 0 then true
          else class in {{Class_pb}}
        end
      )
      and (
        case
          when length (array_join (array {{Sub-Class_pb}}, ',')) = 0 then true
          else sub_class in {{Sub-Class_pb}}
        end
      )
      and (
        case
          when length (array_join (array {{Merchant_pb}}, ',')) = 0 then true
          else merchant in {{Merchant_pb}}
        end
      )
      and (
        case
          when length (array_join (array {{Channel_pb}}, ',')) = 0 then true
          else channel in {{Channel_pb}}
        end
      )
  ),
  sample_guardail_calc as (
    SELECT
      *,
      sum(count) OVER (
        partition by
          department
      ) as major_sample,
      sum(count) OVER (
        partition by
          class
      ) as sub_sample,
      sum(count) OVER (
        partition by
          sub_class
      ) as minor_sample,
      sum(count) OVER (
        partition by
          department,
          merchant
      ) as major_merchant_sample
    from
      filters
  ),
  clean_base as (
    select
      *
    from
      sample_guardail_calc
    where
      major_sample > 1500
  ),
  base_plus_price_band as (
    select
      quarter,
      department,
      class,
      sub_class,
      merchant,
      channel,
      case
        -- when {{Descr}} is null then "all_market"
        when item_price < {{price_step_starting_with}} + {{price_step}} then concat (
          "A. (",
          cast({{price_step_starting_with}} as string),
          " - ",
          cast(
            (
              {{price_step_starting_with}} + {{price_step}}
            ) as string
          ),
          ")"
        )
        when item_price < {{price_step_starting_with}} + 2 * {{price_step}} then concat (
          "B. (",
          cast(
            {{price_step_starting_with}} + 1 * {{price_step}} as string
          ),
          " - ",
          cast(
            (
              {{price_step_starting_with}} + 2 * {{price_step}}
            ) as string
          ),
          ")"
        )
        when item_price < {{price_step_starting_with}} + 3 * {{price_step}} then concat (
          "C. (",
          cast(
            {{price_step_starting_with}} + 2 * {{price_step}} as string
          ),
          " - ",
          cast(
            (
              {{price_step_starting_with}} + 3 * {{price_step}}
            ) as string
          ),
          ")"
        )
        when item_price < {{price_step_starting_with}} + 4 * {{price_step}} then concat (
          "D. (",
          cast(
            {{price_step_starting_with}} + 3 * {{price_step}} as string
          ),
          " - ",
          cast(
            (
              {{price_step_starting_with}} + 4 * {{price_step}}
            ) as string
          ),
          ")"
        )
        else concat (
          "E. (",
          cast(
            {{price_step_starting_with}} + 4 * {{price_step}} as string
          ),
          " +",
          ")"
        )
      end as price_band,
      sum(gmv) as gmv,
      count(*) as sample_size
    from
      clean_base
    group by
      1,
      2,
      3,
      4,
      5,
      6,
      7
  ),
  quarters AS (
    SELECT
      concat (
        substr (fiscal_year, 1, 4),
        " ",
        substr (fiscal_qtr, 1, 2)
      ) as quarter
    FROM
      ydx_thd_analysts_gold.quarterly_reports
    WHERE
      source IN ('cat_geo', 'flooring')
    GROUP BY
      1
  ),
  sub_classes AS (
    SELECT
      department,
      class,
      sub_class,
      price_band,
      merchant,
      channel
    FROM
      base_plus_price_band
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6
  ),
  full_list AS (
    SELECT
      a.*,
      b.quarter
    FROM
      sub_classes a
      LEFT JOIN quarters b
  ),
  joined as (
    SELECT
      a.quarter,
      a.department,
      a.class,
      a.sub_class,
      a.merchant,
      a.channel,
      a.price_band,
      b.gmv,
      b.sample_size
    FROM
      full_list a
      LEFT JOIN base_plus_price_band b ON a.quarter = b.quarter
      AND a.department = b.department
      AND COALESCE(a.class, '') = COALESCE(b.class, '')
      AND COALESCE(a.sub_class, '') = COALESCE(b.sub_class, '')
      AND a.merchant = b.merchant
      AND a.channel = b.channel
      and a.price_band = b.price_band
  ),
  agg_two AS (
    select
      quarter,
      department,
      class,
      sub_class,
      channel,
      merchant as final_name,
      price_band,
      sum(gmv) over (
        partition by
          a.department,
          a.class,
          a.sub_class,
          a.merchant,
          a.channel,
          a.price_band
        order by
          quarter rows between 3 preceding
          and current row
      ) as rolling_merchant_gmv,
      sum(sample_size) over (
        partition by
          a.department,
          a.class,
          a.merchant,
          a.channel,
          a.price_band
        order by
          quarter rows between 3 preceding
          and current row
      ) as rolling_sample_size
    from
      joined a
      -- left join ydx_internal_analysts_gold.retailer_segment_mapping_20250412 b  ----- UPDATE ONCE TABLE COMPLETE
      --   on a.merchant = b.merchant
  ),
  final_agg as (
    select
      *
    from
      agg_two
    WHERE
      rolling_merchant_gmv IS NOT NULL
      -- and
      -- (
      --     case
      --     when {{Descr}} is null then price_band = "all_market"
      --     when left({{Descr}}, 1) = "A" then price_band =
      --     concat("A. (",  cast({{price_step_starting_with}} as string)," - ", cast( ({{price_step_starting_with}} + {{price_step}}) as string), ")")
      --     when left({{Descr}}, 1) = "B" then price_band = concat("B. (", cast({{price_step_starting_with}} + 1*{{price_step}} as string), " - ", cast( ({{price_step_starting_with}} + 2*{{price_step}}) as string), ")")
      --     when left({{Descr}}, 1) = "C"  then price_band = concat("C. (", cast({{price_step_starting_with}} + 2*{{price_step}} as string), " - ", cast( ({{price_step_starting_with}} + 3*{{price_step}}) as string), ")")
      --     when left({{Descr}}, 1) = "D" then price_band = concat("D. (", cast({{price_step_starting_with}} + 3*{{price_step}} as string), " - ", cast( ({{price_step_starting_with}} + 4*{{price_step}}) as string), ")")
      --     when left({{Descr}}, 1) = "E" then price_band = concat("E. (", cast({{price_step_starting_with}} + 4*{{price_step}} as string), " +", ")")
      --     else price_band = concat("E. (", cast({{price_step_starting_with}} + 4*{{price_step}} as string), " +", ")")
      --     end)
  ),
  final_agg_two as (
    select
      *
    from
      final_agg
    where
      quarter >= '2022 1Q'
      and quarter <= '2025 2Q'
  ),
  -- total as(
  -- SELECT
  --   quarter,
  --   sum(rolling_merchant_gmv) as total_gmv
  -- FROM final_agg
  -- group by 1
  -- ),
  -- segment as(
  -- SELECT
  --   quarter,
  --   final_name,
  --   sum(rolling_merchant_gmv) as segment_gmv
  -- FROM final_agg
  -- group by 1,2
  -- ),
  -- top_brands as (
  -- SELECT 
  --   final_name, 
  --   sum(segment_gmv) as total_segment_gmv
  -- FROM segment
  -- GROUP BY 1
  -- ORDER BY 2 DESC
  -- LIMIT 10
  -- ),
  -- final as(
  -- select
  -- a.quarter,
  -- a.final_name,
  -- a.segment_gmv/b.total_gmv as market_share
  -- from segment a
  -- cross join total b
  -- on a.quarter = b.quarter
  -- where a.quarter >= '2022 1Q'
  -- and a.quarter <= '2024 4Q'
  -- and
  --   -- (case when {{Descr}} is null then false else true end)
  --     -- and
  --   (case when length(array_join(array{{Quarter_pb}},',') ) = 0 then true else a.quarter in {{Quarter_pb}} end)
  -- ),
  ------- ADDING PB LEVEL sample size constraints
  price_band_sample_guardail_calc as (
    select distinct
      price_band,
      avg(rolling_sample_size) as avg_rolling_sample_size,
      median (rolling_sample_size) as median_rolling_sample_size
    from
      agg_two
    group by
      1
  ),
  create_pb_sample_flag as (
    select
      *,
      case
        when avg_rolling_sample_size < 20 then 1
        else 0
      end as sample_size_flag_using_avg,
      case
        when median_rolling_sample_size < 8 then 1
        else 0
      end as sample_size_flag_using_median
    from
      price_band_sample_guardail_calc
  ),
  pb_sample_flag as (
    select
      sum(int (sample_size_flag_using_avg)) as sample_size_flag_avg,
      sum(int (sample_size_flag_using_median)) as sample_size_flag_median
    from
      create_pb_sample_flag
      -- group by all
  ),
  -- prep_last_cte as (
  -- select
  -- a.quarter,
  -- case when b.final_name is null then 'Other Retailers' else a.final_name end as final_name,
  -- sum(a.market_share) as market_share
  -- from final a
  -- left join top_brands b
  -- on a.final_name = b.final_name
  -- group by 1,2
  -- order by 2,3
  -- ),
  last_cte as (
    select
      a.*,
      b.sample_size_flag_avg,
      b.sample_size_flag_median
    from
      final_agg_two a
      left join pb_sample_flag b
  ),
  vf as (
    select
      *
    from
      last_cte
    where
      case
        when {{price_step_starting_with}} + {{price_step}} = 0 --- and {{price_step}} = 0
        then false
        when {{price_step}} = 0 then false
        when sample_size_flag_avg = 0
        OR sample_size_flag_median = 0 then true
        else false
      end
      -- when {{price_step_starting_with}} + {{price_step}} = 0 and {{Descr}} is null
      -- then true
      -- when {{price_step_starting_with}} + {{price_step}} = 0 --- and {{price_step}} = 0
      -- then false
      -- when {{price_step}} = 0--- and {{price_step}} = 0
      -- then false
      -- -- when sample_size_flag > 0 then false
      -- else true end
  )
select
  quarter,
  channel,
  department,
  class sub_class,
  final_name as Retailer,
  price_band,
  rolling_merchant_gmv as Rolling_4Q_Merchant_GMV
from
  vf
order by
  1,
  2,
  3,
  4,
  5,
  6
