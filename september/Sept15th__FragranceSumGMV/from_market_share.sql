select
  gmv Gmv
from
  (
    /*
    cat p - sub category market share
     */
    with
      max_month_before as (
        --- calculate the most recent month
        SELECT
          month(max(trunc_day)) as max_month,
          MAX(trunc_day) as max_date
        FROM
          yd_sensitive_corporate.ydx_prospect_analysts_gold.beauty_product_v38_market_share_for_column_null
      ),
      add_ytd_control as (
        --- calculate the date for in case ytd is used
        select
          *,
          '3' as ytd_max_month,
          case
            when '3' > max_month then (
              date_trunc("year", max_date - interval 1 year) + interval '3' month
            ) - interval 1 month
            else date_trunc("year", max_date) + interval '3' month - interval 1 month
          end as ytd_max_date
        from
          max_month_before
      ),
      max_month as (
        --- create a max_month variable, based on most recent ou ytd control
        select
          case
            when '3' is null then max_date
            else ytd_max_date
          end as max_date,
          case
            when '3' is null then max_month
            else ytd_max_month
          end as max_month
        from
          add_ytd_control
      ),
      source as (
        SELECT
          *
        FROM
          yd_sensitive_corporate.ydx_prospect_analysts_gold.beauty_product_v38_market_share_for_column_null
      ),
      add_page_meta_parameters as (
        select
          *,
          case
            when 'major' = "major" then major_cat
            when 'major' = "sub" then sub_cat
            when 'major' = "minor" then minor_cat
            when 'major' = "special attribute" then special_attribute
            else sub_cat
          end as display_cat,
          brand as display_brand
        from
          source
      ),
      filter_by_page_parameters as (
        select
          *
        from
          add_page_meta_parameters
        where
          (
            case
              when length(array_join(array(null), ',')) = 0 then true
              else display_brand in (null)
            end
          )
          and (
            case
              when length(array_join(array('fragrance'), ',')) = 0 then true
              else major_cat in ('fragrance')
            end
          )
          and (
            case
              when length(array_join(array(null), ',')) = 0 then true
              else sub_cat in (null)
            end
          )
          and (
            case
              when length(array_join(array(null), ',')) = 0 then true
              else minor_cat in (null)
            end
          )
          and (
            case
              when length(array_join(array('Ulta Beauty', 'Sephora'), ',')) = 0 then true
              else merchant in ('Ulta Beauty', 'Sephora')
            end
          )
          and (
            case
              when length(array_join(array(null), ',')) = 0 then true
              else special_attribute in (null)
            end
          )
      ),
      recreating_market_share_logic as (
        --- MARKET SHARE LOGIC
        --- https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/2361004218806681?o=3092962415911490#command/2361004218806684
        SELECT
          display_brand,
          display_cat,
          trunc_day as month,
          merchant,
          channel,
          gmv,
          sample_size,
          item_subtotal_observed,
          item_quantity_observed
        FROM
          filter_by_page_parameters
      ),
      flag_top_brands as (
        select
          coalesce(top_blank_table.display_cat, "Other Categories") as display_cat,
          recreating_market_share_logic.*
        except
        (display_cat)
        from
          recreating_market_share_logic
          left join (
            select
              display_cat,
              sum(gmv) as sum_gmv
            from
              recreating_market_share_logic
            group by
              display_cat
            order by
              2 desc
            limit
              coalesce(7, 7)
          ) as top_blank_table on recreating_market_share_logic.display_cat = top_blank_table.display_cat
      ),
      add_max_month as (
        select
          flag_top_brands.*,
          max_month.*
        from
          flag_top_brands
          CROSS JOIN max_month
      ),
      add_viz_meta_parameters_part_1 as (
        --- in case display_interval is YTD or Trailing 12 Months, add this column, filter this dates.
        select
          *
        from
          add_max_month
        where
          (
            case
              when 'Monthly' = 'Year-to-Date' then month(month) <= max_month
              else true
            end
          )
      ),
      add_viz_meta_parameters as (
        --- adapt column for display_time
        select
          *,
          case
            when 'Monthly' = "Annually" then date_trunc("year", month)
            when 'Monthly' = "Quarterly" then date_trunc("quarter", month)
            when 'Monthly' = "Monthly" then date_trunc("month", month)
            when 'Monthly' = "Weekly" then date_trunc("week", month)
            when 'Monthly' = 'Year-to-Date' then date_trunc("year", month)
            when 'Monthly' in (
              "Trailing 12 Months",
              "Trailing 3 Months",
              "Trailing 6 Months"
            ) then date_trunc("month", month)
            else date_trunc("month", month)
          end as display_time
        from
          add_viz_meta_parameters_part_1
      ),
      safe_grouping_1 as (
        select
          display_time,
          display_cat,
          sum(sample_size) as sample_size,
          month(display_time) as partition_factor,
          month(display_time) - month(date_trunc("quarter", display_time)) + 1 as p3_partition_factor,
          case
            when month(display_time) <= 6 then month(display_time)
            else month(display_time) - 6
          end as p6_partition_factor
        from
          add_viz_meta_parameters
        group by
          display_time,
          display_cat
      ),
      safe_grouping_1b as (
        --- over partition by
        select
          *,
          sum(sample_size) OVER (
            partition by display_cat
            order by
              display_time ROWS BETWEEN 11 PRECEDING
              and current row
          ) as p12_sample_size,
          sum(partition_factor) OVER (
            partition by display_cat
            order by
              display_time ROWS BETWEEN 11 PRECEDING
              and current row
          ) as p12_factor_check,
          --- Trailing 3 Months
          sum(sample_size) OVER (
            partition by display_cat
            order by
              display_time ROWS BETWEEN 2 PRECEDING
              and current row
          ) as p3_sample_size,
          sum(p3_partition_factor) OVER (
            partition by display_cat
            order by
              display_time ROWS BETWEEN 2 PRECEDING
              and current row
          ) as p3_factor_check,
          --- Trailing 6 Months
          sum(sample_size) OVER (
            partition by display_cat
            order by
              display_time ROWS BETWEEN 5 PRECEDING
              and current row
          ) as p6_sample_size,
          sum(p6_partition_factor) OVER (
            partition by display_cat
            order by
              display_time ROWS BETWEEN 5 PRECEDING
              and current row
          ) as p6_factor_check
        from
          safe_grouping_1
      ),
      safe_grouping_1c as (
        --- filter for Trailing 12 Months
        select
          case
            when 'Monthly' = "Trailing 12 Months" then p12_sample_size
            when 'Monthly' = "Trailing 3 Months" then p3_sample_size
            when 'Monthly' = "Trailing 6 Months" then p6_sample_size
            else sample_size
          end as sample_size,
          *
        except
        (sample_size, p12_factor_check, p12_sample_size)
        from
          safe_grouping_1b
        where
          case
            when 'Monthly' = "Trailing 12 Months" then p12_factor_check = 78
            when 'Monthly' = "Trailing 3 Months" then p3_factor_check = 6
            when 'Monthly' = "Trailing 6 Months" then p6_factor_check = 21
            else true
          end
      ),
      safe_grouping_2 as (
        select
          display_cat,
          avg(sample_size) as breakdown_ssg
        from
          safe_grouping_1c
        group by
          1
      ),
      safe_grouping_2b as (
        select
          display_cat,
          breakdown_ssg >= ssg.ssg as is_over_threshold
        from
          safe_grouping_2
          cross join (
            SELECT
              ssg
            FROM
              yd_sensitive_corporate.ydx_prospect_analysts_gold.beauty_product_v38_sample_size_guardrail
          ) as ssg
      ),
      safe_grouping_2c as (
        SELECT
          display_cat
        FROM
          safe_grouping_2b
        where
          is_over_threshold is true
      ),
      safe_grouping_3 as (
        select
          coalesce(safe_grouping_2c.display_cat, "Other Categories") as display_cat,
          add_viz_meta_parameters.*
        except
        (display_cat)
        from
          add_viz_meta_parameters
          left join safe_grouping_2c using (display_cat)
      ),
      grouping as (
        select
          display_time,
          display_cat,
          month(display_time) as partition_factor,
          month(display_time) - month(date_trunc("quarter", display_time)) + 1 as p3_partition_factor,
          case
            when month(display_time) <= 6 then month(display_time)
            else month(display_time) - 6
          end as p6_partition_factor,
          sum(gmv) as gmv,
          sum(sample_size) as sample_size,
          sum(item_subtotal_observed) as iso,
          sum(item_quantity_observed) as iqo,
          try_divide(
            sum(gmv),
            try_divide(
              sum(item_subtotal_observed),
              sum(item_quantity_observed)
            )
          ) as units
        from
          safe_grouping_3
        group by
          display_time,
          display_cat
      ),
      added_p12_columns as (
        --- do Trailing 12 Months partition by columns
        select
          *,
          sum(gmv) OVER (
            partition by display_cat
            order by
              display_time ROWS BETWEEN 11 PRECEDING
              and current row
          ) as p12_gmv,
          sum(units) OVER (
            partition by display_cat
            order by
              display_time ROWS BETWEEN 11 PRECEDING
              and current row
          ) as p12_units,
          sum(sample_size) OVER (
            partition by display_cat
            order by
              display_time ROWS BETWEEN 11 PRECEDING
              and current row
          ) as p12_sample_size,
          sum(partition_factor) OVER (
            partition by display_cat
            order by
              display_time ROWS BETWEEN 11 PRECEDING
              and current row
          ) as p12_factor_check,
          --- Trailing 3 Months
          sum(gmv) OVER (
            partition by display_cat
            order by
              display_time ROWS BETWEEN 2 PRECEDING
              and current row
          ) as p3_gmv,
          sum(units) OVER (
            partition by display_cat
            order by
              display_time ROWS BETWEEN 2 PRECEDING
              and current row
          ) as p3_units,
          sum(sample_size) OVER (
            partition by display_cat
            order by
              display_time ROWS BETWEEN 2 PRECEDING
              and current row
          ) as p3_sample_size,
          sum(p3_partition_factor) OVER (
            partition by display_cat
            order by
              display_time ROWS BETWEEN 2 PRECEDING
              and current row
          ) as p3_factor_check,
          --- Trailing 6 Months
          sum(gmv) OVER (
            partition by display_cat
            order by
              display_time ROWS BETWEEN 5 PRECEDING
              and current row
          ) as p6_gmv,
          sum(units) OVER (
            partition by display_cat
            order by
              display_time ROWS BETWEEN 5 PRECEDING
              and current row
          ) as p6_units,
          sum(sample_size) OVER (
            partition by display_cat
            order by
              display_time ROWS BETWEEN 5 PRECEDING
              and current row
          ) as p6_sample_size,
          sum(p6_partition_factor) OVER (
            partition by display_cat
            order by
              display_time ROWS BETWEEN 5 PRECEDING
              and current row
          ) as p6_factor_check
        from
          grouping
      ),
      divide_columns as (
        --- combined with filter_by_78_factor
        --- add new columns, filter if display interval = Trailing 12 Months
        select
          *
        except
        (p12_factor_check, partition_factor),
        try_divide(iso, iqo) as asp
        from
          added_p12_columns
        where
          case
            when 'Monthly' = "Trailing 12 Months" then p12_factor_check = 78
            when 'Monthly' = "Trailing 3 Months" then p3_factor_check = 6
            when 'Monthly' = "Trailing 6 Months" then p6_factor_check = 21
            else true
          end
      ),
      normalize_columns as (
        --- renaming columsn and excepting
        select
          case
            when 'Monthly' = "Trailing 12 Months" then p12_gmv
            when 'Monthly' = "Trailing 3 Months" then p3_gmv
            when 'Monthly' = "Trailing 6 Months" then p6_gmv
            else gmv
          end as gmv,
          case
            when 'Monthly' = "Trailing 12 Months" then p12_units
            when 'Monthly' = "Trailing 3 Months" then p3_units
            when 'Monthly' = "Trailing 6 Months" then p6_units
            else units
          end as units,
          case
            when 'Monthly' = "Trailing 12 Months" then p12_sample_size
            when 'Monthly' = "Trailing 3 Months" then p3_sample_size
            when 'Monthly' = "Trailing 6 Months" then p6_sample_size
            else sample_size
          end as sample_size,
          *
        except
        (gmv, sample_size, units)
        from
          divide_columns
      ),
      filter_month as (
        select
          *
        from
          normalize_columns
        where
          (
            case
              when struct (null as
            end, null as start).start is null then true
            else display_time >= struct (null as
          end, null as start).start
      end
  )
  and (
    case
      when struct (null as
    end, null as start).end is null then true
    else display_time < struct (null as
  end, null as start).end
end
)
),
last_table as (
  select
    filter_month.*,
    /* for tooltip */
    case
      when not (array_join(array(null), ', ') = "")
      and not (array_size (array(null)) > 2) then array_join(array(null), ', ')
      when not (array_join(array(null), ', ') = "")
      and not (array_size (array(null)) > 2) then array_join(array(null), ', ')
      when not (array_join(array('fragrance'), ', ') = "")
      and not (array_size (array('fragrance')) > 2) then array_join(array('fragrance'), ', ')
      else "Multiple Categories"
    end as Category_texts
  from
    filter_month
    inner join (
      select
        display_cat
      from
        filter_month
      group by
        1
        --- having min(sample_size) > 200
    ) using (display_cat)
),
add_perc_metrics as (
  --- another partition by
  select
    *,
    try_divide(gmv, sum(gmv) over (partition by display_time)) as gmv_perc,
    try_divide(units, sum(units) over (partition by display_time)) as units_perc
  from
    last_table
),
display_metric as (
  ---- add display metric based on before tables
  select
    *,
    case
      when false
      and 'GMV' = "GMV" then gmv_perc
      when 'GMV' = "GMV" then gmv
      when false
      and 'GMV' = "Units" then units_perc
      when 'GMV' = "Units" then units
      else gmv
    end as display_metric
  from
    add_perc_metrics
),
final_table as (
  select
    *,
    case
      when 'Monthly' = 'Year-to-Date' then display_time + interval '3' month - interval 1 day
      when 'Monthly' = "Annually" then display_time + interval 12 month - interval 1 day
      when 'Monthly' = "Quarterly" then display_time + interval 3 month - interval 1 day
      when 'Monthly' = "Monthly" then display_time + interval 1 month - interval 1 day
      when 'Monthly' = "Weekly" then display_time + interval 7 day - interval 1 day
      when 'Monthly' = "Trailing 12 Months" then display_time + interval 1 month - interval 1 day
      when 'Monthly' = "Trailing 3 Months" then display_time + interval 1 month - interval 1 day
      when 'Monthly' = "Trailing 6 Months" then display_time + interval 1 month - interval 1 day
      else display_time
    end as period_end,
    case
      when 'Monthly' = "Trailing 12 Months" then display_time - interval 11 month
      when 'Monthly' = "Trailing 3 Months" then display_time - interval 2 month
      when 'Monthly' = "Trailing 6 Months" then display_time - interval 5 month
      else display_time
    end as period_starting
  from
    display_metric
),
_assess_risk AS (
  select
    display_cat,
    min(sample_size) as ss,
    min(
      case
        when display_cat = "Other Categories" then 1000
        else 0
      end
    ) as checks,
    min("Other Categories") as other_text
  from
    final_table
  group by
    1
),
assess_risk AS (
  select
    case
      when min(ss) < 200
      or sum(checks) >= 1000 then concat(
        "Some ",
        lower(substring(min("Other Categories"), len ("Other "))),
        " are not visible due to a sample size smaller than threshold for the period.
    Please try a different time period or adjust the filters accordingly. Check methodology for more details."
      )
      else " "
    end as risk_message
  from
    _assess_risk
)
SELECT
  gmv
FROM
  final_table
  LEFT JOIN assess_risk
order by
  display_time DESC
limit
  1
) Q1
limit
  10001
  -- Sigma Σ {"sourceUrl":"https://app.sigmacomputing.com/yipit/workbook/Brands-Master-V3-8-greater-than-Sept-15-beauty-issue-3hFk5JmhOzHNC7VEFrwPLC?:displayNodeId=13X_BQGoIY","kind":"adhoc","request-id":"g01994e0ec71077358b8ee8322425d95f","email":"pfisch@yipitdata.com"}