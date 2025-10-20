# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC | Total Fragrance, Ulta & Sephora | $ GMV (000's) |       |       |
# MAGIC |---------------------------------|---------------|-------|-------|
# MAGIC |                                 | **MTD**       | **YTD** | **TTM** |
# MAGIC | Portal                          | 214,611       | 1,665,510 | 3,461,288 |
# MAGIC | by SKU file                     | 212,558       | 1,661,150 | 3,455,458 |
# MAGIC | Delta                           | 2,053         | 4,360     | 5,830     |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # MTD  
# MAGIC portal 214666454.08536196
# MAGIC sku 214610606.76683578
# MAGIC diff 55847.3185261786
# MAGIC
# MAGIC | Total Fragrance, Ulta & Sephora | $ GMV (000's) | . |
# MAGIC |---------------------------------|---------------|---------------|
# MAGIC |                                 | **MTD**       | **my results**       |
# MAGIC | Portal                          | 214,611       | 214,666       |
# MAGIC | by SKU file                     | 212,558       | 214,610       |
# MAGIC | Delta                           | 2,053         | 55,847       |
# MAGIC
# MAGIC [sigma with the uncompiled](https://app.sigmacomputing.com/yipit/workbook/Brands-Master-V3-8-greater-than-Sept-15-beauty-issue-3hFk5JmhOzHNC7VEFrwPLC)

# COMMAND ----------

214666454.08536196 - 214610606.76683578

# COMMAND ----------

# DBTITLE 1,My portal calc
# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC   gmv Gmv
# MAGIC from
# MAGIC   (
# MAGIC     /*
# MAGIC     cat p - sub category market share
# MAGIC      */
# MAGIC     with
# MAGIC       max_month_before as (
# MAGIC         --- calculate the most recent month
# MAGIC         SELECT
# MAGIC           month(max(trunc_day)) as max_month,
# MAGIC           MAX(trunc_day) as max_date
# MAGIC         FROM
# MAGIC           yd_sensitive_corporate.ydx_prospect_analysts_gold.beauty_product_v38_market_share_for_column_null
# MAGIC       ),
# MAGIC       add_ytd_control as (
# MAGIC         --- calculate the date for in case ytd is used
# MAGIC         select
# MAGIC           *,
# MAGIC           '3' as ytd_max_month,
# MAGIC           case
# MAGIC             when '3' > max_month then (
# MAGIC               date_trunc("year", max_date - interval 1 year) + interval '3' month
# MAGIC             ) - interval 1 month
# MAGIC             else date_trunc("year", max_date) + interval '3' month - interval 1 month
# MAGIC           end as ytd_max_date
# MAGIC         from
# MAGIC           max_month_before
# MAGIC       ),
# MAGIC       max_month as (
# MAGIC         --- create a max_month variable, based on most recent ou ytd control
# MAGIC         select
# MAGIC           case
# MAGIC             when '3' is null then max_date
# MAGIC             else ytd_max_date
# MAGIC           end as max_date,
# MAGIC           case
# MAGIC             when '3' is null then max_month
# MAGIC             else ytd_max_month
# MAGIC           end as max_month
# MAGIC         from
# MAGIC           add_ytd_control
# MAGIC       ),
# MAGIC       source as (
# MAGIC         SELECT
# MAGIC           *
# MAGIC         FROM
# MAGIC           yd_sensitive_corporate.ydx_prospect_analysts_gold.beauty_product_v38_market_share_for_column_null
# MAGIC       ),
# MAGIC       add_page_meta_parameters as (
# MAGIC         select
# MAGIC           *,
# MAGIC           case
# MAGIC             when 'major' = "major" then major_cat
# MAGIC             when 'major' = "sub" then sub_cat
# MAGIC             when 'major' = "minor" then minor_cat
# MAGIC             when 'major' = "special attribute" then special_attribute
# MAGIC             else sub_cat
# MAGIC           end as display_cat,
# MAGIC           brand as display_brand
# MAGIC         from
# MAGIC           source
# MAGIC       ),
# MAGIC       filter_by_page_parameters as (
# MAGIC         select
# MAGIC           *
# MAGIC         from
# MAGIC           add_page_meta_parameters
# MAGIC         where
# MAGIC           (
# MAGIC             case
# MAGIC               when length(array_join(array(null), ',')) = 0 then true
# MAGIC               else display_brand in (null)
# MAGIC             end
# MAGIC           )
# MAGIC           and (
# MAGIC             case
# MAGIC               when length(array_join(array('fragrance'), ',')) = 0 then true
# MAGIC               else major_cat in ('fragrance')
# MAGIC             end
# MAGIC           )
# MAGIC           and (
# MAGIC             case
# MAGIC               when length(array_join(array(null), ',')) = 0 then true
# MAGIC               else sub_cat in (null)
# MAGIC             end
# MAGIC           )
# MAGIC           and (
# MAGIC             case
# MAGIC               when length(array_join(array(null), ',')) = 0 then true
# MAGIC               else minor_cat in (null)
# MAGIC             end
# MAGIC           )
# MAGIC           and (
# MAGIC             case
# MAGIC               when length(array_join(array('Ulta Beauty', 'Sephora'), ',')) = 0 then true
# MAGIC               else merchant in ('Ulta Beauty', 'Sephora')
# MAGIC             end
# MAGIC           )
# MAGIC           and (
# MAGIC             case
# MAGIC               when length(array_join(array(null), ',')) = 0 then true
# MAGIC               else special_attribute in (null)
# MAGIC             end
# MAGIC           )
# MAGIC       ),
# MAGIC       recreating_market_share_logic as (
# MAGIC         --- MARKET SHARE LOGIC
# MAGIC         --- https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/2361004218806681?o=3092962415911490#command/2361004218806684
# MAGIC         SELECT
# MAGIC           display_brand,
# MAGIC           display_cat,
# MAGIC           trunc_day as month,
# MAGIC           merchant,
# MAGIC           channel,
# MAGIC           gmv,
# MAGIC           sample_size,
# MAGIC           item_subtotal_observed,
# MAGIC           item_quantity_observed
# MAGIC         FROM
# MAGIC           filter_by_page_parameters
# MAGIC       ),
# MAGIC       flag_top_brands as (
# MAGIC         select
# MAGIC           coalesce(top_blank_table.display_cat, "Other Categories") as display_cat,
# MAGIC           recreating_market_share_logic.*
# MAGIC         except
# MAGIC         (display_cat)
# MAGIC         from
# MAGIC           recreating_market_share_logic
# MAGIC           left join (
# MAGIC             select
# MAGIC               display_cat,
# MAGIC               sum(gmv) as sum_gmv
# MAGIC             from
# MAGIC               recreating_market_share_logic
# MAGIC             group by
# MAGIC               display_cat
# MAGIC             order by
# MAGIC               2 desc
# MAGIC             limit
# MAGIC               coalesce(7, 7)
# MAGIC           ) as top_blank_table on recreating_market_share_logic.display_cat = top_blank_table.display_cat
# MAGIC       ),
# MAGIC       add_max_month as (
# MAGIC         select
# MAGIC           flag_top_brands.*,
# MAGIC           max_month.*
# MAGIC         from
# MAGIC           flag_top_brands
# MAGIC           CROSS JOIN max_month
# MAGIC       ),
# MAGIC       add_viz_meta_parameters_part_1 as (
# MAGIC         --- in case display_interval is YTD or Trailing 12 Months, add this column, filter this dates.
# MAGIC         select
# MAGIC           *
# MAGIC         from
# MAGIC           add_max_month
# MAGIC         where
# MAGIC           (
# MAGIC             case
# MAGIC               when 'Monthly' = 'Year-to-Date' then month(month) <= max_month
# MAGIC               else true
# MAGIC             end
# MAGIC           )
# MAGIC       ),
# MAGIC       add_viz_meta_parameters as (
# MAGIC         --- adapt column for display_time
# MAGIC         select
# MAGIC           *,
# MAGIC           case
# MAGIC             when 'Monthly' = "Annually" then date_trunc("year", month)
# MAGIC             when 'Monthly' = "Quarterly" then date_trunc("quarter", month)
# MAGIC             when 'Monthly' = "Monthly" then date_trunc("month", month)
# MAGIC             when 'Monthly' = "Weekly" then date_trunc("week", month)
# MAGIC             when 'Monthly' = 'Year-to-Date' then date_trunc("year", month)
# MAGIC             when 'Monthly' in (
# MAGIC               "Trailing 12 Months",
# MAGIC               "Trailing 3 Months",
# MAGIC               "Trailing 6 Months"
# MAGIC             ) then date_trunc("month", month)
# MAGIC             else date_trunc("month", month)
# MAGIC           end as display_time
# MAGIC         from
# MAGIC           add_viz_meta_parameters_part_1
# MAGIC       ),
# MAGIC       safe_grouping_1 as (
# MAGIC         select
# MAGIC           display_time,
# MAGIC           display_cat,
# MAGIC           sum(sample_size) as sample_size,
# MAGIC           month(display_time) as partition_factor,
# MAGIC           month(display_time) - month(date_trunc("quarter", display_time)) + 1 as p3_partition_factor,
# MAGIC           case
# MAGIC             when month(display_time) <= 6 then month(display_time)
# MAGIC             else month(display_time) - 6
# MAGIC           end as p6_partition_factor
# MAGIC         from
# MAGIC           add_viz_meta_parameters
# MAGIC         group by
# MAGIC           display_time,
# MAGIC           display_cat
# MAGIC       ),
# MAGIC       safe_grouping_1b as (
# MAGIC         --- over partition by
# MAGIC         select
# MAGIC           *,
# MAGIC           sum(sample_size) OVER (
# MAGIC             partition by display_cat
# MAGIC             order by
# MAGIC               display_time ROWS BETWEEN 11 PRECEDING
# MAGIC               and current row
# MAGIC           ) as p12_sample_size,
# MAGIC           sum(partition_factor) OVER (
# MAGIC             partition by display_cat
# MAGIC             order by
# MAGIC               display_time ROWS BETWEEN 11 PRECEDING
# MAGIC               and current row
# MAGIC           ) as p12_factor_check,
# MAGIC           --- Trailing 3 Months
# MAGIC           sum(sample_size) OVER (
# MAGIC             partition by display_cat
# MAGIC             order by
# MAGIC               display_time ROWS BETWEEN 2 PRECEDING
# MAGIC               and current row
# MAGIC           ) as p3_sample_size,
# MAGIC           sum(p3_partition_factor) OVER (
# MAGIC             partition by display_cat
# MAGIC             order by
# MAGIC               display_time ROWS BETWEEN 2 PRECEDING
# MAGIC               and current row
# MAGIC           ) as p3_factor_check,
# MAGIC           --- Trailing 6 Months
# MAGIC           sum(sample_size) OVER (
# MAGIC             partition by display_cat
# MAGIC             order by
# MAGIC               display_time ROWS BETWEEN 5 PRECEDING
# MAGIC               and current row
# MAGIC           ) as p6_sample_size,
# MAGIC           sum(p6_partition_factor) OVER (
# MAGIC             partition by display_cat
# MAGIC             order by
# MAGIC               display_time ROWS BETWEEN 5 PRECEDING
# MAGIC               and current row
# MAGIC           ) as p6_factor_check
# MAGIC         from
# MAGIC           safe_grouping_1
# MAGIC       ),
# MAGIC       safe_grouping_1c as (
# MAGIC         --- filter for Trailing 12 Months
# MAGIC         select
# MAGIC           case
# MAGIC             when 'Monthly' = "Trailing 12 Months" then p12_sample_size
# MAGIC             when 'Monthly' = "Trailing 3 Months" then p3_sample_size
# MAGIC             when 'Monthly' = "Trailing 6 Months" then p6_sample_size
# MAGIC             else sample_size
# MAGIC           end as sample_size,
# MAGIC           *
# MAGIC         except
# MAGIC         (sample_size, p12_factor_check, p12_sample_size)
# MAGIC         from
# MAGIC           safe_grouping_1b
# MAGIC         where
# MAGIC           case
# MAGIC             when 'Monthly' = "Trailing 12 Months" then p12_factor_check = 78
# MAGIC             when 'Monthly' = "Trailing 3 Months" then p3_factor_check = 6
# MAGIC             when 'Monthly' = "Trailing 6 Months" then p6_factor_check = 21
# MAGIC             else true
# MAGIC           end
# MAGIC       ),
# MAGIC       safe_grouping_2 as (
# MAGIC         select
# MAGIC           display_cat,
# MAGIC           avg(sample_size) as breakdown_ssg
# MAGIC         from
# MAGIC           safe_grouping_1c
# MAGIC         group by
# MAGIC           1
# MAGIC       ),
# MAGIC       safe_grouping_2b as (
# MAGIC         select
# MAGIC           display_cat,
# MAGIC           breakdown_ssg >= ssg.ssg as is_over_threshold
# MAGIC         from
# MAGIC           safe_grouping_2
# MAGIC           cross join (
# MAGIC             SELECT
# MAGIC               ssg
# MAGIC             FROM
# MAGIC               yd_sensitive_corporate.ydx_prospect_analysts_gold.beauty_product_v38_sample_size_guardrail
# MAGIC           ) as ssg
# MAGIC       ),
# MAGIC       safe_grouping_2c as (
# MAGIC         SELECT
# MAGIC           display_cat
# MAGIC         FROM
# MAGIC           safe_grouping_2b
# MAGIC         where
# MAGIC           is_over_threshold is true
# MAGIC       ),
# MAGIC       safe_grouping_3 as (
# MAGIC         select
# MAGIC           coalesce(safe_grouping_2c.display_cat, "Other Categories") as display_cat,
# MAGIC           add_viz_meta_parameters.*
# MAGIC         except
# MAGIC         (display_cat)
# MAGIC         from
# MAGIC           add_viz_meta_parameters
# MAGIC           left join safe_grouping_2c using (display_cat)
# MAGIC       ),
# MAGIC       grouping as (
# MAGIC         select
# MAGIC           display_time,
# MAGIC           display_cat,
# MAGIC           month(display_time) as partition_factor,
# MAGIC           month(display_time) - month(date_trunc("quarter", display_time)) + 1 as p3_partition_factor,
# MAGIC           case
# MAGIC             when month(display_time) <= 6 then month(display_time)
# MAGIC             else month(display_time) - 6
# MAGIC           end as p6_partition_factor,
# MAGIC           sum(gmv) as gmv,
# MAGIC           sum(sample_size) as sample_size,
# MAGIC           sum(item_subtotal_observed) as iso,
# MAGIC           sum(item_quantity_observed) as iqo,
# MAGIC           try_divide(
# MAGIC             sum(gmv),
# MAGIC             try_divide(
# MAGIC               sum(item_subtotal_observed),
# MAGIC               sum(item_quantity_observed)
# MAGIC             )
# MAGIC           ) as units
# MAGIC         from
# MAGIC           safe_grouping_3
# MAGIC         group by
# MAGIC           display_time,
# MAGIC           display_cat
# MAGIC       ),
# MAGIC       added_p12_columns as (
# MAGIC         --- do Trailing 12 Months partition by columns
# MAGIC         select
# MAGIC           *,
# MAGIC           sum(gmv) OVER (
# MAGIC             partition by display_cat
# MAGIC             order by
# MAGIC               display_time ROWS BETWEEN 11 PRECEDING
# MAGIC               and current row
# MAGIC           ) as p12_gmv,
# MAGIC           sum(units) OVER (
# MAGIC             partition by display_cat
# MAGIC             order by
# MAGIC               display_time ROWS BETWEEN 11 PRECEDING
# MAGIC               and current row
# MAGIC           ) as p12_units,
# MAGIC           sum(sample_size) OVER (
# MAGIC             partition by display_cat
# MAGIC             order by
# MAGIC               display_time ROWS BETWEEN 11 PRECEDING
# MAGIC               and current row
# MAGIC           ) as p12_sample_size,
# MAGIC           sum(partition_factor) OVER (
# MAGIC             partition by display_cat
# MAGIC             order by
# MAGIC               display_time ROWS BETWEEN 11 PRECEDING
# MAGIC               and current row
# MAGIC           ) as p12_factor_check,
# MAGIC           --- Trailing 3 Months
# MAGIC           sum(gmv) OVER (
# MAGIC             partition by display_cat
# MAGIC             order by
# MAGIC               display_time ROWS BETWEEN 2 PRECEDING
# MAGIC               and current row
# MAGIC           ) as p3_gmv,
# MAGIC           sum(units) OVER (
# MAGIC             partition by display_cat
# MAGIC             order by
# MAGIC               display_time ROWS BETWEEN 2 PRECEDING
# MAGIC               and current row
# MAGIC           ) as p3_units,
# MAGIC           sum(sample_size) OVER (
# MAGIC             partition by display_cat
# MAGIC             order by
# MAGIC               display_time ROWS BETWEEN 2 PRECEDING
# MAGIC               and current row
# MAGIC           ) as p3_sample_size,
# MAGIC           sum(p3_partition_factor) OVER (
# MAGIC             partition by display_cat
# MAGIC             order by
# MAGIC               display_time ROWS BETWEEN 2 PRECEDING
# MAGIC               and current row
# MAGIC           ) as p3_factor_check,
# MAGIC           --- Trailing 6 Months
# MAGIC           sum(gmv) OVER (
# MAGIC             partition by display_cat
# MAGIC             order by
# MAGIC               display_time ROWS BETWEEN 5 PRECEDING
# MAGIC               and current row
# MAGIC           ) as p6_gmv,
# MAGIC           sum(units) OVER (
# MAGIC             partition by display_cat
# MAGIC             order by
# MAGIC               display_time ROWS BETWEEN 5 PRECEDING
# MAGIC               and current row
# MAGIC           ) as p6_units,
# MAGIC           sum(sample_size) OVER (
# MAGIC             partition by display_cat
# MAGIC             order by
# MAGIC               display_time ROWS BETWEEN 5 PRECEDING
# MAGIC               and current row
# MAGIC           ) as p6_sample_size,
# MAGIC           sum(p6_partition_factor) OVER (
# MAGIC             partition by display_cat
# MAGIC             order by
# MAGIC               display_time ROWS BETWEEN 5 PRECEDING
# MAGIC               and current row
# MAGIC           ) as p6_factor_check
# MAGIC         from
# MAGIC           grouping
# MAGIC       ),
# MAGIC       divide_columns as (
# MAGIC         --- combined with filter_by_78_factor
# MAGIC         --- add new columns, filter if display interval = Trailing 12 Months
# MAGIC         select
# MAGIC           *
# MAGIC         except
# MAGIC         (p12_factor_check, partition_factor),
# MAGIC         try_divide(iso, iqo) as asp
# MAGIC         from
# MAGIC           added_p12_columns
# MAGIC         where
# MAGIC           case
# MAGIC             when 'Monthly' = "Trailing 12 Months" then p12_factor_check = 78
# MAGIC             when 'Monthly' = "Trailing 3 Months" then p3_factor_check = 6
# MAGIC             when 'Monthly' = "Trailing 6 Months" then p6_factor_check = 21
# MAGIC             else true
# MAGIC           end
# MAGIC       ),
# MAGIC       normalize_columns as (
# MAGIC         --- renaming columsn and excepting
# MAGIC         select
# MAGIC           case
# MAGIC             when 'Monthly' = "Trailing 12 Months" then p12_gmv
# MAGIC             when 'Monthly' = "Trailing 3 Months" then p3_gmv
# MAGIC             when 'Monthly' = "Trailing 6 Months" then p6_gmv
# MAGIC             else gmv
# MAGIC           end as gmv,
# MAGIC           case
# MAGIC             when 'Monthly' = "Trailing 12 Months" then p12_units
# MAGIC             when 'Monthly' = "Trailing 3 Months" then p3_units
# MAGIC             when 'Monthly' = "Trailing 6 Months" then p6_units
# MAGIC             else units
# MAGIC           end as units,
# MAGIC           case
# MAGIC             when 'Monthly' = "Trailing 12 Months" then p12_sample_size
# MAGIC             when 'Monthly' = "Trailing 3 Months" then p3_sample_size
# MAGIC             when 'Monthly' = "Trailing 6 Months" then p6_sample_size
# MAGIC             else sample_size
# MAGIC           end as sample_size,
# MAGIC           *
# MAGIC         except
# MAGIC         (gmv, sample_size, units)
# MAGIC         from
# MAGIC           divide_columns
# MAGIC       ),
# MAGIC       filter_month as (
# MAGIC         select
# MAGIC           *
# MAGIC         from
# MAGIC           normalize_columns
# MAGIC         where
# MAGIC           (
# MAGIC             case
# MAGIC               when struct (null as
# MAGIC             end, null as start).start is null then true
# MAGIC             else display_time >= struct (null as
# MAGIC           end, null as start).start
# MAGIC       end
# MAGIC   )
# MAGIC   and (
# MAGIC     case
# MAGIC       when struct (null as
# MAGIC     end, null as start).end is null then true
# MAGIC     else display_time < struct (null as
# MAGIC   end, null as start).end
# MAGIC end
# MAGIC )
# MAGIC ),
# MAGIC last_table as (
# MAGIC   select
# MAGIC     filter_month.*,
# MAGIC     /* for tooltip */
# MAGIC     case
# MAGIC       when not (array_join(array(null), ', ') = "")
# MAGIC       and not (array_size (array(null)) > 2) then array_join(array(null), ', ')
# MAGIC       when not (array_join(array(null), ', ') = "")
# MAGIC       and not (array_size (array(null)) > 2) then array_join(array(null), ', ')
# MAGIC       when not (array_join(array('fragrance'), ', ') = "")
# MAGIC       and not (array_size (array('fragrance')) > 2) then array_join(array('fragrance'), ', ')
# MAGIC       else "Multiple Categories"
# MAGIC     end as Category_texts
# MAGIC   from
# MAGIC     filter_month
# MAGIC     inner join (
# MAGIC       select
# MAGIC         display_cat
# MAGIC       from
# MAGIC         filter_month
# MAGIC       group by
# MAGIC         1
# MAGIC         --- having min(sample_size) > 200
# MAGIC     ) using (display_cat)
# MAGIC ),
# MAGIC add_perc_metrics as (
# MAGIC   --- another partition by
# MAGIC   select
# MAGIC     *,
# MAGIC     try_divide(gmv, sum(gmv) over (partition by display_time)) as gmv_perc,
# MAGIC     try_divide(units, sum(units) over (partition by display_time)) as units_perc
# MAGIC   from
# MAGIC     last_table
# MAGIC ),
# MAGIC display_metric as (
# MAGIC   ---- add display metric based on before tables
# MAGIC   select
# MAGIC     *,
# MAGIC     case
# MAGIC       when false
# MAGIC       and 'GMV' = "GMV" then gmv_perc
# MAGIC       when 'GMV' = "GMV" then gmv
# MAGIC       when false
# MAGIC       and 'GMV' = "Units" then units_perc
# MAGIC       when 'GMV' = "Units" then units
# MAGIC       else gmv
# MAGIC     end as display_metric
# MAGIC   from
# MAGIC     add_perc_metrics
# MAGIC ),
# MAGIC final_table as (
# MAGIC   select
# MAGIC     *,
# MAGIC     case
# MAGIC       when 'Monthly' = 'Year-to-Date' then display_time + interval '3' month - interval 1 day
# MAGIC       when 'Monthly' = "Annually" then display_time + interval 12 month - interval 1 day
# MAGIC       when 'Monthly' = "Quarterly" then display_time + interval 3 month - interval 1 day
# MAGIC       when 'Monthly' = "Monthly" then display_time + interval 1 month - interval 1 day
# MAGIC       when 'Monthly' = "Weekly" then display_time + interval 7 day - interval 1 day
# MAGIC       when 'Monthly' = "Trailing 12 Months" then display_time + interval 1 month - interval 1 day
# MAGIC       when 'Monthly' = "Trailing 3 Months" then display_time + interval 1 month - interval 1 day
# MAGIC       when 'Monthly' = "Trailing 6 Months" then display_time + interval 1 month - interval 1 day
# MAGIC       else display_time
# MAGIC     end as period_end,
# MAGIC     case
# MAGIC       when 'Monthly' = "Trailing 12 Months" then display_time - interval 11 month
# MAGIC       when 'Monthly' = "Trailing 3 Months" then display_time - interval 2 month
# MAGIC       when 'Monthly' = "Trailing 6 Months" then display_time - interval 5 month
# MAGIC       else display_time
# MAGIC     end as period_starting
# MAGIC   from
# MAGIC     display_metric
# MAGIC ),
# MAGIC _assess_risk AS (
# MAGIC   select
# MAGIC     display_cat,
# MAGIC     min(sample_size) as ss,
# MAGIC     min(
# MAGIC       case
# MAGIC         when display_cat = "Other Categories" then 1000
# MAGIC         else 0
# MAGIC       end
# MAGIC     ) as checks,
# MAGIC     min("Other Categories") as other_text
# MAGIC   from
# MAGIC     final_table
# MAGIC   group by
# MAGIC     1
# MAGIC ),
# MAGIC assess_risk AS (
# MAGIC   select
# MAGIC     case
# MAGIC       when min(ss) < 200
# MAGIC       or sum(checks) >= 1000 then concat(
# MAGIC         "Some ",
# MAGIC         lower(substring(min("Other Categories"), len ("Other "))),
# MAGIC         " are not visible due to a sample size smaller than threshold for the period.
# MAGIC     Please try a different time period or adjust the filters accordingly. Check methodology for more details."
# MAGIC       )
# MAGIC       else " "
# MAGIC     end as risk_message
# MAGIC   from
# MAGIC     _assess_risk
# MAGIC )
# MAGIC SELECT
# MAGIC   gmv
# MAGIC FROM
# MAGIC   final_table
# MAGIC   LEFT JOIN assess_risk
# MAGIC order by
# MAGIC   display_time DESC
# MAGIC limit
# MAGIC   1
# MAGIC ) Q1
# MAGIC limit
# MAGIC   10001
# MAGIC   -- Sigma Σ {"sourceUrl":"https://app.sigmacomputing.com/yipit/workbook/Brands-Master-V3-8-greater-than-Sept-15-beauty-issue-3hFk5JmhOzHNC7VEFrwPLC?:displayNodeId=13X_BQGoIY","kind":"adhoc","request-id":"g01994e0ec71077358b8ee8322425d95f","email":"pfisch@yipitdata.com"}

# COMMAND ----------

# DBTITLE 1,My by SKU file
# MAGIC %sql
# MAGIC select
# MAGIC   gmv Gmv
# MAGIC from
# MAGIC   (
# MAGIC     /*
# MAGIC     Top SKUs -pf
# MAGIC      */
# MAGIC     with
# MAGIC       source as (
# MAGIC         --- basic table with filters
# MAGIC         SELECT
# MAGIC           *
# MAGIC         FROM
# MAGIC           yd_sensitive_corporate.ydx_prospect_analysts_gold.beauty_product_v38_sku_analysis
# MAGIC         where
# MAGIC           (
# MAGIC             case
# MAGIC               when length(array_join(array(null), ',')) = 0 then true
# MAGIC               else brand in (null)
# MAGIC             end
# MAGIC           )
# MAGIC           and (
# MAGIC             case
# MAGIC               when length(array_join(array('fragrance'), ',')) = 0 then true
# MAGIC               else major_cat in ('fragrance')
# MAGIC             end
# MAGIC           )
# MAGIC           and (
# MAGIC             case
# MAGIC               when length(array_join(array(null), ',')) = 0 then true
# MAGIC               else sub_cat in (null)
# MAGIC             end
# MAGIC           )
# MAGIC           and (
# MAGIC             case
# MAGIC               when length(array_join(array(null), ',')) = 0 then true
# MAGIC               else minor_cat in (null)
# MAGIC             end
# MAGIC           )
# MAGIC           and (
# MAGIC             case
# MAGIC               when length(array_join(array('Ulta Beauty', 'Sephora'), ',')) = 0 then true
# MAGIC               else merchant in ('Ulta Beauty', 'Sephora')
# MAGIC             end
# MAGIC           )
# MAGIC           and (
# MAGIC             case
# MAGIC               when length(array_join(array(null), ',')) = 0 then true
# MAGIC               else channel in (null)
# MAGIC             end
# MAGIC           )
# MAGIC           and display_interval = 'Last Month'
# MAGIC       ),
# MAGIC       grouped as (
# MAGIC         select
# MAGIC           merchant,
# MAGIC           brand,
# MAGIC           web_description,
# MAGIC           product_id,
# MAGIC           min(major_cat) as major_category,
# MAGIC           min(sub_cat) as sub_category,
# MAGIC           min(minor_cat) as minor_category,
# MAGIC           sum(gmv) as total_gmv,
# MAGIC           sum(last_obs) as sum_last_obs,
# MAGIC           sum(last_gmv) as last_year_gmv,
# MAGIC           sum(total_units) as units,
# MAGIC           try_divide(sum(total_spend), sum(total_units)) as asp,
# MAGIC           Sum(Observations) as sample_size,
# MAGIC           min(product_hash) as product_hash
# MAGIC         from
# MAGIC           source
# MAGIC         group by
# MAGIC           merchant,
# MAGIC           brand,
# MAGIC           web_description,
# MAGIC           product_id
# MAGIC       )
# MAGIC     select
# MAGIC       sum(total_gmv) as gmv
# MAGIC     from
# MAGIC       grouped as add_columns
# MAGIC   ) Q1
# MAGIC limit
# MAGIC   10001
# MAGIC   -- Sigma Σ {"sourceUrl":"https://app.sigmacomputing.com/yipit/workbook/Brands-Master-V3-8-greater-than-Sept-15-beauty-issue-3hFk5JmhOzHNC7VEFrwPLC?:displayNodeId=2txEKwE98W","kind":"adhoc","request-id":"g01994e1ec47477dfb66cfda38fefb121","email":"pfisch@yipitdata.com"}

# COMMAND ----------

# DBTITLE 1,MORE COMPLETE SKU FILE
# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC     Recreate SKU Analysis from _filter_items without web_description quality filter
# MAGIC     Based on product_analysis.py logic but sourcing from _filter_items directly
# MAGIC      */
# MAGIC     with
# MAGIC       max_month as (
# MAGIC         SELECT month(max(month)) as max_month, MAX(month) as max_date
# MAGIC         FROM yd_sensitive_corporate.ydx_prospect_analysts_gold.beauty_product_v38_filter_items
# MAGIC       ),
# MAGIC
# MAGIC       source as (
# MAGIC         --- basic table with filters, handling null web_description
# MAGIC         SELECT
# MAGIC           *,
# MAGIC           coalesce(web_description, 'no_web_description') as web_description_clean
# MAGIC         FROM
# MAGIC           yd_sensitive_corporate.ydx_prospect_analysts_gold.beauty_product_v38_filter_items
# MAGIC         CROSS JOIN max_month
# MAGIC         where
# MAGIC           (
# MAGIC             case
# MAGIC               when length(array_join(array(null), ',')) = 0 then true
# MAGIC               else brand in (null)
# MAGIC             end
# MAGIC           )
# MAGIC           and (
# MAGIC             case
# MAGIC               when length(array_join(array('fragrance'), ',')) = 0 then true
# MAGIC               else major_cat in ('fragrance')
# MAGIC             end
# MAGIC           )
# MAGIC           and (
# MAGIC             case
# MAGIC               when length(array_join(array(null), ',')) = 0 then true
# MAGIC               else sub_cat in (null)
# MAGIC             end
# MAGIC           )
# MAGIC           and (
# MAGIC             case
# MAGIC               when length(array_join(array(null), ',')) = 0 then true
# MAGIC               else minor_cat in (null)
# MAGIC             end
# MAGIC           )
# MAGIC           and (
# MAGIC             case
# MAGIC               when length(array_join(array('Ulta Beauty', 'Sephora'), ',')) = 0 then true
# MAGIC               else merchant_clean in ('Ulta Beauty', 'Sephora')
# MAGIC             end
# MAGIC           )
# MAGIC           and (
# MAGIC             case
# MAGIC               when length(array_join(array(null), ',')) = 0 then true
# MAGIC               else channel in (null)
# MAGIC             end
# MAGIC           )
# MAGIC           -- Filter for Last Month equivalent
# MAGIC           and date_trunc('month', month) = date_trunc('month', max_date)
# MAGIC       ),
# MAGIC
# MAGIC       grouped as (
# MAGIC         select
# MAGIC           merchant_clean as merchant,
# MAGIC           brand,
# MAGIC           web_description_clean as web_description,
# MAGIC           coalesce(upc, 'no_product_id') as product_id,
# MAGIC           min(major_cat) as major_category,
# MAGIC           min(sub_cat) as sub_category,
# MAGIC           min(minor_cat) as minor_category,
# MAGIC           sum(gmv) as total_gmv,
# MAGIC           -- Simulate last_obs and last_gmv (would need historical data for proper calculation)
# MAGIC           0 as sum_last_obs,
# MAGIC           0 as last_year_gmv,
# MAGIC           sum(item_quantity) as units,
# MAGIC           try_divide(sum(item_price * item_quantity), sum(item_quantity)) as asp,
# MAGIC           count(*) as sample_size,
# MAGIC           min(coalesce(product_hash, 'no_product_hash')) as product_hash
# MAGIC         from
# MAGIC           source
# MAGIC         group by
# MAGIC           merchant_clean,
# MAGIC           brand,
# MAGIC           web_description_clean,
# MAGIC           coalesce(upc, 'no_product_id')
# MAGIC       )
# MAGIC    
# MAGIC   select
# MAGIC       merchant,
# MAGIC       brand,
# MAGIC       major_category, sub_category, minor_category,
# MAGIC       web_description,
# MAGIC       product_id,
# MAGIC       product_hash,  
# MAGIC       total_gmv / asp as units,
# MAGIC       total_gmv,
# MAGIC       asp,
# MAGIC       sample_size,
# MAGIC       case
# MAGIC       when sample_size > 1000 then "Low Risk"
# MAGIC       when sample_size > 250  then "Some Risk"
# MAGIC     else "High Risk" end as ssr,
# MAGIC     case
# MAGIC       when sample_size > 1000 and sum_last_obs > 500 then try_divide(total_gmv, last_year_gmv) - 1
# MAGIC       else ""
# MAGIC     end as yoy_growth
# MAGIC   from
# MAGIC   grouped as add_columns
