# Databricks notebook source
# MAGIC %sql
# MAGIC select * from ydx_scotts_analysts_gold.scotts_v38_filter_items

# COMMAND ----------

# MAGIC %sql
# MAGIC with max_month as (
# MAGIC   SELECT
# MAGIC   max(month) as max_month
# MAGIC   FROM ydx_scotts_analysts_gold.scotts_v38_filter_items 
# MAGIC   GROUP BY all
# MAGIC ),
# MAGIC
# MAGIC source as (
# MAGIC     select
# MAGIC     s.*,
# MAGIC     max_month.max_month
# MAGIC     fROM ydx_scotts_analysts_gold.scotts_v38_filter_items  as s
# MAGIC     left JOIN
# MAGIC     max_month
# MAGIC ),
# MAGIC
# MAGIC apply_page_filters as (
# MAGIC     select * from source
# MAGIC     WHERE
# MAGIC     (case when length(array_join(array{{shopper-metrics__page__merchant}},',') ) = 0 then true else merchant_clean in {{shopper-metrics__page__merchant}} end)
# MAGIC     and
# MAGIC     CASE WHEN length(array_join(array{{parameter-sm-l1}},',') ) = 0 THEN True  ELSE major_cat IN {{parameter-sm-l1}} END
# MAGIC     AND        
# MAGIC     CASE WHEN length(array_join(array{{parameter-sm-l2}},',') ) = 0 THEN True  ELSE sub_cat IN {{parameter-sm-l2}} END
# MAGIC     AND
# MAGIC     CASE WHEN length(array_join(array{{parameter-sm-l3}},',') ) = 0 THEN True ELSE minor_cat IN {{parameter-sm-l3}} END
# MAGIC
# MAGIC     -- filter YTD
# MAGIC
# MAGIC     and
# MAGIC     case
# MAGIC     when {{parameter-sm-interval}} = 'Year-To-Date'
# MAGIC     then month(month) <= month(max_month)
# MAGIC     else true
# MAGIC     end 
# MAGIC
# MAGIC ),
# MAGIC
# MAGIC prep_columns as (
# MAGIC select
# MAGIC * except (month, age_cohort),
# MAGIC case
# MAGIC   when {{parameter-sm-interval}} = 'Monthly' then month
# MAGIC   when {{parameter-sm-interval}} = 'Quarterly' then date_trunc('quarter', month)
# MAGIC   when {{parameter-sm-interval}} = 'Annual' then date_trunc('year', month)
# MAGIC   when {{parameter-sm-interval}} in ('Year-To-Date','Year-To-Date (Current Year)') then date_trunc('year', month)
# MAGIC end as month, 
# MAGIC case 
# MAGIC   when age in ("55-64", "65+") then 'Boomer+'
# MAGIC   when age in ("45-54") then 'Gen X'
# MAGIC   when age in ("35-44", "25-34") then 'Millenial'
# MAGIC   when age in ("21-24", "18-20", "18-24") then 'Gen Z'
# MAGIC   else null
# MAGIC end as age_cohort,
# MAGIC (item_price * item_quantity) as pre_total_spend,
# MAGIC (user_id) as pre_buyers,
# MAGIC (order_id) as pre_total_trips,
# MAGIC (item_quantity) as pre_total_items,
# MAGIC
# MAGIC case
# MAGIC     when {{parameter-sm-interval}} in ('Year-To-Date (Current Year)')
# MAGIC     then (
# MAGIC         leia_panel_flag_test = 1 or --user active for the rolling period and the year
# MAGIC         leia_panel_flag_test = 2 --user active for the most rolling period but not the year
# MAGIC         )
# MAGIC     when {{parameter-sm-interval}} in ('Annual', 'Year-To-Date')
# MAGIC     then (
# MAGIC         leia_panel_flag_test = 1 or --user active for the rolling period and the year
# MAGIC         leia_panel_flag_test = 3 --user active for the annual year but not the rolling period
# MAGIC         ) 
# MAGIC     else (
# MAGIC         leia_panel_flag_test = 1 or --user active for the rolling period and the year
# MAGIC         leia_panel_flag_test = 2 --user active for the most rolling period but not the year
# MAGIC         ) 
# MAGIC end as should_we_use_this_user
# MAGIC
# MAGIC fROM apply_page_filters
# MAGIC ),
# MAGIC
# MAGIC table_1 as (
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC month, 
# MAGIC age_cohort,       
# MAGIC     SUM(case when should_we_use_this_user then pre_total_spend end) as total_spend, 
# MAGIC     Count(distinct(case when should_we_use_this_user then pre_buyers end)) as buyers, 
# MAGIC     count(distinct(case when should_we_use_this_user then pre_total_trips end)) as total_trips,
# MAGIC     SUM(case when should_we_use_this_user then pre_total_items end) as total_items,
# MAGIC     sum(gmv) as gmv,
# MAGIC     
# MAGIC     FIRST(annual_active_users) as annual_active_users,
# MAGIC     FIRST(rolling_active_users) as rolling_active_users,
# MAGIC     FIRST(age_adj_2yr) as age_adj_2yr,
# MAGIC     FIRST(age_adj_annual) as age_adj_annual,
# MAGIC     
# MAGIC     max(max_month) as max_month
# MAGIC
# MAGIC from prep_columns
# MAGIC
# MAGIC GROUP BY 1,2
# MAGIC ),
# MAGIC
# MAGIC add_panel_metrics as (
# MAGIC select
# MAGIC   *,
# MAGIC   coalesce(
# MAGIC     (CASE
# MAGIC     WHEN {{parameter-sm-interval}} in ('Year-To-Date (Current Year)') then age_adj_2yr
# MAGIC     WHEN {{parameter-sm-interval}} in ('Annual', 'Year-To-Date') then age_adj_annual
# MAGIC     else age_adj_2yr
# MAGIC     end),
# MAGIC     1) as age_adj,
# MAGIC   (
# MAGIC     CASE
# MAGIC     WHEN {{parameter-sm-interval}} in ('Year-To-Date (Current Year)') then rolling_active_users
# MAGIC     WHEN {{parameter-sm-interval}} in ('Annual', 'Year-To-Date')
# MAGIC     then annual_active_users
# MAGIC     else rolling_active_users end
# MAGIC   ) as active_users
# MAGIC from table_1
# MAGIC ),
# MAGIC
# MAGIC filter_pre_grouping as (
# MAGIC select *
# MAGIC FROM add_panel_metrics
# MAGIC WHERe
# MAGIC   month >= (max_month - interval 24 month) OR
# MAGIC   {{parameter-sm-interval}} in ('Annual', 'Year-To-Date')
# MAGIC   
# MAGIC ),
# MAGIC
# MAGIC table_2 as (
# MAGIC
# MAGIC SELECT
# MAGIC month,
# MAGIC sum(total_spend) / sum(buyers) as buy_rate,
# MAGIC SUM(total_trips) / SUM(buyers) as purchase_frequency,
# MAGIC sum(total_spend) / sum(total_trips) as spend_per_trip,
# MAGIC sum(total_spend) / sum(total_items) as asp,
# MAGIC sum(total_items) / sum(total_trips) as units_per_trip, 
# MAGIC
# MAGIC SUM(buyers * age_adj) / FIRST(active_users) as panel_pen,
# MAGIC
# MAGIC SUM(buyers) as total_panelists,
# MAGIC SUM(gmv) as gmv
# MAGIC FROM filter_pre_grouping
# MAGIC GROUP BY 1
# MAGIC ) -- last
# MAGIC
# MAGIC SELECT
# MAGIC *,
# MAGIC (panel_pen / lag(panel_pen) OVER (partition by month(month) order by month)) -1 as pen_change,
# MAGIC gmv / buy_rate as total_shoppers
# MAGIC FROM table_2
# MAGIC WHERE total_panelists > 0
# MAGIC AND
# MAGIC case when {{parameter-sm-interval}} in ('Year-To-Date (Current Year)') then date_trunc("year", month) = date_trunc("year", current_date()) else true end

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC data_trunc = month
# MAGIC
# MAGIC group by mont
# MAGIC partition/ facto
# MAGIC     count(distint users) as t12_users
# MAGIC
# MAGIC sum over 
# MAGIC --- messiness
# MAGIC     sum(t12_user) as users
# MAGIC
# MAGIC renaming things
# MAGIC
# MAGIC addition calculations

# COMMAND ----------

# MAGIC %md
# MAGIC select
# MAGIC count(distinct users) users
# MAGIC from
# MAGIC table
# MAGIC
# MAGIC where t12

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --- t12
# MAGIC
# MAGIC with source as (
# MAGIC   select
# MAGIC   *
# MAGIC from
# MAGIC ydx_scotts_analysts_gold.scotts_v38_filter_items
# MAGIC where
# MAGIC   order_date >= "2024-09-01" -- 
# MAGIC )
# MAGIC
# MAGIC select
# MAGIC   Count(distinct(user_id)) as buyers
# MAGIC from
# MAGIC ydx_scotts_analysts_gold.scotts_v38_filter_items
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --- t12
# MAGIC
# MAGIC with source as (
# MAGIC select
# MAGIC   *,
# MAGIC   date_trunc('month', order_date) as month,
# MAGIC from
# MAGIC ydx_scotts_analysts_gold.scotts_v38_filter_items
# MAGIC where
# MAGIC   order_date >= "2024-09-01" -- 
# MAGIC ),
# MAGIC
# MAGIC grouping as (
# MAGIC   select
# MAGIC   month,
# MAGIC   Count(distinct(user_id)) as buyers
# MAGIC   from
# MAGIC   source
# MAGIC   group by all
# MAGIC ),
# MAGIC
# MAGIC sumover as (
# MAGIC   
# MAGIC )
# MAGIC
# MAGIC select
# MAGIC
# MAGIC   Count(distinct(user_id)) as buyers
# MAGIC from
# MAGIC ydx_scotts_analysts_gold.scotts_v38_filter_items
# MAGIC
# MAGIC /*
# MAGIC
# MAGIC
# MAGIC data_trunc = month
# MAGIC
# MAGIC group by mont
# MAGIC partition/ facto
# MAGIC     count(distint users) as t12_users
# MAGIC
# MAGIC sum over 
# MAGIC --- messiness
# MAGIC     sum(t12_user) as users
# MAGIC
# MAGIC renaming things
# MAGIC
# MAGIC addition calculations
# MAGIC
# MAGIC
# MAGIC */

# COMMAND ----------


