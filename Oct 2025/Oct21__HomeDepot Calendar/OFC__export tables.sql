-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC sigma_data_export_test_portal
-- MAGIC
-- MAGIC [sigma_data_export_test_portal](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/1214896439294806?o=3092962415911490#command/1214896439294841)

-- COMMAND ----------

ydx_thd_analysts_silver.retailer_segment_mapping
ydx_thd_analysts_gold.quarterly_reports

-- COMMAND ----------

WITH quarters AS (
SELECT concat(substr(fiscal_year,1,4)," ", substr(fiscal_qtr,1,2)) as quarter
FROM ydx_thd_analysts_gold.quarterly_reports
GROUP BY 1
),

sub_classes AS (
SELECT department, class, case when sub_class='N/A' then "OTHER PRODUCTS" else sub_class end as sub_class
FROM ydx_thd_analysts_gold.quarterly_reports
GROUP BY 1, 2, 3
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

prep AS (
  select concat(substr(fiscal_year,1,4)," ", substr(fiscal_qtr,1,2)) as quarter, merchant, channel, department, class, case when sub_class='N/A' then "OTHER PRODUCTS" else sub_class end as sub_class, sum(gmv) as gmv, sum(cast(units as double)) as units, sum(cast(sample_size as double)) as sample_size
from ydx_thd_analysts_gold.quarterly_reports
--where class <>'N/A'
where source IN ('cat_geo', 'flooring')
group by 1,2,3,4,5,6
),

joined aS (
SELECT a.quarter, a.department, a.class, a.sub_class, a.merchant, a.channel, b.gmv, b.units, b.sample_size
FROM full_list a
LEFT JOIN prep b ON a.quarter = b.quarter AND a.department = b.department AND COALESCE(a.class, '') = COALESCE(b.class, '') AND COALESCE(a.sub_class, '') = COALESCE(b.sub_class, '') AND a.merchant = b.merchant AND a.channel = b.channel
),

final AS (
select quarter, b.final_name as merchant, channel, department, class, sub_class, sum(gmv) over (partition by a.merchant, channel, department, class, sub_class order by quarter rows between 3 preceding and current row) as gmv, sum(units) over (partition by a.merchant, channel, department, class, sub_class order by quarter rows between 3 preceding and current row) as units ,sum(sample_size) over (partition by a.merchant, channel, department, class, sub_class order by quarter rows between 3 preceding and current row) as sample_size
from joined a
left join ydx_thd_analysts_silver.retailer_segment_mapping b
on a.merchant=b.merchant
order by 2,3,4,5,1 desc)

select quarter, merchant, case when channel='B&M' then "In-Store" when channel='ONLINE' then "Online" end as channel , department, class, sub_class, gmv, units, sample_size
from final
WHERE gmv IS NOT NULL

