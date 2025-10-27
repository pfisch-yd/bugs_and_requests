-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### prep the base table

-- COMMAND ----------

select * from ydx_internal_analysts_gold.thd_price_bands_calc
limit 10

-- COMMAND ----------

select * 
from ydx_internal_analysts_gold.thd_price_bands_calc_20250412
limit 10

-- COMMAND ----------

create or replace temp view price_bands_base_af as

SELECT  
    concat(substr(fiscal_year, 1, 4), " ", substr(fiscal_qtr, 1, 2)) as quarter, 
    CONCAT(merch_dept_nbr, " ", merchandising_department) as department, 
    merchandising_department, 
    "D25P OPE" as class, 
    "D25P OPE" as old_class, 
    concat(sub_class_nbr, " ", sub_class) as sub_class, 
    merchant, 
    channel, 
    item_price, 
    gmv_final as gmv
FROM ydx_thd_analysts_silver.items_table
-- FROM ydx_thd_analysts_gold.items_table_export_2025_04_01
-- FROM ydx_thd_analysts_gold.items_table_export_2025_05_01
WHERE merchandising_department = "POWER" 
    AND class IN ("PRESSURE WASHER", "TRAILERS", "HANDHELD POWER", "CHORE", "RIDERS", "WALKS", "OUTDOOR POWER ACCS")

UNION ALL

SELECT 
    concat(substr(fiscal_year, 1, 4), " ", substr(fiscal_qtr, 1, 2)) as quarter, 
    CONCAT(merch_dept_nbr, " ", merchandising_department) as department, 
    merchandising_department, 
    "D25P TOOLS" as class, 
    "D25P TOOLS" as old_class, 
    concat(sub_class_nbr, " ", sub_class) as sub_class,  
    merchant, 
    channel, 
    item_price, 
    gmv_final as gmv
FROM ydx_thd_analysts_silver.items_table
-- FROM ydx_thd_analysts_gold.items_table_export_2025_04_01
-- FROM ydx_thd_analysts_gold.items_table_export_2025_05_01
WHERE merchandising_department = "POWER" 
    AND class IN ("PNEUMATIC FASTENERS", "COMPRESSORS", "POWER TOOL ACCESSORIES", "PORTABLE POWER")

UNION ALL

SELECT 
    concat(substr(fiscal_year, 1, 4), " ", substr(fiscal_qtr, 1, 2)) as quarter, 
    CONCAT(merch_dept_nbr, " ", merchandising_department) as department, 
    merchandising_department, 
    "D25P OPE LESS OPA" as class, 
    "D25P OPE LESS OPA" as old_class, 
    concat(sub_class_nbr, " ", sub_class) as sub_class,  
    merchant, 
    channel, 
    item_price, 
    gmv_final as gmv
FROM ydx_thd_analysts_silver.items_table
-- FROM ydx_thd_analysts_gold.items_table_export_2025_04_01
-- FROM ydx_thd_analysts_gold.items_table_export_2025_05_01
WHERE merchandising_department = "POWER" 
    AND class IN ("PRESSURE WASHER", "TRAILERS", "HANDHELD POWER", "CHORE", "RIDERS", "WALKS")

UNION ALL

SELECT 
    concat(substr(fiscal_year, 1, 4), " ", substr(fiscal_qtr, 1, 2)) as quarter, 
    CONCAT(merch_dept_nbr, " ", merchandising_department) as department, 
    merchandising_department, 
    "D25P TOOLS LESS PTA" as class, 
    "D25P TOOLS LESS PTA" as old_class, 
    concat(sub_class_nbr, " ", sub_class) as sub_class, 
    merchant, 
    channel, 
    item_price, 
    gmv_final as gmv
FROM ydx_thd_analysts_silver.items_table
-- FROM ydx_thd_analysts_gold.items_table_export_2025_04_01
-- FROM ydx_thd_analysts_gold.items_table_export_2025_05_01
WHERE merchandising_department = "POWER" 
    AND class IN ("PNEUMATIC FASTENERS", "COMPRESSORS", "PORTABLE POWER")

UNION ALL

SELECT 
    concat(substr(fiscal_year, 1, 4), " ", substr(fiscal_qtr, 1, 2)) as quarter, 
    CONCAT(merch_dept_nbr, " ", merchandising_department) as department, 
    merchandising_department, 
    concat(class_nbr, " ", class) as class, 
    class as old_class, 
    concat(sub_class_nbr, " ", sub_class) as sub_class, 
    merchant, 
    channel, 
    item_price, 
    gmv_final as gmv
FROM ydx_thd_analysts_silver.items_table
-- FROM ydx_thd_analysts_gold.items_table_export_2025_04_01
-- FROM ydx_thd_analysts_gold.items_table_export_2025_05_01
WHERE merchandising_department IS NOT NULL

-- COMMAND ----------

--- previous for 4/12 version

create or replace temp view charts_base_prep as

  select distinct
    quarter,
    department,
    class,
    sub_class,
    merchant,
    channel,
    item_price,
    sum(gmv) as gmv,
    count(item_price) as count
from price_bands_base_af
where department is not null
and quarter >= '2020 Q1'
group by 1,2,3,4,5,6,7

-- COMMAND ----------


create or replace temp view charts_base as

  select
  quarter,
    a.department,
    a.class,
    a.sub_class,
    b.final_name as merchant,
    a.channel,
    a.item_price,
    sum(a.gmv) as gmv,
    count(a.item_price) as count
from charts_base_prep a
left join ydx_internal_analysts_gold.retailer_segment_mapping b
on a.merchant = b.merchant
group by 1,2,3,4,5,6,7

-- COMMAND ----------

create or replace temporary view final_charts_base as

select * except (channel), 
    case when channel = 'B&M' THEN "In-Store"
    when channel = 'ONLINE' THEN 'Online'
    ELSE channel 
    end as channel
from charts_base

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from yipit_databricks_utils.future import create_table
-- MAGIC
-- MAGIC
-- MAGIC df = spark.table('final_charts_base')
-- MAGIC
-- MAGIC create_table('ydx_internal_analysts_gold', 'thd_price_bands_charts_vf', df, overwrite=True)
-- MAGIC
-- MAGIC # create_table('ydx_internal_analysts_gold', 'thd_price_bands_charts_vf', df, overwrite=True)
-- MAGIC
-- MAGIC
-- MAGIC # PREVIOUSLY
-- MAGIC # create_table('ydx_internal_analysts_gold', 'thd_price_bands_charts_20250413', df, overwrite=True)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # New price bands calc table

-- COMMAND ----------

create or replace temp view price_bands_base_calc_0 as

SELECT  
    concat(substr(fiscal_year, 1, 4), " ", substr(fiscal_qtr, 1, 2)) as quarter,
    month,
    CONCAT(merch_dept_nbr, " ", merchandising_department) as department, 
    merchandising_department, 
    "D25P OPE" as class, 
    "D25P OPE" as old_class, 
    concat(sub_class_nbr, " ", sub_class) as sub_class,
    brand,
    sku as upc,
    web_description,
    merchant, 
    channel,
    product_description,
    item_price,
    item_quantity,
    gmv_final as gmv
FROM ydx_thd_analysts_silver.items_table
WHERE merchandising_department = "POWER" 
    AND class IN ("PRESSURE WASHER", "TRAILERS", "HANDHELD POWER", "CHORE", "RIDERS", "WALKS", "OUTDOOR POWER ACCS")

UNION ALL

SELECT 
    concat(substr(fiscal_year, 1, 4), " ", substr(fiscal_qtr, 1, 2)) as quarter,
    month,
    CONCAT(merch_dept_nbr, " ", merchandising_department) as department, 
    merchandising_department, 
    "D25P TOOLS" as class, 
    "D25P TOOLS" as old_class, 
    concat(sub_class_nbr, " ", sub_class) as sub_class,
    brand,
    sku as upc,
    web_description,
    merchant, 
    channel,
    product_description,
    item_price,
    item_quantity,
    gmv_final as gmv
FROM ydx_thd_analysts_silver.items_table
WHERE merchandising_department = "POWER" 
    AND class IN ("PNEUMATIC FASTENERS", "COMPRESSORS", "POWER TOOL ACCESSORIES", "PORTABLE POWER")

UNION ALL

SELECT 
    concat(substr(fiscal_year, 1, 4), " ", substr(fiscal_qtr, 1, 2)) as quarter,
    month,
    CONCAT(merch_dept_nbr, " ", merchandising_department) as department, 
    merchandising_department, 
    "D25P OPE LESS OPA" as class, 
    "D25P OPE LESS OPA" as old_class, 
    concat(sub_class_nbr, " ", sub_class) as sub_class,
    brand,
    sku as upc,
    web_description,
    merchant, 
    channel,
    product_description,
    item_price,
    item_quantity,
    gmv_final as gmv
FROM ydx_thd_analysts_silver.items_table
WHERE merchandising_department = "POWER" 
    AND class IN ("PRESSURE WASHER", "TRAILERS", "HANDHELD POWER", "CHORE", "RIDERS", "WALKS")

UNION ALL

SELECT 
    concat(substr(fiscal_year, 1, 4), " ", substr(fiscal_qtr, 1, 2)) as quarter, 
    month,
    CONCAT(merch_dept_nbr, " ", merchandising_department) as department, 
    merchandising_department, 
    "D25P TOOLS LESS PTA" as class, 
    "D25P TOOLS LESS PTA" as old_class, 
    concat(sub_class_nbr, " ", sub_class) as sub_class,
    brand,
    sku as upc,
    web_description,
    merchant, 
    channel,
    product_description,
    item_price,
    item_quantity,
    gmv_final as gmv
FROM ydx_thd_analysts_silver.items_table
WHERE merchandising_department = "POWER" 
    AND class IN ("PNEUMATIC FASTENERS", "COMPRESSORS", "PORTABLE POWER")

UNION ALL

SELECT 
    concat(substr(fiscal_year, 1, 4), " ", substr(fiscal_qtr, 1, 2)) as quarter,
    month,
    CONCAT(merch_dept_nbr, " ", merchandising_department) as department, 
    merchandising_department, 
    concat(class_nbr, " ", class) as class, 
    class as old_class,
    concat(sub_class_nbr, " ", sub_class) as sub_class,
    brand,
    sku as upc,
    web_description,
    merchant, 
    channel,
    product_description,
    item_price,
    item_quantity,
    gmv_final as gmv
FROM ydx_thd_analysts_silver.items_table
WHERE merchandising_department IS NOT NULL

-- COMMAND ----------

create or replace temp view price_bands_item_base_af_agg as

SELECT distinct
        quarter,
        month,
        department,
        class,
        sub_class,
        brand,
        -- sub_brand,
        -- parent_brand,
        upc,
        web_description,
        merchant,
        channel,
        md5(concat(coalesce(web_description, ""), brand, merchant, coalesce(upc, ""))) as sku,
        --sku,
        max(product_description) as product_description,
        --avg(item_price) as ITEM_PRICE_AVG,
        max(item_price) as ITEM_PRICE_max,
        min(item_price) as ITEM_PRICE_min,
        sum(item_quantity) as sum_item_quantity,
        sum(item_price*item_quantity) as sum_dollars,
        sum(item_price*item_quantity) / sum(item_quantity) as ITEM_PRICE_AVG,
        sum(gmv) as gmv
    FROM price_bands_base_calc_0
    where department is not null
    group by 1,2,3,4,5,6,7,8,9,10,11

-- COMMAND ----------

-- with prep as(

--   select *
--   from price_bands_item_base_af_agg
--   where department = "D25P POWER"
--   and class = "C07 POWER GENERATION"
--   and sub_class = "SC03 PORTABLE GENERATORS"
-- )


-- select
--   "oie" as aa,
--   --- sum(sum_dollars) as market_dollars,
--   avg(ITEM_PRICE_AVG) as avg,
--   ((percentile_cont(0.80) WITHIN GROUP (order by ITEM_PRICE_AVG) ) - (percentile_cont(0.20) WITHIN GROUP (order by ITEM_PRICE_AVG) ))/4
--   as step,
  
--   (percentile_cont(0.50) WITHIN GROUP (order by ITEM_PRICE_AVG) ) as median,
--   (percentile_cont(0.80) WITHIN GROUP (order by ITEM_PRICE_AVG) )/4 as m80,
--   (percentile_cont(0.20) WITHIN GROUP (order by ITEM_PRICE_AVG) ) as starting,
--   (percentile_cont(0.80) WITHIN GROUP (order by ITEM_PRICE_AVG) ) as lastt
  

-- from prep
-- group by 1

-- COMMAND ----------

-- %python
-- from yipit_databricks_utils.future import create_table


-- df = spark.table('price_bands_item_base_af_agg')

-- create_table('ydx_internal_analysts_gold', 'thd_price_bands_calc_20250412', df, overwrite=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from yipit_databricks_utils.future import create_table
-- MAGIC
-- MAGIC
-- MAGIC df = spark.table('price_bands_item_base_af_agg')
-- MAGIC
-- MAGIC create_table('ydx_internal_analysts_gold', 'thd_price_bands_calc', df, overwrite=True)

-- COMMAND ----------

-- MAGIC %MD
-- MAGIC # Segment mapping table

-- COMMAND ----------

create or replace temporary view test as

select *
from ydx_thd_analysts_silver.retailer_segment_mapping

-- COMMAND ----------

select * from ydx_internal_analysts_gold.retailer_segment_mapping

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from yipit_databricks_utils.future import create_table
-- MAGIC
-- MAGIC
-- MAGIC df = spark.table('test')
-- MAGIC
-- MAGIC create_table('ydx_internal_analysts_gold', 'retailer_segment_mapping', df, overwrite=True)
