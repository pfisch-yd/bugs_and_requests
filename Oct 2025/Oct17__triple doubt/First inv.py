# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC email : Aly and Paula,
# MAGIC Hope you are both doing well. Question for you…we launched 4 new products with Tractor Supply in June of this year. I am not seeing any of them on this report. Are you not seeing anything on those yet or how long until we see that data?
# MAGIC They are the Triple Crown Diamond products: Senior, Perform, Balancer and Chopped Forage
# MAGIC
# MAGIC ---
# MAGIC Checkar na master items,
# MAGIC get brand,
# MAGIC check filters
# MAGIC

# COMMAND ----------

from yipit_databricks_utils.future import create_table

schema_name = "ydx_triplecrown_analysts_silver"
table_name = "items_table"

query = spark.sql(f"""

with filter_retailer as (
  select
    *
  from ydx_retail_silver.master_items_table
  where
    merchant in ('tractor_supply', 'rural_king', 'bomgaars')
),

filter_sub_cat3_and_exceptions as (
  select
    *
  from filter_retailer
  where
    sub_cat_3 in ('horse feed', 'forage', 'hay', 'horse treats & salt licks')
    or
    sub_cat_5 in ('horse treats', 'horse mineral licks', 'horse salt licks')
    or sku in ('234860599', '234860699','234860799')
),

renaming_categories as (
  select
    * except (parent_brand),
    "Horse" as major_cat_prep,
    "Horse Feed & Treats" as sub_cat_prep,
    case
      when sub_cat_3 in ('horse feed') then 'Horse Feed'
      when sub_cat_3 in ('forage', 'hay') then 'Forage'
      when sub_cat_3 in ('horse treats & salt licks') then 'Horse Treats & Salt Licks'
      when sub_cat_5 in ('horse treats', 'horse mineral licks', 'horse salt licks') then 'Horse Treats & Salt Licks'
      when sku in ('234860599', '234860699','234860799') then 'Horse Treats & Salt Licks'
    else 'Other'
    end as minor_cat_prep,
    brand as parent_brand
  from filter_sub_cat3_and_exceptions
)

select
  *
from renaming_categories


""")

#create_table(schema_name, table_name, query, overwrite=True)

# COMMAND ----------

inv = spark.sql(f"""

with cut_filter_items as (
  select
    brand,
    merchant,
    sub_cat_3,
    sku,
    sub_cat_5,
    gmv
  from ydx_retail_silver.master_items_table
  where
    order_date >= "2025-05-01"
),

filter_retailer as (
    select * from cut_filter_items
    where
        merchant in ('tractor_supply', 'rural_king', 'bomgaars')
        and brand = "Triple Crown"
),

all_prods as (

    select
        brand,
        merchant,
        sub_cat_3,
        sku,
        sub_cat_5,
        sum(gmv) as gmv
    from filter_retailer
    group by all
)

select
    *,
    sub_cat_3 in ('horse feed', 'forage', 'hay', 'horse treats & salt licks')
    or
    sub_cat_5 in ('horse treats', 'horse mineral licks', 'horse salt licks')
    or sku in ('234860599', '234860699','234860799') as out_of_cat
from
all_prods
""")

# COMMAND ----------

inv.display()

# COMMAND ----------

inv = spark.sql(f"""

with cut_filter_items as (
  select
    brand,
    merchant,
    sub_cat_3,
    sku,
    sub_cat_5,
    gmv,
    web_description
  from ydx_retail_silver.master_items_table
  where
    order_date >= "2025-05-01"
),

filter_retailer as (
    select * from cut_filter_items
    where
        brand = "Triple Crown"
),

all_prods as (

    select
        brand,
        merchant,
        sub_cat_3,
        sku,
        sub_cat_5,
        sum(gmv) as gmv,
        min(web_description) as web_description
    from filter_retailer
    group by all
),

add_out_of_cat as (

select
    *,
    sub_cat_3 in ('horse feed', 'forage', 'hay', 'horse treats & salt licks')
    or
    sub_cat_5 in ('horse treats', 'horse mineral licks', 'horse salt licks')
    or sku in ('234860599', '234860699','234860799') as out_of_cat
from
all_prods
)

select * from add_out_of_cat
where out_of_cat is false
-- and merchant = 'tractor_supply'
""")

inv.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC I found 2 Triple Crown products that are not appearing in out data ("Triple Crown Complete High Fat, High Fiber Formula, G4100, 50 LB Bag", Triple Crown Lite Formula, G4103, 50 LB Bag), respectively ("00520917", "00520933").
# MAGIC
# MAGIC They were considered "Horse Vitamins & Supplements" and this is outside the current categorization.
# MAGIC
# MAGIC **They are the Triple Crown Diamond products: Senior, Perform, Balancer and Chopped Forage**

# COMMAND ----------

"50 lb Senior Textured Feed", "50 lb Perform Gold Feed", "50 lb Balancer Gold Feed"
sku = ["1433768", "1433769", "1433770"]


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC I could not find these products on our panel.
# MAGIC
# MAGIC 1. In another merchant Blains : Farm and Fleet, I was able to find "50 lb Senior Textured Feed", "50 lb Perform Gold Feed", "50 lb Balancer Gold Feed" (sku = ["1433768", "1433769", "1433770"]). But I see that they are Gold and not Diamond.
# MAGIC
# MAGIC 2. In Bomgaars, I found 2 Triple Crown products that are not appearing in out data ("Triple Crown Complete High Fat, High Fiber Formula, G4100, 50 LB Bag", Triple Crown Lite Formula, G4103, 50 LB Bag), respectively ("00520917", "00520933").
# MAGIC
# MAGIC They were considered "Horse Vitamins & Supplements" and this is outside the current categorization.
# MAGIC
# MAGIC Do you have an SKU for these products?

# COMMAND ----------


