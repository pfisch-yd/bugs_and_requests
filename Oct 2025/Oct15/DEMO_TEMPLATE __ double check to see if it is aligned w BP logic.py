# Databricks notebook source
# MAGIC %md
# MAGIC #V3.8 Corporate Dashboard Export Function

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC To update your client parameters, submit this ticket. Changes will be automatically reflected.
# MAGIC
# MAGIC  https://docs.google.com/forms/d/e/1FAIpQLSetveoNR1i_9nWWebCOYbSdWYxqZkacJsCMhgt5-r0XuiTOWw/viewform
# MAGIC
# MAGIC
# MAGIC To see all demos look here:
# MAGIC
# MAGIC https://docs.google.com/spreadsheets/d/1F8bvKrBdlY0GIm9xAU5v3IM7ERLM6kUdi_wYph9JJds/edit?resourcekey=&gid=1374540499#gid=1374540499

# COMMAND ----------

# DBTITLE 1,Discovery
# MAGIC %sql
# MAGIC select merchant, sum(gmv) gmv
# MAGIC from ydx_retail_silver.master_items_table
# MAGIC where lower(web_description) rlike 'baseball|base ball'
# MAGIC and year ='2025-01-01'
# MAGIC group by all
# MAGIC order by gmv desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select uct_cat_1, uct_cat_2, uct_cat_3, uct_cat_4, sum(gmv) gmv
# MAGIC from ydx_retail_silver.master_items_table
# MAGIC where lower(web_description) rlike 'baseball|base ball'
# MAGIC and year ='2025-01-01'
# MAGIC group by all
# MAGIC order by gmv desc

# COMMAND ----------

# DBTITLE 1,dk
# MAGIC %sql
# MAGIC create or replace temp view rawlings as(
# MAGIC
# MAGIC     select *, 
# MAGIC     "Sports" as L1,
# MAGIC     "Baseball" as L2,
# MAGIC     case
# MAGIC     when uct_cat_4 in ('Baseball Bats','Baseballs','Baseball & Softball Batting Gloves','Baseball & Softball Gloves & Mitts','Baseball & Softball Protective Gear') then uct_cat_4
# MAGIC     when sub_cat_5='batting helmets' then 'Baseball & Softball Protective Gear'
# MAGIC     when (uct_cat_4='Athletic Pants' and lower(web_description) rlike 'base') then 'Baseball Pants'
# MAGIC     when (uct_cat_3='Sneakers & Athletic' and lower(web_description) rlike 'base') then 'Baseball Sneakers & Cleats'
# MAGIC     end L3
# MAGIC     from ydx_retail_silver.master_items_table
# MAGIC     where merchant rlike 'amazon_l|dicks|hibb|targ|scheel|walmar|nike'
# MAGIC     or merchant = 'academy_sports'
# MAGIC     and (uct_cat_4 in ('Baseball Bats','Baseballs','Baseball & Softball Batting Gloves','Baseball & Softball Gloves & Mitts','Baseball & Softball Protective Gear')
# MAGIC     or
# MAGIC     (uct_cat_4='Athletic Pants' and lower(web_description) rlike 'base')
# MAGIC     or
# MAGIC     (uct_cat_3='Sneakers & Athletic' and lower(web_description) rlike 'base')
# MAGIC     )
# MAGIC     and sub_cat_5!='bags & bat packs' 
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select merchant, count(*) ct, sum(gmv) gmv 
# MAGIC from rawlings
# MAGIC where order_date between '2025-07-01' and '2025-09-30'
# MAGIC and L3='Baseball Bats'
# MAGIC group by all
# MAGIC sort by ct desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select brand, sku, web_description, count(*) ct 
# MAGIC from rawlings 
# MAGIC where L3='Baseball Bats'
# MAGIC group by all 
# MAGIC sort by ct desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select merchant, L3, sum(gmv) gmv
# MAGIC from rawlings
# MAGIC where order_date between '2025-07-01' and '2025-09-30'
# MAGIC and L3='Baseball Bats'
# MAGIC group by all 
# MAGIC sort by gmv desc

# COMMAND ----------

# DBTITLE 1,skus
# MAGIC %sql
# MAGIC select brand, web_description, uct_cat_3, uct_cat_4, count(*) ct, sum(gmv_adj) gmv
# MAGIC from yd_production.cr_aso_analysts.items_final
# MAGIC where order_date between '2025-07-01' and '2025-09-30'
# MAGIC and sub_caT_3='baseball bats'
# MAGIC group by all 
# MAGIC sort by gmv desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select brand, web_Description, uct_cat_3, uct_cat_4, count(*) ct, sum(gmv) gmv
# MAGIC from ydx_retail_silver.master_items_table
# MAGIC where order_date between '2025-07-01' and '2025-09-30'
# MAGIC and sub_caT_3='baseball bats'
# MAGIC and merchant='academy_sports'
# MAGIC group by all 
# MAGIC sort by gmv desc

# COMMAND ----------

# DBTITLE 1,uct vs subcat
# MAGIC %sql
# MAGIC select brand, web_Description, uct_cat_3, uct_cat_4, count(*) ct, sum(gmv_adj) gmv
# MAGIC from yd_production.cr_aso_analysts.items_final
# MAGIC where order_date between '2025-07-01' and '2025-09-30'
# MAGIC and sub_caT_3='baseball bats'
# MAGIC group by all 
# MAGIC sort by gmv desc

# COMMAND ----------

# DBTITLE 1,aso crdt
# MAGIC %sql
# MAGIC select sub_cat_1, sub_cat_2, sub_cat_3, count(*) ct, sum(gmv_adj) gmv
# MAGIC from yd_production.cr_aso_analysts.items_final
# MAGIC where lower(sub_cat_1) rlike 'base ball|baseball'
# MAGIC    or lower(sub_cat_2) rlike 'base ball|baseball'
# MAGIC    or lower(sub_cat_3) rlike 'base ball|baseball'
# MAGIC    or lower(sub_cat_4) rlike 'base ball|baseball'
# MAGIC    or lower(sub_cat_5) rlike 'base ball|baseball'
# MAGIC and order_date between '2025-07-01' and '2025-09-30'
# MAGIC group by all 
# MAGIC sort by gmv desc

# COMMAND ----------

# DBTITLE 1,l3
# MAGIC %sql
# MAGIC select merchant, L3, sum(gmv) gmv
# MAGIC from rawlings
# MAGIC where order_date between '2025-07-01' and '2025-09-30'
# MAGIC and L3='Baseball Bats'
# MAGIC group by all
# MAGIC order by gmv desc

# COMMAND ----------

# DBTITLE 1,Disco Here
# MAGIC %sql
# MAGIC select merchant, L3, sub_cat_1, sub_cat_2, sub_cat_3, sub_cat_4, sub_cat_5, sum(gmv) gmv
# MAGIC from rawlings
# MAGIC --where order_date between '2025-07-01' and '2025-09-30'
# MAGIC group by all
# MAGIC order by gmv desc

# COMMAND ----------


from yipit_databricks_utils.future import create_table


df = spark.table("rawlings")


create_table(
	"ydx_prospect_analysts_sandbox",
	"rawlings",
	df,
	overwrite=True,
)

# COMMAND ----------

# DBTITLE 1,Run this first!
# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/Brands Dashboard Templates/__centralized_udfs_for_client_dashboard_exports"

# COMMAND ----------

ask_for_help()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Inputs

# COMMAND ----------

# need some help remembering the name?
# try the function
search_demo_name("rawlings")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Here we define the demo name

# COMMAND ----------

demo_name = "rawlings"

# COMMAND ----------

print(demo_name)

# COMMAND ----------

check_your_parameters(demo_name)

# COMMAND ----------

run_everything(demo_name)
