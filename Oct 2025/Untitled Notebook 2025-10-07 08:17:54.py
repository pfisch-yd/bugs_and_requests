# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC   brand,
# MAGIC   sum(gmv)
# MAGIC from ydx_amorepacific_analysts_silver.amorepacific_v38_filter_items
# MAGIC where
# MAGIC brand = "LANC%EF%BF%BDME"
# MAGIC
# MAGIC group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC   brand,
# MAGIC   sum(gmv)
# MAGIC from ydx_amorepacific_analysts_silver.amorepacific_v38_filter_items
# MAGIC
# MAGIC group by 1
# MAGIC
# MAGIC order by 2 desc
# MAGIC
# MAGIC limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --- market share
# MAGIC
# MAGIC source
# MAGIC
# MAGIC select
# MAGIC   month,
# MAGIC   product,
# MAGIC   sum(gmv)
# MAGIC from source
# MAGIC group by all
# MAGIC
# MAGIC .
# MAGIC
# MAGIC add_sum_over as (
# MAGIC select
# MAGIC   *,
# MAGIC   sum(gmv) over (partition by product, mont)
# MAGIC from source
# MAGIC group by all
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select brand, sum(gmv)
# MAGIC from yd_sensitive_corporate.ydx_amorepacific_analysts_gold.amorepacific_v38_sku_time_series
# MAGIC where brand = "SIO BEAUTY"
# MAGIC GROUP BY brand
# MAGIC order by brand
# MAGIC -- gives us 7752772.922113713
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select brand, sum(gmv)
# MAGIC from yd_sensitive_corporate.ydx_amorepacific_analysts_gold.amorepacific_v38_market_share_for_column_null
# MAGIC where brand = "SIO BEAUTY"
# MAGIC GROUP BY brand
# MAGIC order by brand
# MAGIC -- gives us 9272244.020761093

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select brand, sum(gmv)
# MAGIC from yd_sensitive_corporate.ydx_amorepacific_analysts_gold.amorepacific_v38_market_share_for_column_null_standard_calendar
# MAGIC where brand = "SIO BEAUTY"
# MAGIC GROUP BY brand
# MAGIC order by brand
# MAGIC -- gives us 9272244.020761093

# COMMAND ----------

9272244.020761093 - 7752772.922113711

# COMMAND ----------

prod_analysis_gmv = 7752772.922113711
mtk_share_gmv = 9272244.02076112
no_webdescription_gmv = 1519471.0986473842

issue = mtk_share_gmv - prod_analysis_gmv

print( (issue - no_webdescription_gmv)/issue )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with combo_analysis as (
# MAGIC select 
# MAGIC             channel,
# MAGIC             parent_brand, 
# MAGIC             brand,
# MAGIC             sub_brand,
# MAGIC             merchant, 
# MAGIC             major_cat,
# MAGIC             sub_cat,
# MAGIC             minor_cat,
# MAGIC             web_description,
# MAGIC             sum(gmv) as gmv
# MAGIC from yd_sensitive_corporate.ydx_amorepacific_analysts_silver.amorepacific_v38_filter_items
# MAGIC where brand = "SIO BEAUTY"
# MAGIC group by all
# MAGIC )
# MAGIC
# MAGIC select
# MAGIC   1 as totaaal,
# MAGIC   sum(gmv)
# MAGIC from combo_analysis
# MAGIC where
# MAGIC web_description is null
# MAGIC
# MAGIC group by all
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC
# MAGIC     channel,
# MAGIC     parent_brand, 
# MAGIC     brand,
# MAGIC     sub_brand,
# MAGIC     merchant, 
# MAGIC     major_cat,
# MAGIC     sub_cat,
# MAGIC     minor_cat,
# MAGIC     web_description,
# MAGIC     sum(gmv)
# MAGIC from yd_sensitive_corporate.ydx_amorepacific_analysts_silver.amorepacific_v38_filter_items
# MAGIC where brand = "SIO BEAUTY"
# MAGIC group by all
