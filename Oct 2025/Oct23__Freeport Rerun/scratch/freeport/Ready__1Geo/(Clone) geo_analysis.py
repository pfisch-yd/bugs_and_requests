# Databricks notebook source
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from yipit_databricks_utils.future import create_table

# COMMAND ----------

# MAGIC %run ../core/setup

# COMMAND ----------

# DBTITLE 1,Geo Analysis
def export_geo_analysis(
        sandbox_schema,
        prod_schema,
        demo_name
        ):
    module_name = '_geographic_analysis'
    geo_analysis = spark.sql(f"""
        -- SET use_cached_result = false;
                         
        with top_brands as (
            SELECT parent_brand, sum(gmv) as gmv, count(*) as sample_size
            FROM {sandbox_schema}.{demo_name}_filter_items
            GROUP BY 1
            HAVING sample_size > 1000
            ),

        demos as (
            SELECT 
                state, 
                month, 
                parent_brand, 
                case when brand IN ('Unknown', '', 'Unknown Brand') or brand is null then 'Unbranded' else brand end as brand,
                sub_brand,
                merchant_clean as merchant, 
                major_cat, sub_cat, minor_cat, 
                sum(gmv) as gmv, count(*) as sample_size, 
                SUM(item_price) as observed_spend, 
                SUM(item_quantity) as observed_units
            FROM {sandbox_schema}.{demo_name}_filter_items
            GROUP BY 1,2,3,4,5,6,7,8,9
            )

        SELECT 
            c.state_name as state, 
            month, 
            case 
                when b.parent_brand is null then "Other Brands" 
                else a.parent_brand 
            end as parent_brand,
            case 
                when b.parent_brand is null then "Other Brands" 
                else a.brand 
            end as brand,
            case 
                when b.parent_brand is null then "Other Brands" 
                else a.sub_brand
            end as sub_brand, 
            a.merchant, 
            major_cat, sub_cat, minor_cat, 
            SUM(a.gmv) as gmv, 
            sum(a.sample_size) as sample_size, 
            SUM(observed_spend) as observed_spend, 
            SUM(observed_units) as observed_units
        FROM demos a
        LEFT JOIN top_brands b ON a.parent_brand = b.parent_brand
        LEFT JOIN yd_production.ydx_retail.clean_state_names c on a.state = c.state
        GROUP BY 1,2,3,4,5,6,7,8,9

    """)

    create_blueprints_table(module_name,
    prod_schema,
    demo_name+'_geographic_analysis',
    geo_analysis
    )

    return spark.table(f'{prod_schema}.{demo_name}_geographic_analysis')

# COMMAND ----------

# DBTITLE 1,Package Wrapper
def run_export_geo_analysis_module(
        sandbox_schema,
        prod_schema,
        demo_name
        ):
    
    export_geo_analysis(
        sandbox_schema,
        prod_schema,
        demo_name
    )

    print('Succesfully exported Geo Analysis module.')

# COMMAND ----------


