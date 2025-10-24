# Databricks notebook source
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from yipit_databricks_utils.future import create_table

# COMMAND ----------

# MAGIC %run ../core/setup

# COMMAND ----------

# DBTITLE 1,Pro Insights Analysis

def export_pro_insights(
        sandbox_schema,
        prod_schema,
        demo_name,
        pro_source_table,
        start_date_of_data
        ):
    module_name = '_pro_insights'
    pro_insights = spark.sql(f"""
        -- SET use_cached_result = false;
                         
        with cats as (
        SELECT merchant, web_description, parent_brand, brand, sub_brand, major_cat, sub_cat, minor_cat, merchant_clean
        FROM {sandbox_schema}.{demo_name}_filter_items
        GROUP BY 1,2,3,4,5,6,7,8,9
        )

        SELECT a.* except (brand, product_description), b.merchant_clean, b.parent_brand, b.brand, b.sub_brand, b.major_cat, b.sub_cat, b.minor_cat, product_description as web_description
        FROM {pro_source_table} a
        LEFT JOIN cats b on a.merchant = b.merchant and a.product_description = b.web_description AND lower(a.brand) = lower(b.brand)
        WHERE b.major_cat is not null
        AND month >= '{start_date_of_data}'
        AND month < date_trunc('month', date_add(current_date(), -13))
    """)

    create_blueprints_table(module_name,
    prod_schema,
    demo_name+'_pro_insights',
    pro_insights
    )

    return spark.table(f'{prod_schema}.{demo_name}_pro_insights')

# COMMAND ----------

# DBTITLE 1,Package Wrapper
def run_export_pro_insights(
        sandbox_schema,
        prod_schema,
        demo_name,
        pro_source_table,
        start_date_of_data
        ):
    
    export_pro_insights(
        sandbox_schema,
        prod_schema,
        demo_name,
        pro_source_table,
        start_date_of_data
    )

    print('Succesfully exported Pro Insights module.')
