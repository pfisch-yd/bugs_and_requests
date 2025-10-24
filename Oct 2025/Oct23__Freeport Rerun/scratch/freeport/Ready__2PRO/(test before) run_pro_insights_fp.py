# Databricks notebook source
import sys

sys.path.append("/Workspace/Repos/ETL_Production/freeport_service/")

from freeport_databricks.client.v1 import (
    get_or_create_deliverable,
    materialize_deliverable,
    sql_from_query_template,
    df_from_query_template,
    get_or_create_query_template
)

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from yipit_databricks_utils.future import create_table

# COMMAND ----------

data_hora = "2025_09_19__09_00"

# COMMAND ----------

def export_pro_insights__fp(
        sandbox_schema,
        prod_schema,
        demo_name,
        pro_source_table,
        start_date_of_data
        ):

    # Parameters embedded directly in query string

    sources = [
        {
            'database_name': sandbox_schema,
            'table_name': demo_name+"_filter_items",
            'catalog_name': "yd_sensitive_corporate",
        },
        # pro_source_table = "ydx_retail_silver.edison_pro_items"
        {
            'database_name': pro_source_table.split('.')[1] if '.' in pro_source_table else "ydx_retail_silver",
            'table_name': pro_source_table.split('.')[-1] if '.' in pro_source_table else "edison_pro_items",
            'catalog_name': "yd_sensitive_corporate",
        }
    ]


    query_string = f"""
        -- SET use_cached_result = false;

        with cats as (
        SELECT merchant, web_description, parent_brand, brand, sub_brand, major_cat, sub_cat, minor_cat, merchant_clean
        FROM {{{{ sources[0].full_name }}}}
        GROUP BY 1,2,3,4,5,6,7,8,9
        )

        SELECT
            a.* except (brand, product_description),
            b.merchant_clean,
            b.parent_brand,
            b.brand,
            b.sub_brand,
            b.major_cat,
            b.sub_cat,
            b.minor_cat,
            product_description as web_description
        FROM {{{{ sources[1].full_name }}}} a
        LEFT JOIN cats b on a.merchant = b.merchant
            and a.product_description = b.web_description
            AND lower(a.brand) = lower(b.brand)
        WHERE b.major_cat is not null
        AND month >= '{start_date_of_data}'
        AND month < date_trunc('month', date_add(current_date(), -13))
    """

    module_name = "pfisch__test_pro_insights"

    query_template = get_or_create_query_template(
        slug=module_name,
        query_string=query_string,
        template_description="blueprints "+ module_name,
        version_description="Initial version - Pro Insights module with Freeport integration",
    )

    fp_catalog = "yd_fp_corporate_staging"
    ss_catalog = "yd_sensitive_corporate"

    pro_input_table = "yd_sensitive_corporate." + pro_source_table

    deliverable = get_or_create_deliverable(
        module_name+"__"+demo_name,
        query_template=query_template["id"],
        input_tables=[
            f"{ss_catalog}.{sandbox_schema}.{demo_name}_filter_items",
            pro_input_table
        ],
        output_table=fp_catalog+"."+prod_schema + "."+demo_name+'_pro_insights',
        description="Pro Insights analysis deliverable with enhanced product data and category mapping.",
        product_org="corporate",
        allow_major_version=True,
        allow_minor_version=True,
        staged_retention_days=100
    )

    materialization = materialize_deliverable(
        deliverable["id"],
        release_on_success=False, # Set to false if you don't want to immediately publish
        wait_for_completion=True,
    )

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testgraco"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

# COMMAND ----------

# Parameters embedded directly in query string

sources = [
    {
        'database_name': sandbox_schema,
        'table_name': demo_name+"_filter_items",
        'catalog_name': "yd_sensitive_corporate",
    },
    # pro_source_table = "ydx_retail_silver.edison_pro_items"
    {
        'database_name': pro_source_table.split('.')[0] if '.' in pro_source_table else "ydx_retail_silver",
        'table_name': pro_source_table.split('.')[-1] if '.' in pro_source_table else "edison_pro_items",
        'catalog_name': "yd_sensitive_corporate",
    }
]




# COMMAND ----------

query_string = f"""
    -- SET use_cached_result = false;

    with cats as (
    SELECT merchant, web_description, parent_brand, brand, sub_brand, major_cat, sub_cat, minor_cat, merchant_clean
    FROM {{{{ sources[0].full_name }}}}
    GROUP BY 1,2,3,4,5,6,7,8,9
    )

    SELECT
        a.* except (brand, product_description),
        b.merchant_clean,
        b.parent_brand,
        b.brand,
        b.sub_brand,
        b.major_cat,
        b.sub_cat,
        b.minor_cat,
        product_description as web_description
    FROM {{{{ sources[1].full_name }}}} a
    LEFT JOIN cats b on a.merchant = b.merchant
        and a.product_description = b.web_description
        AND lower(a.brand) = lower(b.brand)
    WHERE b.major_cat is not null
    AND month >= '{start_date_of_data}'
    AND month < date_trunc('month', date_add(current_date(), -13))
"""



# COMMAND ----------

query_parameters = {
    "metrics": {
        "column": "vin",
    },
}

sql = sql_from_query_template(
    query_parameters=query_parameters,
    sources=sources, 
    query_string=query_string,
)

spark.sql(sql)

# COMMAND ----------

df = spark.sql(sql)

df.display()

# COMMAND ----------

original_table = spark.sql(f"""
    SELECT * FROM {prod_schema}.{demo_name}_pro_insights
""")

original_table.display()

# COMMAND ----------

# MAGIC %run "/Workspace/Users/pfisch@yipitdata.com/corporate_transformation_blueprints/corporate_transformation_blueprints/scratch_pfisch/freeport/testing/compare_df_function"

# COMMAND ----------

original = original_table
freeport_df = df
compare_df(original, freeport_df, show_sample_differences=True, verbose=True)
