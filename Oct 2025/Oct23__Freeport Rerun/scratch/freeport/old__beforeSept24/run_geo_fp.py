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

data_hora = "2025_09_08__15_12"

# COMMAND ----------

def export_geo_analysis__fp(
        sandbox_schema,
        prod_schema,
        demo_name
        ):

    query_parameters = {
        "metrics": {
            "column": "sub_cat",
        },
    }

    sources = [
        {
            'database_name': sandbox_schema,
            'table_name': demo_name+"_filter_items",
            'catalog_name': "yd_sensitive_corporate",
        },
    ]

    query_string = """
        with top_brands as (
                SELECT brand, sum(gmv) as gmv, count(*) as sample_size
                FROM {{ sources[0].full_name }}
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
        FROM {{ sources[0].full_name }}
        GROUP BY 1,2,3,4,5,6,7,8,9
        )

        SELECT
            c.state_name as state,
            month,
            case
                when b.brand is null then "Other Brands"
                else a.parent_brand
            end as parent_brand,
            a.brand,
            a.sub_brand,
            a.merchant,
            major_cat, sub_cat, minor_cat,
            SUM(a.gmv) as gmv,
            sum(a.sample_size) as sample_size,
            SUM(observed_spend) as observed_spend,
            SUM(observed_units) as observed_units
        FROM demos a
        LEFT JOIN top_brands b ON a.parent_brand = b.brand
        LEFT JOIN yd_production.ydx_retail.clean_state_names c on a.state = c.state
        GROUP BY 1,2,3,4,5,6,7,8,9
    """
    data_hora = "2025_09_08__15_12"
    module_name = "pfisch__test_geo_analysis"

    query_template = get_or_create_query_template(
        slug=module_name,
        query_string=query_string,
        template_description="blueprints "+ module_name,
        version_description="Initial version",
    )

    fp_catalog = "yd_fp_corporate_staging"
    ss_catalog = "yd_sensitive_corporate"

    deliverable = get_or_create_deliverable(
        module_name+"__"+demo_name,
        query_template=query_template["id"],
        input_tables=ss_catalog+"."+sandbox_schema+"."+demo_name+"_filter_items",
        output_table=fp_catalog+"."+prod_schema + "."+demo_name+'_geographic_analysis',
        description="First test deliverable to validate Freeport and Dispatch system setup.",
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
