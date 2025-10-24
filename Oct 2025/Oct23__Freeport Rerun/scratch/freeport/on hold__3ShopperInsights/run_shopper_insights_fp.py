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

data_hora = "2025_09_19__11_45"

# COMMAND ----------

def export_shopper_insights__fp(
        sandbox_schema,
        prod_schema,
        demo_name
        ):

    # Parameters embedded directly in query string
    sources = [
        {
            'database_name': sandbox_schema,
            'table_name': demo_name+"_filter_items",
            'catalog_name': "yd_sensitive_corporate",
        },
        {
            'database_name': 'ydx_retail_silver',
            'table_name': 'leia_active_users_2yr_222',
            'catalog_name': "yd_sensitive_corporate",
        },
        {
            'database_name': 'ydx_retail_silver',
            'table_name': 'leia_annual_active_users',
            'catalog_name': "yd_sensitive_corporate",
        },
        {
            'database_name': 'ydx_retail_silver',
            'table_name': 'generation_hh_size_adj_2yr',
            'catalog_name': "yd_sensitive_corporate",
        },
        {
            'database_name': 'ydx_retail_silver',
            'table_name': 'generation_hh_size_adj_annual',
            'catalog_name': "yd_sensitive_corporate",
        }
    ]

    query_string = """
        -- SET use_cached_result = false;

        with rolling_user_count as (
        SELECT count(distinct user_id) as rolling_active_users
        FROM {{ sources[1].full_name }}
        ),

        annual_user_count as (
        SELECT year, count(distinct user_id) as annual_active_users
        FROM {{ sources[2].full_name }}
        GROUP BY 1
        ),

        users as (

        select a.*,
            case when a.leia_panel_flag = 1 and b.user_id is not null then 1 --user active for the rolling period and the year
            when a.leia_panel_flag = 1 then 2 --user active for the most rolling period but not the year
            when b.user_id is not null then 3 --user active for the annual year but not the rolling period
            else null
            end as leia_panel_flag_test,
            annual_active_users,
            rolling_active_users,
            --leia_panel_flag_source as leia_panel_flag,

            case
        when age in ("55-64", "65+") then 'boomer_plus'
        when age in ("45-54") then 'gen_x'
        when age in ("35-44", "25-34") then 'millenial'
        when age in ("21-24", "18-20", "18-24") then 'gen_z'
        else null
        end as age_cohort

        FROM {{ sources[0].full_name }} a
        LEFT join {{ sources[2].full_name }} b ON a.user_id = b.user_id and a.year = b.year
        LEFT JOIN rolling_user_count c
        LEFT JOIN annual_user_count d ON a.year = d.year
        )

        select a.*, b.hh_size_adj_factor as age_adj_2yr, c.hh_size_adj_factor as age_adj_annual
        FROM users a
        LEFT JOIN {{ sources[3].full_name }} b on LEFT(a.year, 4) = b.year and a.age_cohort = b.age_cohort
        LEFT JOIN {{ sources[4].full_name }} c on LEFT(a.year, 4) = c.year and a.age_cohort = c.age_cohort
    """

    module_name = "pfisch__test_shopper_insights__202509196"

    query_template = get_or_create_query_template(
        slug=module_name,
        query_string=query_string,
        template_description="blueprints "+ module_name,
        version_description="Initial version - Shopper Insights module with Freeport integration",
    )

    fp_catalog = "yd_fp_corporate_staging"
    ss_catalog = "yd_sensitive_corporate"

    deliverable = get_or_create_deliverable(
        module_name+"__"+demo_name,
        query_template=query_template["id"],
        input_tables=[
            f"{ss_catalog}.{sandbox_schema}.{demo_name}_filter_items",
            "yd_sensitive_corporate.ydx_retail_silver.leia_active_users_2yr_222",
            "yd_sensitive_corporate.ydx_retail_silver.leia_annual_active_users",
            "yd_sensitive_corporate.ydx_retail_silver.generation_hh_size_adj_2yr",
            "yd_sensitive_corporate.ydx_retail_silver.generation_hh_size_adj_annual"
        ],
        output_table=fp_catalog+"."+prod_schema + "."+demo_name+'_filter_items__202509196',
        description="Shopper Insights analysis with demographics, age cohorts, and household size adjustments.",
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

    return materialization
