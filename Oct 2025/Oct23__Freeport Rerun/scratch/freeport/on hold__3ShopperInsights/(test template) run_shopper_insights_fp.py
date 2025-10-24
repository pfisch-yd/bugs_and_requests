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

# MAGIC %md
# MAGIC def export_shopper_insights__fp(
# MAGIC         sandbox_schema,
# MAGIC         prod_schema,
# MAGIC         demo_name
# MAGIC         ):
# MAGIC
# MAGIC     # Parameters embedded directly in query string
# MAGIC     sources = [
# MAGIC         {
# MAGIC             'database_name': sandbox_schema,
# MAGIC             'table_name': demo_name+"_filter_items",
# MAGIC             'catalog_name': "yd_sensitive_corporate",
# MAGIC         },
# MAGIC         {
# MAGIC             'database_name': 'ydx_retail_silver',
# MAGIC             'table_name': 'leia_active_users_2yr_222',
# MAGIC             'catalog_name': "yd_production",
# MAGIC         },
# MAGIC         {
# MAGIC             'database_name': 'ydx_retail_silver',
# MAGIC             'table_name': 'leia_annual_active_users',
# MAGIC             'catalog_name': "yd_production",
# MAGIC         },
# MAGIC         {
# MAGIC             'database_name': 'ydx_retail_silver',
# MAGIC             'table_name': 'generation_hh_size_adj_2yr',
# MAGIC             'catalog_name': "yd_production",
# MAGIC         },
# MAGIC         {
# MAGIC             'database_name': 'ydx_retail_silver',
# MAGIC             'table_name': 'generation_hh_size_adj_annual',
# MAGIC             'catalog_name': "yd_production",
# MAGIC         }
# MAGIC     ]
# MAGIC
# MAGIC     query_string = """
# MAGIC         -- SET use_cached_result = false;
# MAGIC
# MAGIC         with rolling_user_count as (
# MAGIC         SELECT count(distinct user_id) as rolling_active_users
# MAGIC         FROM {{ sources[1].full_name }}
# MAGIC         ),
# MAGIC
# MAGIC         annual_user_count as (
# MAGIC         SELECT year, count(distinct user_id) as annual_active_users
# MAGIC         FROM {{ sources[2].full_name }}
# MAGIC         GROUP BY 1
# MAGIC         ),
# MAGIC
# MAGIC         users as (
# MAGIC
# MAGIC         select a.*,
# MAGIC             case when a.leia_panel_flag = 1 and b.user_id is not null then 1 --user active for the rolling period and the year
# MAGIC             when a.leia_panel_flag = 1 then 2 --user active for the most rolling period but not the year
# MAGIC             when b.user_id is not null then 3 --user active for the annual year but not the rolling period
# MAGIC             else null
# MAGIC             end as leia_panel_flag_test,
# MAGIC             annual_active_users,
# MAGIC             rolling_active_users,
# MAGIC             --leia_panel_flag_source as leia_panel_flag,
# MAGIC
# MAGIC             case
# MAGIC         when age in ("55-64", "65+") then 'boomer_plus'
# MAGIC         when age in ("45-54") then 'gen_x'
# MAGIC         when age in ("35-44", "25-34") then 'millenial'
# MAGIC         when age in ("21-24", "18-20", "18-24") then 'gen_z'
# MAGIC         else null
# MAGIC         end as age_cohort
# MAGIC
# MAGIC         FROM {{ sources[0].full_name }} a
# MAGIC         LEFT join {{ sources[2].full_name }} b ON a.user_id = b.user_id and a.year = b.year
# MAGIC         LEFT JOIN rolling_user_count c
# MAGIC         LEFT JOIN annual_user_count d ON a.year = d.year
# MAGIC         )
# MAGIC
# MAGIC         select a.*, b.hh_size_adj_factor as age_adj_2yr, c.hh_size_adj_factor as age_adj_annual
# MAGIC         FROM users a
# MAGIC         LEFT JOIN {{ sources[3].full_name }} b on LEFT(a.year, 4) = b.year and a.age_cohort = b.age_cohort
# MAGIC         LEFT JOIN {{ sources[4].full_name }} c on LEFT(a.year, 4) = c.year and a.age_cohort = c.age_cohort
# MAGIC     """
# MAGIC
# MAGIC     module_name = "pfisch__test_shopper_insights"
# MAGIC
# MAGIC     query_template = get_or_create_query_template(
# MAGIC         slug=module_name,
# MAGIC         query_string=query_string,
# MAGIC         template_description="blueprints "+ module_name,
# MAGIC         version_description="Initial version - Shopper Insights module with Freeport integration",
# MAGIC     )
# MAGIC
# MAGIC     fp_catalog = "yd_fp_corporate_staging"
# MAGIC     ss_catalog = "yd_sensitive_corporate"
# MAGIC
# MAGIC     deliverable = get_or_create_deliverable(
# MAGIC         module_name+"__"+demo_name,
# MAGIC         query_template=query_template["id"],
# MAGIC         input_tables=[
# MAGIC             f"{ss_catalog}.{sandbox_schema}.{demo_name}_filter_items",
# MAGIC             "yd_production.ydx_retail_silver.leia_active_users_2yr_222",
# MAGIC             "yd_production.ydx_retail_silver.leia_annual_active_users",
# MAGIC             "yd_production.ydx_retail_silver.generation_hh_size_adj_2yr",
# MAGIC             "yd_production.ydx_retail_silver.generation_hh_size_adj_annual"
# MAGIC         ],
# MAGIC         output_table=fp_catalog+"."+prod_schema + "."+demo_name+'_filter_items',
# MAGIC         description="Shopper Insights analysis with demographics, age cohorts, and household size adjustments.",
# MAGIC         product_org="corporate",
# MAGIC         allow_major_version=True,
# MAGIC         allow_minor_version=True,
# MAGIC         staged_retention_days=100
# MAGIC     )
# MAGIC
# MAGIC     materialization = materialize_deliverable(
# MAGIC         deliverable["id"],
# MAGIC         release_on_success=False, # Set to false if you don't want to immediately publish
# MAGIC         wait_for_completion=True,
# MAGIC     )
# MAGIC
# MAGIC     return materialization

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

module_name = "pfisch__test_shopper_insights__2025_09_19__11_45"


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

# MAGIC %sql
# MAGIC
# MAGIC -- SET use_cached_result = false;
# MAGIC
# MAGIC     with rolling_user_count as (
# MAGIC     SELECT count(distinct user_id) as rolling_active_users
# MAGIC     FROM yd_sensitive_corporate.ydx_retail_silver.leia_active_users_2yr_222
# MAGIC     ),
# MAGIC
# MAGIC     annual_user_count as (
# MAGIC     SELECT year, count(distinct user_id) as annual_active_users
# MAGIC     FROM yd_sensitive_corporate.ydx_retail_silver.leia_annual_active_users
# MAGIC     GROUP BY 1
# MAGIC     ),
# MAGIC
# MAGIC     users as (
# MAGIC
# MAGIC     select a.*,
# MAGIC         case when a.leia_panel_flag = 1 and b.user_id is not null then 1 --user active for the rolling period and the year
# MAGIC         when a.leia_panel_flag = 1 then 2 --user active for the most rolling period but not the year
# MAGIC         when b.user_id is not null then 3 --user active for the annual year but not the rolling period
# MAGIC         else null
# MAGIC         end as leia_panel_flag_test,
# MAGIC         annual_active_users,
# MAGIC         rolling_active_users,
# MAGIC         --leia_panel_flag_source as leia_panel_flag,
# MAGIC
# MAGIC         case
# MAGIC     when age in ("55-64", "65+") then 'boomer_plus'
# MAGIC     when age in ("45-54") then 'gen_x'
# MAGIC     when age in ("35-44", "25-34") then 'millenial'
# MAGIC     when age in ("21-24", "18-20", "18-24") then 'gen_z'
# MAGIC     else null
# MAGIC     end as age_cohort
# MAGIC
# MAGIC     FROM yd_sensitive_corporate.ydx_internal_analysts_sandbox.testgraco_v38_filter_items a
# MAGIC     LEFT join yd_sensitive_corporate.ydx_retail_silver.leia_annual_active_users b ON a.user_id = b.user_id and a.year = b.year
# MAGIC     LEFT JOIN rolling_user_count c
# MAGIC     LEFT JOIN annual_user_count d ON a.year = d.year
# MAGIC     )
# MAGIC
# MAGIC     select a.*, b.hh_size_adj_factor as age_adj_2yr, c.hh_size_adj_factor as age_adj_annual
# MAGIC     FROM users a
# MAGIC     LEFT JOIN yd_sensitive_corporate.ydx_retail_silver.generation_hh_size_adj_2yr b on LEFT(a.year, 4) = b.year and a.age_cohort = b.age_cohort
# MAGIC     LEFT JOIN yd_sensitive_corporate.ydx_retail_silver.generation_hh_size_adj_annual c on LEFT(a.year, 4) = c.year and a.age_cohort = c.age_cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_catalog, table_schema, table_name
# MAGIC FROM information_schema.tables
# MAGIC WHERE table_schema = 'ydx_retail_silver'
# MAGIC
# MAGIC and table_name in ('generation_hh_size_adj_2yr', 'generation_hh_size_adj_annual', 'leia_active_users_2yr_222', 'leia_annual_active_users')

# COMMAND ----------

df = spark.sql(sql)

df.display()

# COMMAND ----------

# MAGIC %run "/Workspace/Users/pfisch@yipitdata.com/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/analysis/shopper_insights"

# COMMAND ----------

export_shopper_insights(
    sandbox_schema,
    prod_schema,
    demo_name
    )

# COMMAND ----------

original_table = spark.sql(f"""
    SELECT * FROM {prod_schema}.{demo_name}_filter_items
""")

original_table.display()

# COMMAND ----------

# MAGIC %run "/Workspace/Users/pfisch@yipitdata.com/corporate_transformation_blueprints/corporate_transformation_blueprints/scratch_pfisch/freeport/testing/compare_df_function"

# COMMAND ----------



# COMMAND ----------

original = original_table
freeport_df = df
compare_df(original, freeport_df, show_sample_differences=True, verbose=True)
