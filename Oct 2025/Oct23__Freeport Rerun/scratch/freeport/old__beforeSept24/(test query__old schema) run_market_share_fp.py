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

data_hora = "2025_09_19__13_13"

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
    }
]

# Create display interval mapping first
query_string = """
    -- SET use_cached_result = false;

    with
    clean_special_attribute_and_columns as (
    select
        * except (merchant),
        coalesce(cast({{ parameters.metrics.column }} as string), major_cat) as special_attribute,
        date_trunc('month', order_date) as trunc_day,
        brand,
        merchant_clean as merchant
        from {{ sources[0].full_name }}
    ),

    grouping_by_all_possible_controls as (
    SELECT
        trunc_day,
        channel,
        parent_brand,
        brand,
        sub_brand,
        merchant,
        major_cat,
        sub_cat,
        minor_cat,
        special_attribute, -- Notice: If no special attribute, then major
        -- and this is redundant. So this grouping goes from 10 args to 9 args

        sum(gmv) as gmv,
        count(*) as sample_size,
        sum(item_price * item_quantity) as item_subtotal_observed,
        sum(item_quantity) as item_quantity_observed
    from clean_special_attribute_and_columns
    group by 1,2,3,4,5,6,7,8,9,10
    ),

    --- Y : getting out of the main road
    calculate_date_range AS (
    SELECT
        *,
        min(trunc_day) over (partition by
            channel,
            parent_brand,
            brand,
            sub_brand,
            merchant,
            major_cat,
            sub_cat,
            minor_cat,
            special_attribute
        ) AS min_day,
        max(trunc_day) over (partition by
            channel,
            parent_brand,
            brand,
            sub_brand,
            merchant,
            major_cat,
            sub_cat,
            minor_cat,
            special_attribute
        ) AS max_day
    FROM grouping_by_all_possible_controls
    ),

    create_a_sequence_array as (
    select
        sequence(
            0,
            CAST(
                months_between(
                    /* capped_end_day */
                    least(
                        add_months(max_day, 12),          -- allow up to +12 m
                        date_trunc('month', MAX(trunc_day) over ()) -- but never beyond max trunc_day of the entire table
                    ),
                    min_day
                ) AS INT
            )
        )           AS month_sequence,
        *
    from calculate_date_range
    ),

    unnest_all_months as (
    SELECT
        create_a_sequence_array.*,
        min_day,
        max_day,
        n AS month_offset  -- Position in the array
    FROM create_a_sequence_array
    LATERAL VIEW posexplode(month_sequence) AS n, month_offset
    ),

    organize_data as (
        select
        --- if it is a non existing row, it will be used the filler row
        add_months(min_day, month_offset) as trunc_day,

        --- clean some rows
        * except (
            trunc_day,
            month_sequence, min_day, max_day, month_offset,
            sample_size,gmv,item_subtotal_observed, item_quantity_observed
            ),

        --- if it is a filler row, than metrics are 0 (not null)
        case when trunc_day = add_months(min_day, month_offset) then sample_size else 0 end as sample_size,
        case when trunc_day = add_months(min_day, month_offset) then gmv else 0 end as gmv,
        case when trunc_day = add_months(min_day, month_offset) then item_subtotal_observed else 0 end as item_subtotal_observed,
        case when trunc_day = add_months(min_day, month_offset) then item_quantity_observed else 0 end as item_quantity_observed
        from unnest_all_months
    ),

    deduplicate as (
        select
            trunc_day,
            channel,
            parent_brand,
            brand,
            sub_brand,
            merchant,
            major_cat,
            sub_cat,
            minor_cat,
            special_attribute,

            sum(gmv) as gmv,
            sum(sample_size) as sample_size,
            sum(item_subtotal_observed) as item_subtotal_observed,
            sum(item_quantity_observed) as item_quantity_observed
        from organize_data
        group by all
    )

    select * from deduplicate
"""

query_parameters = {
    "metrics": {
        "column": "major_cat",
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
# MAGIC     with
# MAGIC     clean_special_attribute_and_columns as (
# MAGIC     select
# MAGIC         * except (merchant),
# MAGIC         coalesce(cast(major_cat as string), major_cat) as special_attribute,
# MAGIC         date_trunc('month', order_date) as trunc_day,
# MAGIC         brand,
# MAGIC         merchant_clean as merchant
# MAGIC         from yd_sensitive_corporate.ydx_internal_analysts_sandbox.testgraco_v38_filter_items
# MAGIC     ),
# MAGIC
# MAGIC     grouping_by_all_possible_controls as (
# MAGIC     SELECT
# MAGIC         trunc_day,
# MAGIC         channel,
# MAGIC         parent_brand,
# MAGIC         brand,
# MAGIC         sub_brand,
# MAGIC         merchant,
# MAGIC         major_cat,
# MAGIC         sub_cat,
# MAGIC         minor_cat,
# MAGIC         special_attribute, -- Notice: If no special attribute, then major
# MAGIC         -- and this is redundant. So this grouping goes from 10 args to 9 args
# MAGIC
# MAGIC         sum(gmv) as gmv,
# MAGIC         count(*) as sample_size,
# MAGIC         sum(item_price * item_quantity) as item_subtotal_observed,
# MAGIC         sum(item_quantity) as item_quantity_observed
# MAGIC     from clean_special_attribute_and_columns
# MAGIC     group by 1,2,3,4,5,6,7,8,9,10
# MAGIC     ),
# MAGIC
# MAGIC     --- Y : getting out of the main road
# MAGIC     calculate_date_range AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         min(trunc_day) over (partition by
# MAGIC             channel,
# MAGIC             parent_brand,
# MAGIC             brand,
# MAGIC             sub_brand,
# MAGIC             merchant,
# MAGIC             major_cat,
# MAGIC             sub_cat,
# MAGIC             minor_cat,
# MAGIC             special_attribute
# MAGIC         ) AS min_day,
# MAGIC         max(trunc_day) over (partition by
# MAGIC             channel,
# MAGIC             parent_brand,
# MAGIC             brand,
# MAGIC             sub_brand,
# MAGIC             merchant,
# MAGIC             major_cat,
# MAGIC             sub_cat,
# MAGIC             minor_cat,
# MAGIC             special_attribute
# MAGIC         ) AS max_day
# MAGIC     FROM grouping_by_all_possible_controls
# MAGIC     ),
# MAGIC
# MAGIC     create_a_sequence_array as (
# MAGIC     select
# MAGIC         sequence(
# MAGIC             0,
# MAGIC             CAST(
# MAGIC                 months_between(
# MAGIC                     /* capped_end_day */
# MAGIC                     least(
# MAGIC                         add_months(max_day, 12),          -- allow up to +12 m
# MAGIC                         date_trunc('month', MAX(trunc_day) over ()) -- but never beyond max trunc_day of the entire table
# MAGIC                     ),
# MAGIC                     min_day
# MAGIC                 ) AS INT
# MAGIC             )
# MAGIC         )           AS month_sequence,
# MAGIC         *
# MAGIC     from calculate_date_range
# MAGIC     ),
# MAGIC
# MAGIC     unnest_all_months as (
# MAGIC     SELECT
# MAGIC         create_a_sequence_array.*,
# MAGIC         min_day,
# MAGIC         max_day,
# MAGIC         n AS month_offset  -- Position in the array
# MAGIC     FROM create_a_sequence_array
# MAGIC     LATERAL VIEW posexplode(month_sequence) AS n, month_offset
# MAGIC     ),
# MAGIC
# MAGIC     organize_data as (
# MAGIC         select
# MAGIC         --- if it is a non existing row, it will be used the filler row
# MAGIC         add_months(min_day, month_offset) as trunc_day,
# MAGIC
# MAGIC         --- clean some rows
# MAGIC         * except (
# MAGIC             trunc_day,
# MAGIC             month_sequence, min_day, max_day, month_offset,
# MAGIC             sample_size,gmv,item_subtotal_observed, item_quantity_observed
# MAGIC             ),
# MAGIC
# MAGIC         --- if it is a filler row, than metrics are 0 (not null)
# MAGIC         case when trunc_day = add_months(min_day, month_offset) then sample_size else 0 end as sample_size,
# MAGIC         case when trunc_day = add_months(min_day, month_offset) then gmv else 0 end as gmv,
# MAGIC         case when trunc_day = add_months(min_day, month_offset) then item_subtotal_observed else 0 end as item_subtotal_observed,
# MAGIC         case when trunc_day = add_months(min_day, month_offset) then item_quantity_observed else 0 end as item_quantity_observed
# MAGIC         from unnest_all_months
# MAGIC     ),
# MAGIC
# MAGIC     deduplicate as (
# MAGIC         select
# MAGIC             trunc_day,
# MAGIC             channel,
# MAGIC             parent_brand,
# MAGIC             brand,
# MAGIC             sub_brand,
# MAGIC             merchant,
# MAGIC             major_cat,
# MAGIC             sub_cat,
# MAGIC             minor_cat,
# MAGIC             special_attribute,
# MAGIC
# MAGIC             sum(gmv) as gmv,
# MAGIC             sum(sample_size) as sample_size,
# MAGIC             sum(item_subtotal_observed) as item_subtotal_observed,
# MAGIC             sum(item_quantity_observed) as item_quantity_observed
# MAGIC         from organize_data
# MAGIC         group by all
# MAGIC     )
# MAGIC
# MAGIC     select * from deduplicate

# COMMAND ----------

# Parameters embedded directly in query string
sources = [
    {
        'database_name': sandbox_schema,
        'table_name': demo_name+"_filter_items",
        'catalog_name': "yd_sensitive_corporate",
    }
]

# Create display interval mapping first
query_string = f"""
    -- SET use_cached_result = false;

    with
    clean_special_attribute_and_columns as (
    select
        * except (merchant),
        coalesce(cast({column} as string), major_cat) as special_attribute,
        date_trunc('month', order_date) as trunc_day,
        brand,
        merchant_clean as merchant
        from {{{{ sources[0].full_name }}}}
    ),

    grouping_by_all_possible_controls as (
    SELECT
        trunc_day,
        channel,
        parent_brand,
        brand,
        sub_brand,
        merchant,
        major_cat,
        sub_cat,
        minor_cat,
        special_attribute, -- Notice: If no special attribute, then major
        -- and this is redundant. So this grouping goes from 10 args to 9 args

        sum(gmv) as gmv,
        count(*) as sample_size,
        sum(item_price * item_quantity) as item_subtotal_observed,
        sum(item_quantity) as item_quantity_observed
    from clean_special_attribute_and_columns
    group by 1,2,3,4,5,6,7,8,9,10
    ),

    --- Y : getting out of the main road
    calculate_date_range AS (
    SELECT
        *,
        min(trunc_day) over (partition by
            channel,
            parent_brand,
            brand,
            sub_brand,
            merchant,
            major_cat,
            sub_cat,
            minor_cat,
            special_attribute
        ) AS min_day,
        max(trunc_day) over (partition by
            channel,
            parent_brand,
            brand,
            sub_brand,
            merchant,
            major_cat,
            sub_cat,
            minor_cat,
            special_attribute
        ) AS max_day
    FROM grouping_by_all_possible_controls
    ),

    create_a_sequence_array as (
    select
        sequence(
            0,
            CAST(
                months_between(
                    /* capped_end_day */
                    least(
                        add_months(max_day, 12),          -- allow up to +12 m
                        date_trunc('month', MAX(trunc_day) over ()) -- but never beyond max trunc_day of the entire table
                    ),
                    min_day
                ) AS INT
            )
        )           AS month_sequence,
        *
    from calculate_date_range
    ),

    unnest_all_months as (
    SELECT
        create_a_sequence_array.*,
        min_day,
        max_day,
        n AS month_offset  -- Position in the array
    FROM create_a_sequence_array
    LATERAL VIEW posexplode(month_sequence) AS n, month_offset
    ),

    organize_data as (
        select
        --- if it is a non existing row, it will be used the filler row
        add_months(min_day, month_offset) as trunc_day,

        --- clean some rows
        * except (
            trunc_day,
            month_sequence, min_day, max_day, month_offset,
            sample_size,gmv,item_subtotal_observed, item_quantity_observed
            ),

        --- if it is a filler row, than metrics are 0 (not null)
        case when trunc_day = add_months(min_day, month_offset) then sample_size else 0 end as sample_size,
        case when trunc_day = add_months(min_day, month_offset) then gmv else 0 end as gmv,
        case when trunc_day = add_months(min_day, month_offset) then item_subtotal_observed else 0 end as item_subtotal_observed,
        case when trunc_day = add_months(min_day, month_offset) then item_quantity_observed else 0 end as item_quantity_observed
        from unnest_all_months
    ),

    deduplicate as (
        select
            trunc_day,
            channel,
            parent_brand,
            brand,
            sub_brand,
            merchant,
            major_cat,
            sub_cat,
            minor_cat,
            special_attribute,

            sum(gmv) as gmv,
            sum(sample_size) as sample_size,
            sum(item_subtotal_observed) as item_subtotal_observed,
            sum(item_quantity_observed) as item_quantity_observed
        from organize_data
        group by all
    )

    select * from deduplicate
"""

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

# MAGIC %run "/Workspace/Users/pfisch@yipitdata.com/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/analysis/market_share"

# COMMAND ----------

#run_export_market_share_module(
#        sandbox_schema,
#        prod_schema,
#        demo_name,
#        special_attribute_column_original
#    )

# COMMAND ----------

original_table = spark.sql(f"""
    SELECT * FROM {prod_schema}.{demo_name}_display_interval_monthly
""")

original_table.display()

# COMMAND ----------

# MAGIC %run "/Workspace/Users/pfisch@yipitdata.com/corporate_transformation_blueprints/corporate_transformation_blueprints/scratch_pfisch/freeport/testing/compare_df_function"

# COMMAND ----------

original = original_table
freeport_df = df
compare_df(original, freeport_df, show_sample_differences=True, verbose=True)

# COMMAND ----------



# COMMAND ----------

# Create display interval deliverable
#display_interval_template = get_or_create_query_template(
#    slug="pfisch__test_display_interval_mapping",
#    query_string=mapping_display_interval_query,
#    template_description="Display interval mapping for market share analysis",
#    version_description="Static mapping table for display intervals",
#)

fp_catalog = "yd_fp_corporate_staging"
ss_catalog = "yd_sensitive_corporate"

#display_deliverable = get_or_create_deliverable(
#    f"pfisch__test_display_interval_mapping__{demo_name}",
#    query_template=display_interval_template["id"],
#    input_tables=[],  # No input tables needed for static data
#    output_table=f"{fp_catalog}.{prod_schema}.{demo_name}_display_interval_monthly",
#    description="Display interval mapping table for market share analysis",
#    product_org="corporate",
#    allow_major_version=True,
#    allow_minor_version=True,
#    staged_retention_days=100
#)

# Materialize display interval table
#materialize_deliverable(
#    display_deliverable["id"],
#    release_on_success=False,
#    wait_for_completion=True,
#)

# Process each special attribute column + major_cat
special_attribute_column = special_attribute_column_original.copy()
special_attribute_column.append("major_cat")

results = []

#for column in special_attribute_column:

column = "major_cat"

sources = [
    {
        'database_name': sandbox_schema,
        'table_name': demo_name+"_filter_items",
        'catalog_name': "yd_sensitive_corporate",
    }
]

# Create the query string with column parameter


# Determine module and table names
module_name = f"pfisch__test_market_share_for_column_{column}"
table_name = f"{demo_name}_market_share_for_column_{column}"

if column == "major_cat":
    table_name = f"{demo_name}_market_share_for_column_null"
    module_name = "pfisch__test_market_share_for_column_null"

# Create query template
#query_template = get_or_create_query_template(
#    slug=module_name,
#    query_string=query_string,
#    template_description=f"Market share analysis for column {column}",
#    version_description=f"Market share calculation with time series expansion for {column}",
#)

# Create deliverable
#deliverable = get_or_create_deliverable(
#    f"{module_name}__{demo_name}",
#    query_template=query_template["id"],
#    input_tables=[f"{ss_catalog}.{sandbox_schema}.{demo_name}_filter_items"],
#    output_table=f"{fp_catalog}.{prod_schema}.{table_name}",
#    description=f"Market share analysis for {column} with time series expansion and gap filling",
#    product_org="corporate",
#    allow_major_version=True,
#    allow_minor_version=True,
#    staged_retention_days=100
#)

# Materialize the deliverable
#materialization = materialize_deliverable(
#    deliverable["id"],
#    release_on_success=False,
#    wait_for_completion=True,
#)

#results.append({
#    'column': column,
#    'table_name': table_name,
#    'materialization': materialization
#})

# COMMAND ----------


