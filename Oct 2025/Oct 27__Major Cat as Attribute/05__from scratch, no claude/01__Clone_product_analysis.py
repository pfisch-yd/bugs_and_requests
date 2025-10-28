# Databricks notebook source
# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/core/setup"

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from yipit_databricks_utils.future import create_table

# COMMAND ----------

def get_client_config(demo_name):
    """
    Retrieve client configuration from data_solutions.client_info table
    Returns a dictionary with sandbox_schema and other relevant config
    """
    try:
        # Try to query the client_info table
        client_info_df = spark.sql(f"""
            SELECT *
            FROM data_solutions_sandbox.corporate_clients_info
            WHERE demo_name = '{demo_name}'
            OR LOWER(demo_name) = LOWER('{demo_name}')
        """)

        # Convert to dictionary
        config = client_info_df.first().asDict()
        print(f"✓ Configuration found for {demo_name}")
        print(f"  Sandbox Schema: {config.get('sandbox_schema', 'N/A')}")

        return config

    except Exception as e:
        print(f"❌ Error retrieving config for {demo_name}: {str(e)}")
        return None

# COMMAND ----------

# DBTITLE 1,Helper Functions to Check Column Existence
def has_product_attributes_column(sandbox_schema, demo_name):
    """
    Check if the product_attributes column exists in the filter_items table
    """
    try:
        columns = spark.table(f"{sandbox_schema}.{demo_name}_filter_items").columns
        return 'product_attributes' in columns
    except Exception:
        return False

def get_optional_columns_availability(sandbox_schema, demo_name):
    """
    Check which optional columns exist in the filter_items table
    """
    try:
        columns = spark.table(f"{sandbox_schema}.{demo_name}_filter_items").columns
        optional_columns = {
            'product_attributes': 'product_attributes' in columns,
            'product_hash': 'product_hash' in columns,
            'parent_sku': 'parent_sku' in columns,
            'parent_sku_name': 'parent_sku_name' in columns,
            'variation_sku': 'variation_sku' in columns,
            'variation_sku_name': 'variation_sku_name' in columns,
            'parent_brand': 'parent_brand' in columns,
            'product_url': 'product_url' in columns
        }
        return optional_columns
    except Exception:
        return {col: False for col in ['product_attributes', 'product_hash', 'parent_sku', 'parent_sku_name', 'variation_sku', 'variation_sku_name', 'parent_brand', 'product_url']}

# COMMAND ----------

# DBTITLE 1,Product Analysis - Product Time Series
def export_sku_time_series(
        sandbox_schema,
        prod_schema,
        demo_name,
        product_id,
        special_attribute_column_original
        ):

    module_name = '_sku_time_series'

    # Check which optional columns exist
    optional_cols = get_optional_columns_availability(sandbox_schema, demo_name)
    has_product_attrs = optional_cols['product_attributes']

    # Build product attributes selection logic
    # This will include both existing product_attributes (if any) and special_attribute_column_original columns

    if special_attribute_column_original:
        # Only special attribute columns - create named_struct from them
        special_cols_struct = ', '.join([f"'{col}', min({col})" for col in special_attribute_column_original])
        product_attrs_select = f"to_json(named_struct({special_cols_struct})) as product_attributes_json,"
    # elif has_product_attrs:
    #     # Only existing product_attributes (variant)
    #     product_attrs_select = "to_json(first(product_attributes)) as product_attributes_json,"
    else:
        # No product attributes at all
        product_attrs_select = "CAST(null as STRING) as product_attributes_json,"

    product_hash_select = "min(product_hash) as product_hash," if optional_cols['product_hash'] else "CAST(null as STRING) as product_hash,"
    parent_sku_select = "min(parent_sku) as parent_sku," if optional_cols['parent_sku'] else "CAST(null as STRING) as parent_sku,"
    parent_sku_name_select = "min(parent_sku_name) as parent_sku_name," if optional_cols['parent_sku_name'] else "CAST(null as STRING) as parent_sku_name,"
    variation_sku_select = "min(variation_sku) as variation_sku," if optional_cols['variation_sku'] else "CAST(null as STRING) as variation_sku,"
    variation_sku_name_select = "min(variation_sku_name) as variation_sku_name," if optional_cols['variation_sku_name'] else "CAST(null as STRING) as variation_sku_name,"
    parent_brand_select = "min(parent_brand) as parent_brand," if optional_cols['parent_brand'] else "CAST(null as STRING) as parent_brand,"
    product_url_select = "min(product_url) as product_url," if optional_cols['product_url'] else "CAST(null as STRING) as product_url,"
    
    
    sku_time_series = spark.sql(f"""
        -- SET use_cached_result = false;
        with 
        max_month as (
            SELECT month(max(month)) as max_month, MAX(month) as max_date
            FROM {sandbox_schema}.{demo_name}_filter_items
        ),
        
        base as (
        SELECT
            "{product_id}" as product_id_type,
            'Monthly Time Series' as display_interval,
            channel,
            major_cat,
            sub_cat,
            minor_cat,
            web_description,
            merchant_clean as merchant,
            brand,
            sub_brand,
            {product_id} as product_id,
            {product_attrs_select}
            {product_hash_select}
            {parent_sku_select}
            {parent_sku_name_select}
            {variation_sku_select}
            {variation_sku_name_select}
            {parent_brand_select}
            {product_url_select}
            month as period_month,
            month as period_starting,
            max_date,
            SUM(item_price * item_quantity) as total_spend,
            SUM(item_quantity) as total_units,
            SUM(gmv) as gmv,
            count(*) as observations,
            min(order_date) as first_observation_in_month,
            max(order_date) as last_observation_in_month
        FROM {sandbox_schema}.{demo_name}_filter_items
        CROSS JOIN max_month
        WHERE web_description is not null
        GROUP BY ALL
        ),
            
        with_lag_and_new_sku_flag as (
        SELECT 
            *,
            lag(observations) OVER (
                partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id 
                order by period_starting
            ) as last_obs,
            lag(gmv) OVER (
                partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id 
                order by period_starting
            ) as last_gmv,
            CASE WHEN row_number() OVER (
                partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id 
                order by period_starting
            ) = 1 then 1 else 0 end as new_sku_flag
        FROM base
        )
        
        SELECT 
            *except(product_attributes_json), parse_json(product_attributes_json) as product_attributes
        FROM with_lag_and_new_sku_flag
        ORDER BY channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id, period_starting
    """)

    create_table_with_variant_support(module_name,prod_schema, demo_name+'_sku_time_series', sku_time_series, overwrite=True, variant_columns=['product_attributes'])
    
    return spark.table(f'{prod_schema}.{demo_name}_sku_time_series')

# COMMAND ----------

root = "weber"

config = get_client_config(root)

# COMMAND ----------

sandbox_schema = config['sandbox_schema']
prod_schema = config['prod_schema']
demo_name = config['demo_name'] + "_v38"
product_id = "sku" # config['product_id']
special_attribute_column = config['special_attribute_column']

special_attribute_column_original = special_attribute_column

print(
        sandbox_schema,
        prod_schema,
        demo_name,
        product_id,
        special_attribute_column_original
        )

# COMMAND ----------

module_name = '_sku_time_series'

optional_cols = get_optional_columns_availability(sandbox_schema, demo_name)
has_product_attrs = optional_cols['product_attributes']
print(optional_cols)

if special_attribute_column_original:
    print(special_attribute_column_original)
    special_cols_struct = ', '.join([f"'{col}', min({col})" for col in special_attribute_column_original])
    product_attrs_select = f"to_json(named_struct({special_cols_struct})) as product_attributes_json,"
    print(product_attrs_select)
else:
    product_attrs_select = "CAST(null as STRING) as product_attributes_json,"
    print("else")

# COMMAND ----------

optional_cols = get_optional_columns_availability(sandbox_schema, demo_name)
print(optional_cols)
has_product_attrs = optional_cols['product_attributes']

product_attrs_select = "first(to_json(product_attributes)) as product_attributes," if optional_cols['product_attributes'] else "CAST(null as STRING) as product_attributes,"
print(product_attrs_select)

# COMMAND ----------

product_hash_select = "min(product_hash) as product_hash," if optional_cols['product_hash'] else "CAST(null as STRING) as product_hash,"
parent_sku_select = "min(parent_sku) as parent_sku," if optional_cols['parent_sku'] else "CAST(null as STRING) as parent_sku,"
parent_sku_name_select = "min(parent_sku_name) as parent_sku_name," if optional_cols['parent_sku_name'] else "CAST(null as STRING) as parent_sku_name,"
variation_sku_select = "min(variation_sku) as variation_sku," if optional_cols['variation_sku'] else "CAST(null as STRING) as variation_sku,"
variation_sku_name_select = "min(variation_sku_name) as variation_sku_name," if optional_cols['variation_sku_name'] else "CAST(null as STRING) as variation_sku_name,"
parent_brand_select = "min(parent_brand) as parent_brand," if optional_cols['parent_brand'] else "CAST(null as STRING) as parent_brand,"
product_url_select = "min(product_url) as product_url," if optional_cols['product_url'] else "CAST(null as STRING) as product_url,"

# COMMAND ----------

product_attrs_select

# COMMAND ----------


sku_time_series = spark.sql(f"""
    -- SET use_cached_result = false;
    with 
    max_month as (
        SELECT month(max(month)) as max_month, MAX(month) as max_date
        FROM {sandbox_schema}.{demo_name}_filter_items
    ),
    
    base as (
    SELECT
        "{product_id}" as product_id_type,
        'Monthly Time Series' as display_interval,
        channel,
        major_cat,
        sub_cat,
        minor_cat,
        web_description,
        merchant_clean as merchant,
        brand,
        sub_brand,
        {product_id} as product_id,
        {product_attrs_select}
        {product_hash_select}
        {parent_sku_select}
        {parent_sku_name_select}
        {variation_sku_select}
        {variation_sku_name_select}
        {parent_brand_select}
        {product_url_select}
        month as period_month,
        month as period_starting,
        max_date,
        SUM(item_price * item_quantity) as total_spend,
        SUM(item_quantity) as total_units,
        SUM(gmv) as gmv,
        count(*) as observations,
        min(order_date) as first_observation_in_month,
        max(order_date) as last_observation_in_month
    FROM {sandbox_schema}.{demo_name}_filter_items
    CROSS JOIN max_month
    WHERE web_description is not null
    GROUP BY ALL
    ),
        
    with_lag_and_new_sku_flag as (
    SELECT 
        *,
        lag(observations) OVER (
            partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id 
            order by period_starting
        ) as last_obs,
        lag(gmv) OVER (
            partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id 
            order by period_starting
        ) as last_gmv,
        CASE WHEN row_number() OVER (
            partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id 
            order by period_starting
        ) = 1 then 1 else 0 end as new_sku_flag
    FROM base
    )
    
    SELECT 
        --- *except(product_attributes_json),
        product_attributes_json as old,
        parse_json(product_attributes_json) as product_attributes
    FROM with_lag_and_new_sku_flag
    ORDER BY channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id, period_starting
""")

sku_time_series.display()

# COMMAND ----------

create_table_with_variant_support(module_name,prod_schema, demo_name+'_sku_time_series', sku_time_series, overwrite=True, variant_columns=['product_attributes'])

return spark.table(f'{prod_schema}.{demo_name}_sku_time_series')
