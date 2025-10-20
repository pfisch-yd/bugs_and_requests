# Databricks notebook source
sandbox_schema = "ydx_osea_analysts_gold"
demo_name = "osea_v38"
product = "hydrating milky toner"
product_id = 'sku'

# COMMAND ----------

# DBTITLE 1,SKU Analysis
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

# Check which optional columns exist
optional_cols = get_optional_columns_availability(sandbox_schema, demo_name)
has_product_attrs = optional_cols['product_attributes']

# Conditionally include optional columns in queries
product_attrs_select = "to_json(product_attributes) as product_attributes," if optional_cols['product_attributes'] else "CAST(null as STRING) as product_attributes,"
product_hash_select = "min(product_hash) as product_hash," if optional_cols['product_hash'] else "CAST(null as STRING) as product_hash,"
parent_sku_select = "min(parent_sku) as parent_sku," if optional_cols['parent_sku'] else "CAST(null as STRING) as parent_sku,"
parent_sku_name_select = "min(parent_sku_name) as parent_sku_name," if optional_cols['parent_sku_name'] else "CAST(null as STRING) as parent_sku_name,"
variation_sku_select = "min(variation_sku) as variation_sku," if optional_cols['variation_sku'] else "CAST(null as STRING) as variation_sku,"
variation_sku_name_select = "min(variation_sku_name) as variation_sku_name," if optional_cols['variation_sku_name'] else "CAST(null as STRING) as variation_sku_name,"
parent_brand_select = "min(parent_brand) as parent_brand," if optional_cols['parent_brand'] else "CAST(null as STRING) as parent_brand,"
product_url_select = "min(product_url) as product_url," if optional_cols['product_url'] else "CAST(null as STRING) as product_url,"

sku_analysis = spark.sql(f"""
    with 
    max_month as (
        SELECT month(max(month)) as max_month, MAX(month) as max_date
        FROM {sandbox_schema}.{demo_name}_filter_items
    ),

    annual as (
        SELECT *
        FROM (
            SELECT 'Last Year' as display_interval, *, 
            lag(observations) OVER (partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id order by period_starting) as last_obs,
            lag(gmv) OVER (partition by 
            channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id
            order by period_starting) as last_gmv,
            CASE WHEN row_number() OVER (partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id order by period_starting) = 1 then 1 else 0 end as new_sku_flag
            FROM (
                SELECT channel, date_trunc('year', max_date) - interval 1 month as max_date, major_cat, sub_cat, minor_cat, web_description, merchant_clean as merchant, brand, sub_brand, year as period_starting, 
                {product_id} as product_id,
                {product_attrs_select}
                {product_hash_select}
                {parent_sku_select}
                {parent_sku_name_select}
                {variation_sku_select}
                {variation_sku_name_select}
                {parent_brand_select}
                {product_url_select}
                SUM(item_price * item_quantity) as total_spend,SUM(item_quantity) as total_units, SUM(gmv) as gmv, count(*) as observations
                FROM {sandbox_schema}.{demo_name}_filter_items
                CROSS JOIN max_month
                WHERE web_description is not null
                AND date_trunc('year', month) < date_trunc('year', max_date)
                GROUP BY ALL
            )
        )
        WHERE period_starting = date_trunc('year', max_date)
    ),

    ytd as (
        SELECT *
        FROM (
            SELECT 'Year-To-Date' as display_interval, *, 
            lag(observations) OVER (partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id order by period_starting) as last_obs,
            lag(gmv) OVER (partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id order by period_starting) as last_gmv,
            CASE WHEN row_number() OVER (partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id order by period_starting) = 1 then 1 else 0 end as new_sku_flag
            FROM (
                SELECT channel, max_date, major_cat, sub_cat, minor_cat, web_description, merchant_clean as merchant, brand, sub_brand, year as period_starting,
                {product_id} as product_id,
                {product_attrs_select}
                {product_hash_select}
                {parent_sku_select}
                {parent_sku_name_select}
                {variation_sku_select}
                {variation_sku_name_select}
                {parent_brand_select}
                {product_url_select}
                SUM(item_price * item_quantity) as total_spend, SUM(item_quantity) as total_units, SUM(gmv) as gmv, count(*) as observations
                FROM {sandbox_schema}.{demo_name}_filter_items
                CROSS JOIN max_month
                WHERE web_description is not null
                AND month(month) <= max_month
                GROUP BY ALL
            )
        )
        WHERE period_starting = date_trunc('year', max_date)

    ),

    quarterly as (
        SELECT *
        FROM (
            SELECT 'Last Quarter' as display_interval, * except (q_partition), 
            lag(observations) OVER (partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id, q_partition order by period_starting) as last_obs,
            lag(gmv) OVER (partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id, q_partition order by period_starting) as last_gmv,
            CASE WHEN row_number() OVER (partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id order by period_starting) = 1 then 1 else 0 end as new_sku_flag
            FROM (
                SELECT channel, date_trunc('quarter', max_date) as max_date, major_cat, sub_cat, minor_cat, web_description, merchant_clean as merchant, brand, sub_brand, date_trunc('quarter', month) as period_starting, quarter(month) as q_partition, 
                {product_id} as product_id,
                {product_attrs_select}
                {product_hash_select}
                {parent_sku_select}
                {parent_sku_name_select}
                {variation_sku_select}
                {variation_sku_name_select}
                {parent_brand_select}
                {product_url_select}
                SUM(item_price * item_quantity) as total_spend, SUM(item_quantity) as total_units, SUM(gmv) as gmv, count(*) as observations
                FROM {sandbox_schema}.{demo_name}_filter_items
                CROSS JOIN max_month
                WHERE web_description is not null
                AND date_trunc('quarter', month) < date_trunc('quarter', max_date)
                GROUP BY ALL
            )
        )
        WHERE period_starting = date_trunc('quarter', max_date)
    ),

    monthly as (
    SELECT *
    FROM (

        SELECT
        'Last Month' as display_interval,
        * except (q_partition), 
        lag(observations) OVER (partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id, q_partition order by period_starting) as last_obs,
        lag(gmv) OVER (partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id, q_partition order by period_starting) as last_gmv,
        CASE WHEN row_number() OVER (partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id order by period_starting) = 1 then 1 else 0 end as new_sku_flag
        FROM (

            SELECT
            channel,
            date_trunc('month', max_date) as max_date,
            major_cat,
            sub_cat,
            minor_cat,
            web_description,
            merchant_clean as merchant,
            brand,
            sub_brand,
            date_trunc('month', month) as period_starting,
            month(month) as q_partition, 
            {product_id} as product_id,
            {product_attrs_select}
            {product_hash_select}
            {parent_sku_select}
            {parent_sku_name_select}
            {variation_sku_select}
            {variation_sku_name_select}
            {parent_brand_select}
            {product_url_select}
            SUM(item_price * item_quantity) as total_spend,
            SUM(item_quantity) as total_units,
            SUM(gmv) as gmv,
            count(*) as observations
            FROM {sandbox_schema}.{demo_name}_filter_items
            CROSS JOIN max_month
            WHERE web_description is not null
            GROUP BY ALL

        )
    )
    WHERE period_starting = date_trunc('month', max_date)
    ),

    p12 as (
        SELECT * except(p12)
        FROM (
            SELECT 'Trailing 12 Months' as display_interval, *, 
            lag(observations) OVER (partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id order by p12 DESC) as last_obs,
            lag(gmv) OVER (partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id order by p12 DESC) as last_gmv,
            CASE WHEN row_number() OVER (partition by channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id order by p12 DESC) = 1 then 1 else 0 end as new_sku_flag
            FROM (
                SELECT channel, CASE WHEN month >= (date_trunc('month', max_date) - interval 11 month) THEN 'a' else 'b' end as p12, max_date, major_cat, sub_cat, minor_cat, web_description, merchant_clean as merchant, brand, sub_brand, (date_trunc('month', max_date) - interval 11 month) as period_starting,
                {product_id} as product_id,
                {product_attrs_select}
                {product_hash_select}
                {parent_sku_select}
                {parent_sku_name_select}
                {variation_sku_select}
                {variation_sku_name_select}
                {parent_brand_select}
                {product_url_select}
                SUM(item_price * item_quantity) as total_spend,
                SUM(item_quantity) as total_units,
                SUM(gmv) as gmv, count(*) as observations
                FROM {sandbox_schema}.{demo_name}_filter_items
                CROSS JOIN max_month
                WHERE web_description is not null
                AND month >= (date_trunc('month', max_date) - interval 23 month)
                GROUP BY ALL
            )
        )
        WHERE p12 = 'a'
    ),

    union_all as (
        SELECT *
        FROM annual
        UNION ALL
        SELECT *
        FROM ytd
        UNION ALL
        SELECT *
        FROM quarterly
        UNION ALL
        SELECT *
        FROM monthly
        UNION ALL
        SELECT *
        FROM p12
    ),

    source as (

        select
            "{product_id}" as product_id_type,
            {f'*except(product_attributes), parse_json(product_attributes) as product_attributes' if has_product_attrs else '*'}
        from
        union_all
        where display_interval = "Year-To-Date"
    ),

grouped as (
  select
    merchant,
    brand,
    web_description,
    product_id,
    min(major_cat) as major_category,
    min(sub_cat) as sub_category,
    min(minor_cat) as minor_category,
    sum(gmv) as total_gmv,
    sum(last_obs) as sum_last_obs,
    sum(last_gmv) as last_year_gmv,
    sum(total_units) as units,
    try_divide(sum(total_spend), sum(total_units)) as asp,
    Sum(Observations) as sample_size,
    min(product_hash) as product_hash
  from
  source
  group by merchant, brand, web_description, product_id
)

  select
      web_description, 
      total_gmv / asp as units,
      total_gmv,
      asp,
      sample_size,
      case
      when sample_size > 1000 then "Low Risk"
      when sample_size > 250  then "Some Risk"
    else "High Risk" end as ssr,
    case
      when sample_size > 1000 and sum_last_obs > 500 then try_divide(total_gmv, last_year_gmv) - 1
      else ""
    end as yoy_growth
  from
  grouped as add_columns
  WHERE
  lower(web_description) = '{product}'
""")

sku_analysis.display()

# COMMAND ----------

# Check which optional columns exist
optional_cols = get_optional_columns_availability(sandbox_schema, demo_name)
has_product_attrs = optional_cols['product_attributes']

# Conditionally include optional columns in queries
product_attrs_select = "to_json(product_attributes) as product_attributes_json," if optional_cols['product_attributes'] else "CAST(null as STRING) as product_attributes_json,"
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
    ),
    
    source as (
    SELECT 
        *except(product_attributes_json), parse_json(product_attributes_json) as product_attributes
    FROM with_lag_and_new_sku_flag
    ORDER BY channel, major_cat, sub_cat, minor_cat, web_description, merchant, brand, sub_brand, product_id, period_starting
    ),

grouped as (
  select
    merchant,
    brand,
    web_description,
    product_id,
    min(major_cat) as major_category,
    min(sub_cat) as sub_category,
    min(minor_cat) as minor_category,
    sum(gmv) as total_gmv,
    sum(last_obs) as sum_last_obs,
    sum(last_gmv) as last_year_gmv,
    sum(total_units) as units,
    try_divide(sum(total_spend), sum(total_units)) as asp,
    Sum(Observations) as sample_size,
    min(product_hash) as product_hash
  from
  source
  group by merchant, brand, web_description, product_id
)

  select
      web_description, 
      total_gmv / asp as units,
      total_gmv,
      asp,
      sample_size,
      case
      when sample_size > 1000 then "Low Risk"
      when sample_size > 250  then "Some Risk"
    else "High Risk" end as ssr,
    case
      when sample_size > 1000 and sum_last_obs > 500 then try_divide(total_gmv, last_year_gmv) - 1
      else ""
    end as yoy_growth
  from
  grouped as add_columns
  WHERE
  lower(web_description) = '{product}'


""")


sku_time_series.display()
