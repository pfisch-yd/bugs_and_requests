# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC If there is something wrong,
# MAGIC please right a new form here
# MAGIC
# MAGIC https://docs.google.com/forms/d/e/1FAIpQLSetveoNR1i_9nWWebCOYbSdWYxqZkacJsCMhgt5-r0XuiTOWw/viewform?usp=dialog 
# MAGIC
# MAGIC demo_name: greatstar_v38
# MAGIC dash_display_title: GreatStar Tools
# MAGIC sandbox_schema: ydx_greatstar_analysts_silver
# MAGIC prod_schema: ydx_greatstar_analysts_gold
# MAGIC source_table: ydx_greatstar_analysts_gold.final_items_table
# MAGIC pro_source_table: ydx_retail_silver.edison_pro_items
# MAGIC sample_size_guardrail_threshold: 200.0
# MAGIC brands_display_list: ['hangzhou great star tools co., ltd.']
# MAGIC parent_brand: GreatStar
# MAGIC product_id: sku
# MAGIC client_email_distro: greatstar@yipitdata.com
# MAGIC start_date_of_data: 2021-01-01
# MAGIC category_cols: ['l1', 'l2', 'l3'],
# MAGIC special_attribute_column: [REDACTED]
# MAGIC
# MAGIC BOUGHT PACKAGES:
# MAGIC Market Share = True
# MAGIC Shopper Insights = True
# MAGIC Pro = True
# MAGIC Pricing n Promo = True
# MAGIC
# MAGIC TIMESTAMP = 2025-10-07 03:27:52
# MAGIC
# MAGIC =======
# MAGIC UC operation: Vacuuming "yd_sensitive_corporate.ydx_greatstar_analysts_gold.greatstar_v38_leakage_users"
# MAGIC With Dataframe:
# MAGIC Empty DataFrame
# MAGIC Columns: [key_retailer, key_category, age, merchant, brand, year, uct_3, uct_4, closer_flag, item_subtotal]
# MAGIC Index: [REDACTED]

# COMMAND ----------


sandbox_schema = 'ydx_greatstar_analysts_silver'
prod_schema = 'ydx_greatstar_analysts_gold'
demo_name = 'greatstar_v38'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ydx_greatstar_analysts_gold.final_items_table

# COMMAND ----------

# MAGIC %run "/Workspace/Users/pfisch@yipitdata.com/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/core/setup"

# COMMAND ----------

leakage_users = spark.sql(f"""
    -- SET use_cached_result = false;
                        
    SELECT
        *

    FROM {sandbox_schema}.{demo_name}_filter_items

""")

leakage_users.display()

# COMMAND ----------

leakage_users = spark.sql(f"""
    -- SET use_cached_result = false;
                        
    SELECT
        b.user_id,
        a.source,
        a.merchant_clean as merchant,
        first_active_quarter,
        brand, date_trunc('quarter', order_date) as quarter,
        major_cat,
        sub_cat,
        minor_cat,
        SUM(a.item_price * a.item_quantity) as item_subtotal,
        FIRST(c.total_users) as total_panelists,
        FIRST(d.total_users) as total_panelists_channel

    FROM {sandbox_schema}.{demo_name}_filter_items a
    LEFT JOIN ydx_retail_silver.retail_users b ON a.user_id = b.user_id AND a.merchant = b.merchant AND a.source = b.source
    LEFT JOIN ydx_retail_silver.total_panelists c ON a.merchant = c.merchant and date_trunc('quarter', a.order_date) = c.quarter
    LEFT JOIN ydx_retail_silver.channel_panelists d ON a.merchant = d.merchant and date_trunc('quarter', a.order_date) = d.quarter AND a.source = d.source
    
    WHERE b.user_id is not null
    AND (a.source = 'edison'
    OR (a.source = 'leia' AND leia_panel_flag = 1))
    
    GROUP BY 1,2,3,4,5,6,7,8,9
""")

leakage_users.display()

# COMMAND ----------

module_name = '_leakage_retailer'

df_all = pd.DataFrame(columns = ['key_retailer','key_category','age','merchant','brand','year','uct_3','uct_4','closer_flag', 'item_subtotal'])

retailers_df = spark.sql(f"""
    SELECT merchant, sum(item_subtotal) as item_sub
    FROM {prod_schema}.{demo_name}_leakage_users
    GROUP BY 1
    ORDER BY 2 DESC
    """)

retailers_df

# COMMAND ----------

retailers_df.display()

# COMMAND ----------

retailer_list = [i[0] for i in retailers_df.collect()]
retailer_list

# COMMAND ----------






for merchant in retailer_list:
    df = spark.sql(f"""
        -- SET use_cached_result = false;
                        
        with closers as (
            SELECT user_id, MIN(first_active_quarter) as first_q
            --- old : FROM {sandbox_schema}.{demo_name}_leakage_users
            FROM {prod_schema}.{demo_name}_leakage_users
            WHERE merchant = "{merchant}"
            GROUP BY 1
        )

        SELECT
            "{merchant}" as key_retailer,
            source,
            merchant,
            brand,
            quarter,
            major_cat,
            sub_cat,
            minor_cat,
            case when b.user_id is not null AND b.first_q < quarter then "Closer" else "Non-closer" end as closer_flag,
            sum(item_subtotal) as item_subtotal,
            FIRST(total_panelists) as total_panelists
        --- old : FROM {sandbox_schema}.{demo_name}_leakage_users
        FROM {prod_schema}.{demo_name}_leakage_users a
        LEFT JOIN closers b on a.user_id = b.user_id
        GROUP BY 1,2,3,4,5,6,7,8,9
    """)
        
    try:
        df_all = df_all.union(df)
    except:
        df_all = df




# COMMAND ----------

df_all
