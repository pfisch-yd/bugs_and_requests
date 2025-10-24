# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC -- SET use_cached_result = false;
# MAGIC
# MAGIC         with cats as (
# MAGIC         SELECT merchant, web_description, parent_brand, brand, sub_brand, major_cat, sub_cat, minor_cat, merchant_clean
# MAGIC         FROM yd_sensitive_corporate.ydx_internal_analysts_sandbox.testgraco_v38_filter_items
# MAGIC         GROUP BY 1,2,3,4,5,6,7,8,9
# MAGIC         )
# MAGIC
# MAGIC         SELECT
# MAGIC             a.* except (brand, product_description),
# MAGIC             b.merchant_clean,
# MAGIC             b.parent_brand,
# MAGIC             b.brand,
# MAGIC             b.sub_brand,
# MAGIC             b.major_cat,
# MAGIC             b.sub_cat,
# MAGIC             b.minor_cat,
# MAGIC             product_description as web_description
# MAGIC         FROM yd_sensitive_corporate.ydx_retail_silver.edison_pro_items a
# MAGIC         LEFT JOIN cats b on a.merchant = b.merchant
# MAGIC             and a.product_description = b.web_description
# MAGIC             AND lower(a.brand) = lower(b.brand)
# MAGIC         WHERE b.major_cat is not null
# MAGIC         AND month >= '2023-01-01'
# MAGIC         AND month < date_trunc('month', date_add(current_date(), -13))

# COMMAND ----------

query = """
-- SET use_cached_result = false;

        with cats as (
        SELECT merchant, web_description, parent_brand, brand, sub_brand, major_cat, sub_cat, minor_cat, merchant_clean
        FROM yd_sensitive_corporate.ydx_internal_analysts_sandbox.testgraco_v38_filter_items
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
        FROM yd_sensitive_corporate.ydx_retail_silver.edison_pro_items a
        LEFT JOIN cats b on a.merchant = b.merchant
            and a.product_description = b.web_description
            AND lower(a.brand) = lower(b.brand)
        WHERE b.major_cat is not null
        AND month >= '2023-01-01'
        AND month < date_trunc('month', date_add(current_date(), -13))
"""

# COMMAND ----------

# MAGIC %run "/Workspace/Users/pfisch@yipitdata.com/corporate_transformation_blueprints/corporate_transformation_blueprints/scratch_pfisch/freeport/time_diff_pro_insights"

# COMMAND ----------

from freeport_databricks.helpers.transformations import parse_individual_queries

# COMMAND ----------

query_parts = parse_individual_queries(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC query_df = spark.sql(f"DESCRIBE QUERY {query_part}")
