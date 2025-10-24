# Databricks notebook source
sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "testesteelauder" #"testgraco"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

# COMMAND ----------

table = spark.sql(f"""
    SELECT * FROM yd_fp_corporate_staging.{prod_schema}.{demo_name}_geographic_analysis__dmv__000
""")
table.display()

# COMMAND ----------

demo_name = "testgraco"
demo_name = demo_name + "_v38"

table = spark.sql(f"""
    SELECT * FROM yd_fp_corporate_staging.{prod_schema}.{demo_name}_pro_insights__2025_09_19__09_6__dmv__000
""")
table.display()

# COMMAND ----------

# yd_fp_corporate_staging.ydx_internal_analysts_gold.testesteelauder_v38_display_interval_monthly__dmv__000

# COMMAND ----------

demo_name = "testesteelauder"
demo_name = demo_name + "_v38"

table = spark.sql(f"""
    SELECT * FROM
    --- yd_fp_corporate_staging.{prod_schema}.{demo_name}_display_interval_monthly__dmv__000
    yd_fp_corporate_staging.ydx_internal_analysts_gold.testesteelauder_v38_display_interval_monthly__dmv__000
""")
table.display()
