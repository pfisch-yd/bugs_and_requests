# Databricks notebook source
# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/blueprints__other branches/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/freeport/freeport_all_tables"

# COMMAND ----------

sol_owner = client_row_data[18]
    print(sol_owner)

    # READY FOR OFFICIAL ROLL OUT!!!
    sol_owner = "pfisch+solwoner@yipitdata.com"

    freeport_all_tables(demo_name, sandbox_schema, prod_schema, sol_owner, special_attribute_column, market_share, shopper_insights, pro_module, pricing_n_promo)
