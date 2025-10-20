# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select merchant_clean from ydx_prospect_analysts_sandbox.hair_serums_dk

# COMMAND ----------

# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/Brands Dashboard Templates/__centralized_udfs_for_client_dashboard_exports"

# COMMAND ----------

search_demo_name("hair_serum")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select merchant_clean from ydx_prospect_analysts_sandbox.hair_serums_dk
