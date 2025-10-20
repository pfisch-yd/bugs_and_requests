# Databricks notebook source
# MAGIC %md
# MAGIC #V3.8 Corporate Dashboard Export Function

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC To update your client parameters, submit this ticket. Changes will be automatically reflected.
# MAGIC
# MAGIC  https://docs.google.com/forms/d/e/1FAIpQLSetveoNR1i_9nWWebCOYbSdWYxqZkacJsCMhgt5-r0XuiTOWw/viewform
# MAGIC
# MAGIC
# MAGIC To see all demos look here:
# MAGIC
# MAGIC https://docs.google.com/spreadsheets/d/1yMjwt2RAvaedrC_KSHwE9af1UsPioJv_siIdGlFWnnA/edit?usp=sharing

# COMMAND ----------

# MAGIC %run setup_serverless

# COMMAND ----------

# DBTITLE 1,Run this first!
# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/blueprints__other branches/corporate_transformation_blueprints/corporate_transformation_blueprints/__centralized_udfs_for_client_dashboard_exports_PARALLEL"

# COMMAND ----------

confirm_version()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Inputs

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Here we define the demo name

# COMMAND ----------

demo_name = dbutils.widgets.get("client_name")
demo_name

# COMMAND ----------

print(demo_name)

# COMMAND ----------

run_everything_parallel(demo_name, mode="INTERNAL")

# COMMAND ----------



# COMMAND ----------


