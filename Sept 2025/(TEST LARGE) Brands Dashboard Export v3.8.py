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
# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/Brands Dashboard Templates/__centralized_udfs_for_client_dashboard_exports"

# COMMAND ----------

ask_for_help()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Inputs

# COMMAND ----------

# need some help remembering the name?
# try the function
search_demo_name("amazon")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Here we define the demo name

# COMMAND ----------

demo_name = "testgraco"

# COMMAND ----------

print(demo_name)

# COMMAND ----------

check_your_parameters(demo_name)

# COMMAND ----------

run_everything(demo_name)

# COMMAND ----------

run_dash_metric_save(demo_name)

# COMMAND ----------


