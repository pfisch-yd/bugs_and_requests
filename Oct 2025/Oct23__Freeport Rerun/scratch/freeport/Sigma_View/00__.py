# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC [jira : Create a latest_version view and test it on Sandbox Sigma](https://yipitdata5.atlassian.net/browse/DS-291)
# MAGIC
# MAGIC as discussed yesterday with Gar.
# MAGIC
# MAGIC Get a FP table with multiple version,
# MAGIC use the code to retrieve latest.
# MAGIC
# MAGIC Create a view.
# MAGIC
# MAGIC Display View on (SANDBOX!!) sigma workbook
# MAGIC
# MAGIC
# MAGIC
# MAGIC ==========================================
# MAGIC
# MAGIC [AI, ChatGPT]:
# MAGIC
# MAGIC How am I going to use ChatGPT/AI for this one.
# MAGIC Well, I don’t know the difference between view x table.
# MAGIC I am going to ask chat gpt to create that with a standard table.
# MAGIC I will take a look on that on sigma.
# MAGIC
# MAGIC After that,
# MAGIC I will apply Gars code on it.
# MAGIC And see if it helps.
# MAGIC
# MAGIC I am going to use AI as google for this task.
# MAGIC I will be dealing with 3 platforms.
# MAGIC 1 - Sigma
# MAGIC 2 - Databricks (on Corporate)
# MAGIC 3 - Databricks (on Freeport)
# MAGIC
# MAGIC I don’t want to do the set up to use Claude for this one.
# MAGIC
# MAGIC
# MAGIC ====================================
# MAGIC
# MAGIC 11. 02_change data type__testestee.py
# MAGIC
# MAGIC Arquivo Run Geo: after__geo2
# MAGIC
# MAGIC Test Client: testesteelauder
# MAGIC
# MAGIC Tabelas Referenciadas:
# MAGIC
# MAGIC yd_fp_corporate_staging.ydx_internal_analysts_gold.renin_v38_geographic_analysis5__dmv__001
# MAGIC
# MAGIC yd_fp_corporate_staging.ydx_internal_analysts_gold.testgraco_v38_geographic_analysis5__dmv__001
# MAGIC
# MAGIC yd_fp_corporate_staging.ydx_internal_analysts_gold.testgraco_v38_geographic_analysis5__dmv__002
