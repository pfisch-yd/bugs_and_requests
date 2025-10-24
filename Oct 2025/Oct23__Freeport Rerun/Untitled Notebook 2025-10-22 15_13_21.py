# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM yd_dist_eng.distribution_gold.dispatch_fullfilments

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM yd_dist_eng.distribution_gold.dispatch_publications
# MAGIC where slug like "corporate_%"
