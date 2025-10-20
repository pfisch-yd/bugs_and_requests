# Databricks notebook source
# MAGIC %sql
# MAGIC select
# MAGIC how_many_rows
# MAGIC from yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info
# MAGIC where
# MAGIC demo_namelowercasenospacenoversionegtriplecrowncentral = "testsolenis"
# MAGIC
# MAGIC -- 1733768

# COMMAND ----------

(((16*60 + 36)/1733768)*7413278)/60

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with clients as (
# MAGIC select
# MAGIC case
# MAGIC when how_many_rows >= 86221257 then "maxx"
# MAGIC when how_many_rows < 86221257 then "large"
# MAGIC when how_many_rows < 11734849 then "medium"
# MAGIC when how_many_rows < 2745380 then "small"
# MAGIC end as size,
# MAGIC *
# MAGIC from yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info)
# MAGIC
# MAGIC select
# MAGIC *
# MAGIC from clients
# MAGIC
# MAGIC where is_prospect is false
# MAGIC and
# MAGIC size = "maxx"
# MAGIC
# MAGIC order by how_many_rows desc
# MAGIC
# MAGIC --- homedepot
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with clients as (
# MAGIC select
# MAGIC case
# MAGIC -- when how_many_rows >= 80266288 then "maxx"
# MAGIC when how_many_rows >= 11915983 and how_many_rows < 80266288 then "large"
# MAGIC when how_many_rows >=  4373925 and how_many_rows < 11915983 then "medium"
# MAGIC when how_many_rows < 4373925 then "small"
# MAGIC end as size,
# MAGIC *
# MAGIC from yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info)
# MAGIC
# MAGIC select
# MAGIC *
# MAGIC from clients
# MAGIC
# MAGIC where is_prospect is false
# MAGIC and
# MAGIC size = "large"
# MAGIC
# MAGIC order by how_many_rows desc
# MAGIC
# MAGIC --- maxx: homedepot
# MAGIC --- large: graco
# MAGIC --- medium : testsolenis
# MAGIC --- small : testblueprints
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC select
# MAGIC   percentile_approx(how_many_rows, 0.25) as q1,
# MAGIC   percentile_approx(how_many_rows, 0.50) as median,
# MAGIC   percentile_approx(how_many_rows, 0.75) as q3
# MAGIC from yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info
# MAGIC where is_prospect is false
# MAGIC
# MAGIC /*
# MAGIC case
# MAGIC when how_many_rows >= 86221257 then "maxx"
# MAGIC when how_many_rows < 86221257 then "large"
# MAGIC when how_many_rows < 11734849 then "medium"
# MAGIC when how_many_rows < 2745380 then "small"
# MAGIC end as size,
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with clients as (
# MAGIC select
# MAGIC case
# MAGIC when how_many_rows >= 80266288 then "very large"
# MAGIC when how_many_rows >= 11915983 and how_many_rows < 80266288 then "large"
# MAGIC when how_many_rows >=  4373925 and how_many_rows < 11915983 then "medium"
# MAGIC when how_many_rows < 4373925 then "small"
# MAGIC end as size,
# MAGIC case
# MAGIC when how_many_rows >= 80266288 then 100
# MAGIC when how_many_rows >= 11915983 and how_many_rows < 80266288 then 45
# MAGIC when how_many_rows >=  4373925 and how_many_rows < 11915983 then 40
# MAGIC when how_many_rows < 4373925 then 25
# MAGIC end as running_time,
# MAGIC *
# MAGIC from yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info
# MAGIC
# MAGIC )
# MAGIC
# MAGIC select
# MAGIC *
# MAGIC from clients
# MAGIC
# MAGIC where is_prospect is false
# MAGIC
# MAGIC order by how_many_rows desc
# MAGIC
# MAGIC --- maxx: homedepot
# MAGIC --- large: graco
# MAGIC --- medium : testsolenis
# MAGIC --- small : testblueprints

# COMMAND ----------

7413278 / 11772448

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC demo_namelowercasenospacenoversionegtriplecrowncentral as demo
# MAGIC from yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info
# MAGIC
# MAGIC where
# MAGIC how_many_rows >= 86221257 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
