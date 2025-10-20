# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC   demo_name, 
# MAGIC   check_shopper_i
# MAGIC from data_solutions_sandbox.corporate_clients_info__freeport
# MAGIC where
# MAGIC packages like "%Shopper Insights%"
# MAGIC and
# MAGIC check_shopper_i is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC   demo_name, 
# MAGIC   check_pnp
# MAGIC from data_solutions_sandbox.corporate_clients_info__freeport
# MAGIC where
# MAGIC packages like "%Pricing & Promo%"
# MAGIC and
# MAGIC check_pnp is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC   demo_name, 
# MAGIC   check_pro
# MAGIC from data_solutions_sandbox.corporate_clients_info__freeport
# MAGIC where
# MAGIC packages like "%Pro%"
# MAGIC and
# MAGIC check_pro is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from data_solutions_sandbox.corporate_clients_info__freeport
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC   demo_name, 
# MAGIC   check_shopper_i
# MAGIC from data_solutions_sandbox.corporate_clients_info__freeport
# MAGIC where
# MAGIC packages like "%Shopper Insights%"
# MAGIC and check_shopper_i < "2025-09-30"
# MAGIC --- and check_shopper_i is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC   demo_name, 
# MAGIC   check_pnp
# MAGIC from data_solutions_sandbox.corporate_clients_info__freeport
# MAGIC where
# MAGIC packages like "%Pricing & Promo%"
# MAGIC and check_pnp < "2025-09-01"
# MAGIC --- and  is null 
