# Databricks notebook source
# MAGIC %md
# MAGIC # Solved
# MAGIC
# MAGIC A lot of clients had issues w _silver schema

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC I think the issue is about casing in the column names
# MAGIC
# MAGIC I am going to:
# MAGIC
# MAGIC 1 - check col names in source table
# MAGIC 2 - I will see the casing in the name
# MAGIC 3 - 

# COMMAND ----------

issue_list = ["savant", "compana", "bosch",  "cecred", "wayfair", "summerfridays", "glossier", "amorepacific", "werner", "marmon",  "athome", "masco", "lutron", "daye", "elanco", "vegamour","husqvarna"]
# "huqsvarna",

# COMMAND ----------

# tab = spark.sql(f"""" select * from ydx_petsmart_analysts_silver.petsmart_v38_filter_items """)

demo_name = "petsmart"

def lower_case_column(demo_name):
    tab = spark.table(f"ydx_{demo_name}_analysts_silver.{demo_name}_v38_filter_items")
    listt = tab.columns
    # print(listt)
    not_lower = [item for item in listt if item != item.lower()]
    # print(not_lower)
    return not_lower

# COMMAND ----------

# "tti_open", "champion"
for demo in issue_list:
    not_lower_list = lower_case_column(demo)
    if len(not_lower_list) > 0:
        print(f"Found {len(not_lower_list)} columns that are not lower case in {demo} ==> {not_lower_list}")


# COMMAND ----------

issue_list = ["savant", "wayfair", "werner", "marmon", "masco", "lutron", "daye", "vegamour", "huqsvarna"]

# COMMAND ----------

issue_list = ["compana", "bosch",  "cecred",  "summerfridays", "glossier", "amorepacific",  "athome"]
# "huqsvarna",

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC I will have to clone the schema to internal
# MAGIC so I can do it again.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC How to fix it
# MAGIC
# MAGIC ==> just for Shopper Filter Items
# MAGIC get cased
# MAGIC ==> 
# MAGIC

# COMMAND ----------

issue_list = ["savant", "wayfair", "werner", "marmon", "masco", "lutron", "daye", "vegamour", "huqsvarna"]

# COMMAND ----------

sandbox_schema = "ydx_savant_analysts_silver"
prod_schema = "ydx_savant_analysts_gold"
demo_name = "savant"

tab = spark.sql(f""" select * from {prod_schema}.{demo_name}_v38_filter_items """)

# COMMAND ----------

def lower_shopper_filter_items(sandbox_schema, prod_schema, demo_name):

    change_list = lower_case_column(demo_name)

    exception = ", ".join(change_list)

    query_parts = []
    for og_column in change_list:
        new_column = og_column.lower()
        query_parts.append(f"{og_column} as {new_column}")

    query = f"""select
        * except ({exception}),
        {', '.join(query_parts)}
    from {prod_schema}.{demo_name}_v38_filter_items"""

    tab = spark.sql(query)
    return tab

# COMMAND ----------

from yipit_databricks_utils.future import create_table

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_silver"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "savant"

tab = lower_shopper_filter_items(sandbox_schema, prod_schema, demo_name)

create_table(prod_schema, f"{demo_name}_v38_filter_items", tab, overwrite=True)

# COMMAND ----------

# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/blueprints__other branches/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/freeport/module_template_function"

# COMMAND ----------

module_type = "shopper_filter_items"
sandbox_schema = "ydx_internal_analysts_silver"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "savant" + "_v38"

freeport_module(sandbox_schema, prod_schema, demo_name, module_type)
