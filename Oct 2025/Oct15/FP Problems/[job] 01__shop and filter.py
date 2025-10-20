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

from pyspark.sql.functions import col
import ast
from datetime import datetime
from yipit_databricks_utils.helpers.gsheets import read_gsheet
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit
from pyspark.sql.functions import lower
import pandas as pd

from datetime import datetime, timedelta
import calendar

import pandas as pd
from pyspark.sql import SparkSession

from pyspark.sql.functions import to_timestamp

from yipit_databricks_utils.future import create_table

# COMMAND ----------

issue_list = ["savant", "compana", "bosch",  "cecred", "wayfair", "summerfridays", "glossier", "amorepacific", "werner", "marmon",  "athome", "masco", "lutron", "daye", "elanco", "vegamour","husqvarna"]

# COMMAND ----------

def lower_case_column(demo_name):

    tab = spark.table(f"ydx_{demo_name}_analysts_silver.{demo_name}_v38_filter_items")
    listt = tab.columns

    not_lower = [item for item in listt if item != item.lower()]
    
    return not_lower

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

demo_name = "lutron"

# COMMAND ----------

def fix_table(demo_name):
    df = read_gsheet("1F8bvKrBdlY0GIm9xAU5v3IM7ERLM6kUdi_wYph9JJds",1374540499)

    client_row = df.filter(df[1] == demo_name) \
                .orderBy(col("timestamp").desc()) \
                .limit(1)
    client_row_data = client_row.collect()[0]


    sandbox_schema = client_row_data[3]
    prod_schema = client_row_data[4]

    print(sandbox_schema)
    print(prod_schema)

    tab = lower_shopper_filter_items(sandbox_schema, prod_schema, demo_name)

    #create_table(prod_schema, f"{demo_name}_v38_filter_items", tab, overwrite=True)

# COMMAND ----------

for i in issue_list:
    fix_table(i)

# COMMAND ----------

module_type = "shopper_filter_items"
client = demo_name + "_v38"

# freeport_module(sandbox_schema, prod_schema, client, module_type)
