# Databricks notebook source
# MAGIC %md
# MAGIC # Where is the issue
# MAGIC
# MAGIC somewhere in the Sku Time series

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

def get_client_config(demo_name):
    """
    Retrieve client configuration from data_solutions.client_info table
    Returns a dictionary with sandbox_schema and other relevant config
    """
    try:
        # Try to query the client_info table
        client_info_df = spark.sql(f"""
            SELECT *
            FROM data_solutions_sandbox.corporate_clients_info
            WHERE demo_name = '{demo_name}'
            OR LOWER(demo_name) = LOWER('{demo_name}')
        """)

        # Convert to dictionary
        config = client_info_df.first().asDict()
        print(f"✓ Configuration found for {demo_name}")
        print(f"  Sandbox Schema: {config.get('sandbox_schema', 'N/A')}")

        return config

    except Exception as e:
        print(f"❌ Error retrieving config for {demo_name}: {str(e)}")
        return None

# COMMAND ----------

root_name = "weber"
config = get_client_config(root_name)

# COMMAND ----------

sandbox_schema = config["sandbox_schema"]
demo_name = root_name + "_v38"

df = spark.sql(f"""
        SELECT product_attributes FROM {sandbox_schema}.{demo_name}_filter_items
        limit 15
""")

print(f"{sandbox_schema}.{demo_name}_filter_items")

df.display()

# a lot of nulls, but a few correct items

# COMMAND ----------

sandbox_schema = config["sandbox_schema"]
demo_name = root_name + "_v38"

df = spark.sql(f"""
        SELECT product_attributes FROM ydx_internal_analysts_sandbox.{demo_name}_filter_items
        limit 15
""")

print(f"ydx_internal_analysts_sandbox.{demo_name}_filter_items")

df.display()

# a lot of nulls, but a few correct items

# COMMAND ----------

sandbox_schema = config["sandbox_schema"]
demo_name = root_name + "_v38"

df = spark.sql(f"""
        SELECT product_attributes FROM ydx_internal_analysts_gold.{demo_name}_sku_time_series
        limit 15
""")

print(f"ydx_internal_analysts_gold.{demo_name}_sku_time_series")

df.display()

# everything is incorrect
