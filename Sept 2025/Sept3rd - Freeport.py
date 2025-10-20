# Databricks notebook source
import sys

sys.path.append("/Workspace/Repos/ETL_Production/freeport_service/")

from freeport_databricks.client.v1 import (
    get_or_create_deliverable,
    materialize_deliverable,
)


# COMMAND ----------

deliverable = get_or_create_deliverable(
    "Testee Lauder",
    input_tables="yd_sensitive_corporate.ydx_internal_analysts_sandbox.testesteelauder_sourcetable",
    output_table="yd_fp_corporate_staging.ydx_internal_analysts_sandbox.testesteelauder_sourcetable",
    description="First test deliverable to validate Freeport and Dispatch system setup.",
    product_org="corporate",
    allow_major_version=False
)

# COMMAND ----------

materialization = materialize_deliverable(
    deliverable["id"],
    release_on_success=False, # Set to false if you don't want to immediately publish
    wait_for_completion=False,
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from yd_fp_corporate_staging.ydx_internal_analysts_sandbox.testesteelauder_sourcetable
# MAGIC limit 10

# COMMAND ----------

df = spark.range(100).withColumn("publication_timestamp", F.current_timestamp())
df.display()

# COMMAND ----------

prod_schema = "ydx_internal_analysts_gold"
table_name = "dispatch_table_function_test"

create_table(
    prod_schema,
    "dispatch_table_function_test",
    df,
    overwrite=True,
    bucket_name="yipit-engineering",
)

# COMMAND ----------

# "distribution_engineering"
dispatch_table("distribution_engineering", "dispatch_table_function_test")

# COMMAND ----------

# 2.1 Definir um FPModelVersion

# COMMAND ----------

# MAGIC %sql
# MAGIC describe yd_sensitive_corporate.ydx_internal_analysts_sandbox.testesteelauder_sourcetable

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

sku_detail = spark.sql(f"""
    SELECT *
    except (product_attributes)
    FROM yd_sensitive_corporate.ydx_internal_analysts_sandbox.testesteelauder_sourcetable
""")

create_table("ydx_internal_analysts_sandbox", 'testesteelauder_sourcetable', sku_detail, overwrite=True)
