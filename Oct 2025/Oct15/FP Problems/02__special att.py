# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC investigate issues in
# MAGIC
# MAGIC I believe there is a lot of issues in the special att
# MAGIC
# MAGIC now let's see if the original table exists
# MAGIC

# COMMAND ----------

issue_list = ["compana", "bosch",  "cecred",  "summerfridays", "glossier", "amorepacific",  "athome"]

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

demo_name = "summerfridays"

# COMMAND ----------

internal_sandbox = "ydx_internal_analysts_sandbox"
internal_gold = "ydx_internal_analysts_gold"

df = read_gsheet(
        "1F8bvKrBdlY0GIm9xAU5v3IM7ERLM6kUdi_wYph9JJds",
        1374540499
    )

client_row = df.filter(df[1] == demo_name) \
            .orderBy(col("timestamp").desc()) \
            .limit(1)
client_row_data = client_row.collect()[0]


external_sandbox= client_row_data[3]
external_gold = client_row_data[4]

if pd.isna(client_row_data[16]) is True:
    special_attribute_column = []
    special_attribute_display = []
else:
    special_attribute_column = client_row_data[16]
    special_attribute_display = client_row_data[16]

print(special_attribute_column)

# COMMAND ----------

def print_special_att(demo_name):
    internal_sandbox = "ydx_internal_analysts_sandbox"
    internal_gold = "ydx_internal_analysts_gold"

    df = read_gsheet("1F8bvKrBdlY0GIm9xAU5v3IM7ERLM6kUdi_wYph9JJds",1374540499)
    df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "M/d/yyyy H:mm:ss"))

    client_row = df.filter(df[1] == demo_name).orderBy(col("timestamp").desc()).limit(1)
    client_row_data = client_row.collect()[0]

    external_sandbox= client_row_data[3]
    external_gold = client_row_data[4]

    special_attribute_column = client_row_data[16]
    special_attribute_display = client_row_data[16]

    return special_attribute_column

# COMMAND ----------

def is_freeported(sandbox_schema, prod_schema, demo_name):
    table_name = f"yd_fp_corporate_staging.{prod_schema}.{demo_name}_geographic_analysis_fp"
    return spark.catalog.tableExists(table_name)

# COMMAND ----------

demo_name = "bosch"
spatt_list = print_special_att(demo_name)
print(spatt_list[0])

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from
# MAGIC yd_bosch_analysts_gold.bosch_v38_market_share_for_column_cross_door_nrf_calendar
# MAGIC -- '_market_share_for_column_'+column+'_nrf_calendar'

# COMMAND ----------

# "tti_open", "champion"
for demo in issue_list:
    special_attribute = print_special_att(demo)
    print("{} ==> {}".format(demo, special_attribute))

# COMMAND ----------

"bosch", "cecred", "summerfridays", "glossier", "amorepacific"

==> just run it again

# COMMAND ----------


