# Databricks notebook source
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
from pyspark.sql import functions as F
from pyspark.sql.window import Window


df = read_gsheet(
        "1F8bvKrBdlY0GIm9xAU5v3IM7ERLM6kUdi_wYph9JJds",
        1374540499
)
df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "M/d/yyyy H:mm:ss"))
distinct_values = df.select(df.columns[1]).distinct().toPandas().values.flatten()

len_clients = len(distinct_values)

# use your existing df with parsed timestamp
client_col = df.columns[1]  # or just put the actual name, e.g. "client"

w = Window.partitionBy(client_col).orderBy(F.col("timestamp").desc())

result_df = (
    df
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

from pyspark.sql import functions as F

result_df = result_df.withColumn(
    "is_prospect",
    (F.col("prod_schemaegydx_demoname_analysts_gold") == "ydx_prospect_analysts_gold")
)

result_df.display()

filtered_df = result_df.filter(F.col("is_prospect") == False)
filtered_df.display()

# COMMAND ----------

len_clients = result_df.count()

should_have_tariffs = []
client_name_list = []
df_result = []

#for client_name in distinct_values:
for i in range(0,len_clients):
#for i in range(0,5):
    client_name = result_df.collect()[i][1]
    demo_name = client_name
    client_row_data = result_df.collect()[i]
    prod_schema = client_row_data[4]
    demo_name = demo_name+ "_v38"
    source_table = client_row_data[5]
    client_name_list.append(client_name)
    try:
        table = spark.sql(f"""
        select count(*) as len_df
        from
        {source_table}
        """)
        table_is_empty = table.collect()[0]['len_df']
    except:
        table_is_empty = 0
    print("{} ===> {} =>> {}".format(client_name, prod_schema, table_is_empty))
    df_result.append(table_is_empty)

# Zip them row by row
data = list(zip(client_name_list, df_result))

# Create DF with schema
df_info = spark.createDataFrame(data, ["client_name", "how_many_rows"])
df_info.display()

# COMMAND ----------

from pyspark.sql import functions as F

# Get the actual column name for position 1
client_col = result_df.columns[1]

# Join df_info with result_df on the client key
merged_df = result_df.join(
    df_info,
    result_df[client_col] == df_info["client_name"],
    how="left"   # or "inner" if you want only matching rows
)

# Optional: drop duplicate join key from df_info
merged_df = merged_df.drop(df_info["client_name"])
merged_df.display()

create_table('data_solutions_sandbox', 'corporate_clients_info', merged_df, overwrite=True)
