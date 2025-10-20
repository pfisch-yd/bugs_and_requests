# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select * from data_solutions_sandbox.corporate_clients_info

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

df = read_gsheet(
        "1F8bvKrBdlY0GIm9xAU5v3IM7ERLM6kUdi_wYph9JJds",
        1374540499
)
df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "M/d/yyyy H:mm:ss"))
distinct_values = df.select(df.columns[1]).distinct().toPandas().values.flatten()

len_clients = len(distinct_values)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

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

filtered_df = result_df.filter(F.col("is_prospect") == False)
filtered_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Actions
# MAGIC
# MAGIC -- Count Rows.

# COMMAND ----------

df_result = filtered_df

# Zip them row by row
data = list(zip(client_name_list, df_result))

# Create DF with schema
df_info = spark.createDataFrame(data, ["client_name", "how_many_rows"])
df_info.display()

# COMMAND ----------

client_info = spark.sql(f"""
select * from data_solutions_sandbox.corporate_clients_info
""")

client_info.columns

# COMMAND ----------

filtered_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # ETL PLAN
# MAGIC
# MAGIC - extract gsheet
# MAGIC - rename
# MAGIC - deduplicate
# MAGIC - enrich with client_info data

# COMMAND ----------

# DBTITLE 1,rename columns
df = read_gsheet(
    "1F8bvKrBdlY0GIm9xAU5v3IM7ERLM6kUdi_wYph9JJds",
    1374540499
)

df1 = df.withColumn("timestamp", to_timestamp(col("timestamp"), "M/d/yyyy H:mm:ss"))

df1.createOrReplaceTempView("my_table")

renaming = f"""
select
  demo_namelowercasenospacenoversionegtriplecrowncentral as demo_name,
  dash_display_titlecapitalizedfirstletterreaderfriendlyeg_triple_crown as dash_display_title,
  sandbox_schemaegydx_demoname_analysts_silver as sandbox_schema,
  prod_schemaegydx_demoname_analysts_gold as prod_schema,
  source_tableegydx_triplecrown_analysts_silveritems_table as source_table,
  pro_source_tableegydx_retail_silveredison_pro_items_optional as pro_source_table,
  sample_size_guardrail_thresholdaint64eg200 as sample_size_guardrail_threshold,
  parent_brand_importan_tusedtodefine_cover_pageeg_triple_crown as parent_brand,
  client_email_distroegtriplecrownyipitdatacom as client_email_distro,
  start_date_of_dataeg2021_01_01 as start_date_of_data,
  category_cols_l2or_subsub_cat_prep as category_cols,
  special_attribute_columndefineallspecialattributescolumnexfinish_or_materialcolor as special_attribute_column,
  packages,
  youremailyipitdatacom,
  timestamp
from
my_table
"""
df2 = spark.sql(renaming)



# COMMAND ----------

# DBTITLE 1,deduplicate
df2 = spark.sql(f"""
    with
    renaming as ({renaming}),

    add_rn as (
    select
        *,
        row_number() over (partition by demo_name order by timestamp desc) as rn
    from
    renaming 
    )

    select * from add_rn
    where rn = 1
""")

# df2.display()

# COMMAND ----------

# DBTITLE 1,add is_prospect
df3 = df2.withColumn(
    "is_prospect",
    (F.col("prod_schema") == "ydx_prospect_analysts_gold")
)

# COMMAND ----------

# DBTITLE 1,add how many rows
old_info = spark.sql(f"""
    select * from data_solutions_sandbox.corporate_clients_info
""").createOrReplaceTempView("old_info")

df3.createOrReplaceTempView("new_info")

df4 = spark.sql(f"""
    select
        new_info.*,
        old_info.how_many_rows,
        old_info.frequency as frequency
    from new_info
    left join
    old_info
    using(demo_name)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC FIX THIS FREQUENCY TYPO

# COMMAND ----------

df4.display()

# COMMAND ----------

#data_solutions_sandbox.corporate_clients_info

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

create_table("data_solutions_sandbox", 'corporate_clients_info', df4, overwrite=True)

# COMMAND ----------

# DBTITLE 1,list add frequency
df = spark.sql(f"""
    select * from data_solutions_sandbox.corporate_clients_info
    where
    frequency is null
    and is_prospect is false
""")

df.display()

# COMMAND ----------

# DBTITLE 1,add is_active

df5 = spark.sql(f"""
select
* except (is_active),
case
-- is test client
when prod_schema in ("ydx_internal_analysts_gold") then false
-- has churned
when demo_name in ("traeger", "fiskars_crafts", "fake_client", "jh_demo","beautyexample", "elanco", "cabinetworks", "Renin", "schlage_v38", "sbm", "duraflame", "s", "athome", "good_earth", "homedepot_v38", "sherwin_williams_limited", "cuisinart_v38", "KIK", "greatstar", "keter_monthly", "rain_bird", "pitboss_monthly", "lasko", "kiko", "ove", "wd40") then false

-- issues in schema
when demo_name in ("ames", "pg_ventures", "onrunning", "anker_v38", "sacheu_v38") then false

-- is prospect
when is_prospect is true then false
else true
end
as is_active
from data_solutions_sandbox.corporate_clients_info
""")

create_table("data_solutions_sandbox", 'corporate_clients_info', df5, overwrite=True)

# COMMAND ----------

["traeger", "fiskars_crafts", "beautyexample", "elanco", "cabinetworks", "Renin", "schlage_v38", "sbm", "duraflame", "s", "athome", "good_earth", "homedepot_v38", "sherwin_williams_limited", "cuisinart_v38", "KIK", "greatstar", "keter_monthly", "rain_bird", "pitboss_monthly", "lasko", "kiko"]

# COMMAND ----------

traeger
fiskars_crafts
beautyexample
elanco
cabinetworks
Renin
schlage_v38
sbm
duraflame
s
athome
good_earth
homedepot_v38
sherwin_williams_limited
cuisinart_v38
KIK
greatstar
keter_monthly
rain_bird
pitboss_monthly
lasko
kiko
