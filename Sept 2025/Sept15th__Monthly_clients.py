# Databricks notebook source
print(" UPDATED CLIENTS ")
updated_clients = spark.sql(f"""
select
  user,
  get_json_object(args, '$[0]') as args,
  end_timestamp,
  notebook_path
from yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
where name like "run_everything%"
and end_timestamp < "2025-08-16T20:38:26.203+00:00"
and end_timestamp > "2025-08-14T20:38:26.203+00:00"
order by user, args
""")

updated_clients.display()

# COMMAND ----------

# updated clients
updated_clients = df.select("args").distinct().collect()
updated_clients = [row['args'] for row in updated_clients]

print(len(updated_clients))
print(" list ")
print(updated_clients)

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
#df = read_gsheet("1F8bvKrBdlY0GIm9xAU5v3IM7ERLM6kUdi_wYph9JJds", 1374540499)
from pyspark.sql.functions import to_timestamp, col


#df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "M/d/yyyy HH:mm:ss"))

df = spark.sql(f"""

select

demo_namelowercasenospacenoversionegtriplecrowncentral as demo_name
from yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info
where is_prospect is false


""")
distinct_values = df.select(df.columns[0]).distinct().toPandas().values.flatten()


# COMMAND ----------

distinct_values

# COMMAND ----------

sku_detail = spark.sql(f"""

select
demo_namelowercasenospacenoversionegtriplecrowncentral as demo_name
from yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info
where is_prospect is false


""")

sku_detail.display()

# COMMAND ----------

# Converter updated_clients para set para busca mais eficiente
updated_clients_set = set(updated_clients)

# Filtrar distinct_values para pegar apenas os que NÃO estão em 
updated_clients
clients_not_updated = [client for client in distinct_values if
client not in updated_clients_set]

# Converter para DataFrame se necessário
import pandas as pd
df_clients_not_updated = pd.DataFrame(clients_not_updated,
columns=['client_name'])

print(f"Most likely Quarterly clients: {len(clients_not_updated)}")
print(clients_not_updated)



# COMMAND ----------

# Converter para DataFrame Spark
from pyspark.sql import Row

updated_clients_df = spark.createDataFrame([Row(client=client) for
client in updated_clients])
distinct_values_df = spark.createDataFrame([Row(client=client) for
client in distinct_values])

# Anti-join para pegar apenas os que NÃO estão em updated
clients_not_updated_df = distinct_values_df.join(
    updated_clients_df,
    on="client",
    how="left_anti"
)

clients_not_updated_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH
# MAGIC real_clients AS (
# MAGIC   SELECT demo_namelowercasenospacenoversionegtriplecrowncentral as demo_name
# MAGIC   FROM yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info
# MAGIC   WHERE is_prospect IS FALSE
# MAGIC ),
# MAGIC all_clients as (
# MAGIC   select get_json_object(args, '$[0]')         AS client, max(end_timestamp) as end_timestamp FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
# MAGIC   WHERE name LIKE 'run_everything%'
# MAGIC   AND end_timestamp > CURRENT_DATE() - INTERVAL 2 MONTH
# MAGIC   AND error is null
# MAGIC   group by all
# MAGIC ), base AS (
# MAGIC   SELECT
# MAGIC     get_json_object(args, '$[0]')         AS client,
# MAGIC     to_timestamp(end_timestamp)           AS end_ts,
# MAGIC     user,
# MAGIC     error
# MAGIC   FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
# MAGIC   WHERE name LIKE 'run_everything%'
# MAGIC     AND month(end_timestamp) = month(CURRENT_DATE()) and year(end_timestamp) = year(current_date())
# MAGIC     -- AND user NOT IN ('pfisch@yipitdata.com', 'dkatz@yipitdata.com')
# MAGIC ),
# MAGIC agg AS (
# MAGIC   SELECT
# MAGIC     client,
# MAGIC     max_by(user, end_ts) as user,
# MAGIC     array_sort(collect_list(end_ts))                       AS attempt_timestamps,
# MAGIC     max(struct(end_ts, error)).error                       AS last_error,
# MAGIC     COUNT(*)                                               AS runs_count,
# MAGIC     COUNT_IF(error IS NOT NULL)                            AS runs_with_error
# MAGIC   FROM base
# MAGIC   GROUP BY client
# MAGIC )
# MAGIC SELECT
# MAGIC   ac.client                                         AS args,
# MAGIC   agg.user,
# MAGIC   attempt_timestamps,
# MAGIC   CASE
# MAGIC     WHEN last_error IS NULL 
# MAGIC          AND runs_count > 1 
# MAGIC          AND runs_with_error > 0 
# MAGIC          THEN 'fixed'
# MAGIC     WHEN last_error IS NULL AND size(attempt_timestamps) > 0
# MAGIC          THEN 'success'
# MAGIC     WHEN last_error IS NULL AND runs_count IS NULL then 'needs_run'
# MAGIC     ELSE 'needs_fix'
# MAGIC   END AS status,
# MAGIC   ac.end_timestamp as last_successful_run
# MAGIC FROM all_clients ac
# MAGIC LEFT JOIN agg ON agg.client = ac.client
# MAGIC INNER JOIN real_clients rc ON rc.demo_name = ac.client
# MAGIC ORDER BY args;
