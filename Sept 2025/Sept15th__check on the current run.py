# Databricks notebook source
demo_name = dbutils.widgets.get("demo_name")
demo_name

# COMMAND ----------

# DBTITLE 1,Search a Demo
df = spark.sql(f"""
select
  user,
  get_json_object(args, '$[0]') as args,
  end_timestamp,
  notebook_path
from yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
where name like "run_everything%"
and get_json_object(args, '$[0]') = "{demo_name}"
and end_timestamp > "2025-09-10T20:38:26.203+00:00"
and user not in ("pfisch@yipitdata.com", "dkatz@yipitdata.com")
order by end_timestamp desc
""")
df.display()

# COMMAND ----------

df = spark.sql(f"""
select
  user,
  get_json_object(args, '$[0]') as args,
  end_timestamp,
  notebook_path,
  error,
  job_id
from yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
where name like "run_everything%"
and end_timestamp > "2025-07-10T20:38:26.203+00:00"
and user not in ("pfisch@yipitdata.com", "dkatz@yipitdata.com")
order by args, end_timestamp asc
""")

df.display()

# COMMAND ----------

df = spark.sql(f"""
select
    distinct  get_json_object(args, '$[0]') as args,
    notebook_path
from yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
where name like "run_everything%"
and end_timestamp > "2025-09-10T20:38:26.203+00:00"
and user not in ("pfisch@yipitdata.com", "dkatz@yipitdata.com")
""")

df.display()

# COMMAND ----------

# DBTITLE 1,Client updates status
# MAGIC %sql
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     get_json_object(args, '$[0]')         AS client,
# MAGIC     to_timestamp(end_timestamp)           AS end_ts,
# MAGIC     error
# MAGIC   FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
# MAGIC   WHERE name LIKE 'run_everything%'
# MAGIC     AND end_timestamp > '2025-09-10T20:38:26.203+00:00'
# MAGIC     AND user NOT IN ('pfisch@yipitdata.com', 'dkatz@yipitdata.com')
# MAGIC ),
# MAGIC agg AS (
# MAGIC   SELECT
# MAGIC     client,
# MAGIC     array_sort(collect_list(end_ts))                       AS attempt_timestamps,
# MAGIC     max(struct(end_ts, error)).error                       AS last_error,
# MAGIC     COUNT(*)                                               AS runs_count,
# MAGIC     COUNT_IF(error IS NOT NULL)                            AS runs_with_error
# MAGIC   FROM base
# MAGIC   GROUP BY client
# MAGIC )
# MAGIC SELECT
# MAGIC   client                                         AS args,
# MAGIC   attempt_timestamps,
# MAGIC   CASE
# MAGIC     WHEN last_error IS NULL 
# MAGIC          AND runs_count > 1 
# MAGIC          AND runs_with_error > 0 
# MAGIC          THEN 'fixed'
# MAGIC     WHEN last_error IS NULL 
# MAGIC          THEN 'success'
# MAGIC     ELSE 'needs_fix'
# MAGIC   END AS status
# MAGIC FROM agg
# MAGIC ORDER BY args;

# COMMAND ----------


