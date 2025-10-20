# Databricks notebook source
print(" UPDATED CLIENTS ")
updated_clients = spark.sql(f"""
with runs as (
select
  user,
  get_json_object(args, '$[0]') as args,
  end_timestamp,
  notebook_path,
  row_number() over (partition by args order by end_timestamp desc) as rn
from yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
where name like "run_everything%"
and user not in ("dkatz@yipitdata.com", "pfisch@yipitdata.com", "mkuchar@yipitdata.com")
and end_timestamp < "2025-10-16T20:38:26.203+00:00"
and end_timestamp > "2025-10-10T20:38:26.203+00:00"
order by user, args)

select * from runs where rn = 1
""")

updated_clients.display()

# COMMAND ----------

print(" UPDATED CLIENTS ")
updated_clients = spark.sql(f"""
with runs as (
select
  user,
  get_json_object(args, '$[0]') as args,
  end_timestamp,
  notebook_path,
  error,
  row_number() over (partition by args order by end_timestamp desc) as rn
from yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
where name like "run_everything%"
and user not in ("dkatz@yipitdata.com", "pfisch@yipitdata.com", "mkuchar@yipitdata.com")
and end_timestamp < "2025-09-16T20:38:26.203+00:00"
and end_timestamp > "2025-09-10T20:38:26.203+00:00"
order by user, args
)

select * from runs where rn = 1
""")

updated_clients.display()

# COMMAND ----------

print(" UPDATED CLIENTS ")
updated_clients = spark.sql(f"""
with runs as (
select
  user,
  get_json_object(args, '$[0]') as args,
  end_timestamp,
  notebook_path,
  error,
  row_number() over (partition by args order by end_timestamp desc) as rn,
  * except (user, args, end_timestamp, notebook_path, error)
from yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
where name like "run_everything%"
and user not in ("dkatz@yipitdata.com", "pfisch@yipitdata.com", "mkuchar@yipitdata.com")
and end_timestamp < "2025-09-16T20:38:26.203+00:00"
and end_timestamp > "2025-09-10T20:38:26.203+00:00"
order by user, args
)

select * from runs where rn = 1a
""").createOrReplaceTempView("sept_run")

# COMMAND ----------

df = spark.sql(f"""
with add_hours as (         
select
    user,
    args,
    end_timestamp,
    timediff(hour, timestamp("2025-09-15T00:00:00.000+00:00"), end_timestamp) as hour,
    notebook_path,
    error,
    rn
    from sept_run
)

select
hour,
count(*) as runs,
count(case when error = "true" then args end) as error_runs
from
add_hours
group by all
""")

df.display()


# COMMAND ----------

df = spark.sql(f"""
with add_hours as (         
select
    user,
    args,
    end_timestamp,
    timediff(hour, timestamp("2025-09-15T00:00:00.000+00:00"), end_timestamp) as hour,
    notebook_path,
    error,
    rn
    from sept_run
)

select
hour,
user,
count(*) as runs,
count(case when error = "true" then args end) as error_runs
from
add_hours
group by all
""")

df.display()


# COMMAND ----------

df = spark.sql(f"""
with add_hours as (         
select
    user,
    args,
    end_timestamp,
    timediff(hour, timestamp("2025-09-15T00:00:00.000+00:00"), end_timestamp) as hour,
    notebook_path,
    error,
    rn
    from sept_run
)

select
* from add_hours
where hour in (21,22)
""")

df.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC --Can you make it as
# MAGIC --source_table ydx_sacheu_analysts_silver.source_table
# MAGIC --parent_brand = Sacheu
# MAGIC with new_columns as (               
# MAGIC         SELECT 
# MAGIC             gmv,
# MAGIC             coalesce(parent_brand, brand) as parent_brand
# MAGIC         FROM ydx_sacheu_analysts_silver.source_table a
# MAGIC         )
# MAGIC
# MAGIC         select
# MAGIC         parent_brand,
# MAGIC         sum(gmv) as gmv
# MAGIC         ---'SACHEU' as truee
# MAGIC         from new_columns
# MAGIC         --where parent_brand = 'SACHEU'
# MAGIC         group by 1
# MAGIC
# MAGIC         order by 2 desc
# MAGIC         
# MAGIC         limit 100

# COMMAND ----------

# DBTITLE 1,FIRST FREEPORT RUN
print(" UPDATED CLIENTS ")
updated_clients = spark.sql(f"""
with tab as (
select
  user,
  get_json_object(args, '$[0]') as args,
  end_timestamp
from yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
where name like "run_everything%"
and user not in ("dkatz@yipitdata.com", "pfisch@yipitdata.com", "mkuchar@yipitdata.com")
and end_timestamp < "2025-10-15T13:38:26.203+00:00"
and end_timestamp > "2025-10-14T00:38:26.203+00:00"
order by user, args
)

select
    args,
    max(user) as user,
    max(end_timestamp) as end_timestamp
from tab
group by 1
order by end_timestamp

""")

updated_clients.display()

# COMMAND ----------

# DBTITLE 1,SECOND FREEPORT
print(" UPDATED CLIENTS ")
updated_clients = spark.sql(f"""
with tab as (
select
  user,
  get_json_object(args, '$[0]') as args,
  end_timestamp
from yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
where name like "run_everything%"
and user not in ("dkatz@yipitdata.com", "pfisch@yipitdata.com", "mkuchar@yipitdata.com")
and end_timestamp < "2025-10-15T15:38:26.203+00:00"
and end_timestamp > "2025-10-15T13:38:26.203+00:00"
order by user, args
)

select
    args,
    max(user) as user,
    max(end_timestamp) as end_timestamp
from tab
group by 1
order by end_timestamp

""")

updated_clients.display()

# COMMAND ----------

# DBTITLE 1,THIRD
print(" UPDATED CLIENTS ")
updated_clients = spark.sql(f"""
with tab as (
select
  user,
  get_json_object(args, '$[0]') as args,
  end_timestamp
from yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
where name like "run_everything%"
and user not in ("dkatz@yipitdata.com", "pfisch@yipitdata.com", "mkuchar@yipitdata.com")
and end_timestamp < "2025-10-15T20:38:26.203+00:00"
and end_timestamp > "2025-10-15T15:38:26.203+00:00"
order by user, args
)

select
    args,
    max(user) as user,
    max(end_timestamp) as end_timestamp
from tab
group by 1
order by end_timestamp

""")

updated_clients.display()

# COMMAND ----------

print(" UPDATED CLIENTS ")
updated_clients = spark.sql(f"""
with tab as (
select
  user,
  get_json_object(args, '$[0]') as args,
  end_timestamp
from yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
where name like "run_everything%"
and user not in ("dkatz@yipitdata.com", "pfisch@yipitdata.com", "mkuchar@yipitdata.com")
and end_timestamp < "2025-10-15T23:00:26.203+00:00"
and end_timestamp > "2025-10-15T20:38:26.203+00:00"
order by user, args
)

select
    args,
    max(user) as user,
    max(end_timestamp) as end_timestamp
from tab
group by 1
order by end_timestamp

""")

updated_clients.display()

# COMMAND ----------

print(" UPDATED CLIENTS ")
updated_clients = spark.sql(f"""
with tab as (
select
  user,
  get_json_object(args, '$[0]') as args,
  end_timestamp
from yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
where name like "run_everything%"
and user not in ("dkatz@yipitdata.com", "pfisch@yipitdata.com", "mkuchar@yipitdata.com")
and end_timestamp < "2025-10-16T23:00:26.203+00:00"
and end_timestamp > "2025-10-15T23:38:26.203+00:00"
order by user, args
)

select
    args,
    max(user) as user,
    max(end_timestamp) as end_timestamp
from tab
group by 1
order by end_timestamp

""")

updated_clients.display()
