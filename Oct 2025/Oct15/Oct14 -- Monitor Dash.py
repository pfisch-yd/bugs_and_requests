# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC FROM yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH
# MAGIC real_clients AS (
# MAGIC   SELECT demo_name, frequency
# MAGIC   FROM yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info
# MAGIC   WHERE is_actIve
# MAGIC )
# MAGIC
# MAGIC , all_clients as (
# MAGIC   select
# MAGIC   get_json_object(args, '$[0]') AS demo_name,
# MAGIC   end_timestamp,
# MAGIC   notebook_path,
# MAGIC   user,
# MAGIC   row_number() over (partition by args order by end_timestamp desc) as rn
# MAGIC   FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry main
# MAGIC   
# MAGIC   WHERE name LIKE 'run_everything%'
# MAGIC   AND end_timestamp > CURRENT_DATE() - INTERVAL 2 MONTH
# MAGIC )
# MAGIC , dd_all_clients as (
# MAGIC   select * from all_clients where rn = 1
# MAGIC )
# MAGIC
# MAGIC , proof as (
# MAGIC
# MAGIC select
# MAGIC real_clients.*,
# MAGIC dd_all_clients.* except (demo_name)
# MAGIC from real_clients
# MAGIC left join dd_all_clients
# MAGIC using(demo_name)
# MAGIC )
# MAGIC
# MAGIC , base AS (
# MAGIC   SELECT
# MAGIC     get_json_object(args, '$[0]') AS demo_name,
# MAGIC     to_timestamp(end_timestamp) AS end_timestamp,
# MAGIC     error
# MAGIC   FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
# MAGIC   WHERE name LIKE 'run_everything%'
# MAGIC     AND month(end_timestamp) = month(CURRENT_DATE())
# MAGIC     and year(end_timestamp) = year(current_date())
# MAGIC     -- AND user NOT IN ('pfisch@yipitdata.com', 'dkatz@yipitdata.com')
# MAGIC )
# MAGIC
# MAGIC , agg AS (
# MAGIC   SELECT
# MAGIC     demo_name,
# MAGIC     array_sort(collect_list(end_timestamp))                       AS attempt_timestamps,
# MAGIC     max(struct(end_timestamp, error)).error                       AS last_error,
# MAGIC     COUNT(*)                                               AS runs_count,
# MAGIC     COUNT_IF(error IS NOT NULL)                            AS runs_with_error
# MAGIC   FROM base
# MAGIC   GROUP BY demo_name
# MAGIC )
# MAGIC
# MAGIC , joined as (
# MAGIC
# MAGIC SELECT
# MAGIC   ac.*,
# MAGIC   agg.* except (demo_name),
# MAGIC   rc.* except (demo_name)
# MAGIC FROM dd_all_clients ac 
# MAGIC left join agg on agg.demo_name = ac.demo_name
# MAGIC INNER JOIN real_clients rc ON rc.demo_name = ac.demo_name
# MAGIC ORDER BY ac.demo_name
# MAGIC )
# MAGIC
# MAGIC select
# MAGIC   demo_name as args,
# MAGIC   user,
# MAGIC   attempt_timestamps,
# MAGIC   CASE
# MAGIC     WHEN demo_name IS NOT NULL THEN 'quarterly_client_fixed_by_engineering'
# MAGIC     WHEN last_error IS NULL 
# MAGIC          AND runs_count > 1 
# MAGIC          AND runs_with_error > 0 
# MAGIC          THEN 'fixed'
# MAGIC     WHEN last_error IS NULL AND size(attempt_timestamps) > 0
# MAGIC          THEN 'success'
# MAGIC     WHEN last_error IS NULL AND runs_count IS NULL then 'needs_run'
# MAGIC     ELSE 'needs_fix'
# MAGIC   END AS status,
# MAGIC
# MAGIC   IF(last_error is not null,concat(left(last_error,200),IF(len(last_error) > 200,'...','')),NULL) as error,
# MAGIC   end_timestamp as last_successful_run,
# MAGIC   frequency
# MAGIC from joined
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH
# MAGIC quarterly_clients as (
# MAGIC  select client from (values ('wd40'),('sbm'), ('bosch'), ('kik'),('duraflame'), ('renin'), ('athome'),  ('cabinetworks'), ('crescent'), ('daye'),  ('echo_ope'), ('ecolab'), ('elanco'), ('electrolux'), ('electrolux_cdi'), ('fiskars_crafts'), ('generac'), ('glossier'), ('good_earth'), ('hft'), ('james_hardie'), ('jbweld'),('keter'), ('kidde'), ('kiko'), ('klein'), ('kohler'), ('lasko'), ('lowes'), ('msi'), ('nest'), ('onesize'), ('ove'), ('petsmart'),  ('rain_bird'), ('renin'),  ('target_home'), ('traeger'), ('wayfair')) as t(client)
# MAGIC )
# MAGIC ,
# MAGIC
# MAGIC
# MAGIC
# MAGIC hot_fix_clients as (
# MAGIC   select client from (values ("wd40"),
# MAGIC ( "triplecrowncentral")) as t(client)
# MAGIC )
# MAGIC
# MAGIC ,real_clients AS (
# MAGIC   SELECT demo_name
# MAGIC   FROM yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info
# MAGIC   WHERE is_prospect IS FALSE
# MAGIC )
# MAGIC
# MAGIC
# MAGIC ,all_clients as (
# MAGIC   select
# MAGIC   get_json_object(args, '$[0]')         AS client,
# MAGIC   max(end_timestamp) as end_timestamp,
# MAGIC   max_by(user, end_timestamp) as user, frequency
# MAGIC
# MAGIC   FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry main
# MAGIC   
# MAGIC   WHERE name LIKE 'run_everything%'
# MAGIC   AND end_timestamp > CURRENT_DATE() - INTERVAL 2 MONTH
# MAGIC   AND error is null
# MAGIC   
# MAGIC   group by all
# MAGIC   
# MAGIC   UNION ALL 
# MAGIC   
# MAGIC   SELECT
# MAGIC   *,
# MAGIC   NULL AS end_timestamp,
# MAGIC   NULL as user,
# MAGIC   'quarterly' as frequency
# MAGIC   FROM quarterly_clients
# MAGIC )
# MAGIC
# MAGIC , all_clients_dedup as (
# MAGIC   select client,
# MAGIC   max(end_timestamp) as end_timestamp,
# MAGIC   max_by(user,end_timestamp) as user,
# MAGIC   max(frequency) as frequency
# MAGIC   from all_clients
# MAGIC   group by all
# MAGIC )
# MAGIC
# MAGIC , base AS (
# MAGIC   SELECT
# MAGIC     get_json_object(args, '$[0]')         AS client,
# MAGIC     to_timestamp(end_timestamp)           AS end_ts,
# MAGIC     error
# MAGIC   FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
# MAGIC   WHERE name LIKE 'run_everything%'
# MAGIC     AND month(end_timestamp) = month(CURRENT_DATE()) and year(end_timestamp) = year(current_date())
# MAGIC     -- AND user NOT IN ('pfisch@yipitdata.com', 'dkatz@yipitdata.com')
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
# MAGIC
# MAGIC SELECT
# MAGIC   ac.client                                         AS args,
# MAGIC   ac.user,
# MAGIC   attempt_timestamps,
# MAGIC   CASE
# MAGIC     WHEN hf.client IS NOT NULL THEN 'quarterly_client_fixed_by_engineering'
# MAGIC     WHEN last_error IS NULL 
# MAGIC          AND runs_count > 1 
# MAGIC          AND runs_with_error > 0 
# MAGIC          THEN 'fixed'
# MAGIC     WHEN last_error IS NULL AND size(attempt_timestamps) > 0
# MAGIC          THEN 'success'
# MAGIC     WHEN last_error IS NULL AND runs_count IS NULL then 'needs_run'
# MAGIC     ELSE 'needs_fix'
# MAGIC   END AS status,
# MAGIC   IF(last_error is not null,concat(left(last_error,200),IF(len(last_error) > 200,'...','')),NULL) as error,
# MAGIC   ac.end_timestamp as last_successful_run,
# MAGIC   ac.frequency
# MAGIC FROM all_clients_dedup ac 
# MAGIC left join agg on agg.client = ac.client
# MAGIC INNER JOIN real_clients rc ON rc.demo_name = ac.client
# MAGIC LEFT JOIN hot_fix_clients hf on hf.client = ac.client
# MAGIC ORDER BY args;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH
# MAGIC real_clients AS (
# MAGIC   SELECT demo_name, frequency
# MAGIC   FROM yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info
# MAGIC   WHERE is_actIve
# MAGIC )
# MAGIC
# MAGIC , all_clients as (
# MAGIC   select
# MAGIC   get_json_object(args, '$[0]') AS demo_name,
# MAGIC   end_timestamp,
# MAGIC   notebook_path,
# MAGIC   user,
# MAGIC   row_number() over (partition by args order by end_timestamp desc) as rn
# MAGIC   FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry main
# MAGIC   
# MAGIC   WHERE name LIKE 'run_everything%'
# MAGIC   AND end_timestamp > CURRENT_DATE() - INTERVAL 2 MONTH
# MAGIC )
# MAGIC , dd_all_clients as (
# MAGIC   select * from all_clients where rn = 1
# MAGIC )
# MAGIC
# MAGIC , proof as (
# MAGIC
# MAGIC select
# MAGIC real_clients.*,
# MAGIC dd_all_clients.* except (demo_name)
# MAGIC from real_clients
# MAGIC left join dd_all_clients
# MAGIC using(demo_name)
# MAGIC )
# MAGIC
# MAGIC , base AS (
# MAGIC   SELECT
# MAGIC     get_json_object(args, '$[0]') AS demo_name,
# MAGIC     to_timestamp(end_timestamp) AS end_timestamp,
# MAGIC     error
# MAGIC   FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
# MAGIC   WHERE name LIKE 'run_everything%'
# MAGIC     and day(end_timestamp) > day("2025-10-12")
# MAGIC     AND month(end_timestamp) = month(CURRENT_DATE())
# MAGIC     and year(end_timestamp) = year(current_date())
# MAGIC     -- AND user NOT IN ('pfisch@yipitdata.com', 'dkatz@yipitdata.com')
# MAGIC   union ALL
# MAGIC   (
# MAGIC     SELECT
# MAGIC       "masco" as demo_name,
# MAGIC       timestamp("2025-10-15T20:44:14.237+00:00") AS end_timestamp,
# MAGIC       null as error
# MAGIC   ) -- 
# MAGIC )
# MAGIC
# MAGIC , agg AS (
# MAGIC   SELECT
# MAGIC     demo_name,
# MAGIC     array_sort(collect_list(end_timestamp))                       AS attempt_timestamps,
# MAGIC     max(struct(end_timestamp, error)).error                       AS last_error,
# MAGIC     COUNT(*)                                               AS runs_count,
# MAGIC     COUNT_IF(error IS NOT NULL)                            AS runs_with_error
# MAGIC   FROM base
# MAGIC   GROUP BY demo_name
# MAGIC )
# MAGIC
# MAGIC , joined as (
# MAGIC
# MAGIC SELECT
# MAGIC   ac.*,
# MAGIC   agg.* except (demo_name),
# MAGIC   rc.* except (demo_name)
# MAGIC FROM dd_all_clients ac 
# MAGIC left join agg on agg.demo_name = ac.demo_name
# MAGIC INNER JOIN real_clients rc ON rc.demo_name = ac.demo_name
# MAGIC ORDER BY ac.demo_name
# MAGIC )
# MAGIC
# MAGIC , last_table as (
# MAGIC
# MAGIC select
# MAGIC   demo_name as args,
# MAGIC   user,
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
# MAGIC
# MAGIC   IF(last_error is not null,concat(left(last_error,200),IF(len(last_error) > 200,'...','')),NULL) as error,
# MAGIC   end_timestamp as last_successful_run,
# MAGIC   frequency
# MAGIC from joined
# MAGIC )
# MAGIC
# MAGIC select * from last_table
# MAGIC WHERE args = "masco"
