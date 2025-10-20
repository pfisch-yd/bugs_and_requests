WITH
real_clients AS (
  SELECT demo_namelowercasenospacenoversionegtriplecrowncentral as demo_name
  FROM yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info
  WHERE is_prospect IS FALSE
),
all_clients as (
  select get_json_object(args, '$[0]')         AS client, max(end_timestamp) as end_timestamp FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
  WHERE name LIKE 'run_everything%'
  AND end_timestamp > CURRENT_DATE() - INTERVAL 2 MONTH
  AND error is null
  group by all
), base AS (
  SELECT
    get_json_object(args, '$[0]')         AS client,
    to_timestamp(end_timestamp)           AS end_ts,
    user,
    error
  FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
  WHERE name LIKE 'run_everything%'
    AND month(end_timestamp) = month(CURRENT_DATE()) and year(end_timestamp) = year(current_date())
    -- AND user NOT IN ('pfisch@yipitdata.com', 'dkatz@yipitdata.com')
),
agg AS (
  SELECT
    client,
    max_by(user, end_ts) as user,
    array_sort(collect_list(end_ts))                       AS attempt_timestamps,
    max(struct(end_ts, error)).error                       AS last_error,
    COUNT(*)                                               AS runs_count,
    COUNT_IF(error IS NOT NULL)                            AS runs_with_error
  FROM base
  GROUP BY client
)
SELECT
  ac.client                                         AS args,
  agg.user,
  attempt_timestamps,
  CASE
    WHEN last_error IS NULL 
         AND runs_count > 1 
         AND runs_with_error > 0 
         THEN 'fixed'
    WHEN last_error IS NULL AND size(attempt_timestamps) > 0
         THEN 'success'
    WHEN last_error IS NULL AND runs_count IS NULL then 'needs_run'
    ELSE 'needs_fix'
  END AS status,
  ac.end_timestamp as last_successful_run
FROM all_clients ac
LEFT JOIN agg ON agg.client = ac.client
INNER JOIN real_clients rc ON rc.demo_name = ac.client
ORDER BY args;