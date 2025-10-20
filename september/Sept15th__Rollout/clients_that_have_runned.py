df = spark.sql(f"""
select
  user,
  get_json_object(args, '$[0]') as args,
  end_timestamp,
  notebook_path
from yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
where name like "run_everything%"
and end_timestamp > "2025-09-10T20:38:26.203+00:00"
and user not in ("pfisch@yipitdata.com", "dkatz@yipitdata.com")
order by end_timestamp desc
""")

df.display()