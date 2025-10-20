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


# updated clients
updated_clients = df.select("args").distinct().collect()
updated_clients = [row['args'] for row in updated_clients]

print(len(updated_clients))
print(" list ")
print(updated_clients)

df = read_gsheet("1F8bvKrBdlY0GIm9xAU5v3IM7ERLM6kUdi_wYph9JJds", 1374540499)
df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "M/d/yyyy H:mm:ss"))
distinct_values = df.select(df.columns[1]).distinct().toPandas().values.flatten()