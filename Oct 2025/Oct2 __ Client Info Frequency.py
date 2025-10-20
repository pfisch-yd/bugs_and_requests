# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC WITH
# MAGIC quarterly_clients as (
# MAGIC  select client from (values ('wd40'),('sbm'), ('bosch'), ('kik'),('duraflame'), ('renin'), ('athome'),  ('cabinetworks'), ('crescent'), ('daye'),  ('echo_ope'), ('ecolab'), ('elanco'), ('electrolux'), ('electrolux_cdi'), ('fiskars_crafts'), ('generac'), ('glossier'), ('good_earth'), ('hft'), ('james_hardie'), ('jbweld'),('keter'), ('kidde'), ('kiko'), ('klein'), ('kohler'), ('lasko'), ('lowes'), ('msi'), ('nest'), ('onesize'), ('ove'), ('petsmart'),  ('rain_bird'), ('renin'),  ('target_home'), ('traeger'), ('wayfair')) as t(client)
# MAGIC )
# MAGIC ,hot_fix_clients as (
# MAGIC   select client from (values ("wd40"),
# MAGIC ("sbm"),
# MAGIC ( "bosch"),
# MAGIC ( "kik"),
# MAGIC ("duraflame"),
# MAGIC ( "renin"),
# MAGIC ( "athome"),
# MAGIC (  "cabinetworks"),
# MAGIC ( "crescent"),
# MAGIC ( "daye"),
# MAGIC (  "echo_ope"),
# MAGIC ( "ecolab"),
# MAGIC ( "elanco"),
# MAGIC ( "electrolux"),
# MAGIC ( "electrolux_cdi"),
# MAGIC ( "fiskars_crafts"),
# MAGIC ( "generac"),
# MAGIC ( "glossier"),
# MAGIC ( "good_earth"),
# MAGIC ( "hft"),
# MAGIC ( "james_hardie"),
# MAGIC ( "jbweld"),
# MAGIC ("keter"),
# MAGIC ( "kidde"),
# MAGIC ( "kiko"),
# MAGIC ( "klein"),
# MAGIC ( "kohler"),
# MAGIC ( "lasko"),
# MAGIC ( "lowes"),
# MAGIC ( "msi"),
# MAGIC ( "nest"),
# MAGIC ( "onesize"),
# MAGIC ( "ove"),
# MAGIC ( "petsmart"),
# MAGIC (  "rain_bird"),
# MAGIC ( "renin"),
# MAGIC (  "target_home"),
# MAGIC ( "traeger"),
# MAGIC ( "wayfair"),
# MAGIC ( "lutron"),
# MAGIC ( "stihl_hardscaping_demo"),
# MAGIC ( "forma_brands"),
# MAGIC ( "graco"),
# MAGIC ( "homedepot"),
# MAGIC ( "royaloak"),
# MAGIC ( "sherwin_williams_limited"),
# MAGIC ( "henkel"),
# MAGIC ( "tecovas"),
# MAGIC ( "bubble_skincare"),
# MAGIC ( "welding"),
# MAGIC ( "tractorsupply"),
# MAGIC ( "sherwin_williams"),
# MAGIC ( "patrick_ta"),
# MAGIC ( "purina"),
# MAGIC ( "emerson"),
# MAGIC ( "triplecrowncentral")) as t(client)
# MAGIC )
# MAGIC ,real_clients AS (
# MAGIC   SELECT demo_namelowercasenospacenoversionegtriplecrowncentral as demo_name
# MAGIC   FROM yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info
# MAGIC   WHERE is_prospect IS FALSE
# MAGIC ),
# MAGIC all_clients as (
# MAGIC   select get_json_object(args, '$[0]')         AS client, max(end_timestamp) as end_timestamp, max_by(user, end_timestamp) as user, 'monthly' as frequency
# MAGIC   FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry main
# MAGIC   WHERE name LIKE 'run_everything%'
# MAGIC   AND end_timestamp > CURRENT_DATE() - INTERVAL 2 MONTH
# MAGIC   AND error is null
# MAGIC   group by all
# MAGIC   UNION ALL 
# MAGIC   SELECT *, NULL AS end_timestamp, NULL as user, 'quarterly' as frequency FROM quarterly_clients
# MAGIC )
# MAGIC , all_clients_dedup as (
# MAGIC   select client, max(end_timestamp) as end_timestamp, max_by(user,end_timestamp) as user, max(frequency) as frequency from all_clients
# MAGIC   group by all
# MAGIC )
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
# MAGIC ORDER BY args

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH
# MAGIC quarterly_clients as (
# MAGIC  select client from (values ('wd40'),('sbm'), ('bosch'), ('kik'),('duraflame'), ('renin'), ('athome'),  ('cabinetworks'), ('crescent'), ('daye'),  ('echo_ope'), ('ecolab'), ('elanco'), ('electrolux'), ('electrolux_cdi'), ('fiskars_crafts'), ('generac'), ('glossier'), ('good_earth'), ('hft'), ('james_hardie'), ('jbweld'),('keter'), ('kidde'), ('kiko'), ('klein'), ('kohler'), ('lasko'), ('lowes'), ('msi'), ('nest'), ('onesize'), ('ove'), ('petsmart'),  ('rain_bird'), ('renin'),  ('target_home'), ('traeger'), ('wayfair')) as t(client)
# MAGIC )
# MAGIC
# MAGIC ,hot_fix_clients as (
# MAGIC   select client from (values ("wd40"),
# MAGIC ("sbm"),
# MAGIC ( "bosch"),
# MAGIC ( "kik"),
# MAGIC ("duraflame"),
# MAGIC ( "renin"),
# MAGIC ( "athome"),
# MAGIC (  "cabinetworks"),
# MAGIC ( "crescent"),
# MAGIC ( "daye"),
# MAGIC (  "echo_ope"),
# MAGIC ( "ecolab"),
# MAGIC ( "elanco"),
# MAGIC ( "electrolux"),
# MAGIC ( "electrolux_cdi"),
# MAGIC ( "fiskars_crafts"),
# MAGIC ( "generac"),
# MAGIC ( "glossier"),
# MAGIC ( "good_earth"),
# MAGIC ( "hft"),
# MAGIC ( "james_hardie"),
# MAGIC ( "jbweld"),
# MAGIC ("keter"),
# MAGIC ( "kidde"),
# MAGIC ( "kiko"),
# MAGIC ( "klein"),
# MAGIC ( "kohler"),
# MAGIC ( "lasko"),
# MAGIC ( "lowes"),
# MAGIC ( "msi"),
# MAGIC ( "nest"),
# MAGIC ( "onesize"),
# MAGIC ( "ove"),
# MAGIC ( "petsmart"),
# MAGIC (  "rain_bird"),
# MAGIC ( "renin"),
# MAGIC (  "target_home"),
# MAGIC ( "traeger"),
# MAGIC ( "wayfair"),
# MAGIC ( "lutron"),
# MAGIC ( "stihl_hardscaping_demo"),
# MAGIC ( "forma_brands"),
# MAGIC ( "graco"),
# MAGIC ( "homedepot"),
# MAGIC ( "royaloak"),
# MAGIC ( "sherwin_williams_limited"),
# MAGIC ( "henkel"),
# MAGIC ( "tecovas"),
# MAGIC ( "bubble_skincare"),
# MAGIC ( "welding"),
# MAGIC ( "tractorsupply"),
# MAGIC ( "sherwin_williams"),
# MAGIC ( "patrick_ta"),
# MAGIC ( "purina"),
# MAGIC ( "emerson"),
# MAGIC ( "triplecrowncentral")) as t(client)
# MAGIC )
# MAGIC
# MAGIC
# MAGIC
# MAGIC ,real_clients AS (
# MAGIC   SELECT demo_namelowercasenospacenoversionegtriplecrowncentral as demo_name
# MAGIC   FROM yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info
# MAGIC   WHERE is_prospect IS FALSE
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC all_clients as (
# MAGIC   select get_json_object(args, '$[0]')         AS client, max(end_timestamp) as end_timestamp, max_by(user, end_timestamp) as user, 'monthly' as frequency
# MAGIC   FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry main
# MAGIC   WHERE name LIKE 'run_everything%'
# MAGIC   AND end_timestamp > CURRENT_DATE() - INTERVAL 2 MONTH
# MAGIC   AND error is null
# MAGIC   group by all
# MAGIC   UNION ALL 
# MAGIC   SELECT *, NULL AS end_timestamp, NULL as user, 'quarterly' as frequency FROM quarterly_clients
# MAGIC )
# MAGIC
# MAGIC
# MAGIC , all_clients_dedup as (
# MAGIC   select client, max(end_timestamp) as end_timestamp, max_by(user,end_timestamp) as user, max(frequency) as frequency from all_clients
# MAGIC   group by all
# MAGIC )
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
# MAGIC ORDER BY args

# COMMAND ----------

quarterly = ['KIK', 'Renin', 'athome', 'beautyexample', 'bestbuy', 'bosch', 'cabinetworks', 'crescent', 'cuisinart_v38', 'daye', 'duraflame', 'echo_ope', 'ecolab', 'elanco', 'electrolux', 'electrolux_cdi', 'estee_lauder', 'fiskars_crafts', 'generac', 'glossier', 'good_earth', 'hft', 'homedepot', 'homedepot_v38', 'james_hardie', 'jbweld', 'jh_demo', 'keter', 'keter_monthly', 'kidde', 'kiko', 'klein', 'kohler', 'lasko', 'lowes', 'msi', 'nest', 'onesize', 'ove', 'petsmart', 'pitboss_monthly', 'primo_water', 'rain_bird', 'renin', 's', 'sbm', 'schlage_v38', 'sherwin_williams_limited', 'target_home', 'testblueprints', 'testesteelauder', 'testsolenis', 'traeger', 'wayfair', 'wd40']

monthly = ['marmon', 'vegamour', 'beauty_product', 'woodstream', 'weber', 'tile_and_vinyl', 'skinfix', 'cecred', 'odele', 'solenis', 'michaels', 'caulk_demo', 'kosas', 'summerfridays', 'pratt', 'pitboss', 'biggreenegg', 'cuisinart', 'nexgrill', 'solostove', 'middleby', 'ooni', 'newell', 'libman', 'sherwin_williams', 'osea', 'kik', 'lixil', 'google_demo', 'myers', 'masco', 'amorepacific', 'husqvarna', 'werner', 'wooster', 'andersenwindows', 'ydx_assa_abloy', 'champion', 'ecobee', 'royaloak', 'pfister', 'on', 'graco', 'triplecrowncentral', 'lutron', 'mayzon', 'ideal_electric', 'compana', 'purina', 'scotts', 'sharkninja', 'schlage', 'cargill', 'fbin', 'supergoop', 'fiskars', 'creating', 'shiseido', 'savant', 'amazon', 'stihl', 'tti_power_tools', 'emerson', 'pic_corp', 'pbb_v1', 'hart', 'tti_ope', 'worthington', 'tractorsupply', 'wells_lamont', 'morton_salt', 'chamberlain']

# COMMAND ----------

# MAGIC %run /setup_serverless

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

df.printSchema()

# COMMAND ----------

df = spark.sql(f"""

select

`demo_namelowercasenospacenoversionegtriplecrowncentral` as demo_name,

case
when demo_namelowercasenospacenoversionegtriplecrowncentral in ('KIK', 'Renin', 'athome', 'beautyexample', 'bestbuy', 'bosch', 'cabinetworks', 'crescent', 'cuisinart_v38', 'daye', 'duraflame', 'echo_ope', 'ecolab', 'elanco', 'electrolux', 'electrolux_cdi', 'estee_lauder', 'fiskars_crafts', 'generac', 'glossier', 'good_earth', 'hft', 'homedepot', 'homedepot_v38', 'james_hardie', 'jbweld', 'jh_demo', 'keter', 'keter_monthly', 'kidde', 'kiko', 'klein', 'kohler', 'lasko', 'lowes', 'msi', 'nest', 'onesize', 'ove', 'petsmart', 'pitboss_monthly', 'primo_water', 'rain_bird', 'renin', 's', 'sbm', 'schlage_v38', 'sherwin_williams_limited', 'target_home', 'testblueprints', 'testesteelauder', 'testsolenis', 'traeger', 'wayfair', 'wd40'])

then "QUARTERLY"

when demo_namelowercasenospacenoversionegtriplecrowncentral in ('marmon', 'vegamour', 'beauty_product', 'woodstream', 'weber', 'tile_and_vinyl', 'skinfix', 'cecred', 'odele', 'solenis', 'michaels', 'caulk_demo', 'kosas', 'summerfridays', 'pratt', 'pitboss', 'biggreenegg', 'cuisinart', 'nexgrill', 'solostove', 'middleby', 'ooni', 'newell', 'libman', 'sherwin_williams', 'osea', 'kik', 'lixil', 'google_demo', 'myers', 'masco', 'amorepacific', 'husqvarna', 'werner', 'wooster', 'andersenwindows', 'ydx_assa_abloy', 'champion', 'ecobee', 'royaloak', 'pfister', 'on', 'graco', 'triplecrowncentral', 'lutron', 'mayzon', 'ideal_electric', 'compana', 'purina', 'scotts', 'sharkninja', 'schlage', 'cargill', 'fbin', 'supergoop', 'fiskars', 'creating', 'shiseido', 'savant', 'amazon', 'stihl', 'tti_power_tools', 'emerson', 'pic_corp', 'pbb_v1', 'hart', 'tti_ope', 'worthington', 'tractorsupply', 'wells_lamont', 'morton_salt', 'chamberlain')

then "MONTHLY"

else "INVESTIGATE"

end as frenquency
from yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info
where is_prospect is false


""")

# COMMAND ----------

# MAGIC %sql
# MAGIC with 
# MAGIC source as (
# MAGIC   select
# MAGIC   demo_namelowercasenospacenoversionegtriplecrowncentral as demo_name,
# MAGIC   * except (demo_namelowercasenospacenoversionegtriplecrowncentral)
# MAGIC   from yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info
# MAGIC ),
# MAGIC
# MAGIC add_frequency as (
# MAGIC   select
# MAGIC   case
# MAGIC   when is_prospect then "_prospect_"
# MAGIC
# MAGIC   when demo_name in ('KIK', 'Renin', 'athome', 'beautyexample', 'bestbuy', 'bosch', 'cabinetworks', 'crescent', 'cuisinart_v38', 'daye', 'duraflame', 'echo_ope', 'ecolab', 'elanco', 'electrolux', 'electrolux_cdi', 'estee_lauder', 'fiskars_crafts', 'generac', 'glossier', 'good_earth', 'hft', 'homedepot', 'homedepot_v38', 'james_hardie', 'jbweld', 'jh_demo', 'keter', 'keter_monthly', 'kidde', 'kiko', 'klein', 'kohler', 'lasko', 'lowes', 'msi', 'nest', 'onesize', 'ove', 'petsmart', 'pitboss_monthly', 'primo_water', 'rain_bird', 'renin', 's', 'sbm', 'schlage_v38', 'sherwin_williams_limited', 'target_home', 'testblueprints', 'testesteelauder', 'testsolenis', 'traeger', "fake_client", 'wayfair', 'wd40')
# MAGIC
# MAGIC   then "QUARTERLY"
# MAGIC
# MAGIC   when demo_name in ('marmon', 'vegamour', 'beauty_product', 'woodstream', 'weber', 'tile_and_vinyl', 'skinfix', 'cecred', 'odele', 'solenis', 'michaels', 'caulk_demo', 'kosas', 'summerfridays', 'pratt', 'pitboss', 'biggreenegg', 'cuisinart', 'nexgrill', 'solostove', 'middleby', 'ooni', 'newell', 'libman', 'sherwin_williams', 'osea', 'kik', 'lixil', 'google_demo', 'myers', 'masco', 'amorepacific', 'husqvarna', 'werner', 'wooster', 'andersenwindows', 'ydx_assa_abloy', 'champion', 'ecobee', 'royaloak', 'pfister', 'on', 'graco', 'triplecrowncentral', 'lutron', 'mayzon', 'ideal_electric', 'compana', 'purina', 'scotts', 'sharkninja', 'schlage', 'cargill', 'fbin', 'supergoop', 'fiskars', 'creating', 'shiseido', 'savant', 'amazon', 'stihl', 'tti_power_tools', 'emerson', 'pic_corp', 'pbb_v1', 'hart', 'tti_ope', 'worthington', 'tractorsupply', 'wells_lamont', 'morton_salt', 'chamberlain')
# MAGIC
# MAGIC   then "MONTHLY"
# MAGIC
# MAGIC   else "INVESTIGATE"
# MAGIC
# MAGIC   end as frenquency,
# MAGIC
# MAGIC   *
# MAGIC
# MAGIC   from source
# MAGIC )
# MAGIC
# MAGIC select
# MAGIC   demo_name,
# MAGIC   frenquency,
# MAGIC   dash_display_titlecapitalizedfirstletterreaderfriendlyeg_triple_crown as dash_display_title,
# MAGIC   sandbox_schemaegydx_demoname_analysts_silver as sandbox_schema,
# MAGIC   prod_schemaegydx_demoname_analysts_gold as prod_schema,
# MAGIC   pro_source_tableegydx_retail_silveredison_pro_items_optional as pro_source_table,
# MAGIC   sample_size_guardrail_thresholdaint64eg200 as sample_size_guardrail_threshold,
# MAGIC   parent_brand_importan_tusedtodefine_cover_pageeg_triple_crown as parent_brand,
# MAGIC   client_email_distroegtriplecrownyipitdatacom as client_email_distro,
# MAGIC   start_date_of_dataeg2021_01_01 as start_date_of_data,
# MAGIC   category_cols_l2or_subsub_cat_prep as category_cols,
# MAGIC   special_attribute_columndefineallspecialattributescolumnexfinish_or_materialcolor as special_attribute_column,
# MAGIC   packages,
# MAGIC   youremailyipitdatacom,
# MAGIC   is_prospect,
# MAGIC   how_many_rows,
# MAGIC   timestamp
# MAGIC from
# MAGIC add_frequency

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC [ important notebooks ](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/935387296435874?o=3092962415911490#command/8338015708186695)
# MAGIC
# MAGIC [ important notebooks ](https://yipitdata-corporate.cloud.databricks.com/editor/notebooks/935387296437423?o=3092962415911490#command/8338015708196171)

# COMMAND ----------

merged_df = spark.sql(f"""
with 
source as (
  select
  demo_namelowercasenospacenoversionegtriplecrowncentral as demo_name,
  * except (demo_namelowercasenospacenoversionegtriplecrowncentral)
  from yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info
),

add_frequency as (
  select
  case
  when is_prospect then "_prospect_"

  when demo_name in ('KIK', 'Renin', 'athome', 'beautyexample', 'bestbuy', 'bosch', 'cabinetworks', 'crescent', 'cuisinart_v38', 'daye', 'duraflame', 'echo_ope', 'ecolab', 'elanco', 'electrolux', 'electrolux_cdi', 'estee_lauder', 'fiskars_crafts', 'generac', 'glossier', 'good_earth', 'hft', 'homedepot', 'homedepot_v38', 'james_hardie', 'jbweld', 'jh_demo', 'keter', 'keter_monthly', 'kidde', 'kiko', 'klein', 'kohler', 'lasko', 'lowes', 'msi', 'nest', 'onesize', 'ove', 'petsmart', 'pitboss_monthly', 'primo_water', 'rain_bird', 'renin', 's', 'sbm', 'schlage_v38', 'sherwin_williams_limited', 'target_home', 'testblueprints', 'testesteelauder', 'testsolenis', 'traeger', "fake_client", 'wayfair', 'wd40')

  then "QUARTERLY"

  when demo_name in ('marmon', 'vegamour', 'beauty_product', 'woodstream', 'weber', 'tile_and_vinyl', 'skinfix', 'cecred', 'odele', 'solenis', 'michaels', 'caulk_demo', 'kosas', 'summerfridays', 'pratt', 'pitboss', 'biggreenegg', 'cuisinart', 'nexgrill', 'solostove', 'middleby', 'ooni', 'newell', 'libman', 'sherwin_williams', 'osea', 'kik', 'lixil', 'google_demo', 'myers', 'masco', 'amorepacific', 'husqvarna', 'werner', 'wooster', 'andersenwindows', 'ydx_assa_abloy', 'champion', 'ecobee', 'royaloak', 'pfister', 'on', 'graco', 'triplecrowncentral', 'lutron', 'mayzon', 'ideal_electric', 'compana', 'purina', 'scotts', 'sharkninja', 'schlage', 'cargill', 'fbin', 'supergoop', 'fiskars', 'creating', 'shiseido', 'savant', 'amazon', 'stihl', 'tti_power_tools', 'emerson', 'pic_corp', 'pbb_v1', 'hart', 'tti_ope', 'worthington', 'tractorsupply', 'wells_lamont', 'morton_salt', 'chamberlain')

  then "MONTHLY"

  else "INVESTIGATE"

  end as frenquency,

  *

  from source
)

select
  demo_name,
  frenquency,
  dash_display_titlecapitalizedfirstletterreaderfriendlyeg_triple_crown as dash_display_title,
  sandbox_schemaegydx_demoname_analysts_silver as sandbox_schema,
  prod_schemaegydx_demoname_analysts_gold as prod_schema,
  pro_source_tableegydx_retail_silveredison_pro_items_optional as pro_source_table,
  sample_size_guardrail_thresholdaint64eg200 as sample_size_guardrail_threshold,
  parent_brand_importan_tusedtodefine_cover_pageeg_triple_crown as parent_brand,
  client_email_distroegtriplecrownyipitdatacom as client_email_distro,
  start_date_of_dataeg2021_01_01 as start_date_of_data,
  category_cols_l2or_subsub_cat_prep as category_cols,
  special_attribute_columndefineallspecialattributescolumnexfinish_or_materialcolor as special_attribute_column,
  packages,
  youremailyipitdatacom,
  is_prospect,
  how_many_rows,
  timestamp
from
add_frequency
""")

# COMMAND ----------

create_table('data_solutions_sandbox', 'corporate_clients_info', merged_df, overwrite=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from yd_sensitive_corporate.data_solutions_sandbox.corporate_clients_info
