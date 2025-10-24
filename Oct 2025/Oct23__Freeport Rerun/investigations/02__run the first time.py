# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Let's re run
# MAGIC I want 1 client, internal, 1 table
# MAGIC
# MAGIC I will stick to the classics
# MAGIC
# MAGIC internal triplecrown
# MAGIC module geo
# MAGIC

# COMMAND ----------

# MAGIC %run "./01__create email function and body"

# COMMAND ----------

# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/blueprints__other branches/corporate_transformation_blueprints/corporate_transformation_blueprints/retail_analytics_platform/freeport/module_template_function"

# COMMAND ----------

demo_name = "triplecrowncentral"
sol_owner = "pfisch+freeport@yipitdata.com"
module = "geo"
total_tasks = 10
successful_tasks = 10

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "triplecrowncentral" + "_v38"
module_type = "geographic_analysis"
table_suffix = "_geographic_analysis"
use_sampling =False
sample_fraction =0.01
column = None

# COMMAND ----------

df = spark.table(f"{prod_schema}.{demo_name}{table_suffix}")
df.display()

# COMMAND ----------

#freeport_module(sandbox_schema, prod_schema, demo_name, module_type, use_sampling=False, sample_fraction=0.01, column=None)

# COMMAND ----------



# COMMAND ----------

# Some folders conventions
fp_catalog = "yd_fp_corporate_staging"
ss_catalog = "yd_sensitive_corporate"
view_suffix = "_fp"

# Get module configuration
config = get_module_config(module_type)

if not config:
    raise ValueError(f"Unknown module type: {module_type}")

table_suffix = config["table_suffix"]
table_nickname = config["table_nickname"]
pattern = config["pattern"]
query_type = config["query_type"]
exclude_columns = config["exclude_columns"]
description = config["description"]
schema_type = config["schema_type"]

if schema_type == "sandbox":
    schema = sandbox_schema
else:
    schema = prod_schema

if module_type in ("market_share_standard_calendar", "market_share_nrf_calendar", "market_share_for_column"):
    if module_type == "market_share_for_column":
        table_suffix = table_suffix + "_" + column
        table_nickname = table_nickname + column
    elif module_type == "market_share_standard_calendar":
        table_suffix = table_suffix + "_" + column +'_standard_calendar'
        table_nickname = table_nickname + column + '_std'
    elif module_type == "market_share_nrf_calendar":
        table_suffix = table_suffix + "_" + column +'_nrf_calendar'
        table_nickname = table_nickname + column +'_nrf'

# Create module name, allow for test versions
if sandbox_schema == "ydx_internal_analysts_sandbox":
    module_name = f"corporate_{table_nickname}_test{demo_name}"
else:
    module_name = f"corporate_{table_nickname}_{demo_name}"
#module_name = f"corporate_{table_nickname}_{demo_name}"
input_tables_name = f"{ss_catalog}.{schema}.{demo_name}{table_suffix}"
output_table_name = f"{fp_catalog}.{schema}.{demo_name}{table_suffix}2a"

# Define sources
sources = [{
    'database_name': schema,
    'table_name': demo_name + table_suffix,
    'catalog_name': "yd_sensitive_corporate",
}]

# Apply pattern-specific logic
if pattern == "SIMPLE":
    # SIMPLE pattern: Use SELECT * or explicit column list
    if query_type == "SELECT_ALL":
        query_string = """
            SELECT *
            FROM {{ sources[0].full_name }}
        """
        query_parameters = None
    else:
        raise NotImplementedError("Explicit column selection not implemented for SIMPLE pattern")
else:
    raise ValueError(f"Unknown pattern: {pattern}")  

# Apply pattern-specific logic
if module_type == "sku_detail":
    query_string = """
        SELECT * except (ASP),
        ASP as asp
        FROM {{ sources[0].full_name }}
    """
    query_parameters = None  

# COMMAND ----------

df_play = spark.sql(f"select * except (gmv), gmv + 7 as gmv from ydx_internal_analysts_gold.{demo_name}{table_suffix}")
df_play.printSchema()

# COMMAND ----------

module_name

# COMMAND ----------

# Create query template
query_template = get_or_create_query_template(
    slug=module_name,
    query_string=query_string,
    template_description=f"Corporate {description}",
    version_description=f"Production {description} table",
)

# sem mudar nada, Found matching template: 653
# Found matching template: 3222

# COMMAND ----------

output_table_name

# COMMAND ----------

# Create deliverable : FPModel 15315 : 3 minutes
deliverable = get_or_create_deliverable(
    module_name+"_deliverable",
    query_template=query_template["id"],
    input_tables=[input_tables_name],
    output_table=output_table_name,
    query_parameters=query_parameters,
    description=f"{description} for {demo_name}",
    product_org="corporate",
    allow_major_version=True,
    allow_minor_version=True,
    staged_retention_days=100,
    enable_in_dispatch=Fals
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from yd_fp_corporate_staging.ydx_triplecrown_analysts_gold.triplecrowncentral_v38_geographic_analysis__dmv__001

# COMMAND ----------

# Materialize deliverable : 5 min
materialization = materialize_deliverable(
    deliverable["id"],
    release_on_success=False,
    wait_for_completion=True,
)

# COMMAND ----------

materialization_id = materialization['id']
release_response = release_materialization(materialization_id)

# COMMAND ----------

def get_lastest_table(materialization_id):
    # Assess latest version
    print(f"Waiting for release to complete...")
    while True:
        response = get(f"api/v1/data_model/fp_materialization/{materialization_id}")
        print(response)

        last_fp_release = response.json()["last_fp_release"]
        if last_fp_release:
            if last_fp_release['airflow_status'] == "success":
                print(f"✅ FP Release completed successfully!")
                break
            elif last_fp_release['airflow_status'] == "failed":
                print("❌ FP Release failed!")
                break

            print("Release still running. Waiting 45 seconds...")
            time.sleep(45)
        else:
            print("No releases found. Waiting 45 seconds...")
            time.sleep(45)

    latest_table_name = last_fp_release['view_details']['table_name']
    latest_catalog = last_fp_release['view_details']['catalog_name']
    latest_database = last_fp_release['view_details']['database_name']

    latest_table = f"{latest_catalog}.{latest_database}.{latest_table_name}"
    return latest_table

# COMMAND ----------

latest_table = get_lastest_table(materialization_id)
print(latest_table)

# COMMAND ----------

view_name =  f"{fp_catalog}.{schema}.{demo_name}{table_suffix}{view_suffix}"
view_comment = f"{description} for {demo_name}"

query_view = f"""
    CREATE OR REPLACE VIEW {view_name}
    COMMENT '{view_comment}'
    AS
    SELECT
        *
    FROM {latest_table}
"""
qv = spark.sql(query_view)

spark.table(view_name).display()
