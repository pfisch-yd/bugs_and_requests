# Databricks notebook source
# MAGIC %md
# MAGIC # Freeport Materialization: Renin Data Type Change Test
# MAGIC
# MAGIC Este notebook combina:
# MAGIC 1. A função `freeport_geo_analysis` do arquivo `after__geo2`
# MAGIC 2. O teste de mudança de tipo de dado do cliente Renin
# MAGIC 3. O processo de materialização e release do Freeport
# MAGIC
# MAGIC **Objetivo:** Testar como o Freeport lida com mudanças de tipo de dado (state: string -> int)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Import Freeport Service

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/ETL_Production/freeport_service/")

from freeport_databricks.client.v1 import (
    get_or_create_deliverable,
    materialize_deliverable,
    release_materialization,
    list_deliverable_materializations,
    get_or_create_query_template,
    get_or_create_deliverable_group,
    sql_from_query_template
)

from freeport_databricks.client.api_client import (
    get,
    FREEPORT_DOMAIN
)

import json
import time
from yipit_databricks_utils.future import create_table

print(f"Freeport Domain: {FREEPORT_DOMAIN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Define Freeport Geo Analysis Function

# COMMAND ----------

def freeport_geo_analysis(sandbox_schema, prod_schema, demo_name):
    """
    Cria um deliverable no Freeport para análise geográfica

    Args:
        sandbox_schema: Schema do sandbox (ex: ydx_internal_analysts_sandbox)
        prod_schema: Schema de produção (ex: ydx_internal_analysts_gold)
        demo_name: Nome do cliente demo (ex: renin_v38)
    """
    sources = [
        {
            'database_name': sandbox_schema,
            'table_name': demo_name + "_geographic_analysis",
            'catalog_name': "yd_sensitive_corporate",
        }
    ]

    query_string = """
        SELECT
            state,
            month,
            parent_brand,
            brand,
            sub_brand,
            merchant,
            major_cat,
            sub_cat,
            minor_cat,
            gmv,
            sample_size,
            observed_spend,
            observed_units
        FROM {{ sources[0].full_name }}
    """

    module_name = "pfisch__geog" + demo_name

    # Create query template
    query_template = get_or_create_query_template(
        slug=module_name,
        query_string=query_string,
        template_description="Display interval mapping for market share analysis",
        version_description="Static mapping table for display intervals",
    )

    fp_catalog = "yd_fp_corporate_staging"
    ss_catalog = "yd_sensitive_corporate"

    # Create deliverable with schema change support
    deliverable = get_or_create_deliverable(
        module_name + "dd",
        query_template=query_template["id"],
        input_tables=[
            f"{ss_catalog}.{prod_schema}.{demo_name}_geographic_analysis"
        ],
        output_table=fp_catalog + "." + prod_schema + "." + demo_name + '_geographic_analysis5',
        description="test Geo 20250923b - Change Data Type Test",
        product_org="corporate",
        allow_major_version=True,  # Permite mudanças de schema major (breaking changes)
        allow_minor_version=True,  # Permite mudanças de schema minor
        staged_retention_days=100
    )

    # Materialize deliverable
    materialization = materialize_deliverable(
        deliverable["id"],
        release_on_success=False,
        wait_for_completion=True,
    )

    return deliverable, materialization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Configure Test Parameters (Renin Client)

# COMMAND ----------

sandbox_schema = "ydx_internal_analysts_sandbox"
prod_schema = "ydx_internal_analysts_gold"
demo_name = "renin"
demo_name = demo_name + "_v38"
pro_source_table = "ydx_retail_silver.edison_pro_items"
start_date_of_data = "2023-01-01"

print(f"Test Client: {demo_name}")
print(f"Prod Schema: {prod_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: View Original Table Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver schema original da coluna 'state'
# MAGIC SELECT column_name, data_type
# MAGIC FROM information_schema.columns
# MAGIC WHERE table_name = 'renin_v38_geographic_analysis'
# MAGIC AND column_name = "state"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver dados originais
# MAGIC SELECT *
# MAGIC FROM ydx_internal_analysts_gold.renin_v38_geographic_analysis
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Store Original Table (Backup)

# COMMAND ----------

store_table = spark.sql(f"""
    SELECT *
    FROM {prod_schema}.{demo_name}_geographic_analysis
""")

print(f"Original table rows: {store_table.count()}")
store_table.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Change Data Type - Modify 'state' Column (String -> Int)
# MAGIC
# MAGIC **Mudança de Schema:** Trocar a coluna `state` (string) por um valor inteiro (2025)
# MAGIC
# MAGIC Isso simula uma mudança de tipo de dado que deveria ser detectada pelo Freeport

# COMMAND ----------

# Modificar schema: trocar 'state' de string para int
modified_table = spark.sql(f"""
    SELECT
        * EXCEPT (state),
        2025 AS state
    FROM {prod_schema}.{demo_name}_geographic_analysis
""")

print("Modified table schema:")
modified_table.printSchema()
modified_table.display()

# COMMAND ----------

# Sobrescrever tabela com novo schema
create_table(prod_schema, demo_name + '_geographic_analysis', modified_table, overwrite=True)

print(f"✅ Table {prod_schema}.{demo_name}_geographic_analysis overwritten with new schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Schema Change

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar que o tipo mudou para INT
# MAGIC SELECT column_name, data_type
# MAGIC FROM information_schema.columns
# MAGIC WHERE table_name = 'renin_v38_geographic_analysis'
# MAGIC AND column_name = "state"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create Freeport Deliverable & Materialize
# MAGIC
# MAGIC Agora vamos criar o deliverable no Freeport e materializar.
# MAGIC O Freeport deve detectar a mudança de schema e criar uma nova versão DMV.

# COMMAND ----------

deliverable, materialization = freeport_geo_analysis(sandbox_schema, prod_schema, demo_name)

print(f"Deliverable ID: {deliverable['id']}")
print(f"Materialization ID: {materialization['id']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Release Materialization
# MAGIC
# MAGIC Release a materialização para que a tabela fique disponível no Freeport

# COMMAND ----------

materialization_id = materialization['id']
release_response = release_materialization(materialization_id)

print(f"Release initiated for materialization {materialization_id}")
print(release_response)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Wait for Release to Complete

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Get Latest Table Information

# COMMAND ----------

latest_table_name = last_fp_release['view_details']['table_name']
latest_catalog = last_fp_release['view_details']['catalog_name']
latest_database = last_fp_release['view_details']['database_name']

latest_table = f"{latest_catalog}.{latest_database}.{latest_table_name}"
print(f"✅ Latest table: {latest_table}")

# COMMAND ----------

view_name = f"{prod_schema}.{demo_name}_geographic_analysis" + "_view"
print(view_name)

# COMMAND ----------

view_name = f"{prod_schema}.{demo_name}_geographic_analysis" + "_view"

query_view = f"""
    CREATE OR REPLACE VIEW {view_name}
    COMMENT 'Exemplo de view virtual - apenas definição SQL'
    AS
    SELECT
        *
    FROM {latest_table}
"""

print(query_view)

spark.sql(query_view)

print("✅ View criada com sucesso!")
