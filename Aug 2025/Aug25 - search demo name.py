# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/Corporate Sensitive - Dashboard Templates/Brands Dashboard Templates/__centralized_udfs_for_client_dashboard_exports"

# COMMAND ----------

search_demo_name("amazon")

# COMMAND ----------

window_spec = (
    Window
    .partitionBy(F.col("demo_namelowercasenospacenoversionegtriplecrowncentral"))
    .orderBy(F.monotonically_increasing_id().desc())
)

out = (
    df.withColumn("rank", F.row_number().over(window_spec))
      .select("demo_namelowercasenospacenoversionegtriplecrowncentral", "rank")
)

out = out.filter("rank = 1")
clients = out.select("demo_namelowercasenospacenoversionegtriplecrowncentral")

filtered_clients = clients.filter(
    lower(clients["demo_namelowercasenospacenoversionegtriplecrowncentral"]).like("%"+search_word+"%")
)

if search_word is None:
    final_table = clients
else:
    final_table = filtered_clients

final_table = final_table.withColumnRenamed("demo_namelowercasenospacenoversionegtriplecrowncentral", "demo_name")

final_table.display()

# COMMAND ----------

df = read_gsheet(
"1F8bvKrBdlY0GIm9xAU5v3IM7ERLM6kUdi_wYph9JJds",
1374540499
)

# Define a dummy window to assign reverse order (biggest first)
window_spec = Window.partitionBy("demo_namelowercasenospacenoversionegtriplecrowncentral").orderBy(lit(1).desc())


# COMMAND ----------


clients = df.withColumn("rank", row_number().over(window_spec))
clients = clients.select("demo_namelowercasenospacenoversionegtriplecrowncentral")

clients.display()

# COMMAND ----------

# Add a rank within each group and filter for the first (i.e., "best")
clients = df.select("demo_namelowercasenospacenoversionegtriplecrowncentral")
clients = clients.withColumn("rank", row_number().over(window_spec)) \
            .filter("rank = 1") \
            .select("demo_namelowercasenospacenoversionegtriplecrowncentral")

# COMMAND ----------

def search_demo_name(search_word):
    window_spec = (
        Window
        .partitionBy(F.col("demo_namelowercasenospacenoversionegtriplecrowncentral"))
        .orderBy(F.monotonically_increasing_id().desc())
    )

    out = (
        df.withColumn("rank", F.row_number().over(window_spec))
        .select("demo_namelowercasenospacenoversionegtriplecrowncentral", "rank")
    )

    out = out.filter("rank = 1")
    clients = out.select("demo_namelowercasenospacenoversionegtriplecrowncentral")

    filtered_clients = clients.filter(
        lower(clients["demo_namelowercasenospacenoversionegtriplecrowncentral"]).like("%"+search_word+"%")
    )

    if search_word is None:
        final_table = clients
    else:
        final_table = filtered_clients

    final_table = final_table.withColumnRenamed("demo_namelowercasenospacenoversionegtriplecrowncentral", "demo_name")

    final_table.display()
    return final_table

# COMMAND ----------

search_demo_name("test")
