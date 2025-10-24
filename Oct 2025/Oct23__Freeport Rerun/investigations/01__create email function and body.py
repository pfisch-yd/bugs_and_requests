# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Create an email function

# COMMAND ----------

from yipit_databricks_utils import send_email

# COMMAND ----------

demo_name = "triplecrowncentral"
sol_owner = "pfisch+freeport@yipitdata.com"
module = "geo"
total_tasks = 10
successful_tasks = 10

# COMMAND ----------

def freeport_email(demo_name, sol_owner, module, total_tasks, successful_tasks):
        # ============================================
    # Summary Report
    # ============================================

    summary = []
    summary.append
    summary.append("=" * 80)
    summary.append(f"📈 EXECUTION SUMMARY: {demo_name}")
    summary.append("=" * 80)


    failed_tasks = total_tasks - successful_tasks

    summary.append(f"✅ Successful: {successful_tasks}/{total_tasks}")
    summary.append(f"❌ Failed: {failed_tasks}/{total_tasks}")
    summary.append("\n")

    if failed_tasks > 0:
        email_title = f"PROBLEM IN FREEPORT : {demo_name}"
        print(email_title)
        recipient_emails = ["pfisch@yipitdata.com"]
        recipient_emails.append(sol_owner)

        # summary.append("❌ Failed Modules:")
        summary.append(f"✅ Successful:  - {module}")

    else:
        email_title = f"success in freeport : {demo_name}"
        print(email_title)
        recipient_emails = ["pfisch@yipitdata.com"]

    summary.append("=" * 80)
    summary.append(f"✅ Successful:   - {module}")

    summary.append("hash_for_email_filtering : aRiSb8fh8GXsg1upJqQ92P4SPwFmS8IIC49vVKQS01X3cPio02")
    summary.append("=" * 80)

    body = ""
    for i in summary:
        body = body + i + "<br />"

    send_email(subject=email_title,\
            body = body,\
            to_emails=recipient_emails)

# COMMAND ----------


