## **⚓ Freeport User Guide ⚓** {#⚓-freeport-user-guide-⚓}

*\- Freeport's goal is to let users ship data with confidence \-*

[⚓ Freeport User Guide ⚓](#⚓-freeport-user-guide-⚓)

[What is Freeport?](#what-is-freeport?)

[Freeport Adoption Today](#freeport-adoption-today)

[Freeport \+ Dispatch Integration](#freeport-+-dispatch-integration)

[Getting Started](#getting-started)

[Creating Deliverables on Freeport](#creating-deliverables-on-freeport)

[Setup](#setup)

[1\. Configure a Deliverable](#1.-configure-a-deliverable)

[Configure a Variant Deliverable](#configure-a-variant-deliverable)

[2\. Complete deliverable configuration on Dispatch](#2.-complete-deliverable-configuration-on-dispatch)

[Complete the setup for a Variant Deliverable on Dispatch](#complete-the-setup-for-a-variant-deliverable-on-dispatch)

[3\. Materialize the deliverable](#3.-materialize-the-deliverable)

[Core Features](#core-features)

[Query Templating](#query-templating)

[Query template requirements](#query-template-requirements)

[Query template techniques](#query-template-techniques)

[Testing new query templates as a SQL query](#testing-new-query-templates-as-a-sql-query)

[Testing new query templates as a DataFrame](#testing-new-query-templates-as-a-dataframe)

[Preview existing query template](#preview-existing-query-template)

[Save a query template](#save-a-query-template)

[Updating query template (creating a new version)](#updating-query-template-\(creating-a-new-version\))

[Table Release Management](#table-release-management)

[Staging, Prod, and Audit Catalogs](#staging,-prod,-and-audit-catalogs)

[Release Lifecycle](#release-lifecycle)

[When and which table / catalog to use](#when-and-which-table-/-catalog-to-use)

[Auto-Generated Tables](#auto-generated-tables)

[QA Framework](#qa-framework)

[Recipe Book](#recipe-book)

[Common Deliverable Tables](#common-deliverable-tables)

[Batch Table](#batch-table)

[Changes (Incremental) Daily Table](#changes-\(incremental\)-daily-table)

[Changes (Incremental) Monthly Table](#changes-\(incremental\)-monthly-table)

[Changes (Incremental) Weekly Table](#changes-\(incremental\)-weekly-table)

[PIT Table](#pit-table)

[Internal-Only Table](#internal-only-table)

[Releasing Deliverables](#releasing-deliverables)

[Disabling Release](#disabling-release)

[Scheduling Release](#scheduling-release)

[Release Disabled Materializations](#release-disabled-materializations)

[Disabled Scheduled Materializations](#disabled-scheduled-materializations)

[List Materializations](#list-materializations)

[Customizing Content](#customizing-content)

[Filtering Content](#filtering-content)

[FAQs](#faqs)

[Common Errors](#common-errors)

[Running into schema errors at materialization time](#running-into-schema-errors-at-materialization-time)

[Running in Production](#running-in-production)

[Where to add Freeport code?](#where-to-add-freeport-code?)

[Are tables ready when materialize\_deliverable returns?](#are-tables-ready-when-materialize_deliverable-returns?)

[When are Freeport tables sent to clients?](#when-are-freeport-tables-sent-to-clients?)

[What cluster is used for deliverables and materializations?](#what-cluster-is-used-for-deliverables-and-materializations?)

### **What is Freeport?** {#what-is-freeport?}

Freeport is a data packaging system that allows users to create and validate cuts of data shared externally with clients or internally with other teams. Freeport is typically accessed via the `freeport_client` found in Databricks or via REST API for technical use cases. With Freeport, you have the ability to:

* Generate a wide range of deliverables (a.k.a. data models) from input gold tables using templated transformations  
* Automate QA to run tests on input gold tables and/or deliverable tables created by Freeport  
* Control when deliverable tables are shared with clients or other teams.   
* Tracks all released versions of a deliverable over time with an auto-generated audit table.

Freeport does much more as it incorporates best practices from the ETL team in producing deliverables on your behalf. To further understand its range of functionality, check out the [**Getting Started**](#getting-started)  and [**Recipe Book**](#recipe-book) sections below.

#### **Freeport Adoption Today** {#freeport-adoption-today}

Freeport is used across the company, supporting our most critical products 24/7:

* Core Metrics and YDL Feeds  
* YDL Dashboards  
* Edison Source and Pulse Feeds  
* US Brands Dashboards and Feeds  
* Corporate Solutions Feeds

#### **Freeport \+ Dispatch Integration** {#freeport-+-dispatch-integration}

Freeport is tightly integrated with Dispatch to support teams in distributing data to clients. Deliverables created in Freeport are automatically synced to Dispatch and can be linked to Products and SKUs. Dispatch will then take Freeport deliverables that are released and export them to clients in their preferred destinations.

### **Getting Started** {#getting-started}

#### **Creating Deliverables on Freeport** {#creating-deliverables-on-freeport}

Generating deliverables on Freeport involves 3 core steps:

1. Configure a deliverable  
2. Complete deliverable configuration on Dispatch (*1 time, only needed for client facing deliverables*)  
3. Materialize the deliverable

##### **Setup** {#setup}

These steps above can be completed using the Freeport client on Databricks. To import the client, do the following:

```py
import sys
sys.path.append(
    "/Workspace/Repos/ETL_Production/freeport_service/"
)

from freeport_databricks.client.v1 import (
    get_or_create_deliverable, 
    materialize_deliverable
)
```

##### **1\. Configure a Deliverable** {#1.-configure-a-deliverable}

The `get_or_create_deliverable`  function should be used to configure a deliverable. It is safe to re-run in notebooks and workflows. The function will:

* Create a deliverable if it doesn't exist  
* Update and then retrieve a deliverable from Freeport if the deliverable existed

**Examples**:

```py
deliverable = get_or_create_deliverable(
   "EI Aggregate Feeds - Merchant",
   "yd_production.ed_agg_feeds_gold.merchant_metrics",
   output_table="yd_fp_ei_staging.ed_agg_feeds_gold.merchant_metrics",
   slug="ei_agg_feeds_merchant",
)
```

In this example, a Deliverable titled "EI Aggregate Feeds \- Merchant" is created and the output table is an exact copy of the input table: `yd_production.ed_agg_feeds_gold.merchant_metrics`. The `slug` argument is used to uniquely identify the deliverable so that the title can be edited in the future, if needed. The returned value is a dictionary with metadata about the deliverable. Freeport will generate an output database and table based on the arguments provided when materializing the deliverable later on. 

Modify the function's arguments and re-run to configure the deliverable's output table among many other settings:

```py
deliverable = get_or_create_deliverable(
   "EI Aggregate Feeds - Merchant",
   "yd_production.ed_agg_feeds_gold.merchant_metrics",
   output_table="yd_fp_ei_staging.ed_agg_feeds_gold.mm",
   slug="ei_agg_feeds_merchant",
)
```

This will in most cases update the existing deliverable, but if there are breaking schema changes to a table a new deliverable is created.

Run `?? get_or_create_deliverable` in a databricks cell to see the function's docstring, argument list, and options. For examples of this function, check the **Recipe Book** section below.

###### *Configure a Variant Deliverable* {#configure-a-variant-deliverable}

In the case of a variant-enabled deliverable, once you have set up the deliverable and have at least one materialization on the deliverable published and sent through Dispatch, you can create a variant. This is an efficient way of enabling filtering on a deliverable with minimal management. For example, the U.S. Brands dataset uses variants to send filtered feeds to select customers.

```py
deliverable = get_or_create_deliverable(
   "EI Aggregate Feeds - Merchant",
   "yd_production.ed_agg_feeds_gold.merchant_metrics",
   output_table="yd_fp_ei_staging.ed_agg_feeds_gold.mm",
   slug="ei_agg_feeds_merchant",
   enable_variants = True,  # Add this
   variant_field_names = ["ticker"]  # Select the columns you would like to allow filtering on; you cannot delete the selected columns from the table going forward
```

Once you release your subsequent materialization, Dispatch will pick up the requested variant configuration on the Deliverable.

##### **2\. Complete deliverable configuration on Dispatch** {#2.-complete-deliverable-configuration-on-dispatch}

This step is required if you intend for a deliverable to be sent to clients. After 1), Freeport will sync the deliverable to Dispatch, where additional settings need to be added to the deliverable for distribution. Submit a ticket via this [Jira form](https://get.support.yipitdata.com/servicedesk/customer/portal/139/group/293/create/576) (Select “Create a New Custom Feed for Corporate Client” under “What do you need help with?) to edit these deliverables to include the relevant product and SKU tags, S3 prefix, and Snowflake names. Members of the Product Infrastructure and Distribution Engineering teams will guide you on the specific steps after the ticket is filed.

From there, Dispatch will know how to distribute a Freeport Deliverable to a client based on the deliverable's configuration and the client's delivery preferences. This should be a 1-time setup that will be applied to all future publications of the deliverable.

###### *Complete the setup for a Variant Deliverable on Dispatch* {#complete-the-setup-for-a-variant-deliverable-on-dispatch}

For variants, you must select the variant definition that would be shared with a client. This is available on the Dispatch UI. Please submit a support ticket to ensure the setup is completed successfully.

##### **3\. Materialize the deliverable** {#3.-materialize-the-deliverable}

A deliverable is only a configuration, and to generate output data, it must be materialized. Materializations are the event of generating a new table version of a Freeport deliverable. These table versions live in the output\_table of the deliverable.

The `materialize_deliverable` function can be used to generate a new table version and release it to clients and internal consumers.

**Example**

```py
materialization = materialize_deliverable(
   deliverable["id"],
)
```

After the function completes, the output table can be queried for the newly generated data. The table name will be in the cell output of the deliverable created in 1). The materialization will be processed in the background on a separate cluster managed by Freeport while the function polls for its completion. 

If the deliverable was configured in Dispatch, Dispatch will release the materialization (i.e. new table version) to subscribed clients in their preferred distribution channel (S3, snowflake, sigma, etc.). There are options to schedule the release timestamp of the materialization or to prevent a release immediately. The schedule and release behavior can be adjusted later using the `release_materialization` function.

Run `?? materialize_deliverable` in a databricks cell to see the function's full argument list and options. For more relevant examples of this function, visit the [**Recipe Book**](#recipe-book) section below.

### **Core Features** {#core-features}

#### **Query Templating** {#query-templating}

Up until now, the deliverable examples copy the input table to the output table managed by Freeport. Freeport can do much more powerful transformations using its query templating engine. 

```py
deliverable = get_or_create_deliverable(
   "EI Standard Feeds Purchase - US",
   "yd_production.edison_receipts_gold.purchase_safe",
   output_table="yd_fp_ei_staging.ed_standard_feeds_gold.purchase_us",
   slug="ei_std_feeds_purchase_us",
   query_template=PURCHASE_QUERY_TEMPLATE_ID,
   query_parameters={
       "country": "US",
   },
)
```

The general idea is: (**(query template) (query parameters)) (input table) \= output table**. The query template and query parameters produce SQL queries that are applied on the input table(s) to write to the output table. The goal of templates is to reduce the number of transformations independently coded, especially for repetitive cuts of deliverables. 

Query templates are stored in Freeport in its backend and referenced via its version ID. New versions of query templates can be developed by users to iterate on their transformations. The versioning mechanism allows for transformations to be "locked" until a new version is opted to be used, which **significantly reduces the risk** of modifying deliverables incorrectly.

##### **Query template requirements** {#query-template-requirements}

Query templates must satisfy the following:

* Be a valid Jinja template string  
* Resolve to at least 1 SQL query that is a SELECT .. expression  
* Multiple SQL query statements can be generated from the template but they must be separated by a semicolon.  
  * Freeport will union the various select expressions together when executing the query  
  * When creating tables, these expressions will be unioned efficiently using an append\_table operation  
  * It is assumed by Freeport that all select expressions have the same output schema  
* Use the following variables provided to the the Jinja template context via Freeport  
  * `parameters`: This is the core variable (dict) where user inputs are allowed. Any value for query\_parameters passed into Freeport functions will be referenced here.  
  * `sources`: List of all source tables used in the Freeport deliverable or query expression. For deliverables, the values are supplied automatically via Freeport. In the template context, the sources variable will be a list of dicts with the following keys:  
    * `catalog_name`  
    * `database_name`  
    * `table_name`  
    * `full_name` (i.e.  `<catalog_name>.<database_name>.<table_name>`)

##### **Query template techniques** {#query-template-techniques}

Refer to the [following documentation](https://docs.google.com/document/d/15oKCug-IoqYwaxb6DA0VA6pPEg_8EBp-yzNWfh14y2I/edit#heading=h.8hq1q3xige36) to learn about specific techniques and best practices in constructing Jinja query templates

##### **Testing new query templates as a SQL query** {#testing-new-query-templates-as-a-sql-query}

Preview the output of a new query template string with custom parameters by passing in a `query_string` to `sql_from_query_template`. This will return the SQL queries generated by the template as string to use for development purposes. 

```py
from freeport_databricks.client.v1 import sql_from_query_template

query_parameters = {
    "metrics": {
        "column": "vin",
    },
}

sources = [
    {
        database_name: "carmax_reported",
        table_name: "inventory",
        catalog_name: "yd_production",
    },
]

query_string = "SELECT {{ parameters.metrics.column }} FROM {{ sources[0].full_name }}"

sql = sql_from_query_template(
    query_parameters=query_parameters,
    sources=sources, 
    query_string=query_string,
)

spark.sql(sql)
```

##### **Testing new query templates as a DataFrame** {#testing-new-query-templates-as-a-dataframe}

Preview the output of a new query template string with custom parameters by passing in a `query_string` to `df_from_query_template`. This will use the SQL queries generated by the template and return a dataframe. If multiple SQL queries are produced from the template, they are unioned together in the dataframe.

```py
​​from freeport_databricks.client.v1 import df_from_query_template

query_parameters = {
    "metrics": {
        "column": "vin",
    },
}

sources = [
    {
        database_name: "carmax_reported",
        table_name: "inventory",
        catalog_name: "yd_production",
    },
]

query_string = "SELECT {{ parameters.metrics.column }} FROM {{ sources[0].full_name }}"

df = df_from_query_template(
    query_parameters=query_parameters,
    sources=sources, 
    query_string=query_string,
)

display(df)
```

##### **Preview existing query template** {#preview-existing-query-template}

You can preview the output of an existing query template with your own parameters using this following snippet. This will return the SQL queries generated by the template and a dataframe generated using the SQL queries. 

```py
​​from freeport_databricks.client.v1 import df_from_query_template

query_parameters = {
    "metrics": {
        "column": "vin",
    },
}

sources = [
    {
        database_name: "carmax_reported",
        table_name: "inventory",
        catalog_name: "yd_production",
    },
]

df = df_from_query_template(
    query_parameters=query_parameters,
    sources=sources, 
    query_template=117,  # ID of query_template_version to use
)
```

##### **Save a query template** {#save-a-query-template}

To create a new query template, generate a Jinja template string based on the desired query to produce. Refer to the following code snippet as an example. **Adjust the arguments to fit your use case**.

```py
query_string = "SELECT * FROM {{ sources[0].full_name }}"

query_template = get_or_create_query_template(
   slug="ei_standard_rideshare_template",
   query_string=query_string,
   template_description="Template used for rideshare",
   version_description="Initial version",
)
```

This will create a new query template and query template version using the provided query string if one doesn’t exist. This code is safe to rerun and will return the existing query template version if there are no changes.

##### **Updating query template (creating a new version)** {#updating-query-template-(creating-a-new-version)}

To create a new query template version for an existing query template, use the following snippet. The slug passed in should be of the query template to be updated.

```py
query_string = "SELECT {{ parameters.metrics.column }} FROM {{ sources[0].full_name }}"

query_template = get_or_create_query_template(
   slug="ei_standard_rideshare_template",
   query_string=query_string,
   version_description="Updating version",
)
```

#### **Table Release Management** {#table-release-management}

##### **Staging, Prod, and Audit Catalogs** {#staging,-prod,-and-audit-catalogs}

##### **Release Lifecycle** {#release-lifecycle}

##### **When and which table / catalog to use**  {#when-and-which-table-/-catalog-to-use}

##### **Auto-Generated Tables** {#auto-generated-tables}

#### **QA Framework** {#qa-framework}

* Training on using the [Freeport QA Notebooks](https://yipitdata-product.cloud.databricks.com/?o=2498774132964769#notebook/2042909830704937/command/2042909830704938) can be found here  
* More docs coming soon

### **Recipe Book** {#recipe-book}

Quickstart guide for common tasks to do on Freeport. Use the code snippets below as references for managing deliverables with Freeport. **Make sure to adjust arguments** in the snippets to match your specific use case.

#### **Common Deliverable Tables** {#common-deliverable-tables}

##### **Batch Table** {#batch-table}

To create a batch table feed that matches an existing gold table, use the following code snippets. **Make sure to adjust the arguments to fit your delivery**.

```py
deliverable = get_or_create_deliverable(
    "AUTO UK Dealer Matching FP Demo",
    "yd_production.ydc_auto_uk_reported.dealer_matching_export_2023_07",
    output_table="yd_fp_corporate_staging.auto_deliverable_gold.dealer_matching_export",
    slug="auto_uk_dealer_matching_demo",
)

materialization = materialize_deliverable(
    deliverable["id"],
)
```

This will create an output table **yd\_fp\_corporate\_staging.auto\_uk\_deliverable\_gold.dealer\_matching\_export\_\_dmv\_\_000** if it doesn't already exist and add a new table version through the materialize\_deliverable function. 

The **\_\_dmv\_\_000** suffix at the end of the table name is expected and is an indicator of the schema version. It is incremented automatically if the schema changes in a backwards-incompatible way.

##### **Changes (Incremental) Daily Table** {#changes-(incremental)-daily-table}

To create an incremental daily table feed based on an existing gold table, use the following code snippets. **Make sure to adjust the title, input/output tables, and primary keys to fit your delivery**.

```py
deliverable = get_or_create_deliverable(
    "Edison Investor Purchase Feed",
    "yd_production.edison_receipts_gold.purchase_safe",
    output_table="yd_fp_ei_staging.ei_standard_feeds_gold.purchase_safe",
    output_table_type="changes_table",
    primary_key_field_names=["item_checksum"],
    slug="ei_std_purchase_feed",
)

materialization = materialize_deliverable(
    deliverable["id"],
)
```

This will create 2 output tables and 2 output tables in the audit catalog on release:

* yd\_fp\_ei\_staging.ei\_standard\_feeds\_gold.purchase\_safe\_\_dmv\_\_000  
* yd\_fp\_ei\_staging.ei\_standard\_feeds\_gold.purchase\_safe\_changes\_\_dmv\_\_000  
* yd\_fp\_ei\_audit.ei\_standard\_feeds\_gold.purchase\_safe\_\_dmv\_\_000  
* yd\_fp\_ei\_audit.ei\_standard\_feeds\_gold.purchase\_safe\_changes\_\_dmv\_\_000

There is a batch table that is always maintained, and the "changes" table will keep track of the daily rows that have been inserted, updated, or deleted compared to the last released materialization using Freeport. The `record_status` and `record_status_timestamp` columns are added automatically to these changes tables.

##### **Changes (Incremental) Monthly Table** {#changes-(incremental)-monthly-table}

To create an incremental monthly table feed based on an existing gold table, use the following code snippets. **Make sure to adjust the title, input/output tables, and primary keys to fit your delivery**.

```py
deliverable = get_or_create_deliverable(
    "Corporate Edison Receipts - McD Purchase Items Gold Monthly",
    "yd_production.corporate_food_delivery_grocery_gold",
    output_table="yd_fp_corporate_staging.fp_corporate_mcd_gold.purchase",
    output_table_type="changes_table",
    primary_key_field_names=["item_checksum"],
    slug="corp_mcd_ereceipts_items_monthly",
    staged_retention_days=35,
)

materialization = materialize_deliverable(
    deliverable["id"],
)
```

This will create 2 output tables and 2 output tables in the audit catalog on release:

* yd\_fp\_corporate\_staging.fp\_corporate\_mcd\_gold.purchase\_\_dmv\_\_000  
* yd\_fp\_corporate\_staging.fp\_corporate\_mcd\_gold.purchase\_changes\_\_dmv\_\_000  
* yd\_fp\_corporate\_staging.fp\_corporate\_mcd\_gold.purchase\_\_dmv\_\_000  
* yd\_fp\_corporate\_staging.fp\_corporate\_mcd\_gold.purchase\_changes\_\_dmv\_\_000

There is a batch table that is always maintained, and the "changes" table will keep track of the month over month rows that have been inserted, updated, or deleted compared to the last released materialization using Freeport. The `record_status` and `record_status_timestamp` columns are added automatically to these changes tables.

An important flag to set is the `staged_retention_days`, this should be set to be more than 30 days for a monthly, giving a little buffer in case a delivery is delayed for some reason. This controls the data retention period for staging tables generated in Freeport.

When running this in production, set up a workflow to run `get_or_create_deliverable` and `materialize_deliverable` on a monthly schedule. Clients will then receive a delivery each month with the changes from the last month's delivery. It's not recommended to run this more frequently as that would result in an intra-month delivery to clients.

##### **Changes (Incremental) Weekly Table** {#changes-(incremental)-weekly-table}

To create an incremental weekly table feed based on an existing gold table, use the following code snippets. **Make sure to adjust the title, input/output tables, and primary keys to fit your delivery**.

```py
deliverable = get_or_create_deliverable(
    "Corporate Edison Receipts - McD Purchase Items Gold Weekly",
    "yd_production.corporate_food_delivery_grocery_gold",
    output_table="yd_fp_corporate_staging.fp_corporate_mcd_gold.purchase",
    output_table_type="changes_table",
    primary_key_field_names=["item_checksum"],
    slug="corp_mcd_ereceipts_items_weekly",
    staged_retention_days=10,
)

materialization = materialize_deliverable(
    deliverable["id"],
)
```

This will create 2 output tables and 2 output tables in the audit catalog on release:

* yd\_fp\_corporate\_staging.fp\_corporate\_mcd\_gold.purchase\_\_dmv\_\_000  
* yd\_fp\_corporate\_staging.fp\_corporate\_mcd\_gold.purchase\_changes\_\_dmv\_\_000  
* yd\_fp\_corporate\_staging.fp\_corporate\_mcd\_gold.purchase\_\_dmv\_\_000  
* yd\_fp\_corporate\_staging.fp\_corporate\_mcd\_gold.purchase\_changes\_\_dmv\_\_000

There is a batch table that is always maintained, and the "changes" table will keep track of the week over week rows that have been inserted, updated, or deleted compared to the last released materialization using Freeport. The `record_status` and `record_status_timestamp` columns are added automatically to these changes tables.

An important flag to set is the `staged_retention_days`, this should be set to be more than 10 days for a weekly, giving a little buffer in case a delivery is delayed for some reason. This controls the data retention period for staging tables generated in Freeport.

When running this in production, set up a workflow to run `get_or_create_deliverable` and `materialize_deliverable` on a weekly schedule. Clients will then receive a delivery each week with the changes from the last week's delivery. It's not recommended to run this more frequently as that would result in an intra-week delivery to clients.

##### **PIT Table** {#pit-table}

To create a Point-in-Time append-only table feed that matches an existing gold table, use the following code snippets. **Make sure to adjust the arguments to fit your delivery**.

```py
deliverable = get_or_create_deliverable(
    "US Brands Granular Feed Monthly",
    "yd_production.us_brands_gold.gmv_monthly",
    output_table="yd_fp_investor_staging.us_brands_deliverable_gold.gmv_monthly",
    output_table_type="pit_append_table",
    release_timestamp_field_name="publication_timestamp",
    slug="us_brands_granular_monthly",
)

materialization = materialize_deliverable(
    deliverable["id"],
)
```

This will create 1 output table in the staging catalog and 2 in the audit catalog at release time:

* yd\_fp\_investor\_staging.us\_brands\_deliverable\_gold.gmv\_monthly\_\_dmv\_\_000  
* yd\_fp\_investor\_audit.us\_brands\_deliverable\_gold.gmv\_monthly\_\_dmv\_\_000  
* yd\_fp\_investor\_audit.us\_brands\_deliverable\_gold.gmv\_monthly\_pit\_append\_\_dmv\_\_000

There is a batch table that is always maintained in the staging and audit catalogs, and a "PIT append" table will keep appending each released materialization using Freeport. In both audit tables, `release_timestamp_field_name` , in this case `publication_timestamp` , will be updated by Freeport to reflect the time of release automatically. Because of this, `release_timestamp_field_name` is expected to exist in the output table schema by the user. The values of the field can be anything of a **timestamp** type since Freeport will replace it. `CURRENT_TIMESTAMP()` is a recommended placeholder value.

##### **Internal-Only Table** {#internal-only-table}

Freeport can be used to build deliverable tables that are not meant for clients. This is useful if you need to prepare tables for other YD teams to consume. The core difference is that the deliverable will not exist in Dispatch. 

```py
from freeport_databricks.client.v1 import (
    get_or_create_deliverable,
    materialize_deliverable
    release_materialization,
)

deliverable = get_or_create_deliverable(
    "Internal-Only Edison Investor Purchase Feed",
    "yd_production.edison_receipts_gold.purchase_safe",
    output_table="yd_fp_ei_staging.ei_internal_gold.purchase_safe",
    slug="ei_internal_purchase",
    # Block the deliverable from existing in Dispatch
    enable_in_dispatch=False,
)

# Defaults to releasing materialization as soon as possible (release_on_success=True)
materialization = materialize_deliverable(
    deliverable["id"],
)

```

#### **Releasing Deliverables** {#releasing-deliverables}

##### **Disabling Release** {#disabling-release}

To materialize a deliverable without sending it to clients, set `release_on_success=False`. 

```py
materialization = materialize_deliverable(
    deliverable["id"],
    release_on_success=False,
)
```

This will create a new table version for the output table but not tell Dispatch to send it to clients. This materialization can be delivered later via Dispatch using `release_materialization`.

##### **Scheduling Release** {#scheduling-release}

To materialize a deliverable and send it to clients at a specific time in the future, set `release_timestamp` with a valid python datetime (UTC time). 

```py
materialization = materialize_deliverable(
    deliverable["id"],
    release_timestamp=datetime(2023, 10, 1),
)
```

Once the materialization is processed, Dispatch will be told to release it to subscribed clients at 2023-10-01 at UTC time. 

##### **Release Disabled Materializations** {#release-disabled-materializations}

If a materialization was created but not delivered right away (`release_on_success=False`), it can be released on-demand using `release_materialization`: 

```py
from freeport_databricks.client.v1 import release_materialization

release_materialization(
    materialization["id"],
)
```

`release_materialization` also accepts scheduling materializations for the future. Set `release_timestamp` as an argument in the function with a valid python datetime (UTC time). 

##### **Disabled Scheduled Materializations** {#disabled-scheduled-materializations}

If a materialization was scheduled for release in the future, it can be disabled between the current time up to a few minutes before the release is scheduled. To do this, use `release_materialization` with `allow_release=False`.

```py
from freeport_databricks.client.v1 import release_materialization

release_materialization(
    materialization["id"],
    allow_release=False,
)
```

Subsequent calls of `release_materialization` setting `allow_release=True` will reschedule the release. The schedule can also be updated at this time with `release_timestamp`.

##### **List Materializations** {#list-materializations}

It may be helpful to see all materializations that have not been released, to do this use the `list_deliverable_materializations` function. It returns a spark dataframe that can then be displayed in notebooks.

```py
from freeport_databricks.client.v1 import list_deliverable_materializations

df = list_deliverable_materializations(
    deliverable["id"],
)

display(df)
```

Set `include_released_materializations=True` in the function to have the dataframe include materializations released in the past.

#### **Customizing Content** {#customizing-content}

##### **Filtering Content**  {#filtering-content}

When using the "Star" query template (default template for `get_or_create_deliverable`), it is possible to filter the input table data down based on allowed or disallowed values in specific fields. This is useful for generating various "cuts" of a singular input table. The behavior is controlled via `query_parameters` argument in `get_or_create_deliverable`.

For example, the following query parameters to down US brands data based on specific tickers and brands.

```py
deliverable = get_or_create_deliverable(
    "US Brands Filtered",
    "yd_production.us_brands_reported.week_and_month_products_grouped",
    output_table="yd_fp_investor_staging.us_brands_deliverable_gold.merchants_cut",
    query_parameters={
        "condition_groups": [
            {
                "conditions": [
                    {
                        "condition_operator": "NOT IN",
                        "field_name": "ticker",
                        "values": ["AMZN"],
                    },
                    {
                        "field_name": "brand_reported",
                        "values": ["Nike", "Adidas", "M&M's"],
                    },
                ]
            }
        ]
    },
)
```

The resulting SQL query for the deliverable output table will be:

```
SELECT
    *
FROM
    yd_production.us_brands_reported.week_and_month_products_grouped@v30
WHERE
    (
        TRIM(LOWER(ticker)) NOT IN (
            'amzn'
        )
    )
    AND (
        TRIM(LOWER(brand_reported)) IN (
            'nike',
            'adidas',
            'm&m\'s'
        )
    )
```

Conditions and condition\_groups can be as complex as needed to construct filtering logic. Groups are evaluated together using `AND` expressions. Conditions within a group can be evaluated as an `AND` (default) or as an `OR` operation using the condition\_operator argument within a group. Values within a condition are used as a case insensitive allow-list or a deny-list using `IN` or `NOT IN` as the condition\_operator within a condition.

**Important**: Whenever filter conditions need to be updated, adjust the query parameters and re-run `get_or_create_deliverable` to update the deliverable configuration. The filters will be applied the next time the deliverable is materialized.

### **FAQs** {#faqs}

#### **Common Errors** {#common-errors}

##### **Running into schema errors at materialization time** {#running-into-schema-errors-at-materialization-time}

* Update the deliverable by running `get_or_create_deliverable`, and pass in a `description` argument equal to some string (*should include a quick comment of the changes involved*), along with any other configuration changes needed, if any  
* This will refresh the deliverable, afterwards re-materialize the data with `materialize_deliverable`   
* Bumping the deliverable like this can be a helpful tactic whenever the deliverable gets into a bad state

#### **Running in Production** {#running-in-production}

##### **Where to add Freeport code?** {#where-to-add-freeport-code?}

* Freeport code for managing deliverables should be added to the end of a databricks workflow where the input tables are generated  
  * A separate workflow can be used if   
  * If more ad-hoc scheduling is required, set `release_on_succcess=False` in materialize\_deliverable and use release\_materialization as needed  
* Freeport function calls do not require a large cluster so use the smallest cluster size available if that is the only code being executed in a workflow

##### **Are tables ready when** `materialize_deliverable` **returns?** {#are-tables-ready-when-materialize_deliverable-returns?}

* If `wait_for_completion=True` (default), then the materialized table will be ready when the function returns.   
  * The function output will log that it is repeatedly polling the Freeport API until the table creation is finished.  
* If `wait_for_completion=False` , then the function will not wait for table completion and returns immediately after Freeport accepts the materialization API request.   
  * The table creation will happen asynchronously and can be used at later point in time  
  * If `release_on_success=True`, the table will still be released as soon as the table creation is completed  
* As a best practice, production workflows that materialize data on recurring schedules should set `wait_for_completion=False` so that the workflow is not running for an extended period of time and generating costs. Freeport will run separate clusters to materialize and release the table automatically.

##### **When are Freeport tables sent to clients?** {#when-are-freeport-tables-sent-to-clients?}

* For Freeport deliverables enabled in Dispatch (default behavior), the content will be released to clients after the following criteria are met:  
  1. The deliverable must be materialized successfully  
  2. The materialization must have `allow_release=True` and a valid release\_timestamp at some future time. The default values for these fields are `allow_release=True` and a `release_timestamp=<datetime.utcnow>` (i.e. release asap).  
  3. At or after that `release_timestamp`, Dispatch will communicate with Freeport to start the release. This happens automatically and the release status and client fulfillments can be tracked in the Dispatch UI

##### **What cluster is used for deliverables and materializations?** {#what-cluster-is-used-for-deliverables-and-materializations?}

* Freeport manages cluster configurations automatically for the user. In fact most operations for Freeport happen outside of the cluster you would be using on Databricks.   
  * When generating deliverables using `get_or_create_deliverable`, the separate interactive cluster is used. There are only lightweight metadata operations executed in this step so a dedicated cluster is not needed.  
  * For materialization and audit version events, a dedicated job cluster is used. Freeport calculates the job cluster size based on the input table size(s) and complexity of the transformations involved to produce the output table.   
    * Once the materialization or audit table is finished processing the job cluster is terminated (similar to databricks workflows)  
    * The job cluster being used can be seen when visiting the `databricks_job_run_url` for these FP events.  
    * For very small workloads, an interactive cluster is used given it executes quickly without the typical 5 min job cluster boot time.  
* Cost components set on the databases of the input tables for a deliverable are automatically propagated to the FP job clusters to allocate deliverable costs to the appropriate product.

