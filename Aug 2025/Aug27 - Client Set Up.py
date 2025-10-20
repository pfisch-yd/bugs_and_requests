# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC [ddd](https://yipitdata5.atlassian.net/wiki/spaces/PE/pages/3944546323/Adding+Pricing+and+Promotions+to+Workbooks)

# COMMAND ----------

# Jira Ticket: https://yipitdata5.atlassian.net/browse/CS-375

import logging

import time
from portal.apps.sigma.models import SigmaWorkbook, SigmaWorkbookPage
from portal.apps.sigma.sigma_api import SigmaAPI

from django.db import transaction

# COMMAND ----------

pip install portal.apps.sigma.models

# COMMAND ----------

pip install portal

# COMMAND ----------

# Jira Ticket: https://yipitdata5.atlassian.net/browse/CS-375

import logging

import time
from portal.apps.sigma.models import SigmaWorkbook, SigmaWorkbookPage
from portal.apps.sigma.sigma_api import SigmaAPI

from django.db import transaction


OLD_SIGMA_WORKBOOK_ID = "DHDOPzA3AlrS8fSykPtBE"
OLD_FILTER_SUFFIX = "_v37"

NEW_SIGMA_WORKBOOK_ID = "7g8i8sasACf2DR0DNC4eMl"
NEW_EMBED_PATH = "https://app.sigmacomputing.com/embed/3BxmPw4lNdqQBgGXcuBQSv"
NEW_FILTER_SUFFIX = "_v38"


def run(dry_run: bool = True):

    start = time.time()
    workbooks = SigmaWorkbook.objects.prefetch_related("pages").filter(
        sigma_workbook_id=OLD_SIGMA_WORKBOOK_ID
    )

    print(f"{workbooks.count()} Sigma Workbooks (V37) with workbook ID of '{OLD_SIGMA_WORKBOOK_ID}' will be updated to V38")
    print(f"dry run: {dry_run}")

    for workbook in workbooks.iterator(chunk_size=500):
        print(f"Working on workbook: {str(workbook)}")

        # 1. Change the sigma_workbook_id
        workbook.sigma_workbook_id = NEW_SIGMA_WORKBOOK_ID

        # 2. Change the embed path
        workbook.embed_path = NEW_EMBED_PATH

        # 3. Update root-client from v37 to v38
        if root_client := workbook.default_filters.get("root-client"):
            if str(root_client).endswith(OLD_FILTER_SUFFIX):
                root_client = (
                    root_client.split(OLD_FILTER_SUFFIX)[0] + NEW_FILTER_SUFFIX
                )
                workbook.default_filters["root-client"] = root_client

        # 4. 5. and 6. Sync Workbook pages from Sigma:
        # As we're accessing the directly the logic to sync the sigma pages, we don't need to
        # set the 'disable_page_override' to False.
        sigma_client = SigmaAPI()

        is_production_tag = workbook.embed_path.endswith("/tag/Production")
        is_staging_tag = workbook.embed_path.endswith("/tag/Staging")

        print("\t* Retrieving pages from Sigma")
        sigma_workbook_pages = sigma_client.get_all_workbook_pages(
            workbook_id=workbook.sigma_workbook_id,
            is_production_tag=is_production_tag,
            is_staging_tag=is_staging_tag,
        )

        # Takes a snapshot of the current pages state.
        print("\t* Creating pages snapshot")
        pages_snapshot = {
            page.sigma_node_id: page
            for page in workbook.pages.all().iterator(chunk_size=500)
        }

        # Will hold the pages instances for a later bulk_create
        synced_pages = {}

        print("\t* Creating new pages (lazy)")
        for order, workbook_page in enumerate(sigma_workbook_pages):
            active = not workbook_page.get("hidden", False)
            workbook_page_id = workbook_page.get("pageId")
            name = workbook_page.get("name")

            if not workbook_page_id or not name:
                continue

            # Set the active attribute based on the old page snapshot value
            if page_snapshot := pages_snapshot.get(workbook_page_id):
                active = page_snapshot.active

            synced_pages[workbook_page_id] = SigmaWorkbookPage(
                active=active,
                display_name=name,
                order=order,
                sigma_node_id=workbook_page_id,
                workbook=workbook,
            )

        # 7. Set the ‘active’ checkbox to TRUE for the Shopper Profile page,
        # NodeID o7S8DIFKSX,  only if the Loyalty page, nodeId -iIzWX3nbH
        # is also TRUE. Else, set FALSE.
        try:
            shopper_profile_page = synced_pages["o7S8DIFKSX"]
            loyalty_page = synced_pages[ "-iIzWX3nbH"]
            shopper_profile_page.active = True if loyalty_page.active else False
        except KeyError:
            # log something here
            pass

        # 8. Set the ‘active’ checkbox to TRUE for the Tariff Impact page,
        # NodeID PJi-2ytQ7O,  only if the Pricing module, nodeId NmYpXaOLX_
        # is also TRUE. Else, set FALSE.
        try:
            tarrif_impact_page = synced_pages["PJi-2ytQ7O"]
            pricing_module = synced_pages["NmYpXaOLX_"]
            tarrif_impact_page.active = True if pricing_module.active else False
        except KeyError:
            # log something here
            pass

        # 9. Set the active flag to False for the Shopper Leakage module,
        # nodeid 4nGYIFSLdM, for all accounts. We are discontinuing this module.
        try:
            shopper_leakage_module = synced_pages["4nGYIFSLdM"]
            shopper_leakage_module.active = False
        except KeyError:
            # log something here
            pass
        
        if not dry_run:
            print("\t* APPLYING CHANGES")
            with transaction.atomic():
                workbook.save()
                workbook.pages.all().delete()
                SigmaWorkbookPage.objects.bulk_create(synced_pages.values(), batch_size=500)
    
    end = time.time()
    duration = end - start

    print(f"PROCESS FINISHED IN: {duration} seconds")


# COMMAND ----------




def run():
    accounts = []

    for account_name in accounts:
        print(f"Processing account: {account_name}")
        account = CorporateAccount.objects.get(name=account_name)
        workbook = SigmaWorkbook.objects.get(
            product=account.product,
            sigma_workbook_id="<REPLACE_WITH_WORKBOOK_TEMPLATE_ID:DHDOPzA3AlrS8fSykPtBE>",
        )

        last_page = workbook.pages.order_by("order").last()

        existing_pricing_page = workbook.pages.filter(
            sigma_node_id="<REPLACE_WITH_PRICING_PAGE_NODE_ID:NmYpXaOLX_>"
        ).first()
        if existing_pricing_page:
            # Don't need to worry about order for teams
            existing_pricing_page.order = last_page.order - 3
            existing_pricing_page.save()
            print('"Pricing" page already exists, skipping creation.')
        else:
            print('Creating "Pricing" page.')
            SigmaWorkbookPage.objects.create(
                active=False,
                display_name="Pricing",
                order=last_page.order - 3,
                sigma_node_id="<REPLACE_WITH_PRICING_PAGE_NODE_ID:NmYpXaOLX_>",
                workbook=workbook,
            )

        existing_promo_page = workbook.pages.filter(
            sigma_node_id="<REPLACE_WITH_PROMOTION_PAGE_NODE_ID:_53yt0tzgQ>"
        ).first()
        if existing_promo_page:
            # Don't need to worry about order for teams
            existing_promo_page.order = last_page.order - 2
            existing_promo_page.save()
            print('"Promotions" page already exists, skipping creation.')
        else:
            print('Creating "Promotions" page.')
            SigmaWorkbookPage.objects.create(
                active=False,
                display_name="Promotions",
                order=last_page.order - 2,
                sigma_node_id="<REPLACE_WITH_PROMOTION_PAGE_NODE_ID:_53yt0tzgQ>",
                workbook=workbook,
            )


run()
