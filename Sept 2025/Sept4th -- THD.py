# Databricks notebook source
# first add 2 columns to thd source table

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

sku_detail = spark.sql(f"""
    SELECT
        *,
        1 as leia_panel_flag_source,
        1 as factor_age
    FROM ydx_thd_analysts_silver.home_depot_filter_items_dash
""")

create_table('ydx_internal_analysts_gold', 'homedepot_v38_sourcetable', sku_detail, overwrite=True)
