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

def check_if_table_properties_where_added(demo_name, sandbox_schema, prod_schema):
    # demo_name = client
    version = "_v38"
    demo_name = demo_name + version

    try:
        max_date = spark.sql(f"""
            select max(order_date) from {sandbox_schema}.{demo_name}_filter_items
            """)
        max_date = max_date.collect()[0][0]
        max_date = max_date[:10]
    except:
        max_date = "-- problem --"

    return max_date

    should_have_tariffs = []
for i in range(0,len_clients):
    client_name = distinct_values[i]
    demo_name = client_name
    client_row = df.filter(df[1] == demo_name) \
               .orderBy(col("timestamp").desc()) \
               .limit(1)
    client_row_data = client_row.collect()[0]
    sandbox_schema = client_row_data[3]
    prod_schema = client_row_data[4]
    if prod_schema == "ydx_prospect_analysts_gold":
        max_date = "-- demo --"
    else:
        if prod_schema == "ydx_prospect_analysts_gold":
            max_date = "-- demo --"
        else:
            max_date = check_if_table_properties_where_added(demo_name, sandbox_schema, prod_schema)
    
    print("{}:{} ===> {} ".format(i, client_name, max_date))

print(len_clients)
print(should_have_tariffs)