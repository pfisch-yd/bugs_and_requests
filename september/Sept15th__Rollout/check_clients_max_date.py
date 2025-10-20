from pyspark.sql.functions import col
from datetime import datetime

def check_max_order_date(client_name, expected_date="2025-08-31"):
    """
    Checks the maximum date (max order_date) from a client's filter_items table

    Args:
        client_name (str): Client name
        expected_date (str): Expected date in YYYY-MM-DD format

    Returns:
        dict: Information about the client and its maximum date
    """
    version = "_v38"
    table_name = client_name + version + "_filter_items"

    # Possible schemas where tables might be located
    schemas_to_check = [
        "ydx_internal_analysts_gold",
        "ydx_prospect_analysts_gold",
        "ydx_external_analysts_gold"
    ]

    max_date = None
    schema_used = None
    status = "ERROR"

    # Try to find the table in different schemas
    for schema in schemas_to_check:
        try:
            full_table_name = f"{schema}.{table_name}"
            result = spark.sql(f"SELECT MAX(order_date) as max_date FROM {full_table_name}")
            max_date_raw = result.collect()[0]['max_date']

            if max_date_raw:
                # Convert to string in YYYY-MM-DD format
                if hasattr(max_date_raw, 'strftime'):
                    max_date = max_date_raw.strftime('%Y-%m-%d')
                else:
                    max_date = str(max_date_raw)[:10]

                schema_used = schema

                # Check if the date is correct
                if max_date == expected_date:
                    status = "OK"
                else:
                    status = f"WRONG_DATE (expected: {expected_date})"
                break

        except Exception as e:
            # Table not found in this schema, continue searching
            continue

    if max_date is None:
        status = "TABLE_NOT_FOUND"
        max_date = "N/A"

    return {
        'client': client_name,
        'table_name': table_name,
        'schema': schema_used,
        'max_order_date': max_date,
        'status': status
    }

# Search for clients that have run recently
print("Searching for clients that have executed jobs recently...")
df_clients = spark.sql("""
SELECT DISTINCT
    user,
    get_json_object(args, '$[0]') as client_name,
    end_timestamp
FROM yd_production.data_engineering_telemetry_observed.ydbu_functions_telemetry
WHERE name like "run_everything%"
    AND end_timestamp > "2025-09-10T20:38:26.203+00:00"
    AND user NOT IN ("pfisch@yipitdata.com", "dkatz@yipitdata.com")
    AND get_json_object(args, '$[0]') IS NOT NULL
    AND get_json_object(args, '$[0]') != ''
ORDER BY end_timestamp DESC
""")

# Collect unique clients
clients_list = df_clients.select("client_name").distinct().collect()
clients = [row.client_name for row in clients_list if row.client_name]

print(f"Found {len(clients)} unique clients to verify")
print("="*60)

# List to store results
results = []

# Check each client
for i, client in enumerate(clients):
    print(f"Checking {i+1}/{len(clients)}: {client}")

    result = check_max_order_date(client, "2025-08-31")
    results.append(result)

    # Show immediate result
    print(f"  → Schema: {result['schema'] or 'N/A'}")
    print(f"  → Max Date: {result['max_order_date']}")
    print(f"  → Status: {result['status']}")
    print("-" * 40)

print("\n" + "="*60)
print("FINAL SUMMARY:")
print("="*60)

# Counters for statistics
ok_count = 0
wrong_date_count = 0
not_found_count = 0
error_count = 0

for result in results:
    status_icon = "✅" if result['status'] == "OK" else "❌"
    print(f"{status_icon} {result['client']:30} | {result['max_order_date']:12} | {result['status']}")

    if result['status'] == "OK":
        ok_count += 1
    elif "WRONG_DATE" in result['status']:
        wrong_date_count += 1
    elif result['status'] == "TABLE_NOT_FOUND":
        not_found_count += 1
    else:
        error_count += 1

print("="*60)
print("STATISTICS:")
print(f"✅ OK (correct date): {ok_count}")
print(f"📅 Incorrect date: {wrong_date_count}")
print(f"🔍 Table not found: {not_found_count}")
print(f"❌ Other errors: {error_count}")
print(f"📊 Total clients: {len(results)}")