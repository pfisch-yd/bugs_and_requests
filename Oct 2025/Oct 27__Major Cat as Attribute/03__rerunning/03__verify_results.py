# Databricks notebook source
# MAGIC %md
# MAGIC # Verify Results: Analyze Output Tables
# MAGIC
# MAGIC **Purpose**: Use investigation functions from 02__investigating to analyze the output tables
# MAGIC
# MAGIC **Process**:
# MAGIC 1. Import analysis functions from 02__investigating
# MAGIC 2. Run comprehensive analysis on _sku_time_series_test tables
# MAGIC 3. Compare input (_filter_items) vs output (_sku_time_series_test)
# MAGIC 4. Determine root cause

# COMMAND ----------

# DBTITLE 1,Setup and Imports
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.getOrCreate()

# Test catalog and schema
TEST_CATALOG = "yd_sensitive_corporate"
TEST_SCHEMA = "ydx_internal_analysts_sandbox"  # Adjust if needed

# COMMAND ----------

def get_client_config(demo_name):
    """
    Retrieve client configuration from data_solutions.client_info table
    Returns a dictionary with sandbox_schema and other relevant config
    """
    try:
        # Try to query the client_info table
        client_info_df = spark.sql(f"""
            SELECT *
            FROM data_solutions_sandbox.corporate_clients_info
            WHERE demo_name = '{demo_name}'
            OR LOWER(demo_name) = LOWER('{demo_name}')
        """)

        # Convert to dictionary
        config = client_info_df.first().asDict()
        print(f"✓ Configuration found for {demo_name}")
        print(f"  Sandbox Schema: {config.get('sandbox_schema', 'N/A')}")

        return config

    except Exception as e:
        print(f"❌ Error retrieving config for {demo_name}: {str(e)}")
        return None

# COMMAND ----------

# DBTITLE 1,Function: Analyze Product Attributes (From 02__investigating)
def analyze_product_attributes(demo_name, table_suffix):
    """
    Analyze the product_attributes column in _filter_items table

    Returns:
    - Data type
    - Fill rate (% non-null)
    - Distinct value count
    - Top 10 most frequent values with distribution
    """

    config = get_client_config(demo_name)
    if config is None:
        print(f"Cannot proceed without configuration for {demo_name}")
        return None

    # Construct table name
    sandbox_schema = "ydx_internal_analysts_sandbox"
    prod_schema = "ydx_internal_analysts_gold"
    if table_suffix == "filter_items":
        table_name = f"{sandbox_schema}.{demo_name}_v38_filter_items"
    else:
        table_name = f"{prod_schema}.{demo_name}_v38_sku_time_series"

    print(f"\n{'='*80}")
    print(f"ANALYZING: {table_name}")
    print(f"{'='*80}\n")

    try:
        # Check if table exists
        table_df = spark.table(table_name)
        total_records = table_df.count()

        print(f"✓ Table found: {table_name}")
        print(f"  Total records: {total_records:,}")

    except Exception as e:
        print(f"❌ Table not found: {table_name}")
        print(f"   Error: {str(e)}")
        return None

    # 1. Check if product_attributes column exists
    columns = table_df.columns
    has_product_attrs = 'product_attributes' in columns

    print(f"\n{'─'*80}")
    print("1. COLUMN EXISTENCE")
    print(f"{'─'*80}")
    print(f"  product_attributes column exists: {has_product_attrs}")

    if not has_product_attrs:
        print("  ⚠️  product_attributes column does not exist in this table")
        return {
            'demo_name': demo_name,
            'table_name': table_name,
            'column_exists': False
        }

    # 2. Get data type
    print(f"\n{'─'*80}")
    print("2. DATA TYPE")
    print(f"{'─'*80}")

    schema_field = [f for f in table_df.schema.fields if f.name == 'product_attributes'][0]
    data_type = str(schema_field.dataType)
    print(f"  Data Type: {data_type}")

    # 3. Calculate fill rate
    print(f"\n{'─'*80}")
    print("3. FILL RATE")
    print(f"{'─'*80}")

    null_count = table_df.filter(F.col('product_attributes').isNull()).count()
    non_null_count = total_records - null_count
    fill_rate = (non_null_count / total_records * 100) if total_records > 0 else 0

    print(f"  Non-null records: {non_null_count:,}")
    print(f"  Null records: {null_count:,}")
    print(f"  Fill rate: {fill_rate:.2f}%")

    # 4. Convert to JSON string for analysis
    print(f"\n{'─'*80}")
    print("4. DISTINCT VALUES COUNT")
    print(f"{'─'*80}")

    # Convert product_attributes to JSON string for comparison
    attrs_as_json = table_df.filter(F.col('product_attributes').isNotNull()) \
        .select(F.to_json('product_attributes').alias('attrs_json'))

    distinct_count = attrs_as_json.select('attrs_json').distinct().count()
    print(f"  Distinct product_attributes values: {distinct_count:,}")

    # 5. Get top 10 most frequent values
    print(f"\n{'─'*80}")
    print("5. TOP 10 MOST FREQUENT VALUES")
    print(f"{'─'*80}")

    top_values = attrs_as_json.groupBy('attrs_json') \
        .agg(F.count('*').alias('count')) \
        .orderBy(F.desc('count')) \
        .limit(10)

    top_values_pd = top_values.toPandas()
    top_values_pd['percentage'] = (top_values_pd['count'] / non_null_count * 100).round(2)

    print(top_values_pd.to_string(index=False))

    # 6. Check for "Major Cat" or similar category fields in the JSON
    print(f"\n{'─'*80}")
    print("6. CHECKING FOR CATEGORY FIELDS IN PRODUCT ATTRIBUTES")
    print(f"{'─'*80}")

    # Look for common category-related keywords in the JSON
    category_keywords = ['major_cat', 'Major Cat', 'major cat', 'category', 'Category']

    for keyword in category_keywords:
        count_with_keyword = attrs_as_json.filter(
            F.col('attrs_json').contains(keyword)
        ).count()

        if count_with_keyword > 0:
            pct = (count_with_keyword / non_null_count * 100) if non_null_count > 0 else 0
            print(f"  ⚠️  Found '{keyword}' in {count_with_keyword:,} records ({pct:.2f}%)")

            # Show sample records with this keyword
            print(f"\n  Sample records containing '{keyword}':")
            sample_records = table_df.filter(F.to_json('product_attributes').contains(keyword)) \
                .select('major_cat', 'web_description', 'product_attributes') \
                .limit(3)
            sample_records.show(truncate=False)
        else:
            print(f"  ✓ No records contain '{keyword}'")

    """
    # 7. Analyze structure of product_attributes
    print(f"\n{'─'*80}")
    print("7. PRODUCT ATTRIBUTES STRUCTURE ANALYSIS")
    print(f"{'─'*80}")

    # Get sample records to understand structure
    sample_df = table_df.filter(F.col('product_attributes').isNotNull()) \
        .select('major_cat', 'sub_cat', 'minor_cat', 'web_description', 'product_attributes') \
        .limit(5)

    print("\n  Sample records:")
    sample_df.show(truncate=False)

    # Try to extract keys from the product_attributes if it's a map/struct
    try:
        if 'map' in data_type.lower() or 'struct' in data_type.lower():
            print("\n  Attempting to extract attribute keys...")

            # For map type, get all keys
            keys_df = table_df.filter(F.col('product_attributes').isNotNull()) \
                .select(F.explode(F.map_keys('product_attributes')).alias('attr_key')) \
                .groupBy('attr_key') \
                .agg(F.count('*').alias('count')) \
                .orderBy(F.desc('count'))

            print("\n  All attribute keys found:")
            keys_df.show(50, truncate=False)
    except Exception as e:
        print(f"  Could not extract keys: {str(e)}")
    """

    print(f"\n{'='*80}")
    print(f"ANALYSIS COMPLETE: {demo_name}")
    print(f"{'='*80}\n")

    most_frequent1 = top_values_pd
    most_frequent1["demo_name"] = demo_name
    most_frequent1["table analysed"] = "_filter_items"
    most_frequent1 = most_frequent1.to_csv(f"most_frequent__filter_items_{demo_name}.csv")

    return {
        'demo_name': demo_name,
        'table_name': table_name,
        'column_exists': True,
        'data_type': data_type,
        'total_records': total_records,
        'non_null_count': non_null_count,
        'fill_rate': fill_rate,
        'distinct_count': distinct_count,
        'top_values': top_values_pd
    }

# COMMAND ----------

# DBTITLE 1,Client Configuration
clients_config = {
    'weber': {'demo_name': 'weber'},
    'werner': {'demo_name': 'werner'},
    'odele': {'demo_name': 'odele'}
}

# COMMAND ----------

# DBTITLE 1,Analyze Input Tables (_filter_items)
print("\n\n")
print("#" * 100)
print("# PART 1: ANALYZING INPUT TABLES (_filter_items)")
print("#" * 100)

input_results = {}

for client_name, config in clients_config.items():
    demo_name = config['demo_name']
    input_table = f"{TEST_CATALOG}.{TEST_SCHEMA}.{demo_name}_v38_filter_items"

    result = analyze_product_attributes(demo_name, "filter_items")

    input_results[client_name] = result

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC ####################################################################################################
# MAGIC # PART 1: ANALYZING INPUT TABLES (_filter_items)
# MAGIC ####################################################################################################
# MAGIC ✓ Configuration found for weber
# MAGIC   Sandbox Schema: ydx_weber_analysts_silver
# MAGIC
# MAGIC ================================================================================
# MAGIC ANALYZING: ydx_internal_analysts_sandbox.weber_v38_filter_items
# MAGIC ================================================================================
# MAGIC
# MAGIC ✓ Table found: ydx_internal_analysts_sandbox.weber_v38_filter_items
# MAGIC   Total records: 24,393,525
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 1. COLUMN EXISTENCE
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   product_attributes column exists: True
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 2. DATA TYPE
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   Data Type: VariantType()
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 3. FILL RATE
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   Non-null records: 4,486,021
# MAGIC   Null records: 19,907,504
# MAGIC   Fill rate: 18.39%
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 4. DISTINCT VALUES COUNT
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   Distinct product_attributes values: 5,741
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 5. TOP 10 MOST FREQUENT VALUES
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC                                                                                                                                                                                                                                                                                                                                                                                                                                                    attrs_json  count  percentage
# MAGIC                                                         {"assembled_depth_in":"12.3 in","assembled_height_in":"17.8 in","assembled_width_in":"12.3 in","number_of_cylinders_included":"1","outdoor_living_product_type":"Liquid Propane","pre_filled":"Yes","product_depth_in":"12.2 in","product_diameter_in":"12 in","product_height_in":"18 in","product_weight_lb":"15 lb","product_width_in":"12.2 in","tank_capacity_lb":"15 lb","tank_weight_lb":"31"} 318422        7.10
# MAGIC {"CA Residents: Prop 65 Warning(s)":"Yes","Color/Finish Family":"Gray","Contains Overfill Protection Device":"Yes","Depth (Inches)":"12","Diameter (Inches)":"15","Height (Inches)":"18","Length (Inches)":"15","Manufacturer Color/Finish":"Gray","Package Quantity":"1","Primary Material":"Steel","Type":"Propane tank exchange","UNSPSC":"24111800","Unit of Measure":"Pound(s)","Unit of Measure Quantity":"15","Warranty":"None","Width (Inches)":"12"} 220224        4.91
# MAGIC                                                                                                                                                                                                                                                                                                                                                                                                                        {"Brand":"Kingsford","Case Count":"2"} 202408        4.51
# MAGIC                                                                                                                                                                                                                                                                                                                                                                                                     {"Brand":"Kirkland Signature","Fuel Type":"Wood Pellets"} 156520        3.49
# MAGIC                                                                    {"assembled_depth_in":"11.75 in","assembled_height_in":"9.5 in","assembled_width_in":"22.5 in","bag_weight_lb":"16 lb","flavor":"No Flavor","lighter_fluid_required":"Lighter Fluid Required","number_of_bags_boxes":"2","outdoor_living_product_type":"Charcoal Briquettes","product_depth_in":"22.5 in","product_height_in":"9.5 in","product_width_in":"22.5 in","returnable":"90-Day"} 108664        2.42
# MAGIC                                       {"certifications_and_listings":"No Certifications or Listings","color_family":"Assorted Colors","fuel_type":"Butane","grilling_tools_and_utensils_features":"No Additional Features","manufacturer_warranty":"CALL FOR REPLACEMENT","outdoor_living_product_type":"Grill Lighter","pack_size":"1","product_depth_in":"1.25 in","product_height_in":"12.95 in","product_weight_lb":"0.2 lb","product_width_in":"3.5 in"} 103856        2.32
# MAGIC                                                                                                                                                                                                                                                                                                                                                                                                                {"Brand":"Traeger","Fuel Type":"Wood Pellets"}  97933        2.18
# MAGIC                                                                                                                                                                                    {"CA Residents: Prop 65 Warning(s)":"Yes","Material":"Wood","Package Quantity":"2","Refillable":"No","Safety Listing":"Not safety listed","Type":"Charcoal briquettes","UNSPSC":"15101600","Unit of Measure":"Pound(s)","Unit of Measure Quantity":"16","Warranty":"None"}  84906        1.89
# MAGIC                                                         {"Bag Weight (lbs.)":"40","CA Residents: Prop 65 Warning(s)":"Yes","Flavor":"Competition Blend","Recommended for All Food":"Yes","Recommended for Baked Goods":"Yes","Recommended for Beef":"Yes","Recommended for Lamb":"Yes","Recommended for Pork":"Yes","Recommended for Poultry":"Yes","Recommended for Seafood":"Yes","Recommended for Vegetables":"Yes","UNSPSC":"15101600","Warranty":"None"}  81491        1.82
# MAGIC                                                                                 {"CA Residents: Prop 65 Warning(s)":"Yes","Material":"Plastic","Package Quantity":"2","Refillable":"No","Safety Listing":"UL safety listing","Series Name":"Aim N Flame Max 2pk","Type":"Lighter","UNSPSC":"15101600","Unit of Measure":"Count","Unit of Measure Quantity":"2","Voltage":"0","Warranty":"Lifetime","Watts":"0","Width (Inches)":"3.25","Wind Resistant":"No"}  61156        1.36
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 6. CHECKING FOR CATEGORY FIELDS IN PRODUCT ATTRIBUTES
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   ✓ No records contain 'major_cat'
# MAGIC   ✓ No records contain 'Major Cat'
# MAGIC   ✓ No records contain 'major cat'
# MAGIC   ⚠️  Found 'category' in 161,723 records (3.61%)
# MAGIC
# MAGIC   Sample records containing 'category':
# MAGIC +---------------+----------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# MAGIC |major_cat      |web_description                                                                         |product_attributes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
# MAGIC +---------------+----------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# MAGIC |gas grills     |5-Burner Propane Gas Grill in Matte Black with TriVantage Multifunctional Cooking System|{"accessories_included":"No Additional Items Included","assembled_depth_in":"23.82 in","assembled_height_in":"47.24 in","assembled_width_in":"55.12 in","assembly_required":"Yes","burner_material":"Stainless Steel","burner_warranty":"5 Years Limited Warranty","certifications_and_listings":"CSA Certified","color":"Matte Black","fits_no_of_burgers":"23","front_side_shelf":"Fixed","grate_warranty":"1 Year Limited Warranty","grill_color_family":"Black","grill_grate_surface_material":"Porcelain-Coated Cast Iron","grill_material":"Painted Steel","grill_smoker_category":"Freestanding/Cart Style","grill_smoker_features":"Grease Pan,Heat Thermometer,Side Burner,Warming Rack","grill_smoker_fuel_type":"Propane","grill_wheels":"Two","ignition_type":"Piezo","includes_grill_cover":"Without grill cover","number_of_burners":"5","number_of_main_burners":"5 Burners","number_of_side_burners":"1","outdoor_living_product_type":"Propane Grill","overall_grill_warranty":"1 Year Limited Warranty","primary_burner_btus":"75000","primary_cooking_space_sq_in":"473 sq in","product_weight_lb":"72.69 lb","returnable":"90-Day","secondary_cooking_space_sq_in":"150 sq in","total_cooking_space_sq_in":"623"}|
# MAGIC |charcoal grills|29 in. Barrel Offset Charcoal Smoker and Grill in Black                                 |{"accessories_included":"No Additional Items Included","assembled_depth_in":"24.00 in","assembled_height_in":"47.00 in","assembled_width_in":"54.00 in","cooking_space_sq_in":"741.2 sq in","grill_color_family":"Black","grill_smoker_category":"Offset","grill_smoker_features":"Heat Thermometer,Warming Rack","grill_smoker_fuel_type":"Charcoal","grill_wheels":"Two","hopper_capacity_lb":"0 lb","ignition_type":"No Ignition System","includes_grill_cover":"Without grill cover","manufacturer_warranty":"1","material":"Steel","number_of_cooking_racks":"2 Racks","outdoor_living_product_type":"Charcoal/Wood Grill","product_weight_lb":"54.02 lb","returnable":"90-Day","total_btu_btu":"0 Btu"}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
# MAGIC |electric grills|Lock N' Go Portable Electric Grill in Red                                               |{"accessories_included":"No Additional Items Included","assembled_depth_in":"20 in","assembled_height_in":"16 in","assembled_width_in":"21 in","assembly_required":"Yes","certifications_and_listings":"UL Listed","color":"Red","cord_length_ft":"8","fits_no_of_burgers":"6","grill_color_family":"Red","grill_grate_surface_material":"Chrome-Plated Steel","grill_material":"Painted Steel","grill_smoker_category":"Tabletop","grill_smoker_features":"No Additional Features","grill_smoker_fuel_type":"Electric","grill_wheels":"None","ignition_type":"Electrical","outdoor_living_product_type":"Electric Grill","overall_grill_warranty":"90 Day Limited Warranty","primary_cooking_space_sq_in":"176","product_weight_lb":"11 lb","returnable":"90-Day","wattage_w":"1500.00"}                                                                                                                                                                                                                                                                                                                                                                                                                                            |
# MAGIC +---------------+----------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# MAGIC
# MAGIC   ✓ No records contain 'Category'
# MAGIC
# MAGIC ================================================================================
# MAGIC ANALYSIS COMPLETE: weber
# MAGIC ================================================================================
# MAGIC
# MAGIC ✓ Configuration found for werner
# MAGIC   Sandbox Schema: ydx_werner_analysts_silver
# MAGIC
# MAGIC ================================================================================
# MAGIC ANALYZING: ydx_internal_analysts_sandbox.werner_v38_filter_items
# MAGIC ================================================================================
# MAGIC
# MAGIC ✓ Table found: ydx_internal_analysts_sandbox.werner_v38_filter_items
# MAGIC   Total records: 364,765
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 1. COLUMN EXISTENCE
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   product_attributes column exists: False
# MAGIC   ⚠️  product_attributes column does not exist in this table
# MAGIC ✓ Configuration found for odele
# MAGIC   Sandbox Schema: ydx_odele_analysts_silver
# MAGIC
# MAGIC ================================================================================
# MAGIC ANALYZING: ydx_internal_analysts_sandbox.odele_v38_filter_items
# MAGIC ================================================================================
# MAGIC
# MAGIC ✓ Table found: ydx_internal_analysts_sandbox.odele_v38_filter_items
# MAGIC   Total records: 284,385,400
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 1. COLUMN EXISTENCE
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   product_attributes column exists: True
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 2. DATA TYPE
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   Data Type: VariantType()
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 3. FILL RATE
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   Non-null records: 7,947,876
# MAGIC   Null records: 276,437,524
# MAGIC   Fill rate: 2.79%
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 4. DISTINCT VALUES COUNT
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   Distinct product_attributes values: 7,712
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 5. TOP 10 MOST FREQUENT VALUES
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          attrs_json  count  percentage
# MAGIC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 {"details":"[\"Item 2237618\"]","how_to_use":"[\"How To Use\",\"To open, insert pump into liter bottle. Firmly secure cap on liter bottle, twisting cap in a clockwise motion. To open spout, hold the cap on the neck of the bottle and twist the spout counter-clockwise until it pops up.\"]","summary":"[\"Summary\",\"Highlights\",\"Clean Ingredients\",\"Cruelty Free\",\"Vegan\",\"Sustainable Packaging\",\"The Jumbo Liter Pump by AG Care can be used on any liter-size bottle. Perfect for dispensing product with ease, this pump will help ensure that you enjoy every liter-size product to the very last drop.\"]"}  84418        1.06
# MAGIC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      {"details":"[\"Benefits\",\"Activated by heat, effects last through 3 to 4 shampoos\",\"Dream Coat Supernatural Spray provides heat protection. Do not exceed 380 F degrees for normal to coarse hair types. For chemically treated, fine, o
# MAGIC
# MAGIC ... [*** WARNING: max output size exceeded, skipping output. ***] ...
# MAGIC
# MAGIC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  {"details":"[\"Benefits\",\"Instantly boosts shine, color vibrancy, and softness\",\"450ºF/232ºC heat protection\",\"Key Ingredients\",\"Powered by: A blend of Coconut-Derived Emollient, Vitamin E, and Sunflower Seed Oil Conditions, smoothes, and provides shine\",\"Research Results\",\"125% more shine*\",\"72 Hr frizz control and humidity resistance\",\"77% reduction in breakage\",\"450°F/232°C heat protection\",\"*Compared to bleached hair without application of OLAPLEX No.7 Bonding Oil™.\",\"Item 2591018\"]","how_to_use":"[\"How To Use\",\"Uncap and turn bottle upside down.\",\"Gently tap your index finger on the bottom of the bottle to dispense a metered drop.\",\"Apply 2-3 drops to damp or dry hair and style as desired.\",\"Can be used daily on wet and dry hair, before styling with heat, and after styling for added shine and smoothness.\"]","ingredients":"Dimethicone, Isohexadecane, C13-14 Isoparaffin, Coco-Caprylate, Phenyl Trimethicone, Bis-Aminopropyl Diglycol Dimaleate, Propanediol, Zea Mays (Corn) Oil, Beta-Carotene, Helianthus Annuus (Sunflower) Seed Oil, Moringa Oleifera Seed Oil, Punica Granatum Seed Oil, Water (Aqua/Eau), Morinda Citrifolia Fruit Powder, Fragrance (Parfum), Hexyl Cinnamal, Eclipta Prostrata Extract, Ethylhexyl Methoxycinnamate, Limonene, Tocopherol, Citral, Linalool, Melia Azadirachta Leaf Extract, Citronellol, Pseudozyma Epicola/Camellia Sinensis Seed Oil Ferment Extract Filtrate.","size":"1.0 oz","summary":"[\"Summary\",\"Highlights\",\"Vegan\",\"OLAPLEX No.7 Bonding Hair Oil is a highly-concentrated, weightless reparative styling oil that dramatically increases shine, softness, and maintains color vibrancy. OLAPLEX N°7 Bonding Hair Oil minimizes flyaways and frizz, while providing heat protection of up to 450°F/232°C.\"]"}  47331        0.60
# MAGIC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           {"details":"[\"Benefits\",\"Cleanses while creating soft, silky hair\",\"Leaves hair silky soft with increased manageability, suppleness and shine\",\"Provides 15x more conditioning*\",\"*When used with All Soft Conditioner and Argan-6 oil\",\"Key Ingredients\",\"Redken's Moisture Complex featuring Argan Oil\",\"Item 2580410\"]","how_to_use":"[\"How To Use\",\"Apply to damp hair. Lather. Rinse. Follow with All Soft Mega Conditioner.\"]","ingredients":"Aqua/Water/Eau, Sodium Laureth Sulfate, Sodium Chloride, Cocamidopropyl Betaine, Dimethicone, Parfum/Fragrance, Sodium Benzoate, Amodimethicone, Carbomer, Glycerin, Guar Hydroxypropyltrimonium Chloride, Trideceth-10, Salicylic Acid, Hexylene Glycol, Glycol Distearate, Citric Acid, Mica, Sodium Cocoyl Amino Acids, Peg-100 Stearate, Phenoxyethanol, Steareth-6, Trideceth-3, Potassium Dimethicone Peg-7 Panthenyl Phosphate, CI 77891/Titanium Dioxide, Sodium Sarcosinate, Benzyl Alcohol, Propylene Glycol, Coumarin, Linalool, Peg-45M, Benzyl Benzoate, Hexyl Cinnamal, Limonene, Aloe Barbadensis Leaf Juice Powder, Arginine, Hydrolyzed Soy Protein, Plukenetia Volubilis Seed Oil, Caramel, Acetic Acid, Fumaric Acid, Hydrolyzed Vegetable Protein Pg-Propyl Silanetriol, Cereus Grandiflorus Flower Extract/Cactus Flower Extract, Glucose, Lactic Acid, Potassium Sorbate, Tetrasodium Edta, BHT, Tocopherol, Sodium Hydroxide.","size":"10.1 oz","summary":"[\"Summary\",\"Highlights\",\"Vegan\",\"Sustainable Packaging\",\"Redken's All Soft Shampoo is a 2024 Cosmopolitan Beauty Awards Winner and cleanses, restores, and softens dry, brittle hair. This professional product provides 15x more conditioning when used with the complete All Soft System of Shampoo, Conditioner and Argan Oil.\"]"}  41836        0.53
# MAGIC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         {"details":"[\"Benefits\",\"Creates soft, silky hair\",\"Detangles & moisturizes hair\",\"Leaves hair silky soft with increased manageability, suppleness & shine\",\"Provides 15x more conditioning*\",\"*When used with All Soft Shampoo and Argan-6 oil\",\"Key Ingredients\",\"Redken's Moisture Complex with Argan Oil to soften, detangle and moisturize hair\",\"Item 2580408\"]","how_to_use":"[\"How To Use\",\"Use as a complete system with All Soft Shampoo and Argan-6 Oil. After shampooing, apply and distribute through hair. Rinse. Follow with Argan-6 Oil. In case of contact with eyes, rinse them immediately.\"]","ingredients":"Aqua/Water, Cetearyl Alcohol, Behentrimonium Chloride, Elaeis Guineensis Oil/Palm Oil, Cetyl Alcohol, Isopropyl Alcohol, Phenoxyethanol, Stearamidopropyl Dimethylamine, Octyldodecanol, Sodium Pca, Parfum/Fragrance, Citric Acid, Sodium Cocoyl Amino Acids, Chlorhexidine Dihydrochloride, Stearyl Alcohol, Myristyl Alcohol, Potassium Dimethicone Peg-7 Panthenyl Phosphate, Sodium Sarcosinate, Propylene Glycol, Argania Spinosa Kernel Oil, Arginine, Hydrolyzed Soy Protein, Hydrolyzed Vegetable Protein Pg-Propyl Silanetriol, Sodium Chloride, Tetrasodium Edta, Potassium Sorbate (D184537/1).","size":"10.1 oz","summary":"[\"Summary\",\"Highlights\",\"Clean Ingredients\",\"Vegan\",\"Sustainable Packaging\",\"Redken's All Soft Conditioner detangles, conditions and softens dry, brittle hair. This professional product provides 15x more conditioning when used with the complete All Soft System of Shampoo, Conditioner and Argan Oil.\"]"}  41524        0.52
# MAGIC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            {"details":"[\"Benefits\",\"Controls frizz and helps define air-dried styles\",\"Softens, smooths, and hydrates hair\",\"450ºF/232ºC heat protection\",\"Key Ingredients\",\"Cationic Conditioning Agents Soften hair and provide anti-static action to help smooth hair cuticles.\",\"Blend of Coconut-Derived Emollient, Vitamin E, and Sunflower Seed Oil Smoothes and provides a subtle shine\",\"Research Results\",\"72 Hr frizz control + humidity resistance\",\"91% easier detangling\",\"71% reduction in breakage during styling and brushing\",\"Item 2592990\"]","how_to_use":"[\"How To Use\",\"1.Apply one pump to clean, damp hair 2.Comb through & style as desired. Begin with one pump. Add more as needed. Can be applied to dry hair for added frizz control and hydration.\"]","ingredients":"Water (Aqua/Eau), Cetearyl Alcohol, Dimethicone, Isohexadecane, Coco-Caprylate, Neopentyl Glycol Diheptanoate, Behentrimonium Chloride, Isododecane, Phenyl Trimethicone, Propanediol, Bis-Aminopropyl Diglycol Dimaleate, Fragrance (Parfum), Cetrimonium Chloride, Phenoxyethanol, Glyceryl Stearate, Isopropyl Alcohol, Hydroxyethylcellulose, Hydroxypropyl Guar, Sodium Stearoyl Lactylate, Hydroxypropyl Cyclodextrin, Hexyl Cinnamal, Limonene, Citral, Hydrolyzed Vegetable Protein PG-Propyl Silanetriol, Disodium EDTA, Linalool, Citronellol, Iodopropynyl Butylcarbamate, Hydroxycitronellal, Etidronic Acid, Tocopherol, Geraniol, Potassium Sorbate, Helianthus Annuus (Sunflower) Seed Oil, Phytantriol, Sodium Benzoate, Pseudozyma Epicola/Camellia Sinensis Seed Oil Ferment Extract Filtrate, Tocopheryl Acetate, Vitis Vinifera (Grape) Seed Oil, Aloe Barbadensis Leaf Juice, Panthenol, Citric Acid, Gigartina Stellata Extract, Chondrus Crispus (Carrageenan) Extract, Ascorbic Acid, Cocos Nucifera (Coconut) Oil.","size":"3.3 oz","summary":"[\"Summary\",\"Highlights\",\"Vegan\",\"OLAPLEX No.6 Bond Smoother is a clinically-proven smoothing styling cream that controls frizz for up to 72 hours. Nourishing, humidity-resistant formula hydrates, softens and repairs all frizz-prone hair types without adding excess weight.\"]"}  34989        0.44
# MAGIC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        {"details":"[\"Benefits\",\"Infused with Biotin\",\"Use for daily hair care or specialized scalp treatments\",\"Features more than 30 essential oils and nutrients\",\"Item 2601408\"]","how_to_use":"[\"How To Use\",\"Scalp Treatment: Section hair into 4 parts exposing scalp. Apply a small amount of oil to scalp, massage oil in with fingers and comb through to ends of hair. Leave in and style as desired.\",\"Daily Use: Apply a small amount to scalp and comb through to ends.\",\"Split End Care: Apply oil to ends of hair, place a processing cap on head and leave on for 10 minutes. Rinse and proceed with shampooing.\"]","ingredients":"Glycine Soja (Soybean) Oil, Ricinus Communis (Castor) Seed Oil, Rosmarinus Officinalis (Rosemary) Leaf Oil, Simmondsia Chinensis (Jojoba) Seed Oil, Mentha Piperita (Peppermint) Oil, Eucalyptus Globolus (Eucalyptus) Leaf Oil, Menthol, Melalueca Alternifolia (Tea Tree) Leaf Oil, Cocos Nucifera (Coconut) Oil, Equisetum Arvense (Horsetail) Extract, Aloe Barbadensis Extract, Lavandula Angustifolia (Lavender) Oil, Triticum Vulgare (Wheat) Germ Oil, Carthamus Tinctorius (Safflower) Seed Oil, Oenothera Biennis (Evening Primrose) Oil, Vitis Vinifera (Grape) Seed Oil, Benzyl Nicotinate, Prunus Amygdalus Dulcis (Sweet Almond) Oil, Oryza Sativa (Rice) Bran Oil, Tocopheryl Acetate, Biotin, Arctium Lappa (Burdock) Root Extract, Glycerin, Apium Graveolens (Celery) Seed Extract, Retinyl Palmitate, Cholecalciferol (Vitamin D), Ascorbic Acid, Ocimum, Basilicum (Basil) Oil, Pogostemon Cablin (Patchouli) Oil, Salvia Officinalis (Sage) Oil, Silica, Urtica Dioica (Nettle) Extract","size":"2.0 oz","summary":"[\"Summary\",\"Encourage stronger and healthier hair with Mielle Organic's organic Rosemary Mint Scalp & Hair Strengthening Oil.\"]"}  34102        0.43
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 6. CHECKING FOR CATEGORY FIELDS IN PRODUCT ATTRIBUTES
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   ✓ No records contain 'major_cat'
# MAGIC   ✓ No records contain 'Major Cat'
# MAGIC   ✓ No records contain 'major cat'
# MAGIC   ⚠️  Found 'category' in 1 records (0.00%)
# MAGIC
# MAGIC   Sample records containing 'category':
# MAGIC +---------+------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# MAGIC |major_cat|web_description               |product_attributes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
# MAGIC +---------+------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# MAGIC |hair     |All Soft Conditioner - 33.8 oz|{"color":"100","details":"[\"Benefits\",\"Up to 12HR moisturizing wear & crease-resistant coverage\",\"Quickly covers up blemishes or dark spots to transform your look instantly\",\"Sponge tip applicator lets you apply with ease in a click\",\"Features\",\"Infused with Goji berry & Haloxyl to create a radiant refreshed looking eye area\",\"Oil-free\",\"Fragrance-free\",\"Non-comedogenic & suitable for sensitive skin\",\"*L'Oreal calculation based in part on data reported by NielsenIQ through its Scantrack Service for the Concealer category for the 52-week period ending July 31, 2021, for the US xAOC market according to L'Oreal's custom product hierarchy. Copyright 2021, Nielsen Consumer LLC.\",\"Item 2529473\"]","how_to_use":"[\"How To Use\",\"Step 1. Twist the sponge applicator in direction of arrows until the concealer is visible on the sponge.\",\"Step 2. Apply concealer directly to the under-eye area, blending in an outward motion.\",\"Step 3. For extreme dark circles, apply the Neutralizer shade under concealer shade.\",\"Step 4. For a luminous touch, apply the Brightener shade to the inner corner of eyes, brow bones and bridge of the nose.\",\"The Dark Circles Eraser is protected by an anti-microbial system. Do not wet applicator. Wipe off excess eye concealer with dry tissue.\"]","ingredients":"G852364 1 Aqua/Water/Eau, Cyclopentasiloxane, Dimethicone, Isododecane, Glycerin, Peg-9 Polydimethylsiloxyethyl Dimethicone, Butylene Glycol, Dimethicone Crosspolymer, Nylon-12, Disteardimonium Hectorite, Cyclohexasiloxane, Peg-10 Dimethicone, Cetyl Peg/Ppg-10/1 Dimethicone, Phenoxyethanol, Sodium Chloride, Polyglyceryl-4 Isostearate, Caprylyl Glycol, Disodium Stearoyl Glutamate, Ethylhexylglycerin, Methylparaben, Lycium Barbarum Fruit Extract, Chlorphenesin, Ethylparaben, Aluminum Hydroxide, Peg-9, N-Hydroxysuccinimide, Palmitoyl Oligopeptide, Chrysin, Palmitoyl Tetr Apeptide-7. [+/- May Contain/Peut Contenir: CI 77891/Titanium Dioxide, CI 77491, CI 77492, CI 77499/Iron Oxides] F.I.L. D44980/4 U.S.Patent Pending.","size":"0.2 oz","summary":"[\"Summary\",\"Highlights\",\"Vegan\",\"Give Back\",\"Maybelline Instant Eraser is America's #1 concealer* and does it all in just a click - it conceals dark circles, correct blemishes, and contours.\"]"}|
# MAGIC +---------+------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# MAGIC
# MAGIC   ✓ No records contain 'Category'
# MAGIC
# MAGIC ================================================================================
# MAGIC ANALYSIS COMPLETE: odele
# MAGIC ================================================================================
# MAGIC

# COMMAND ----------

# DBTITLE 1,Analyze Output Tables (_sku_time_series_test)
print("\n\n")
print("#" * 100)
print("# PART 2: ANALYZING OUTPUT TABLES (_sku_time_series_test)")
print("#" * 100)

output_results = {}

for client_name, config in clients_config.items():
    demo_name = config['demo_name']
    output_table = f"{TEST_CATALOG}.{TEST_SCHEMA}.{demo_name}_sku_time_series"

    result = analyze_product_attributes(demo_name, "sku_time_series")

    output_results[client_name] = result

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC ####################################################################################################
# MAGIC # PART 2: ANALYZING OUTPUT TABLES (_sku_time_series_test)
# MAGIC ####################################################################################################
# MAGIC ✓ Configuration found for weber
# MAGIC   Sandbox Schema: ydx_weber_analysts_silver
# MAGIC
# MAGIC ================================================================================
# MAGIC ANALYZING: ydx_internal_analysts_gold.weber_v38_sku_time_series
# MAGIC ================================================================================
# MAGIC
# MAGIC ✓ Table found: ydx_internal_analysts_gold.weber_v38_sku_time_series
# MAGIC   Total records: 954,470
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 1. COLUMN EXISTENCE
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   product_attributes column exists: True
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 2. DATA TYPE
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   Data Type: VariantType()
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 3. FILL RATE
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   Non-null records: 0
# MAGIC   Null records: 954,470
# MAGIC   Fill rate: 0.00%
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 4. DISTINCT VALUES COUNT
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   Distinct product_attributes values: 0
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 5. TOP 10 MOST FREQUENT VALUES
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC Empty DataFrame
# MAGIC Columns: [attrs_json, count, percentage]
# MAGIC Index: []
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 6. CHECKING FOR CATEGORY FIELDS IN PRODUCT ATTRIBUTES
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   ✓ No records contain 'major_cat'
# MAGIC   ✓ No records contain 'Major Cat'
# MAGIC   ✓ No records contain 'major cat'
# MAGIC   ✓ No records contain 'category'
# MAGIC   ✓ No records contain 'Category'
# MAGIC
# MAGIC ================================================================================
# MAGIC ANALYSIS COMPLETE: weber
# MAGIC ================================================================================
# MAGIC
# MAGIC ✓ Configuration found for werner
# MAGIC   Sandbox Schema: ydx_werner_analysts_silver
# MAGIC
# MAGIC ================================================================================
# MAGIC ANALYZING: ydx_internal_analysts_gold.werner_v38_sku_time_series
# MAGIC ================================================================================
# MAGIC
# MAGIC ✓ Table found: ydx_internal_analysts_gold.werner_v38_sku_time_series
# MAGIC   Total records: 20,376
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 1. COLUMN EXISTENCE
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   product_attributes column exists: True
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 2. DATA TYPE
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   Data Type: VariantType()
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 3. FILL RATE
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   Non-null records: 0
# MAGIC   Null records: 20,376
# MAGIC   Fill rate: 0.00%
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 4. DISTINCT VALUES COUNT
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   Distinct product_attributes values: 0
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 5. TOP 10 MOST FREQUENT VALUES
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC Empty DataFrame
# MAGIC Columns: [attrs_json, count, percentage]
# MAGIC Index: []
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 6. CHECKING FOR CATEGORY FIELDS IN PRODUCT ATTRIBUTES
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   ✓ No records contain 'major_cat'
# MAGIC   ✓ No records contain 'Major Cat'
# MAGIC   ✓ No records contain 'major cat'
# MAGIC   ✓ No records contain 'category'
# MAGIC   ✓ No records contain 'Category'
# MAGIC
# MAGIC ================================================================================
# MAGIC ANALYSIS COMPLETE: werner
# MAGIC ================================================================================
# MAGIC
# MAGIC ✓ Configuration found for odele
# MAGIC   Sandbox Schema: ydx_odele_analysts_silver
# MAGIC
# MAGIC ================================================================================
# MAGIC ANALYZING: ydx_internal_analysts_gold.odele_v38_sku_time_series
# MAGIC ================================================================================
# MAGIC
# MAGIC ✓ Table found: ydx_internal_analysts_gold.odele_v38_sku_time_series
# MAGIC   Total records: 7,230,953
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 1. COLUMN EXISTENCE
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   product_attributes column exists: True
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 2. DATA TYPE
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   Data Type: VariantType()
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 3. FILL RATE
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   Non-null records: 0
# MAGIC   Null records: 7,230,953
# MAGIC   Fill rate: 0.00%
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 4. DISTINCT VALUES COUNT
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   Distinct product_attributes values: 0
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 5. TOP 10 MOST FREQUENT VALUES
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC Empty DataFrame
# MAGIC Columns: [attrs_json, count, percentage]
# MAGIC Index: []
# MAGIC
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC 6. CHECKING FOR CATEGORY FIELDS IN PRODUCT ATTRIBUTES
# MAGIC ────────────────────────────────────────────────────────────────────────────────
# MAGIC   ✓ No records contain 'major_cat'
# MAGIC   ✓ No records contain 'Major Cat'
# MAGIC   ✓ No records contain 'major cat'
# MAGIC   ✓ No records contain 'category'
# MAGIC   ✓ No records contain 'Category'
# MAGIC
# MAGIC ================================================================================
# MAGIC ANALYSIS COMPLETE: odele
# MAGIC ================================================================================
# MAGIC

# COMMAND ----------

# DBTITLE 1,Comparison: Input vs Output
print("\n\n")
print("=" * 100)
print("COMPARISON: INPUT (_filter_items) vs OUTPUT (_sku_time_series_test)")
print("=" * 100)

for client_name in clients_config.keys():
    input_result = input_results.get(client_name)
    output_result = output_results.get(client_name)

    print(f"\n{'-'*100}")
    print(f"CLIENT: {client_name.upper()}")
    print(f"{'-'*100}")

    if input_result and output_result:
        if input_result.get('column_exists') and output_result.get('column_exists'):
            # Compare keywords found
            input_keywords = input_result.get('keywords_found', {})
            output_keywords = output_result.get('keywords_found', {})

            print("\nCategory Keywords Comparison:")
            print(f"{'Keyword':<20s} {'Input (_filter_items)':<25s} {'Output (_sku_time_series)':<25s} {'Status':<15s}")
            print("-" * 85)

            all_keywords = set(list(input_keywords.keys()) + list(output_keywords.keys()))

            for keyword in all_keywords:
                input_count = input_keywords.get(keyword, 0)
                output_count = output_keywords.get(keyword, 0)

                if input_count > 0 and output_count > 0:
                    status = "⚠️  PERSISTED"
                elif input_count > 0 and output_count == 0:
                    status = "✓ REMOVED"
                elif input_count == 0 and output_count > 0:
                    status = "❌ ADDED"
                else:
                    status = "✓ CLEAN"

                print(f"{keyword:<20s} {input_count:<25,} {output_count:<25,} {status:<15s}")

            # Fill rate comparison
            print(f"\nFill Rate Comparison:")
            print(f"  Input:  {input_result.get('fill_rate', 0):.2f}%")
            print(f"  Output: {output_result.get('fill_rate', 0):.2f}%")

    else:
        print("  ⚠️  Unable to compare - missing data")

print("\n" + "=" * 100)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC ====================================================================================================
# MAGIC COMPARISON: INPUT (_filter_items) vs OUTPUT (_sku_time_series_test)
# MAGIC ====================================================================================================
# MAGIC
# MAGIC ----------------------------------------------------------------------------------------------------
# MAGIC CLIENT: WEBER
# MAGIC ----------------------------------------------------------------------------------------------------
# MAGIC
# MAGIC Category Keywords Comparison:
# MAGIC Keyword              Input (_filter_items)     Output (_sku_time_series) Status         
# MAGIC -------------------------------------------------------------------------------------
# MAGIC
# MAGIC Fill Rate Comparison:
# MAGIC   Input:  18.39%
# MAGIC   Output: 0.00%
# MAGIC
# MAGIC ----------------------------------------------------------------------------------------------------
# MAGIC CLIENT: WERNER
# MAGIC ----------------------------------------------------------------------------------------------------
# MAGIC
# MAGIC ----------------------------------------------------------------------------------------------------
# MAGIC CLIENT: ODELE
# MAGIC ----------------------------------------------------------------------------------------------------
# MAGIC
# MAGIC Category Keywords Comparison:
# MAGIC Keyword              Input (_filter_items)     Output (_sku_time_series) Status         
# MAGIC -------------------------------------------------------------------------------------
# MAGIC
# MAGIC Fill Rate Comparison:
# MAGIC   Input:  2.79%
# MAGIC   Output: 0.00%
# MAGIC
# MAGIC ====================================================================================================

# COMMAND ----------

# DBTITLE 1,Root Cause Determination
print("\n\n")
print("=" * 100)
print("ROOT CAUSE ANALYSIS")
print("=" * 100)

conclusions = []

for client_name in clients_config.keys():
    input_result = input_results.get(client_name)
    output_result = output_results.get(client_name)

    if input_result and output_result:
        if input_result.get('column_exists') and output_result.get('column_exists'):
            input_keywords = input_result.get('keywords_found', {})
            output_keywords = output_result.get('keywords_found', {})

            # Check if Major Cat appears in input
            input_has_major_cat = any(
                count > 0 for keyword, count in input_keywords.items()
                if 'major' in keyword.lower() or 'cat' in keyword.lower()
            )

            # Check if Major Cat appears in output
            output_has_major_cat = any(
                count > 0 for keyword, count in output_keywords.items()
                if 'major' in keyword.lower() or 'cat' in keyword.lower()
            )

            if input_has_major_cat and output_has_major_cat:
                conclusion = f"❌ {client_name.upper()}: Major Cat is ALREADY in _filter_items and PERSISTED to output"
                root_cause = "UPSTREAM DATA ISSUE"
            elif not input_has_major_cat and output_has_major_cat:
                conclusion = f"❌ {client_name.upper()}: Major Cat is NOT in _filter_items but ADDED in output"
                root_cause = "BLUEPRINT CODE ISSUE"
            elif input_has_major_cat and not output_has_major_cat:
                conclusion = f"✓ {client_name.upper()}: Major Cat was in _filter_items but REMOVED in output"
                root_cause = "FIXED BY BLUEPRINT"
            else:
                conclusion = f"✓ {client_name.upper()}: No Major Cat in input or output"
                root_cause = "NO ISSUE"

            conclusions.append({
                'client': client_name,
                'conclusion': conclusion,
                'root_cause': root_cause
            })

print("\nFINDINGS:")
print("-" * 100)

for item in conclusions:
    print(f"\n{item['conclusion']}")
    print(f"   Root Cause: {item['root_cause']}")

print("\n" + "=" * 100)

# Determine overall root cause
if conclusions:
    root_causes = [item['root_cause'] for item in conclusions]

    if 'BLUEPRINT CODE ISSUE' in root_causes:
        print("\n🔍 OVERALL ROOT CAUSE: BLUEPRINT CODE ISSUE")
        print("   The product_analysis.py code is ADDING Major Cat to product_attributes")
        print("   Action: Fix the blueprint code to exclude category fields")

    elif 'UPSTREAM DATA ISSUE' in root_causes:
        print("\n🔍 OVERALL ROOT CAUSE: UPSTREAM DATA ISSUE")
        print("   The _filter_items table ALREADY contains Major Cat in product_attributes")
        print("   Action: Fix the core/setup process or manual data modification")

    else:
        print("\n✓ NO SYSTEMATIC ISSUE FOUND")
        print("   Either the issue was already fixed or doesn't exist in test data")

print("\n" + "=" * 100)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC ====================================================================================================
# MAGIC ROOT CAUSE ANALYSIS
# MAGIC ====================================================================================================
# MAGIC
# MAGIC FINDINGS:
# MAGIC ----------------------------------------------------------------------------------------------------
# MAGIC
# MAGIC ✓ WEBER: No Major Cat in input or output
# MAGIC    Root Cause: NO ISSUE
# MAGIC
# MAGIC ✓ ODELE: No Major Cat in input or output
# MAGIC    Root Cause: NO ISSUE
# MAGIC
# MAGIC ====================================================================================================
# MAGIC
# MAGIC ✓ NO SYSTEMATIC ISSUE FOUND
# MAGIC    Either the issue was already fixed or doesn't exist in test data
# MAGIC
# MAGIC ====================================================================================================

# COMMAND ----------

# DBTITLE 1,Detailed Investigation: Extract Actual Attribute Keys
print("\n\n")
print("=" * 100)
print("DETAILED INVESTIGATION: EXTRACTING ACTUAL ATTRIBUTE KEYS")
print("=" * 100)

for client_name, config in clients_config.items():
    demo_name = config['demo_name']

    print(f"\n{'-'*100}")
    print(f"CLIENT: {client_name.upper()}")
    print(f"{'-'*100}")

    # Check both input and output tables
    for table_type, table_suffix in [('INPUT', '_v38_filter_items'), ('OUTPUT', '_sku_time_series')]:
        
        if table_suffix == '_v38_filter_items':
            table_name = f"{TEST_SCHEMA}.{demo_name}{table_suffix}"
        else:
            table_name = f"ydx_internal_analysts_gold.{demo_name}_v38{table_suffix}"

        print(f"\n{table_type}: {table_name}")

        try:
            df = spark.table(table_name)
            if 'product_attributes' in df.columns:
                # Get records where product_attributes is not null
                sample_df = df.filter(F.col('product_attributes').isNotNull()) \
                    .select(
                        'major_cat',
                        'web_description',
                        F.to_json('product_attributes').alias('product_attributes_json')
                    ) \
                    .limit(5)

                print("\n  Sample records with product_attributes:")
                sample_df.show(truncate=False)
        except Exception as e:
            print(f"  ❌ Error: {str(e)}")

print("\n" + "=" * 100)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC ====================================================================================================
# MAGIC DETAILED INVESTIGATION: EXTRACTING ACTUAL ATTRIBUTE KEYS
# MAGIC ====================================================================================================
# MAGIC
# MAGIC ----------------------------------------------------------------------------------------------------
# MAGIC CLIENT: WEBER
# MAGIC ----------------------------------------------------------------------------------------------------
# MAGIC
# MAGIC INPUT: ydx_internal_analysts_sandbox.weber_v38_filter_items
# MAGIC
# MAGIC   Sample records with product_attributes:
# MAGIC +-------------------------+--------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# MAGIC |major_cat                |web_description                             |product_attributes_json                                                                                                                                                                                                                                                                                                                                                                                                   |
# MAGIC +-------------------------+--------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# MAGIC |grill tools & accessories|23 oz. BBQ and Grill Cleaner                |{"certifications_and_listings":"No Certifications or Listings","cleaning_tool_type":"Degreaser","features":"No Additional Features","formulation":"Regular Strength","manufacturer_warranty":"N/A","pack_size":"1","product_form":"Liquid","product_size":"23 OZ-Ounce","returnable":"90-Day","scent":"Citrus"}                                                                                                           |
# MAGIC |grilling fuels           |Cowboy Charcoal Hardwood 15-lb Lump Charcoal|{"CA Residents: Prop 65 Warning(s)":"Yes","Flavor":"Hardwood","Material":"Wood","Package Quantity":"1","Safety Listing":"Not safety listed","Type":"Lump charcoal","UNSPSC":"15101600","Unit of Measure":"Pound(s)","Unit of Measure Quantity":"15","Warranty":"None"}                                                                                                                                                    |
# MAGIC |grilling fuels           |Propane Tank Exchange                       |{"assembled_depth_in":"12.3 in","assembled_height_in":"17.8 in","assembled_width_in":"12.3 in","number_of_cylinders_included":"1","outdoor_living_product_type":"Liquid Propane","pre_filled":"Yes","product_depth_in":"12.2 in","product_diameter_in":"12 in","product_height_in":"18 in","product_weight_lb":"15 lb","product_width_in":"12.2 in","tank_capacity_lb":"15 lb","tank_weight_lb":"31"}                     |
# MAGIC |grill tools & accessories|Grill Spatula with Wood Handle              |{"color_family":"Brown","grilling_tools_and_utensils_features":"No Additional Features","handle_material":"Wood","outdoor_living_product_type":"Cooking Accessory","package_quantity":"1","product_depth_in":"3.94 in","product_height_in":"1.18 in","product_width_in":"19.09 in","returnable":"90-Day"}                                                                                                                 |
# MAGIC |grill tools & accessories|Aim n Flame MAX Utility Lighter (3-Pack)    |{"certifications_and_listings":"OSHA Compliant","color_family":"Assorted Colors","fuel_type":"Butane","grilling_tools_and_utensils_features":"No Additional Features","manufacturer_warranty":"Contact customer service at 1-800-LIGHTER","outdoor_living_product_type":"Grill Lighter","pack_size":"3","product_depth_in":"7.25 in","product_height_in":"1.5 in","product_weight_lb":"0.6 lb","product_width_in":"13 in"}|
# MAGIC +-------------------------+--------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# MAGIC
# MAGIC
# MAGIC OUTPUT: ydx_internal_analysts_gold.weber_v38_sku_time_series
# MAGIC
# MAGIC   Sample records with product_attributes:
# MAGIC +---------+---------------+-----------------------+
# MAGIC |major_cat|web_description|product_attributes_json|
# MAGIC +---------+---------------+-----------------------+
# MAGIC +---------+---------------+-----------------------+
# MAGIC
# MAGIC
# MAGIC ----------------------------------------------------------------------------------------------------
# MAGIC CLIENT: WERNER
# MAGIC ----------------------------------------------------------------------------------------------------
# MAGIC
# MAGIC INPUT: ydx_internal_analysts_sandbox.werner_v38_filter_items
# MAGIC
# MAGIC OUTPUT: ydx_internal_analysts_gold.werner_v38_sku_time_series
# MAGIC
# MAGIC   Sample records with product_attributes:
# MAGIC +---------+---------------+-----------------------+
# MAGIC |major_cat|web_description|product_attributes_json|
# MAGIC +---------+---------------+-----------------------+
# MAGIC +---------+---------------+-----------------------+
# MAGIC
# MAGIC
# MAGIC ----------------------------------------------------------------------------------------------------
# MAGIC CLIENT: ODELE
# MAGIC ----------------------------------------------------------------------------------------------------
# MAGIC
# MAGIC INPUT: ydx_internal_analysts_sandbox.odele_v38_filter_items
# MAGIC
# MAGIC   Sample records with product_attributes:
# MAGIC +---------+--------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# MAGIC |major_cat|web_description                                               |product_attributes_json                                                                                                                                                          |
# MAGIC +---------+--------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# MAGIC |hair     |Mini Frizz-Me-Not Hydrating Anti-Frizz Treatment              |{"highlights":"Good for: Frizz, Heat Protection, Clean + Planet Aware, Increases Shine","size":"2 oz / 60 mL"}                                                                   |
# MAGIC |hair     |Cactus Fruit 3-in-1 Styling Cream with Taming Wand Taming Wand|{"color":"Taming Wand","highlights":"Good for: Frizz, Hold & Style Extending, Good for: Dryness, All Hair Textures, All Hair Types, Increases Shine","size":"0.6 oz / 20 mL"}    |
# MAGIC |hair     |No. 4 Bond Maintenance™ Strengthening Hair Repair Shampoo     |{"highlights":"Good for: Dryness, Bond Building, All Hair Types, Good for: Damage, Increases Shine, All Hair Textures","size":"8.5 oz/ 250 mL"}                                  |
# MAGIC |hair     |No. 7 Bonding Frizz Reduction & Heat Protectant Hair Oil      |{"highlights":"UV Protection, Bond Building, Heat Protection, All Hair Types, High Shine Finish, Vegan","size":"1 oz/ 30 mL"}                                                    |
# MAGIC |hair     |Mini Honey Infused Hair Oil                                   |{"highlights":"Increases Shine, All Hair Types, Good for: Dryness, allure 2023 Best of Beauty Award Winner, Good for: Frizz, Clean at Sephora, Hydrating","size":"0.7 oz/ 20 mL"}|
# MAGIC +---------+--------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# MAGIC
# MAGIC
# MAGIC OUTPUT: ydx_internal_analysts_gold.odele_v38_sku_time_series
# MAGIC
# MAGIC   Sample records with product_attributes:
# MAGIC +---------+---------------+-----------------------+
# MAGIC |major_cat|web_description|product_attributes_json|
# MAGIC +---------+---------------+-----------------------+
# MAGIC +---------+---------------+-----------------------+
# MAGIC
# MAGIC
# MAGIC ====================================================================================================
