# Databricks notebook source
# MAGIC %md
# MAGIC # Schema and Structure Tests for filter_items Table
# MAGIC
# MAGIC This notebook contains comprehensive tests for validating the schema and structure of the
# MAGIC `{sandbox_schema}.{demo_name}_filter_items` table created by blueprints.
# MAGIC
# MAGIC Tests included:
# MAGIC 1. Validate presence of mandatory columns
# MAGIC 2. Verify correct data types for each column
# MAGIC 3. Test cases where optional columns may be absent
# MAGIC 4. Validate column ordering (if expected)
# MAGIC 5. Test schema with extra undocumented columns

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import json
from datetime import datetime, date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Expected Schema Definition

# COMMAND ----------

class FilterItemsSchemaValidator:
    """
    Comprehensive schema validator for filter_items table created by blueprints.

    This class validates that client-generated tables conform to expected blueprint standards
    and identifies divergences that could break downstream processing.
    """

    def __init__(self, sandbox_schema, demo_name, prod_schema=None):
        """
        Initialize the schema validator.

        Args:
            sandbox_schema (str): Sandbox schema name (e.g., "ydx_client_analysts_silver")
            demo_name (str): Demo name (e.g., "client_name_v38")
            prod_schema (str, optional): Production schema name for comparison
        """
        self.sandbox_schema = sandbox_schema
        self.demo_name = demo_name
        self.prod_schema = prod_schema
        self.table_name = f"{sandbox_schema}.{demo_name}_filter_items"

        # Define expected schema based on blueprint requirements
        self.mandatory_columns = {
            'order_date': ['date', 'timestamp', 'string'],  # Multiple acceptable types
            'merchant': ['string'],
            'brand': ['string'],
            'item_price': ['double', 'float', 'decimal', 'int', 'bigint'],
            'item_quantity': ['int', 'bigint', 'double', 'float'],
            'gmv': ['double', 'float', 'decimal'],
            'leia_panel_flag_source': ['string', 'boolean', 'int'],
            'month': ['date', 'string', 'int'],
            'user_id': ['string', 'bigint', 'int']
        }

        # Optional columns that may be present depending on client configuration
        self.optional_columns = {
            'order_id': ['string', 'bigint', 'int'],
            'product_id': ['string', 'bigint', 'int'],
            'web_description': ['string'],
            'channel': ['string'],
            'source': ['string'],
            'year': ['date', 'int', 'string'],
            'factor_age': ['float', 'string', 'int'],
            'age': ['string', 'int', 'double'],
            'gender': ['string'],
            'region': ['string'],
            'state': ['string'],
            'panel_end_date': ['date', 'timestamp', 'string'],
            'full_price_estimate': ['double', 'float', 'decimal'],
            'category': ['string'],
            'subcategory': ['string'],
            'special_attribute_column': ['string']  # Dynamic based on client config
        }

        # Expected column order (based on typical blueprint output)
        self.expected_column_order = [
            'order_date', 'merchant', 'month', 'brand', 'item_price',
            'item_quantity', 'gmv', 'leia_panel_flag_source', 'user_id'
        ]

    def load_table(self):
        """Load the filter_items table and handle errors gracefully."""
        try:
            self.df = spark.table(self.table_name)
            return True
        except Exception as e:
            print(f"❌ ERROR: Could not load table {self.table_name}")
            print(f"   Error: {str(e)}")
            self.df = None
            return False

    def test_mandatory_columns_presence(self):
        """
        Test 1: Validate presence of mandatory columns

        Returns:
            dict: Test results with missing columns and status
        """
        print("=" * 70)
        print("TEST 1: MANDATORY COLUMNS PRESENCE")
        print("=" * 70)

        if not hasattr(self, 'df') or self.df is None:
            return {'status': 'FAILED', 'error': 'Table not loaded'}

        actual_columns = set(self.df.columns)
        expected_columns = set(self.mandatory_columns.keys())
        missing_columns = expected_columns - actual_columns

        results = {
            'status': 'PASSED' if not missing_columns else 'FAILED',
            'expected_columns': list(expected_columns),
            'actual_columns': list(actual_columns),
            'missing_columns': list(missing_columns),
            'total_columns': len(actual_columns)
        }

        if not missing_columns:
            print(f"✅ SUCCESS: All {len(expected_columns)} mandatory columns present")
            print(f"   Total columns in table: {len(actual_columns)}")
        else:
            print(f"❌ FAILED: {len(missing_columns)} mandatory columns missing")
            print(f"   Missing columns: {sorted(missing_columns)}")
            print(f"   Present columns: {len(actual_columns)}/{len(expected_columns)}")

        return results

    def test_data_types_validation(self):
        """
        Test 2: Verify correct data types for each column

        Returns:
            dict: Test results with type mismatches and validation status
        """
        print("\n" + "=" * 70)
        print("TEST 2: DATA TYPES VALIDATION")
        print("=" * 70)

        if not hasattr(self, 'df') or self.df is None:
            return {'status': 'FAILED', 'error': 'Table not loaded'}

        # Get actual column types
        actual_types = {field.name: field.dataType.simpleString().lower()
                       for field in self.df.schema.fields}

        type_issues = []
        all_columns_schema = {**self.mandatory_columns, **self.optional_columns}

        for column, expected_types in all_columns_schema.items():
            if column in actual_types:
                actual_type = actual_types[column]
                # Normalize type names for comparison
                normalized_expected = [t.lower() for t in expected_types]

                if actual_type not in normalized_expected:
                    type_issues.append({
                        'column': column,
                        'actual_type': actual_type,
                        'expected_types': expected_types,
                        'is_mandatory': column in self.mandatory_columns
                    })

        results = {
            'status': 'PASSED' if not type_issues else 'FAILED',
            'type_issues': type_issues,
            'total_columns_checked': len([c for c in all_columns_schema.keys()
                                        if c in actual_types]),
            'columns_with_issues': len(type_issues)
        }

        if not type_issues:
            print(f"✅ SUCCESS: All data types are valid")
            print(f"   Validated {results['total_columns_checked']} columns")
        else:
            print(f"❌ FAILED: {len(type_issues)} columns have type issues")
            for issue in type_issues:
                mandatory_flag = "MANDATORY" if issue['is_mandatory'] else "OPTIONAL"
                print(f"   [{mandatory_flag}] {issue['column']}: "
                      f"{issue['actual_type']} (expected: {issue['expected_types']})")

        return results

    def test_optional_columns_handling(self):
        """
        Test 3: Test cases where optional columns may be absent

        Returns:
            dict: Test results showing which optional columns are present/absent
        """
        print("\n" + "=" * 70)
        print("TEST 3: OPTIONAL COLUMNS HANDLING")
        print("=" * 70)

        if not hasattr(self, 'df') or self.df is None:
            return {'status': 'FAILED', 'error': 'Table not loaded'}

        actual_columns = set(self.df.columns)
        expected_optional = set(self.optional_columns.keys())

        present_optional = expected_optional & actual_columns
        absent_optional = expected_optional - actual_columns

        results = {
            'status': 'PASSED',  # Optional columns being absent is acceptable
            'expected_optional_columns': list(expected_optional),
            'present_optional_columns': list(present_optional),
            'absent_optional_columns': list(absent_optional),
            'optional_coverage_percentage': (len(present_optional) / len(expected_optional)) * 100
        }

        print(f"📊 ANALYSIS: Optional columns coverage")
        print(f"   Present: {len(present_optional)}/{len(expected_optional)} "
              f"({results['optional_coverage_percentage']:.1f}%)")

        if present_optional:
            print(f"   ✅ Present optional columns: {sorted(present_optional)}")

        if absent_optional:
            print(f"   ➖ Absent optional columns: {sorted(absent_optional)}")
            print(f"      (This is acceptable - they are optional)")

        return results

    def test_column_ordering(self):
        """
        Test 4: Validate column ordering (if expected)

        Returns:
            dict: Test results for column order validation
        """
        print("\n" + "=" * 70)
        print("TEST 4: COLUMN ORDERING VALIDATION")
        print("=" * 70)

        if not hasattr(self, 'df') or self.df is None:
            return {'status': 'FAILED', 'error': 'Table not loaded'}

        actual_columns = self.df.columns

        # Check if expected columns appear in the expected order
        # Only check columns that are actually present
        present_expected_columns = [col for col in self.expected_column_order
                                  if col in actual_columns]

        # Find the positions of expected columns in actual column list
        order_correct = True
        order_issues = []

        for i, expected_col in enumerate(present_expected_columns):
            actual_position = actual_columns.index(expected_col)
            expected_position = i

            if actual_position != expected_position:
                order_correct = False
                order_issues.append({
                    'column': expected_col,
                    'expected_position': expected_position,
                    'actual_position': actual_position
                })

        results = {
            'status': 'PASSED' if order_correct else 'WARNING',  # Order is often not critical
            'expected_order': self.expected_column_order,
            'actual_order': actual_columns,
            'present_expected_columns': present_expected_columns,
            'order_issues': order_issues,
            'order_correct': order_correct
        }

        if order_correct:
            print(f"✅ SUCCESS: Column ordering matches expected pattern")
            print(f"   First {len(present_expected_columns)} columns in correct order")
        else:
            print(f"⚠️  WARNING: Column ordering differs from expected pattern")
            print(f"   This may not be critical but could indicate schema changes")
            for issue in order_issues[:3]:  # Show first 3 issues
                print(f"   • {issue['column']}: position {issue['actual_position']} "
                      f"(expected: {issue['expected_position']})")

        return results

    def test_extra_undocumented_columns(self):
        """
        Test 5: Test schema with extra undocumented columns

        Returns:
            dict: Test results for undocumented columns
        """
        print("\n" + "=" * 70)
        print("TEST 5: EXTRA UNDOCUMENTED COLUMNS")
        print("=" * 70)

        if not hasattr(self, 'df') or self.df is None:
            return {'status': 'FAILED', 'error': 'Table not loaded'}

        actual_columns = set(self.df.columns)
        documented_columns = set(self.mandatory_columns.keys()) | set(self.optional_columns.keys())

        extra_columns = actual_columns - documented_columns

        results = {
            'status': 'WARNING' if extra_columns else 'PASSED',
            'documented_columns': list(documented_columns),
            'extra_columns': list(extra_columns),
            'extra_columns_count': len(extra_columns),
            'total_columns': len(actual_columns)
        }

        if not extra_columns:
            print(f"✅ SUCCESS: No undocumented columns found")
            print(f"   All {len(actual_columns)} columns are documented")
        else:
            print(f"⚠️  WARNING: {len(extra_columns)} undocumented columns found")
            print(f"   Extra columns: {sorted(extra_columns)}")
            print(f"   This may indicate:")
            print(f"   • Client-specific customizations")
            print(f"   • New columns added without documentation updates")
            print(f"   • Schema evolution that needs to be reviewed")

            # Show sample data from extra columns to help understand their purpose
            print(f"\n   Sample values from extra columns:")
            for col in list(extra_columns)[:3]:  # Show first 3 extra columns
                try:
                    sample_vals = (self.df.select(col)
                                  .filter(F.col(col).isNotNull())
                                  .limit(3)
                                  .collect())
                    vals = [row[col] for row in sample_vals]
                    print(f"   • {col}: {vals}")
                except Exception as e:
                    print(f"   • {col}: Error reading sample values - {str(e)}")

        return results

    def run_all_schema_tests(self):
        """
        Run all schema and structure tests

        Returns:
            dict: Complete test results summary
        """
        print("🧪 STARTING COMPREHENSIVE SCHEMA & STRUCTURE TESTS")
        print(f"📋 Table: {self.table_name}")
        print(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Load table first
        if not self.load_table():
            return {'status': 'FAILED', 'error': 'Could not load table'}

        # Run all tests
        test_results = {}
        test_results['test_1_mandatory_columns'] = self.test_mandatory_columns_presence()
        test_results['test_2_data_types'] = self.test_data_types_validation()
        test_results['test_3_optional_columns'] = self.test_optional_columns_handling()
        test_results['test_4_column_ordering'] = self.test_column_ordering()
        test_results['test_5_extra_columns'] = self.test_extra_undocumented_columns()

        # Overall assessment
        failed_tests = [name for name, result in test_results.items()
                       if result.get('status') == 'FAILED']
        warning_tests = [name for name, result in test_results.items()
                        if result.get('status') == 'WARNING']

        overall_status = 'FAILED' if failed_tests else ('WARNING' if warning_tests else 'PASSED')

        # Summary
        print("\n" + "=" * 70)
        print("📊 COMPREHENSIVE TEST RESULTS SUMMARY")
        print("=" * 70)

        test_results['summary'] = {
            'overall_status': overall_status,
            'total_tests': len(test_results) - 1,  # Exclude summary itself
            'passed_tests': len([r for r in test_results.values()
                               if r.get('status') == 'PASSED']),
            'warning_tests': len(warning_tests),
            'failed_tests': len(failed_tests),
            'table_name': self.table_name,
            'test_timestamp': datetime.now().isoformat()
        }

        summary = test_results['summary']

        if overall_status == 'PASSED':
            print(f"🎉 OVERALL: SCHEMA VALIDATION PASSED")
            print(f"   ✅ All {summary['total_tests']} tests passed")
            print(f"   📋 Table schema is compliant with blueprint requirements")
        elif overall_status == 'WARNING':
            print(f"⚠️  OVERALL: SCHEMA VALIDATION PASSED WITH WARNINGS")
            print(f"   ✅ {summary['passed_tests']} tests passed")
            print(f"   ⚠️  {summary['warning_tests']} tests have warnings")
            print(f"   💡 Review warnings - they may not be blocking issues")
        else:
            print(f"❌ OVERALL: SCHEMA VALIDATION FAILED")
            print(f"   ✅ {summary['passed_tests']} tests passed")
            print(f"   ⚠️  {summary['warning_tests']} tests have warnings")
            print(f"   ❌ {summary['failed_tests']} tests failed")
            print(f"   🚨 Critical issues found - blueprint processing may fail")

        print(f"\n📋 Table: {self.table_name}")
        print(f"🕐 Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)

        return test_results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples

# COMMAND ----------

# Example 1: Basic schema validation
#validator = FilterItemsSchemaValidator(
#     sandbox_schema="ydx_internal_analysts_sandbox",
#     demo_name="testesteelauder_v38"
#)
#results = validator.run_all_schema_tests()

# Example 2: Test specific client configuration
validator = FilterItemsSchemaValidator(
     sandbox_schema="ydx_internal_analysts_sandbox",
     demo_name="testesteelauder_v38",
     prod_schema="ydx_internal_analysts_gold"
)
results = validator.run_all_schema_tests()

# Example 3: Run individual tests
# validator = FilterItemsSchemaValidator("schema", "demo_name")
# validator.load_table()
# mandatory_results = validator.test_mandatory_columns_presence()
# type_results = validator.test_data_types_validation()

# Example 4: Check results programmatically
# results = validator.run_all_schema_tests()
# if results['summary']['overall_status'] == 'FAILED':
#     print("Schema validation failed - investigate before proceeding")
#     failed_tests = [name for name, result in results.items()
#                    if result.get('status') == 'FAILED']
#     print(f"Failed tests: {failed_tests}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 🧪 STARTING COMPREHENSIVE SCHEMA & STRUCTURE TESTS
# MAGIC 📋 Table: ydx_internal_analysts_sandbox.testesteelauder_v38_filter_items
# MAGIC 🕐 Started at: 2025-09-25 16:29:31
# MAGIC ======================================================================
# MAGIC TEST 1: MANDATORY COLUMNS PRESENCE
# MAGIC ======================================================================
# MAGIC ✅ SUCCESS: All 9 mandatory columns present
# MAGIC    Total columns in table: 161
# MAGIC
# MAGIC ======================================================================
# MAGIC TEST 2: DATA TYPES VALIDATION
# MAGIC ======================================================================
# MAGIC ✅ SUCCESS: All data types are valid
# MAGIC    Validated 22 columns
# MAGIC
# MAGIC ======================================================================
# MAGIC TEST 3: OPTIONAL COLUMNS HANDLING
# MAGIC ======================================================================
# MAGIC 📊 ANALYSIS: Optional columns coverage
# MAGIC    Present: 13/16 (81.2%)
# MAGIC    ✅ Present optional columns: ['age', 'channel', 'factor_age', 'full_price_estimate', 'gender', 'order_id', 'panel_end_date', 'region', 'source', 'special_attribute_column', 'state', 'web_description', 'year']
# MAGIC    ➖ Absent optional columns: ['category', 'product_id', 'subcategory']
# MAGIC       (This is acceptable - they are optional)
# MAGIC
# MAGIC ======================================================================
# MAGIC TEST 4: COLUMN ORDERING VALIDATION
# MAGIC ======================================================================
# MAGIC ⚠️  WARNING: Column ordering differs from expected pattern
# MAGIC    This may not be critical but could indicate schema changes
# MAGIC    • order_date: position 22 (expected: 0)
# MAGIC    • merchant: position 3 (expected: 1)
# MAGIC    • month: position 25 (expected: 2)
# MAGIC
# MAGIC ======================================================================
# MAGIC TEST 5: EXTRA UNDOCUMENTED COLUMNS
# MAGIC ======================================================================
# MAGIC ⚠️  WARNING: 139 undocumented columns found
# MAGIC    Extra columns: ['_inference_metadata', 'age_attribute', 'brand_original', 'category_1_proba_1', 'category_1_proba_2', 'category_1_proba_3', 'category_1_top_1', 'category_1_top_2', 'category_1_top_3', 'category_2_proba_1', 'category_2_proba_2', 'category_2_proba_3', 'category_2_top_1', 'category_2_top_2', 'category_2_top_3', 'category_3_proba_1', 'category_3_proba_2', 'category_3_proba_3', 'category_3_top_1', 'category_3_top_2', 'category_3_top_3', 'cetagory_pre_manual_remap', 'cohort_date', 'color', 'combined_cat_1_final', 'combined_cat_1_final_uct_backup', 'combined_cat_2_final', 'combined_cat_2_final_uct_backup', 'combined_cat_3_final', 'combined_cat_3_final_uct_backup', 'coverage', 'day', 'deduper', 'delivery_method', 'division', 'factor_cat', 'factor_gender', 'factor_geo', 'final_cat_1', 'final_cat_2', 'final_cat_3', 'finish', 'formulation', 'fragrance_family', 'gender_attribute', 'gmv_unadj', 'highlights', 'household_income', 'is_autoship', 'item', 'item_fidelity', 'item_id', 'item_id_org', 'item_id_v2', 'join_key', 'key_notes', 'l1', 'l2', 'l3', 'ld', 'leia_panel_flag', 'major_cat', 'manual_cat_1', 'manual_cat_2', 'manual_cat_3', 'manual_remapping', 'mapped_brand', 'match_type', 'merchant_clean', 'minor_cat', 'model_num', 'msa', 'original_brand', 'original_parent_brand', 'outlier_flag', 'panel_start_date', 'parent_brand', 'parent_sku', 'parent_sku_name', 'private_label_flag', 'pro_adj_gmv', 'product_description', 'product_hash', 'reported_category_name', 'row_id', 'scent_type', 'sds_cat_1_final', 'sds_cat_1_remap', 'sds_cat_2_final', 'sds_cat_2_remap', 'sds_cat_3_final', 'sds_cat_3_remap', 'sds_mini_final', 'sds_set_final', 'sds_uct_1', 'sds_uct_2', 'sds_uct_3', 'sds_uct_mini', 'sds_uct_set', 'seller', 'seller_name', 'size', 'sku', 'special_attribute_display', 'sub_brand', 'sub_cat', 'sub_cat_1', 'sub_cat_2', 'sub_cat_3', 'sub_cat_4', 'sub_cat_5', 'subscribe_and_save', 'terminal_cat_final', 'uct_cat_1', 'uct_cat_1_sheet', 'uct_cat_2', 'uct_cat_2_sheet', 'uct_cat_3', 'uct_cat_3_sheet', 'uct_cat_4', 'uct_cat_4_sheet', 'uct_cat_5', 'uct_cat_5_sheet', 'uct_cat_6', 'uct_cat_6_sheet', 'uct_cat_7', 'uct_cat_7_sheet', 'upc', 'variation_sku', 'variation_sku_name', 'walmart_web_cat_1', 'walmart_web_cat_2', 'walmart_web_cat_3', 'walmart_web_cat_4', 'walmart_web_cat_5', 'week', 'yd_adj_gmv', 'zip', 'zip_prefix']
# MAGIC    This may indicate:
# MAGIC    • Client-specific customizations
# MAGIC    • New columns added without documentation updates
# MAGIC    • Schema evolution that needs to be reviewed
# MAGIC
# MAGIC    Sample values from extra columns:
# MAGIC    • item_id: ['B01LZ2X7UI', 'B01N33VJZV', '2404846']
# MAGIC    • row_id: ['75280250-265a-4969-a44f-d65d9985b39a_0', 'c7a50ad7-6250-41f0-b791-5e8b787f73d9_2', 'dab0a9b0-8b79-4f3b-b722-c0695cc51e46_1']
# MAGIC    • seller_name: []
# MAGIC
# MAGIC ======================================================================
# MAGIC 📊 COMPREHENSIVE TEST RESULTS SUMMARY
# MAGIC ======================================================================
# MAGIC ⚠️  OVERALL: SCHEMA VALIDATION PASSED WITH WARNINGS
# MAGIC    ✅ 3 tests passed
# MAGIC    ⚠️  2 tests have warnings
# MAGIC    💡 Review warnings - they may not be blocking issues
# MAGIC
# MAGIC 📋 Table: ydx_internal_analysts_sandbox.testesteelauder_v38_filter_items
# MAGIC 🕐 Completed at: 2025-09-25 16:29:32
# MAGIC ======================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Integration with compare_df Function
# MAGIC
# MAGIC This schema validator can be used alongside the existing `compare_df` function to provide
# MAGIC comprehensive validation:
# MAGIC
# MAGIC 1. **Schema Validation** (this file): Ensures table structure meets blueprint requirements
# MAGIC 2. **Content Comparison** (compare_df): Ensures data content matches between implementations
# MAGIC
# MAGIC Together, they provide complete validation coverage for Freeport migrations.
