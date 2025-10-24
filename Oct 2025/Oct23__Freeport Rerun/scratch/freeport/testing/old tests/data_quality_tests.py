# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Tests for filter_items Table
# MAGIC
# MAGIC This notebook contains comprehensive data quality tests for validating the content of the
# MAGIC `{sandbox_schema}.{demo_name}_filter_items` table created by blueprints.
# MAGIC
# MAGIC Tests included:
# MAGIC 1. Null values in critical columns (order_date, brand, merchant)
# MAGIC 2. Negative values in monetary fields (item_price, gmv)
# MAGIC 3. Dates outside expected range (order_date in future or too old)
# MAGIC 4. Empty strings or whitespace-only values
# MAGIC 5. Duplicate values where they shouldn't exist
# MAGIC 6. Inconsistent date formats

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import json
from datetime import datetime, date, timedelta
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Data Quality Rules Definition

# COMMAND ----------

class FilterItemsDataQualityValidator:
    """
    Comprehensive data quality validator for filter_items table created by blueprints.

    This class validates data content quality and identifies issues that could
    break downstream processing or produce incorrect analytics results.
    """

    def __init__(self, sandbox_schema, demo_name, prod_schema=None):
        """
        Initialize the data quality validator.

        Args:
            sandbox_schema (str): Sandbox schema name (e.g., "ydx_client_analysts_silver")
            demo_name (str): Demo name (e.g., "client_name_v38")
            prod_schema (str, optional): Production schema name for comparison
        """
        self.sandbox_schema = sandbox_schema
        self.demo_name = demo_name
        self.prod_schema = prod_schema
        self.table_name = f"{sandbox_schema}.{demo_name}_filter_items"

        # Define critical columns that cannot have null values
        self.critical_columns = [
            'order_date',
            'brand',
            'merchant',
            'item_price',
            'item_quantity',
            'gmv',
            'user_id'
        ]

        # Define monetary columns that should not be negative
        self.monetary_columns = [
            'item_price',
            'gmv',
            'full_price_estimate'
        ]

        # Define quantity/count columns that should not be negative
        self.quantity_columns = [
            'item_quantity'
        ]

        # Define date columns for validation
        self.date_columns = [
            'order_date',
            'panel_end_date',
            'panel_start_date',
            'cohort_date'
        ]

        # Define columns that should not have empty/whitespace values
        self.non_empty_string_columns = [
            'brand',
            'merchant',
            'user_id',
            'order_id',
            'product_id'
        ]

        # Define business rules for data ranges
        self.earliest_valid_date = date(2015, 1, 1)  # Earliest reasonable transaction date
        self.latest_valid_date = date.today() + timedelta(days=1)  # Allow today + buffer
        self.max_reasonable_price = 50000.00  # Maximum reasonable item price
        self.max_reasonable_quantity = 1000    # Maximum reasonable quantity per transaction

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

    def test_null_values_in_critical_columns(self):
        """
        Test 1: Check for null values in critical columns

        Returns:
            dict: Test results with null counts and affected columns
        """
        print("=" * 70)
        print("TEST 1: NULL VALUES IN CRITICAL COLUMNS")
        print("=" * 70)

        if not hasattr(self, 'df') or self.df is None:
            return {'status': 'FAILED', 'error': 'Table not loaded'}

        null_counts = {}
        total_rows = self.df.count()
        critical_issues = []

        for column in self.critical_columns:
            if column in self.df.columns:
                null_count = self.df.filter(F.col(column).isNull()).count()
                null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0

                null_counts[column] = {
                    'null_count': null_count,
                    'null_percentage': null_percentage,
                    'total_rows': total_rows
                }

                if null_count > 0:
                    critical_issues.append(column)

        results = {
            'status': 'FAILED' if critical_issues else 'PASSED',
            'total_rows': total_rows,
            'null_counts': null_counts,
            'columns_with_nulls': critical_issues,
            'critical_columns_checked': len([c for c in self.critical_columns if c in self.df.columns])
        }

        if not critical_issues:
            print(f"✅ SUCCESS: No null values in critical columns")
            print(f"   Validated {results['critical_columns_checked']} critical columns")
            print(f"   Total rows: {total_rows:,}")
        else:
            print(f"❌ FAILED: {len(critical_issues)} critical columns have null values")
            for column in critical_issues:
                null_info = null_counts[column]
                print(f"   • {column}: {null_info['null_count']:,} nulls "
                      f"({null_info['null_percentage']:.2f}% of {total_rows:,} rows)")

        return results

    def test_negative_values_in_monetary_fields(self):
        """
        Test 2: Check for negative values in monetary fields

        Returns:
            dict: Test results with negative value counts and statistics
        """
        print("\n" + "=" * 70)
        print("TEST 2: NEGATIVE VALUES IN MONETARY FIELDS")
        print("=" * 70)

        if not hasattr(self, 'df') or self.df is None:
            return {'status': 'FAILED', 'error': 'Table not loaded'}

        negative_issues = []
        all_monetary_columns = self.monetary_columns + self.quantity_columns
        monetary_stats = {}

        for column in all_monetary_columns:
            if column in self.df.columns:
                # Check for negative values
                negative_count = self.df.filter(F.col(column) < 0).count()

                # Get basic statistics
                stats_row = self.df.select(
                    F.min(column).alias('min_val'),
                    F.max(column).alias('max_val'),
                    F.avg(column).alias('avg_val'),
                    F.count(column).alias('non_null_count')
                ).collect()[0]

                monetary_stats[column] = {
                    'negative_count': negative_count,
                    'min_value': stats_row['min_val'],
                    'max_value': stats_row['max_val'],
                    'avg_value': stats_row['avg_val'],
                    'non_null_count': stats_row['non_null_count']
                }

                if negative_count > 0:
                    negative_issues.append(column)

        results = {
            'status': 'FAILED' if negative_issues else 'PASSED',
            'monetary_stats': monetary_stats,
            'columns_with_negatives': negative_issues,
            'monetary_columns_checked': len([c for c in all_monetary_columns if c in self.df.columns])
        }

        if not negative_issues:
            print(f"✅ SUCCESS: No negative values in monetary/quantity fields")
            print(f"   Validated {results['monetary_columns_checked']} monetary/quantity columns")
        else:
            print(f"❌ FAILED: {len(negative_issues)} columns have negative values")
            for column in negative_issues:
                stats = monetary_stats[column]
                print(f"   • {column}: {stats['negative_count']:,} negative values")
                print(f"     Range: {stats['min_value']} to {stats['max_value']}")

        # Check for unreasonably high values (potential data quality issues)
        high_value_warnings = []
        for column in ['item_price', 'gmv']:
            if column in monetary_stats:
                max_val = monetary_stats[column]['max_value']
                if max_val and max_val > self.max_reasonable_price:
                    high_value_warnings.append((column, max_val))

        if high_value_warnings:
            print(f"\n⚠️  WARNING: Potentially unreasonable high values detected")
            for column, max_val in high_value_warnings:
                print(f"   • {column}: Max value ${max_val:,.2f} exceeds ${self.max_reasonable_price:,.2f}")

        return results

    def test_date_range_validation(self):
        """
        Test 3: Check for dates outside expected range

        Returns:
            dict: Test results with date range violations
        """
        print("\n" + "=" * 70)
        print("TEST 3: DATE RANGE VALIDATION")
        print("=" * 70)

        if not hasattr(self, 'df') or self.df is None:
            return {'status': 'FAILED', 'error': 'Table not loaded'}

        date_issues = []
        date_stats = {}

        for column in self.date_columns:
            if column in self.df.columns:
                # Convert to date if it's string/timestamp
                date_col = F.col(column)
                if dict(self.df.dtypes)[column] in ['string', 'timestamp']:
                    date_col = F.to_date(F.col(column))

                # Count dates outside valid range
                too_old_count = self.df.filter(date_col < F.lit(self.earliest_valid_date)).count()
                too_new_count = self.df.filter(date_col > F.lit(self.latest_valid_date)).count()

                # Get date range statistics
                stats_row = self.df.select(
                    F.min(date_col).alias('min_date'),
                    F.max(date_col).alias('max_date'),
                    F.count(date_col).alias('non_null_count')
                ).collect()[0]

                date_stats[column] = {
                    'too_old_count': too_old_count,
                    'too_new_count': too_new_count,
                    'min_date': stats_row['min_date'],
                    'max_date': stats_row['max_date'],
                    'non_null_count': stats_row['non_null_count'],
                    'total_invalid': too_old_count + too_new_count
                }

                if too_old_count > 0 or too_new_count > 0:
                    date_issues.append(column)

        results = {
            'status': 'FAILED' if date_issues else 'PASSED',
            'date_stats': date_stats,
            'columns_with_date_issues': date_issues,
            'date_columns_checked': len([c for c in self.date_columns if c in self.df.columns]),
            'earliest_valid_date': self.earliest_valid_date,
            'latest_valid_date': self.latest_valid_date
        }

        if not date_issues:
            print(f"✅ SUCCESS: All dates are within expected ranges")
            print(f"   Validated {results['date_columns_checked']} date columns")
            print(f"   Valid date range: {self.earliest_valid_date} to {self.latest_valid_date}")
        else:
            print(f"❌ FAILED: {len(date_issues)} columns have dates outside valid range")
            for column in date_issues:
                stats = date_stats[column]
                print(f"   • {column}: {stats['total_invalid']:,} invalid dates")
                if stats['too_old_count'] > 0:
                    print(f"     - {stats['too_old_count']:,} dates before {self.earliest_valid_date}")
                if stats['too_new_count'] > 0:
                    print(f"     - {stats['too_new_count']:,} dates after {self.latest_valid_date}")
                print(f"     Range: {stats['min_date']} to {stats['max_date']}")

        return results

    def test_empty_strings_and_whitespace(self):
        """
        Test 4: Check for empty strings or whitespace-only values

        Returns:
            dict: Test results with empty/whitespace value counts
        """
        print("\n" + "=" * 70)
        print("TEST 4: EMPTY STRINGS AND WHITESPACE VALUES")
        print("=" * 70)

        if not hasattr(self, 'df') or self.df is None:
            return {'status': 'FAILED', 'error': 'Table not loaded'}

        empty_issues = []
        empty_stats = {}

        for column in self.non_empty_string_columns:
            if column in self.df.columns:
                # Count empty strings
                empty_count = self.df.filter(F.col(column) == "").count()

                # Count whitespace-only strings
                whitespace_count = self.df.filter(
                    (F.col(column).isNotNull()) &
                    (F.trim(F.col(column)) == "")
                ).count()

                # Count null values for context
                null_count = self.df.filter(F.col(column).isNull()).count()

                # Total non-null count
                non_null_count = self.df.filter(F.col(column).isNotNull()).count()

                empty_stats[column] = {
                    'empty_string_count': empty_count,
                    'whitespace_only_count': whitespace_count,
                    'null_count': null_count,
                    'non_null_count': non_null_count,
                    'total_empty_like': empty_count + whitespace_count
                }

                if empty_count > 0 or whitespace_count > 0:
                    empty_issues.append(column)

        results = {
            'status': 'FAILED' if empty_issues else 'PASSED',
            'empty_stats': empty_stats,
            'columns_with_empty_values': empty_issues,
            'string_columns_checked': len([c for c in self.non_empty_string_columns if c in self.df.columns])
        }

        if not empty_issues:
            print(f"✅ SUCCESS: No empty or whitespace-only values in critical string columns")
            print(f"   Validated {results['string_columns_checked']} string columns")
        else:
            print(f"❌ FAILED: {len(empty_issues)} columns have empty/whitespace values")
            for column in empty_issues:
                stats = empty_stats[column]
                print(f"   • {column}:")
                if stats['empty_string_count'] > 0:
                    print(f"     - {stats['empty_string_count']:,} empty strings")
                if stats['whitespace_only_count'] > 0:
                    print(f"     - {stats['whitespace_only_count']:,} whitespace-only strings")
                print(f"     - Total problematic: {stats['total_empty_like']:,}")

        return results

    def test_duplicate_values_validation(self):
        """
        Test 5: Check for duplicate values where they shouldn't exist

        Returns:
            dict: Test results with duplicate counts and examples
        """
        print("\n" + "=" * 70)
        print("TEST 5: DUPLICATE VALUES VALIDATION")
        print("=" * 70)

        if not hasattr(self, 'df') or self.df is None:
            return {'status': 'FAILED', 'error': 'Table not loaded'}

        total_rows = self.df.count()
        duplication_stats = {}

        # Check for complete duplicate rows
        distinct_rows = self.df.distinct().count()
        duplicate_rows = total_rows - distinct_rows

        duplication_stats['complete_duplicates'] = {
            'total_rows': total_rows,
            'distinct_rows': distinct_rows,
            'duplicate_count': duplicate_rows,
            'duplicate_percentage': (duplicate_rows / total_rows) * 100 if total_rows > 0 else 0
        }

        # Check for duplicates in key identifier columns
        key_columns_to_check = ['order_id', 'user_id', 'row_id']
        key_column_issues = []

        for column in key_columns_to_check:
            if column in self.df.columns:
                # Count total non-null values
                non_null_count = self.df.filter(F.col(column).isNotNull()).count()

                # Count distinct non-null values
                distinct_count = self.df.select(column).filter(F.col(column).isNotNull()).distinct().count()

                # Calculate duplicates
                duplicate_count = non_null_count - distinct_count

                duplication_stats[column] = {
                    'non_null_count': non_null_count,
                    'distinct_count': distinct_count,
                    'duplicate_count': duplicate_count,
                    'duplicate_percentage': (duplicate_count / non_null_count) * 100 if non_null_count > 0 else 0
                }

                # Flag as issue if significant duplication (for order_id, any duplication is concerning)
                if column == 'order_id' and duplicate_count > 0:
                    key_column_issues.append(column)
                elif duplicate_count > (non_null_count * 0.01):  # More than 1% duplication
                    key_column_issues.append(column)

        results = {
            'status': 'WARNING' if (duplicate_rows > 0 or key_column_issues) else 'PASSED',
            'duplication_stats': duplication_stats,
            'key_columns_with_issues': key_column_issues,
            'complete_duplicate_rows': duplicate_rows
        }

        if duplicate_rows == 0 and not key_column_issues:
            print(f"✅ SUCCESS: No problematic duplicate values found")
            print(f"   {total_rows:,} total rows, all distinct")
        else:
            print(f"⚠️  WARNING: Duplicate values detected")

            if duplicate_rows > 0:
                stats = duplication_stats['complete_duplicates']
                print(f"   • Complete duplicate rows: {duplicate_rows:,} "
                      f"({stats['duplicate_percentage']:.2f}%)")

            for column in key_column_issues:
                stats = duplication_stats[column]
                print(f"   • {column}: {stats['duplicate_count']:,} duplicates "
                      f"({stats['duplicate_percentage']:.2f}%)")

        return results

    def test_inconsistent_date_formats(self):
        """
        Test 6: Check for inconsistent date formats

        Returns:
            dict: Test results with date format inconsistencies
        """
        print("\n" + "=" * 70)
        print("TEST 6: INCONSISTENT DATE FORMATS")
        print("=" * 70)

        if not hasattr(self, 'df') or self.df is None:
            return {'status': 'FAILED', 'error': 'Table not loaded'}

        format_issues = []
        format_stats = {}

        for column in self.date_columns:
            if column in self.df.columns:
                column_type = dict(self.df.dtypes)[column]

                if column_type == 'string':
                    # For string columns, check for various date format patterns
                    # Sample some values to analyze formats
                    sample_dates = (self.df.select(column)
                                   .filter(F.col(column).isNotNull())
                                   .limit(1000)
                                   .collect())

                    date_formats_found = set()
                    invalid_format_count = 0
                    total_sampled = len(sample_dates)

                    # Common date format patterns
                    date_patterns = {
                        r'^\d{4}-\d{2}-\d{2}$': 'YYYY-MM-DD',
                        r'^\d{2}/\d{2}/\d{4}$': 'MM/DD/YYYY',
                        r'^\d{4}/\d{2}/\d{2}$': 'YYYY/MM/DD',
                        r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$': 'YYYY-MM-DD HH:MM:SS',
                        r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}': 'ISO 8601'
                    }

                    for row in sample_dates:
                        date_str = row[column]
                        if date_str:
                            matched = False
                            for pattern, format_name in date_patterns.items():
                                if re.match(pattern, str(date_str)):
                                    date_formats_found.add(format_name)
                                    matched = True
                                    break
                            if not matched:
                                invalid_format_count += 1

                    format_stats[column] = {
                        'column_type': column_type,
                        'formats_found': list(date_formats_found),
                        'format_count': len(date_formats_found),
                        'invalid_format_count': invalid_format_count,
                        'total_sampled': total_sampled,
                        'has_mixed_formats': len(date_formats_found) > 1
                    }

                    if len(date_formats_found) > 1 or invalid_format_count > 0:
                        format_issues.append(column)

                else:
                    # For date/timestamp columns, check if values can be properly parsed
                    try:
                        # Try to convert and count failures
                        conversion_test = self.df.select(
                            F.count(column).alias('total'),
                            F.count(F.to_date(F.col(column))).alias('convertible')
                        ).collect()[0]

                        unconvertible = conversion_test['total'] - conversion_test['convertible']

                        format_stats[column] = {
                            'column_type': column_type,
                            'total_values': conversion_test['total'],
                            'convertible_values': conversion_test['convertible'],
                            'unconvertible_count': unconvertible,
                            'conversion_success_rate': (conversion_test['convertible'] / conversion_test['total']) * 100 if conversion_test['total'] > 0 else 0
                        }

                        if unconvertible > 0:
                            format_issues.append(column)

                    except Exception as e:
                        format_stats[column] = {
                            'column_type': column_type,
                            'error': str(e)
                        }
                        format_issues.append(column)

        results = {
            'status': 'FAILED' if format_issues else 'PASSED',
            'format_stats': format_stats,
            'columns_with_format_issues': format_issues,
            'date_columns_checked': len([c for c in self.date_columns if c in self.df.columns])
        }

        if not format_issues:
            print(f"✅ SUCCESS: All date formats are consistent")
            print(f"   Validated {results['date_columns_checked']} date columns")
        else:
            print(f"❌ FAILED: {len(format_issues)} columns have date format issues")
            for column in format_issues:
                stats = format_stats[column]
                if 'formats_found' in stats:
                    print(f"   • {column} (string): {stats['format_count']} different formats found")
                    if stats['formats_found']:
                        print(f"     Formats: {stats['formats_found']}")
                    if stats['invalid_format_count'] > 0:
                        print(f"     Invalid formats: {stats['invalid_format_count']}/{stats['total_sampled']} sampled")
                elif 'conversion_success_rate' in stats:
                    print(f"   • {column} ({stats['column_type']}): "
                          f"{stats['conversion_success_rate']:.1f}% conversion success rate")
                    print(f"     {stats['unconvertible_count']:,} values cannot be converted to date")

        return results

    def run_all_data_quality_tests(self):
        """
        Run all data quality tests

        Returns:
            dict: Complete test results summary
        """
        print("🔍 STARTING COMPREHENSIVE DATA QUALITY TESTS")
        print(f"📋 Table: {self.table_name}")
        print(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Load table first
        if not self.load_table():
            return {'status': 'FAILED', 'error': 'Could not load table'}

        # Run all tests
        test_results = {}
        test_results['test_1_null_values'] = self.test_null_values_in_critical_columns()
        test_results['test_2_negative_values'] = self.test_negative_values_in_monetary_fields()
        test_results['test_3_date_ranges'] = self.test_date_range_validation()
        test_results['test_4_empty_strings'] = self.test_empty_strings_and_whitespace()
        test_results['test_5_duplicates'] = self.test_duplicate_values_validation()
        test_results['test_6_date_formats'] = self.test_inconsistent_date_formats()

        # Overall assessment
        failed_tests = [name for name, result in test_results.items()
                       if result.get('status') == 'FAILED']
        warning_tests = [name for name, result in test_results.items()
                        if result.get('status') == 'WARNING']

        overall_status = 'FAILED' if failed_tests else ('WARNING' if warning_tests else 'PASSED')

        # Summary
        print("\n" + "=" * 70)
        print("📊 COMPREHENSIVE DATA QUALITY RESULTS SUMMARY")
        print("=" * 70)

        test_results['summary'] = {
            'overall_status': overall_status,
            'total_tests': len(test_results) - 1,  # Exclude summary itself
            'passed_tests': len([r for r in test_results.values()
                               if r.get('status') == 'PASSED']),
            'warning_tests': len(warning_tests),
            'failed_tests': len(failed_tests),
            'table_name': self.table_name,
            'total_rows': getattr(self, 'df', None).count() if hasattr(self, 'df') and self.df else 0,
            'test_timestamp': datetime.now().isoformat()
        }

        summary = test_results['summary']

        if overall_status == 'PASSED':
            print(f"🎉 OVERALL: DATA QUALITY VALIDATION PASSED")
            print(f"   ✅ All {summary['total_tests']} tests passed")
            print(f"   📊 {summary['total_rows']:,} rows validated")
            print(f"   💡 Data quality meets blueprint requirements")
        elif overall_status == 'WARNING':
            print(f"⚠️  OVERALL: DATA QUALITY VALIDATION PASSED WITH WARNINGS")
            print(f"   ✅ {summary['passed_tests']} tests passed")
            print(f"   ⚠️  {summary['warning_tests']} tests have warnings")
            print(f"   📊 {summary['total_rows']:,} rows validated")
            print(f"   💡 Review warnings - data may still be usable")
        else:
            print(f"❌ OVERALL: DATA QUALITY VALIDATION FAILED")
            print(f"   ✅ {summary['passed_tests']} tests passed")
            print(f"   ⚠️  {summary['warning_tests']} tests have warnings")
            print(f"   ❌ {summary['failed_tests']} tests failed")
            print(f"   📊 {summary['total_rows']:,} rows analyzed")
            print(f"   🚨 Critical data quality issues found - fix before proceeding")

        print(f"\n📋 Table: {self.table_name}")
        print(f"🕐 Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)

        return test_results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples

# COMMAND ----------

# Example 1: Basic data quality validation
# validator = FilterItemsDataQualityValidator(
#     sandbox_schema="ydx_client_analysts_silver",
#     demo_name="testclient_v38"
# )
# results = validator.run_all_data_quality_tests()

# Example 2: Test specific client configuration
validator = FilterItemsDataQualityValidator(
    sandbox_schema="ydx_internal_analysts_sandbox",
    demo_name="testesteelauder_v38",
    prod_schema="ydx_internal_analysts_gold"
)
results = validator.run_all_data_quality_tests()

# Example 3: Run individual tests
# validator = FilterItemsDataQualityValidator("schema", "demo_name")
# validator.load_table()
# null_results = validator.test_null_values_in_critical_columns()
# monetary_results = validator.test_negative_values_in_monetary_fields()

# Example 4: Check results programmatically
# results = validator.run_all_data_quality_tests()
# if results['summary']['overall_status'] == 'FAILED':
#     print("Data quality validation failed - investigate before proceeding")
#     failed_tests = [name for name, result in results.items()
#                    if result.get('status') == 'FAILED']
#     print(f"Failed tests: {failed_tests}")

# Example 5: Focus on specific data quality aspects
# critical_null_check = validator.test_null_values_in_critical_columns()
# if critical_null_check['status'] == 'FAILED':
#     print("Critical columns have null values - this will break analytics")
#     print(f"Affected columns: {critical_null_check['columns_with_nulls']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Integration with Schema Tests
# MAGIC
# MAGIC This data quality validator complements the schema validation tests:
# MAGIC
# MAGIC 1. **Schema Tests** (schema_structure_tests.py): Validates table structure and column presence/types
# MAGIC 2. **Data Quality Tests** (this file): Validates actual data content and business rule compliance
# MAGIC 3. **Content Comparison** (compare_df_function.py): Ensures identical results between implementations
# MAGIC
# MAGIC **Recommended Testing Workflow:**
# MAGIC ```python
# MAGIC # Step 1: Validate schema structure
# MAGIC schema_validator = FilterItemsSchemaValidator(sandbox_schema, demo_name)
# MAGIC schema_results = schema_validator.run_all_schema_tests()
# MAGIC
# MAGIC # Step 2: Validate data quality (only if schema passes)
# MAGIC if schema_results['summary']['overall_status'] != 'FAILED':
# MAGIC     quality_validator = FilterItemsDataQualityValidator(sandbox_schema, demo_name)
# MAGIC     quality_results = quality_validator.run_all_data_quality_tests()
# MAGIC
# MAGIC # Step 3: Compare with reference implementation (if available)
# MAGIC if quality_results['summary']['overall_status'] != 'FAILED':
# MAGIC     comparison_results = compare_df(original_df, new_df)
# MAGIC ```
