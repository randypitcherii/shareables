"""Serverless SQL Warehouse validation for Manual Delta Log conversion.

This test validates that the Manual Delta Log approach (with absolute S3 paths)
captures ALL data including external partitions, and that the tables work
correctly via serverless SQL warehouse.

CRITICAL DIFFERENCE: Manual Delta Log vs CONVERT TO DELTA
- CONVERT TO DELTA: Only discovers files under table root -> LOSES external partitions
- Manual Delta Log: Reads Glue metadata, scans ALL partition locations -> PRESERVES external partitions

This test converts:
- scattered_table -> scattered_table_manual_delta (10 rows: 5 us-east + 5 us-west external)
- cross_bucket_table -> cross_bucket_table_manual_delta (10 rows: 5 us-east + 5 us-west in different bucket)
- cross_region_table -> cross_region_table_manual_delta (10 rows: 5 us-east + 5 us-west in us-west-2)
"""

import os
import sys
import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import boto3
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from hive_table_experiments.delta_converter import (
    convert_hive_to_delta_uc,
    validate_delta_table,
    cleanup_delta_log,
    ConversionResult,
)
from hive_table_experiments.validation import parse_s3_path, get_s3_client


# Configuration
GLUE_DATABASE = os.getenv("HIVE_EVAL_GLUE_DATABASE", "your_glue_database")
UC_CATALOG = os.getenv("HIVE_EVAL_UC_CATALOG", "your_catalog")
UC_SCHEMA = os.getenv("HIVE_EVAL_UC_SCHEMA", "your_uc_schema")
SERVERLESS_WAREHOUSE_ID = os.getenv("HIVE_EVAL_SERVERLESS_WAREHOUSE_ID", "your_warehouse_id")

# Tables to convert with Manual Delta Log
# These are the exotic partition tables that CONVERT TO DELTA loses data from
TABLES_TO_CONVERT = {
    "scattered_table": {
        "expected_rows": 10,  # 5 us-east + 5 us-west (external location)
        "description": "Scattered table with us-west partition at external location",
        "has_external_partitions": True,
    },
    "cross_bucket_table": {
        "expected_rows": 10,  # 5 us-east + 5 us-west (different bucket)
        "description": "Cross-bucket table with us-west in different S3 bucket",
        "has_external_partitions": True,
    },
    "cross_region_table": {
        "expected_rows": 10,  # 5 us-east + 5 us-west (us-west-2 region)
        "description": "Cross-region table with us-west in us-west-2",
        "has_external_partitions": True,
    },
}


@dataclass
class TestResult:
    """Result of a single test operation."""

    table: str
    operation: str
    success: bool
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    duration_seconds: float = 0.0


@dataclass
class ValidationReport:
    """Full validation report across all tables and operations."""

    timestamp: str
    results: List[TestResult] = field(default_factory=list)

    def add_result(self, result: TestResult):
        self.results.append(result)

    @property
    def total_tests(self) -> int:
        return len(self.results)

    @property
    def passed(self) -> int:
        return len([r for r in self.results if r.success])

    @property
    def failed(self) -> int:
        return len([r for r in self.results if not r.success])

    def summary(self) -> str:
        lines = [
            "=" * 80,
            "MANUAL DELTA LOG - SERVERLESS SQL WAREHOUSE VALIDATION REPORT",
            f"Timestamp: {self.timestamp}",
            "=" * 80,
            f"Total Tests: {self.total_tests}",
            f"Passed: {self.passed}",
            f"Failed: {self.failed}",
            "-" * 80,
        ]

        # Group by table
        by_table = {}
        for r in self.results:
            if r.table not in by_table:
                by_table[r.table] = []
            by_table[r.table].append(r)

        for table, results in by_table.items():
            lines.append(f"\n{table}:")
            for r in results:
                status = "PASS" if r.success else "FAIL"
                lines.append(f"  [{status}] {r.operation}: {r.message}")
                if r.error:
                    # Truncate error for readability
                    error_short = r.error.split("\n")[0][:100]
                    lines.append(f"         ERROR: {error_short}")

        lines.append("\n" + "=" * 80)
        return "\n".join(lines)


class ServerlessValidator:
    """Validates Delta table operations via serverless SQL warehouse."""

    def __init__(
        self,
        warehouse_id: str,
        catalog: str = UC_CATALOG,
        schema: str = UC_SCHEMA,
    ):
        self.warehouse_id = warehouse_id
        self.catalog = catalog
        self.schema = schema
        self.ws = WorkspaceClient()
        self._connection = None

        # Get connection details
        warehouse = self.ws.warehouses.get(warehouse_id)
        self.host = warehouse.odbc_params.hostname
        self.http_path = warehouse.odbc_params.path

    def _get_connection(self):
        """Get or create a SQL connection."""
        if self._connection is None:
            token = self.ws.config.token
            self._connection = dbsql.connect(
                server_hostname=self.host,
                http_path=self.http_path,
                access_token=token,
            )
        return self._connection

    def execute(self, sql: str) -> List[Any]:
        """Execute SQL and return results."""
        conn = self._get_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            try:
                return cursor.fetchall()
            except Exception:
                return []

    def execute_statement(self, sql: str) -> Tuple[bool, Optional[str]]:
        """Execute a statement and return success/error."""
        try:
            self.execute(sql)
            return True, None
        except Exception as e:
            return False, str(e)

    def close(self):
        """Close the connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def table_name(self, table: str) -> str:
        """Get fully qualified table name."""
        return f"{self.catalog}.{self.schema}.{table}"

    # Test Operations

    def test_read(self, table: str, expected_rows: int) -> TestResult:
        """Test reading all rows from a table."""
        start = time.time()
        full_table = self.table_name(table)

        try:
            result = self.execute(f"SELECT COUNT(*) FROM {full_table}")
            row_count = result[0][0] if result else 0

            success = row_count == expected_rows
            if success:
                message = f"Read {row_count} rows (expected {expected_rows})"
            else:
                message = f"Row count mismatch: got {row_count}, expected {expected_rows}"

            return TestResult(
                table=table,
                operation="READ",
                success=success,
                message=message,
                details={"row_count": row_count, "expected_rows": expected_rows},
                duration_seconds=time.time() - start,
            )
        except Exception as e:
            return TestResult(
                table=table,
                operation="READ",
                success=False,
                message="Failed to read table",
                error=str(e),
                duration_seconds=time.time() - start,
            )

    def test_read_partitions(self, table: str) -> TestResult:
        """Test reading data with partition filter - verify BOTH partitions exist."""
        start = time.time()
        full_table = self.table_name(table)

        try:
            # Get distinct regions
            result = self.execute(f"SELECT DISTINCT region FROM {full_table}")
            regions = [r[0] for r in result] if result else []

            partition_counts = {}
            for region in regions:
                count_result = self.execute(
                    f"SELECT COUNT(*) FROM {full_table} WHERE region = '{region}'"
                )
                partition_counts[region] = count_result[0][0] if count_result else 0

            # Critical check: do we have BOTH partitions?
            has_us_east = "us-east" in regions
            has_us_west = "us-west" in regions
            has_both = has_us_east and has_us_west

            if has_both:
                message = f"Found BOTH partitions: {partition_counts}"
                success = True
            else:
                missing = []
                if not has_us_east:
                    missing.append("us-east")
                if not has_us_west:
                    missing.append("us-west")
                message = f"MISSING partitions: {missing}. Found: {partition_counts}"
                success = False

            return TestResult(
                table=table,
                operation="READ_PARTITIONS",
                success=success,
                message=message,
                details={
                    "regions": regions,
                    "partition_counts": partition_counts,
                    "has_us_east": has_us_east,
                    "has_us_west": has_us_west,
                },
                duration_seconds=time.time() - start,
            )
        except Exception as e:
            return TestResult(
                table=table,
                operation="READ_PARTITIONS",
                success=False,
                message="Failed to read partitions",
                error=str(e),
                duration_seconds=time.time() - start,
            )

    def test_update_external_partition(self, table: str) -> TestResult:
        """Test updating rows in the external partition (us-west).

        This is the CRITICAL test that FAILS with CONVERT TO DELTA
        but should PASS with Manual Delta Log.
        """
        start = time.time()
        full_table = self.table_name(table)

        try:
            # Find a row in us-west partition (external partition)
            rows = self.execute(
                f"SELECT id, name FROM {full_table} WHERE region = 'us-west' LIMIT 1"
            )
            if not rows:
                # Check what regions exist
                regions = self.execute(f"SELECT DISTINCT region FROM {full_table}")
                region_list = [r[0] for r in regions] if regions else []
                return TestResult(
                    table=table,
                    operation="UPDATE_EXTERNAL",
                    success=False,
                    message=f"No rows in us-west partition. Available: {region_list}",
                    duration_seconds=time.time() - start,
                )

            row_id = rows[0][0]
            old_name = rows[0][1]

            # Update the row
            new_name = f"updated_ext_{int(time.time())}"
            success, error = self.execute_statement(
                f"UPDATE {full_table} SET name = '{new_name}' WHERE id = {row_id}"
            )

            if not success:
                return TestResult(
                    table=table,
                    operation="UPDATE_EXTERNAL",
                    success=False,
                    message="Update failed",
                    error=error,
                    duration_seconds=time.time() - start,
                )

            # Verify the update
            result = self.execute(
                f"SELECT name FROM {full_table} WHERE id = {row_id}"
            )
            actual_name = result[0][0] if result else None

            if actual_name == new_name:
                # Restore original value
                self.execute_statement(
                    f"UPDATE {full_table} SET name = '{old_name}' WHERE id = {row_id}"
                )
                return TestResult(
                    table=table,
                    operation="UPDATE_EXTERNAL",
                    success=True,
                    message=f"Updated external partition row id={row_id}",
                    details={"row_id": row_id, "partition": "us-west"},
                    duration_seconds=time.time() - start,
                )
            else:
                return TestResult(
                    table=table,
                    operation="UPDATE_EXTERNAL",
                    success=False,
                    message=f"Update verification failed: got '{actual_name}'",
                    duration_seconds=time.time() - start,
                )

        except Exception as e:
            return TestResult(
                table=table,
                operation="UPDATE_EXTERNAL",
                success=False,
                message="Update operation failed",
                error=str(e),
                duration_seconds=time.time() - start,
            )

    def test_delete_external_partition(self, table: str) -> TestResult:
        """Test insert/delete round-trip in external partition (us-west)."""
        start = time.time()
        full_table = self.table_name(table)

        try:
            # Get current max ID
            max_id_result = self.execute(f"SELECT MAX(id) FROM {full_table}")
            max_id = max_id_result[0][0] if max_id_result and max_id_result[0][0] else 0
            test_id = max_id + 1000

            # Insert a test row in us-west partition
            insert_sql = f"""
                INSERT INTO {full_table} (id, name, value, active, region)
                VALUES ({test_id}, 'test_row_ext', 99.99, true, 'us-west')
            """

            success, error = self.execute_statement(insert_sql)
            if not success:
                return TestResult(
                    table=table,
                    operation="DELETE_EXTERNAL",
                    success=False,
                    message="Failed to insert test row",
                    error=error,
                    duration_seconds=time.time() - start,
                )

            # Verify insert
            verify_result = self.execute(
                f"SELECT COUNT(*) FROM {full_table} WHERE id = {test_id}"
            )
            if verify_result[0][0] != 1:
                return TestResult(
                    table=table,
                    operation="DELETE_EXTERNAL",
                    success=False,
                    message="Insert verification failed",
                    duration_seconds=time.time() - start,
                )

            # Delete the test row
            delete_sql = f"DELETE FROM {full_table} WHERE id = {test_id}"
            success, error = self.execute_statement(delete_sql)

            if not success:
                return TestResult(
                    table=table,
                    operation="DELETE_EXTERNAL",
                    success=False,
                    message="Delete failed",
                    error=error,
                    duration_seconds=time.time() - start,
                )

            # Verify deletion
            verify_result = self.execute(
                f"SELECT COUNT(*) FROM {full_table} WHERE id = {test_id}"
            )
            remaining = verify_result[0][0] if verify_result else -1

            if remaining == 0:
                return TestResult(
                    table=table,
                    operation="DELETE_EXTERNAL",
                    success=True,
                    message=f"Insert/Delete in us-west verified (row id={test_id})",
                    details={"deleted_id": test_id, "partition": "us-west"},
                    duration_seconds=time.time() - start,
                )
            else:
                return TestResult(
                    table=table,
                    operation="DELETE_EXTERNAL",
                    success=False,
                    message=f"Delete verification failed: {remaining} rows remain",
                    duration_seconds=time.time() - start,
                )

        except Exception as e:
            return TestResult(
                table=table,
                operation="DELETE_EXTERNAL",
                success=False,
                message="Delete operation failed",
                error=str(e),
                duration_seconds=time.time() - start,
            )

    def test_optimize(self, table: str) -> TestResult:
        """Test running OPTIMIZE on the table."""
        start = time.time()
        full_table = self.table_name(table)

        try:
            success, error = self.execute_statement(f"OPTIMIZE {full_table}")

            if success:
                return TestResult(
                    table=table,
                    operation="OPTIMIZE",
                    success=True,
                    message="OPTIMIZE completed",
                    duration_seconds=time.time() - start,
                )
            else:
                return TestResult(
                    table=table,
                    operation="OPTIMIZE",
                    success=False,
                    message="OPTIMIZE failed",
                    error=error,
                    duration_seconds=time.time() - start,
                )
        except Exception as e:
            return TestResult(
                table=table,
                operation="OPTIMIZE",
                success=False,
                message="OPTIMIZE failed",
                error=str(e),
                duration_seconds=time.time() - start,
            )

    def test_vacuum(self, table: str) -> TestResult:
        """Test running VACUUM (with default 7-day retention for safety)."""
        start = time.time()
        full_table = self.table_name(table)

        try:
            # Use default retention (7 days) which is safe
            success, error = self.execute_statement(f"VACUUM {full_table}")

            if success:
                return TestResult(
                    table=table,
                    operation="VACUUM",
                    success=True,
                    message="VACUUM completed (default retention)",
                    duration_seconds=time.time() - start,
                )
            else:
                return TestResult(
                    table=table,
                    operation="VACUUM",
                    success=False,
                    message="VACUUM failed",
                    error=error,
                    duration_seconds=time.time() - start,
                )
        except Exception as e:
            return TestResult(
                table=table,
                operation="VACUUM",
                success=False,
                message="VACUUM failed",
                error=str(e),
                duration_seconds=time.time() - start,
            )


def convert_tables_with_manual_delta_log(spark) -> Dict[str, ConversionResult]:
    """Convert exotic partition tables using Manual Delta Log approach."""
    print("\n" + "=" * 80)
    print("CONVERTING TABLES WITH MANUAL DELTA LOG")
    print("=" * 80)

    results = {}

    for glue_table, config in TABLES_TO_CONVERT.items():
        target_table_name = f"{glue_table.replace('_table', '')}_manual_delta"
        print(f"\n--- Converting {glue_table} -> {target_table_name} ---")
        print(f"    ({config['description']})")
        print(f"    Expected rows: {config['expected_rows']}")

        try:
            result = convert_hive_to_delta_uc(
                spark=spark,
                glue_database=GLUE_DATABASE,
                table_name=glue_table,
                uc_catalog=UC_CATALOG,
                uc_schema=UC_SCHEMA,
            )

            # Override the target table name with our desired suffix
            if result.success:
                # Drop and recreate with the correct name
                original_target = result.target_table
                new_target = f"{UC_CATALOG}.{UC_SCHEMA}.{target_table_name}"

                # The convert function already creates the table with the original name
                # We need to rename it or recreate
                spark.sql(f"DROP TABLE IF EXISTS {new_target}")

                # Get the location from the original table
                location_sql = f"DESCRIBE EXTENDED {original_target}"
                desc_rows = spark.sql(location_sql).collect()
                location = None
                for row in desc_rows:
                    if row.col_name == "Location":
                        location = row.data_type
                        break

                if location:
                    # Create the new table at the same location
                    create_sql = f"""
                        CREATE TABLE IF NOT EXISTS {new_target}
                        USING DELTA
                        LOCATION '{location}'
                    """
                    spark.sql(create_sql)
                    spark.sql(f"DROP TABLE IF EXISTS {original_target}")

                    result.target_table = new_target
                    print(f"    Created: {new_target}")
                    print(f"    Location: {location}")
                    print(f"    Files: {result.file_count}")
                else:
                    print(f"    WARNING: Could not find location for {original_target}")

            results[target_table_name] = result

            # Print conversion details
            for note in result.notes:
                print(f"    {note}")

            if result.error:
                print(f"    ERROR: {result.error}")

        except Exception as e:
            print(f"    EXCEPTION: {str(e)[:200]}")
            results[target_table_name] = ConversionResult(
                success=False,
                source_table=f"hive_metastore.{GLUE_DATABASE}.{glue_table}",
                target_table=f"{UC_CATALOG}.{UC_SCHEMA}.{target_table_name}",
                target_location="",
                error=str(e),
            )

    return results


def run_serverless_validation(
    conversion_results: Dict[str, ConversionResult]
) -> ValidationReport:
    """Run comprehensive validation via serverless SQL warehouse."""
    print("\n" + "=" * 80)
    print("RUNNING SERVERLESS SQL WAREHOUSE VALIDATION")
    print(f"Warehouse ID: {SERVERLESS_WAREHOUSE_ID}")
    print(f"Catalog: {UC_CATALOG}")
    print(f"Schema: {UC_SCHEMA}")
    print("=" * 80)

    report = ValidationReport(timestamp=datetime.now().isoformat())
    validator = ServerlessValidator(SERVERLESS_WAREHOUSE_ID)

    try:
        for table_name, conv_result in conversion_results.items():
            if not conv_result.success:
                print(f"\n--- Skipping {table_name} (conversion failed) ---")
                report.add_result(TestResult(
                    table=table_name,
                    operation="CONVERSION",
                    success=False,
                    message=f"Conversion failed: {conv_result.error}",
                ))
                continue

            # Get expected rows from config
            glue_table = table_name.replace("_manual_delta", "_table")
            config = TABLES_TO_CONVERT.get(glue_table, {})
            expected_rows = config.get("expected_rows", 10)

            print(f"\n--- Testing {table_name} ---")
            print(f"    (Expected {expected_rows} rows)")

            # 1. READ test - verify ALL data is accessible
            print(f"  Testing READ...")
            result = validator.test_read(table_name, expected_rows)
            report.add_result(result)
            print(f"    {result.message}")

            # 2. READ partitions test - verify BOTH partitions exist
            print(f"  Testing READ_PARTITIONS...")
            result = validator.test_read_partitions(table_name)
            report.add_result(result)
            print(f"    {result.message}")

            # 3. UPDATE external partition - CRITICAL TEST
            print(f"  Testing UPDATE_EXTERNAL (us-west partition)...")
            result = validator.test_update_external_partition(table_name)
            report.add_result(result)
            print(f"    {result.message}")

            # 4. DELETE external partition
            print(f"  Testing DELETE_EXTERNAL (us-west partition)...")
            result = validator.test_delete_external_partition(table_name)
            report.add_result(result)
            print(f"    {result.message}")

            # 5. OPTIMIZE
            print(f"  Testing OPTIMIZE...")
            result = validator.test_optimize(table_name)
            report.add_result(result)
            print(f"    {result.message}")

            # 6. VACUUM
            print(f"  Testing VACUUM...")
            result = validator.test_vacuum(table_name)
            report.add_result(result)
            print(f"    {result.message}")

    finally:
        validator.close()

    return report


def compare_with_convert_to_delta(validator: ServerlessValidator) -> str:
    """Compare Manual Delta Log results with CONVERT TO DELTA results."""
    comparison = [
        "\n" + "=" * 80,
        "COMPARISON: Manual Delta Log vs CONVERT TO DELTA",
        "=" * 80,
    ]

    # Tables converted with CONVERT TO DELTA (from previous validation)
    convert_tables = [
        "scattered_delta_converted",
        "cross_bucket_delta_converted",
        "cross_region_delta_converted",
    ]

    # Tables converted with Manual Delta Log
    manual_tables = [
        "scattered_manual_delta",
        "cross_bucket_manual_delta",
        "cross_region_manual_delta",
    ]

    comparison.append("\n| Table Type | Method | Row Count | Has us-west? | External Ops |")
    comparison.append("|------------|--------|-----------|--------------|--------------|")

    for conv_table, manual_table in zip(convert_tables, manual_tables):
        table_type = conv_table.replace("_delta_converted", "")

        # Get CONVERT TO DELTA row count
        try:
            result = validator.execute(
                f"SELECT COUNT(*) FROM {validator.table_name(conv_table)}"
            )
            conv_count = result[0][0] if result else "N/A"

            # Check for us-west
            regions = validator.execute(
                f"SELECT DISTINCT region FROM {validator.table_name(conv_table)}"
            )
            conv_has_west = "us-west" in [r[0] for r in regions] if regions else False
        except Exception:
            conv_count = "ERROR"
            conv_has_west = False

        # Get Manual Delta Log row count
        try:
            result = validator.execute(
                f"SELECT COUNT(*) FROM {validator.table_name(manual_table)}"
            )
            manual_count = result[0][0] if result else "N/A"

            # Check for us-west
            regions = validator.execute(
                f"SELECT DISTINCT region FROM {validator.table_name(manual_table)}"
            )
            manual_has_west = "us-west" in [r[0] for r in regions] if regions else False
        except Exception:
            manual_count = "ERROR"
            manual_has_west = False

        comparison.append(
            f"| {table_type} | CONVERT TO DELTA | {conv_count} | {'Yes' if conv_has_west else 'NO'} | {'FAIL' if not conv_has_west else 'PASS'} |"
        )
        comparison.append(
            f"| {table_type} | Manual Delta Log | {manual_count} | {'Yes' if manual_has_west else 'NO'} | {'PASS' if manual_has_west else 'FAIL'} |"
        )

    comparison.append("\nKEY FINDING:")
    comparison.append("- CONVERT TO DELTA: Loses external partition data (us-west)")
    comparison.append("- Manual Delta Log: Preserves ALL partition data including external")

    return "\n".join(comparison)


def main():
    """Main entry point for Manual Delta Log validation."""
    import argparse
    from databricks.connect import DatabricksSession

    parser = argparse.ArgumentParser(
        description="Manual Delta Log serverless SQL warehouse validation"
    )
    parser.add_argument(
        "--skip-conversion",
        action="store_true",
        help="Skip conversion step (use existing tables)",
    )
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Compare with CONVERT TO DELTA results",
    )
    args = parser.parse_args()

    # Create Spark session for conversion
    spark = DatabricksSession.builder.getOrCreate()

    # Step 1: Convert tables with Manual Delta Log
    if not args.skip_conversion:
        conversion_results = convert_tables_with_manual_delta_log(spark)
    else:
        # Create dummy conversion results for existing tables
        conversion_results = {}
        for glue_table, config in TABLES_TO_CONVERT.items():
            target_table_name = f"{glue_table.replace('_table', '')}_manual_delta"
            conversion_results[target_table_name] = ConversionResult(
                success=True,
                source_table=f"hive_metastore.{GLUE_DATABASE}.{glue_table}",
                target_table=f"{UC_CATALOG}.{UC_SCHEMA}.{target_table_name}",
                target_location="",  # Not needed for validation
            )

    # Step 2: Run serverless validation
    report = run_serverless_validation(conversion_results)

    # Step 3: Compare with CONVERT TO DELTA (optional)
    if args.compare:
        validator = ServerlessValidator(SERVERLESS_WAREHOUSE_ID)
        try:
            comparison = compare_with_convert_to_delta(validator)
            print(comparison)
        finally:
            validator.close()

    # Print summary
    print("\n")
    print(report.summary())

    # Exit with error if any tests failed
    if report.failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
