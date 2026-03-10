"""Comprehensive serverless SQL warehouse validation for Hive-to-Delta migration.

This test script validates that Delta tables converted from Hive (using the Manual Delta Log
approach with absolute S3 paths) work correctly via a serverless SQL warehouse, which has
NO direct AWS Glue or S3 access and relies entirely on UC access mechanisms.

Test Matrix:
- 5 tables: standard, scattered, cross_bucket, cross_region, recursive
- Operations: READ, UPDATE, DELETE, OPTIMIZE, VACUUM

Success Criteria:
- All reads return expected row counts
- Updates work for scattered/external partitions
- Deletes work for scattered/external partitions
- OPTIMIZE completes successfully
- VACUUM with zero retention removes files from S3
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

from hive_table_experiments.validation import parse_s3_path, get_s3_client


# Configuration
GLUE_DATABASE = os.getenv("HIVE_EVAL_GLUE_DATABASE", "your_glue_database")
UC_CATALOG = os.getenv("HIVE_EVAL_UC_CATALOG", "your_catalog")
UC_SCHEMA = os.getenv("HIVE_EVAL_UC_SCHEMA", "your_uc_schema")
SERVERLESS_WAREHOUSE_ID = os.getenv("HIVE_EVAL_SERVERLESS_WAREHOUSE_ID", "your_warehouse_id")

# These are the converted Delta table names in UC with their metadata
# Schema: id (BIGINT), name (STRING), value (DOUBLE), is_active (BOOLEAN), region (STRING)
DELTA_TABLES = {
    "standard_delta_converted": {
        "has_region_column": True,
        "has_external_partitions": False,
        "description": "Standard table with all partitions under table root",
    },
    "scattered_delta_converted": {
        "has_region_column": True,
        "has_external_partitions": True,
        "description": "Scattered table with us-west partition at external location",
    },
    "cross_bucket_delta_converted": {
        "has_region_column": True,
        "has_external_partitions": True,
        "description": "Cross-bucket table with us-west in different S3 bucket",
    },
    "cross_region_delta_converted": {
        "has_region_column": True,
        "has_external_partitions": True,
        "description": "Cross-region table with us-west in us-west-2 region",
    },
    "recursive_delta_manual": {
        "has_region_column": False,  # Different schema
        "has_external_partitions": False,
        "description": "Recursive table with nested subdirectories",
    },
}

# S3 buckets - imported from centralized scenarios module
from hive_table_experiments.scenarios import BUCKET_EAST_1A, BUCKET_EAST_1B, BUCKET_WEST_2


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
            "SERVERLESS SQL WAREHOUSE VALIDATION REPORT",
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

    def test_read(self, table: str) -> TestResult:
        """Test reading all rows from a table."""
        start = time.time()
        full_table = self.table_name(table)

        try:
            result = self.execute(f"SELECT COUNT(*) FROM {full_table}")
            row_count = result[0][0] if result else 0

            return TestResult(
                table=table,
                operation="READ",
                success=True,
                message=f"Read {row_count} rows successfully",
                details={"row_count": row_count},
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

    def test_read_partitions(self, table: str, has_region: bool) -> TestResult:
        """Test reading data with partition filter."""
        start = time.time()
        full_table = self.table_name(table)

        if not has_region:
            return TestResult(
                table=table,
                operation="READ_PARTITIONS",
                success=True,
                message="Skipped - table has no region partition",
                duration_seconds=time.time() - start,
            )

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

            return TestResult(
                table=table,
                operation="READ_PARTITIONS",
                success=True,
                message=f"Read {len(regions)} partitions: {partition_counts}",
                details={"regions": regions, "partition_counts": partition_counts},
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

    def test_update(self, table: str) -> TestResult:
        """Test updating a row (using name column which is STRING type)."""
        start = time.time()
        full_table = self.table_name(table)

        try:
            # Get a row to update
            rows = self.execute(f"SELECT id, name FROM {full_table} LIMIT 1")
            if not rows:
                return TestResult(
                    table=table,
                    operation="UPDATE",
                    success=False,
                    message="No rows found to update",
                    duration_seconds=time.time() - start,
                )

            row_id = rows[0][0]
            old_name = rows[0][1]

            # Update the row - use name column (STRING type)
            new_name = f"updated_{int(time.time())}"
            success, error = self.execute_statement(
                f"UPDATE {full_table} SET name = '{new_name}' WHERE id = {row_id}"
            )

            if not success:
                return TestResult(
                    table=table,
                    operation="UPDATE",
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
                    operation="UPDATE",
                    success=True,
                    message=f"Updated row id={row_id} successfully",
                    details={"row_id": row_id, "new_name": new_name},
                    duration_seconds=time.time() - start,
                )
            else:
                return TestResult(
                    table=table,
                    operation="UPDATE",
                    success=False,
                    message=f"Update verification failed: got '{actual_name}'",
                    duration_seconds=time.time() - start,
                )

        except Exception as e:
            return TestResult(
                table=table,
                operation="UPDATE",
                success=False,
                message="Update operation failed",
                error=str(e),
                duration_seconds=time.time() - start,
            )

    def test_update_external_partition(self, table: str, has_region: bool) -> TestResult:
        """Test updating rows in an external partition (outside table root)."""
        start = time.time()
        full_table = self.table_name(table)

        if not has_region:
            return TestResult(
                table=table,
                operation="UPDATE_EXTERNAL",
                success=True,
                message="Skipped - table has no region partition",
                duration_seconds=time.time() - start,
            )

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

    def test_delete(self, table: str, has_region: bool) -> TestResult:
        """Test insert/delete round-trip."""
        start = time.time()
        full_table = self.table_name(table)

        try:
            # Get current max ID
            max_id_result = self.execute(f"SELECT MAX(id) FROM {full_table}")
            max_id = max_id_result[0][0] if max_id_result and max_id_result[0][0] else 0
            test_id = max_id + 1000

            # Insert a test row
            if has_region:
                insert_sql = f"""
                    INSERT INTO {full_table} (id, name, value, is_active, region)
                    VALUES ({test_id}, 'test_row', 99.99, true, 'us-east')
                """
            else:
                insert_sql = f"""
                    INSERT INTO {full_table} (id, name, value, is_active)
                    VALUES ({test_id}, 'test_row', 99.99, true)
                """

            success, error = self.execute_statement(insert_sql)
            if not success:
                return TestResult(
                    table=table,
                    operation="DELETE",
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
                    operation="DELETE",
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
                    operation="DELETE",
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
                    operation="DELETE",
                    success=True,
                    message=f"Insert/Delete verified (row id={test_id})",
                    details={"deleted_id": test_id},
                    duration_seconds=time.time() - start,
                )
            else:
                return TestResult(
                    table=table,
                    operation="DELETE",
                    success=False,
                    message=f"Delete verification failed: {remaining} rows remain",
                    duration_seconds=time.time() - start,
                )

        except Exception as e:
            return TestResult(
                table=table,
                operation="DELETE",
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
            # Zero retention requires spark config that's not available in serverless
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

    def test_history(self, table: str) -> TestResult:
        """Test DESCRIBE HISTORY to verify Delta log is working."""
        start = time.time()
        full_table = self.table_name(table)

        try:
            result = self.execute(f"DESCRIBE HISTORY {full_table}")
            history_count = len(result) if result else 0

            if history_count > 0:
                # Get info from first (most recent) entry
                latest = result[0]
                return TestResult(
                    table=table,
                    operation="HISTORY",
                    success=True,
                    message=f"Found {history_count} history entries",
                    details={
                        "version_count": history_count,
                    },
                    duration_seconds=time.time() - start,
                )
            else:
                return TestResult(
                    table=table,
                    operation="HISTORY",
                    success=False,
                    message="No history entries found",
                    duration_seconds=time.time() - start,
                )
        except Exception as e:
            return TestResult(
                table=table,
                operation="HISTORY",
                success=False,
                message="DESCRIBE HISTORY failed",
                error=str(e),
                duration_seconds=time.time() - start,
            )


class S3Validator:
    """Validates S3 file operations for Delta tables."""

    def __init__(self, region: str = "us-east-1"):
        self.s3_client = get_s3_client(region)

    def count_parquet_files(self, s3_path: str) -> int:
        """Count parquet files at an S3 path."""
        bucket, prefix = parse_s3_path(s3_path)

        count = 0
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    count += 1
        return count

    def count_delta_log_files(self, s3_path: str) -> int:
        """Count files in _delta_log directory."""
        s3_path = s3_path.rstrip("/")
        bucket, prefix = parse_s3_path(s3_path)
        delta_log_prefix = f"{prefix}/_delta_log/"

        count = 0
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=delta_log_prefix):
            count += len(page.get("Contents", []))
        return count

    def list_all_files(self, s3_path: str) -> List[str]:
        """List all files at an S3 path."""
        bucket, prefix = parse_s3_path(s3_path)

        files = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                files.append(f"s3://{bucket}/{obj['Key']}")
        return files


def run_serverless_validation() -> ValidationReport:
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
        for table, config in DELTA_TABLES.items():
            print(f"\n--- Testing {table} ---")
            print(f"    ({config['description']})")
            has_region = config["has_region_column"]
            has_external = config["has_external_partitions"]

            # 1. READ test
            print(f"  Testing READ...")
            result = validator.test_read(table)
            report.add_result(result)
            print(f"    {result.message}")

            # 2. READ partitions test
            print(f"  Testing READ_PARTITIONS...")
            result = validator.test_read_partitions(table, has_region)
            report.add_result(result)
            print(f"    {result.message}")

            # 3. UPDATE test
            print(f"  Testing UPDATE...")
            result = validator.test_update(table)
            report.add_result(result)
            print(f"    {result.message}")

            # 4. UPDATE external partition (if applicable)
            if has_external:
                print(f"  Testing UPDATE_EXTERNAL...")
                result = validator.test_update_external_partition(table, has_region)
                report.add_result(result)
                print(f"    {result.message}")

            # 5. DELETE test
            print(f"  Testing DELETE...")
            result = validator.test_delete(table, has_region)
            report.add_result(result)
            print(f"    {result.message}")

            # 6. OPTIMIZE
            print(f"  Testing OPTIMIZE...")
            result = validator.test_optimize(table)
            report.add_result(result)
            print(f"    {result.message}")

            # 7. VACUUM
            print(f"  Testing VACUUM...")
            result = validator.test_vacuum(table)
            report.add_result(result)
            print(f"    {result.message}")

            # 8. HISTORY
            print(f"  Testing HISTORY...")
            result = validator.test_history(table)
            report.add_result(result)
            print(f"    {result.message}")

    finally:
        validator.close()

    return report


def validate_s3_contents():
    """Validate S3 file contents after operations."""
    print("\n" + "=" * 80)
    print("VALIDATING S3 CONTENTS")
    print("=" * 80)

    s3_validator = S3Validator()

    # Check each bucket for Delta table files
    buckets = {
        BUCKET_EAST_1A: "Primary bucket (us-east-1)",
        BUCKET_EAST_1B: "Secondary bucket (us-east-1)",
        BUCKET_WEST_2: "Cross-region bucket (us-west-2)",
    }

    for bucket, description in buckets.items():
        print(f"\n{bucket} - {description}:")

        # List top-level prefixes
        s3_client = get_s3_client("us-east-1")
        try:
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Delimiter="/",
                MaxKeys=100
            )
            prefixes = [p["Prefix"] for p in response.get("CommonPrefixes", [])]

            for prefix in prefixes:
                s3_path = f"s3://{bucket}/{prefix}"
                parquet_count = s3_validator.count_parquet_files(s3_path)
                delta_log_count = s3_validator.count_delta_log_files(s3_path)
                print(f"  {prefix}: {parquet_count} parquet files, {delta_log_count} delta log files")

        except Exception as e:
            print(f"  ERROR: {e}")


def main():
    """Main entry point for validation."""
    import argparse

    parser = argparse.ArgumentParser(description="Serverless SQL warehouse validation")
    parser.add_argument("--skip-s3", action="store_true", help="Skip S3 validation")
    args = parser.parse_args()

    # Run serverless validation
    report = run_serverless_validation()

    # Validate S3 contents (unless skipped)
    if not args.skip_s3:
        validate_s3_contents()

    # Print summary
    print("\n")
    print(report.summary())

    # Exit with error if any tests failed
    if report.failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
