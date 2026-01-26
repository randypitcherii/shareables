"""Integration tests for hive_to_delta conversion functionality.

Tests conversion of Hive tables (via AWS Glue) to Delta tables in Unity Catalog.
Validates:
- Single table conversion with ConversionResult verification
- Bulk conversion with parallel workers
- Pattern-based table selection

Scenarios tested:
- standard: Tables in a single S3 location
- cross_bucket: Tables with partitions in different S3 buckets
- cross_region: Tables with partitions in different AWS regions

NOTE: Cross-bucket and cross-region tables require special credential configuration.
These tables are queryable via SQL Warehouse (with workspace instance profile + fallback
enabled on external locations), but NOT via Databricks Connect due to credential
resolution limitations. The tests verify conversion succeeds and Delta log is valid;
query verification is skipped for cross-bucket/cross-region when using Databricks Connect.
"""

from typing import Dict
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config

# Import from hive_to_delta package
from hive_to_delta import convert_tables
from hive_to_delta.converter import convert_single_table
from hive_to_delta.models import ConversionResult

# Import SQL warehouse query helper for cross-bucket/cross-region verification
from tests.conftest import query_via_sql_warehouse


# =============================================================================
# Configuration
# =============================================================================

# Test table configurations by scenario
# These table names match the actual test tables created in Glue
TEST_TABLES = {
    "standard": {
        "glue_tables": ["standard_table"],
        "description": "Standard table with all partitions in single location",
        "expected_regions": ["us-east-1"],
    },
    "cross_bucket": {
        "glue_tables": ["cross_bucket_table"],
        "description": "Table with partitions scattered across multiple S3 buckets",
        "expected_regions": ["us-east-1"],
    },
    "cross_region": {
        "glue_tables": ["cross_region_table"],
        "description": "Table with partitions in different AWS regions",
        "expected_regions": ["us-east-1", "us-west-2"],
    },
}


# =============================================================================
# Fixtures
# =============================================================================

# Note: spark, target_catalog, target_schema, glue_database, and aws_region
# fixtures are provided by conftest.py


@pytest.fixture(scope="session")
def s3_clients() -> Dict[str, boto3.client]:
    """S3 clients for different regions.

    Returns a dict mapping region name to boto3 S3 client.
    """
    regions = ["us-east-1", "us-west-2"]
    clients = {}
    for region in regions:
        config = Config(
            region_name=region,
            retries={"max_attempts": 3, "mode": "standard"},
        )
        clients[region] = boto3.client("s3", config=config)
    return clients


# =============================================================================
# Helper Functions
# =============================================================================


def parse_s3_path(s3_path: str) -> tuple:
    """Parse s3://bucket/key into (bucket, key)."""
    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    return bucket, key


def get_region_for_bucket(bucket: str) -> str:
    """Determine the AWS region for a bucket based on naming convention."""
    if "west-2" in bucket:
        return "us-west-2"
    return "us-east-1"


def delta_log_exists(s3_clients: Dict[str, boto3.client], table_location: str) -> bool:
    """Check if Delta log exists at the given table location.

    Args:
        s3_clients: Dict of region -> boto3 client
        table_location: S3 path to the table root

    Returns:
        True if _delta_log/00000000000000000000.json exists
    """
    bucket, prefix = parse_s3_path(table_location.rstrip("/"))
    delta_log_key = f"{prefix}/_delta_log/00000000000000000000.json"
    region = get_region_for_bucket(bucket)
    s3 = s3_clients[region]

    try:
        s3.head_object(Bucket=bucket, Key=delta_log_key)
        return True
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def list_delta_log_files(
    s3_clients: Dict[str, boto3.client], table_location: str
) -> list:
    """List all files in the _delta_log directory.

    Args:
        s3_clients: Dict of region -> boto3 client
        table_location: S3 path to the table root

    Returns:
        List of file paths in _delta_log
    """
    bucket, prefix = parse_s3_path(table_location.rstrip("/"))
    delta_log_prefix = f"{prefix}/_delta_log/"
    region = get_region_for_bucket(bucket)
    s3 = s3_clients[region]

    files = []
    paginator = s3.get_paginator("list_objects_v2")

    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=delta_log_prefix):
            for obj in page.get("Contents", []):
                files.append(f"s3://{bucket}/{obj['Key']}")
    except Exception as e:
        print(f"Warning: Error listing delta log at {table_location}: {e}")

    return files


# =============================================================================
# Test Class
# =============================================================================


class TestConversion:
    """Integration tests for Hive to Delta conversion.

    Tests conversion scenarios:
    - standard: Single location tables
    - cross_bucket: Multi-bucket partitioned tables
    - cross_region: Multi-region partitioned tables
    """

    # -------------------------------------------------------------------------
    # Single Table Conversion Tests
    # -------------------------------------------------------------------------

    @pytest.mark.standard
    @pytest.mark.parametrize("scenario", ["standard"])
    def test_convert_single_table_standard(
        self, spark, s3_clients, target_catalog, target_schema, glue_database, aws_region, sql_warehouse_id, scenario
    ):
        """Convert a single standard table and verify results."""
        self._run_single_table_conversion_test(
            spark, s3_clients, target_catalog, target_schema, glue_database, aws_region, sql_warehouse_id, scenario
        )

    @pytest.mark.cross_bucket
    @pytest.mark.parametrize("scenario", ["cross_bucket"])
    def test_convert_single_table_cross_bucket(
        self, spark, s3_clients, target_catalog, target_schema, glue_database, aws_region, sql_warehouse_id, scenario
    ):
        """Convert a single cross-bucket table and verify results."""
        self._run_single_table_conversion_test(
            spark, s3_clients, target_catalog, target_schema, glue_database, aws_region, sql_warehouse_id, scenario
        )

    @pytest.mark.cross_region
    @pytest.mark.parametrize("scenario", ["cross_region"])
    def test_convert_single_table_cross_region(
        self, spark, s3_clients, target_catalog, target_schema, glue_database, aws_region, sql_warehouse_id, scenario
    ):
        """Convert a single cross-region table and verify results."""
        self._run_single_table_conversion_test(
            spark, s3_clients, target_catalog, target_schema, glue_database, aws_region, sql_warehouse_id, scenario
        )

    def _run_single_table_conversion_test(
        self,
        spark,
        s3_clients,
        target_catalog: str,
        target_schema: str,
        glue_database: str,
        aws_region: str,
        sql_warehouse_id: str,
        scenario: str,
    ):
        """Convert one table and verify:
        - ConversionResult.success is True
        - Delta log was written to S3
        - Table is queryable (via spark.sql for standard, via SQL Warehouse for cross-bucket/cross-region)

        Args:
            spark: Active Spark session
            s3_clients: Dict of region -> boto3 client
            target_catalog: Unity Catalog name
            target_schema: Unity Catalog schema name
            glue_database: AWS Glue database name
            aws_region: AWS region
            sql_warehouse_id: SQL Warehouse ID for cross-bucket/cross-region verification
            scenario: Test scenario key from TEST_TABLES
        """
        config = TEST_TABLES[scenario]
        table_name = config["glue_tables"][0]

        print(f"\n{'='*60}")
        print(f"Testing single table conversion: {scenario}")
        print(f"Source: {glue_database}.{table_name}")
        print(f"Target: {target_catalog}.{target_schema}.{table_name}")
        print(f"Description: {config['description']}")
        print(f"{'='*60}")

        # Perform conversion
        result = convert_single_table(
            spark=spark,
            glue_database=glue_database,
            table_name=table_name,
            target_catalog=target_catalog,
            target_schema=target_schema,
            aws_region=aws_region,
        )

        print(f"\nConversion Result:")
        print(f"  Success: {result.success}")
        print(f"  File count: {result.file_count}")
        print(f"  Delta log location: {result.delta_log_location}")
        print(f"  Duration: {result.duration_seconds:.2f}s")
        if result.error:
            print(f"  Error: {result.error}")

        # Assertion 1: ConversionResult.success is True
        assert result.success, f"Conversion failed: {result.error}"
        assert isinstance(result, ConversionResult)

        # Assertion 2: Delta log was written to S3
        assert result.delta_log_location, "Delta log location should not be empty"

        # Extract table location from delta log path (remove _delta_log suffix)
        table_location = result.delta_log_location.rsplit("/_delta_log/", 1)[0]
        delta_log_files = list_delta_log_files(s3_clients, table_location)

        print(f"\nDelta log files found: {len(delta_log_files)}")
        for f in delta_log_files[:5]:
            print(f"  - {f}")

        assert len(delta_log_files) >= 1, "Delta log should have at least one file"
        assert any(
            "00000000000000000000.json" in f for f in delta_log_files
        ), "Initial commit (version 0) should exist"

        # Assertion 3: Table is queryable via spark.sql
        # NOTE: Cross-bucket/cross-region tables may fail query via Databricks Connect
        # due to credential resolution limitations. These tables ARE queryable via SQL Warehouse
        # with workspace instance profile + fallback enabled on external locations.
        target_table = f"{target_catalog}.{target_schema}.{table_name}"

        if scenario in ("cross_bucket", "cross_region"):
            # Cross-bucket/cross-region tables require SQL Warehouse with instance profile + fallback
            # Databricks Connect doesn't support this credential resolution
            if not sql_warehouse_id:
                pytest.skip("SQL warehouse required for cross-bucket/cross-region query verification")

            print(f"\nVerifying {scenario} table via SQL Warehouse (instance profile + fallback)")
            print(f"  Warehouse ID: {sql_warehouse_id}")

            # Query via SQL Warehouse API
            count_result = query_via_sql_warehouse(
                sql_warehouse_id,
                f"SELECT COUNT(*) as cnt FROM {target_table}",
            )
            row_count = int(count_result["data"][0][0])
            print(f"  Row count: {row_count}")

            assert row_count > 0, f"Table {target_table} should have at least one row"

            # Verify sample data is readable
            sample_result = query_via_sql_warehouse(
                sql_warehouse_id,
                f"SELECT * FROM {target_table} LIMIT 5",
            )
            sample_count = len(sample_result["data"])
            print(f"  Sample rows retrieved: {sample_count}")

            assert sample_count > 0, "Should be able to read sample data from table"
        else:
            row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table}").collect()[
                0
            ]["cnt"]
            print(f"\nTable query result: {row_count} rows")

            assert row_count > 0, f"Table {target_table} should have at least one row"

            # Verify sample data is readable
            sample = spark.sql(f"SELECT * FROM {target_table} LIMIT 5").collect()
            print(f"Sample rows retrieved: {len(sample)}")
            for row in sample:
                print(f"  {row}")

            assert len(sample) > 0, "Should be able to read sample data from table"

        # Verify file count matches expected
        assert result.file_count > 0, "Should have converted at least one file"

    # -------------------------------------------------------------------------
    # Bulk Conversion Tests
    # -------------------------------------------------------------------------

    @pytest.mark.skip(reason="Bulk test tables not yet created in Glue")
    @pytest.mark.standard
    def test_convert_tables_bulk(
        self, spark, s3_clients, target_catalog, target_schema, glue_database, aws_region
    ):
        """Convert multiple tables with max_workers=2.

        Verifies:
        - All tables are converted successfully
        - Results are returned for each table
        - Parallel execution completes without errors
        """
        bulk_test_tables = ["test_bulk_1", "test_bulk_2", "test_bulk_3"]

        print(f"\n{'='*60}")
        print("Testing bulk table conversion with parallel workers")
        print(f"Tables: {bulk_test_tables}")
        print(f"Max workers: 2")
        print(f"{'='*60}")

        results = convert_tables(
            spark=spark,
            glue_database=glue_database,
            tables=bulk_test_tables,
            target_catalog=target_catalog,
            target_schema=target_schema,
            aws_region=aws_region,
            max_workers=2,
        )

        print(f"\nBulk conversion results:")
        for result in results:
            status = "SUCCESS" if result.success else f"FAILED: {result.error}"
            print(f"  {result.source_table}: {status}")
            print(f"    Files: {result.file_count}, Duration: {result.duration_seconds:.2f}s")

        # Verify we got results for all tables
        assert len(results) == len(
            bulk_test_tables
        ), f"Expected {len(bulk_test_tables)} results, got {len(results)}"

        # Verify all results are ConversionResult objects
        for result in results:
            assert isinstance(
                result, ConversionResult
            ), f"Result should be ConversionResult, got {type(result)}"

        # Count successes and failures
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]

        print(f"\nSummary: {len(successful)}/{len(results)} tables converted successfully")

        # All should succeed (adjust if some test tables are expected to fail)
        assert len(successful) == len(
            bulk_test_tables
        ), f"Expected all tables to convert successfully. Failed: {[r.source_table for r in failed]}"

        # Verify each converted table is queryable
        for result in successful:
            row_count = spark.sql(
                f"SELECT COUNT(*) as cnt FROM {result.target_table}"
            ).collect()[0]["cnt"]
            print(f"  {result.target_table}: {row_count} rows")
            assert (
                row_count > 0
            ), f"Table {result.target_table} should have at least one row"

    # -------------------------------------------------------------------------
    # Pattern Matching Tests
    # -------------------------------------------------------------------------

    @pytest.mark.skip(reason="Pattern test tables not yet created in Glue")
    @pytest.mark.standard
    def test_convert_tables_pattern(
        self, spark, s3_clients, target_catalog, target_schema, glue_database, aws_region
    ):
        """Convert using pattern matching "test_*".

        Verifies:
        - Pattern matching correctly identifies tables
        - Only matching tables are converted
        - Results are returned for each matched table
        """
        pattern = "test_pattern_*"

        print(f"\n{'='*60}")
        print(f"Testing pattern-based table conversion")
        print(f"Pattern: {pattern}")
        print(f"Expected matches: test_pattern_alpha, test_pattern_beta")
        print(f"Should NOT match: test_unmatched")
        print(f"{'='*60}")

        results = convert_tables(
            spark=spark,
            glue_database=glue_database,
            tables=pattern,
            target_catalog=target_catalog,
            target_schema=target_schema,
            aws_region=aws_region,
            max_workers=2,
        )

        print(f"\nPattern conversion results:")
        for result in results:
            status = "SUCCESS" if result.success else f"FAILED: {result.error}"
            print(f"  {result.source_table}: {status}")

        # Verify pattern matching worked
        converted_tables = [r.source_table for r in results]
        print(f"\nTables matched by pattern: {converted_tables}")

        # Should match test_pattern_alpha and test_pattern_beta
        expected_matches = ["test_pattern_alpha", "test_pattern_beta"]
        for expected in expected_matches:
            assert (
                expected in converted_tables
            ), f"Pattern '{pattern}' should have matched '{expected}'"

        # Should NOT match test_unmatched
        assert (
            "test_unmatched" not in converted_tables
        ), f"Pattern '{pattern}' should NOT have matched 'test_unmatched'"

        # Verify all matched tables converted successfully
        successful = [r for r in results if r.success]
        assert len(successful) == len(
            results
        ), f"All matched tables should convert successfully"

        # Verify each converted table is queryable
        for result in successful:
            row_count = spark.sql(
                f"SELECT COUNT(*) as cnt FROM {result.target_table}"
            ).collect()[0]["cnt"]
            print(f"  {result.target_table}: {row_count} rows")
            assert (
                row_count > 0
            ), f"Table {result.target_table} should have at least one row"


# =============================================================================
# Parametrized Scenario Tests
# =============================================================================


@pytest.mark.standard
class TestStandardScenario:
    """Tests for standard scenario (single location tables)."""

    def test_standard_conversion_workflow(
        self, spark, s3_clients, target_catalog, target_schema, glue_database, aws_region
    ):
        """Full workflow test for standard scenario."""
        scenario = "standard"
        config = TEST_TABLES[scenario]

        print(f"\n{'='*60}")
        print(f"Standard scenario full workflow test")
        print(f"{'='*60}")

        for table_name in config["glue_tables"]:
            result = convert_single_table(
                spark=spark,
                glue_database=glue_database,
                table_name=table_name,
                target_catalog=target_catalog,
                target_schema=target_schema,
                aws_region=aws_region,
            )

            assert result.success, f"Conversion of {table_name} failed: {result.error}"
            print(f"  Converted {table_name}: {result.file_count} files")


@pytest.mark.cross_bucket
class TestCrossBucketScenario:
    """Tests for cross-bucket scenario (multi-bucket partitioned tables)."""

    def test_cross_bucket_conversion_workflow(
        self, spark, s3_clients, target_catalog, target_schema, glue_database, aws_region
    ):
        """Full workflow test for cross-bucket scenario."""
        scenario = "cross_bucket"
        config = TEST_TABLES[scenario]

        print(f"\n{'='*60}")
        print(f"Cross-bucket scenario full workflow test")
        print(f"{'='*60}")

        for table_name in config["glue_tables"]:
            result = convert_single_table(
                spark=spark,
                glue_database=glue_database,
                table_name=table_name,
                target_catalog=target_catalog,
                target_schema=target_schema,
                aws_region=aws_region,
            )

            assert result.success, f"Conversion of {table_name} failed: {result.error}"
            print(f"  Converted {table_name}: {result.file_count} files")


@pytest.mark.cross_region
class TestCrossRegionScenario:
    """Tests for cross-region scenario (multi-region partitioned tables)."""

    def test_cross_region_conversion_workflow(
        self, spark, s3_clients, target_catalog, target_schema, glue_database, aws_region
    ):
        """Full workflow test for cross-region scenario."""
        scenario = "cross_region"
        config = TEST_TABLES[scenario]

        print(f"\n{'='*60}")
        print(f"Cross-region scenario full workflow test")
        print(f"{'='*60}")

        for table_name in config["glue_tables"]:
            result = convert_single_table(
                spark=spark,
                glue_database=glue_database,
                table_name=table_name,
                target_catalog=target_catalog,
                target_schema=target_schema,
                aws_region=aws_region,
            )

            assert result.success, f"Conversion of {table_name} failed: {result.error}"
            print(f"  Converted {table_name}: {result.file_count} files")
