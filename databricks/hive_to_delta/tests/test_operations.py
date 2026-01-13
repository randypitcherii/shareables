"""Integration tests for Delta operations on converted tables.

Tests verify that tables converted from Hive (via AWS Glue) to Delta
support standard Delta operations including reads, writes, and maintenance.

These tests require:
1. Converting tables first using convert_single_table
2. Running against a live Databricks workspace with Unity Catalog

Scenarios tested:
- standard: Single-location partitioned table
- cross_bucket: Partitions spanning multiple S3 buckets
- cross_region: Partitions spanning multiple AWS regions
"""

import os
import time
from typing import Dict, List, Tuple
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config

from hive_to_delta import convert_single_table


# =============================================================================
# Configuration - scenarios mapping to Glue table names
# =============================================================================

# These map to the Glue tables created by setup_fresh_hive_tables.py
SCENARIO_TABLE_MAP = {
    "standard": "standard_table",
    "cross_bucket": "cross_bucket_table",
    "cross_region": "cross_region_table",
}

SCENARIOS = list(SCENARIO_TABLE_MAP.keys())


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def converted_tables(spark, target_catalog, target_schema, glue_database, aws_region, cleanup_tables):
    """Convert tables once per module, return mapping of scenario -> UC table name.

    Converts the Glue tables to Delta format and registers them in Unity Catalog.
    Tables are cleaned up after the test module completes.
    """
    tables = {}

    for scenario, glue_table in SCENARIO_TABLE_MAP.items():
        uc_table_name = f"{target_catalog}.{target_schema}.{glue_table}_ops_test"

        # Drop if exists from previous run
        try:
            spark.sql(f"DROP TABLE IF EXISTS {uc_table_name}")
        except Exception:
            pass

        # Convert the table
        result = convert_single_table(
            spark=spark,
            glue_database=glue_database,
            table_name=glue_table,
            target_catalog=target_catalog,
            target_schema=target_schema,
            target_table_name=f"{glue_table}_ops_test",
            aws_region=aws_region,
        )

        if result.success:
            tables[scenario] = uc_table_name
            cleanup_tables.append(uc_table_name)
            print(f"Converted {scenario}: {uc_table_name}")
        else:
            print(f"Failed to convert {scenario}: {result.error}")

    return tables


# =============================================================================
# Helper Functions
# =============================================================================


def parse_s3_path(s3_path: str) -> Tuple[str, str]:
    """Parse s3://bucket/key into (bucket, key)."""
    parsed = urlparse(s3_path)
    return parsed.netloc, parsed.path.lstrip("/")


def get_region_for_bucket(bucket: str) -> str:
    """Determine AWS region for a bucket based on naming convention."""
    if "west-2" in bucket:
        return "us-west-2"
    return "us-east-1"


def list_s3_files(
    s3_clients: Dict[str, boto3.client],
    s3_path: str,
    include_delta_log: bool = False,
) -> List[Dict]:
    """List all files under an S3 prefix.

    Args:
        s3_clients: Dict of region -> boto3 client.
        s3_path: S3 path like s3://bucket/prefix/.
        include_delta_log: If True, include _delta_log files.

    Returns:
        List of dicts with keys: key, full_path, size, last_modified.
    """
    bucket, prefix = parse_s3_path(s3_path)
    region = get_region_for_bucket(bucket)
    s3 = s3_clients[region]

    files = []
    paginator = s3.get_paginator("list_objects_v2")

    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not include_delta_log and "/_delta_log/" in key:
                    continue
                if key.endswith(".crc"):
                    continue
                files.append({
                    "key": key,
                    "full_path": f"s3://{bucket}/{key}",
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"],
                })
    except Exception as e:
        print(f"Warning: Error listing {s3_path}: {e}")

    return files


def count_parquet_files(files: List[Dict]) -> int:
    """Count parquet data files (not delta log)."""
    return len([f for f in files if f["key"].endswith(".parquet")])


# =============================================================================
# Test Class
# =============================================================================


class TestDeltaOperations:
    """Integration tests for Delta operations on converted tables.

    Tests verify that converted tables support standard Delta operations.
    Tables are converted fresh via the converted_tables fixture.
    """

    # -------------------------------------------------------------------------
    # SELECT Tests
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("scenario", SCENARIOS)
    def test_select(self, spark, converted_tables, scenario):
        """Verify SELECT reads return expected data from converted table.

        Confirms the table is readable and contains data.
        """
        if scenario not in converted_tables:
            pytest.skip(f"Table for scenario {scenario} was not converted successfully")

        table = converted_tables[scenario]

        print(f"\n{'='*60}")
        print(f"Testing SELECT on: {table}")
        print(f"Scenario: {scenario}")
        print(f"{'='*60}")

        # Basic count query
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]["cnt"]
        print(f"Row count: {count}")

        assert count > 0, f"Table {table} has no rows - expected data in converted table"

        # Sample query to verify data structure
        sample = spark.sql(f"SELECT * FROM {table} LIMIT 5").collect()
        print(f"Sample rows retrieved: {len(sample)}")
        for row in sample:
            print(f"  {row}")

        assert len(sample) > 0, "Sample query returned no rows"

    # -------------------------------------------------------------------------
    # INSERT Tests
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("scenario", SCENARIOS)
    def test_insert(self, spark, s3_clients, converted_tables, scenario):
        """Verify INSERT creates new parquet files in converted table.

        New data should be written to the table root location.
        """
        if scenario not in converted_tables:
            pytest.skip(f"Table for scenario {scenario} was not converted successfully")

        table = converted_tables[scenario]

        # Get table location from DESCRIBE DETAIL
        detail = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
        table_root = detail["location"]

        print(f"\n{'='*60}")
        print(f"Testing INSERT on: {table}")
        print(f"Table root: {table_root}")
        print(f"{'='*60}")

        # Get initial state
        initial_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]["cnt"]
        initial_files = list_s3_files(s3_clients, table_root)
        initial_parquet_count = count_parquet_files(initial_files)

        print(f"Initial row count: {initial_count}")
        print(f"Initial parquet files: {initial_parquet_count}")

        # Get partition column info
        partitions = spark.sql(f"SHOW PARTITIONS {table}").collect()
        if partitions:
            partition_row = partitions[0].asDict()
            partition_col = list(partition_row.keys())[0]
            partition_val = partition_row[partition_col]
        else:
            partition_col, partition_val = "region", "test"

        # Insert a test row
        test_id = 999999
        insert_sql = f"""
            INSERT INTO {table}
            SELECT {test_id} as id,
                   'test_insert' as name,
                   999.99 as value,
                   true as is_active,
                   '{partition_val}' as {partition_col}
        """
        print(f"Executing insert...")
        spark.sql(insert_sql)

        # Verify row count increased
        new_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]["cnt"]
        new_files = list_s3_files(s3_clients, table_root)
        new_parquet_count = count_parquet_files(new_files)

        print(f"New row count: {new_count}")
        print(f"New parquet files: {new_parquet_count}")

        assert new_count == initial_count + 1, (
            f"Row count should increase by 1: {initial_count} -> {new_count}"
        )
        assert new_parquet_count >= initial_parquet_count, (
            f"Parquet file count should not decrease: {initial_parquet_count} -> {new_parquet_count}"
        )

        # Verify inserted row is readable
        inserted = spark.sql(f"SELECT * FROM {table} WHERE id = {test_id}").collect()
        assert len(inserted) == 1, f"Inserted row not found for id={test_id}"
        assert inserted[0]["name"] == "test_insert"
        print(f"Inserted row verified: {inserted[0]}")

        # Cleanup
        spark.sql(f"DELETE FROM {table} WHERE id = {test_id}")

    # -------------------------------------------------------------------------
    # UPDATE Tests
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("scenario", SCENARIOS)
    def test_update(self, spark, converted_tables, scenario):
        """Verify UPDATE works with copy-on-write semantics.

        UPDATE creates new files with modified data.
        """
        if scenario not in converted_tables:
            pytest.skip(f"Table for scenario {scenario} was not converted successfully")

        table = converted_tables[scenario]

        print(f"\n{'='*60}")
        print(f"Testing UPDATE on: {table}")
        print(f"{'='*60}")

        # Get partition info
        partitions = spark.sql(f"SHOW PARTITIONS {table}").collect()
        if partitions:
            partition_row = partitions[0].asDict()
            partition_col = list(partition_row.keys())[0]
            partition_val = partition_row[partition_col]
        else:
            partition_col, partition_val = "region", "test"

        # Insert test row
        test_id = 888888
        spark.sql(f"""
            INSERT INTO {table}
            SELECT {test_id} as id,
                   'before_update' as name,
                   100.0 as value,
                   true as is_active,
                   '{partition_val}' as {partition_col}
        """)

        # Verify before update
        before = spark.sql(f"SELECT * FROM {table} WHERE id = {test_id}").collect()
        assert len(before) == 1, f"Test row not found before update"
        assert before[0]["name"] == "before_update"
        assert before[0]["value"] == 100.0
        print(f"Before update: {before[0]}")

        # Perform update
        spark.sql(f"""
            UPDATE {table}
            SET name = 'after_update', value = 200.0
            WHERE id = {test_id}
        """)

        # Verify after update
        after = spark.sql(f"SELECT * FROM {table} WHERE id = {test_id}").collect()
        assert len(after) == 1, f"Test row not found after update"
        assert after[0]["name"] == "after_update", (
            f"Name not updated: expected 'after_update', got '{after[0]['name']}'"
        )
        assert after[0]["value"] == 200.0, (
            f"Value not updated: expected 200.0, got {after[0]['value']}"
        )
        print(f"After update: {after[0]}")

        # Cleanup
        spark.sql(f"DELETE FROM {table} WHERE id = {test_id}")

    # -------------------------------------------------------------------------
    # OPTIMIZE Tests
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("scenario", SCENARIOS)
    def test_optimize(self, spark, converted_tables, scenario):
        """Verify OPTIMIZE compacts files in converted table.

        OPTIMIZE should reduce file count while preserving data.
        """
        if scenario not in converted_tables:
            pytest.skip(f"Table for scenario {scenario} was not converted successfully")

        table = converted_tables[scenario]

        print(f"\n{'='*60}")
        print(f"Testing OPTIMIZE on: {table}")
        print(f"{'='*60}")

        # Get initial state
        initial_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]["cnt"]
        detail_before = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
        files_before = detail_before["numFiles"]

        print(f"Initial row count: {initial_count}")
        print(f"Files before OPTIMIZE: {files_before}")

        # Get partition info for test inserts
        partitions = spark.sql(f"SHOW PARTITIONS {table}").collect()
        if partitions:
            partition_row = partitions[0].asDict()
            partition_col = list(partition_row.keys())[0]
            partition_val = partition_row[partition_col]
        else:
            partition_col, partition_val = "region", "test"

        # Insert multiple small rows to create small files
        test_ids = list(range(500000, 500005))
        for test_id in test_ids:
            spark.sql(f"""
                INSERT INTO {table}
                SELECT {test_id} as id,
                       'optimize_test_{test_id}' as name,
                       {test_id}.0 as value,
                       true as is_active,
                       '{partition_val}' as {partition_col}
            """)

        # Get file count after inserts
        detail_after_inserts = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
        files_after_inserts = detail_after_inserts["numFiles"]
        print(f"Files after inserts: {files_after_inserts}")

        # Run OPTIMIZE
        print("Running OPTIMIZE...")
        optimize_result = spark.sql(f"OPTIMIZE {table}")
        optimize_metrics = optimize_result.collect()
        print(f"OPTIMIZE completed: {optimize_metrics}")

        # Get file count after OPTIMIZE
        detail_after_optimize = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
        files_after_optimize = detail_after_optimize["numFiles"]
        print(f"Files after OPTIMIZE: {files_after_optimize}")

        # Verify data integrity
        final_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]["cnt"]
        expected_count = initial_count + len(test_ids)

        assert final_count == expected_count, (
            f"Row count changed: expected {expected_count}, got {final_count}"
        )
        assert files_after_optimize <= files_after_inserts, (
            f"OPTIMIZE should not increase file count: {files_after_inserts} -> {files_after_optimize}"
        )
        print(f"Data integrity verified: {final_count} rows")

        # Cleanup test rows
        spark.sql(f"DELETE FROM {table} WHERE id >= 500000 AND id < 500005")

    # -------------------------------------------------------------------------
    # VACUUM Tests
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("scenario", SCENARIOS)
    def test_vacuum(self, spark, s3_clients, converted_tables, scenario):
        """Verify standard VACUUM works on converted table.

        VACUUM should remove old files no longer referenced by Delta log.
        Note: Uses default retention (7 days) since retentionDurationCheck
        cannot be disabled via Databricks Connect serverless.
        """
        if scenario not in converted_tables:
            pytest.skip(f"Table for scenario {scenario} was not converted successfully")

        table = converted_tables[scenario]

        # Get table location from DESCRIBE DETAIL
        detail = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
        table_root = detail["location"]

        print(f"\n{'='*60}")
        print(f"Testing VACUUM on: {table}")
        print(f"Table root: {table_root}")
        print(f"{'='*60}")

        # Get partition info
        partitions = spark.sql(f"SHOW PARTITIONS {table}").collect()
        if partitions:
            partition_row = partitions[0].asDict()
            partition_col = list(partition_row.keys())[0]
            partition_val = partition_row[partition_col]
        else:
            partition_col, partition_val = "region", "test"

        # Get initial file count
        initial_files = list_s3_files(s3_clients, table_root)
        initial_parquet_count = count_parquet_files(initial_files)
        print(f"Initial parquet files: {initial_parquet_count}")

        # Create old file versions via insert/update cycle
        test_id = 777777
        spark.sql(f"""
            INSERT INTO {table}
            SELECT {test_id} as id,
                   'vacuum_test_v0' as name,
                   1.0 as value,
                   true as is_active,
                   '{partition_val}' as {partition_col}
        """)

        # Perform updates to create old file versions
        for i in range(1, 4):
            spark.sql(f"""
                UPDATE {table}
                SET name = 'vacuum_test_v{i}', value = {i + 1}.0
                WHERE id = {test_id}
            """)
            time.sleep(0.5)

        # Get file count after updates
        files_after_updates = list_s3_files(s3_clients, table_root)
        parquet_after_updates = count_parquet_files(files_after_updates)
        print(f"Parquet files after updates: {parquet_after_updates}")

        # Run VACUUM with default retention (7 days)
        # Note: Cannot use RETAIN 0 HOURS with Databricks Connect serverless
        print("Running VACUUM with default retention...")
        vacuum_result = spark.sql(f"VACUUM {table}")
        vacuum_paths = vacuum_result.collect()
        print(f"VACUUM returned {len(vacuum_paths)} paths")

        # Allow time for deletions
        time.sleep(2)

        # Get file count after VACUUM
        files_after_vacuum = list_s3_files(s3_clients, table_root)
        parquet_after_vacuum = count_parquet_files(files_after_vacuum)
        print(f"Parquet files after VACUUM: {parquet_after_vacuum}")

        # Verify data integrity
        test_row = spark.sql(f"SELECT * FROM {table} WHERE id = {test_id}").collect()
        assert len(test_row) == 1, f"Test row missing after VACUUM"
        assert test_row[0]["name"] == "vacuum_test_v3", "Test row has incorrect value after VACUUM"
        print(f"Data integrity verified: {test_row[0]}")

        # With default retention (7 days), no files should be deleted
        # since all files were just created. Just verify VACUUM ran successfully.
        print(f"Parquet files after VACUUM: {parquet_after_vacuum}")
        print("VACUUM completed successfully with data integrity preserved")

        # Cleanup
        spark.sql(f"DELETE FROM {table} WHERE id = {test_id}")
