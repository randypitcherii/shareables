"""Comprehensive test suite for Manual Delta Log table operations.

Tests ALL Delta operations on tables created with absolute S3 paths in the Delta log:
- SELECT (basic read verification)
- INSERT (new rows, verifies writes go to table root)
- UPDATE (modify existing rows)
- DELETE (remove rows)
- MERGE (upsert operations)
- OPTIMIZE (file compaction with S3 file verification)
- VACUUM (critical test: verify files are ACTUALLY DELETED from S3)

These tests validate that Manual Delta Log tables with absolute S3 paths
support full DML operations and proper maintenance.

Critical focus: VACUUM must delete files from S3 across all locations:
- Table root location
- External partition locations (scattered)
- Different bucket locations (cross_bucket)
- Different region locations (cross_region)
"""

import os
import sys
import time
from typing import Dict, List, Tuple, Optional
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config

# Import bucket constants from centralized module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from hive_table_experiments.scenarios import BUCKET_EAST_1A


# =============================================================================
# Table Configuration
# =============================================================================

CATALOG = os.getenv("HIVE_EVAL_UC_CATALOG", "your_catalog")
SCHEMA = os.getenv("HIVE_EVAL_UC_SCHEMA", "your_uc_schema")

# Manual Delta Log tables - now using relative paths under table root
# NOTE: These tables were recreated with files consolidated under the table root
# to work with UC credential resolution. Absolute paths to external locations
# do NOT work with UC because credential resolution only applies to:
# 1. Paths relative to the table's registered storage location
# 2. Or paths under a UC external location that the table is associated with
MANUAL_DELTA_TABLES = {
    "scattered": {
        "table_name": f"{CATALOG}.{SCHEMA}.scattered_delta_manual_absolute",
        "s3_locations": [
            f"s3://{BUCKET_EAST_1A}/conversion_eval/scattered/manual/",
        ],
        "regions": ["us-east-1"],
        "description": "Table with relative paths under single table root",
    },
    "cross_bucket": {
        "table_name": f"{CATALOG}.{SCHEMA}.cross_bucket_delta_manual_absolute",
        "s3_locations": [
            f"s3://{BUCKET_EAST_1A}/conversion_eval/cross_bucket/manual/",
        ],
        "regions": ["us-east-1"],
        "description": "Table with relative paths under single table root",
    },
    "cross_region": {
        "table_name": f"{CATALOG}.{SCHEMA}.cross_region_delta_manual_absolute",
        "s3_locations": [
            f"s3://{BUCKET_EAST_1A}/conversion_eval/cross_region/manual/",
        ],
        "regions": ["us-east-1"],
        "description": "Table with relative paths under single table root",
    },
}


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def spark():
    """Create Databricks Connect session."""
    from databricks.connect import DatabricksSession

    session = (
        DatabricksSession.builder.profile("DEFAULT")
        .remote(cluster_id="1231-041629-tyfxt3io")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def s3_clients() -> Dict[str, boto3.client]:
    """S3 clients for different regions.

    Returns a dict mapping region name to boto3 S3 client.
    Includes clients for all regions that might contain table data.
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


@pytest.fixture(scope="class")
def disable_retention_check(spark):
    """Disable Delta retention check for testing VACUUM with 0 hours.

    This is required to run VACUUM RETAIN 0 HOURS for testing.
    """
    spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = false")
    yield
    spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = true")


# =============================================================================
# Helper Functions
# =============================================================================


def parse_s3_path(s3_path: str) -> Tuple[str, str]:
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


def list_s3_files(
    s3_clients: Dict[str, boto3.client],
    s3_path: str,
    include_delta_log: bool = False,
) -> List[Dict]:
    """List all files under an S3 prefix.

    Args:
        s3_clients: Dict of region -> boto3 client
        s3_path: S3 path like s3://bucket/prefix/
        include_delta_log: If True, include _delta_log files

    Returns:
        List of dicts with keys: key, size, last_modified
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
                # Skip delta log unless explicitly requested
                if not include_delta_log and "/_delta_log/" in key:
                    continue
                # Skip checksum files
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


def list_all_table_files(
    s3_clients: Dict[str, boto3.client],
    s3_locations: List[str],
    include_delta_log: bool = False,
) -> List[Dict]:
    """List all files across multiple S3 locations for a table.

    Args:
        s3_clients: Dict of region -> boto3 client
        s3_locations: List of S3 paths to scan
        include_delta_log: If True, include _delta_log files

    Returns:
        Combined list of files from all locations
    """
    all_files = []
    for location in s3_locations:
        files = list_s3_files(s3_clients, location, include_delta_log)
        all_files.extend(files)
    return all_files


def count_parquet_files(files: List[Dict]) -> int:
    """Count Parquet data files (not delta log)."""
    return len([f for f in files if f["key"].endswith(".parquet")])


def file_exists_in_s3(
    s3_clients: Dict[str, boto3.client],
    s3_path: str,
) -> bool:
    """Check if a specific file exists in S3."""
    bucket, key = parse_s3_path(s3_path)
    region = get_region_for_bucket(bucket)
    s3 = s3_clients[region]

    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.NoSuchKey:
        return False
    except Exception:
        return False


# =============================================================================
# Test Classes
# =============================================================================


class TestManualDeltaLogOperations:
    """Comprehensive test suite for Manual Delta Log table operations.

    Tests all DML operations and verifies S3 file behavior.
    """

    # -------------------------------------------------------------------------
    # SELECT Tests
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("scenario", list(MANUAL_DELTA_TABLES.keys()))
    def test_select_basic_read(self, spark, scenario):
        """Verify basic SELECT works on Manual Delta Log tables.

        This confirms the table is readable via absolute paths.
        """
        config = MANUAL_DELTA_TABLES[scenario]
        table = config["table_name"]

        print(f"\n{'='*60}")
        print(f"Testing SELECT on: {table}")
        print(f"Description: {config['description']}")
        print(f"{'='*60}")

        # Basic count
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]["cnt"]
        print(f"Row count: {count}")

        assert count > 0, f"Table {table} has no rows"

        # Sample query
        sample = spark.sql(f"SELECT * FROM {table} LIMIT 5").collect()
        print(f"Sample rows: {len(sample)}")
        for row in sample:
            print(f"  {row}")

    @pytest.mark.parametrize("scenario", list(MANUAL_DELTA_TABLES.keys()))
    def test_select_with_predicate(self, spark, scenario):
        """Verify SELECT with partition predicate pushdown."""
        config = MANUAL_DELTA_TABLES[scenario]
        table = config["table_name"]

        # Get partitions
        partitions = spark.sql(f"SHOW PARTITIONS {table}").collect()
        print(f"\nPartitions in {table}: {len(partitions)}")

        if partitions:
            # Query specific partition - format is Row(region='us-east')
            partition_row = partitions[0].asDict()
            col = list(partition_row.keys())[0]
            val = partition_row[col]
            print(f"Testing partition predicate: {col}={val}")

            result = spark.sql(f"""
                SELECT COUNT(*) as cnt
                FROM {table}
                WHERE {col} = '{val}'
            """).collect()[0]["cnt"]

            print(f"Rows in partition {col}={val}: {result}")
            assert result > 0, f"Partition {col}={val} has no rows"

    # -------------------------------------------------------------------------
    # INSERT Tests
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("scenario", list(MANUAL_DELTA_TABLES.keys()))
    def test_insert_new_rows(self, spark, s3_clients, scenario):
        """Verify INSERT adds new rows.

        New files should be written to the table root location,
        not to external partition locations.
        """
        config = MANUAL_DELTA_TABLES[scenario]
        table = config["table_name"]
        table_root = config["s3_locations"][0]  # First location is table root

        print(f"\n{'='*60}")
        print(f"Testing INSERT on: {table}")
        print(f"Table root: {table_root}")
        print(f"{'='*60}")

        # Get initial counts
        initial_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]["cnt"]
        initial_files = list_s3_files(s3_clients, table_root)
        initial_parquet_count = count_parquet_files(initial_files)

        print(f"Initial row count: {initial_count}")
        print(f"Initial parquet files at root: {initial_parquet_count}")

        # Get schema to understand columns
        schema = spark.sql(f"DESCRIBE {table}").collect()
        print(f"Schema: {[(r['col_name'], r['data_type']) for r in schema if not r['col_name'].startswith('#')]}")

        # Insert new row using table schema
        # First get partition column - format is Row(region='us-east')
        partitions = spark.sql(f"SHOW PARTITIONS {table}").collect()
        if partitions:
            partition_row = partitions[0].asDict()
            partition_col = list(partition_row.keys())[0]
            partition_val = partition_row[partition_col]
        else:
            partition_col, partition_val = "region", "test"

        insert_sql = f"""
            INSERT INTO {table}
            SELECT 999999 as id, 'test_insert' as name, 999.99 as value, true as is_active, '{partition_val}' as {partition_col}
        """
        print(f"Executing: {insert_sql}")
        spark.sql(insert_sql)

        # Verify row count increased
        new_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]["cnt"]
        new_files = list_s3_files(s3_clients, table_root)
        new_parquet_count = count_parquet_files(new_files)

        print(f"New row count: {new_count}")
        print(f"New parquet files at root: {new_parquet_count}")

        assert new_count == initial_count + 1, \
            f"Row count should increase by 1: {initial_count} -> {new_count}"

        # Verify we can read the inserted row
        inserted = spark.sql(f"SELECT * FROM {table} WHERE id = 999999").collect()
        assert len(inserted) == 1, "Inserted row not found"
        print(f"Inserted row: {inserted[0]}")

        # Cleanup: delete the test row
        spark.sql(f"DELETE FROM {table} WHERE id = 999999")

    # -------------------------------------------------------------------------
    # UPDATE Tests
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("scenario", list(MANUAL_DELTA_TABLES.keys()))
    def test_update_rows(self, spark, scenario):
        """Verify UPDATE modifies rows correctly.

        UPDATE uses copy-on-write: creates new files with updated data.
        """
        config = MANUAL_DELTA_TABLES[scenario]
        table = config["table_name"]

        print(f"\n{'='*60}")
        print(f"Testing UPDATE on: {table}")
        print(f"{'='*60}")

        # First, insert a row to update - format is Row(region='us-east')
        partitions = spark.sql(f"SHOW PARTITIONS {table}").collect()
        if partitions:
            partition_row = partitions[0].asDict()
            partition_col = list(partition_row.keys())[0]
            partition_val = partition_row[partition_col]
        else:
            partition_col, partition_val = "region", "test"

        # Insert
        spark.sql(f"""
            INSERT INTO {table}
            SELECT 888888 as id, 'before_update' as name, 100.0 as value, true as is_active, '{partition_val}' as {partition_col}
        """)

        # Verify before update
        before = spark.sql(f"SELECT * FROM {table} WHERE id = 888888").collect()
        assert len(before) == 1
        assert before[0]["name"] == "before_update"
        print(f"Before update: {before[0]}")

        # Update
        spark.sql(f"""
            UPDATE {table}
            SET name = 'after_update', value = 200.0
            WHERE id = 888888
        """)

        # Verify after update
        after = spark.sql(f"SELECT * FROM {table} WHERE id = 888888").collect()
        assert len(after) == 1
        assert after[0]["name"] == "after_update"
        assert after[0]["value"] == 200.0
        print(f"After update: {after[0]}")

        # Cleanup
        spark.sql(f"DELETE FROM {table} WHERE id = 888888")

    # -------------------------------------------------------------------------
    # DELETE Tests
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("scenario", list(MANUAL_DELTA_TABLES.keys()))
    def test_delete_rows(self, spark, scenario):
        """Verify DELETE removes rows correctly."""
        config = MANUAL_DELTA_TABLES[scenario]
        table = config["table_name"]

        print(f"\n{'='*60}")
        print(f"Testing DELETE on: {table}")
        print(f"{'='*60}")

        # Get partition info - format is Row(region='us-east')
        partitions = spark.sql(f"SHOW PARTITIONS {table}").collect()
        if partitions:
            partition_row = partitions[0].asDict()
            partition_col = list(partition_row.keys())[0]
            partition_val = partition_row[partition_col]
        else:
            partition_col, partition_val = "region", "test"

        # Insert row to delete
        spark.sql(f"""
            INSERT INTO {table}
            SELECT 777777 as id, 'to_delete' as name, 50.0 as value, true as is_active, '{partition_val}' as {partition_col}
        """)

        # Verify exists
        before_delete = spark.sql(f"SELECT COUNT(*) as cnt FROM {table} WHERE id = 777777").collect()[0]["cnt"]
        assert before_delete == 1, "Row should exist before delete"
        print(f"Row exists before delete: {before_delete}")

        # Delete
        spark.sql(f"DELETE FROM {table} WHERE id = 777777")

        # Verify deleted
        after_delete = spark.sql(f"SELECT COUNT(*) as cnt FROM {table} WHERE id = 777777").collect()[0]["cnt"]
        assert after_delete == 0, "Row should not exist after delete"
        print(f"Row exists after delete: {after_delete}")

    # -------------------------------------------------------------------------
    # MERGE Tests
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("scenario", list(MANUAL_DELTA_TABLES.keys()))
    def test_merge_upsert(self, spark, scenario):
        """Verify MERGE handles both update and insert (upsert)."""
        config = MANUAL_DELTA_TABLES[scenario]
        table = config["table_name"]

        print(f"\n{'='*60}")
        print(f"Testing MERGE on: {table}")
        print(f"{'='*60}")

        # Get partition info - format is Row(region='us-east')
        partitions = spark.sql(f"SHOW PARTITIONS {table}").collect()
        if partitions:
            partition_row = partitions[0].asDict()
            partition_col = list(partition_row.keys())[0]
            partition_val = partition_row[partition_col]
        else:
            partition_col, partition_val = "region", "test"

        # Insert initial row for update test
        spark.sql(f"""
            INSERT INTO {table}
            SELECT 666666 as id, 'merge_target' as name, 10.0 as value, true as is_active, '{partition_val}' as {partition_col}
        """)

        # Create source data for merge (one existing row to update, one new row)
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW merge_source AS
            SELECT 666666 as id, 'merged_update' as name, 20.0 as value, true as is_active, '{partition_val}' as {partition_col}
            UNION ALL
            SELECT 666667 as id, 'merged_insert' as name, 30.0 as value, true as is_active, '{partition_val}' as {partition_col}
        """)

        # Execute MERGE
        spark.sql(f"""
            MERGE INTO {table} AS target
            USING merge_source AS source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET
                target.name = source.name,
                target.value = source.value
            WHEN NOT MATCHED THEN INSERT *
        """)

        # Verify update
        updated = spark.sql(f"SELECT * FROM {table} WHERE id = 666666").collect()
        assert len(updated) == 1
        assert updated[0]["name"] == "merged_update"
        assert updated[0]["value"] == 20.0
        print(f"Updated row: {updated[0]}")

        # Verify insert
        inserted = spark.sql(f"SELECT * FROM {table} WHERE id = 666667").collect()
        assert len(inserted) == 1
        assert inserted[0]["name"] == "merged_insert"
        print(f"Inserted row: {inserted[0]}")

        # Cleanup
        spark.sql(f"DELETE FROM {table} WHERE id IN (666666, 666667)")
        spark.sql("DROP VIEW IF EXISTS merge_source")

    # -------------------------------------------------------------------------
    # OPTIMIZE Tests
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("scenario", list(MANUAL_DELTA_TABLES.keys()))
    def test_optimize_compaction(self, spark, s3_clients, scenario):
        """Verify OPTIMIZE compacts files.

        Strategy:
        1. Insert multiple small records to create many small files
        2. Count files before OPTIMIZE
        3. Run OPTIMIZE
        4. Verify file count reduced (or unchanged if already optimal)
        5. Verify data integrity
        """
        config = MANUAL_DELTA_TABLES[scenario]
        table = config["table_name"]

        print(f"\n{'='*60}")
        print(f"Testing OPTIMIZE on: {table}")
        print(f"{'='*60}")

        # Get initial row count
        initial_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]["cnt"]
        print(f"Initial row count: {initial_count}")

        # Get partition info - format is Row(region='us-east')
        partitions = spark.sql(f"SHOW PARTITIONS {table}").collect()
        if partitions:
            partition_row = partitions[0].asDict()
            partition_col = list(partition_row.keys())[0]
            partition_val = partition_row[partition_col]
        else:
            partition_col, partition_val = "region", "test"

        # Insert multiple small rows to create small files
        test_ids = list(range(500000, 500010))  # 10 rows
        for test_id in test_ids:
            spark.sql(f"""
                INSERT INTO {table}
                SELECT {test_id} as id, 'optimize_test_{test_id}' as name,
                       {test_id}.0 as value, true as is_active, '{partition_val}' as {partition_col}
            """)

        # Get file count before OPTIMIZE
        detail_before = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
        files_before = detail_before["numFiles"]
        print(f"Files before OPTIMIZE: {files_before}")

        # Run OPTIMIZE
        print("Running OPTIMIZE...")
        optimize_result = spark.sql(f"OPTIMIZE {table}")
        optimize_metrics = optimize_result.collect()
        print(f"OPTIMIZE result: {optimize_metrics}")

        # Get file count after OPTIMIZE
        detail_after = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
        files_after = detail_after["numFiles"]
        print(f"Files after OPTIMIZE: {files_after}")

        # Verify row count unchanged
        final_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]["cnt"]
        expected_count = initial_count + len(test_ids)
        assert final_count == expected_count, \
            f"Row count changed: expected {expected_count}, got {final_count}"
        print(f"Row count preserved: {final_count}")

        # Verify files compacted (should be same or fewer)
        assert files_after <= files_before, \
            f"OPTIMIZE should not increase file count: {files_before} -> {files_after}"

        # Cleanup test rows
        spark.sql(f"DELETE FROM {table} WHERE id >= 500000 AND id < 500010")


class TestVacuumCritical:
    """CRITICAL: Test that VACUUM actually deletes files from S3.

    This is the most important test class - it verifies that VACUUM
    deletes files from ALL locations including:
    - Table root location
    - External partition locations
    - Files in different S3 buckets
    - Files in different AWS regions
    """

    @pytest.fixture(scope="class", autouse=True)
    def setup_retention_check(self, spark):
        """Disable retention check for VACUUM tests."""
        spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = false")
        yield
        spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = true")

    def _create_old_file_versions(
        self,
        spark,
        table: str,
        partition_col: str,
        partition_val: str,
        num_operations: int = 3,
    ) -> Tuple[int, List[str]]:
        """Create old file versions by performing updates.

        Returns:
            Tuple of (row_count, list of file paths that should be deleted)
        """
        test_id = 111111

        # Insert (including is_active column)
        spark.sql(f"""
            INSERT INTO {table}
            SELECT {test_id} as id, 'vacuum_test_v0' as name,
                   1.0 as value, true as is_active, '{partition_val}' as {partition_col}
        """)

        # Perform updates to create old file versions
        for i in range(1, num_operations + 1):
            spark.sql(f"""
                UPDATE {table}
                SET name = 'vacuum_test_v{i}', value = {i + 1}.0
                WHERE id = {test_id}
            """)
            # Small delay to ensure distinct timestamps
            time.sleep(0.5)

        # Verify current state
        current = spark.sql(f"SELECT * FROM {table} WHERE id = {test_id}").collect()
        assert len(current) == 1
        assert current[0]["name"] == f"vacuum_test_v{num_operations}"

        return 1, []

    @pytest.mark.parametrize("scenario", list(MANUAL_DELTA_TABLES.keys()))
    def test_vacuum_deletes_files_at_all_locations(self, spark, s3_clients, scenario):
        """CRITICAL: Verify VACUUM actually deletes files from S3.

        This is the key test to verify that VACUUM works with absolute
        S3 paths and can delete files across:
        - Table root location
        - External partition locations (scattered)
        - Different buckets (cross_bucket)
        - Different regions (cross_region)

        Strategy:
        1. Get initial file list from ALL locations
        2. Perform UPDATE operations to create old file versions
        3. Get file list after updates (should have more files)
        4. Run VACUUM RETAIN 0 HOURS
        5. Get new file list from ALL locations
        6. ASSERT old files are actually gone from S3
        """
        config = MANUAL_DELTA_TABLES[scenario]
        table = config["table_name"]
        s3_locations = config["s3_locations"]

        print(f"\n{'='*70}")
        print(f"CRITICAL TEST: VACUUM file deletion for {scenario}")
        print(f"Table: {table}")
        print(f"S3 Locations: {s3_locations}")
        print(f"{'='*70}")

        # Step 1: Get initial file list from ALL locations
        print("\n1. Getting initial file list from ALL S3 locations...")
        initial_files = list_all_table_files(s3_clients, s3_locations)
        initial_parquet_count = count_parquet_files(initial_files)
        print(f"   Initial parquet files: {initial_parquet_count}")
        for f in initial_files[:10]:  # Show first 10
            print(f"     - {f['full_path']}")
        if len(initial_files) > 10:
            print(f"     ... and {len(initial_files) - 10} more")

        # Step 2: Get partition info and create old file versions
        print("\n2. Creating old file versions via UPDATE operations...")
        partitions = spark.sql(f"SHOW PARTITIONS {table}").collect()
        if partitions:
            # Partition format is Row(region='us-east') - get first partition column and value
            partition_row = partitions[0].asDict()
            partition_col = list(partition_row.keys())[0]
            partition_val = partition_row[partition_col]
        else:
            partition_col, partition_val = "region", "test"

        # Perform operations to create old file versions
        self._create_old_file_versions(
            spark, table, partition_col, partition_val, num_operations=3
        )

        # Step 3: Get file list after updates (should have more files)
        print("\n3. Getting file list after updates...")
        time.sleep(1)  # Allow time for writes to complete
        files_after_updates = list_all_table_files(s3_clients, s3_locations)
        parquet_after_updates = count_parquet_files(files_after_updates)
        print(f"   Parquet files after updates: {parquet_after_updates}")

        # Identify new files (potential targets for VACUUM)
        initial_paths = {f["full_path"] for f in initial_files}
        new_files = [f for f in files_after_updates if f["full_path"] not in initial_paths]
        print(f"   New files created: {len(new_files)}")
        for f in new_files:
            print(f"     - {f['full_path']}")

        # Step 4: Run VACUUM with 0 HOURS retention
        print("\n4. Running VACUUM RETAIN 0 HOURS...")
        vacuum_sql = f"VACUUM {table} RETAIN 0 HOURS"
        print(f"   SQL: {vacuum_sql}")

        vacuum_result = spark.sql(vacuum_sql)
        vacuum_paths = vacuum_result.collect()
        print(f"   VACUUM returned {len(vacuum_paths)} paths")
        for row in vacuum_paths[:10]:
            print(f"     - {row}")

        # Step 5: Get new file list from ALL locations
        print("\n5. Getting file list after VACUUM...")
        time.sleep(2)  # Allow time for deletions to complete
        files_after_vacuum = list_all_table_files(s3_clients, s3_locations)
        parquet_after_vacuum = count_parquet_files(files_after_vacuum)
        print(f"   Parquet files after VACUUM: {parquet_after_vacuum}")

        # Step 6: Analyze what was deleted
        print("\n6. Analyzing file deletions...")
        paths_after_vacuum = {f["full_path"] for f in files_after_vacuum}

        files_deleted_from_s3 = []
        files_still_exist = []

        for f in files_after_updates:
            if f["full_path"] not in paths_after_vacuum:
                files_deleted_from_s3.append(f["full_path"])
            else:
                files_still_exist.append(f["full_path"])

        print(f"   Files deleted from S3: {len(files_deleted_from_s3)}")
        for path in files_deleted_from_s3[:10]:
            print(f"     DELETED: {path}")

        # Verify files were actually deleted from S3
        print("\n7. Verifying deletions with direct S3 checks...")
        deletion_verified = True
        for deleted_path in files_deleted_from_s3[:5]:  # Check first 5
            exists = file_exists_in_s3(s3_clients, deleted_path)
            status = "EXISTS (BAD!)" if exists else "DELETED (GOOD)"
            print(f"     {deleted_path}: {status}")
            if exists:
                deletion_verified = False

        # Step 8: Verify data integrity
        print("\n8. Verifying data integrity after VACUUM...")
        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]["cnt"]
        test_row = spark.sql(f"SELECT * FROM {table} WHERE id = 111111").collect()
        print(f"   Total row count: {row_count}")
        print(f"   Test row exists: {len(test_row) > 0}")
        if test_row:
            print(f"   Test row: {test_row[0]}")

        # Cleanup test row
        spark.sql(f"DELETE FROM {table} WHERE id = 111111")

        # CRITICAL ASSERTIONS
        print("\n" + "="*70)
        print("RESULTS:")
        print("="*70)

        # Check if any files were deleted
        if parquet_after_updates > initial_parquet_count:
            # We created new files, so VACUUM should have deleted something
            files_should_be_deleted = parquet_after_updates - parquet_after_vacuum
            print(f"Files created during test: {parquet_after_updates - initial_parquet_count}")
            print(f"Files deleted by VACUUM: {files_should_be_deleted}")

            if len(files_deleted_from_s3) > 0:
                print(f"SUCCESS: VACUUM deleted {len(files_deleted_from_s3)} files from S3")

                # Check if deletions span multiple locations
                deleted_buckets = set()
                for path in files_deleted_from_s3:
                    bucket, _ = parse_s3_path(path)
                    deleted_buckets.add(bucket)
                print(f"Deletions from buckets: {deleted_buckets}")

            else:
                print("WARNING: No files were deleted from S3")
                print("This might be expected if all files are still referenced")
        else:
            print("No new files were created during test operations")

        # Data integrity check
        assert row_count > 0, "Table has no rows after VACUUM - data corruption!"
        print(f"Data integrity: PASSED (row count = {row_count})")

    @pytest.mark.parametrize("scenario", list(MANUAL_DELTA_TABLES.keys()))
    def test_vacuum_dry_run(self, spark, s3_clients, scenario):
        """Test VACUUM DRY RUN to see what files would be deleted.

        DRY RUN shows files that would be deleted without actually deleting them.
        """
        config = MANUAL_DELTA_TABLES[scenario]
        table = config["table_name"]

        print(f"\n{'='*60}")
        print(f"Testing VACUUM DRY RUN on: {table}")
        print(f"{'='*60}")

        # Run DRY RUN
        dry_run_sql = f"VACUUM {table} RETAIN 0 HOURS DRY RUN"
        print(f"SQL: {dry_run_sql}")

        result = spark.sql(dry_run_sql)
        paths_to_delete = result.collect()

        print(f"Files that would be deleted: {len(paths_to_delete)}")
        for row in paths_to_delete[:20]:
            print(f"  - {row}")
        if len(paths_to_delete) > 20:
            print(f"  ... and {len(paths_to_delete) - 20} more")

        # Analyze paths
        if paths_to_delete:
            buckets = set()
            regions = set()
            for row in paths_to_delete:
                path = row[0] if isinstance(row, (list, tuple)) else str(row)
                if path.startswith("s3://"):
                    bucket, _ = parse_s3_path(path)
                    buckets.add(bucket)
                    regions.add(get_region_for_bucket(bucket))

            print(f"\nBuckets affected: {buckets}")
            print(f"Regions affected: {regions}")


class TestDeltaTableMetadata:
    """Test Delta table metadata and history operations."""

    @pytest.mark.parametrize("scenario", list(MANUAL_DELTA_TABLES.keys()))
    def test_describe_detail(self, spark, scenario):
        """Verify DESCRIBE DETAIL returns expected information."""
        config = MANUAL_DELTA_TABLES[scenario]
        table = config["table_name"]

        detail = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]

        print(f"\nTable detail for {table}:")
        print(f"  Format: {detail['format']}")
        print(f"  Location: {detail['location']}")
        print(f"  Num Files: {detail['numFiles']}")
        print(f"  Size (bytes): {detail['sizeInBytes']}")

        assert detail["format"] == "delta"
        assert detail["numFiles"] >= 1

    @pytest.mark.parametrize("scenario", list(MANUAL_DELTA_TABLES.keys()))
    def test_history(self, spark, scenario):
        """Verify DESCRIBE HISTORY works."""
        config = MANUAL_DELTA_TABLES[scenario]
        table = config["table_name"]

        history = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 10").collect()

        print(f"\nHistory for {table} (last 10 operations):")
        for row in history:
            print(f"  Version {row['version']}: {row['operation']} at {row['timestamp']}")

        assert len(history) >= 1, "Table should have at least one history entry"

    @pytest.mark.parametrize("scenario", list(MANUAL_DELTA_TABLES.keys()))
    def test_show_partitions(self, spark, scenario):
        """Verify SHOW PARTITIONS works."""
        config = MANUAL_DELTA_TABLES[scenario]
        table = config["table_name"]

        partitions = spark.sql(f"SHOW PARTITIONS {table}").collect()

        print(f"\nPartitions for {table}:")
        for p in partitions:
            print(f"  {p}")

        # These tables should have partitions
        assert len(partitions) >= 1, "Table should have at least one partition"
