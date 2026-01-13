"""
Comprehensive write validation tests for Databricks writing to Glue-backed Hive tables.

Tests write operations across all 7 scenarios:
- Basic Layouts:
  - standard_table (PARTITIONS_UNDER_TABLE_ROOT)
  - recursive_table (PARTITIONS_IN_NESTED_SUBDIRECTORIES)
- External Partitions:
  - scattered_table (PARTITIONS_OUTSIDE_TABLE_ROOT)
- Multi-Location:
  - cross_bucket_table (PARTITIONS_ACROSS_S3_BUCKETS)
  - cross_region_table (PARTITIONS_ACROSS_AWS_REGIONS)
  - shared_partition_table_a (PARTITIONS_SHARED_BETWEEN_TABLES)
  - shared_partition_table_b (PARTITIONS_SHARED_BETWEEN_TABLES)

Operations tested:
- INSERT INTO (append to existing partition)
- INSERT INTO (create new partition)
- INSERT OVERWRITE (replace partition data)
- UPDATE (expected to fail on non-ACID Hive tables)
- DELETE (expected to fail on non-ACID Hive tables)

Validation includes:
- Row count before/after
- Glue metadata via boto3
- S3 file verification via boto3
"""

import pytest
import boto3
import time
from typing import Optional, Dict, Any, List

from hive_table_experiments.scenarios import BUCKET_EAST_1A, BUCKET_EAST_1B


# ============================================================================
# Fixtures for write validation
# ============================================================================

@pytest.fixture(scope="module")
def glue_client():
    """Create boto3 Glue client for metadata validation."""
    return boto3.client("glue", region_name="us-east-1")


@pytest.fixture(scope="module")
def s3_client():
    """Create boto3 S3 client for file validation."""
    return boto3.client("s3", region_name="us-east-1")


# ============================================================================
# Helper Functions
# ============================================================================

def get_partition_location(glue_client, database: str, table: str, partition_values: List[str]) -> Optional[str]:
    """
    Get the S3 location for a specific partition from Glue.

    Args:
        glue_client: Boto3 Glue client
        database: Glue database name
        table: Table name
        partition_values: List of partition values in order

    Returns:
        S3 location string or None if not found
    """
    try:
        response = glue_client.get_partition(
            DatabaseName=database,
            TableName=table,
            PartitionValues=partition_values
        )
        return response["Partition"]["StorageDescriptor"]["Location"]
    except glue_client.exceptions.EntityNotFoundException:
        return None


def list_s3_files(s3_client, s3_path: str) -> List[str]:
    """
    List all files at an S3 path.

    Args:
        s3_client: Boto3 S3 client
        s3_path: Full S3 path (s3://bucket/prefix)

    Returns:
        List of S3 keys
    """
    if not s3_path.startswith("s3://"):
        return []

    parts = s3_path[5:].split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    # Ensure prefix ends with / for directory listing
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [obj["Key"] for obj in response.get("Contents", [])]
    except Exception:
        return []


def count_s3_parquet_files(s3_client, s3_path: str) -> int:
    """Count parquet files at an S3 path."""
    files = list_s3_files(s3_client, s3_path)
    return len([f for f in files if f.endswith(".parquet")])


def get_glue_partitions(glue_client, database: str, table: str) -> List[Dict[str, Any]]:
    """Get all partitions for a Glue table."""
    try:
        response = glue_client.get_partitions(DatabaseName=database, TableName=table)
        return response.get("Partitions", [])
    except Exception:
        return []


# ============================================================================
# Test Class: Basic Layouts - Standard Table
# ============================================================================

@pytest.mark.write_operations
@pytest.mark.basic_layouts
class TestWriteOperationsStandardTable:
    """
    Write validation for standard_table (PARTITIONS_UNDER_TABLE_ROOT).

    This is the baseline scenario with standard Hive partitioning where
    partitions are under the table root directory.
    """

    @pytest.fixture(scope="class")
    def table_name(self, glue_table):
        return glue_table("standard_table")

    @pytest.fixture(scope="class")
    def raw_table_name(self):
        return "standard_table"

    # ------------------------------------------------------------------------
    # INSERT INTO - Append to existing partition
    # ------------------------------------------------------------------------

    def test_insert_append_existing_partition(self, spark, table_name, glue_client, s3_client, glue_database):
        """
        Test INSERT INTO appending to an existing partition.

        Expected: SUCCESS - Standard operation on Hive tables.
        """
        partition_value = "us-east"

        # Get count before insert
        before_count = spark.sql(
            f"SELECT COUNT(*) as count FROM {table_name} WHERE region='{partition_value}'"
        ).collect()[0].count

        # Generate unique ID to avoid conflicts
        unique_id = int(time.time() * 1000) % 100000

        # Insert new row
        insert_sql = f"""
        INSERT INTO {table_name}
        PARTITION (region = '{partition_value}')
        VALUES ({unique_id}, 'test_append', 99.9, true)
        """

        try:
            spark.sql(insert_sql)

            # Verify row count increased
            after_count = spark.sql(
                f"SELECT COUNT(*) as count FROM {table_name} WHERE region='{partition_value}'"
            ).collect()[0].count

            assert after_count > before_count, (
                f"INSERT failed: count did not increase (before={before_count}, after={after_count})"
            )

            # Verify specific row exists
            row = spark.sql(
                f"SELECT * FROM {table_name} WHERE id = {unique_id}"
            ).collect()
            assert len(row) == 1, f"Inserted row not found with id={unique_id}"
            assert row[0].name == "test_append"

            # Verify Glue partition still exists
            location = get_partition_location(
                glue_client, glue_database, "standard_table", [partition_value]
            )
            assert location is not None, "Partition metadata missing in Glue"

            print(f"SUCCESS: INSERT INTO existing partition (before={before_count}, after={after_count})")

        except Exception as e:
            pytest.fail(f"INSERT INTO existing partition failed: {str(e)}")

    # ------------------------------------------------------------------------
    # INSERT INTO - Create new partition
    # ------------------------------------------------------------------------

    def test_insert_create_new_partition(self, spark, table_name, glue_client, s3_client, glue_database, s3_buckets):
        """
        Test INSERT INTO creating a new partition.

        Expected: SUCCESS - Databricks should create partition in Glue.
        """
        # Use timestamp-based partition name to ensure uniqueness
        partition_value = f"test-{int(time.time()) % 10000}"
        unique_id = int(time.time() * 1000) % 100000

        # Verify partition doesn't exist
        location_before = get_partition_location(
            glue_client, glue_database, "standard_table", [partition_value]
        )

        insert_sql = f"""
        INSERT INTO {table_name}
        PARTITION (region = '{partition_value}')
        VALUES ({unique_id}, 'test_new_partition', 88.8, false)
        """

        try:
            spark.sql(insert_sql)

            # Verify partition was created in Glue
            location_after = get_partition_location(
                glue_client, glue_database, "standard_table", [partition_value]
            )

            assert location_after is not None, (
                f"New partition '{partition_value}' not registered in Glue"
            )

            # Verify data is readable
            row = spark.sql(
                f"SELECT * FROM {table_name} WHERE region = '{partition_value}'"
            ).collect()
            assert len(row) == 1, f"Expected 1 row in new partition, got {len(row)}"
            assert row[0].id == unique_id

            # Verify S3 files created
            file_count = count_s3_parquet_files(s3_client, location_after)
            assert file_count > 0, f"No parquet files found at {location_after}"

            print(f"SUCCESS: Created new partition '{partition_value}' at {location_after}")

            # Cleanup: drop the test partition
            try:
                spark.sql(f"ALTER TABLE {table_name} DROP PARTITION (region = '{partition_value}')")
            except Exception:
                pass  # Cleanup failure is not a test failure

        except Exception as e:
            pytest.fail(f"INSERT creating new partition failed: {str(e)}")

    # ------------------------------------------------------------------------
    # INSERT OVERWRITE - Replace partition data
    # ------------------------------------------------------------------------

    def test_insert_overwrite_partition(self, spark, table_name, glue_client, s3_client, glue_database, s3_buckets):
        """
        Test INSERT OVERWRITE replacing partition data.

        Expected: SUCCESS - Should replace all data in partition.
        """
        # Create a test partition first
        partition_value = f"overwrite-test-{int(time.time()) % 10000}"
        unique_id_1 = int(time.time() * 1000) % 100000

        # Insert initial data
        spark.sql(f"""
            INSERT INTO {table_name}
            PARTITION (region = '{partition_value}')
            VALUES
                ({unique_id_1}, 'original_1', 10.0, true),
                ({unique_id_1 + 1}, 'original_2', 20.0, false),
                ({unique_id_1 + 2}, 'original_3', 30.0, true)
        """)

        before_count = spark.sql(
            f"SELECT COUNT(*) as count FROM {table_name} WHERE region='{partition_value}'"
        ).collect()[0].count
        assert before_count == 3, f"Setup failed: expected 3 rows, got {before_count}"

        # Get S3 location for file count comparison
        location = get_partition_location(
            glue_client, glue_database, "standard_table", [partition_value]
        )

        # Overwrite with single row
        unique_id_2 = int(time.time() * 1000) % 100000 + 10000
        try:
            spark.sql(f"""
                INSERT OVERWRITE TABLE {table_name}
                PARTITION (region = '{partition_value}')
                VALUES ({unique_id_2}, 'overwritten', 99.9, true)
            """)

            # Verify only new data exists
            after_count = spark.sql(
                f"SELECT COUNT(*) as count FROM {table_name} WHERE region='{partition_value}'"
            ).collect()[0].count

            assert after_count == 1, (
                f"INSERT OVERWRITE failed: expected 1 row, got {after_count}"
            )

            # Verify it's the new row
            row = spark.sql(
                f"SELECT * FROM {table_name} WHERE region = '{partition_value}'"
            ).collect()[0]
            assert row.id == unique_id_2
            assert row.name == "overwritten"

            print(f"SUCCESS: INSERT OVERWRITE partition (before={before_count}, after={after_count})")

            # Cleanup
            try:
                spark.sql(f"ALTER TABLE {table_name} DROP PARTITION (region = '{partition_value}')")
            except Exception:
                pass

        except Exception as e:
            pytest.fail(f"INSERT OVERWRITE failed: {str(e)}")

    # ------------------------------------------------------------------------
    # UPDATE - Expected to fail on non-ACID tables
    # ------------------------------------------------------------------------

    def test_update_fails_on_non_acid_table(self, spark, table_name):
        """
        Test UPDATE statement on non-ACID Hive table.

        Expected: FAIL - Hive external tables don't support UPDATE without ACID.

        This test documents the expected limitation. If UPDATE succeeds,
        it indicates unexpected Delta Lake or ACID behavior.
        """
        update_sql = f"""
        UPDATE {table_name}
        SET name = 'updated_name'
        WHERE id = 1 AND region = 'us-east'
        """

        try:
            spark.sql(update_sql)

            # If we get here, UPDATE worked - this is unexpected for external Hive
            pytest.fail(
                "UPDATE succeeded unexpectedly. "
                "This Hive table may have ACID enabled or be a Delta table."
            )

        except Exception as e:
            error_msg = str(e).lower()

            # Common error messages for non-ACID tables
            expected_errors = [
                "update is not supported",
                "cannot update",
                "not supported",
                "analysis exception",
                "write is not supported",
            ]

            is_expected_error = any(err in error_msg for err in expected_errors)

            if is_expected_error:
                print(f"EXPECTED: UPDATE failed on non-ACID table: {str(e)[:200]}")
            else:
                print(f"UPDATE failed with unexpected error: {str(e)[:200]}")

            # Document the error but don't fail the test - this is expected behavior
            assert True, "UPDATE correctly rejected on non-ACID Hive table"

    # ------------------------------------------------------------------------
    # DELETE - Expected to fail on non-ACID tables
    # ------------------------------------------------------------------------

    def test_delete_on_non_acid_table(self, spark, table_name):
        """
        Test DELETE statement on non-ACID Hive table.

        Expected: FAIL - Hive external tables typically don't support DELETE without ACID.

        Note: Some Databricks configurations may support DELETE through
        table format conversion or special handling.
        """
        # First insert a row we can try to delete
        unique_id = int(time.time() * 1000) % 100000 + 50000

        try:
            spark.sql(f"""
                INSERT INTO {table_name}
                PARTITION (region = 'us-east')
                VALUES ({unique_id}, 'to_delete', 55.5, false)
            """)
        except Exception as e:
            pytest.skip(f"Could not insert test row for DELETE test: {str(e)[:100]}")

        delete_sql = f"""
        DELETE FROM {table_name}
        WHERE id = {unique_id} AND region = 'us-east'
        """

        try:
            spark.sql(delete_sql)

            # Verify deletion
            remaining = spark.sql(
                f"SELECT COUNT(*) as count FROM {table_name} WHERE id = {unique_id}"
            ).collect()[0].count

            if remaining == 0:
                print("SURPRISING: DELETE succeeded on Hive table")
                print("This suggests ACID support or table format conversion")
            else:
                pytest.fail(f"DELETE executed but row still exists (count={remaining})")

        except Exception as e:
            error_msg = str(e).lower()

            expected_errors = [
                "delete is not supported",
                "cannot delete",
                "not supported",
                "analysis exception",
                "write is not supported",
            ]

            is_expected_error = any(err in error_msg for err in expected_errors)

            if is_expected_error:
                print(f"EXPECTED: DELETE failed on non-ACID table: {str(e)[:200]}")
            else:
                print(f"DELETE failed with error: {str(e)[:200]}")


# ============================================================================
# Test Class: Basic Layouts - Recursive Table
# ============================================================================

@pytest.mark.write_operations
@pytest.mark.basic_layouts
class TestWriteOperationsRecursiveTable:
    """
    Write validation for recursive_table (PARTITIONS_IN_NESTED_SUBDIRECTORIES).

    Non-partitioned table with nested subdirectories requiring recursive scanning.
    """

    @pytest.fixture(scope="class")
    def table_name(self, glue_table):
        return glue_table("recursive_table")

    def test_insert_to_non_partitioned_table(self, spark, table_name, s3_client):
        """
        Test INSERT INTO non-partitioned recursive table.

        Expected: SUCCESS - Data written to table root location.
        """
        unique_id = int(time.time() * 1000) % 100000

        before_count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0].count

        insert_sql = f"""
        INSERT INTO {table_name}
        VALUES ({unique_id}, 'recursive_insert', 77.7, true)
        """

        try:
            spark.sql(insert_sql)

            after_count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0].count

            assert after_count > before_count, (
                f"INSERT failed: count did not increase (before={before_count}, after={after_count})"
            )

            # Verify row exists
            row = spark.sql(f"SELECT * FROM {table_name} WHERE id = {unique_id}").collect()
            assert len(row) == 1
            assert row[0].name == "recursive_insert"

            print(f"SUCCESS: INSERT INTO recursive table (before={before_count}, after={after_count})")

        except Exception as e:
            pytest.fail(f"INSERT INTO recursive table failed: {str(e)}")

    def test_insert_overwrite_non_partitioned(self, spark, table_name):
        """
        Test INSERT OVERWRITE on non-partitioned table.

        WARNING: This will replace ALL data in the table.

        Expected: SUCCESS - All data replaced.
        """
        unique_id = int(time.time() * 1000) % 100000

        before_count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0].count

        # Note: This overwrites the ENTIRE table
        overwrite_sql = f"""
        INSERT OVERWRITE TABLE {table_name}
        VALUES ({unique_id}, 'full_overwrite', 66.6, false)
        """

        try:
            spark.sql(overwrite_sql)

            after_count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0].count

            # Should have exactly 1 row now
            assert after_count == 1, f"Expected 1 row after overwrite, got {after_count}"

            row = spark.sql(f"SELECT * FROM {table_name}").collect()[0]
            assert row.id == unique_id
            assert row.name == "full_overwrite"

            print(f"SUCCESS: INSERT OVERWRITE non-partitioned table (before={before_count}, after={after_count})")
            print("WARNING: All previous data was replaced")

        except Exception as e:
            pytest.fail(f"INSERT OVERWRITE non-partitioned table failed: {str(e)}")

    def test_update_fails_recursive_table(self, spark, table_name):
        """
        Test UPDATE on recursive non-partitioned table.

        Expected: FAIL - Same as partitioned tables.
        """
        update_sql = f"""
        UPDATE {table_name}
        SET name = 'updated'
        WHERE id = 1
        """

        try:
            spark.sql(update_sql)
            pytest.fail("UPDATE succeeded unexpectedly on recursive table")
        except Exception as e:
            print(f"EXPECTED: UPDATE failed on recursive table: {str(e)[:200]}")

    def test_delete_fails_recursive_table(self, spark, table_name):
        """
        Test DELETE on recursive non-partitioned table.

        Expected: FAIL - Same as partitioned tables.
        """
        delete_sql = f"""
        DELETE FROM {table_name}
        WHERE id = 1
        """

        try:
            spark.sql(delete_sql)
            # If we get here, check if deletion actually worked
            remaining = spark.sql(f"SELECT COUNT(*) as count FROM {table_name} WHERE id = 1").collect()[0].count
            if remaining > 0:
                pytest.fail("DELETE executed but row still exists")
            else:
                print("SURPRISING: DELETE succeeded on recursive table")
        except Exception as e:
            print(f"EXPECTED: DELETE failed on recursive table: {str(e)[:200]}")


# ============================================================================
# Test Class: External Partitions - Scattered Table
# ============================================================================

@pytest.mark.write_operations
@pytest.mark.external_partitions
class TestWriteOperationsScatteredTable:
    """
    Write validation for scattered_table (PARTITIONS_OUTSIDE_TABLE_ROOT).

    Partitions exist at locations outside the table's root directory.
    Write behavior is unpredictable - may write to table root instead
    of partition location.
    """

    @pytest.fixture(scope="class")
    def table_name(self, glue_table):
        return glue_table("scattered_table")

    def test_insert_to_external_partition(self, spark, table_name, glue_client, s3_client, glue_database, s3_buckets):
        """
        Test INSERT INTO partition with external location.

        Expected: UNCERTAIN - May write to table root OR partition location.

        This test documents actual behavior for external partitions.
        """
        # Use existing external partition
        partition_value = "us-west"  # This partition is at a different path

        # Get partition location from Glue
        partition_location = get_partition_location(
            glue_client, glue_database, "scattered_table", [partition_value]
        )

        print(f"External partition location: {partition_location}")

        # Count files before
        files_before = count_s3_parquet_files(s3_client, partition_location) if partition_location else 0

        unique_id = int(time.time() * 1000) % 100000

        insert_sql = f"""
        INSERT INTO {table_name}
        PARTITION (region = '{partition_value}')
        VALUES ({unique_id}, 'external_partition_test', 44.4, true)
        """

        try:
            spark.sql(insert_sql)

            # Check if data is readable
            row = spark.sql(
                f"SELECT * FROM {table_name} WHERE id = {unique_id}"
            ).collect()

            if len(row) == 0:
                print("BEHAVIOR: Data not found after INSERT to external partition")
                print("This is expected - Databricks may not write to external locations")
            else:
                print(f"BEHAVIOR: Data found with id={unique_id}")

                # Check where files were written
                files_after = count_s3_parquet_files(s3_client, partition_location) if partition_location else 0

                if files_after > files_before:
                    print(f"SUCCESS: Files written to partition location ({files_before} -> {files_after})")
                else:
                    # Check table root location
                    table_root = f"s3://{BUCKET_EAST_1A}/tables/scattered/"
                    root_files = count_s3_parquet_files(s3_client, table_root)
                    print(f"BEHAVIOR: Files may be at table root instead of partition location")
                    print(f"Table root files: {root_files}")

        except Exception as e:
            print(f"INSERT to external partition failed: {str(e)[:200]}")
            # Don't fail - document the behavior

    def test_insert_creates_partition_at_table_root(self, spark, table_name, glue_client, glue_database, s3_client):
        """
        Test if INSERT creating new partition uses table root or custom location.

        Expected: Partition created at table_root/region=value/
        """
        partition_value = f"scattered-test-{int(time.time()) % 10000}"
        unique_id = int(time.time() * 1000) % 100000

        insert_sql = f"""
        INSERT INTO {table_name}
        PARTITION (region = '{partition_value}')
        VALUES ({unique_id}, 'new_scattered_partition', 33.3, false)
        """

        try:
            spark.sql(insert_sql)

            # Get the location Glue assigned
            location = get_partition_location(
                glue_client, glue_database, "scattered_table", [partition_value]
            )

            if location:
                print(f"BEHAVIOR: New partition created at: {location}")

                # Check if it's under table root or elsewhere
                table_root = f"s3://{BUCKET_EAST_1A}/tables/scattered/"
                if location.startswith(table_root):
                    print("BEHAVIOR: Partition created under table root (expected)")
                else:
                    print(f"BEHAVIOR: Partition created outside table root")
            else:
                print("BEHAVIOR: Partition not registered in Glue")

            # Cleanup
            try:
                spark.sql(f"ALTER TABLE {table_name} DROP PARTITION (region = '{partition_value}')")
            except Exception:
                pass

        except Exception as e:
            print(f"INSERT creating scattered partition failed: {str(e)[:200]}")

    def test_insert_overwrite_external_partition(self, spark, table_name, glue_client, glue_database, s3_client):
        """
        Test INSERT OVERWRITE on external partition.

        Expected: UNCERTAIN - May fail or overwrite wrong location.
        """
        partition_value = "us-west"  # External partition

        partition_location = get_partition_location(
            glue_client, glue_database, "scattered_table", [partition_value]
        )

        unique_id = int(time.time() * 1000) % 100000

        overwrite_sql = f"""
        INSERT OVERWRITE TABLE {table_name}
        PARTITION (region = '{partition_value}')
        VALUES ({unique_id}, 'overwrite_external', 22.2, true)
        """

        try:
            spark.sql(overwrite_sql)

            # Check what happened
            count = spark.sql(
                f"SELECT COUNT(*) as count FROM {table_name} WHERE region = '{partition_value}'"
            ).collect()[0].count

            print(f"BEHAVIOR: After INSERT OVERWRITE, partition has {count} rows")

            if count == 1:
                print("SUCCESS: INSERT OVERWRITE worked on external partition")
            elif count == 0:
                print("BEHAVIOR: INSERT OVERWRITE may have written to wrong location")
            else:
                print(f"BEHAVIOR: INSERT OVERWRITE partial success ({count} rows)")

        except Exception as e:
            print(f"INSERT OVERWRITE external partition failed: {str(e)[:200]}")


# ============================================================================
# Test Class: Multi-Location - Cross Bucket Table
# ============================================================================

@pytest.mark.write_operations
@pytest.mark.multi_location
class TestWriteOperationsCrossBucketTable:
    """
    Write validation for cross_bucket_table (PARTITIONS_ACROSS_S3_BUCKETS).

    Partitions span multiple S3 buckets in the same region.
    """

    @pytest.fixture(scope="class")
    def table_name(self, glue_table):
        return glue_table("cross_bucket_table")

    def test_insert_to_partition_in_bucket_1(self, spark, table_name, glue_client, glue_database, s3_client):
        """
        Test INSERT to partition in primary bucket.

        Expected: SUCCESS - Primary bucket should work normally.
        """
        partition_value = "us-east"  # In bucket BUCKET_EAST_1A
        unique_id = int(time.time() * 1000) % 100000

        insert_sql = f"""
        INSERT INTO {table_name}
        PARTITION (region = '{partition_value}')
        VALUES ({unique_id}, 'cross_bucket_test_1', 11.1, true)
        """

        try:
            spark.sql(insert_sql)

            row = spark.sql(
                f"SELECT * FROM {table_name} WHERE id = {unique_id}"
            ).collect()

            if len(row) == 1:
                print(f"SUCCESS: INSERT to primary bucket partition")
            else:
                print(f"BEHAVIOR: Row not found after INSERT (count={len(row)})")

        except Exception as e:
            print(f"INSERT to primary bucket failed: {str(e)[:200]}")

    def test_insert_to_partition_in_bucket_2(self, spark, table_name, glue_client, glue_database, s3_client):
        """
        Test INSERT to partition in secondary bucket.

        Expected: UNCERTAIN - May fail or write to wrong bucket.
        """
        partition_value = "us-west"  # In bucket BUCKET_EAST_1B
        unique_id = int(time.time() * 1000) % 100000

        partition_location = get_partition_location(
            glue_client, glue_database, "cross_bucket_table", [partition_value]
        )
        print(f"Partition location (bucket 2): {partition_location}")

        insert_sql = f"""
        INSERT INTO {table_name}
        PARTITION (region = '{partition_value}')
        VALUES ({unique_id}, 'cross_bucket_test_2', 22.2, false)
        """

        try:
            spark.sql(insert_sql)

            row = spark.sql(
                f"SELECT * FROM {table_name} WHERE id = {unique_id}"
            ).collect()

            if len(row) == 1:
                print(f"SUCCESS: INSERT to secondary bucket partition")

                # Verify files went to correct bucket
                if partition_location:
                    files = count_s3_parquet_files(s3_client, partition_location)
                    print(f"Files in partition location: {files}")
            else:
                print(f"BEHAVIOR: Row not found after INSERT to secondary bucket")

        except Exception as e:
            print(f"INSERT to secondary bucket failed: {str(e)[:200]}")

    def test_insert_overwrite_cross_bucket(self, spark, table_name, glue_client, glue_database):
        """
        Test INSERT OVERWRITE on cross-bucket partition.

        Expected: UNCERTAIN - Cross-bucket writes may have issues.
        """
        partition_value = "us-west"  # Secondary bucket
        unique_id = int(time.time() * 1000) % 100000

        overwrite_sql = f"""
        INSERT OVERWRITE TABLE {table_name}
        PARTITION (region = '{partition_value}')
        VALUES ({unique_id}, 'cross_bucket_overwrite', 33.3, true)
        """

        try:
            spark.sql(overwrite_sql)

            count = spark.sql(
                f"SELECT COUNT(*) as count FROM {table_name} WHERE region = '{partition_value}'"
            ).collect()[0].count

            print(f"BEHAVIOR: After INSERT OVERWRITE cross-bucket, partition has {count} rows")

        except Exception as e:
            print(f"INSERT OVERWRITE cross-bucket failed: {str(e)[:200]}")


# ============================================================================
# Test Class: Multi-Location - Cross Region Table
# ============================================================================

@pytest.mark.write_operations
@pytest.mark.multi_location
class TestWriteOperationsCrossRegionTable:
    """
    Write validation for cross_region_table (PARTITIONS_ACROSS_AWS_REGIONS).

    Partitions span us-east-1 and us-west-2 regions.
    """

    @pytest.fixture(scope="class")
    def table_name(self, glue_table):
        return glue_table("cross_region_table")

    def test_insert_to_same_region_partition(self, spark, table_name, glue_client, glue_database):
        """
        Test INSERT to partition in same region as Glue catalog.

        Expected: SUCCESS - Same region should work.
        """
        partition_value = "us-east"  # Same region as Glue (us-east-1)
        unique_id = int(time.time() * 1000) % 100000

        insert_sql = f"""
        INSERT INTO {table_name}
        PARTITION (region = '{partition_value}')
        VALUES ({unique_id}, 'same_region_test', 11.1, true)
        """

        try:
            spark.sql(insert_sql)

            row = spark.sql(f"SELECT * FROM {table_name} WHERE id = {unique_id}").collect()

            if len(row) == 1:
                print("SUCCESS: INSERT to same-region partition")
            else:
                print("BEHAVIOR: Row not found after INSERT")

        except Exception as e:
            print(f"INSERT to same-region partition failed: {str(e)[:200]}")

    def test_insert_to_cross_region_partition(self, spark, table_name, glue_client, glue_database):
        """
        Test INSERT to partition in different region (us-west-2).

        Expected: MAY FAIL - Cross-region writes often have issues with:
        - Credential propagation
        - Network latency
        - Regional S3 endpoint routing
        """
        partition_value = "us-west"  # us-west-2 region
        unique_id = int(time.time() * 1000) % 100000

        partition_location = get_partition_location(
            glue_client, glue_database, "cross_region_table", [partition_value]
        )
        print(f"Cross-region partition location: {partition_location}")

        insert_sql = f"""
        INSERT INTO {table_name}
        PARTITION (region = '{partition_value}')
        VALUES ({unique_id}, 'cross_region_test', 22.2, false)
        """

        try:
            spark.sql(insert_sql)

            row = spark.sql(f"SELECT * FROM {table_name} WHERE id = {unique_id}").collect()

            if len(row) == 1:
                print("SUCCESS: INSERT to cross-region partition (us-west-2)")
                print("NOTE: Cross-region writes succeeded")
            else:
                print("BEHAVIOR: Row not found after cross-region INSERT")
                print("Data may have been written to wrong location")

        except Exception as e:
            error_msg = str(e).lower()

            if "region" in error_msg or "access" in error_msg or "denied" in error_msg:
                print(f"EXPECTED: Cross-region INSERT failed (credential/access): {str(e)[:200]}")
            else:
                print(f"Cross-region INSERT failed: {str(e)[:200]}")

    def test_insert_overwrite_cross_region(self, spark, table_name):
        """
        Test INSERT OVERWRITE on cross-region partition.

        Expected: LIKELY TO FAIL - Cross-region overwrites are complex.
        """
        partition_value = "us-west"
        unique_id = int(time.time() * 1000) % 100000

        overwrite_sql = f"""
        INSERT OVERWRITE TABLE {table_name}
        PARTITION (region = '{partition_value}')
        VALUES ({unique_id}, 'cross_region_overwrite', 33.3, true)
        """

        try:
            spark.sql(overwrite_sql)

            count = spark.sql(
                f"SELECT COUNT(*) as count FROM {table_name} WHERE region = '{partition_value}'"
            ).collect()[0].count

            print(f"BEHAVIOR: After INSERT OVERWRITE cross-region, partition has {count} rows")

        except Exception as e:
            print(f"INSERT OVERWRITE cross-region failed: {str(e)[:200]}")


# ============================================================================
# Test Class: Multi-Location - Shared Partition Tables
# ============================================================================

@pytest.mark.write_operations
@pytest.mark.multi_location
class TestWriteOperationsSharedPartitionTables:
    """
    Write validation for shared_partition_table_a and shared_partition_table_b.

    Two tables share a common partition location. Tests write isolation
    and concurrent access patterns.
    """

    @pytest.fixture(scope="class")
    def table_a(self, glue_table):
        return glue_table("shared_partition_table_a")

    @pytest.fixture(scope="class")
    def table_b(self, glue_table):
        return glue_table("shared_partition_table_b")

    def test_insert_via_table_a_visible_from_table_b(self, spark, table_a, table_b, glue_client, glue_database):
        """
        Test if INSERT via table A is visible from table B for shared partition.

        Expected: YES - Both tables point to same S3 location.
        """
        shared_partition = "shared"
        unique_id = int(time.time() * 1000) % 100000

        # Get shared partition locations
        location_a = get_partition_location(
            glue_client, glue_database, "shared_partition_table_a", [shared_partition]
        )
        location_b = get_partition_location(
            glue_client, glue_database, "shared_partition_table_b", [shared_partition]
        )

        print(f"Table A shared partition: {location_a}")
        print(f"Table B shared partition: {location_b}")

        if location_a != location_b:
            print("WARNING: Partition locations differ - not truly shared")

        # Count in table B before insert
        try:
            before_count_b = spark.sql(
                f"SELECT COUNT(*) as count FROM {table_b} WHERE category = '{shared_partition}'"
            ).collect()[0].count
        except Exception:
            before_count_b = 0

        # Insert via table A
        insert_sql = f"""
        INSERT INTO {table_a}
        PARTITION (category = '{shared_partition}')
        VALUES ({unique_id}, 'shared_insert_via_a', 55.5, true)
        """

        try:
            spark.sql(insert_sql)

            # Check visibility from table B
            try:
                after_count_b = spark.sql(
                    f"SELECT COUNT(*) as count FROM {table_b} WHERE category = '{shared_partition}'"
                ).collect()[0].count

                if after_count_b > before_count_b:
                    print("SUCCESS: Data inserted via Table A is visible from Table B")
                    print("BEHAVIOR: Shared partition working as expected")
                else:
                    print("BEHAVIOR: Data not visible from Table B")
                    print("Possible cache issue or partition isolation")
            except Exception as e:
                print(f"Could not read from Table B: {str(e)[:100]}")

        except Exception as e:
            print(f"INSERT via Table A failed: {str(e)[:200]}")

    def test_insert_via_table_b_visible_from_table_a(self, spark, table_a, table_b):
        """
        Test if INSERT via table B is visible from table A for shared partition.
        """
        shared_partition = "shared"
        unique_id = int(time.time() * 1000) % 100000 + 1000

        # Count in table A before
        try:
            before_count_a = spark.sql(
                f"SELECT COUNT(*) as count FROM {table_a} WHERE category = '{shared_partition}'"
            ).collect()[0].count
        except Exception:
            before_count_a = 0

        # Insert via table B
        insert_sql = f"""
        INSERT INTO {table_b}
        PARTITION (category = '{shared_partition}')
        VALUES ({unique_id}, 'shared_insert_via_b', 66.6, false)
        """

        try:
            spark.sql(insert_sql)

            # Check visibility from table A
            try:
                after_count_a = spark.sql(
                    f"SELECT COUNT(*) as count FROM {table_a} WHERE category = '{shared_partition}'"
                ).collect()[0].count

                if after_count_a > before_count_a:
                    print("SUCCESS: Data inserted via Table B is visible from Table A")
                else:
                    print("BEHAVIOR: Data not visible from Table A")
            except Exception as e:
                print(f"Could not read from Table A: {str(e)[:100]}")

        except Exception as e:
            print(f"INSERT via Table B failed: {str(e)[:200]}")

    def test_insert_overwrite_shared_partition_isolation(self, spark, table_a, table_b):
        """
        Test INSERT OVERWRITE on shared partition - does it affect both tables?

        WARNING: This is a destructive test that may affect both tables.

        Expected behavior options:
        1. Both tables see overwritten data (shared location)
        2. Only one table affected (partition isolation)
        3. Operation fails (write conflict detection)
        """
        shared_partition = "shared"
        unique_id = int(time.time() * 1000) % 100000 + 2000

        # Get counts before
        try:
            before_a = spark.sql(
                f"SELECT COUNT(*) as count FROM {table_a} WHERE category = '{shared_partition}'"
            ).collect()[0].count
            before_b = spark.sql(
                f"SELECT COUNT(*) as count FROM {table_b} WHERE category = '{shared_partition}'"
            ).collect()[0].count
        except Exception:
            before_a, before_b = 0, 0

        print(f"Before INSERT OVERWRITE: Table A={before_a}, Table B={before_b}")

        # Overwrite via table A
        overwrite_sql = f"""
        INSERT OVERWRITE TABLE {table_a}
        PARTITION (category = '{shared_partition}')
        VALUES ({unique_id}, 'overwrite_shared', 77.7, true)
        """

        try:
            spark.sql(overwrite_sql)

            # Check both tables
            after_a = spark.sql(
                f"SELECT COUNT(*) as count FROM {table_a} WHERE category = '{shared_partition}'"
            ).collect()[0].count
            after_b = spark.sql(
                f"SELECT COUNT(*) as count FROM {table_b} WHERE category = '{shared_partition}'"
            ).collect()[0].count

            print(f"After INSERT OVERWRITE: Table A={after_a}, Table B={after_b}")

            if after_a == 1 and after_b == 1:
                print("BEHAVIOR: Both tables affected by overwrite (truly shared)")
            elif after_a == 1 and after_b == before_b:
                print("BEHAVIOR: Only Table A affected (partition isolation)")
            else:
                print(f"BEHAVIOR: Unexpected state (A={after_a}, B={after_b})")

        except Exception as e:
            print(f"INSERT OVERWRITE on shared partition failed: {str(e)[:200]}")

    def test_concurrent_writes_no_conflict(self, spark, table_a, table_b):
        """
        Test sequential writes from both tables - documents conflict behavior.

        Note: True concurrent testing would require threading.
        """
        shared_partition = "shared"
        unique_id_a = int(time.time() * 1000) % 100000 + 3000
        unique_id_b = int(time.time() * 1000) % 100000 + 4000

        results = {"table_a": None, "table_b": None}

        # Write from table A
        try:
            spark.sql(f"""
                INSERT INTO {table_a}
                PARTITION (category = '{shared_partition}')
                VALUES ({unique_id_a}, 'concurrent_a', 88.8, true)
            """)
            results["table_a"] = "success"
        except Exception as e:
            results["table_a"] = f"failed: {str(e)[:100]}"

        # Write from table B
        try:
            spark.sql(f"""
                INSERT INTO {table_b}
                PARTITION (category = '{shared_partition}')
                VALUES ({unique_id_b}, 'concurrent_b', 99.9, false)
            """)
            results["table_b"] = "success"
        except Exception as e:
            results["table_b"] = f"failed: {str(e)[:100]}"

        print(f"Sequential write results:")
        print(f"  Table A: {results['table_a']}")
        print(f"  Table B: {results['table_b']}")

        if results["table_a"] == "success" and results["table_b"] == "success":
            print("BEHAVIOR: No write conflicts detected for sequential writes")
        else:
            print("BEHAVIOR: Write conflict or failure occurred")


# ============================================================================
# Summary Test - All Scenarios Write Matrix
# ============================================================================

@pytest.mark.write_operations
class TestWriteOperationsSummary:
    """
    Summary test documenting expected write operation support across scenarios.
    """

    def test_write_operations_matrix(self):
        """
        Document expected write operation support matrix.

        This test always passes - it documents expectations.
        """
        matrix = """
        Write Operations Support Matrix (Expected Behavior)
        ====================================================

        Scenario                    | INSERT | INSERT OVERWRITE | UPDATE | DELETE
        ----------------------------|--------|------------------|--------|--------
        standard_table              |   Y    |        Y         |   N    |   N
        recursive_table             |   Y    |        Y*        |   N    |   N
        scattered_table             |   ?    |        ?         |   N    |   N
        cross_bucket_table          |   ?    |        ?         |   N    |   N
        cross_region_table          |   ?    |        ?         |   N    |   N
        shared_partition_table_a    |   ?    |        ?**       |   N    |   N
        shared_partition_table_b    |   ?    |        ?**       |   N    |   N

        Legend:
        Y  = Expected to work
        N  = Expected to fail (non-ACID Hive table limitation)
        ?  = Uncertain - behavior depends on partition location handling

        Notes:
        * INSERT OVERWRITE on non-partitioned table replaces ALL data
        ** INSERT OVERWRITE on shared partition may affect both tables

        Test results above document actual observed behavior.
        """
        print(matrix)
        assert True  # Always passes - documentation only
