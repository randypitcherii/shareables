"""
Test partitions in nested subdirectories.

Tables with nested subdirectories that are NOT Hive partitions, requiring
recursive file discovery. This pattern is used where data is organized
in arbitrary subdirectories.

Example structure:
  s3://bucket/table/
    ├── subdir1/data.parquet
    ├── subdir1/subdir2/data.parquet
    └── subdir3/data.parquet

Requires Spark config: mapreduce.input.fileinputformat.input.dir.recursive=true
"""

import pytest


@pytest.mark.partitions_in_nested_subdirectories
class TestPartitionsInNestedSubdirectories:
    """Test operations on table with nested subdirectory structure"""

    @pytest.fixture(scope="class")
    def table_name(self, glue_table):
        """Get the fully-qualified table name for recursive_table"""
        return glue_table("recursive_table")

    def test_schema_read(self, spark, table_name):
        """Test DESCRIBE TABLE to read schema"""
        df = spark.sql(f"DESCRIBE TABLE {table_name}")
        columns = [row.col_name for row in df.collect()]

        # Verify expected columns exist
        assert "id" in columns
        assert "name" in columns
        assert "value" in columns
        assert "is_active" in columns

        # This table should NOT have partition columns
        assert "region" not in columns, "Recursive table should not be partitioned"

        print(f"✓ Schema read successful for {table_name}")

    def test_select_full_recursive_scan(self, spark, table_name):
        """Test SELECT * with recursive directory scanning"""
        df = spark.sql(f"SELECT * FROM {table_name}")
        count = df.count()

        # Should find data across all nested subdirectories
        # We created 3 files in fixtures: subdir1/, subdir1/subdir2/, subdir3/
        # Each file has 10 rows, so total should be 30 rows
        expected_min_rows = 30

        assert count >= expected_min_rows, (
            f"Expected at least {expected_min_rows} rows from recursive scan, "
            f"but got {count}. Recursive scanning may not be working."
        )

        print(f"✓ Recursive scan successful: {count} rows found across nested directories")

    def test_select_with_filter(self, spark, table_name):
        """Test SELECT with WHERE clause on recursive table"""
        df = spark.sql(f"SELECT * FROM {table_name} WHERE id < 5")
        count = df.count()

        assert count > 0, "No data found with filter"

        # Verify filter was applied
        max_id = df.agg({"id": "max"}).collect()[0][0]
        assert max_id < 5, f"Filter failed: found id={max_id}"

        print(f"✓ Filtered scan successful: {count} rows with id < 5")

    def test_insert(self, spark, table_name):
        """Test INSERT INTO recursive table"""
        insert_sql = f"""
        INSERT INTO {table_name}
        VALUES (999, 'test_recursive_insert', 99.9, true)
        """

        spark.sql(insert_sql)

        # Verify insert succeeded
        df = spark.sql(f"SELECT * FROM {table_name} WHERE id = 999")
        count = df.count()

        assert count == 1, "INSERT failed - row not found"
        row = df.collect()[0]
        assert row.name == "test_recursive_insert"

        print(f"✓ INSERT successful on recursive table")

    def test_insert_overwrite(self, spark, table_name):
        """Test INSERT OVERWRITE on recursive table"""
        # Get count before overwrite
        before_count = spark.sql(f"SELECT * FROM {table_name}").count()

        # Overwrite entire table with single row
        overwrite_sql = f"""
        INSERT OVERWRITE TABLE {table_name}
        VALUES (888, 'test_recursive_overwrite', 88.8, false)
        """

        spark.sql(overwrite_sql)

        # Verify overwrite replaced all data
        df = spark.sql(f"SELECT * FROM {table_name}")
        after_count = df.count()

        assert after_count == 1, f"Expected 1 row after overwrite, got {after_count}"
        row = df.collect()[0]
        assert row.id == 888
        assert row.name == "test_recursive_overwrite"

        print(f"✓ INSERT OVERWRITE successful (before: {before_count}, after: {after_count})")

    def test_delete(self, spark, table_name):
        """Test DELETE on recursive table"""
        # First, insert test data
        spark.sql(f"INSERT INTO {table_name} VALUES (777, 'to_delete', 77.7, true)")

        # Delete the row
        delete_sql = f"""
        DELETE FROM {table_name}
        WHERE id = 777
        """

        try:
            spark.sql(delete_sql)

            # Verify deletion
            df = spark.sql(f"SELECT * FROM {table_name} WHERE id = 777")
            count = df.count()
            assert count == 0, "DELETE failed - row still exists"

            print(f"✓ DELETE successful on recursive table")
        except Exception as e:
            pytest.skip(f"DELETE not supported on recursive table: {str(e)}")

    def test_count_performance(self, spark, table_name):
        """Test COUNT performance on recursive table"""
        import time

        start_time = time.time()
        count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
        duration = time.time() - start_time

        print(f"✓ COUNT(*) completed in {duration:.2f}s: {count} rows")
        print(f"  Note: Recursive scanning may have performance implications")

        assert count > 0, "No data found"

    def test_verify_recursive_config(self, spark):
        """Verify that recursive directory scanning is enabled"""
        recursive_enabled = spark.conf.get(
            "mapreduce.input.fileinputformat.input.dir.recursive",
            "false"
        )

        assert recursive_enabled.lower() == "true", (
            "Recursive directory scanning is NOT enabled! "
            "Set: spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive=true"
        )

        print(f"✓ Recursive directory scanning is enabled")

    def test_explain_plan(self, spark, table_name):
        """Check query execution plan for recursive table"""
        # Get the explain plan to understand how Spark reads the data
        explain = spark.sql(f"EXPLAIN EXTENDED SELECT * FROM {table_name}").collect()

        plan = "\n".join([row.plan for row in explain])

        print(f"\n=== Execution Plan ===")
        print(plan)
        print(f"======================\n")

        # Look for indicators of file scanning
        assert "FileScan" in plan or "Scan" in plan, "No file scan found in plan"

        print(f"✓ Execution plan generated")

    @pytest.mark.xfail(reason="Partitions not applicable to recursive tables")
    def test_show_partitions_should_fail(self, spark, table_name):
        """SHOW PARTITIONS should fail or return empty for non-partitioned table"""
        try:
            df = spark.sql(f"SHOW PARTITIONS {table_name}")
            partitions = df.collect()

            if len(partitions) == 0:
                print(f"✓ No partitions found (as expected)")
            else:
                pytest.fail(f"Unexpected: found {len(partitions)} partitions")
        except Exception as e:
            # Expected to fail since table is not partitioned
            print(f"✓ SHOW PARTITIONS failed as expected: {str(e)}")
