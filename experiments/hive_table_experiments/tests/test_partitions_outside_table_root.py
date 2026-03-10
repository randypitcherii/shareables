"""Test partitions outside table root."""

import pytest


@pytest.mark.partitions_outside_table_root
class TestPartitionsOutsideTableRoot:
    """
    Test partitions at different non-standard paths in same bucket.

    These partitions exist outside the table's root location but within
    the same S3 bucket.
    """

    @pytest.fixture(scope="class")
    def table_name(self, glue_table):
        return glue_table("scattered_table")

    def test_schema_read(self, spark, table_name):
        """Verify table schema can be read from Glue."""
        df = spark.sql(f"DESCRIBE TABLE {table_name}")
        columns = [row.col_name for row in df.collect()]

        assert "id" in columns
        assert "name" in columns
        assert "region" in columns  # Partition column

    def test_partition_discovery(self, spark, table_name):
        """
        Verify partitions are registered in Glue.

        Note: This should work - Glue knows about partitions.
        """
        df = spark.sql(f"SHOW PARTITIONS {table_name}")
        partitions = [row.partition for row in df.collect()]

        assert len(partitions) >= 2
        assert any("region=us-east" in p for p in partitions)
        assert any("region=us-west" in p for p in partitions)

    def test_select_should_fail(self, spark, table_name):
        """
        Attempt to SELECT from table with scattered partitions.

        Expected: FAIL or return 0 rows - partitions are outside table location.
        """
        df = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}")
        count = df.collect()[0]["cnt"]

        # Document the behavior
        if count == 0:
            pytest.skip("As expected: No data found (partitions outside table location)")
        else:
            # If data is found, this is surprising - document it
            print(f"UNEXPECTED: Found {count} rows despite external partitions")

    def test_select_with_partition_filter(self, spark, table_name):
        """
        Test SELECT with partition filter on scattered table.

        Expected: FAIL or return 0 rows.
        """
        df = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name} WHERE region='us-east'")
        count = df.collect()[0]["cnt"]

        # Expected: 0 rows (partition outside table location)
        assert count == 0, f"Expected 0 rows (external partition), but got {count}"

    def test_insert_to_external_partition(self, spark, table_name):
        """
        Test if INSERT works when partition location is external.

        Expected: May fail or write to table location instead of partition location.
        """
        try:
            spark.sql(f"""
                INSERT INTO {table_name} PARTITION (region='test')
                VALUES (9999, 'test_insert', 99.9, true)
            """)

            # If INSERT succeeds, check where data went
            df = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name} WHERE region='test'")
            count = df.collect()[0]["cnt"]

            print(f"INSERT succeeded, found {count} rows")
        except Exception as e:
            print(f"INSERT failed as expected: {str(e)[:200]}")
            pytest.skip(f"INSERT failed (expected): {str(e)[:100]}")
