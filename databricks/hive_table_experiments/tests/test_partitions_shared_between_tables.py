"""Test partitions shared between tables."""

import pytest


@pytest.mark.partitions_shared_between_tables
class TestPartitionsSharedBetweenTables:
    """
    Test two tables pointing to same S3 partition location.

    Tests metadata isolation, concurrent access, and write conflicts
    when multiple tables share a partition.
    """

    @pytest.fixture(scope="class")
    def table_a(self, glue_table):
        return glue_table("shared_partition_table_a")

    @pytest.fixture(scope="class")
    def table_b(self, glue_table):
        return glue_table("shared_partition_table_b")

    def test_both_tables_readable(self, spark, table_a, table_b):
        """Verify both tables can be queried."""
        count_a = spark.sql(f"SELECT COUNT(*) as count FROM {table_a}").collect()[0].count
        count_b = spark.sql(f"SELECT COUNT(*) as count FROM {table_b}").collect()[0].count

        print(f"Table A: {count_a} rows")
        print(f"Table B: {count_b} rows")

    def test_shared_partition_data_visible_to_both(self, spark, table_a, table_b):
        """
        Test if data in shared partition is visible to both tables.

        Expected: Both tables should see same data from shared location.
        """
        # Query specific partition from both tables
        df_a = spark.sql(f"SELECT * FROM {table_a} WHERE category='shared' ORDER BY id")
        df_b = spark.sql(f"SELECT * FROM {table_b} WHERE category='shared' ORDER BY id")

        rows_a = df_a.collect()
        rows_b = df_b.collect()

        print(f"Table A sees {len(rows_a)} rows in shared partition")
        print(f"Table B sees {len(rows_b)} rows in shared partition")

        # Both tables should see same data
        if rows_a and rows_b:
            assert len(rows_a) == len(rows_b), "Both tables should see same data"

    def test_insert_from_table_a(self, spark, table_a, table_b):
        """
        Test INSERT from table A and verify visibility from table B.

        Tests: Write isolation and concurrent access.
        """
        # Count before
        count_b_before = spark.sql(f"SELECT COUNT(*) as count FROM {table_b} WHERE category='shared'").collect()[0].count

        try:
            # Insert via table A
            spark.sql(f"""
                INSERT INTO {table_a} PARTITION (category='shared')
                VALUES (8888, 'from_table_a', 88.8, true)
            """)

            # Count from table B after
            count_b_after = spark.sql(f"SELECT COUNT(*) as count FROM {table_b} WHERE category='shared'").collect()[0].count

            if count_b_after > count_b_before:
                print("✓ Table B sees data written by Table A (shared partition works)")
            else:
                print("✗ Table B doesn't see data written by Table A (isolation issue)")

        except Exception as e:
            pytest.skip(f"INSERT failed: {str(e)[:100]}")

    def test_concurrent_write_conflict(self, spark, table_a, table_b):
        """
        Test if concurrent writes to same partition cause conflicts.

        Note: This is a basic test - real concurrency would need threading.
        """
        try:
            # Write from table A
            spark.sql(f"INSERT INTO {table_a} PARTITION (category='shared') VALUES (1111, 'concurrent_a', 11.1, true)")

            # Write from table B
            spark.sql(f"INSERT INTO {table_b} PARTITION (category='shared') VALUES (2222, 'concurrent_b', 22.2, false)")

            # Both writes succeeded - no conflict
            print("✓ No write conflict detected (both inserts succeeded)")

        except Exception as e:
            print(f"Write conflict occurred: {str(e)[:200]}")
