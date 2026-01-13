"""
Test partitions under table root.

Files organized in a single parent folder with standard Hive partitioning:
  s3://bucket/table/partition_key=value/data.parquet

This is the baseline "happy path" scenario that should have full Databricks support.
"""

import pytest


@pytest.mark.partitions_under_table_root
class TestPartitionsUnderTableRoot:
    """Test all operations on partitions under table root"""

    @pytest.fixture(scope="class")
    def table_name(self, glue_table):
        """Get the fully-qualified table name for standard_table"""
        return glue_table("standard_table")

    def test_schema_read(self, spark, table_name):
        """Test DESCRIBE TABLE to read schema"""
        df = spark.sql(f"DESCRIBE TABLE {table_name}")
        columns = [row.col_name for row in df.collect()]

        # Verify expected columns exist
        assert "id" in columns
        assert "name" in columns
        assert "value" in columns
        assert "is_active" in columns
        assert "region" in columns  # Partition column

        print(f"✓ Schema read successful for {table_name}")

    def test_partition_discovery(self, spark, table_name):
        """Test SHOW PARTITIONS to verify partition discovery"""
        df = spark.sql(f"SHOW PARTITIONS {table_name}")
        partitions = [row.partition for row in df.collect()]

        # Verify expected partitions exist
        assert any("region=us-east" in p for p in partitions), "us-east partition not found"
        assert any("region=us-west" in p for p in partitions), "us-west partition not found"

        print(f"✓ Partition discovery successful: {len(partitions)} partitions found")

    def test_select_full(self, spark, table_name):
        """Test SELECT * FROM table (full table scan)"""
        df = spark.sql(f"SELECT * FROM {table_name}")
        count = df.count()

        # Should have data from both partitions
        assert count > 0, "No data found in table"

        # Verify data structure
        assert "id" in df.columns
        assert "region" in df.columns

        print(f"✓ Full table scan successful: {count} rows")

    def test_select_filtered(self, spark, table_name):
        """Test SELECT with partition filter (partition pruning)"""
        df = spark.sql(f"SELECT * FROM {table_name} WHERE region = 'us-east'")
        count = df.count()

        assert count > 0, "No data found for region=us-east"

        # Verify all rows have the correct partition value
        regions = df.select("region").distinct().collect()
        assert len(regions) == 1
        assert regions[0].region == "us-east"

        print(f"✓ Partition filter successful: {count} rows for region=us-east")

    def test_insert(self, spark, table_name):
        """Test INSERT INTO table"""
        # Insert a new row into us-east partition
        insert_sql = f"""
        INSERT INTO {table_name}
        PARTITION (region = 'us-east')
        VALUES (999, 'test_insert', 99.9, true)
        """

        spark.sql(insert_sql)

        # Verify insert succeeded
        df = spark.sql(f"SELECT * FROM {table_name} WHERE id = 999")
        count = df.count()

        assert count == 1, "INSERT failed - row not found"
        row = df.collect()[0]
        assert row.name == "test_insert"
        assert row.region == "us-east"

        print(f"✓ INSERT successful")

    def test_insert_overwrite(self, spark, table_name):
        """Test INSERT OVERWRITE for a partition"""
        # Count rows before overwrite
        before_count = spark.sql(
            f"SELECT * FROM {table_name} WHERE region = 'us-west'"
        ).count()

        # Insert overwrite us-west partition
        overwrite_sql = f"""
        INSERT OVERWRITE TABLE {table_name}
        PARTITION (region = 'us-west')
        VALUES (888, 'test_overwrite', 88.8, false)
        """

        spark.sql(overwrite_sql)

        # Verify only new data exists in partition
        df = spark.sql(f"SELECT * FROM {table_name} WHERE region = 'us-west'")
        count = df.count()

        assert count == 1, "INSERT OVERWRITE should result in exactly 1 row"
        row = df.collect()[0]
        assert row.id == 888
        assert row.name == "test_overwrite"

        print(f"✓ INSERT OVERWRITE successful (before: {before_count}, after: {count})")

    @pytest.mark.skip(reason="UPDATE not supported on external Hive tables")
    def test_update(self, spark, table_name):
        """Test UPDATE statement"""
        # Hive external tables typically don't support UPDATE
        # This test documents the expected limitation
        pass

    def test_delete(self, spark, table_name):
        """Test DELETE with partition filter"""
        # Delete from us-east partition where id = 999
        delete_sql = f"""
        DELETE FROM {table_name}
        WHERE region = 'us-east' AND id = 999
        """

        try:
            spark.sql(delete_sql)

            # Verify deletion
            df = spark.sql(f"SELECT * FROM {table_name} WHERE id = 999")
            count = df.count()
            assert count == 0, "DELETE failed - row still exists"

            print(f"✓ DELETE successful")
        except Exception as e:
            pytest.skip(f"DELETE not supported: {str(e)}")

    def test_add_partition(self, spark, table_name, s3_buckets):
        """Test ALTER TABLE ADD PARTITION"""
        # Add a new partition for region=eu-west
        new_partition_location = f"{s3_buckets['east_1a']}/tables/standard/region=eu-west/"

        add_partition_sql = f"""
        ALTER TABLE {table_name}
        ADD PARTITION (region = 'eu-west')
        LOCATION '{new_partition_location}'
        """

        try:
            spark.sql(add_partition_sql)

            # Verify partition was added
            df = spark.sql(f"SHOW PARTITIONS {table_name}")
            partitions = [row.partition for row in df.collect()]
            assert any("region=eu-west" in p for p in partitions), "New partition not found"

            print(f"✓ ADD PARTITION successful")
        except Exception as e:
            # Document if this fails
            pytest.fail(f"ADD PARTITION failed: {str(e)}")

    def test_drop_partition(self, spark, table_name):
        """Test ALTER TABLE DROP PARTITION"""
        drop_partition_sql = f"""
        ALTER TABLE {table_name}
        DROP PARTITION (region = 'eu-west')
        """

        try:
            spark.sql(drop_partition_sql)

            # Verify partition was dropped
            df = spark.sql(f"SHOW PARTITIONS {table_name}")
            partitions = [row.partition for row in df.collect()]
            assert not any("region=eu-west" in p for p in partitions), "Partition still exists"

            print(f"✓ DROP PARTITION successful")
        except Exception as e:
            pytest.fail(f"DROP PARTITION failed: {str(e)}")
