"""Test partitions across AWS regions."""

import pytest
import time


@pytest.mark.partitions_across_aws_regions
class TestPartitionsAcrossAWSRegions:
    """
    Test partitions across us-east-1 and us-west-2 regions.

    Tests whether Databricks can read from multiple AWS regions and
    measures the latency impact.
    """

    @pytest.fixture(scope="class")
    def table_name(self, glue_table):
        return glue_table("cross_region_table")

    def test_schema_read(self, spark, table_name):
        """Verify table schema can be read."""
        df = spark.sql(f"DESCRIBE TABLE {table_name}")
        columns = [row.col_name for row in df.collect()]
        assert "region" in columns

    def test_partition_discovery(self, spark, table_name):
        """Verify partitions in both regions are registered."""
        df = spark.sql(f"SHOW PARTITIONS {table_name}")
        partitions = [row.partition for row in df.collect()]
        assert len(partitions) >= 2

    def test_select_with_latency_check(self, spark, table_name):
        """
        Test SELECT across regions and measure latency.

        Expected: May work but with higher latency for cross-region access.
        """
        start = time.time()
        df = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}")
        count = df.collect()[0].count
        elapsed = time.time() - start

        print(f"Rows found: {count}")
        print(f"Query time: {elapsed:.2f}s")

        # If query takes >10s, note potential latency issue
        if elapsed > 10:
            print("WARNING: High latency detected for cross-region query")

    def test_select_east_region(self, spark, table_name):
        """Test reading partition from us-east-1."""
        start = time.time()
        df = spark.sql(f"SELECT COUNT(*) as count FROM {table_name} WHERE region='us-east'")
        count = df.collect()[0].count
        elapsed = time.time() - start
        print(f"East region: {count} rows in {elapsed:.2f}s")

    def test_select_west_region(self, spark, table_name):
        """Test reading partition from us-west-2 (cross-region)."""
        start = time.time()
        df = spark.sql(f"SELECT COUNT(*) as count FROM {table_name} WHERE region='us-west'")
        count = df.collect()[0].count
        elapsed = time.time() - start
        print(f"West region: {count} rows in {elapsed:.2f}s")
