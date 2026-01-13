"""Test partitions across S3 buckets."""

import pytest


@pytest.mark.partitions_across_s3_buckets
class TestPartitionsAcrossS3Buckets:
    """
    Test partitions across different S3 buckets in same region.

    Tests whether Databricks can read partitions from multiple buckets
    within the same AWS region.
    """

    @pytest.fixture(scope="class")
    def table_name(self, glue_table):
        return glue_table("cross_bucket_table")

    def test_schema_read(self, spark, table_name):
        """Verify table schema can be read."""
        df = spark.sql(f"DESCRIBE TABLE {table_name}")
        columns = [row.col_name for row in df.collect()]
        assert "region" in columns

    def test_partition_discovery(self, spark, table_name):
        """Verify both bucket partitions are registered."""
        df = spark.sql(f"SHOW PARTITIONS {table_name}")
        partitions = [row.partition for row in df.collect()]
        assert len(partitions) >= 2

    def test_select_all(self, spark, table_name):
        """
        Test SELECT across partitions in different buckets.

        Expected: May fail or only read from one bucket.
        """
        df = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}")
        count = df.collect()[0].count

        print(f"Rows found across buckets: {count}")
        # Document behavior - don't assert specific value

    def test_select_bucket1_partition(self, spark, table_name):
        """Test reading partition from bucket 1."""
        df = spark.sql(f"SELECT COUNT(*) as count FROM {table_name} WHERE region='us-east'")
        count = df.collect()[0].count
        print(f"Bucket 1 partition rows: {count}")

    def test_select_bucket2_partition(self, spark, table_name):
        """Test reading partition from bucket 2."""
        df = spark.sql(f"SELECT COUNT(*) as count FROM {table_name} WHERE region='us-west'")
        count = df.collect()[0].count
        print(f"Bucket 2 partition rows: {count}")
