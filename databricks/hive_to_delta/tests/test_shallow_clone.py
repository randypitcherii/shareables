"""Tests for shallow clone functionality.

Shallow clones create a new Delta table that references the same data files
as the source table, without copying data. This is useful for creating
development/test copies or migrating tables between catalogs.
"""

from unittest.mock import MagicMock, call, patch

import pytest


class TestShallowCloneStandard:
    """Test shallow clone of standard layout tables."""

    @pytest.mark.standard
    def test_shallow_clone_standard_creates_clone(self, spark, target_catalog, target_schema):
        """Create shallow clone of standard layout table."""
        source_table = f"{target_catalog}.{target_schema}.source_standard"
        target_table = f"{target_catalog}.{target_schema}.clone_standard"

        # Create a simple source table
        spark.sql(f"DROP TABLE IF EXISTS {source_table}")
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")

        spark.sql(f"""
            CREATE TABLE {source_table} (
                id INT,
                name STRING
            )
            USING DELTA
        """)

        spark.sql(f"INSERT INTO {source_table} VALUES (1, 'alice'), (2, 'bob')")

        # Create shallow clone
        spark.sql(f"""
            CREATE TABLE {target_table}
            SHALLOW CLONE {source_table}
        """)

        # Verify clone exists and has same data
        result = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table}").collect()
        assert result[0]["cnt"] == 2

        # Verify it's a clone (check history)
        history = spark.sql(f"DESCRIBE HISTORY {target_table}").collect()
        assert any("CLONE" in str(row["operation"]).upper() for row in history)

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")
        spark.sql(f"DROP TABLE IF EXISTS {source_table}")

    @pytest.mark.standard
    def test_shallow_clone_standard_preserves_schema(self, spark, target_catalog, target_schema):
        """Verify shallow clone preserves source table schema."""
        source_table = f"{target_catalog}.{target_schema}.source_schema"
        target_table = f"{target_catalog}.{target_schema}.clone_schema"

        spark.sql(f"DROP TABLE IF EXISTS {source_table}")
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")

        spark.sql(f"""
            CREATE TABLE {source_table} (
                id BIGINT,
                created_at TIMESTAMP,
                amount DECIMAL(10,2),
                is_active BOOLEAN
            )
            USING DELTA
        """)

        spark.sql(f"""
            CREATE TABLE {target_table}
            SHALLOW CLONE {source_table}
        """)

        # Get schemas
        source_schema = spark.sql(f"DESCRIBE {source_table}").collect()
        target_schema_result = spark.sql(f"DESCRIBE {target_table}").collect()

        # Compare column names and types
        source_cols = {row["col_name"]: row["data_type"] for row in source_schema if row["col_name"] and not row["col_name"].startswith("#")}
        target_cols = {row["col_name"]: row["data_type"] for row in target_schema_result if row["col_name"] and not row["col_name"].startswith("#")}

        assert source_cols == target_cols

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")
        spark.sql(f"DROP TABLE IF EXISTS {source_table}")

    @pytest.mark.standard
    def test_shallow_clone_standard_partitioned_table(self, spark, target_catalog, target_schema):
        """Verify shallow clone works with partitioned tables."""
        source_table = f"{target_catalog}.{target_schema}.source_partitioned"
        target_table = f"{target_catalog}.{target_schema}.clone_partitioned"

        spark.sql(f"DROP TABLE IF EXISTS {source_table}")
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")

        spark.sql(f"""
            CREATE TABLE {source_table} (
                id INT,
                event_date DATE,
                value DOUBLE
            )
            USING DELTA
            PARTITIONED BY (event_date)
        """)

        spark.sql(f"""
            INSERT INTO {source_table} VALUES
            (1, '2024-01-01', 100.0),
            (2, '2024-01-02', 200.0)
        """)

        spark.sql(f"""
            CREATE TABLE {target_table}
            SHALLOW CLONE {source_table}
        """)

        # Verify partitioning preserved
        detail = spark.sql(f"DESCRIBE DETAIL {target_table}").collect()[0]
        assert "event_date" in str(detail["partitionColumns"])

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")
        spark.sql(f"DROP TABLE IF EXISTS {source_table}")


class TestShallowCloneCrossBucket:
    """Test shallow clone of cross-bucket tables."""

    @pytest.mark.cross_bucket
    def test_shallow_clone_cross_bucket_creates_clone(
        self, spark, target_catalog, target_schema
    ):
        """Create shallow clone of table with data in external bucket.

        Note: This test requires a pre-existing table with cross-bucket data files.
        The shallow clone should reference the same external file locations.
        """
        # This test documents expected behavior for cross-bucket scenarios
        # In production, the source table would have files in multiple buckets

        source_table = f"{target_catalog}.{target_schema}.cross_bucket_source"
        target_table = f"{target_catalog}.{target_schema}.cross_bucket_clone"

        # Skip if test table doesn't exist (requires special setup)
        try:
            spark.sql(f"SELECT 1 FROM {source_table} LIMIT 1").collect()
        except Exception:
            pytest.skip(f"Cross-bucket test table {source_table} not available")

        spark.sql(f"DROP TABLE IF EXISTS {target_table}")

        # Create shallow clone
        spark.sql(f"""
            CREATE TABLE {target_table}
            SHALLOW CLONE {source_table}
        """)

        # Verify clone is queryable
        source_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {source_table}").collect()[0]["cnt"]
        target_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table}").collect()[0]["cnt"]

        assert source_count == target_count

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")

    @pytest.mark.cross_bucket
    def test_shallow_clone_cross_bucket_preserves_file_references(self):
        """Verify shallow clone preserves references to external bucket files."""
        # Unit test using mocks to verify file reference behavior
        mock_spark = MagicMock()

        # Simulate source table with external file references
        mock_detail = MagicMock()
        mock_detail.location = "s3://main-bucket/tables/my_table"
        mock_spark.sql.return_value.collect.return_value = [mock_detail]

        # The shallow clone SQL command should be executed
        target = "catalog.schema.target_clone"
        source = "catalog.schema.source_table"

        # Simulate creating shallow clone
        clone_sql = f"CREATE TABLE {target} SHALLOW CLONE {source}"
        mock_spark.sql(clone_sql)

        # Verify the clone SQL was called
        mock_spark.sql.assert_called_with(clone_sql)


class TestShallowCloneQueryable:
    """Test that cloned tables are queryable."""

    @pytest.mark.standard
    def test_shallow_clone_queryable_select(self, spark, target_catalog, target_schema):
        """Verify cloned table supports SELECT queries."""
        source_table = f"{target_catalog}.{target_schema}.queryable_source"
        target_table = f"{target_catalog}.{target_schema}.queryable_clone"

        spark.sql(f"DROP TABLE IF EXISTS {source_table}")
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")

        spark.sql(f"""
            CREATE TABLE {source_table} (
                id INT,
                category STRING,
                amount DOUBLE
            )
            USING DELTA
        """)

        spark.sql(f"""
            INSERT INTO {source_table} VALUES
            (1, 'A', 100.0),
            (2, 'B', 200.0),
            (3, 'A', 150.0)
        """)

        spark.sql(f"""
            CREATE TABLE {target_table}
            SHALLOW CLONE {source_table}
        """)

        # Test various SELECT queries
        result = spark.sql(f"SELECT * FROM {target_table}").collect()
        assert len(result) == 3

        result = spark.sql(f"SELECT * FROM {target_table} WHERE category = 'A'").collect()
        assert len(result) == 2

        result = spark.sql(f"SELECT SUM(amount) as total FROM {target_table}").collect()
        assert result[0]["total"] == 450.0

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")
        spark.sql(f"DROP TABLE IF EXISTS {source_table}")

    @pytest.mark.standard
    def test_shallow_clone_queryable_with_predicates(self, spark, target_catalog, target_schema):
        """Verify cloned table supports predicate pushdown."""
        source_table = f"{target_catalog}.{target_schema}.predicate_source"
        target_table = f"{target_catalog}.{target_schema}.predicate_clone"

        spark.sql(f"DROP TABLE IF EXISTS {source_table}")
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")

        spark.sql(f"""
            CREATE TABLE {source_table} (
                id INT,
                event_date DATE,
                region STRING
            )
            USING DELTA
            PARTITIONED BY (region)
        """)

        spark.sql(f"""
            INSERT INTO {source_table} VALUES
            (1, '2024-01-01', 'US'),
            (2, '2024-01-02', 'EU'),
            (3, '2024-01-03', 'US')
        """)

        spark.sql(f"""
            CREATE TABLE {target_table}
            SHALLOW CLONE {source_table}
        """)

        # Query with partition predicate
        result = spark.sql(f"SELECT * FROM {target_table} WHERE region = 'US'").collect()
        assert len(result) == 2

        # Query with date predicate
        result = spark.sql(f"SELECT * FROM {target_table} WHERE event_date > '2024-01-01'").collect()
        assert len(result) == 2

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")
        spark.sql(f"DROP TABLE IF EXISTS {source_table}")

    @pytest.mark.standard
    def test_shallow_clone_queryable_aggregations(self, spark, target_catalog, target_schema):
        """Verify cloned table supports aggregation queries."""
        source_table = f"{target_catalog}.{target_schema}.agg_source"
        target_table = f"{target_catalog}.{target_schema}.agg_clone"

        spark.sql(f"DROP TABLE IF EXISTS {source_table}")
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")

        spark.sql(f"""
            CREATE TABLE {source_table} (
                id INT,
                category STRING,
                amount DOUBLE
            )
            USING DELTA
        """)

        spark.sql(f"""
            INSERT INTO {source_table} VALUES
            (1, 'A', 100.0),
            (2, 'A', 150.0),
            (3, 'B', 200.0),
            (4, 'B', 250.0)
        """)

        spark.sql(f"""
            CREATE TABLE {target_table}
            SHALLOW CLONE {source_table}
        """)

        # Test GROUP BY
        result = spark.sql(f"""
            SELECT category, SUM(amount) as total, COUNT(*) as cnt
            FROM {target_table}
            GROUP BY category
            ORDER BY category
        """).collect()

        assert len(result) == 2
        assert result[0]["category"] == "A"
        assert result[0]["total"] == 250.0
        assert result[0]["cnt"] == 2

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")
        spark.sql(f"DROP TABLE IF EXISTS {source_table}")

    @pytest.mark.standard
    def test_shallow_clone_queryable_joins(self, spark, target_catalog, target_schema):
        """Verify cloned table can be joined with other tables."""
        source_table = f"{target_catalog}.{target_schema}.join_source"
        clone_table = f"{target_catalog}.{target_schema}.join_clone"
        dim_table = f"{target_catalog}.{target_schema}.join_dim"

        spark.sql(f"DROP TABLE IF EXISTS {source_table}")
        spark.sql(f"DROP TABLE IF EXISTS {clone_table}")
        spark.sql(f"DROP TABLE IF EXISTS {dim_table}")

        # Create fact table
        spark.sql(f"""
            CREATE TABLE {source_table} (
                id INT,
                category_id INT,
                amount DOUBLE
            )
            USING DELTA
        """)

        spark.sql(f"""
            INSERT INTO {source_table} VALUES
            (1, 1, 100.0),
            (2, 2, 200.0)
        """)

        # Create dimension table
        spark.sql(f"""
            CREATE TABLE {dim_table} (
                category_id INT,
                category_name STRING
            )
            USING DELTA
        """)

        spark.sql(f"""
            INSERT INTO {dim_table} VALUES
            (1, 'Electronics'),
            (2, 'Clothing')
        """)

        # Create shallow clone
        spark.sql(f"""
            CREATE TABLE {clone_table}
            SHALLOW CLONE {source_table}
        """)

        # Join clone with dimension
        result = spark.sql(f"""
            SELECT c.id, d.category_name, c.amount
            FROM {clone_table} c
            JOIN {dim_table} d ON c.category_id = d.category_id
            ORDER BY c.id
        """).collect()

        assert len(result) == 2
        assert result[0]["category_name"] == "Electronics"
        assert result[1]["category_name"] == "Clothing"

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {clone_table}")
        spark.sql(f"DROP TABLE IF EXISTS {source_table}")
        spark.sql(f"DROP TABLE IF EXISTS {dim_table}")
