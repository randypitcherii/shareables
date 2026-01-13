"""Fixtures specific to Delta operations testing."""

import pytest
from hive_table_experiments import UC_EXTERNAL_DELTA


@pytest.fixture(scope="module")
def access_config():
    """Access method configuration for Delta operations testing."""
    return UC_EXTERNAL_DELTA


@pytest.fixture(scope="function")
def delta_test_table(spark, access_config, tmp_path):
    """Create a temporary Delta table for testing OPTIMIZE/VACUUM.

    Creates multiple small files to verify compaction behavior.
    """
    table_name = f"{access_config.catalog}.{access_config.schema}.delta_test_temp"

    # Create table with short retention for testing
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id BIGINT,
            name STRING,
            value DOUBLE
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.logRetentionDuration' = 'interval 1 hours',
            'delta.deletedFileRetentionDuration' = 'interval 1 hours'
        )
    """)

    yield table_name

    # Cleanup
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
