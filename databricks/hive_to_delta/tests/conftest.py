"""Pytest configuration and fixtures for hive_to_delta tests."""

import os
from typing import Dict, Generator

import boto3
import pytest
from botocore.config import Config


@pytest.fixture(scope="session")
def spark():
    """Create a Databricks Connect SparkSession for testing.

    Connection is configured via:
    1. DATABRICKS_CONFIG_PROFILE env var to select profile from ~/.databrickscfg
    2. Or DEFAULT profile with serverless_compute_id=auto
    3. Or explicit DATABRICKS_CLUSTER_ID for classic clusters

    Yields:
        SparkSession: Databricks Connect session.
    """
    from databricks.connect import DatabricksSession

    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")

    builder = DatabricksSession.builder

    if profile:
        builder = builder.profile(profile)

    if cluster_id and cluster_id != "auto":
        builder = builder.clusterId(cluster_id)

    session = builder.getOrCreate()

    yield session

    session.stop()


@pytest.fixture(scope="session")
def glue_database() -> str:
    """Return the test Glue database name.

    Reads from HIVE_TO_DELTA_TEST_GLUE_DATABASE env var,
    defaults to "hive_to_delta_test".
    """
    return os.environ.get("HIVE_TO_DELTA_TEST_GLUE_DATABASE", "hive_to_delta_test")


@pytest.fixture(scope="session")
def aws_region() -> str:
    """Return the AWS region for Glue/S3 operations.

    Reads from AWS_REGION env var, defaults to "us-east-1".
    """
    return os.environ.get("AWS_REGION", "us-east-1")


@pytest.fixture(scope="session")
def s3_clients() -> Dict[str, boto3.client]:
    """S3 clients for each region that may contain table data.

    Returns dict mapping region name to boto3 S3 client.
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


@pytest.fixture(scope="session")
def target_catalog() -> str:
    """Return the Unity Catalog name for test tables.

    Reads from HIVE_TO_DELTA_TEST_CATALOG env var,
    defaults to "fe_randy_pitcher_workspace_catalog".
    """
    return os.environ.get("HIVE_TO_DELTA_TEST_CATALOG", "fe_randy_pitcher_workspace_catalog")


@pytest.fixture(scope="session")
def target_schema() -> str:
    """Return the Unity Catalog schema name for test tables.

    Reads from HIVE_TO_DELTA_TEST_SCHEMA env var,
    defaults to "hive_to_delta_tests".
    """
    return os.environ.get("HIVE_TO_DELTA_TEST_SCHEMA", "hive_to_delta_tests")


@pytest.fixture(scope="module")
def created_tables() -> list[str]:
    """Track tables created during a test module for cleanup.

    Returns:
        List to append fully-qualified table names to.
    """
    return []


@pytest.fixture(scope="module")
def cleanup_tables(spark, created_tables) -> Generator[list[str], None, None]:
    """Cleanup fixture that drops test tables after each test module.

    Usage:
        def test_something(spark, cleanup_tables, target_catalog, target_schema):
            table_name = f"{target_catalog}.{target_schema}.my_test_table"
            cleanup_tables.append(table_name)
            # ... test code that creates the table ...

    Yields:
        List to append fully-qualified table names for cleanup.
    """
    yield created_tables

    # Cleanup: drop all tables that were created during the test module
    for table_name in created_tables:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        except Exception as e:
            # Log but don't fail - cleanup is best effort
            print(f"Warning: Failed to drop table {table_name}: {e}")
