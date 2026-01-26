"""Pytest configuration and fixtures for hive_to_delta tests."""

import os
import subprocess
import json
from typing import Dict, Generator, Optional

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


@pytest.fixture(scope="session")
def sql_warehouse_id() -> Optional[str]:
    """Return SQL warehouse ID for cross-bucket/cross-region query verification.

    Cross-bucket and cross-region tables require SQL Warehouse with instance profile
    + fallback enabled on external locations. Databricks Connect doesn't support this.

    Reads from HIVE_TO_DELTA_TEST_WAREHOUSE_ID env var.
    If not set, attempts to find a serverless warehouse.
    """
    warehouse_id = os.environ.get("HIVE_TO_DELTA_TEST_WAREHOUSE_ID")
    if warehouse_id:
        return warehouse_id

    # Try to find a serverless warehouse
    try:
        result = subprocess.run(
            ["databricks", "warehouses", "list", "--output", "json"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            warehouses = json.loads(result.stdout)
            # Prefer serverless warehouses
            for wh in warehouses:
                if wh.get("enable_serverless_compute"):
                    return wh["id"]
            # Fall back to any warehouse
            if warehouses:
                return warehouses[0]["id"]
    except Exception:
        pass

    return None


def query_via_sql_warehouse(warehouse_id: str, statement: str, timeout: int = 60) -> dict:
    """Execute SQL via SQL Warehouse statement API.

    Used for cross-bucket/cross-region tables that can't be queried via Databricks Connect
    due to credential resolution limitations. SQL Warehouse with instance profile + fallback
    CAN query these tables.

    Args:
        warehouse_id: SQL warehouse ID
        statement: SQL statement to execute
        timeout: Timeout in seconds

    Returns:
        Dict with 'state', 'row_count', and 'data' keys

    Raises:
        RuntimeError: If query fails or times out
    """
    import requests

    # Get auth token
    token_result = subprocess.run(
        ["databricks", "auth", "token", "-p", "DEFAULT"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if token_result.returncode != 0:
        raise RuntimeError(f"Failed to get auth token: {token_result.stderr}")

    token_data = json.loads(token_result.stdout)
    access_token = token_data.get("access_token")

    # Get host from config
    host = None
    try:
        with open(os.path.expanduser("~/.databrickscfg")) as f:
            for line in f:
                if line.strip().startswith("host"):
                    host = line.split("=")[1].strip()
                    break
    except Exception:
        pass

    if not host:
        raise RuntimeError("Could not determine Databricks host")

    # Execute statement
    url = f"{host}/api/2.0/sql/statements/"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "warehouse_id": warehouse_id,
        "statement": statement,
        "wait_timeout": f"{timeout}s",
    }

    response = requests.post(url, headers=headers, json=payload, timeout=timeout + 10)
    response.raise_for_status()

    result = response.json()
    state = result.get("status", {}).get("state")

    if state == "FAILED":
        error = result.get("status", {}).get("error", {}).get("message", "Unknown error")
        raise RuntimeError(f"SQL statement failed: {error}")

    if state != "SUCCEEDED":
        raise RuntimeError(f"SQL statement did not succeed: {state}")

    return {
        "state": state,
        "row_count": result.get("manifest", {}).get("total_row_count", 0),
        "data": result.get("result", {}).get("data_array", []),
    }
