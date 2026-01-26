"""Pytest configuration and fixtures for hive_to_delta tests."""

import os
import subprocess
import json
from typing import Any, Dict, Generator, List, Optional

import boto3
import pytest
from botocore.config import Config
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient


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


class SqlWarehouseConnection:
    """SQL Warehouse connection using databricks-sql-connector.

    This provides a reliable way to query cross-bucket/cross-region external tables
    that Databricks Connect cannot handle due to credential resolution limitations.
    SQL Warehouse with instance profile + fallback CAN query these tables.

    The connection is lazily initialized and reused across queries.
    """

    def __init__(self, warehouse_id: str):
        """Initialize SQL Warehouse connection.

        Args:
            warehouse_id: SQL warehouse ID to connect to
        """
        self._ws = WorkspaceClient()
        warehouse = self._ws.warehouses.get(warehouse_id)
        self._hostname = warehouse.odbc_params.hostname
        self._http_path = warehouse.odbc_params.path
        self._connection = None

    def _get_connection(self):
        """Get or create the SQL Warehouse connection."""
        if self._connection is None:
            self._connection = dbsql.connect(
                server_hostname=self._hostname,
                http_path=self._http_path,
                access_token=self._ws.config.token,
            )
        return self._connection

    def execute(self, sql: str) -> List[Any]:
        """Execute SQL and return all rows.

        Args:
            sql: SQL statement to execute

        Returns:
            List of result rows (tuples)
        """
        conn = self._get_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall() if cursor.description else []

    def get_count(self, table_name: str) -> int:
        """Get row count for a table.

        Args:
            table_name: Fully-qualified table name

        Returns:
            Row count
        """
        result = self.execute(f"SELECT COUNT(*) FROM {table_name}")
        return result[0][0] if result else 0

    def get_sample(self, table_name: str, limit: int = 5) -> List[Any]:
        """Get sample rows from a table.

        Args:
            table_name: Fully-qualified table name
            limit: Number of rows to return

        Returns:
            List of sample rows
        """
        return self.execute(f"SELECT * FROM {table_name} LIMIT {limit}")

    def close(self):
        """Close the connection."""
        if self._connection:
            self._connection.close()


@pytest.fixture(scope="session")
def sql_warehouse_connection(sql_warehouse_id) -> Optional[SqlWarehouseConnection]:
    """Create SQL Warehouse connection for cross-bucket/cross-region queries.

    Yields:
        SqlWarehouseConnection if warehouse is available, None otherwise
    """
    if not sql_warehouse_id:
        yield None
        return

    conn = SqlWarehouseConnection(sql_warehouse_id)
    yield conn
    conn.close()
