"""Shared constants and helpers for GCS access experiment scripts."""

import os

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient

BUCKET = "dbx-gcs-access-experiment"
SCOPE = "gcs-experiment"
SEPARATOR = "=" * 60


def get_spark_and_dbutils():
    """Initialize Spark session and dbutils from WorkspaceClient.

    Set DATABRICKS_CLUSTER_ID env var to target a classic cluster.
    Unset it (or leave default config) for serverless.
    """
    cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")
    if cluster_id:
        spark = DatabricksSession.builder.clusterId(cluster_id).getOrCreate()
        print(f"Connected to classic cluster: {cluster_id}")
    else:
        spark = DatabricksSession.builder.getOrCreate()
        print("Connected via default config (serverless)")
    dbutils = WorkspaceClient().dbutils
    return spark, dbutils


def get_secret(dbutils, key: str) -> str:
    """Retrieve a secret from the experiment scope."""
    return dbutils.secrets.get(scope=SCOPE, key=key)


def print_header(title: str):
    """Print a section header."""
    print(f"\n{SEPARATOR}")
    print(title)
    print(SEPARATOR)


def get_cluster_type(spark) -> str:
    """Detect the Databricks compute type."""
    try:
        return spark.conf.get("spark.databricks.clusterUsageTags.clusterType")
    except Exception:
        return "unknown"


def print_summary(spark):
    """Print compute type summary footer."""
    print_header(f"Compute type: {get_cluster_type(spark)}")
