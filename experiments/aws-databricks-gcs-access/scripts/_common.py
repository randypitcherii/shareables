"""Shared constants and helpers for GCS access experiment scripts."""

import os

BUCKET = "dbx-gcs-access-experiment"
SCOPE = "gcs-experiment"
SEPARATOR = "=" * 60


def _try_native_spark():
    """Try to get a native SparkSession (running on a Databricks cluster).

    Returns (spark, mode_description) or raises ImportError/Exception if not available.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    # Verify we're actually on a cluster by checking for Databricks-specific config
    try:
        cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
        return spark, f"native SparkSession on cluster {cluster_id}"
    except Exception:
        # Might be a local Spark — check if it's a Databricks environment
        try:
            spark.conf.get("spark.databricks.clusterUsageTags.clusterType")
            return spark, "native SparkSession (Databricks environment)"
        except Exception:
            raise RuntimeError("SparkSession exists but not on a Databricks cluster")


def _try_connect_spark():
    """Fall back to Databricks Connect for remote execution.

    Returns (spark, mode_description).
    """
    from databricks.connect import DatabricksSession

    cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")
    if cluster_id:
        spark = DatabricksSession.builder.clusterId(cluster_id).getOrCreate()
        return spark, f"Databricks Connect → classic cluster {cluster_id}"
    else:
        spark = DatabricksSession.builder.getOrCreate()
        return spark, "Databricks Connect → serverless"


def _try_native_dbutils():
    """Try to get dbutils from the native Databricks runtime."""
    # On Databricks clusters, dbutils is injected by the runtime
    # via IPython or the DBUtils module
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        return DBUtils(SparkSession.builder.getOrCreate())
    except Exception:
        pass

    # Fallback: try the Databricks SDK's dbutils
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient().dbutils


def get_spark_and_dbutils():
    """Initialize Spark session and dbutils, auto-detecting the environment.

    Priority:
      1. Native SparkSession (running directly on a Databricks cluster)
      2. Databricks Connect (running locally, connecting remotely)

    Set DATABRICKS_CLUSTER_ID env var to target a specific classic cluster
    when using Databricks Connect. Unset it for serverless.
    """
    mode = None

    # Try native Spark first (for job clusters, interactive notebooks)
    try:
        spark, mode = _try_native_spark()
    except Exception:
        pass

    # Fall back to Databricks Connect (for local development)
    if mode is None:
        spark, mode = _try_connect_spark()

    print(f"Spark mode: {mode}")
    dbutils = _try_native_dbutils()
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
