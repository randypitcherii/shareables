"""Shared constants and helpers for DuckDB + Unity Catalog experiment."""

import os

CATALOG = "fe_randy_pitcher_workspace_catalog"
SCHEMA = "duckdb_uc_experiment"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"
SEPARATOR = "=" * 60


def _try_connect_spark():
    """Get a Databricks Connect session (serverless by default)."""
    from databricks.connect import DatabricksSession

    cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")
    if cluster_id:
        spark = DatabricksSession.builder.clusterId(cluster_id).getOrCreate()
        return spark, f"Databricks Connect → classic cluster {cluster_id}"
    else:
        spark = DatabricksSession.builder.getOrCreate()
        return spark, "Databricks Connect → serverless"


def get_spark():
    """Initialize Spark session via Databricks Connect."""
    spark, mode = _try_connect_spark()
    print(f"Spark mode: {mode}")
    return spark


def print_header(title: str):
    """Print a section header."""
    print(f"\n{SEPARATOR}")
    print(title)
    print(SEPARATOR)


def print_result(operation: str, success: bool, detail: str = ""):
    """Print a formatted result line."""
    icon = "✅" if success else "❌"
    msg = f"  {icon} {operation}"
    if detail:
        msg += f" — {detail}"
    print(msg)
