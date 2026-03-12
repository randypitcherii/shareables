"""Shared constants and helpers for DuckDB + Unity Catalog experiment."""

import os

CATALOG = os.environ["DATABRICKS_CATALOG"]
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "duckdb_uc_experiment")
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"
WORKSPACE = os.environ["DATABRICKS_HOST"].removeprefix("https://")
WORKSPACE_URL = f"https://{WORKSPACE}"
SEPARATOR = "=" * 60


def get_databricks_token() -> str:
    """Get a Databricks access token via the SDK's default auth chain.

    Uses WorkspaceClient which respects ~/.databrickscfg, env vars, and
    OAuth (databricks-cli auth type). No static PAT needed.
    """
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    # The SDK's config.authenticate() returns headers dict or callable depending on version
    headers_or_fn = w.config.authenticate()
    if callable(headers_or_fn):
        auth_header = headers_or_fn()
    else:
        auth_header = headers_or_fn
    token = auth_header.get("Authorization", "").removeprefix("Bearer ")
    if not token:
        raise RuntimeError("Failed to get Databricks token via SDK auth chain")
    print(f"  Auth: Databricks SDK ({w.config.auth_type})")
    return token


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
