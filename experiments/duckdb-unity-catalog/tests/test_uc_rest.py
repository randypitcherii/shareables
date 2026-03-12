"""
DuckDB UC REST client eval against Databricks Unity Catalog.

Path: UC REST catalog (uc_catalog extension) + Delta reader/writer
Expected: Reads work for all tables. Writes work for managed Delta only.

Requires:
  - GRANT EXTERNAL USE SCHEMA on the target schema
  - Unnamed secret workaround for duckdb/unity_catalog#48

Run:
  cd experiments/duckdb-unity-catalog
  uv run pytest tests/test_uc_rest.py -v
"""

from __future__ import annotations

import importlib
import os

import duckdb
import pytest

importlib.import_module("pytz")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_host = os.environ["DATABRICKS_HOST"]
WORKSPACE_URL = _host if _host.startswith("https://") else f"https://{_host}"
CATALOG = os.environ["DATABRICKS_CATALOG"]
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "duckdb_uc_experiment")

TABLE_MANAGED_DELTA = "managed_delta"
TABLE_EXTERNAL_DELTA = "external_delta"
TABLE_MANAGED_ICEBERG = "managed_iceberg"

# ---------------------------------------------------------------------------
# xfail markers — clearly labeled as BUG (should work) or EXPECTED (wrong path)
# ---------------------------------------------------------------------------

# BUG: Reads/writes to managed Delta SHOULD work via UC REST with Delta reader/writer.
# The temporary-table-credentials API returns Bad Request for Delta+UniForm tables.
# Tracked: https://github.com/duckdb/uc_catalog/issues/68
XFAIL_BUG_BAD_REQUEST = pytest.mark.xfail(
    reason="BUG: temporary-table-credentials API returns Bad Request for Delta tables (duckdb/uc_catalog#68)"
)

# EXPECTED FAILURE: Delta writer cannot write to Iceberg tables.
# Writes to Iceberg should go through Iceberg REST with an Iceberg writer, not here.
XFAIL_EXPECTED_ICEBERG_NO_WRITE = pytest.mark.xfail(
    reason="EXPECTED: Delta writer cannot write to Iceberg tables — use Iceberg REST (Iceberg writer) instead"
)

# ---------------------------------------------------------------------------
# Shared parametrize lists
# ---------------------------------------------------------------------------

# Reads: all types should work. Delta tables blocked by Bad Request bug.
PARAMS_READ = [
    pytest.param(TABLE_MANAGED_DELTA, marks=XFAIL_BUG_BAD_REQUEST),
    pytest.param(TABLE_EXTERNAL_DELTA, marks=XFAIL_BUG_BAD_REQUEST),
    pytest.param(TABLE_MANAGED_ICEBERG),  # reads work
]

# Writes to Delta: should work, blocked by Bad Request bug.
# Writes to Iceberg: not expected to work (wrong writer for this format).
PARAMS_WRITE = [
    pytest.param(TABLE_MANAGED_DELTA, marks=XFAIL_BUG_BAD_REQUEST),
    pytest.param(TABLE_EXTERNAL_DELTA, marks=XFAIL_BUG_BAD_REQUEST),
    pytest.param(TABLE_MANAGED_ICEBERG, marks=XFAIL_EXPECTED_ICEBERG_NO_WRITE),
]

# ---------------------------------------------------------------------------
# Auth helper
# ---------------------------------------------------------------------------

def _get_databricks_token() -> str:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    headers_or_fn = w.config.authenticate()
    auth_header = headers_or_fn() if callable(headers_or_fn) else headers_or_fn
    token = auth_header.get("Authorization", "").removeprefix("Bearer ")
    if not token:
        raise RuntimeError("Failed to get Databricks token via SDK auth chain")
    return token


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def duckdb_con():
    """DuckDB connection via UC REST catalog (Delta reader/writer).

    Uses unnamed secret as workaround for duckdb/unity_catalog#48.
    """
    token = _get_databricks_token()
    con = duckdb.connect()
    con.execute("INSTALL uc_catalog FROM core; LOAD uc_catalog;")
    con.execute("INSTALL delta; LOAD delta;")
    con.execute(f"""
        CREATE SECRET (
            TYPE UC,
            TOKEN '{token}',
            ENDPOINT '{WORKSPACE_URL}',
            AWS_REGION 'us-east-1'
        );
    """)
    con.execute(f"ATTACH '{CATALOG}' AS uc (TYPE UC_CATALOG);")
    yield con
    con.close()


# ---------------------------------------------------------------------------
# READS — all table types should work
# ---------------------------------------------------------------------------

@pytest.mark.reads
@pytest.mark.parametrize("table_type", PARAMS_READ)
def test_select(duckdb_con, table_type):
    """Read via UC REST (Delta reader). All table types should work."""
    fqn = f"uc.{SCHEMA}.{table_type}"
    rows = duckdb_con.execute(f"SELECT * FROM {fqn} LIMIT 5").fetchall()
    assert len(rows) > 0, f"Expected rows from {fqn}"


@pytest.mark.reads
@pytest.mark.parametrize("table_type", PARAMS_READ)
def test_predicate_pushdown(duckdb_con, table_type):
    """Filtered read via UC REST (Delta reader). All table types should work."""
    fqn = f"uc.{SCHEMA}.{table_type}"
    duckdb_con.execute(f"SELECT * FROM {fqn} WHERE trip_distance > 100 LIMIT 5").fetchall()


# ---------------------------------------------------------------------------
# DML — only managed Delta writes should work (Iceberg is wrong path here)
# ---------------------------------------------------------------------------

@pytest.mark.dml
@pytest.mark.parametrize("table_type", PARAMS_WRITE)
def test_insert(duckdb_con, table_type):
    """INSERT via UC REST (Delta writer). Only managed Delta should work."""
    fqn = f"uc.{SCHEMA}.{table_type}"
    duckdb_con.execute(f"INSERT INTO {fqn} SELECT * FROM {fqn} LIMIT 1;")


@pytest.mark.dml
@pytest.mark.parametrize("table_type", PARAMS_WRITE)
def test_update(duckdb_con, table_type):
    """UPDATE via UC REST (Delta writer). Only managed Delta should work."""
    fqn = f"uc.{SCHEMA}.{table_type}"
    before = duckdb_con.execute(f"SELECT count(*) FROM {fqn}").fetchone()[0]
    duckdb_con.execute(f"""
        UPDATE {fqn}
        SET trip_distance = trip_distance
        WHERE trip_distance = (SELECT MIN(trip_distance) FROM {fqn})
    """)
    after = duckdb_con.execute(f"SELECT count(*) FROM {fqn}").fetchone()[0]
    assert after == before


@pytest.mark.dml
@pytest.mark.parametrize("table_type", PARAMS_WRITE)
def test_delete(duckdb_con, table_type):
    """DELETE via UC REST (Delta writer). Only managed Delta should work."""
    fqn = f"uc.{SCHEMA}.{table_type}"
    before = duckdb_con.execute(f"SELECT count(*) FROM {fqn}").fetchone()[0]
    duckdb_con.execute(f"""
        DELETE FROM {fqn}
        WHERE trip_distance = (SELECT MAX(trip_distance) FROM {fqn})
    """)
    after = duckdb_con.execute(f"SELECT count(*) FROM {fqn}").fetchone()[0]
    assert after < before
