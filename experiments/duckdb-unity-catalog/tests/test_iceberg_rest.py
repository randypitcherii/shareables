"""
DuckDB Iceberg REST client eval against Databricks Unity Catalog.

Path: Iceberg REST catalog + Iceberg reader/writer
Expected: Reads work for all tables. Writes work for managed Iceberg only.

Run:
  cd experiments/duckdb-unity-catalog
  uv run pytest tests/test_iceberg_rest.py -v
"""

from __future__ import annotations

import importlib
import os
import uuid

import duckdb
import pytest

importlib.import_module("pytz")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WORKSPACE = os.environ["DATABRICKS_HOST"].removeprefix("https://")
CATALOG = os.environ["DATABRICKS_CATALOG"]
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "duckdb_uc_experiment")
ENDPOINT = f"https://{WORKSPACE}/api/2.1/unity-catalog/iceberg-rest"

TABLE_MANAGED_ICEBERG = "managed_iceberg"
TABLE_MANAGED_DELTA = "managed_delta"

# ---------------------------------------------------------------------------
# xfail markers — clearly labeled as BUG (should work) or EXPECTED (wrong path)
# ---------------------------------------------------------------------------

# BUG: Reads/writes to managed Iceberg SHOULD work via Iceberg REST.
# DuckDB's vended credential scope covers metadata but not data files.
# Tracked: https://github.com/duckdb/duckdb-iceberg/issues/792
XFAIL_BUG_CREDENTIAL_SCOPE = pytest.mark.xfail(
    reason="BUG: Iceberg REST credential scope doesn't cover data files (duckdb/duckdb-iceberg#792)"
)

# EXPECTED FAILURE: Delta tables are read-only via Iceberg REST.
# Writes to Delta should go through UC REST with a Delta writer, not here.
XFAIL_EXPECTED_DELTA_NO_WRITE = pytest.mark.xfail(
    reason="EXPECTED: Iceberg writer cannot write to Delta tables — use UC REST (Delta writer) instead"
)

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
    """DuckDB connection via Iceberg REST catalog (Iceberg reader/writer)."""
    token = _get_databricks_token()
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("UPDATE EXTENSIONS;")
    con.execute(f"CREATE SECRET uc_secret (TYPE iceberg, TOKEN '{token}');")
    con.execute(f"""
        ATTACH '{CATALOG}' AS uc (
            TYPE iceberg,
            SECRET uc_secret,
            ENDPOINT '{ENDPOINT}',
            ACCESS_DELEGATION_MODE 'vended_credentials'
        );
    """)
    yield con
    con.close()


@pytest.fixture(scope="session")
def workspace_client():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


# ---------------------------------------------------------------------------
# READS — both table types should work
# ---------------------------------------------------------------------------

@pytest.mark.reads
@pytest.mark.parametrize(
    "table_type",
    [
        pytest.param(TABLE_MANAGED_DELTA),  # reads via UniForm — works
        pytest.param(TABLE_MANAGED_ICEBERG, marks=XFAIL_BUG_CREDENTIAL_SCOPE),  # should work, blocked by bug
    ],
)
def test_select(duckdb_con, table_type):
    """Read via Iceberg REST. Both table types should work."""
    fqn = f"uc.{SCHEMA}.{table_type}"
    rows = duckdb_con.execute(f"SELECT * FROM {fqn} LIMIT 5").fetchall()
    assert len(rows) > 0, f"Expected rows from {fqn}"


@pytest.mark.reads
@pytest.mark.parametrize(
    "table_type",
    [
        pytest.param(TABLE_MANAGED_DELTA),
        pytest.param(TABLE_MANAGED_ICEBERG),  # predicate pushdown works (metadata-only)
    ],
)
def test_predicate_pushdown(duckdb_con, table_type):
    """Filtered read via Iceberg REST. Both table types should work."""
    fqn = f"uc.{SCHEMA}.{table_type}"
    duckdb_con.execute(f"SELECT * FROM {fqn} WHERE trip_distance > 100 LIMIT 5").fetchall()


# ---------------------------------------------------------------------------
# DML — only managed Iceberg writes should work (Delta is read-only here)
# ---------------------------------------------------------------------------

@pytest.mark.dml
@pytest.mark.parametrize(
    "table_type",
    [
        pytest.param(TABLE_MANAGED_ICEBERG, marks=XFAIL_BUG_CREDENTIAL_SCOPE),  # should work, blocked by bug
        pytest.param(TABLE_MANAGED_DELTA, marks=XFAIL_EXPECTED_DELTA_NO_WRITE),  # not expected to work
    ],
)
def test_insert(duckdb_con, table_type):
    """INSERT via Iceberg REST. Only managed Iceberg should work."""
    fqn = f"uc.{SCHEMA}.{table_type}"
    duckdb_con.execute(f"INSERT INTO {fqn} SELECT * FROM {fqn} LIMIT 1;")


@pytest.mark.dml
@pytest.mark.parametrize(
    "table_type",
    [
        pytest.param(TABLE_MANAGED_ICEBERG, marks=XFAIL_BUG_CREDENTIAL_SCOPE),
        pytest.param(TABLE_MANAGED_DELTA, marks=XFAIL_EXPECTED_DELTA_NO_WRITE),
    ],
)
def test_update(duckdb_con, table_type):
    """UPDATE via Iceberg REST. Only managed Iceberg should work."""
    fqn = f"uc.{SCHEMA}.{table_type}"
    duckdb_con.execute(f"""
        UPDATE {fqn}
        SET trip_distance = trip_distance
        WHERE trip_distance = (SELECT MIN(trip_distance) FROM {fqn})
    """)


@pytest.mark.dml
@pytest.mark.parametrize(
    "table_type",
    [
        pytest.param(TABLE_MANAGED_ICEBERG, marks=XFAIL_BUG_CREDENTIAL_SCOPE),
        pytest.param(TABLE_MANAGED_DELTA, marks=XFAIL_EXPECTED_DELTA_NO_WRITE),
    ],
)
def test_delete(duckdb_con, table_type):
    """DELETE via Iceberg REST. Only managed Iceberg should work."""
    fqn = f"uc.{SCHEMA}.{table_type}"
    before = duckdb_con.execute(f"SELECT count(*) FROM {fqn}").fetchone()[0]
    duckdb_con.execute(f"""
        DELETE FROM {fqn}
        WHERE trip_distance = (SELECT MAX(trip_distance) FROM {fqn})
    """)
    after = duckdb_con.execute(f"SELECT count(*) FROM {fqn}").fetchone()[0]
    assert after < before


# ---------------------------------------------------------------------------
# DDL — Iceberg REST supports CREATE/DROP TABLE
# ---------------------------------------------------------------------------

@pytest.mark.ddl
def test_create_and_drop_table(duckdb_con, workspace_client):
    """CREATE TABLE via Iceberg REST, verify in UC, then DROP.

    Note: UC creates Delta+UniForm tables even via the Iceberg REST endpoint.
    """
    suffix = uuid.uuid4().hex[:8]
    tmp_table = f"duckdb_eval_ddl_{suffix}"
    duckdb_fqn = f"uc.{SCHEMA}.{tmp_table}"
    uc_fqn = f"{CATALOG}.{SCHEMA}.{tmp_table}"

    try:
        duckdb_con.execute(f"""
            CREATE TABLE {duckdb_fqn} (
                id BIGINT, label VARCHAR, value DOUBLE, ts TIMESTAMP
            )
        """)

        from databricks.sdk.service.catalog import DataSourceFormat
        table_info = workspace_client.tables.get(uc_fqn)
        assert table_info.data_source_format in (
            DataSourceFormat.DELTA, DataSourceFormat.ICEBERG,
        ), f"Unexpected format: {table_info.data_source_format}"

        duckdb_con.execute(f"DROP TABLE {duckdb_fqn}")
    finally:
        try:
            duckdb_con.execute(f"DROP TABLE IF EXISTS {duckdb_fqn}")
        except Exception:
            pass
