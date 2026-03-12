"""
Pytest evaluation of DuckDB as a UC REST client against Databricks Unity Catalog.

Tests the UC REST connection (uc_catalog extension, Delta protocol) for each
cell in the experiment matrix:

  UC REST x {Managed Delta, External Delta, Managed Iceberg}
         x {Read, Predicate Pushdown, Write Append, Write Update, Delete Row}

DDL (Create/Drop Table) is not supported by the uc_catalog extension and is
documented but not tested.

Requires:
  - DuckDB >= 1.4.0
  - Databricks SDK auth configured (e.g. ~/.databrickscfg with databricks-cli auth)
  - GRANT EXTERNAL USE SCHEMA on the target schema

Run:
  cd experiments/duckdb-unity-catalog
  uv run pytest tests/test_uc_rest.py -v
"""

from __future__ import annotations

import importlib

import duckdb
import pytest

# Ensure pytz is available -- DuckDB needs it for timestamp handling
importlib.import_module("pytz")

WORKSPACE_URL = "https://fe-vm-fe-randy-pitcher-workspace.cloud.databricks.com"
CATALOG = "fe_randy_pitcher_workspace_catalog"
SCHEMA = "duckdb_uc_experiment"

TABLE_MANAGED_DELTA = "managed_delta"
TABLE_EXTERNAL_DELTA = "external_delta"
TABLE_MANAGED_ICEBERG = "managed_iceberg"

XFAIL_BAD_REQUEST = pytest.mark.xfail(
    reason="Delta+UniForm tables fail with 'Bad Request' on temporary-table-credentials API"
)
XFAIL_ICEBERG_WRITER_COMPAT = pytest.mark.xfail(
    reason="DeltaKernel error: 'Unsupported: Unknown feature icebergWriterCompatV1'"
)
XFAIL_BASE_TABLE_ONLY = pytest.mark.xfail(
    reason="'Can only update/delete from base table'"
)

PARAMS_READ = [
    pytest.param(TABLE_MANAGED_DELTA, marks=XFAIL_BAD_REQUEST),
    pytest.param(TABLE_EXTERNAL_DELTA, marks=XFAIL_BAD_REQUEST),
    pytest.param(TABLE_MANAGED_ICEBERG),
]

PARAMS_INSERT = [
    pytest.param(TABLE_MANAGED_DELTA, marks=XFAIL_BAD_REQUEST),
    pytest.param(TABLE_EXTERNAL_DELTA, marks=XFAIL_BAD_REQUEST),
    pytest.param(TABLE_MANAGED_ICEBERG, marks=XFAIL_ICEBERG_WRITER_COMPAT),
]

PARAMS_UPDATE_DELETE = [
    pytest.param(TABLE_MANAGED_DELTA, marks=XFAIL_BAD_REQUEST),
    pytest.param(TABLE_EXTERNAL_DELTA, marks=XFAIL_BAD_REQUEST),
    pytest.param(TABLE_MANAGED_ICEBERG, marks=XFAIL_BASE_TABLE_ONLY),
]

def _get_databricks_token() -> str:
    """Get a Databricks access token via the SDK's default auth chain."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    headers_or_fn = w.config.authenticate()
    auth_header = headers_or_fn() if callable(headers_or_fn) else headers_or_fn
    token = auth_header.get("Authorization", "").removeprefix("Bearer ")
    if not token:
        raise RuntimeError("Failed to get Databricks token via SDK auth chain")
    return token


@pytest.fixture(scope="session")
def duckdb_con():
    """Create a DuckDB connection with uc_catalog extension and UC attached.

    Uses unnamed secret as workaround for duckdb/unity_catalog#48
    (named secrets are silently ignored).
    """
    token = _get_databricks_token()

    con = duckdb.connect()
    con.execute("INSTALL uc_catalog FROM core; LOAD uc_catalog;")
    con.execute("INSTALL delta; LOAD delta;")

    # IMPORTANT: Must use UNNAMED secret — named secrets are silently ignored
    # due to bug: https://github.com/duckdb/unity_catalog/issues/48
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


@pytest.mark.reads
@pytest.mark.parametrize("table_type", PARAMS_READ)
def test_select(duckdb_con, table_type):
    """SELECT * LIMIT 5 from each table type via UC REST."""
    fqn = f"uc.{SCHEMA}.{table_type}"
    rows = duckdb_con.execute(f"SELECT * FROM {fqn} LIMIT 5").fetchall()
    cols = [desc[0] for desc in duckdb_con.description]
    assert len(rows) > 0, f"Expected rows from {fqn}"
    assert len(cols) > 0, f"Expected columns from {fqn}"


@pytest.mark.reads
@pytest.mark.parametrize("table_type", PARAMS_READ)
def test_predicate_pushdown(duckdb_con, table_type):
    """Filtered SELECT — exercises predicate pushdown via UC REST."""
    fqn = f"uc.{SCHEMA}.{table_type}"
    # Success means no exception was raised — predicate pushdown did not break execution.
    duckdb_con.execute(f"SELECT * FROM {fqn} WHERE trip_distance > 100 LIMIT 5").fetchall()


@pytest.mark.dml
@pytest.mark.parametrize("table_type", PARAMS_INSERT)
def test_insert(duckdb_con, table_type):
    """INSERT INTO ... SELECT exercises the append write path via UC REST."""
    fqn = f"uc.{SCHEMA}.{table_type}"
    before = duckdb_con.execute(f"SELECT count(*) FROM {fqn}").fetchone()[0]
    duckdb_con.execute(f"INSERT INTO {fqn} SELECT * FROM {fqn} LIMIT 1;")
    after = duckdb_con.execute(f"SELECT count(*) FROM {fqn}").fetchone()[0]
    assert after > before, f"Expected row count to increase after INSERT"


@pytest.mark.dml
@pytest.mark.parametrize("table_type", PARAMS_UPDATE_DELETE)
def test_update(duckdb_con, table_type):
    """Identity UPDATE on one row — exercises the update write path via UC REST."""
    fqn = f"uc.{SCHEMA}.{table_type}"
    before = duckdb_con.execute(f"SELECT count(*) FROM {fqn}").fetchone()[0]
    duckdb_con.execute(f"""
        UPDATE {fqn}
        SET trip_distance = trip_distance
        WHERE trip_distance = (SELECT MIN(trip_distance) FROM {fqn})
    """)
    after = duckdb_con.execute(f"SELECT count(*) FROM {fqn}").fetchone()[0]
    assert after == before, f"Expected row count unchanged after identity UPDATE"


@pytest.mark.dml
@pytest.mark.parametrize("table_type", PARAMS_UPDATE_DELETE)
def test_delete(duckdb_con, table_type):
    """DELETE rows — exercises the delete path via UC REST."""
    fqn = f"uc.{SCHEMA}.{table_type}"
    before = duckdb_con.execute(f"SELECT count(*) FROM {fqn}").fetchone()[0]
    duckdb_con.execute(f"""
        DELETE FROM {fqn}
        WHERE trip_distance = (SELECT MAX(trip_distance) FROM {fqn})
    """)
    after = duckdb_con.execute(f"SELECT count(*) FROM {fqn}").fetchone()[0]
    assert after < before, f"Expected row count to decrease after DELETE"
