"""
Pytest evaluation of DuckDB as an Iceberg REST client against Databricks Unity Catalog.

Tests the 6 cells from the experiment matrix:
  Iceberg REST x {Managed Iceberg, Managed Delta} x {Reads, DML, DDL}

Run:
  cd experiments/duckdb-unity-catalog
  uv run pytest tests/test_iceberg_rest.py -v
"""

from __future__ import annotations

import importlib
import uuid

import duckdb
import pytest

# Ensure pytz is available -- DuckDB needs it for timestamp handling
importlib.import_module("pytz")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WORKSPACE = "fe-vm-fe-randy-pitcher-workspace.cloud.databricks.com"
CATALOG = "fe_randy_pitcher_workspace_catalog"
SCHEMA = "duckdb_uc_experiment"
ENDPOINT = f"https://{WORKSPACE}/api/2.1/unity-catalog/iceberg-rest"

# Pre-existing test tables
TABLE_TYPES = ["managed_iceberg", "managed_delta"]

# ---------------------------------------------------------------------------
# Custom markers
# ---------------------------------------------------------------------------

pytest.mark.reads = pytest.mark.reads
pytest.mark.dml = pytest.mark.dml
pytest.mark.ddl = pytest.mark.ddl


# ---------------------------------------------------------------------------
# Auth helper
# ---------------------------------------------------------------------------

def _get_databricks_token() -> str:
    """Get a Databricks access token via the SDK's default auth chain."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    headers_or_fn = w.config.authenticate()
    if callable(headers_or_fn):
        auth_header = headers_or_fn()
    else:
        auth_header = headers_or_fn
    token = auth_header.get("Authorization", "").removeprefix("Bearer ")
    if not token:
        raise RuntimeError("Failed to get Databricks token via SDK auth chain")
    return token


# ---------------------------------------------------------------------------
# Type helpers for INSERT VALUES
# ---------------------------------------------------------------------------

def _default_value_for_type(dtype: str) -> str:
    """Return a safe default literal for a SQL type."""
    dtype = dtype.strip().upper()
    if "INT" in dtype or "LONG" in dtype:
        return "0"
    if "FLOAT" in dtype or "DOUBLE" in dtype or "DECIMAL" in dtype or "NUMERIC" in dtype:
        return "0.0"
    if "BOOL" in dtype:
        return "false"
    if "DATE" in dtype and "TIME" not in dtype:
        return "'2026-01-01'"
    if "TIMESTAMP" in dtype:
        return "'2026-01-01 00:00:00'"
    if "TIME" in dtype:
        return "'00:00:00'"
    return "'duckdb_eval_test'"


def _build_insert_values(con: duckdb.DuckDBPyConnection, fqn: str) -> str:
    """Build an INSERT ... VALUES statement by inspecting the table schema via DESCRIBE."""
    cols_info = con.execute(f"DESCRIBE {fqn}").fetchall()
    col_names = []
    col_values = []
    for row in cols_info:
        col_names.append(row[0])
        col_values.append(_default_value_for_type(row[1]))
    assert col_names, f"DESCRIBE returned no columns for {fqn}"
    names_str = ", ".join(col_names)
    values_str = ", ".join(col_values)
    return f"INSERT INTO {fqn} ({names_str}) VALUES ({values_str})"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def duckdb_con():
    """Create a DuckDB connection with Iceberg extensions and UC catalog attached."""
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
    """Databricks WorkspaceClient for DDL verification."""
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


# ---------------------------------------------------------------------------
# xfail conditions by (table_type, category)
# ---------------------------------------------------------------------------

_XFAIL = {
    ("managed_iceberg", "reads"): pytest.mark.xfail(
        reason="Credential scope bug: vended creds metadata_path scope doesn't cover data file paths"
    ),
    ("managed_iceberg", "dml"): pytest.mark.xfail(
        reason="Credential scope bug: vended creds metadata_path scope doesn't cover data file paths"
    ),
    ("managed_delta", "dml"): pytest.mark.xfail(
        reason="Delta tables are read-only via Iceberg REST"
    ),
}


def _apply_xfail(table_type: str, category: str):
    """Return the xfail marker if this combo is expected to fail, else empty list."""
    key = (table_type, category)
    if key in _XFAIL:
        return [_XFAIL[key]]
    return []


# ---------------------------------------------------------------------------
# READS
# ---------------------------------------------------------------------------

@pytest.mark.reads
@pytest.mark.parametrize(
    "table_type",
    [
        pytest.param("managed_iceberg", marks=pytest.mark.xfail(
            reason="Credential scope bug: vended creds metadata_path scope doesn't cover data file paths"
        )),
        pytest.param("managed_delta"),
    ],
)
def test_select(duckdb_con, table_type):
    """SELECT * LIMIT 5 from each table type."""
    fqn = f"uc.{SCHEMA}.{table_type}"
    rows = duckdb_con.execute(f"SELECT * FROM {fqn} LIMIT 5").fetchall()
    cols = [desc[0] for desc in duckdb_con.description]
    assert len(rows) > 0, f"Expected rows from {fqn}"
    assert len(cols) > 0, f"Expected columns from {fqn}"


# ---------------------------------------------------------------------------
# DML
# ---------------------------------------------------------------------------

@pytest.mark.dml
@pytest.mark.parametrize(
    "table_type",
    [
        pytest.param("managed_iceberg", marks=pytest.mark.xfail(
            reason="Credential scope bug: vended creds metadata_path scope doesn't cover data file paths"
        )),
        pytest.param("managed_delta", marks=pytest.mark.xfail(
            reason="Delta tables are read-only via Iceberg REST"
        )),
    ],
)
def test_insert(duckdb_con, table_type):
    """INSERT INTO ... VALUES using schema discovered via DESCRIBE."""
    fqn = f"uc.{SCHEMA}.{table_type}"
    insert_sql = _build_insert_values(duckdb_con, fqn)
    duckdb_con.execute(insert_sql)


@pytest.mark.dml
@pytest.mark.parametrize(
    "table_type",
    [
        pytest.param("managed_iceberg", marks=pytest.mark.xfail(
            reason="Credential scope bug: vended creds metadata_path scope doesn't cover data file paths"
        )),
        pytest.param("managed_delta", marks=pytest.mark.xfail(
            reason="Delta tables are read-only via Iceberg REST"
        )),
    ],
)
def test_update(duckdb_con, table_type):
    """Identity UPDATE on one row -- exercises the full write path."""
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
        pytest.param("managed_iceberg", marks=pytest.mark.xfail(
            reason="Credential scope bug: vended creds metadata_path scope doesn't cover data file paths"
        )),
        pytest.param("managed_delta", marks=pytest.mark.xfail(
            reason="Delta tables are read-only via Iceberg REST"
        )),
    ],
)
def test_delete(duckdb_con, table_type):
    """DELETE rows with max trip_distance."""
    fqn = f"uc.{SCHEMA}.{table_type}"
    before = duckdb_con.execute(f"SELECT count(*) FROM {fqn}").fetchone()[0]
    duckdb_con.execute(f"""
        DELETE FROM {fqn}
        WHERE trip_distance = (SELECT MAX(trip_distance) FROM {fqn})
    """)
    after = duckdb_con.execute(f"SELECT count(*) FROM {fqn}").fetchone()[0]
    assert after <= before, f"Expected row count to decrease or stay same after DELETE"


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

@pytest.mark.ddl
@pytest.mark.parametrize("table_type", TABLE_TYPES)
def test_create_and_drop_table(duckdb_con, workspace_client, table_type):
    """CREATE TABLE via Iceberg REST, verify in UC via SDK, then DROP."""
    suffix = uuid.uuid4().hex[:8]
    tmp_table = f"duckdb_eval_ddl_{table_type}_{suffix}"
    duckdb_fqn = f"uc.{SCHEMA}.{tmp_table}"
    uc_fqn = f"{CATALOG}.{SCHEMA}.{tmp_table}"

    try:
        # CREATE TABLE
        duckdb_con.execute(f"""
            CREATE TABLE {duckdb_fqn} (
                id      BIGINT,
                label   VARCHAR,
                value   DOUBLE,
                ts      TIMESTAMP
            )
        """)

        # Verify via Databricks SDK -- the table should exist in UC
        from databricks.sdk.service.catalog import DataSourceFormat

        table_info = workspace_client.tables.get(uc_fqn)
        assert table_info.table_type is not None, "table_type should not be None"
        assert table_info.data_source_format is not None, "data_source_format should not be None"

        # Record what UC actually created -- Unity Catalog creates Delta tables
        # (with UniForm/Iceberg compatibility) even via the Iceberg REST endpoint
        assert table_info.data_source_format in (
            DataSourceFormat.DELTA,
            DataSourceFormat.ICEBERG,
        ), f"Unexpected format: {table_info.data_source_format}"

        # DROP TABLE
        duckdb_con.execute(f"DROP TABLE {duckdb_fqn}")

    finally:
        # Best-effort cleanup
        try:
            duckdb_con.execute(f"DROP TABLE IF EXISTS {duckdb_fqn}")
        except Exception:
            pass
