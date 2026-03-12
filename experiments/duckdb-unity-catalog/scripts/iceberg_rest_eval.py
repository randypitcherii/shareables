"""
DuckDB Iceberg REST Catalog Evaluation
=======================================
Isolated evaluation of DuckDB as an Iceberg REST client against Databricks
Unity Catalog. Tests the 6 cells from the experiment matrix:

  Iceberg REST x {Managed Iceberg, Managed Delta} x {Reads, DML, DDL}

Design:
  - Self-contained: no shared imports, single file
  - Auth via Databricks SDK (respects ~/.databrickscfg)
  - Real operations only — no WHERE 1=0 or vacuous writes
  - INSERT uses VALUES to isolate write path from read path
  - Results map directly to the README matrix

Prerequisites:
  - Tables `managed_delta` and `managed_iceberg` in target schema with data
  - managed_delta must have UniForm (Iceberg reads) enabled
  - GRANT EXTERNAL USE SCHEMA on target schema
  - Databricks CLI auth configured (~/.databrickscfg)

Run:
  cd experiments/duckdb-unity-catalog
  uv run python scripts/iceberg_rest_eval.py

Known issues:
  - Native Iceberg reads may 403 due to DuckDB credential scope bug
    (metadata_path scope doesn't cover data file paths)
  - DDL via Iceberg REST creates Iceberg tables only — Delta DDL expected to fail
  - Delta tables are read-only via Iceberg REST (DML expected to fail)
"""

from __future__ import annotations

import importlib
import os
import sys
import traceback
from dataclasses import dataclass

import duckdb

# Ensure pytz is available — DuckDB needs it for timestamp handling
try:
    importlib.import_module("pytz")
except ImportError:
    print("ERROR: pytz is required. Install with: uv pip install pytz")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WORKSPACE = os.environ["DATABRICKS_HOST"].removeprefix("https://")
CATALOG = os.environ["DATABRICKS_CATALOG"]
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "duckdb_uc_experiment")
ENDPOINT = f"https://{WORKSPACE}/api/2.1/unity-catalog/iceberg-rest"

SEP = "=" * 60

# Table types to test
TABLE_TYPES = ["managed_iceberg", "managed_delta"]

# Temp table for DDL tests
DDL_TEST_TABLE = "duckdb_iceberg_eval_ddl_tmp"


# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------

@dataclass
class TestResult:
    table_type: str
    operation: str
    category: str  # Reads, DML, DDL
    success: bool
    detail: str = ""

    @property
    def icon(self) -> str:
        return "PASS" if self.success else "FAIL"


results: list[TestResult] = []


def record(table_type: str, operation: str, category: str, success: bool, detail: str = ""):
    """Record and print a test result."""
    icon = "PASS" if success else "FAIL"
    print(f"  [{icon}] {table_type} / {operation} -- {detail}")
    results.append(TestResult(table_type, operation, category, success, detail))


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def get_databricks_token() -> str:
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
    print(f"  Auth: Databricks SDK ({w.config.auth_type})")
    return token


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def connect() -> duckdb.DuckDBPyConnection:
    """Connect DuckDB and attach UC via Iceberg REST endpoint."""
    print(f"\n{SEP}")
    print("DuckDB Iceberg REST Catalog Evaluation")
    print(SEP)
    print(f"  Workspace: {WORKSPACE}")
    print(f"  Catalog:   {CATALOG}")
    print(f"  Schema:    {SCHEMA}")
    print(f"  Endpoint:  {ENDPOINT}")

    token = get_databricks_token()

    con = duckdb.connect()

    # Install and load extensions
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("UPDATE EXTENSIONS;")

    # DuckDB version info
    version = con.execute("SELECT version()").fetchone()[0]
    print(f"  DuckDB:    {version}")

    # Create secret with SDK-obtained token
    con.execute(f"""
        CREATE SECRET uc_secret (
            TYPE iceberg,
            TOKEN '{token}'
        );
    """)

    # Attach catalog -- vended_credentials is the default but explicit for clarity
    con.execute(f"""
        ATTACH '{CATALOG}' AS uc (
            TYPE iceberg,
            SECRET uc_secret,
            ENDPOINT '{ENDPOINT}',
            ACCESS_DELEGATION_MODE 'vended_credentials'
        );
    """)

    print("  Catalog attached as 'uc'")
    return con


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def test_reads(con: duckdb.DuckDBPyConnection):
    """Test SELECT against each table type."""
    print(f"\n{SEP}")
    print("READS (SELECT)")
    print(SEP)

    for tbl in TABLE_TYPES:
        fqn = f"uc.{SCHEMA}.{tbl}"
        try:
            rows = con.execute(f"SELECT * FROM {fqn} LIMIT 5").fetchall()
            cols = [desc[0] for desc in con.description]
            record(tbl, "SELECT", "Reads", True, f"{len(rows)} rows, {len(cols)} cols")
        except Exception as e:
            record(tbl, "SELECT", "Reads", False, _short_error(e))


# ---------------------------------------------------------------------------
# DML (INSERT, UPDATE, DELETE)
# ---------------------------------------------------------------------------

def test_dml(con: duckdb.DuckDBPyConnection):
    """Test DML operations against each table type.

    INSERT uses VALUES to isolate the write path from read capability.
    UPDATE/DELETE inherently require reading existing data.
    """
    print(f"\n{SEP}")
    print("DML (INSERT, UPDATE, DELETE)")
    print(SEP)

    for tbl in TABLE_TYPES:
        fqn = f"uc.{SCHEMA}.{tbl}"

        # --- Discover schema for VALUES-based INSERT ---
        insert_values_sql = _build_insert_values(con, tbl, fqn)

        # --- INSERT ---
        try:
            if insert_values_sql:
                con.execute(insert_values_sql)
                record(tbl, "INSERT", "DML", True, "INSERT INTO ... VALUES succeeded")
            else:
                # Fallback: INSERT ... SELECT (requires reads to work)
                con.execute(f"INSERT INTO {fqn} SELECT * FROM {fqn} LIMIT 1;")
                record(tbl, "INSERT", "DML", True, "INSERT ... SELECT fallback succeeded")
        except Exception as e:
            record(tbl, "INSERT", "DML", False, _short_error(e))

        # --- UPDATE ---
        try:
            # Identity update on one row -- still exercises the full write path
            con.execute(f"""
                UPDATE {fqn}
                SET trip_distance = trip_distance
                WHERE trip_distance = (SELECT MIN(trip_distance) FROM {fqn})
            """)
            record(tbl, "UPDATE", "DML", True, "identity update succeeded")
        except Exception as e:
            record(tbl, "UPDATE", "DML", False, _short_error(e))

        # --- DELETE ---
        try:
            before = con.execute(f"SELECT count(*) FROM {fqn}").fetchone()[0]
            con.execute(f"""
                DELETE FROM {fqn}
                WHERE trip_distance = (SELECT MAX(trip_distance) FROM {fqn})
            """)
            after = con.execute(f"SELECT count(*) FROM {fqn}").fetchone()[0]
            record(tbl, "DELETE", "DML", True, f"rows {before} -> {after}")
        except Exception as e:
            record(tbl, "DELETE", "DML", False, _short_error(e))


def _build_insert_values(
    con: duckdb.DuckDBPyConnection, tbl: str, fqn: str
) -> str | None:
    """Try to build an INSERT ... VALUES statement by inspecting the table schema.

    Returns the SQL string or None if schema discovery fails.
    """
    try:
        # DESCRIBE returns column info from the Iceberg metadata (no S3 read needed)
        cols_info = con.execute(f"DESCRIBE {fqn}").fetchall()
    except Exception:
        return None

    col_names = []
    col_values = []
    for row in cols_info:
        name = row[0]
        dtype = row[1].upper()
        col_names.append(name)
        col_values.append(_default_value_for_type(dtype))

    if not col_names:
        return None

    names_str = ", ".join(col_names)
    values_str = ", ".join(col_values)
    return f"INSERT INTO {fqn} ({names_str}) VALUES ({values_str})"


def _default_value_for_type(dtype: str) -> str:
    """Return a safe default literal for a SQL type."""
    dtype = dtype.strip()
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
    # VARCHAR, STRING, or anything else
    return "'duckdb_eval_test'"


# ---------------------------------------------------------------------------
# DDL (CREATE TABLE, DROP TABLE)
# ---------------------------------------------------------------------------

def test_ddl(con: duckdb.DuckDBPyConnection):
    """Test CREATE TABLE and DROP TABLE.

    The Iceberg REST endpoint only creates Iceberg tables.
    Creating Delta tables via this path is expected to fail.
    """
    print(f"\n{SEP}")
    print("DDL (CREATE TABLE, DROP TABLE)")
    print(SEP)

    for tbl_type in TABLE_TYPES:
        test_fqn = f"uc.{SCHEMA}.{DDL_TEST_TABLE}_{tbl_type}"

        # --- Cleanup from any prior failed run ---
        try:
            con.execute(f"DROP TABLE IF EXISTS {test_fqn}")
        except Exception:
            pass

        # --- CREATE TABLE ---
        created = False
        try:
            con.execute(f"""
                CREATE TABLE {test_fqn} (
                    id      BIGINT,
                    label   VARCHAR,
                    value   DOUBLE,
                    ts      TIMESTAMP
                )
            """)
            record(tbl_type, "CREATE TABLE", "DDL", True, "table created")
            created = True
        except Exception as e:
            record(tbl_type, "CREATE TABLE", "DDL", False, _short_error(e))

        # --- DROP TABLE ---
        if created:
            try:
                con.execute(f"DROP TABLE {test_fqn}")
                record(tbl_type, "DROP TABLE", "DDL", True, "table dropped")
            except Exception as e:
                record(tbl_type, "DROP TABLE", "DDL", False, _short_error(e))
                # Best-effort cleanup
                try:
                    con.execute(f"DROP TABLE IF EXISTS {test_fqn}")
                except Exception:
                    pass
        else:
            record(tbl_type, "DROP TABLE", "DDL", False, "skipped -- CREATE failed")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def _categorize_error(detail: str, table_type: str = "", category: str = "") -> str:
    """Classify an error into a short root cause label."""
    d = detail.lower()
    if "http get error" in d or "unable to connect to url s3://" in d:
        if table_type == "managed_delta" and category == "DML":
            return "expected: Delta tables are read-only via Iceberg REST"
        return "S3 credential scope bug (vended creds don't cover data paths)"
    if "pytz" in d:
        return "missing pytz dependency"
    return detail[:80]


def print_summary():
    """Print results in the README matrix format."""
    print(f"\n{SEP}")
    print("RESULTS MATRIX")
    print(SEP)
    print()
    print("| Catalog Interface | Table Type | Access Type | Support | Notes |")
    print("|---|---|---|:---:|---|")

    for tbl_type in TABLE_TYPES:
        nice_type = "Managed Iceberg" if tbl_type == "managed_iceberg" else "Managed Delta"

        for category in ["Reads", "DML", "DDL"]:
            cat_results = [r for r in results if r.table_type == tbl_type and r.category == category]

            if not cat_results:
                print(f"| Iceberg REST | {nice_type} | {category} | -- | not tested |")
                continue

            all_pass = all(r.success for r in cat_results)
            all_fail = all(not r.success for r in cat_results)

            if all_pass:
                icon = "PASS"
            elif all_fail:
                icon = "FAIL"
            else:
                icon = "PARTIAL"

            # Build concise notes
            if all_pass:
                ops = ", ".join(r.operation for r in cat_results)
                notes = f"{ops} all succeeded"
            else:
                # Group by pass/fail
                passed = [r for r in cat_results if r.success]
                failed = [r for r in cat_results if not r.success]
                parts = []
                if passed:
                    parts.append(f"PASS: {', '.join(r.operation for r in passed)}")
                if failed:
                    # Use categorized error for conciseness
                    cause = _categorize_error(failed[0].detail, tbl_type, category)
                    ops = ", ".join(r.operation for r in failed)
                    parts.append(f"FAIL: {ops} -- {cause}")
                notes = "; ".join(parts)

            print(f"| Iceberg REST | {nice_type} | {category} | {icon} | {notes} |")

    # Individual operation details
    print()
    print("### Individual Operations")
    print()
    for r in results:
        icon = "PASS" if r.success else "FAIL"
        print(f"  [{icon}] {r.table_type} / {r.operation} ({r.category}) -- {r.detail}")

    print()
    passed = sum(1 for r in results if r.success)
    total = len(results)
    failed = total - passed
    print(f"  {passed}/{total} operations passed, {failed} failed")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _short_error(e: Exception, max_len: int = 200) -> str:
    """Return a truncated error string."""
    msg = str(e)
    if len(msg) > max_len:
        return msg[:max_len] + "..."
    return msg


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        con = connect()
    except Exception as e:
        print(f"\n  FATAL: Failed to connect -- {e}")
        traceback.print_exc()
        sys.exit(1)

    test_reads(con)
    test_dml(con)
    test_ddl(con)
    print_summary()

    con.close()
    print(f"\n{SEP}")
    print("Done.")
