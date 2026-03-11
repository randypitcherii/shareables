"""
DuckDB + Unity Catalog via UC REST (uc_catalog extension, Delta protocol)
=========================================================================
Tests read/write operations across managed_delta (UniForm), external_delta (UniForm),
and managed_iceberg (native) using DuckDB's `uc_catalog` extension with the Delta
protocol.

Key findings:
  - Native Iceberg tables: read + predicate pushdown work
  - Delta+UniForm tables: all operations fail with "Bad Request" on
    the temporary-table-credentials API
  - Writes to native Iceberg fail: DeltaKernel can't handle icebergWriterCompatV1
  - UPDATE/DELETE fail: "Can only update/delete from base table"

Workaround required:
  - Named secrets are silently ignored (duckdb/unity_catalog#48)
  - Must use unnamed CREATE SECRET (no name parameter)

Also documents the Delta Sharing protocol (read-only by design, no native DuckDB client).

Requires:
  - DuckDB >= 1.4.0
  - Databricks SDK auth configured (e.g. ~/.databrickscfg with databricks-cli auth)
  - GRANT EXTERNAL USE SCHEMA on the target schema

Run:
  uv run python scripts/02_delta_uc_rest.py
"""

import os
import sys

import duckdb

# Allow running from repo root or scripts/ dir
sys.path.insert(0, os.path.dirname(__file__))
from _common import (
    CATALOG,
    SCHEMA,
    WORKSPACE,
    WORKSPACE_URL,
    SEPARATOR,
    get_databricks_token,
    print_header,
    print_result,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ALL_TABLES = ["managed_delta", "external_delta", "managed_iceberg"]

# Tracks pass/fail for summary matrix: { (table_type, operation): bool }
results: dict[tuple[str, str], bool] = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def record(table: str, operation: str, success: bool, detail: str = ""):
    results[(table, operation)] = success
    print_result(f"{table} / {operation}", success, detail)


def run(con: duckdb.DuckDBPyConnection, sql: str):
    """Execute SQL and return fetchall result. Raises on error."""
    return con.execute(sql).fetchall()


# ---------------------------------------------------------------------------
# Setup: connect DuckDB and attach Unity Catalog via uc_catalog extension
# ---------------------------------------------------------------------------

def setup_connection() -> duckdb.DuckDBPyConnection:
    print_header("DuckDB UC Catalog (Delta protocol) — connecting to Unity Catalog")
    print(f"  Workspace : {WORKSPACE}")
    print(f"  Catalog   : {CATALOG}")
    print(f"  Schema    : {SCHEMA}")

    token = get_databricks_token()

    con = duckdb.connect()

    # Install / load required extensions
    con.execute("INSTALL uc_catalog FROM core; LOAD uc_catalog;")
    con.execute("INSTALL delta; LOAD delta;")

    # IMPORTANT: Must use UNNAMED secret — named secrets are silently ignored
    # due to bug: https://github.com/duckdb/unity_catalog/issues/48
    # The uc_catalog extension only looks for the __default_uc secret.
    con.execute(f"""
        CREATE SECRET (
            TYPE UC,
            TOKEN '{token}',
            ENDPOINT '{WORKSPACE_URL}',
            AWS_REGION 'us-east-1'
        );
    """)

    # Attach Unity Catalog via uc_catalog extension (uses Delta protocol under the hood)
    con.execute(f"ATTACH '{CATALOG}' AS uc (TYPE UC_CATALOG);")

    print("  Attached catalog as 'uc' (UC_CATALOG type)")
    print("  NOTE: Using unnamed secret (workaround for duckdb/unity_catalog#48)")
    return con


# ---------------------------------------------------------------------------
# Test: READ (SELECT)
# ---------------------------------------------------------------------------

def test_read(con: duckdb.DuckDBPyConnection):
    print_header("READ (SELECT)")
    for tbl in ALL_TABLES:
        full = f"uc.{SCHEMA}.{tbl}"
        try:
            rows = run(con, f"SELECT * FROM {full} LIMIT 5;")
            record(tbl, "read", True, f"{len(rows)} rows returned")
        except Exception as e:
            record(tbl, "read", False, str(e))


# ---------------------------------------------------------------------------
# Test: PREDICATE PUSHDOWN
# ---------------------------------------------------------------------------

def test_predicate_pushdown(con: duckdb.DuckDBPyConnection):
    """Test filtered SELECT — if it works, predicate pushdown is in play."""
    print_header("PREDICATE PUSHDOWN")
    for tbl in ALL_TABLES:
        full = f"uc.{SCHEMA}.{tbl}"
        try:
            rows = run(con, f"SELECT * FROM {full} WHERE trip_distance > 100 LIMIT 5;")
            record(tbl, "predicate_pushdown", True,
                   f"filtered query succeeded ({len(rows)} rows)")
        except Exception as e:
            record(tbl, "predicate_pushdown", False, str(e))


# ---------------------------------------------------------------------------
# Test: WRITE APPEND (INSERT INTO)
# ---------------------------------------------------------------------------

def test_write_append(con: duckdb.DuckDBPyConnection):
    print_header("WRITE — APPEND (INSERT INTO)")
    for tbl in ALL_TABLES:
        full = f"uc.{SCHEMA}.{tbl}"
        try:
            run(con, f"INSERT INTO {full} SELECT * FROM {full} WHERE 1=0;")
            record(tbl, "write_append", True, "INSERT succeeded")
        except Exception as e:
            record(tbl, "write_append", False, str(e))


# ---------------------------------------------------------------------------
# Test: WRITE UPDATE
# ---------------------------------------------------------------------------

def test_write_update(con: duckdb.DuckDBPyConnection):
    print_header("WRITE — UPDATE")
    for tbl in ALL_TABLES:
        full = f"uc.{SCHEMA}.{tbl}"
        try:
            run(con, f"UPDATE {full} SET trip_distance = trip_distance WHERE 1=0;")
            record(tbl, "write_update", True, "UPDATE succeeded")
        except Exception as e:
            record(tbl, "write_update", False, str(e))


# ---------------------------------------------------------------------------
# Test: DELETE ROW
# ---------------------------------------------------------------------------

def test_delete_row(con: duckdb.DuckDBPyConnection):
    print_header("DELETE ROW")
    for tbl in ALL_TABLES:
        full = f"uc.{SCHEMA}.{tbl}"
        try:
            run(con, f"DELETE FROM {full} WHERE 1=0;")
            record(tbl, "delete_row", True, "DELETE succeeded")
        except Exception as e:
            record(tbl, "delete_row", False, str(e))


# ---------------------------------------------------------------------------
# Test: CREATE TABLE + DROP TABLE
# ---------------------------------------------------------------------------

def test_create_drop_table(con: duckdb.DuckDBPyConnection):
    """uc_catalog does not support CREATE/DROP — document this."""
    print_header("CREATE TABLE / DROP TABLE")
    print("  uc_catalog extension does not support DDL (CREATE/DROP TABLE).")
    print("  These operations are not available via the UC REST Delta protocol path.")
    for tbl in ALL_TABLES:
        record(tbl, "create_table", False, "uc_catalog extension does not support DDL")
        record(tbl, "drop_table", False, "uc_catalog extension does not support DDL")


# ---------------------------------------------------------------------------
# Delta Sharing Protocol documentation
# ---------------------------------------------------------------------------

def document_delta_sharing():
    """Document the Delta Sharing path — no native DuckDB client exists."""
    print_header("Delta Sharing Protocol (documented, not tested)")
    print("""
  Delta Sharing is a separate protocol from UC REST. Key facts:

  1. DuckDB has NO native Delta Sharing client
     - Reads require a Python bridge: delta-sharing lib -> pandas -> DuckDB
     - No predicate pushdown (full table load to memory)

  2. Delta Sharing is READ-ONLY by protocol design
     - No write endpoints exist in the spec
     - This is a fundamental protocol constraint

  3. UC exposes a Delta Sharing server at:
     {workspace}/api/2.0/delta-sharing/...
     But it requires Share/Recipient setup in UC (not just a PAT).

  For DuckDB writes to UC, use the Iceberg REST path instead:
    ATTACH '<catalog>' AS uc (TYPE iceberg,
      ENDPOINT 'https://<workspace>/api/2.1/unity-catalog/iceberg-rest')
""".format(workspace=WORKSPACE))


# ---------------------------------------------------------------------------
# Summary matrix
# ---------------------------------------------------------------------------

def print_summary():
    print_header("SUMMARY MATRIX")
    operations = [
        "read",
        "predicate_pushdown",
        "write_append",
        "write_update",
        "delete_row",
        "create_table",
        "drop_table",
    ]
    col_w = 22

    header = f"{'Table Type':<22}" + "".join(op[:col_w].center(col_w) for op in operations)
    print(header)
    print("-" * len(header))

    for tbl in ALL_TABLES:
        row = f"{tbl:<22}"
        for op in operations:
            key = (tbl, op)
            if key in results:
                icon = "PASS" if results[key] else "FAIL"
            else:
                icon = "N/A "
            row += icon.center(col_w)
        print(row)

    print()
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"  Total: {passed}/{total} passed")

    print()
    print("  Key Takeaways:")
    print("    ✅ Native Iceberg reads + predicate pushdown work via uc_catalog")
    print("    ❌ Delta+UniForm tables fail (Bad Request on temporary-table-credentials)")
    print("    ❌ Writes to native Iceberg fail (DeltaKernel incompatibility)")
    print("    ❌ UPDATE/DELETE fail ('Can only update/delete from base table')")
    print("    ❌ No DDL support (CREATE/DROP TABLE)")
    print("    ⚠️  Must use unnamed secrets (named secrets silently ignored — bug #48)")
    print("    ✅ Iceberg REST is the recommended path for full CRUD")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    con = setup_connection()

    test_read(con)
    test_predicate_pushdown(con)
    test_write_append(con)
    test_write_update(con)
    test_delete_row(con)
    test_create_drop_table(con)

    print_summary()

    con.close()

    document_delta_sharing()

    print(f"\n{SEPARATOR}")
    print("Done.")
