"""
DuckDB + Unity Catalog: Delta / UC REST Path Experiment
=======================================================

Tests whether DuckDB can interact with UC Delta tables via:
  A) The Delta Sharing REST protocol (UC exposes a Delta Sharing server)
  B) The DuckDB `delta` extension (direct cloud storage access — bypasses UC)

Key finding from research: DuckDB has NO native Delta Sharing REST client.
  - `delta_scan` reads Delta tables directly from cloud storage (bypasses UC)
  - Delta Sharing protocol requires a Python bridge (delta-sharing lib → pandas → DuckDB)
  - Delta Sharing is READ-ONLY by protocol design (no writes possible)

This script documents both approaches and captures actual error messages.

Run: uv run python scripts/02_delta_uc_rest.py
Requires: DATABRICKS_TOKEN env var
"""

import os
import sys
import json
import duckdb

# Add scripts/ dir to path so _common imports work from any cwd
sys.path.insert(0, os.path.dirname(__file__))
from _common import (
    CATALOG,
    SCHEMA,
    FULL_SCHEMA,
    SEPARATOR,
    print_header,
    print_result,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WORKSPACE_URL = "https://fe-vm-fe-randy-pitcher-workspace.cloud.databricks.com"
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

# Delta tables to test (managed and external)
DELTA_TABLES = ["managed_delta", "external_delta"]

# Iceberg table via UC REST (not Iceberg REST) — testing Delta path
ICEBERG_TABLE = "managed_iceberg"

ALL_TABLES = DELTA_TABLES + [ICEBERG_TABLE]

# Operations from the README capabilities grid
OPERATIONS = [
    "Read (SELECT)",
    "Predicate Pushdown",
    "Write (Append)",
    "Write (Update)",
    "Delete Row",
    "Create Table",
    "Drop Table",
]

# Accumulate results: {table_name: {operation: (success, detail)}}
results: dict[str, dict[str, tuple[bool, str]]] = {t: {} for t in ALL_TABLES}


# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------

def check_prerequisites() -> bool:
    """Verify required environment variables and imports are available."""
    print_header("Pre-flight Checks")

    ok = True

    if not DATABRICKS_TOKEN:
        print_result(
            "DATABRICKS_TOKEN env var",
            False,
            "not set — set this to your Databricks PAT before running",
        )
        ok = False
    else:
        print_result("DATABRICKS_TOKEN env var", True, "set")

    # Check DuckDB version
    try:
        ver = duckdb.__version__
        print_result("DuckDB import", True, f"version {ver}")
    except Exception as e:
        print_result("DuckDB import", False, str(e))
        ok = False

    # Check delta extension availability (install if needed)
    try:
        conn = duckdb.connect()
        conn.execute("INSTALL delta; LOAD delta;")
        conn.close()
        print_result("DuckDB delta extension", True, "installed and loaded")
    except Exception as e:
        print_result("DuckDB delta extension", False, str(e))
        ok = False

    # Check requests library (needed for Delta Sharing REST calls)
    try:
        import requests  # noqa: F401
        print_result("requests library", True, "available")
    except ImportError:
        print_result(
            "requests library",
            False,
            "not installed — needed for Delta Sharing REST probe",
        )
        # Non-fatal; we'll skip that section

    return ok


# ---------------------------------------------------------------------------
# Approach A: Delta Sharing REST (UC REST path)
# ---------------------------------------------------------------------------
# UC exposes a Delta Sharing server. We probe it directly via HTTP to
# understand what's reachable, then explain why DuckDB can't use it natively.

def probe_delta_sharing_rest() -> dict[str, object]:
    """
    Probe the UC Delta Sharing REST endpoint to see what's accessible.
    Returns a dict with findings.
    """
    try:
        import requests
    except ImportError:
        return {"error": "requests library not available"}

    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

    # Step 1: List shares
    list_shares_url = f"{WORKSPACE_URL}/api/2.0/delta-sharing/shares"
    try:
        resp = requests.get(list_shares_url, headers=headers, timeout=10)
        if resp.status_code == 200:
            shares_data = resp.json()
            shares = shares_data.get("shares", [])
            return {"status": resp.status_code, "shares": shares, "raw": shares_data}
        else:
            return {
                "status": resp.status_code,
                "error": resp.text[:500],
                "note": "Delta Sharing endpoint returned non-200",
            }
    except Exception as e:
        return {"error": str(e)}


def run_delta_sharing_rest_section():
    """Document and probe the Delta Sharing REST path."""
    print_header("Approach A: Delta Sharing REST Protocol (UC REST Path)")

    print("""
  What this path is:
    UC exposes a Delta Sharing REST server at:
      {workspace}/api/2.0/delta-sharing/...

    Clients authenticate with a bearer token (PAT).
    The server returns presigned cloud storage URLs for Parquet files.

  Why DuckDB can't use this natively:
    - DuckDB has NO built-in Delta Sharing protocol client
    - The `delta` extension reads storage directly, NOT via this protocol
    - To use this path from DuckDB, you need a Python bridge:
        delta-sharing library → pandas DataFrame → DuckDB in-memory query

  Protocol is READ-ONLY by design:
    - No write endpoints exist in the Delta Sharing spec
    - This is a fundamental protocol constraint, not a DuckDB limitation
""")

    if not DATABRICKS_TOKEN:
        print("  Skipping REST probe — DATABRICKS_TOKEN not set.")
        return

    print("  Probing Delta Sharing REST endpoint...")
    findings = probe_delta_sharing_rest()

    status = findings.get("status")
    if "error" in findings and "status" not in findings:
        print_result(
            "Delta Sharing REST probe",
            False,
            findings["error"],
        )
    elif status == 200:
        shares = findings.get("shares", [])
        print_result(
            "Delta Sharing REST /shares endpoint",
            True,
            f"reachable — {len(shares)} share(s) returned: {[s.get('name') for s in shares]}",
        )
        if not shares:
            print("""
  Note: No shares returned. To use Delta Sharing, a UC admin must:
    1. Create a Share object in UC covering the target tables
    2. Create a Recipient in UC
    3. Grant the Recipient access to the Share
    4. Download the credential file for the Recipient
  A PAT alone does not expose tables — the Share/Recipient layer is required.
""")
    elif status == 403:
        print_result(
            "Delta Sharing REST /shares endpoint",
            False,
            f"HTTP 403 Forbidden — PAT lacks permission or Delta Sharing not enabled: {findings.get('error', '')[:200]}",
        )
    elif status == 404:
        print_result(
            "Delta Sharing REST /shares endpoint",
            False,
            "HTTP 404 — Delta Sharing endpoint not found at this workspace URL",
        )
    else:
        print_result(
            "Delta Sharing REST /shares endpoint",
            False,
            f"HTTP {status} — {findings.get('error', '')[:200]}",
        )

    # Document the Python bridge approach
    print("""
  Python Bridge Approach (out of scope for pure DuckDB testing):
    To query UC Delta tables via Delta Sharing from DuckDB, you would:

      import delta_sharing, duckdb
      profile = "/path/to/credential.share"   # downloaded from UC UI
      table_url = f"{profile}#<share>.<schema>.<table>"
      df = delta_sharing.load_as_pandas(table_url)  # reads all data to memory
      conn = duckdb.connect()
      conn.register("shared_table", df)
      result = conn.execute("SELECT * FROM shared_table WHERE id = 1").df()

    Limitations:
      - Requires a credential .share file (PAT-based or OIDC)
      - Full table data loads into memory before DuckDB can query it
      - No predicate pushdown (hints are best-effort server-side only)
      - Read-only — no writes possible
      - Requires: pip install delta-sharing
""")


# ---------------------------------------------------------------------------
# Approach B: DuckDB `delta` extension (direct cloud storage — bypasses UC)
# ---------------------------------------------------------------------------

def get_table_storage_path(table_name: str) -> str | None:
    """
    Attempt to retrieve the storage path for a UC table via UC REST API.
    This is what you'd need to pass to delta_scan().
    """
    if not DATABRICKS_TOKEN:
        return None

    try:
        import requests
    except ImportError:
        return None

    url = (
        f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables"
        f"/{CATALOG}.{SCHEMA}.{table_name}"
    )
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("storage_location")
        return None
    except Exception:
        return None


def try_delta_scan_read(conn: duckdb.DuckDBPyConnection, storage_path: str, table_name: str) -> tuple[bool, str]:
    """Attempt to read a Delta table via delta_scan()."""
    try:
        result = conn.execute(
            f"SELECT COUNT(*) as row_count FROM delta_scan('{storage_path}')"
        ).fetchone()
        count = result[0] if result else 0
        return True, f"read {count} rows"
    except Exception as e:
        err = str(e)
        # Classify the error type for clarity
        if "credential" in err.lower() or "permission" in err.lower() or "access" in err.lower():
            return False, f"storage credential error (expected without cloud creds): {err[:200]}"
        elif "not found" in err.lower() or "no such" in err.lower():
            return False, f"table not found at storage path: {err[:200]}"
        else:
            return False, f"{err[:200]}"


def try_delta_scan_predicate(conn: duckdb.DuckDBPyConnection, storage_path: str) -> tuple[bool, str]:
    """Attempt predicate pushdown via delta_scan()."""
    try:
        result = conn.execute(
            f"SELECT COUNT(*) FROM delta_scan('{storage_path}') WHERE id = 1"
        ).fetchone()
        return True, "predicate pushdown executed"
    except Exception as e:
        err = str(e)
        if "credential" in err.lower() or "access" in err.lower() or "permission" in err.lower():
            return False, f"storage credential error (blocks predicate test): {err[:200]}"
        return False, f"{err[:200]}"


def try_delta_scan_append(conn: duckdb.DuckDBPyConnection, storage_path: str) -> tuple[bool, str]:
    """Attempt blind append (insert) via delta_scan()."""
    try:
        conn.execute(
            f"INSERT INTO delta_scan('{storage_path}') SELECT 99999 AS id, 'duckdb_test' AS name"
        )
        return True, "blind append succeeded (bypasses UC governance)"
    except Exception as e:
        err = str(e)
        if "read.only" in err.lower() or "not supported" in err.lower() or "cannot" in err.lower():
            return False, f"write not supported via delta_scan: {err[:200]}"
        elif "credential" in err.lower() or "access" in err.lower() or "permission" in err.lower():
            return False, f"storage credential error: {err[:200]}"
        return False, f"{err[:200]}"


def run_delta_scan_section():
    """Test the DuckDB delta extension (direct cloud storage path)."""
    print_header("Approach B: DuckDB delta Extension (Direct Cloud Storage)")

    print("""
  What this path is:
    The DuckDB `delta` extension uses delta-kernel-rs to read Delta tables
    directly from cloud storage by accessing the _delta_log/ transaction log.
    It does NOT go through Unity Catalog at all — it bypasses UC governance.

  Authentication required:
    Cloud storage credentials (S3 keys, Azure SAS, GCS SA key)
    NOT a Databricks token — delta_scan does not understand Databricks auth.

  Expected outcome:
    Without cloud storage credentials, all delta_scan() calls will fail
    with storage permission errors. This is expected behavior.
    The errors confirm the approach path but show the credential gap.
""")

    # Retrieve storage paths from UC REST API (using PAT)
    print("  Looking up table storage paths from UC REST API...")
    storage_paths = {}
    for table_name in ALL_TABLES:
        path = get_table_storage_path(table_name)
        if path:
            storage_paths[table_name] = path
            print(f"    {table_name}: {path}")
        else:
            print(f"    {table_name}: could not retrieve (no token or API error)")

    if not storage_paths:
        print("""
  No storage paths retrieved. To test delta_scan manually:
    1. In Databricks SQL run: DESCRIBE EXTENDED {catalog}.{schema}.{table}
    2. Find the "Location" field (e.g., s3://bucket/path/to/table)
    3. Run: FROM delta_scan('s3://bucket/path/to/table')
       with appropriate cloud storage credentials configured in DuckDB.
""".format(catalog=CATALOG, schema=SCHEMA, table="managed_delta"))

    conn = duckdb.connect()
    try:
        conn.execute("LOAD delta;")
    except Exception as e:
        print_result("Load delta extension", False, str(e))
        return

    for table_name in ALL_TABLES:
        print(f"\n  --- Table: {table_name} ---")

        storage_path = storage_paths.get(table_name)
        if not storage_path:
            no_path_msg = "storage path unknown — cannot test delta_scan without storage location"
            for op in OPERATIONS:
                results[table_name][op] = (False, no_path_msg)
                print_result(op, False, no_path_msg)
            continue

        # Read
        success, detail = try_delta_scan_read(conn, storage_path, table_name)
        results[table_name]["Read (SELECT)"] = (success, detail)
        print_result("Read (SELECT)", success, detail)

        # Predicate pushdown (only if read worked, otherwise same error)
        if success:
            pp_success, pp_detail = try_delta_scan_predicate(conn, storage_path)
        else:
            pp_success, pp_detail = False, "skipped — read failed (same credential barrier)"
        results[table_name]["Predicate Pushdown"] = (pp_success, pp_detail)
        print_result("Predicate Pushdown", pp_success, pp_detail)

        # Append (blind insert — the one write operation delta extension supports)
        if success:
            app_success, app_detail = try_delta_scan_append(conn, storage_path)
        else:
            app_success, app_detail = False, "skipped — read failed (same credential barrier)"
        results[table_name]["Write (Append)"] = (app_success, app_detail)
        print_result("Write (Append)", app_success, app_detail)

        # Update — not supported by delta extension
        update_detail = "delta extension supports blind append only; UPDATE not supported (no MERGE/UPDATE/DELETE)"
        results[table_name]["Write (Update)"] = (False, update_detail)
        print_result("Write (Update)", False, update_detail)

        # Delete — not supported by delta extension
        delete_detail = "delta extension does not support DELETE operations"
        results[table_name]["Delete Row"] = (False, delete_detail)
        print_result("Delete Row", False, delete_detail)

        # Create Table — delta extension cannot create Delta tables
        create_detail = "delta extension cannot CREATE tables; tables must pre-exist on storage"
        results[table_name]["Create Table"] = (False, create_detail)
        print_result("Create Table", False, create_detail)

        # Drop Table — delta extension cannot drop Delta tables
        drop_detail = "delta extension cannot DROP tables; no DDL support"
        results[table_name]["Drop Table"] = (False, drop_detail)
        print_result("Drop Table", False, drop_detail)

    conn.close()


# ---------------------------------------------------------------------------
# Protocol architecture explanation
# ---------------------------------------------------------------------------

def explain_why_uc_rest_cant_write():
    """Print architectural explanation for why writes are impossible via UC REST."""
    print_header("Why UC REST / Delta Sharing Cannot Support Writes")
    print("""
  The Delta Sharing protocol is a READ-ONLY data sharing protocol by design.

  Architecture:
    UC Delta Sharing Server → returns presigned S3/ADLS/GCS URLs for Parquet files
    Client downloads those files and queries locally

  There are no write endpoints in the Delta Sharing spec:
    ✅  GET  /shares                     list shares
    ✅  GET  /shares/{share}/schemas      list schemas
    ✅  GET  /shares/{share}/.../tables   list tables
    ✅  POST /shares/{share}/.../query    get presigned URLs for read
    ❌  no INSERT / UPDATE / DELETE / MERGE endpoints exist

  This is a fundamental protocol constraint, not a DuckDB or UC limitation.
  The protocol was designed for safe, governed, read-only data sharing with
  external recipients — write access would break the security model.

  If you need writes from DuckDB to UC Delta tables, your options are:
    1. Iceberg REST path (DuckDB iceberg extension) — full CRUD with UC governance
       → ATTACH 'https://{workspace}/api/2.1/unity-catalog/iceberg-rest'
    2. Databricks SQL endpoint (JDBC/ODBC) — writes via SQL, not native Delta
    3. delta-rs Python library — writes Delta format directly to storage (bypasses UC)
""".format(workspace=WORKSPACE_URL.replace("https://", "")))


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary():
    """Print a summary table of all results."""
    print_header("Summary: Delta / UC REST Path Results")

    # Column widths
    table_col = 20
    op_col = 22
    result_col = 8

    header = (
        f"  {'Table':<{table_col}} {'Operation':<{op_col}} {'Result':<{result_col}} Detail"
    )
    print(header)
    print("  " + "-" * 90)

    for table_name in ALL_TABLES:
        for op in OPERATIONS:
            if op in results[table_name]:
                success, detail = results[table_name][op]
                icon = "✅" if success else "❌"
                print(
                    f"  {table_name:<{table_col}} {op:<{op_col}} {icon:<{result_col}} {detail[:60]}"
                )
            else:
                print(
                    f"  {table_name:<{table_col}} {op:<{op_col}} {'➖':<{result_col}} not tested"
                )

    print()

    # High-level takeaways
    print("  Key Takeaways:")
    print("    ❌ No native DuckDB Delta Sharing REST client exists")
    print("    ❌ delta_scan() bypasses UC entirely — needs cloud storage creds, not a PAT")
    print("    ❌ Delta Sharing protocol is read-only by design — no writes possible")
    print("    ❌ delta_scan() supports blind-append only — no UPDATE/DELETE/MERGE")
    print("    ❌ delta_scan() cannot CREATE or DROP tables")
    print("    ⚠️  managed_iceberg tested here via Delta path — expect same failures")
    print("    ✅  Iceberg REST path is the recommended DuckDB ↔ UC integration")
    print("       (see scripts/03_iceberg_uc_rest.py for that experiment)")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    print(f"\n{SEPARATOR}")
    print("DuckDB + Unity Catalog: Delta / UC REST Path Experiment")
    print(f"Workspace: {WORKSPACE_URL}")
    print(f"Catalog:   {CATALOG}")
    print(f"Schema:    {SCHEMA}")
    print(f"Tables:    {', '.join(ALL_TABLES)}")
    print(SEPARATOR)

    prereqs_ok = check_prerequisites()
    if not prereqs_ok:
        print("\n  WARNING: Some prerequisites missing. Continuing with available functionality.\n")

    run_delta_sharing_rest_section()
    run_delta_scan_section()
    explain_why_uc_rest_cant_write()
    print_summary()


if __name__ == "__main__":
    main()
