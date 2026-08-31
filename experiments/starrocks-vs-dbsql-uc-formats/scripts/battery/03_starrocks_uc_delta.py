"""Battery row 3 — StarRocks writing UC MANAGED DELTA: expected UNSUPPORTED.

Two independent doc-level grounds, each probed live so the cell carries evidence
rather than assertion:

1. StarRocks' Delta Lake catalog supports only Hive Metastore and AWS Glue as
   metastore types — there is no Unity Catalog metastore option to even point at.
   (docs.starrocks.io Delta Lake catalog; feature-support matrix lists no Delta
   write features at all — the integration is read-only.)
2. UC's Iceberg REST endpoint exposes managed DELTA tables read-only (Iceberg
   metadata via UniForm); write attempts through that path are rejected server-side.
   That write attempt is exercised in interop script 07.

This script records the live errors for ground 1.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common import (  # noqa: E402
    databricks_config,
    load_config,
    oauth_token,
    record_result,
    section,
    sr_exec,
    starrocks_conn,
)
from _uc_catalog import sr_identity  # noqa: E402


def attempt(conn, name: str, ddl: str) -> dict:
    try:
        sr_exec(conn, ddl)
        # If creation parses, force a resolution against the metastore.
        sr_exec(conn, f"SHOW DATABASES FROM {name}")
        result = {"status": "unexpected_ok"}
    except Exception as e:  # noqa: BLE001 — the error text is the evidence
        result = {"status": "error", "error": str(e)[:1500]}
    try:
        sr_exec(conn, f"DROP CATALOG {name}")
    except Exception:  # noqa: BLE001
        pass
    return result


def main() -> None:
    cfg = load_config()
    conn = starrocks_conn(cfg)
    host = databricks_config(cfg).host
    token = oauth_token(cfg)

    section("attempt: deltalake catalog with a 'unity' metastore type (no such option)")
    a1 = attempt(
        conn,
        "uc_delta_probe1",
        f"""
CREATE EXTERNAL CATALOG uc_delta_probe1 PROPERTIES (
  "type" = "deltalake",
  "hive.metastore.type" = "unity",
  "hive.metastore.uris" = "{host}/api/2.1/unity-catalog"
)
""",
    )
    print(a1)

    section("attempt: deltalake catalog pointed at the UC REST endpoint as a metastore")
    a2 = attempt(
        conn,
        "uc_delta_probe2",
        f"""
CREATE EXTERNAL CATALOG uc_delta_probe2 PROPERTIES (
  "type" = "deltalake",
  "hive.metastore.type" = "rest",
  "hive.metastore.uris" = "{host}/api/2.1/unity-catalog/iceberg-rest",
  "deltalake.catalog.oauth2.token" = "{token}"
)
""",
    )
    print(a2)

    record_result(
        "battery_03_starrocks_uc_managed_delta",
        {
            "engine": f"starrocks-{sr_identity(conn)['version']}",
            "format": "uc-managed-delta",
            "identity": sr_identity(conn),
            "verdict": "unsupported",
            "grounds": {
                "delta_catalog_metastore_types": "hive metastore and AWS Glue only (docs)",
                "delta_write_support": "none — Delta Lake integration is read-only (docs)",
                "probe_unity_metastore_type": a1,
                "probe_rest_metastore_type": a2,
                "write_via_iceberg_rest_uniform": "see interop_07 write probe",
            },
            "ops": {},
        },
    )
    conn.close()


if __name__ == "__main__":
    main()
