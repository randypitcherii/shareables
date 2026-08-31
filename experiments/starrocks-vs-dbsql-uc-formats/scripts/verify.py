"""Prove auth + connectivity end-to-end with real calls before any battery runs.

- Databricks: SELECT current_user() + current_version() on the serverless warehouse.
- Warehouse cost basis: size -> DBU/hr used by the cost model.
- StarRocks: current_user() + current_version() over the MySQL protocol.
- UC Iceberg REST: GET /v1/config with the profile's OAuth token (status code is
  the finding — 401/403 are authorization results, not connectivity failures).
"""

import requests
from _common import (
    databricks_config,
    dbsql_exec,
    load_config,
    oauth_token,
    section,
    starrocks_conn,
    warehouse_dbu_per_hour,
    workspace_client,
)
from _uc_catalog import sr_identity


def main() -> None:
    cfg = load_config()

    section("Databricks serverless SQL")
    w = workspace_client(cfg)
    rows, elapsed = dbsql_exec(cfg, "SELECT current_user(), current_version()", w)
    print(f"identity={rows[0][0]} version={rows[0][1]} ({elapsed:.2f}s)")
    dbu, size = warehouse_dbu_per_hour(cfg, w)
    print(f"warehouse size={size} -> {dbu} DBU/hr @ ${cfg.usd_per_dbu}/DBU")

    section("UC Iceberg REST endpoint")
    host = databricks_config(cfg).host
    resp = requests.get(
        f"{host}/api/2.1/unity-catalog/iceberg-rest/v1/config",
        params={"warehouse": cfg.uc_catalog},
        headers={"Authorization": f"Bearer {oauth_token(cfg)}"},
        timeout=30,
    )
    print(f"GET /v1/config?warehouse={cfg.uc_catalog} -> HTTP {resp.status_code}")
    print(resp.text[:500])

    section("StarRocks")
    conn = starrocks_conn(cfg)
    ident = sr_identity(conn)
    print(f"identity={ident['user']} version={ident['version']}")
    conn.close()

    print("\nverify: all surfaces answered")


if __name__ == "__main__":
    main()
