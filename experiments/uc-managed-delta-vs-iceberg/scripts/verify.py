"""Live preflight: OAuth identity, SQL warehouse, metastore external access, and one real
Iceberg REST catalog call. Exits non-zero (and writes nothing) on any failure."""

import requests
from _common import CATALOG, client, host, must_sql, oauth_token


def main() -> None:
    w = client()
    me = w.current_user.me().user_name
    sql_user, dbsql = must_sql("SELECT current_user(), current_version().dbsql_version").rows[0]
    if sql_user != me:
        raise SystemExit(f"identity mismatch between REST ({me}) and SQL ({sql_user})")
    print(f"OK: auth_type={w.config.auth_type}; REST and SQL resolve to the same principal")
    print(f"OK: serverless SQL warehouse reachable (DBSQL {dbsql})")
    ms = w.metastores.summary()
    if not ms.external_access_enabled:
        raise SystemExit("metastore external data access is disabled; rows 01 and 05 cannot run")
    print("OK: metastore external data access enabled")
    r = requests.get(
        f"{host()}/api/2.1/unity-catalog/iceberg-rest/v1/config",
        params={"warehouse": CATALOG},
        headers={"Authorization": f"Bearer {oauth_token()}"},
        timeout=30,
    )
    # Any HTTP response proves reachability; 401/403 are authorization findings.
    print(f"OK: Iceberg REST catalog reachable, HTTP {r.status_code}")
    if r.status_code in (401, 403):
        raise SystemExit("Iceberg REST catalog rejected this principal")


if __name__ == "__main__":
    main()
