"""Prove auth + connectivity end-to-end with one real request.

Runs `SELECT current_user()` on the warehouse. A config check would not prove
anything; this proves the profile, the warehouse, and Unity Catalog are all
reachable and that we know which identity we are.
"""

from __future__ import annotations

from _common import CATALOG, PROFILE, admin_client, info, ok, run_sql, section, step, warehouse_id


def main() -> int:
    section("verify: auth + connectivity")
    step(f"building WorkspaceClient from profile {PROFILE!r}")
    w = admin_client()
    wh = warehouse_id(w)
    info(f"warehouse_id={wh}")

    step("SELECT current_user(), current_catalog()")
    outcome = run_sql(w, "SELECT current_user() AS who, current_version().dbsql_version AS v")
    if not outcome.succeeded:
        print(f"  !!  {outcome.error_code}: {outcome.error}")
        return 1

    who = outcome.rows[0][0]
    version = outcome.rows[0][1]
    ok(f"authenticated as {who} on DBSQL {version}")

    step(f"confirming catalog {CATALOG!r} is reachable")
    cat = run_sql(w, f"SHOW SCHEMAS IN `{CATALOG}`")
    if not cat.succeeded:
        print(f"  !!  {cat.error_code}: {cat.error}")
        return 1
    ok(f"catalog {CATALOG!r} reachable ({len(cat.rows)} schemas visible)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
