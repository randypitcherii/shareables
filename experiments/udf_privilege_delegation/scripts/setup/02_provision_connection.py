"""Provision the Unity Catalog HTTP connection scenario.

Rows 1-7 established that the UDF route to a delegated administrative action is
closed: the function type that carries definer's rights cannot express identity
administration, and the one that can has no credential to spend. A UC HTTP
*connection* is the obvious next primitive, because it is the platform's own
answer to "hold a credential somewhere the caller cannot read it".

This script creates, as the high-privilege author:

  udf_delegation_conn         an HTTP connection carrying a bearer token
  f_sql_http_via_connection() a SQL UDF whose body calls http_request() against it

and grants the caller EXECUTE on the function and *nothing on the connection*.
That grant shape is the whole experiment: if the caller can invoke the function
successfully while holding no privilege on the connection, the connection
privilege resolved as definer, exactly as table SELECT did in row 1. If the
platform refuses, it resolved as invoker and the connection is not a wrapping
primitive at all.

The target is example.com, not this workspace's own control plane. That is
deliberate: this workspace enforces IP access lists, and the serverless egress
address http_request() presents is not on the allow list, so a SCIM target
would answer 403 before the privilege question was ever reached — the platform
would refuse for a reason that has nothing to do with delegation. example.com is
the same neutral target row 2 used, and it isolates the one question this row
exists to answer. See the README caveat for what that does and does not license.

The bearer token is a sentinel, not a credential. Whether the *connection*
mechanism hides a secret from the caller is answered by row 9 reading it back,
and that question does not need a real secret to be answered — a public repo is
a bad place to find out otherwise.

Idempotent: CREATE OR REPLACE / DROP IF EXISTS throughout.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common import (  # noqa: E402
    CONNECTION_NAME,
    CONNECTION_SCIM_NAME,
    CONNECTION_SENTINEL_TOKEN,
    FQ_SCHEMA,
    LOWPRIV_DISPLAY_NAME,
    admin_client,
    info,
    ok,
    run_sql,
    run_sql_or_die,
    section,
    step,
    warehouse_id,
)

LOWPRIV_APP_ID = os.environ.get("EXPERIMENT_LOWPRIV_CLIENT_ID", "")

# The connection's reach: one host, one base path. Everything the connection can
# be pointed at afterwards lives under this prefix, which is the only shape
# constraint the connection object itself provides.
CONNECTION_HOST = "https://example.com"
CONNECTION_BASE_PATH = "/"

# The control-plane connection row 11 uses.
SCIM_BASE_PATH = "/api/2.0/preview/scim/v2/"


def main() -> int:
    section("setup: Unity Catalog HTTP connection and its SQL wrapper")
    if not LOWPRIV_APP_ID:
        raise SystemExit(
            "EXPERIMENT_LOWPRIV_CLIENT_ID is unset — run 00_create_lowpriv_principal.py first."
        )

    w = admin_client()
    wh = warehouse_id(w)

    step(f"creating connection {CONNECTION_NAME}")
    run_sql(w, f"DROP CONNECTION IF EXISTS `{CONNECTION_NAME}`", wh=wh)
    run_sql_or_die(
        w,
        f"""
CREATE CONNECTION `{CONNECTION_NAME}` TYPE HTTP
OPTIONS (
  host '{CONNECTION_HOST}',
  port '443',
  base_path '{CONNECTION_BASE_PATH}',
  bearer_token '{CONNECTION_SENTINEL_TOKEN}'
)
""",
        wh=wh,
    )
    ok(f"connection created (host {CONNECTION_HOST}, base_path {CONNECTION_BASE_PATH})")

    step("creating the admin-owned SQL UDF that wraps http_request()")
    run_sql_or_die(
        w,
        f"""
CREATE OR REPLACE FUNCTION {FQ_SCHEMA}.f_sql_http_via_connection()
RETURNS STRING
COMMENT 'SQL UDF owned by an admin whose body calls http_request() against an admin-owned connection.'
RETURN (
  SELECT to_json(http_request(
    conn => '{CONNECTION_NAME}',
    method => 'GET',
    path => ''
  ))
)
""",
        wh=wh,
    )
    run_sql_or_die(
        w,
        f"GRANT EXECUTE ON FUNCTION {FQ_SCHEMA}.f_sql_http_via_connection TO `{LOWPRIV_APP_ID}`",
        wh=wh,
    )
    ok("f_sql_http_via_connection created; caller granted EXECUTE")

    step("creating the SQL SECURITY DEFINER procedure that wraps the same call")
    run_sql_or_die(
        w,
        f"""
CREATE OR REPLACE PROCEDURE {FQ_SCHEMA}.p_http_via_connection()
LANGUAGE SQL
SQL SECURITY DEFINER
COMMENT 'The same http_request() call as the UDF, in the object that asks for definer rights explicitly.'
AS BEGIN
  SELECT to_json(http_request(
    conn => '{CONNECTION_NAME}',
    method => 'GET',
    path => ''
  ));
END
""",
        wh=wh,
    )
    run_sql_or_die(
        w,
        f"GRANT EXECUTE ON PROCEDURE {FQ_SCHEMA}.p_http_via_connection TO `{LOWPRIV_APP_ID}`",
        wh=wh,
    )
    ok("p_http_via_connection created; caller granted EXECUTE")
    info(
        "CREATE PROCEDURE requires the SQL SECURITY clause explicitly — there is no "
        "default — so a definer-rights procedure is an intentional object, not an "
        "accident of syntax"
    )

    step(f"creating connection {CONNECTION_SCIM_NAME} (this workspace's own SCIM API)")
    run_sql(w, f"DROP CONNECTION IF EXISTS `{CONNECTION_SCIM_NAME}`", wh=wh)
    run_sql_or_die(
        w,
        f"""
CREATE CONNECTION `{CONNECTION_SCIM_NAME}` TYPE HTTP
OPTIONS (
  host '{w.config.host.rstrip("/")}',
  port '443',
  base_path '{SCIM_BASE_PATH}',
  bearer_token '{CONNECTION_SENTINEL_TOKEN}'
)
""",
        wh=wh,
    )
    ok(f"connection created (base_path {SCIM_BASE_PATH})")
    info(
        "the token is the same sentinel: row 11 asks whether the request reaches "
        "authentication at all, which a real credential would only obscure"
    )

    # Row 9 grants USE CONNECTION and leaves it granted, so re-provisioning after a
    # matrix run would otherwise start row 8 from a state that cannot answer it.
    step("revoking any USE CONNECTION the caller carries from a previous run")
    run_sql(
        w,
        f"REVOKE USE CONNECTION ON CONNECTION `{CONNECTION_NAME}` FROM `{LOWPRIV_APP_ID}`",
        wh=wh,
    )

    step(f"confirming the caller ({LOWPRIV_DISPLAY_NAME}) holds nothing on the connection")
    grants = run_sql_or_die(
        w, f"SHOW GRANTS `{LOWPRIV_APP_ID}` ON CONNECTION `{CONNECTION_NAME}`", wh=wh
    )
    info(f"grants held by caller on the connection: {grants.rows or 'none'}")
    if grants.rows:
        raise SystemExit(
            "The caller already holds a privilege on the connection — row 8 would "
            "not be able to distinguish definer from invoker. Revoke it and re-run."
        )
    ok("caller holds no privilege on the connection")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
