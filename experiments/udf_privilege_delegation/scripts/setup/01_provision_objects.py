"""Provision the scenario: admin-owned objects, low-privilege caller grants.

Creates, as the high-privilege author:

  sensitive_table            a table the caller is deliberately NOT granted on
  f_sql_read_sensitive()     SQL UDF whose body reads sensitive_table
  f_py_read_sensitive()      Python UDF that reads nothing (see note below)
  f_py_egress(url)           Python UDF that attempts one outbound HTTPS request
  f_py_credential_probe()    Python UDF that reports its own execution context
  f_py_create_sp(name)       Python UDF that attempts a SCIM create-SP call

and grants the low-privilege caller USE CATALOG / USE SCHEMA / EXECUTE — and
nothing else. In particular it is never granted SELECT on sensitive_table, so
any successful read through a function is evidence of definer's rights.

Note on f_py_read_sensitive: Unity Catalog Python UDFs cannot open a Spark
session or issue SQL from inside the sandbox, so the Python-side privilege
question is answered by the egress and credential probes rather than by a table
read. That constraint is itself recorded as a matrix finding.

Idempotent: CREATE OR REPLACE throughout.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common import (  # noqa: E402
    CATALOG,
    FQ_SCHEMA,
    LOWPRIV_DISPLAY_NAME,
    admin_client,
    info,
    ok,
    run_sql_or_die,
    section,
    step,
    warehouse_id,
)

# The caller is referenced in GRANT statements by application id.
import os  # noqa: E402

LOWPRIV_APP_ID = os.environ.get("EXPERIMENT_LOWPRIV_CLIENT_ID", "")


PY_EGRESS = """
import json, urllib.request, urllib.error
try:
    req = urllib.request.Request(url, headers={"User-Agent": "uc-python-udf-probe"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.dumps({"reached": True, "status": resp.status})
except urllib.error.HTTPError as exc:
    # An HTTP status back from the origin means the packet made the round trip.
    # 401/403 is an authorization answer, not a connectivity failure — conflating
    # the two would understate the sandbox's reach.
    return json.dumps({"reached": True, "status": exc.code, "http_error": True})
except Exception as exc:
    return json.dumps({"reached": False, "error_type": type(exc).__name__, "error": str(exc)[:400]})
"""

PY_CREDENTIAL_PROBE = """
import json, os
report = {}
# Every DATABRICKS_* var, and separately the subset that could actually
# authenticate a request. The distinction matters: the sandbox exports
# unrelated bookkeeping vars, and counting those as "a credential exists"
# would turn this row into a false positive.
CREDENTIAL_VARS = (
    "DATABRICKS_TOKEN", "DATABRICKS_HOST", "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET", "DATABRICKS_OAUTH_TOKEN",
    "DATABRICKS_USERNAME", "DATABRICKS_PASSWORD",
)
report["databricks_env_vars"] = sorted(k for k in os.environ if k.startswith("DATABRICKS_"))
report["credential_env_vars_present"] = sorted(k for k in CREDENTIAL_VARS if os.environ.get(k))
try:
    from databricks.sdk import WorkspaceClient
    report["sdk_importable"] = True
    try:
        w = WorkspaceClient()
        report["sdk_default_auth"] = "constructed"
        report["sdk_auth_type"] = str(getattr(w.config, "auth_type", None))
    except Exception as exc:
        report["sdk_default_auth"] = f"{type(exc).__name__}: {str(exc)[:200]}"
except Exception as exc:
    report["sdk_importable"] = f"{type(exc).__name__}: {str(exc)[:200]}"
try:
    import IPython
    report["dbutils_available"] = "dbutils" in dir(IPython.get_ipython() or object())
except Exception as exc:
    report["dbutils_available"] = f"unavailable: {type(exc).__name__}"
try:
    from pyspark.sql import SparkSession
    report["spark_session"] = "active" if SparkSession.getActiveSession() else "none"
except Exception as exc:
    report["spark_session"] = f"{type(exc).__name__}: {str(exc)[:120]}"
return json.dumps(report)
"""

# The concrete administrative action under test. Creating a service principal
# has no SQL surface at all — it is a SCIM REST call — so a UDF wrapper must
# make an authenticated outbound request.
PY_CREATE_SP = """
import json, urllib.request, urllib.error, os
result = {}
token = os.environ.get("DATABRICKS_TOKEN")
host = os.environ.get("DATABRICKS_HOST")
result["had_ambient_token"] = bool(token)
result["had_ambient_host"] = bool(host)
if not token or not host:
    result["outcome"] = "no ambient credential in UDF sandbox"
    return json.dumps(result)
body = json.dumps({
    "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"],
    "displayName": name,
    "active": True,
}).encode()
req = urllib.request.Request(
    host.rstrip("/") + "/api/2.0/preview/scim/v2/ServicePrincipals",
    data=body,
    method="POST",
    headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
)
try:
    with urllib.request.urlopen(req, timeout=15) as resp:
        result["outcome"] = "created"
        result["status"] = resp.status
except Exception as exc:
    result["outcome"] = "failed"
    result["error_type"] = type(exc).__name__
    result["error"] = str(exc)[:400]
return json.dumps(result)
"""


FUNCTIONS = {
    "f_sql_read_sensitive": f"""
CREATE OR REPLACE FUNCTION {FQ_SCHEMA}.f_sql_read_sensitive()
RETURNS BIGINT
COMMENT 'SQL UDF owned by an admin whose body reads a table the caller cannot select from.'
RETURN (SELECT count(*) FROM {FQ_SCHEMA}.sensitive_table)
""",
    "f_py_egress": f"""
CREATE OR REPLACE FUNCTION {FQ_SCHEMA}.f_py_egress(url STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Attempts one outbound HTTPS request from inside the UC Python UDF sandbox.'
AS $${PY_EGRESS}$$
""",
    "f_py_credential_probe": f"""
CREATE OR REPLACE FUNCTION {FQ_SCHEMA}.f_py_credential_probe()
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Reports what credentials and runtime facilities exist inside the UDF sandbox.'
AS $${PY_CREDENTIAL_PROBE}$$
""",
    "f_py_create_sp": f"""
CREATE OR REPLACE FUNCTION {FQ_SCHEMA}.f_py_create_sp(name STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'The concrete admin action under test: create a service principal via SCIM.'
AS $${PY_CREATE_SP}$$
""",
}


def main() -> int:
    section("setup: admin-owned objects and caller grants")
    if not LOWPRIV_APP_ID:
        raise SystemExit(
            "EXPERIMENT_LOWPRIV_CLIENT_ID is unset — run 00_create_lowpriv_principal.py first."
        )

    w = admin_client()
    wh = warehouse_id(w)

    step(f"creating schema {FQ_SCHEMA}")
    run_sql_or_die(w, f"CREATE SCHEMA IF NOT EXISTS {FQ_SCHEMA}", wh=wh)

    step("creating sensitive_table (caller gets no grant on it)")
    run_sql_or_die(
        w,
        f"CREATE OR REPLACE TABLE {FQ_SCHEMA}.sensitive_table "
        "(id INT, secret STRING)",
        wh=wh,
    )
    run_sql_or_die(
        w,
        f"INSERT INTO {FQ_SCHEMA}.sensitive_table VALUES "
        "(1, 'placeholder-row-a'), (2, 'placeholder-row-b'), (3, 'placeholder-row-c')",
        wh=wh,
    )
    ok("sensitive_table created with 3 rows")

    for fname, ddl in FUNCTIONS.items():
        step(f"creating function {fname}")
        run_sql_or_die(w, ddl, wh=wh)
        ok(f"{fname} created")

    step(f"granting the caller ({LOWPRIV_DISPLAY_NAME}) EXECUTE and nothing else")
    run_sql_or_die(w, f"GRANT USE CATALOG ON CATALOG `{CATALOG}` TO `{LOWPRIV_APP_ID}`", wh=wh)
    run_sql_or_die(w, f"GRANT USE SCHEMA ON SCHEMA {FQ_SCHEMA} TO `{LOWPRIV_APP_ID}`", wh=wh)
    for fname in FUNCTIONS:
        run_sql_or_die(
            w,
            f"GRANT EXECUTE ON FUNCTION {FQ_SCHEMA}.{fname} TO `{LOWPRIV_APP_ID}`",
            wh=wh,
        )
    ok("EXECUTE granted on every function; no SELECT granted on sensitive_table")

    step("confirming the caller's effective grants on sensitive_table")
    grants = run_sql_or_die(
        w, f"SHOW GRANTS `{LOWPRIV_APP_ID}` ON TABLE {FQ_SCHEMA}.sensitive_table", wh=wh
    )
    info(f"grants held by caller on sensitive_table: {grants.rows or 'none'}")
    if grants.rows:
        raise SystemExit(
            "The caller holds a direct grant on sensitive_table — that would "
            "invalidate the definer-vs-invoker rows. Revoke it and re-run."
        )
    ok("caller holds no direct privilege on sensitive_table")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
