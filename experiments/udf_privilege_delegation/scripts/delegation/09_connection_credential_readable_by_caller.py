"""Row 9 — does a connection hide its credential from the principals granted it?

This is row 6 asked of the other primitive. Row 6 killed the embedded-credential
workaround: a caller holding only EXECUTE read the function body, sentinel and
all, through DESCRIBE FUNCTION EXTENDED and information_schema.routines. The
whole reason to reach for a connection instead is the claim that Unity Catalog
stores the secret somewhere the grantee cannot read.

Row 8 showed the grantee has to be given USE CONNECTION for the connection to be
usable at all, so the population that can read it is exactly the population that
can use it. That makes this the load-bearing question for the entire connection
family: if USE CONNECTION also discloses the token, the connection is row 6 with
a nicer name.

The caller is granted USE CONNECTION *first* — with a control proving the grant
took effect — and then every read path is probed for the sentinel the connection
was created with.

A negative here is scoped to the paths actually probed. It is not a proof that
no read path exists, and the finding says so.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import (  # noqa: E402
    CONNECTION_NAME,
    CONNECTION_SENTINEL_TOKEN,
    admin_client,
    assert_distinct_identities,
    fail,
    info,
    lowpriv_client,
    ok,
    run_sql,
    run_sql_or_die,
    section,
    step,
    write_result,
)
from _delegation_common import http_request_outcome  # noqa: E402

ROW = "9"
QUESTION = (
    "Can a caller granted USE CONNECTION read the credential the connection stores?"
)

DIRECT_CALL = (
    f"SELECT to_json(http_request(conn => '{CONNECTION_NAME}', method => 'GET', path => ''))"
)


def _sdk_read(caller) -> dict:
    """The REST path the SQL probes do not cover: GET /connections/{name}."""
    try:
        conn = caller.connections.get(name=CONNECTION_NAME)
        blob = json.dumps(conn.as_dict(), default=str)
        return {"succeeded": True, "response": blob[:800], "sentinel_visible_to_caller": CONNECTION_SENTINEL_TOKEN in blob}
    except Exception as exc:  # noqa: BLE001 - the refusal is the evidence
        return {
            "succeeded": False,
            "error_type": type(exc).__name__,
            "error": str(exc)[:400],
            "sentinel_visible_to_caller": CONNECTION_SENTINEL_TOKEN in str(exc),
        }


def main() -> int:
    section(f"row {ROW}: is the connection's credential readable by its grantees?")
    admin = admin_client()
    caller = lowpriv_client()
    admin_who, caller_who = assert_distinct_identities(admin, caller)
    info(f"author={admin_who}  caller={caller_who}")

    step("granting the caller USE CONNECTION (row 8 proved it needs this to use it at all)")
    run_sql_or_die(
        admin, f"GRANT USE CONNECTION ON CONNECTION `{CONNECTION_NAME}` TO `{caller_who}`"
    )
    ok("USE CONNECTION granted")

    step("control: the grant took effect — the caller can now use the connection")
    control = http_request_outcome(caller, DIRECT_CALL)
    if control["allowed"]:
        ok(f"caller's direct call now allowed (status_code {control['status_code']})")
    else:
        fail("the grant did not take effect; the read probes below would prove nothing")

    read_paths = {
        "describe_connection": f"DESCRIBE CONNECTION `{CONNECTION_NAME}`",
        "show_connections": "SHOW CONNECTIONS",
        "information_schema_connections": (
            "SELECT * FROM system.information_schema.connections "
            f"WHERE connection_name = '{CONNECTION_NAME}'"
        ),
    }

    evidence: dict[str, object] = {"grant_control": control}
    leaked_via = []

    for name, stmt in read_paths.items():
        step(f"caller attempts: {name}")
        outcome = run_sql(caller, stmt)
        blob = " ".join(" ".join(str(c) for c in row) for row in outcome.rows)
        leaked = CONNECTION_SENTINEL_TOKEN in blob
        evidence[name] = {
            "statement": stmt,
            "succeeded": outcome.succeeded,
            "error_code": outcome.error_code,
            "error": outcome.error,
            "row_count": len(outcome.rows),
            "sentinel_visible_to_caller": leaked,
        }
        if leaked:
            leaked_via.append(name)
            fail("the stored credential is visible to the caller through this path")
        elif outcome.succeeded:
            ok(f"allowed, {len(outcome.rows)} row(s), no credential in the output")
        else:
            ok(f"refused: {outcome.error_code}")

    step("caller attempts: connections REST API (GET /unity-catalog/connections/{name})")
    sdk = _sdk_read(caller)
    evidence["connections_rest_api"] = sdk
    if sdk["sentinel_visible_to_caller"]:
        leaked_via.append("connections_rest_api")
        fail("the stored credential came back through the REST API")
    elif sdk["succeeded"]:
        ok("allowed, but the response carried no credential")
    else:
        ok(f"refused: {sdk.get('error_type')}")

    if leaked_via:
        write_result(
            ROW,
            question=QUESTION,
            status="fail",
            finding=(
                "The connection discloses its credential to the principals granted "
                f"USE CONNECTION, via {', '.join(leaked_via)}. That is row 6 with a "
                "different object: the population that can use the connection can "
                "also extract what it holds, so the connection narrows nothing."
            ),
            evidence=evidence,
        )
    elif not control["allowed"]:
        write_result(
            ROW,
            question=QUESTION,
            status="inconclusive",
            finding=(
                "Could not isolate the question: the USE CONNECTION grant did not "
                "take effect, so it is not established that the read paths were "
                "probed by a principal that actually held the privilege."
            ),
            evidence=evidence,
        )
    else:
        write_result(
            ROW,
            question=QUESTION,
            status="pass",
            finding=(
                "The credential stays hidden. The caller held USE CONNECTION and "
                "demonstrably used the connection, and none of DESCRIBE CONNECTION, "
                "SHOW CONNECTIONS, system.information_schema.connections, or the "
                "connections REST API returned the sentinel the connection was "
                "created with — DESCRIBE reports 'auth_scheme -> bearer' and the "
                "host, and stops there. This is the one property the embedded-"
                "credential workaround could not provide (row 6). Scoped to the "
                "paths probed; not a proof that no read path exists."
            ),
            evidence=evidence,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
