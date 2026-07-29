"""Row 8 — does a connection privilege resolve as definer, the way table SELECT did?

Row 1 established that a UC SQL UDF is a real privilege boundary for *data*: the
caller read a table it had been refused, through a function it merely held
EXECUTE on. This row asks whether that property extends to a Unity Catalog
**connection** — the platform's own primitive for holding a credential somewhere
the caller cannot read it.

If it does, the shape the original proposal wanted actually exists: an admin owns
a connection carrying the credential, writes a SQL UDF that calls http_request()
against it with the method and path fixed, and grants callers nothing but
EXECUTE. The caller gets one shaped action and is never trusted with a secret.

If it does not — if the platform re-checks the connection privilege against the
invoker — then the connection is not a wrapping primitive at all. Whoever can
use it through the function can use it directly, at any method and path they
choose, and the author's fixed shape constrains nothing.

Two measurements, and the control is what makes the row mean anything:

  control     the caller calls http_request() on the connection *directly*.
              It must be refused. Without that, a successful call through the
              function is indistinguishable from the caller having held the
              privilege all along — the same mistake trap 1 records about the
              identity of the caller itself.
  measurement the same caller invokes the admin-owned function.

Scoring note: Unity Catalog reports this refusal as ``status_code = 403`` inside
a *successful* statement rather than as a SQL error, so both calls are
classified by ``http_request_outcome`` on the body of the response, not on
whether the statement ran. See trap 4 in the evidence trail.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import (  # noqa: E402
    CONNECTION_NAME,
    FQ_SCHEMA,
    admin_client,
    assert_distinct_identities,
    fail,
    info,
    lowpriv_client,
    ok,
    section,
    step,
    write_result,
)
from _delegation_common import http_request_outcome  # noqa: E402

ROW = "8"
QUESTION = (
    "Does a UC SQL UDF resolve the USE CONNECTION privilege on an admin-owned "
    "connection as definer, the way it resolves table SELECT?"
)

DIRECT_CALL = (
    f"SELECT to_json(http_request(conn => '{CONNECTION_NAME}', method => 'GET', path => ''))"
)
VIA_FUNCTION = f"SELECT {FQ_SCHEMA}.f_sql_http_via_connection()"


def _describe(label: str, result: dict) -> None:
    if result["allowed"]:
        ok(f"{label}: allowed (status_code {result['status_code']})")
    elif result["denied_by_unity_catalog"]:
        info(f"{label}: refused by Unity Catalog (status_code {result['status_code']})")
    else:
        fail(f"{label}: neither allowed nor a recognisable refusal — {result}")


def main() -> int:
    section(f"row {ROW}: connection privilege — definer or invoker?")
    admin = admin_client()
    caller = lowpriv_client()
    admin_who, caller_who = assert_distinct_identities(admin, caller)
    info(f"author={admin_who}  caller={caller_who}")

    step("control: admin calls http_request() directly (proves the connection works at all)")
    admin_direct = http_request_outcome(admin, DIRECT_CALL)
    _describe("admin direct", admin_direct)

    step("control: caller calls http_request() directly (must be refused)")
    caller_direct = http_request_outcome(caller, DIRECT_CALL)
    _describe("caller direct", caller_direct)

    step("measurement: caller invokes the admin-owned function that wraps it")
    caller_via_fn = http_request_outcome(caller, VIA_FUNCTION)
    _describe("caller via function", caller_via_fn)

    evidence = {
        "admin_direct_call": admin_direct,
        "caller_direct_call": caller_direct,
        "caller_via_function": caller_via_fn,
    }

    if not admin_direct["allowed"]:
        write_result(
            ROW,
            question=QUESTION,
            status="inconclusive",
            finding=(
                "Could not isolate the question: the connection did not work for its "
                "own owner, so neither the control nor the measurement says anything "
                "about how the privilege resolves."
            ),
            evidence=evidence,
        )
    elif not caller_direct["denied_by_unity_catalog"]:
        write_result(
            ROW,
            question=QUESTION,
            status="inconclusive",
            finding=(
                "Could not isolate the question: the caller was not refused the "
                "connection directly, so using it through the function is not "
                "evidence of definer's rights."
            ),
            evidence=evidence,
        )
    elif caller_via_fn["allowed"]:
        write_result(
            ROW,
            question=QUESTION,
            status="pass",
            finding=(
                "Definer. The caller was refused the connection directly and then "
                "used it through the admin-owned function holding nothing but "
                "EXECUTE. The connection privilege resolves against the function's "
                "owner, exactly as table SELECT did in row 1."
            ),
            evidence=evidence,
        )
    else:
        write_result(
            ROW,
            question=QUESTION,
            status="fail",
            finding=(
                "Invoker. The caller holds EXECUTE on the admin-owned function and "
                "was refused anyway, with the same message it got calling the "
                "connection directly — 'User is missing USE CONNECTION on "
                f"{CONNECTION_NAME}', surfaced as status_code "
                f"{caller_via_fn['status_code']} inside a successful statement. "
                "Unity Catalog re-checks the connection privilege against the "
                "invoker, so wrapping http_request() in a SQL UDF confers nothing. "
                "Definer's rights cover the objects the body reads, not the "
                "connection the body calls out through."
            ),
            evidence=evidence,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
