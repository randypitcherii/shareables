"""Row 14 — does an explicit SQL SECURITY DEFINER procedure change row 8's answer?

Row 8 measured a SQL *function*, where definer's rights are implicit in the
object type. Databricks also has stored procedures, and there the security mode
is a required clause — `CREATE PROCEDURE` refuses to compile without
`SQL SECURITY DEFINER` or `SQL SECURITY INVOKER`. An author who writes DEFINER
has said, in the object definition, exactly the thing row 8 found the function
model would not do. Whether the platform honours it for a *connection* is a
different question from whether it honours it for a table, and it is cheap to
ask, so it gets asked rather than assumed.

The row is deliberately three-way, because there turn out to be three distinct
answers and collapsing any two of them would lose the finding:

  admin CALL      the owner invoking their own procedure — the control that says
                  the procedure and the connection both work.
  caller direct   the caller calling http_request() itself, with USE CONNECTION
                  revoked — the control that says the caller is unprivileged.
  caller CALL     the measurement.

This row revokes USE CONNECTION from the caller (row 9 grants it) and restores it
before returning, so rows 9-10 stay reproducible in any order.
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
    run_sql,
    section,
    step,
    write_result,
)
from _delegation_common import http_request_outcome  # noqa: E402

ROW = "14"
QUESTION = (
    "Does a SQL SECURITY DEFINER stored procedure resolve the connection privilege "
    "as definer, where the SQL UDF did not?"
)

DIRECT_CALL = (
    f"SELECT to_json(http_request(conn => '{CONNECTION_NAME}', method => 'GET', path => ''))"
)
PROCEDURE_CALL = f"CALL {FQ_SCHEMA}.p_http_via_connection()"

# The refusal row 8 saw: Unity Catalog declining the connection outright.
_UC_REFUSAL = "USE CONNECTION"
# A different refusal: the connection resolved, but no credential was presented.
_CREDENTIAL_MARKERS = ("Credential was not sent", "unsupported type for this API")


def main() -> int:
    section(f"row {ROW}: definer-rights procedure against a connection")
    admin = admin_client()
    caller = lowpriv_client()
    admin_who, caller_who = assert_distinct_identities(admin, caller)
    info(f"author={admin_who}  caller={caller_who}")

    step("revoking USE CONNECTION from the caller so the question is the procedure's")
    revoked = run_sql(
        admin, f"REVOKE USE CONNECTION ON CONNECTION `{CONNECTION_NAME}` FROM `{caller_who}`"
    )
    info(f"revoke reported: {'ok' if revoked.succeeded else revoked.error_code}")

    try:
        step("control: admin CALLs its own procedure")
        admin_call = http_request_outcome(admin, PROCEDURE_CALL)
        if admin_call["allowed"]:
            ok(f"allowed (status_code {admin_call['status_code']})")
        else:
            fail(f"the owner's own CALL did not work: {admin_call}")

        step("control: caller calls http_request() directly (must be refused)")
        caller_direct = http_request_outcome(caller, DIRECT_CALL)
        if caller_direct["denied_by_unity_catalog"]:
            ok("refused, as required — the caller holds nothing on the connection")
        else:
            fail("the caller was not refused; this row cannot isolate its question")

        step("measurement: caller CALLs the definer-rights procedure")
        caller_call = http_request_outcome(caller, PROCEDURE_CALL)
        caller_body = caller_call["body"] or caller_call["error"] or ""
        caller_call["refused_for_missing_use_connection"] = _UC_REFUSAL in caller_body
        caller_call["failed_at_credential_resolution"] = any(
            m in caller_body for m in _CREDENTIAL_MARKERS
        )
        if caller_call["allowed"]:
            ok(f"allowed (status_code {caller_call['status_code']})")
        elif caller_call["refused_for_missing_use_connection"]:
            info("refused for missing USE CONNECTION — same answer as the UDF")
        elif caller_call["failed_at_credential_resolution"]:
            info(
                f"not refused for missing USE CONNECTION; failed at credential "
                f"resolution instead (status_code {caller_call['status_code']})"
            )
        else:
            info(f"refused, unclassified: {caller_body[:200]}")
    finally:
        run_sql(
            admin,
            f"GRANT USE CONNECTION ON CONNECTION `{CONNECTION_NAME}` TO `{caller_who}`",
        )
        ok("USE CONNECTION restored to the caller")

    evidence = {
        "admin_call": admin_call,
        "caller_direct_call": caller_direct,
        "caller_call": caller_call,
    }

    if not admin_call["allowed"] or not caller_direct["denied_by_unity_catalog"]:
        write_result(
            ROW,
            question=QUESTION,
            status="inconclusive",
            finding=(
                "Could not isolate the question: either the procedure did not work "
                "for its own owner, or the caller was not actually unprivileged on "
                "the connection."
            ),
            evidence=evidence,
        )
    elif caller_call["allowed"]:
        write_result(
            ROW,
            question=QUESTION,
            status="pass",
            finding=(
                "A SQL SECURITY DEFINER procedure does what the SQL UDF would not: "
                "the caller, refused the connection directly, completed the same call "
                "through the procedure holding only EXECUTE. Row 8's answer is a "
                "property of the function object, not of connections."
            ),
            evidence=evidence,
        )
    elif caller_call["failed_at_credential_resolution"]:
        write_result(
            ROW,
            question=QUESTION,
            status="partial",
            finding=(
                "The procedure changes the answer without delivering the outcome. The "
                "caller was NOT refused for missing USE CONNECTION — the refusal row 8 "
                "produced through the UDF — so the definer clause is honoured far "
                "enough to get past that check. The call then failed at credential "
                f"resolution: status_code {caller_call['status_code']}, 'Credential was "
                "not sent or was of an unsupported type', a platform-shaped message "
                "carrying a Databricks request id rather than anything the target host "
                "would say. The definer-rights procedure therefore resolves the "
                "connection but does not present its credential, which is neither a "
                "working delegation nor the same refusal as the function. Treat this "
                "as a measured behaviour of one workspace on one date, not as a "
                "documented contract: nothing in the public documentation describes "
                "this path, and it is the kind of edge that changes without notice."
            ),
            evidence=evidence,
        )
    else:
        write_result(
            ROW,
            question=QUESTION,
            status="fail",
            finding=(
                "Same answer as the SQL UDF. The definer-rights procedure was refused "
                "for the caller's missing USE CONNECTION, so the connection privilege "
                "is re-checked against the invoker regardless of the object wrapping "
                "the call or of an explicit SQL SECURITY DEFINER clause."
            ),
            evidence=evidence,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
