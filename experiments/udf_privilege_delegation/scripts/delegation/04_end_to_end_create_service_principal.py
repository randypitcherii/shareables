"""Row 4 — end to end: does the low-privilege caller actually create a service principal?

The rows above test the mechanism in pieces. This one asks the question the way
a customer would: an admin publishes f_py_create_sp, a low-privilege user is
granted EXECUTE on it and nothing else, the user calls it — does a service
principal exist afterwards?

The verdict is not the function's return value. It is the workspace SCIM
directory, read back by the admin: an actual principal, or nothing.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _delegation_common import call_function, decode_json_scalar  # noqa: E402

from _common import (  # noqa: E402
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

ROW = "4"
QUESTION = "Can a low-privilege caller create a service principal by invoking an admin-owned UDF wrapper?"

TARGET_NAME = "udf-delegation-target-sp"


def directory_contains(w, display_name: str) -> bool:
    return any(w.service_principals.list(filter=f'displayName eq "{display_name}"'))


def main() -> int:
    section(f"row {ROW}: end-to-end delegated service principal creation")
    admin = admin_client()
    caller = lowpriv_client()
    admin_who, caller_who = assert_distinct_identities(admin, caller)
    info(f"author={admin_who}  caller={caller_who}")

    step(f"pre-check: is {TARGET_NAME!r} already in the directory?")
    if directory_contains(admin, TARGET_NAME):
        fail("target already exists — run `make teardown` first so the result is meaningful")
        write_result(
            ROW,
            question=QUESTION,
            status="inconclusive",
            finding=(
                f"Could not isolate the result: a service principal named "
                f"{TARGET_NAME!r} already existed before the call, so its presence "
                "afterwards would prove nothing. Tear down and re-run."
            ),
            evidence={"precheck": "target already present"},
        )
        return 1
    ok("directory is clean")

    step("low-privilege caller invokes the admin-owned wrapper")
    outcome = call_function(caller, "f_py_create_sp", TARGET_NAME)
    decoded = decode_json_scalar(outcome)
    if outcome.succeeded:
        info(f"function returned: {decoded}")
    else:
        fail(f"invocation failed: {outcome.error_code}")
        info(f"error: {outcome.error}")

    step("verdict: reading the SCIM directory back as admin")
    created = directory_contains(admin, TARGET_NAME)

    evidence = {
        "invocation": outcome.summary(),
        "udf_report": decoded,
        "directory_contains_target_after_call": created,
    }

    if created:
        ok(f"{TARGET_NAME} exists — delegation succeeded")
        write_result(
            ROW,
            question=QUESTION,
            status="pass",
            finding=(
                "End-to-end delegation worked: a caller holding only EXECUTE on the "
                "function caused a service principal to be created, without holding "
                "any identity-management privilege directly."
            ),
            evidence=evidence,
        )
    else:
        fail(f"{TARGET_NAME} was not created")
        write_result(
            ROW,
            question=QUESTION,
            status="fail",
            finding=(
                "End-to-end delegation did not work. The caller could invoke the "
                "admin-owned function, but no service principal was created — the "
                "function body could not perform the administrative action. The "
                "failure is a capability failure inside the sandbox (see rows 2 and "
                "3), not an authorization failure on the function itself."
            ),
            evidence=evidence,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
