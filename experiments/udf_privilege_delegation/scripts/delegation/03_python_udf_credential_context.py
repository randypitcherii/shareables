"""Row 3 — does a UC Python UDF receive an ambient Databricks credential?

Even with egress, a wrapper function needs *something to authenticate with*.
The delegation pattern only works if the function body can act as the owner —
which requires the sandbox to hand it an owner-scoped credential.

This asks the sandbox to describe itself: which DATABRICKS_* environment
variables exist, whether the SDK can construct a default-auth client, whether
dbutils or an active Spark session are reachable.

Run as both identities: a credential that appears only for the admin would
still not help, because the function must work when the *caller* invokes it.
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

ROW = "3"
QUESTION = "Does a UC Python UDF receive an ambient Databricks credential it could use to act as the function owner?"


def probe(w, label: str) -> dict:
    step(f"[{label}] f_py_credential_probe()")
    outcome = call_function(w, "f_py_credential_probe")
    decoded = decode_json_scalar(outcome)
    if outcome.succeeded:
        ok(f"[{label}] sandbox reported: {decoded}")
    else:
        fail(f"[{label}] invocation failed: {outcome.error_code}")
        info(f"error: {outcome.error}")
    return {"invocation": outcome.summary(), "udf_report": decoded}


def main() -> int:
    section(f"row {ROW}: UC Python UDF — credential context")

    admin_w, caller_w = admin_client(), lowpriv_client()
    admin_who, caller_who = assert_distinct_identities(admin_w, caller_w)
    info(f"author={admin_who}  caller={caller_who}")

    admin = probe(admin_w, "admin")
    caller = probe(caller_w, "low-priv caller")
    evidence = {"as_admin": admin, "as_lowpriv_caller": caller}

    if not (admin["invocation"]["succeeded"] and caller["invocation"]["succeeded"]):
        write_result(
            ROW,
            question=QUESTION,
            status="inconclusive",
            finding=(
                "Could not isolate the credential context: the probe function failed "
                "to execute for at least one identity, so the sandbox never reported "
                "its environment."
            ),
            evidence=evidence,
        )
        return 0

    def has_credential(res: dict) -> bool:
        # Only credential-bearing variables count. The sandbox exports unrelated
        # DATABRICKS_* bookkeeping (e.g. DATABRICKS_ROOT_VIRTUALENV_ENV), and
        # treating any DATABRICKS_* prefix as proof of a credential would score
        # this row "pass" on a variable that cannot authenticate anything.
        report = res["udf_report"]
        creds = report.get("credential_env_vars_present") or []
        sdk_ok = report.get("sdk_default_auth") == "constructed"
        return bool(creds) or sdk_ok

    if has_credential(caller):
        write_result(
            ROW,
            question=QUESTION,
            status="pass",
            finding=(
                "The sandbox exposed a usable Databricks credential when the "
                "low-privilege caller invoked the function. See evidence for exactly "
                "which variables or auth path were available and whose identity they "
                "resolve to — an owner-scoped credential enables delegation, a "
                "caller-scoped one does not."
            ),
            evidence=evidence,
        )
    else:
        write_result(
            ROW,
            question=QUESTION,
            status="fail",
            finding=(
                "No ambient credential. The UC Python UDF sandbox exposed no "
                "DATABRICKS_* environment variables and the SDK could not construct a "
                "default-auth client, for either identity. A function body therefore "
                "has nothing to authenticate as — it cannot act on the owner's behalf "
                "even in principle. Any credential would have to be passed in "
                "explicitly by the caller, which inverts the pattern: the caller would "
                "need to already hold the privileged credential."
            ),
            evidence=evidence,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
