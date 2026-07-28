"""Row 1 — does a UC SQL UDF body resolve underlying privileges as definer or invoker?

This is the load-bearing question for the whole pattern. If the function body
runs with the *owner's* privileges, an admin-authored function is a real
privilege boundary and narrow delegation is possible. If it runs with the
*caller's*, a function can never grant access the caller does not already have.

Design: the caller holds EXECUTE on f_sql_read_sensitive and no privilege at
all on the table its body reads. Two statements, run as the caller:

  A. direct  SELECT count(*) FROM sensitive_table   -- must fail (control)
  B. wrapped SELECT f_sql_read_sensitive()          -- the actual question

A must fail for B to mean anything, so A is asserted, not assumed.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common import (  # noqa: E402
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

ROW = "1"
QUESTION = "Does a UC SQL UDF body run with definer (owner) privileges on objects the caller cannot access?"


def main() -> int:
    section(f"row {ROW}: SQL UDF — definer vs invoker rights")
    lw = lowpriv_client()
    admin_who, caller_who = assert_distinct_identities(admin_client(), lw)
    info(f"author={admin_who}  caller={caller_who}")

    step("control A: caller reads sensitive_table directly (expected: denied)")
    direct = run_sql(lw, f"SELECT count(*) FROM {FQ_SCHEMA}.sensitive_table")
    if direct.succeeded:
        fail(
            "the caller CAN read sensitive_table directly — the scenario is not "
            "set up correctly and rows 1-2 would be meaningless"
        )
        write_result(
            ROW,
            question=QUESTION,
            status="inconclusive",
            finding=(
                "Could not isolate the question: the low-privilege caller already had "
                "read access to the underlying table, so a successful call through the "
                "function would not have demonstrated definer's rights. Re-provision "
                "with setup/01 and confirm SHOW GRANTS returns nothing."
            ),
            evidence={"control_direct_read": direct.summary()},
        )
        return 1
    ok(f"denied as required: {direct.error_code}")
    info(f"error: {direct.error}")

    step("B: caller invokes the admin-owned SQL UDF that reads the same table")
    wrapped = run_sql(lw, f"SELECT {FQ_SCHEMA}.f_sql_read_sensitive()")

    evidence = {
        "control_direct_read": direct.summary(),
        "function_invocation": wrapped.summary(),
    }

    if wrapped.succeeded:
        ok(f"function returned {wrapped.scalar} — the body read a table the caller cannot")
        write_result(
            ROW,
            question=QUESTION,
            status="pass",
            finding=(
                "Definer's rights. The caller was denied a direct read of "
                "sensitive_table but successfully invoked an admin-owned SQL UDF "
                f"whose body reads it, receiving {wrapped.scalar}. A SQL UDF is "
                "therefore a genuine privilege boundary for data access: EXECUTE on "
                "the function is sufficient and the caller never gains the "
                "underlying SELECT."
            ),
            evidence=evidence,
        )
    else:
        fail(f"function invocation also denied: {wrapped.error_code}")
        info(f"error: {wrapped.error}")
        write_result(
            ROW,
            question=QUESTION,
            status="fail",
            finding=(
                "Invoker's rights. The caller holds EXECUTE on the function but the "
                "invocation was refused with "
                f"{wrapped.error_code}, meaning the body's object references are "
                "authorized against the caller, not the owner. A SQL UDF cannot "
                "elevate access to objects the caller lacks."
            ),
            evidence=evidence,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
