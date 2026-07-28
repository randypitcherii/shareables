"""Row 5 — is there any SQL surface for identity administration a UDF could call?

Rows 2-4 fail through the network path. This row checks the other possible
path: if creating a principal were expressible in SQL, a SQL UDF (which row 1
shows *does* carry definer's rights) could wrap it and the pattern would work
without any egress at all.

So: does the SQL dialect accept identity-administration DDL? Probed as the
admin, because if the grammar does not exist for an admin it does not exist for
anyone. A PARSE_SYNTAX_ERROR is the finding — it means the operation lives
outside SQL entirely.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common import (  # noqa: E402
    admin_client,
    fail,
    info,
    ok,
    run_sql,
    section,
    step,
    write_result,
)

ROW = "5"
QUESTION = "Does any SQL surface exist for creating a principal, such that a definer-rights SQL UDF could wrap it?"

CANDIDATES = {
    "create_service_principal": "CREATE SERVICE PRINCIPAL `udf-delegation-probe`",
    "create_user": "CREATE USER `udf-delegation-probe`",
    "create_login": "CREATE LOGIN `udf-delegation-probe`",
}


def main() -> int:
    section(f"row {ROW}: SQL surface for identity administration")
    w = admin_client()

    evidence = {}
    any_accepted = False
    for name, stmt in CANDIDATES.items():
        step(f"probing: {stmt}")
        outcome = run_sql(w, stmt)
        evidence[name] = {"statement": stmt, "outcome": outcome.summary()}
        if outcome.succeeded:
            any_accepted = True
            ok("accepted by the SQL parser")
        else:
            fail(f"rejected: {outcome.error_code}")
            info(f"error: {outcome.error}")

    if any_accepted:
        write_result(
            ROW,
            question=QUESTION,
            status="pass",
            finding=(
                "At least one identity-administration statement is expressible in "
                "SQL, so a definer-rights SQL UDF could wrap it. See evidence for "
                "which grammar was accepted."
            ),
            evidence=evidence,
        )
    else:
        write_result(
            ROW,
            question=QUESTION,
            status="fail",
            finding=(
                "No SQL surface. Every identity-administration statement was rejected "
                "by the parser even for a workspace admin, so principal creation is "
                "reachable only over the SCIM REST API. The one function type that "
                "carries definer's rights (SQL UDFs, row 1) therefore cannot express "
                "the action, and the one that could express it (Python UDFs) has "
                "neither egress nor a credential (rows 2-3). The two halves of the "
                "pattern never meet."
            ),
            evidence=evidence,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
