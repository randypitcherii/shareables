"""Row 6 — can a caller holding only EXECUTE read the function's body?

Rows 2-4 show the sandbox has network reach but no identity. The obvious next
move for anyone determined to make the pattern work is for the author to
hardcode a credential into the function body. That only stays a *delegation* if
the body is opaque to the caller: if EXECUTE also confers the ability to read
the source, the "wrapper" hands every caller the admin credential in plaintext
and is strictly worse than granting the privilege directly.

A sentinel string stands in for the credential here — the question is whether
the body is readable, and a real token is not needed to answer it (and must not
be written into a public repo's evidence).

Every plausible read path the caller could try is probed, because "DESCRIBE was
blocked" would not be a finding if information_schema still exposed the text.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common import (  # noqa: E402
    CATALOG,
    FQ_SCHEMA,
    SCHEMA,
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

ROW = "6"
QUESTION = "Can a caller holding only EXECUTE read the function body, and therefore any credential embedded in it?"

SENTINEL = "SENTINEL-NOT-A-REAL-SECRET-9f3a2c"
FN = "f_py_with_embedded_sentinel"


def main() -> int:
    section(f"row {ROW}: function body readability")
    admin = admin_client()
    caller = lowpriv_client()
    admin_who, caller_who = assert_distinct_identities(admin, caller)
    info(f"author={admin_who}  caller={caller_who}")

    step(f"admin creates {FN} with a sentinel embedded in the body")
    run_sql_or_die(
        admin,
        f"""
CREATE OR REPLACE FUNCTION {FQ_SCHEMA}.{FN}()
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Body contains a sentinel standing in for a hardcoded credential.'
AS $$
embedded = "{SENTINEL}"
return "ran"
$$
""",
    )
    run_sql_or_die(admin, f"GRANT EXECUTE ON FUNCTION {FQ_SCHEMA}.{FN} TO `{caller_who}`")
    ok("function created; caller granted EXECUTE only")

    read_paths = {
        "describe_function_extended": f"DESCRIBE FUNCTION EXTENDED {FQ_SCHEMA}.{FN}",
        "show_create_table": f"SHOW CREATE TABLE {FQ_SCHEMA}.{FN}",
        "information_schema_routines": (
            "SELECT routine_definition FROM "
            f"`{CATALOG}`.information_schema.routines "
            f"WHERE routine_schema = '{SCHEMA}' AND routine_name = '{FN}'"
        ),
    }

    evidence = {}
    leaked_via = []
    for name, stmt in read_paths.items():
        step(f"caller attempts: {name}")
        outcome = run_sql(caller, stmt)
        blob = " ".join(" ".join(str(c) for c in row) for row in outcome.rows)
        leaked = SENTINEL in blob
        evidence[name] = {
            "statement": stmt,
            "succeeded": outcome.succeeded,
            "error_code": outcome.error_code,
            "error": outcome.error,
            "sentinel_visible_to_caller": leaked,
        }
        if leaked:
            leaked_via.append(name)
            fail("sentinel is visible to the caller through this path")
        elif outcome.succeeded:
            ok("statement allowed, but the body text was not exposed")
        else:
            ok(f"refused: {outcome.error_code}")

    step("confirming the caller can still invoke it (EXECUTE really is held)")
    invoked = run_sql(caller, f"SELECT {FQ_SCHEMA}.{FN}()")
    evidence["invocation_control"] = invoked.summary()
    if invoked.succeeded:
        ok(f"invocation returned {invoked.scalar!r}")
    else:
        fail(f"caller cannot invoke: {invoked.error_code}")

    if leaked_via:
        write_result(
            ROW,
            question=QUESTION,
            status="fail",
            finding=(
                "The function body is readable by a caller holding only EXECUTE, via "
                f"{', '.join(leaked_via)}. A credential hardcoded into the body is "
                "therefore disclosed to exactly the population the wrapper was meant "
                "to keep it from — the workaround is not a delegation boundary, it is "
                "a credential handout with extra steps."
            ),
            evidence=evidence,
        )
    elif not invoked.succeeded:
        write_result(
            ROW,
            question=QUESTION,
            status="inconclusive",
            finding=(
                "Could not isolate the question: the caller could not invoke the "
                "function, so it is not established that it held EXECUTE at the time "
                "the read paths were probed."
            ),
            evidence=evidence,
        )
    else:
        write_result(
            ROW,
            question=QUESTION,
            status="pass",
            finding=(
                "The body is opaque to the caller. EXECUTE permitted invocation but "
                "none of the probed read paths (DESCRIBE FUNCTION EXTENDED, SHOW "
                "CREATE TABLE, information_schema.routines) exposed the sentinel, so "
                "a credential embedded in the body is not disclosed to callers by "
                "these paths. Note this is a negative over the paths actually "
                "probed — it is not a proof that no read path exists."
            ),
            evidence=evidence,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
