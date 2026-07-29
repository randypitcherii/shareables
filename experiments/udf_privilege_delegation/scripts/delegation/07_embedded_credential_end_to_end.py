"""Row 7 — does an author-embedded credential make the delegated action work?

Rows 2-4 establish that the sandbox can reach the network but has no identity
of its own. This row closes the loop on the only remaining construction: the
author embeds their own credential in the function body, so the body
authenticates as the author no matter who invokes it.

If this works, the honest answer to "can a UDF wrap one admin action?" is
"mechanically yes, by hardcoding a credential" — and row 6 decides whether that
is a security boundary or a leak.

Credential hygiene, because this repo is public:
  - the token is fetched at runtime from the caller's own OAuth session and is
    short-lived; it is never a PAT
  - the function DDL containing it is never printed and never written to
    results/
  - the function is dropped at the end of this script, pass or fail
Nothing in the committed evidence contains the token — only booleans, HTTP
status codes, and the SCIM directory verdict.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _delegation_common import call_function, decode_json_scalar  # noqa: E402

from _common import (  # noqa: E402
    FQ_SCHEMA,
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

ROW = "7"
QUESTION = "Does embedding the author's credential in the function body make the delegated admin action succeed for a low-privilege caller?"

FN = "f_py_create_sp_embedded"
TARGET_NAME = "udf-delegation-target-sp-embedded"


def author_bearer_token(w) -> str:
    """The author's own short-lived OAuth access token."""
    headers = w.config.authenticate()
    auth = headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise SystemExit(
            "Could not obtain a bearer token for the author identity; this row "
            "requires an OAuth session."
        )
    return auth[len("Bearer ") :]


def directory_contains(w, display_name: str) -> bool:
    return any(w.service_principals.list(filter=f'displayName eq "{display_name}"'))


def build_ddl(host: str, token: str) -> str:
    """Never printed, never logged, never written to results/."""
    return f"""
CREATE OR REPLACE FUNCTION {FQ_SCHEMA}.{FN}(name STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Author-embedded credential. Dropped at the end of row 7.'
AS $$
import json, urllib.request, urllib.error
HOST = "{host}"
TOKEN = "{token}"
body = json.dumps({{
    "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"],
    "displayName": name,
    "active": True,
}}).encode()
req = urllib.request.Request(
    HOST.rstrip("/") + "/api/2.0/preview/scim/v2/ServicePrincipals",
    data=body, method="POST",
    headers={{"Authorization": "Bearer " + TOKEN, "Content-Type": "application/json"}},
)
try:
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.dumps({{"outcome": "created", "status": resp.status}})
except urllib.error.HTTPError as exc:
    return json.dumps({{"outcome": "http_error", "status": exc.code}})
except Exception as exc:
    return json.dumps({{"outcome": "failed", "error_type": type(exc).__name__}})
$$
"""


def main() -> int:
    section(f"row {ROW}: author-embedded credential, end to end")
    admin = admin_client()
    caller = lowpriv_client()
    admin_who, caller_who = assert_distinct_identities(admin, caller)
    info(f"author={admin_who}  caller={caller_who}")

    step("pre-check: target must not already exist")
    if directory_contains(admin, TARGET_NAME):
        fail("target already exists — run `make teardown` first")
        write_result(
            ROW,
            question=QUESTION,
            status="inconclusive",
            finding=(
                f"Could not isolate the result: {TARGET_NAME!r} already existed "
                "before the call."
            ),
            evidence={"precheck": "target already present"},
        )
        return 1
    ok("directory is clean")

    try:
        step("author creates the wrapper with their own short-lived token embedded")
        run_sql_or_die(admin, build_ddl(admin.config.host, author_bearer_token(admin)))
        run_sql_or_die(
            admin, f"GRANT EXECUTE ON FUNCTION {FQ_SCHEMA}.{FN} TO `{caller_who}`"
        )
        ok("wrapper created; caller granted EXECUTE only (DDL deliberately not logged)")

        step("low-privilege caller invokes the wrapper")
        outcome = call_function(caller, FN, TARGET_NAME)
        decoded = decode_json_scalar(outcome)
        info(f"function returned: {decoded}")

        step("verdict: reading the SCIM directory back as admin")
        created = directory_contains(admin, TARGET_NAME)

        evidence = {
            "invocation": outcome.summary(),
            "udf_report": decoded,
            "directory_contains_target_after_call": created,
            "credential_source": "author's short-lived OAuth access token, embedded in the body",
        }

        if created:
            ok(f"{TARGET_NAME} exists — the caller performed an admin action")
            write_result(
                ROW,
                question=QUESTION,
                status="pass",
                finding=(
                    "It works, by embedding a credential. A caller holding only "
                    "EXECUTE created a service principal through an admin-authored "
                    "Python UDF whose body carries the author's token. The action is "
                    "genuinely delegated and genuinely narrow — the caller can pass "
                    "only a display name. But the elevation comes from a stored "
                    "secret, not from the function model: the platform never "
                    "re-authorizes the body as the owner, it simply replays whatever "
                    "credential the author left in the source. Row 6 decides whether "
                    "that secret stays hidden from the caller, and the token expires, "
                    "so the wrapper silently stops working when it does."
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
                    "Even with the author's credential embedded in the body, the "
                    "delegated action did not complete. See evidence.udf_report for "
                    "the status the control plane returned."
                ),
                evidence=evidence,
            )
    finally:
        step("dropping the function so no credential is left in the metastore")
        dropped = run_sql(admin, f"DROP FUNCTION IF EXISTS {FQ_SCHEMA}.{FN}")
        ok("function dropped") if dropped.succeeded else fail(f"drop failed: {dropped.error}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
