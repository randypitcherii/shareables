"""Row 10 — how wide is USE CONNECTION? What does the grant actually authorise?

Row 8 forced the caller to hold USE CONNECTION directly, and row 9 established
that holding it does not disclose the credential. Those two together define the
only usable shape for the connection family: the caller is granted the
connection, and the author hopes a wrapper function keeps the caller to one
action.

This row asks what that grant is actually scoped to. The author's function fixes
a method (GET) and a path (the connection root). If USE CONNECTION is scoped to
the *connection* rather than to the author's chosen call, the caller can ignore
the function entirely and issue any request under the connection's base_path —
and every shape constraint the author thought they were enforcing is decoration.

The distinction the probes turn on is the one trap 3 records: a status code from
the origin is proof the platform let the request out. A 404 from example.com for
a path the author never blessed is a *stronger* result than a 200, because it
can only have come from the far end.

The last probe is the constraint that does exist — the connection pins one host
and one base_path, and the documentation says traversal above it is rejected.
Measuring where the boundary really is matters as much as measuring that it is
too wide.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import (  # noqa: E402
    CONNECTION_NAME,
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

ROW = "10"
QUESTION = (
    "Is USE CONNECTION scoped to the call the author's function makes, or to the "
    "whole connection?"
)


def _call(method: str, path: str) -> str:
    return (
        f"SELECT to_json(http_request(conn => '{CONNECTION_NAME}', "
        f"method => '{method}', path => '{path}'))"
    )


# The author's function makes exactly one of these. Every other entry is a call
# the author never wrote and never intended to expose.
PROBES = {
    "author_blessed_call__GET_root": ("GET", ""),
    "unblessed_path__GET_other": ("GET", "some/other/resource"),
    "unblessed_method__POST_root": ("POST", ""),
    "unblessed_method__DELETE_root": ("DELETE", ""),
    "escape_above_base_path": ("GET", "../../etc/passwd"),
}


def main() -> int:
    section(f"row {ROW}: what does USE CONNECTION authorise?")
    admin = admin_client()
    caller = lowpriv_client()
    admin_who, caller_who = assert_distinct_identities(admin, caller)
    info(f"author={admin_who}  caller={caller_who}")
    info("the caller holds USE CONNECTION, granted by row 9")

    evidence: dict[str, object] = {}
    reached_origin = []
    refused_by_uc = []
    blocked_by_validation = []

    for label, (method, path) in PROBES.items():
        step(f"caller attempts: {method} {path or '<connection root>'}")
        result = http_request_outcome(caller, _call(method, path))
        result["method"] = method
        result["path"] = path
        evidence[label] = result

        if "INVALID_HTTP_REQUEST_PATH" in (result["error"] or ""):
            # Not an authorization decision — http_request() rejects the argument
            # before any privilege is consulted. Worth separating: it is the one
            # place the connection's base_path really does bound the caller.
            blocked_by_validation.append(label)
            result["refusal_kind"] = "input validation (path traversal)"
            ok("rejected before dispatch: path traversal above base_path is not allowed")
        elif result["denied_by_unity_catalog"]:
            refused_by_uc.append(label)
            result["refusal_kind"] = "unity catalog authorization"
            ok("refused by Unity Catalog")
        elif result["statement_succeeded"] and result["status_code"]:
            # Any status code from the far end means Unity Catalog let it out.
            reached_origin.append(label)
            info(
                f"permitted — the origin answered {result['status_code']}, so the "
                "platform allowed a call the author never wrote"
            )
        else:
            fail(f"neither a refusal nor an origin response: {result}")

    unblessed_reached = [x for x in reached_origin if not x.startswith("author_blessed")]

    if unblessed_reached:
        write_result(
            ROW,
            question=QUESTION,
            status="fail",
            finding=(
                "USE CONNECTION is scoped to the connection, not to the author's "
                f"call. Holding it, the caller reached the origin on {len(unblessed_reached)} "
                "request shapes the author's function never makes "
                f"({', '.join(unblessed_reached)}) — different paths and different "
                "methods, each answered by the far end rather than refused by the "
                "platform. The wrapper function's fixed method and path are therefore "
                "not a constraint on the caller; they are a convention the caller can "
                "decline to follow. The one boundary the connection does enforce is "
                "its host and base_path"
                + (
                    " — the probe that tried to climb above base_path was rejected "
                    "outright with INVALID_HTTP_REQUEST_PATH, before any privilege "
                    "was consulted."
                    if blocked_by_validation
                    else "."
                )
            ),
            evidence=evidence,
        )
    else:
        write_result(
            ROW,
            question=QUESTION,
            status="pass",
            finding=(
                "USE CONNECTION did not authorise any request shape beyond the one "
                "the author's function makes: every unblessed method and path was "
                "refused by the platform. On this evidence the connection is scoped "
                "narrowly enough to carry the author's constraints."
            ),
            evidence=evidence,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
