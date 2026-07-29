"""Row 2 — can a UC Python UDF make an outbound HTTPS request?

Creating a service principal is a SCIM REST operation. If the Python UDF
sandbox has no network egress, then no UDF can perform that action regardless
of who owns the function or how the privilege model resolves — the pattern is
blocked on capability, not on authorization.

Two targets are probed so a failure can be attributed:
  - the workspace's own control plane (same-origin, most likely to be allowed)
  - a public internet endpoint (broadest egress)

This row is a capability question, not an authorization question, so it is run
as BOTH identities: if the admin cannot reach the network either, the sandbox
is the constraint rather than the caller's privileges.
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

ROW = "2"
QUESTION = "Can a UC Python UDF make an outbound HTTPS request (the prerequisite for any REST-based admin action)?"


def probe(w, label: str) -> dict:
    results = {}
    host = w.config.host.rstrip("/")
    targets = {
        "workspace_control_plane": f"{host}/api/2.0/preview/scim/v2/Me",
        "public_internet": "https://example.com",
    }
    for name, url in targets.items():
        step(f"[{label}] f_py_egress -> {name}")
        outcome = call_function(w, "f_py_egress", url)
        decoded = decode_json_scalar(outcome)
        results[name] = {"invocation": outcome.summary(), "udf_report": decoded}
        if not outcome.succeeded:
            fail(f"invocation itself failed: {outcome.error_code}")
            info(f"error: {outcome.error}")
        elif decoded.get("reached"):
            ok(f"reached {name} (HTTP {decoded.get('status')})")
        else:
            fail(f"{name} unreachable: {decoded.get('error_type')}: {decoded.get('error')}")
    return results


def main() -> int:
    section(f"row {ROW}: UC Python UDF — network egress")

    admin_w, caller_w = admin_client(), lowpriv_client()
    admin_who, caller_who = assert_distinct_identities(admin_w, caller_w)
    info(f"author={admin_who}  caller={caller_who}")

    admin = probe(admin_w, "admin")
    caller = probe(caller_w, "low-priv caller")

    evidence = {"as_admin": admin, "as_lowpriv_caller": caller}

    def reached(res: dict) -> bool:
        return any(v["udf_report"].get("reached") for v in res.values())

    invocable = all(
        v["invocation"]["succeeded"] for v in list(admin.values()) + list(caller.values())
    )

    if not invocable:
        write_result(
            ROW,
            question=QUESTION,
            status="inconclusive",
            finding=(
                "Could not isolate egress: at least one f_py_egress invocation failed "
                "before the UDF body ran, so the sandbox's network behaviour was never "
                "exercised. See evidence.invocation error codes."
            ),
            evidence=evidence,
        )
        return 0

    if reached(admin) or reached(caller):
        detail = ", ".join(
            f"{target} -> HTTP {res['udf_report'].get('status')}"
            if res["udf_report"].get("reached")
            else f"{target} -> unreachable ({res['udf_report'].get('error_type')})"
            for target, res in caller.items()
        )
        write_result(
            ROW,
            question=QUESTION,
            status="pass",
            finding=(
                "The UC Python UDF sandbox has outbound network access. As the "
                f"low-privilege caller: {detail}. A REST-based administrative action "
                "is therefore mechanically reachable from inside a function body — "
                "egress is not the constraint. Whether the request can be "
                "*authenticated* is row 3."
            ),
            evidence=evidence,
        )
    else:
        write_result(
            ROW,
            question=QUESTION,
            status="fail",
            finding=(
                "No egress. Every outbound HTTPS request from inside the UC Python "
                "UDF sandbox failed, for both the owning admin and the low-privilege "
                "caller, against both the workspace control plane and the public "
                "internet. Because creating a service principal has no SQL surface "
                "and can only be done over REST, this blocks the wrapper pattern for "
                "that action on capability grounds — before authorization is even "
                "considered."
            ),
            evidence=evidence,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
