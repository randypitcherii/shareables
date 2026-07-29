"""Row 11 — can http_request() reach this workspace's own control plane at all?

Rows 8-10 point at example.com on purpose, so the privilege question is answered
without a second variable in the way. This row is that second variable, measured
on its own: the whole connection family assumes a SQL statement can call the
Databricks SCIM API, and on a workspace with IP access lists enabled that
assumption is worth checking before anyone designs around it.

Two probes, because one of them alone is misleading in both directions:

  sentinel credential   answers whether the request arrives at the control plane
                        and is evaluated at all. A 401 here is a *good* result —
                        it means DNS, egress and TLS all worked and the platform
                        got as far as looking at the credential. Reading that 401
                        as "unreachable" would be trap 3 exactly.
  live credential       answers whether an authenticated request from this egress
                        address is then allowed. This is where an IP access list
                        speaks, and it cannot speak until authentication has
                        already succeeded.

Only the second probe needs a real token, and it is the author's own short-lived
one. It is registered with the results writer so a platform error that quotes it
cannot carry it into the repo, and the connection holding it is dropped before
this script returns.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import (  # noqa: E402
    CONNECTION_SCIM_NAME,
    admin_client,
    fail,
    info,
    ok,
    register_secret,
    run_sql,
    run_sql_or_die,
    section,
    step,
    write_result,
)
from _delegation_common import http_request_outcome  # noqa: E402

ROW = "11"
QUESTION = (
    "Can http_request() reach this workspace's own SCIM API, or does the network "
    "perimeter stop it first?"
)

LIVE_CONNECTION = "udf_delegation_scim_live_conn"

# Markers that say an *authenticated* request was refused by the perimeter rather
# than by authentication. An invalid-token answer names the credential; these
# name the caller's address.
_PERIMETER_MARKERS = ("Source IP address", "IP access", "ip_access")
_AUTH_MARKERS = ("Credential was not sent", "invalid access token", "Invalid access token")


def _probe(w, connection: str) -> dict:
    return http_request_outcome(
        w,
        f"SELECT to_json(http_request(conn => '{connection}', method => 'GET', path => 'Me'))",
    )


def main() -> int:
    section(f"row {ROW}: is the control plane reachable from http_request()?")
    admin = admin_client()

    step("probe 1: sentinel credential — does the request reach authentication?")
    sentinel = _probe(admin, CONNECTION_SCIM_NAME)
    sentinel_body = sentinel["body"] or sentinel["error"] or ""
    sentinel["reached_authentication"] = any(m in sentinel_body for m in _AUTH_MARKERS)
    if sentinel["reached_authentication"]:
        ok(
            f"the control plane answered {sentinel['status_code']} about the credential "
            "— the request completed the round trip"
        )
    else:
        info(f"unexpected answer to the sentinel probe: {sentinel_body[:200]}")

    token = admin.config.authenticate()["Authorization"].removeprefix("Bearer ")
    register_secret(token)

    live: dict = {}
    try:
        step("probe 2: live credential — is an authenticated request from here allowed?")
        run_sql(admin, f"DROP CONNECTION IF EXISTS `{LIVE_CONNECTION}`")
        run_sql_or_die(
            admin,
            f"""
CREATE CONNECTION `{LIVE_CONNECTION}` TYPE HTTP
OPTIONS (
  host '{admin.config.host.rstrip("/")}',
  port '443',
  base_path '/api/2.0/preview/scim/v2/',
  bearer_token '{token}'
)
""",
        )
        live = _probe(admin, LIVE_CONNECTION)
    finally:
        run_sql(admin, f"DROP CONNECTION IF EXISTS `{LIVE_CONNECTION}`")
        ok("live-credential connection dropped")

    live_body = live.get("body") or live.get("error") or ""
    live["refused_by_network_perimeter"] = any(m in live_body for m in _PERIMETER_MARKERS)

    if live.get("allowed"):
        ok(f"authenticated call allowed — the control plane answered {live['status_code']}")
    elif live["refused_by_network_perimeter"]:
        info(f"authenticated call refused at the perimeter (status_code {live['status_code']})")
    else:
        fail(f"refused, but not by the perimeter: {live_body[:200]}")

    evidence = {"sentinel_credential_probe": sentinel, "live_credential_probe": live}

    if live.get("allowed"):
        write_result(
            ROW,
            question=QUESTION,
            status="pass",
            finding=(
                "Reachable. An authenticated request from the address http_request() "
                "egresses on was allowed and the SCIM API answered, so in this "
                "environment the connection family's viability turns on the privilege "
                "questions in rows 8-10 rather than on connectivity."
            ),
            evidence=evidence,
        )
    elif live["refused_by_network_perimeter"]:
        write_result(
            ROW,
            question=QUESTION,
            status="fail",
            finding=(
                "Reached, then refused. The sentinel probe came back as an answer "
                f"about the credential ({sentinel['status_code']}), which establishes "
                "the request completes the round trip — this is not a firewall or DNS "
                "failure. The same call with a valid credential was then refused "
                f"({live['status_code']}) by the workspace's IP access list, naming the "
                "egress address http_request() presents. So on an IP-access-listed "
                "workspace, a UC connection pointed at the workspace that hosts it is "
                "a call that leaves and re-enters the perimeter, and the perimeter "
                "gets a vote — independently of every privilege question. "
                "Environment-dependent: a workspace without IP access lists, or one "
                "whose list covers serverless egress, would not see this."
            ),
            evidence=evidence,
        )
    else:
        write_result(
            ROW,
            question=QUESTION,
            status="inconclusive",
            finding=(
                "The authenticated call did not succeed and its refusal could not be "
                "attributed to the network perimeter, so this row cannot separate a "
                "reachability problem from an authorization one. The raw responses "
                "are in the evidence."
            ),
            evidence=evidence,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
