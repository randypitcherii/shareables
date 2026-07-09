"""Matrix row: revoke_gateway_no_fallback — revocation has no legacy leak.

Claim/source: field reports of a leak where revoking a principal's gateway /
serving-endpoint permission does not stop them: the workspace's LEGACY
pay-per-token endpoints (plain `databricks-claude-*` names under
/serving-endpoints) keep serving that principal, so access silently falls
back around the revocation. This script is the hard repro.

✅ means: revocation is airtight — after removing the principal's endpoint
permission, BOTH (a) gateway inference with the three-part id and (b) direct
calls to the legacy pay-per-token endpoints are refused for that principal.
❌ means: the leak reproduces — the principal can still infer via legacy
endpoints after the gateway revoke.
❓ means: untestable — TEST_PRINCIPAL unset, mutations not opted in, or no
TEST_PRINCIPAL_PROFILE to run the as-principal probes (instructions are
printed and recorded).

Needs: TEST_PRINCIPAL, ALLOW_ENDPOINT_MUTATION=1 (the revoke is a permission
mutation), TEST_PRINCIPAL_PROFILE for the as-principal probes. The endpoint
ACL is snapshotted before the revoke and restored in a finally block.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _gov_common as gov  # noqa: E402
import _common  # noqa: E402

ROW_KEY = "revoke_gateway_no_fallback"


def matches_principal(acl_entry: dict, principal: str) -> bool:
    return principal in (
        acl_entry.get("user_name"),
        acl_entry.get("group_name"),
        acl_entry.get("service_principal_name"),
    )


def main() -> int:
    _common.section("06 — Gateway revoke leaves NO legacy pay-per-token fallback")

    model_id = _common.resolve_model_id()
    endpoint_name = gov.endpoint_tail(model_id)
    principal = gov.get_test_principal()
    profile = gov.get_test_principal_profile()
    print(f"  Gateway model id: {model_id}")
    print(f"  Legacy endpoint name under test: {endpoint_name}")

    if principal is None:
        gov.print_test_principal_setup()
        gov.conclude(ROW_KEY, None, "TEST_PRINCIPAL not set — untested")
        return 0

    print(f"  Target principal: {principal}")

    if not gov.mutation_allowed():
        gov.print_mutation_gate()
        print("  The mutation path would run, then restore the original ACL:")
        print(f"    1. Remove {principal}'s direct permission entries on serving endpoint '{endpoint_name}'")
        print(f"    2. AS the principal: chat_completion('{model_id}')  # expect 401/403")
        print("    3. AS the principal: GET /api/2.0/serving-endpoints (enumerate legacy databricks-* endpoints)")
        print(f"    4. AS the principal: POST /serving-endpoints/{endpoint_name}/invocations  # leak if 200")
        gov.conclude(
            ROW_KEY,
            None,
            f"Dry-run only: {gov.MUTATION_ENV}=1 not set — revoke-then-probe "
            "sequence not executed, leak repro untested",
        )
        return 0

    endpoint_id: str | None = None
    acl_snapshot: list[dict] | None = None
    notes_parts: list[str] = []
    verdict: bool | None = None
    try:
        # --- 1. Revoke: replace ACL without the principal's entries --------
        print("  Step 1: revoking the principal's direct endpoint permissions...")
        endpoint_id = gov.get_endpoint_id(endpoint_name)
        if not endpoint_id:
            _common.fail(f"Could not resolve endpoint id for '{endpoint_name}'.")
            verdict = None
            notes_parts.insert(0, f"Endpoint '{endpoint_name}' not found — untestable:")
            return 0
        acl_status, acl_body = gov.get_endpoint_acl(endpoint_id)
        if acl_status != 200 or not isinstance(acl_body, dict):
            _common.fail(f"Could not read endpoint ACL (HTTP {acl_status}).")
            verdict = None
            notes_parts.insert(0, f"ACL read failed HTTP {acl_status} ({gov.snip(acl_body, 150)}) — untestable:")
            return 0
        original_entries = gov.direct_acl_entries(acl_body)
        stripped_entries = [e for e in original_entries if not matches_principal(e, principal)]
        had_direct = len(stripped_entries) != len(original_entries)
        print(f"    Principal had direct entries: {had_direct}")
        if had_direct:
            acl_snapshot = original_entries  # only restore if we changed something
            put_status, put_body = gov.put_endpoint_acl(endpoint_id, stripped_entries)
            print(f"    PUT stripped ACL -> HTTP {put_status}")
            notes_parts.append(f"revoke (PUT stripped ACL) -> HTTP {put_status}")
            if put_status != 200:
                _common.fail(f"Revoke PUT failed: {gov.snip(put_body, 150)}")
                verdict = None
                notes_parts.insert(0, "Could not revoke the endpoint permission — untestable:")
                return 0
        else:
            notes_parts.append(
                "principal had no direct endpoint permission entries (revoked "
                "state already in effect — any remaining access is inherited/workspace-default)"
            )

        # --- 2-4. Probe AS the principal ------------------------------------
        if not profile:
            manual = (
                "As the second user: (a) RPW_MODEL="
                f"{model_id} uv run python scripts/governance/01_query_via_gateway_sso.py "
                "(expect 401/403); (b) databricks serving-endpoints list --profile <their-profile> "
                f"and POST /serving-endpoints/{endpoint_name}/invocations with their OAuth "
                "token — HTTP 200 there reproduces the legacy-fallback leak."
            )
            print("  TEST_PRINCIPAL_PROFILE not set — cannot probe as the principal.")
            print(f"  Manual steps: {manual}")
            verdict = None
            notes_parts.insert(0, f"Revoke done but probes need the second identity — {manual} —")
            return 0

        with gov.as_profile(profile):
            principal_token = _common.get_token()
            # (a) gateway route with the three-part id
            gw_status, gw_body = _common.chat_completion(model_id, "Reply with: pong", max_tokens=8)
        print(f"  (a) gateway three-part inference as principal -> HTTP {gw_status}")
        notes_parts.append(f"gateway probe -> HTTP {gw_status} ({gov.snip(gw_body, 150)})")

        # (b) enumerate + hit legacy pay-per-token endpoints as the principal
        list_status, list_body = gov.api_request(
            "GET", "/api/2.0/serving-endpoints", token=principal_token
        )
        legacy_names: list[str] = []
        if list_status == 200 and isinstance(list_body, dict):
            legacy_names = [
                e.get("name", "")
                for e in list_body.get("endpoints", [])
                if e.get("name", "").startswith("databricks-")
            ]
        print(f"  (b) legacy endpoint enumeration as principal -> HTTP {list_status}, {len(legacy_names)} databricks-* endpoints visible")
        notes_parts.append(f"legacy enumeration -> HTTP {list_status}, visible={legacy_names[:5]}")

        probe_targets = [endpoint_name] + [n for n in legacy_names if n != endpoint_name][:2]
        leaked: list[str] = []
        for name in probe_targets:
            inv_status, inv_body = gov.api_request(
                "POST",
                f"/serving-endpoints/{name}/invocations",
                payload={"messages": [{"role": "user", "content": "Reply with: pong"}], "max_tokens": 8},
                token=principal_token,
            )
            print(f"      legacy invocations '{name}' -> HTTP {inv_status}")
            notes_parts.append(f"legacy '{name}' -> HTTP {inv_status}")
            if inv_status == 200:
                leaked.append(name)

        gateway_blocked = gw_status in (401, 403)
        if leaked:
            _common.fail(
                f"LEAK REPRODUCED: principal still infers via legacy endpoint(s) {leaked} after the gateway revoke."
            )
            verdict = False
            notes_parts.insert(0, f"Leak reproduced — legacy fallback endpoints {leaked} still serve the revoked principal:")
        elif gateway_blocked:
            _common.ok("Revocation is airtight: gateway blocked AND no legacy endpoint served the principal.")
            verdict = True
            notes_parts.insert(0, "No fallback: gateway 401/403 and all legacy probes refused:")
        elif gw_status == 200:
            _common.fail("Gateway itself still served the principal after revoke (workspace-default/inherited access?).")
            verdict = False
            notes_parts.insert(0, "Gateway still serves the revoked principal (inherited/default access leak):")
        else:
            verdict = None
            notes_parts.insert(0, f"Ambiguous: gateway HTTP {gw_status}, no legacy 200s:")
        return 0
    finally:
        if endpoint_id and acl_snapshot is not None:
            print("  Cleanup: restoring the endpoint's original ACL...")
            restore_status, restore_body = gov.put_endpoint_acl(endpoint_id, acl_snapshot)
            if restore_status == 200:
                _common.ok("Endpoint ACL restored.")
            else:
                _common.fail(f"ACL restore failed (HTTP {restore_status}): {gov.snip(restore_body, 150)}")
                notes_parts.append("WARNING: endpoint ACL restore failed — check permissions UI.")
        gov.conclude(ROW_KEY, verdict, " | ".join(notes_parts))


if __name__ == "__main__":
    sys.exit(main())
