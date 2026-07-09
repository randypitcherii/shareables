"""Matrix row: deny_model_grant_gateway — definer's rights on the gateway.

Claim/source: Databricks serving endpoints run with definer's rights — the
endpoint OWNER needs EXECUTE on the underlying model; the CALLER only needs
CAN_QUERY on the endpoint. Docs therefore imply a principal with DENY on the
model but a GRANT on the gateway/serving endpoint can STILL infer.

✅ means: inference succeeded despite the model-level DENY — the
counterintuitive-but-DOCUMENTED definer's-rights behavior confirmed
empirically ("works as documented"). Read the notes: ✅ here does NOT mean
model-level denies gate endpoint traffic — it means the documented ownership
model held.
❌ means: the model DENY blocked the endpoint-permission-holding caller
(docs contradicted).
❓ means: untestable — TEST_PRINCIPAL unset, mutations not opted in, the DENY
or endpoint grant was refused by the API, or no TEST_PRINCIPAL_PROFILE to
query as the principal.

Needs: TEST_PRINCIPAL, ALLOW_ENDPOINT_MUTATION=1, ideally
TEST_PRINCIPAL_PROFILE. All mutations (model DENY, endpoint CAN_QUERY grant)
are reverted in a finally block (best effort) — the endpoint ACL is
snapshotted first and restored with a full-replace PUT.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _gov_common as gov  # noqa: E402
import _common  # noqa: E402

ROW_KEY = "deny_model_grant_gateway"


def main() -> int:
    _common.section("05 — DENY on model + GRANT on gateway endpoint (definer's rights)")

    model_id = _common.resolve_model_id()
    endpoint_name = gov.endpoint_tail(model_id)
    principal = gov.get_test_principal()
    profile = gov.get_test_principal_profile()
    print(f"  Model securable: {model_id}")
    print(f"  Serving endpoint: {endpoint_name}")

    if principal is None:
        gov.print_test_principal_setup()
        gov.conclude(ROW_KEY, None, "TEST_PRINCIPAL not set — untested")
        return 0

    print(f"  Target principal: {principal}")

    if not gov.mutation_allowed():
        gov.print_mutation_gate()
        print("  The mutation path would run, then revert:")
        print(f"    1. DENY EXECUTE ON MODEL|FUNCTION {model_id} TO `{principal}`")
        print(f"    2. PATCH permissions/serving-endpoints/{endpoint_name}: CAN_QUERY for {principal}")
        print("    3. Inference AS the principal — docs predict SUCCESS (definer's rights)")
        gov.conclude(
            ROW_KEY,
            None,
            f"Dry-run only: {gov.MUTATION_ENV}=1 not set — no DENY/grant "
            "attempted, definer's-rights behavior untested",
        )
        return 0

    denied = False
    acl_snapshot: list[dict] | None = None
    endpoint_id: str | None = None
    notes_parts: list[str] = []
    verdict: bool | None = None
    try:
        # --- 1. DENY EXECUTE on the model ----------------------------------
        print("  Step 1: DENY EXECUTE on the model...")
        deny_ok, deny_attempts = gov.try_privilege_statements("DENY", model_id, principal)
        denied = deny_ok
        for stmt, res in deny_attempts:
            marker = "ok" if res["ok"] else f"{res['state']}: {gov.snip(res['error'], 150)}"
            print(f"    {stmt} -> {marker}")
            notes_parts.append(f"[{stmt}] -> {'OK' if res['ok'] else gov.snip(res['error'], 150)}")
        if not deny_ok:
            _common.fail("DENY refused by the API — the definer's-rights contrast can't be set up.")
            verdict = None
            notes_parts.insert(0, "DENY on the model was refused — untestable:")
            return 0

        # --- 2. GRANT CAN_QUERY on the serving endpoint --------------------
        print("  Step 2: grant CAN_QUERY on the serving endpoint...")
        endpoint_id = gov.get_endpoint_id(endpoint_name)
        if not endpoint_id:
            _common.fail(f"Could not resolve endpoint id for '{endpoint_name}'.")
            verdict = None
            notes_parts.insert(0, f"Endpoint '{endpoint_name}' not found/resolvable — untestable:")
            return 0
        acl_status, acl_body = gov.get_endpoint_acl(endpoint_id)
        if acl_status == 200 and isinstance(acl_body, dict):
            acl_snapshot = gov.direct_acl_entries(acl_body)
        grant_status, grant_body = gov.grant_endpoint_can_query(endpoint_id, principal)
        print(f"    PATCH permissions -> HTTP {grant_status}")
        notes_parts.append(
            f"endpoint CAN_QUERY grant -> HTTP {grant_status} ({gov.snip(grant_body, 150)})"
        )
        if grant_status != 200:
            _common.fail("Endpoint permission grant refused (pay-per-token endpoints may not support per-principal ACLs).")
            verdict = None
            notes_parts.insert(0, "Endpoint CAN_QUERY grant refused — untestable:")
            return 0

        # --- 3. Inference AS the principal ---------------------------------
        if not profile:
            manual = (
                "As the second user, run: "
                f"RPW_MODEL={model_id} uv run python "
                "scripts/governance/01_query_via_gateway_sso.py — docs predict "
                "HTTP 200 despite the model DENY (definer's rights)."
            )
            print(f"  TEST_PRINCIPAL_PROFILE not set. Manual step: {manual}")
            verdict = None
            notes_parts.insert(0, f"DENY+grant in place but unverified — {manual} —")
            return 0

        with gov.as_profile(profile):
            status, body = _common.chat_completion(model_id, "Reply with: pong", max_tokens=8)
        print(f"  Step 3: inference AS {principal}: HTTP {status}")
        notes_parts.append(f"inference as principal -> HTTP {status} ({gov.snip(body, 200)})")

        if status == 200:
            _common.ok(
                "Inference SUCCEEDED despite the model-level DENY — this is the "
                "counterintuitive-but-documented definer's-rights behavior: the "
                "endpoint owner's EXECUTE is what counts; the caller only needs "
                "CAN_QUERY on the endpoint."
            )
            verdict = True
            notes_parts.insert(
                0,
                "Definer's rights confirmed as documented: model DENY + endpoint "
                "CAN_QUERY -> inference still succeeds. Governance implication: "
                "model-level denies do NOT gate endpoint traffic — endpoint "
                "permissions are the real control plane:",
            )
        elif status in (401, 403):
            _common.fail("Model DENY blocked the endpoint-permitted caller — contradicts documented definer's rights.")
            verdict = False
            notes_parts.insert(0, "Docs contradicted: model DENY blocked despite endpoint grant:")
        else:
            verdict = None
            notes_parts.insert(0, f"Ambiguous HTTP {status} — neither clean success nor auth denial:")
        return 0
    finally:
        if denied:
            print("  Cleanup: reverting model DENY...")
            ok_cleanup, attempts = gov.try_privilege_statements("REVOKE", model_id, principal)
            if ok_cleanup:
                _common.ok("Model DENY reverted.")
            else:
                _common.fail(
                    f"DENY cleanup failed — manually run: REVOKE EXECUTE ON MODEL {model_id} FROM `{principal}`"
                )
                notes_parts.append("WARNING: model DENY cleanup failed — manual revoke needed.")
        if endpoint_id and acl_snapshot is not None:
            print("  Cleanup: restoring the endpoint's original ACL...")
            restore_status, restore_body = gov.put_endpoint_acl(endpoint_id, acl_snapshot)
            if restore_status == 200:
                _common.ok("Endpoint ACL restored.")
            else:
                _common.fail(f"Endpoint ACL restore failed (HTTP {restore_status}): {gov.snip(restore_body, 150)}")
                notes_parts.append("WARNING: endpoint ACL restore failed — check permissions UI.")
        gov.conclude(ROW_KEY, verdict, " | ".join(notes_parts))


if __name__ == "__main__":
    sys.exit(main())
