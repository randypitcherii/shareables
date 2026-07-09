"""Matrix row: deny_execute_blocks_inference — DENY EXECUTE stops inference.

Claim/source: with governed foundation models, an explicit DENY of EXECUTE on
`system.ai.<model>` to a principal should block that principal's inference
through the gateway. Whether Unity Catalog even accepts DENY on this
securable (vs the legacy Hive-metastore-only DENY) is itself under test, and
is suspected to be gated behind the "Foundation Model Permissions" Preview.

✅ means: DENY was accepted and the denied principal's inference attempt was
rejected (401/403).
❌ means: DENY was accepted but the principal could still infer.
❓ means: untestable — TEST_PRINCIPAL unset, mutations not opted in, DENY
refused by the API, or no TEST_PRINCIPAL_PROFILE to run the query-as-user
step (in that case the exact manual command is printed and recorded).

Needs: TEST_PRINCIPAL, ALLOW_ENDPOINT_MUTATION=1, optionally
TEST_PRINCIPAL_PROFILE (a databrickscfg profile authenticated AS the
principal). The DENY is reverted in a finally block (best effort).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _gov_common as gov  # noqa: E402
import _common  # noqa: E402

ROW_KEY = "deny_execute_blocks_inference"


def revert_deny(model_id: str, principal: str) -> bool:
    """Best-effort removal of a DENY (REVOKE removes explicit denies too)."""
    ok, attempts = gov.try_privilege_statements("REVOKE", model_id, principal)
    for stmt, res in attempts:
        marker = "ok" if res["ok"] else gov.snip(res["error"], 150)
        print(f"    cleanup: {stmt} -> {marker}")
    return ok


def main() -> int:
    _common.section("04 — DENY EXECUTE on the model blocks inference")

    model_id = _common.resolve_model_id()
    principal = gov.get_test_principal()
    profile = gov.get_test_principal_profile()
    print(f"  Model: {model_id}")

    if principal is None:
        gov.print_test_principal_setup()
        gov.conclude(ROW_KEY, None, "TEST_PRINCIPAL not set — untested")
        return 0

    print(f"  Target principal: {principal}")

    if not gov.mutation_allowed():
        gov.print_mutation_gate()
        print("  The mutation path would run, then revert:")
        print(f"    DENY EXECUTE ON MODEL|FUNCTION {model_id} TO `{principal}`")
        print("    (inference attempt AS the principal — expected 401/403)")
        gov.conclude(
            ROW_KEY,
            None,
            f"Dry-run only: {gov.MUTATION_ENV}=1 not set, so no DENY was "
            f"attempted against {model_id} for {principal} — untested",
        )
        return 0

    denied = False
    notes_parts: list[str] = []
    verdict: bool | None = None
    try:
        print("  Attempting DENY EXECUTE (MODEL, then FUNCTION spelling)...")
        deny_ok, deny_attempts = gov.try_privilege_statements(
            "DENY", model_id, principal
        )
        denied = deny_ok
        for stmt, res in deny_attempts:
            marker = "ok" if res["ok"] else f"{res['state']}: {gov.snip(res['error'], 200)}"
            print(f"    {stmt} -> {marker}")
            notes_parts.append(f"[{stmt}] -> {'OK' if res['ok'] else gov.snip(res['error'], 200)}")

        if not deny_ok:
            _common.fail("DENY EXECUTE was refused by the API (all spellings).")
            print("  Consistent with UC not supporting DENY on this securable, or")
            print("  the Foundation Model Permissions Preview being disabled.")
            verdict = None
            notes_parts.insert(
                0,
                "API refused DENY EXECUTE on the model securable — blocking "
                "behavior untestable:",
            )
            return 0

        _common.ok("DENY EXECUTE accepted.")

        if profile:
            with gov.as_profile(profile):
                status, body = _common.chat_completion(
                    model_id, "Reply with: pong", max_tokens=8
                )
            print(f"  Inference AS {principal} (profile {profile}): HTTP {status}")
            notes_parts.append(
                f"post-DENY inference as principal -> HTTP {status} ({gov.snip(body, 200)})"
            )
            if status in (401, 403):
                _common.ok("Denied principal was blocked — DENY is enforced.")
                verdict = True
                notes_parts.insert(0, "DENY EXECUTE accepted AND blocks inference:")
            elif status == 200:
                _common.fail("Denied principal could still infer — DENY NOT enforced.")
                verdict = False
                notes_parts.insert(0, "DENY accepted but inference still succeeded (leak):")
            else:
                verdict = None
                notes_parts.insert(0, f"DENY accepted; ambiguous HTTP {status} from principal:")
        else:
            manual = (
                "As the second user (their own SSO login), run: "
                f"RPW_MODEL={model_id} uv run python "
                "scripts/governance/01_query_via_gateway_sso.py — expect HTTP 401/403 "
                "while the DENY is in place."
            )
            print("  TEST_PRINCIPAL_PROFILE not set — cannot query as the principal.")
            print(f"  Manual step: {manual}")
            verdict = None
            notes_parts.insert(0, f"DENY accepted but unverified — {manual} —")
        return 0
    finally:
        if denied:
            print("  Cleanup: reverting the DENY made by this script...")
            if revert_deny(model_id, principal):
                _common.ok("DENY reverted.")
            else:
                _common.fail(
                    "Could not revert the DENY — manually run: "
                    f"REVOKE EXECUTE ON MODEL {model_id} FROM `{principal}`"
                )
                notes_parts.append("WARNING: DENY cleanup failed — manual revoke needed.")
        gov.conclude(ROW_KEY, verdict, " | ".join(notes_parts))


if __name__ == "__main__":
    sys.exit(main())
