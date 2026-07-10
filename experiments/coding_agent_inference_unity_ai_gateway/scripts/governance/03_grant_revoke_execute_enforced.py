"""Matrix row: grant_revoke_execute_enforced — EXECUTE grants on the model.

Claim/source: with Unity Catalog governance of foundation models, admins can
GRANT/REVOKE EXECUTE on `system.ai.<model>` securables and the gateway
enforces it. The exact securable keyword for model *services* is itself part
of what's under test — UC registered models surface as securable type
FUNCTION in the grants API, SQL also documents a MODEL keyword, and the whole
capability is suspected to be gated behind the account-console
"Foundation Model Permissions" Preview (Beta). We try both spellings and
capture every API error verbatim.

✅ means: GRANT/REVOKE EXECUTE are accepted AND enforced (principal can infer
after GRANT, cannot after REVOKE).
❌ means: the API accepts the grant/revoke but does NOT enforce it.
❓ means: untestable here — TEST_PRINCIPAL unset, mutations not opted in, the
API refuses the operation as not-enabled (Preview off), or enforcement could
not be observed because no TEST_PRINCIPAL_PROFILE was available.

Needs: TEST_PRINCIPAL (target user email or group), metastore-admin-level
rights on the current identity, ALLOW_ENDPOINT_MUTATION=1, and optionally
TEST_PRINCIPAL_PROFILE for the query-as-principal enforcement check.
All mutations are reverted in a finally block (best effort).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _gov_common as gov  # noqa: E402
import _common  # noqa: E402

ROW_KEY = "grant_revoke_execute_enforced"


def main() -> int:
    _common.section("03 — GRANT/REVOKE EXECUTE on the model is enforced")

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
        print(f"    GRANT EXECUTE ON MODEL|FUNCTION {model_id} TO `{principal}`")
        print("    (inference check as principal, if TEST_PRINCIPAL_PROFILE set)")
        print(f"    REVOKE EXECUTE ON MODEL|FUNCTION {model_id} FROM `{principal}`")
        print("    (inference check again — should now be denied)")
        gov.conclude(
            ROW_KEY,
            None,
            f"Dry-run only: {gov.MUTATION_ENV}=1 not set, so no GRANT/REVOKE "
            f"was attempted against {model_id} for {principal} — untested",
        )
        return 0

    granted = False
    notes_parts: list[str] = []
    verdict: bool | None = None
    try:
        # --- GRANT --------------------------------------------------------
        print("  Attempting GRANT EXECUTE (MODEL, then FUNCTION spelling)...")
        grant_ok, grant_attempts = gov.try_privilege_statements(
            "GRANT", model_id, principal
        )
        granted = grant_ok
        for stmt, res in grant_attempts:
            marker = "ok" if res["ok"] else f"{res['state']}: {gov.snip(res['error'], 200)}"
            print(f"    {stmt} -> {marker}")
            notes_parts.append(f"[{stmt}] -> {'OK' if res['ok'] else gov.snip(res['error'], 200)}")

        if not grant_ok:
            _common.fail("GRANT EXECUTE was refused by the API (all spellings).")
            print("  This is consistent with the Foundation Model Permissions")
            print("  Preview (Beta) not being enabled in the account console.")
            verdict = None
            notes_parts.insert(
                0,
                "API refused GRANT EXECUTE on the model securable (likely the "
                "Foundation Model Permissions Preview is not enabled) — "
                "enforcement untestable:",
            )
            return 0

        _common.ok("GRANT EXECUTE accepted.")

        # --- Enforcement probe after GRANT ---------------------------------
        if profile:
            with gov.as_profile(profile):
                status_g, body_g = _common.chat_completion(
                    model_id, "Reply with: pong", max_tokens=8
                )
            print(f"  As {principal} after GRANT: HTTP {status_g}")
            notes_parts.append(f"post-GRANT inference as principal -> HTTP {status_g}")
        else:
            status_g = None
            print("  TEST_PRINCIPAL_PROFILE not set — cannot verify post-GRANT access.")

        # --- REVOKE ---------------------------------------------------------
        print("  Attempting REVOKE EXECUTE...")
        revoke_ok, revoke_attempts = gov.try_privilege_statements(
            "REVOKE", model_id, principal
        )
        for stmt, res in revoke_attempts:
            marker = "ok" if res["ok"] else f"{res['state']}: {gov.snip(res['error'], 200)}"
            print(f"    {stmt} -> {marker}")
            notes_parts.append(f"[{stmt}] -> {'OK' if res['ok'] else gov.snip(res['error'], 200)}")
        if revoke_ok:
            granted = False  # cleanup done inline

        # --- Enforcement probe after REVOKE --------------------------------
        if profile and revoke_ok:
            with gov.as_profile(profile):
                status_r, body_r = _common.chat_completion(
                    model_id, "Reply with: pong", max_tokens=8
                )
            print(f"  As {principal} after REVOKE: HTTP {status_r}")
            notes_parts.append(f"post-REVOKE inference as principal -> HTTP {status_r} ({gov.snip(body_r, 150)})")
            if status_g == 200 and status_r in (401, 403):
                _common.ok("GRANT allowed inference; REVOKE blocked it — enforced.")
                verdict = True
                notes_parts.insert(0, "GRANT/REVOKE EXECUTE accepted AND enforced:")
            elif status_r == 200:
                _common.fail("REVOKE accepted but the principal can still infer — NOT enforced.")
                verdict = False
                # A post-revoke 200 can be plain UC semantics if a broader
                # grant path remains (e.g. a schema-level EXECUTE to
                # `account users`, which ships by default). Capture that
                # evidence so the verdict reads correctly: revoking a direct
                # grant does not restrict access OOTB. DENY (row 04) is the
                # discriminating probe.
                securable = gov.uc_model_securable(model_id)
                if securable:
                    inherited = gov.sql_exec(
                        "SHOW GRANTS ON SCHEMA "
                        + gov.backtick_parts(".".join(securable.split(".")[:2]))
                    )
                    if inherited["ok"]:
                        others = [r for r in inherited["rows"] if "EXECUTE" in str(r)]
                        notes_parts.append(
                            f"schema-level EXECUTE grants still in effect: {gov.snip(others, 220)}"
                        )
                notes_parts.insert(
                    0,
                    "API accepted GRANT/REVOKE but post-revoke inference still 200 — "
                    "revoking a direct grant does NOT restrict access OOTB (note the "
                    "default schema-wide grant below; DENY in row 04 is the real test):",
                )
            else:
                verdict = None
                notes_parts.insert(0, "Grant/revoke accepted; enforcement signal ambiguous:")
        else:
            verdict = None
            if not profile:
                print("  Cannot observe enforcement without TEST_PRINCIPAL_PROFILE.")
                print("  Run this AS the second principal to check manually:")
                print(f"    RPW_MODEL={model_id} uv run python scripts/verify.py")
                notes_parts.insert(
                    0,
                    "GRANT/REVOKE statements accepted, but enforcement unobserved "
                    "(no TEST_PRINCIPAL_PROFILE to query as the principal):",
                )
            else:
                notes_parts.insert(0, "GRANT accepted but REVOKE refused — partial API surface:")
        return 0
    finally:
        # Best-effort cleanup: never leave a grant behind.
        if granted:
            print("  Cleanup: revoking the EXECUTE grant made by this script...")
            cleanup_ok, cleanup_attempts = gov.try_privilege_statements(
                "REVOKE", model_id, principal
            )
            if cleanup_ok:
                _common.ok("Cleanup revoke succeeded.")
            else:
                _common.fail(
                    "Cleanup revoke FAILED — manually run: "
                    f"REVOKE EXECUTE ON MODEL {model_id} FROM `{principal}`"
                )
                notes_parts.append("WARNING: cleanup revoke failed — manual revoke needed.")
        gov.conclude(ROW_KEY, verdict, " | ".join(notes_parts))


if __name__ == "__main__":
    sys.exit(main())
