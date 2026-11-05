"""Create (or reuse) the throwaway non-owner SP the matrix acts as, and mint it an OAuth secret.

The owner deploys the services, and an owner passes every Unity Catalog check, so rows that prove
the EXECUTE gate need a second identity. This one is a plain workspace user: no admin, no
entitlements beyond workspace access. The bundle grants it EXECUTE on sub_a only.

Writes .state/test_caller.json (gitignored). Idempotent: reuses the SP and mints a fresh secret.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common import STATE_FILE, client, ok, section  # noqa: E402

DISPLAY_NAME = os.environ.get("TEST_CALLER_NAME", "multi-claude-sub-test-caller")


def main() -> int:
    from databricks.sdk.service import iam

    section("setup: test caller SP")
    w = client("owner")
    sp = next(iter(w.service_principals.list(filter=f'displayName eq "{DISPLAY_NAME}"')), None)
    if sp is None:
        sp = w.service_principals.create(display_name=DISPLAY_NAME, active=True,
                                         entitlements=[iam.ComplexValue(value="workspace-access")])
        ok(f"created {DISPLAY_NAME}")
    else:
        ok(f"reusing {DISPLAY_NAME}")

    admins = next(iter(w.groups.list(filter='displayName eq "admins"')), None)
    if admins and any(m.value == sp.id for m in admins.members or []):
        raise SystemExit("test caller is a workspace admin; that would invalidate the EXECUTE rows")

    secret = w.service_principal_secrets_proxy.create(service_principal_id=int(sp.id))
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps({"id": sp.id, "application_id": sp.application_id,
                                      "secret_id": secret.id, "client_secret": secret.secret}, indent=2))
    STATE_FILE.chmod(0o600)
    ok(f"secret minted; wrote {STATE_FILE.relative_to(STATE_FILE.parent.parent)} (gitignored)")
    print(sp.application_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
