"""Create the low-privilege caller identity.

The whole experiment turns on the caller being *genuinely* less privileged than
the function author. Running the matrix as the admin who owns the functions
would prove nothing, so this script provisions a throwaway OAuth M2M service
principal that holds no workspace entitlements and no Unity Catalog grants
beyond what 01_provision_objects.py gives it.

Writes EXPERIMENT_LOWPRIV_CLIENT_ID / _CLIENT_SECRET into the gitignored .env.

Idempotent: re-running reuses the existing service principal and mints a fresh
secret.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from databricks.sdk.service import iam  # noqa: E402

from _common import (  # noqa: E402
    EXPERIMENT_ROOT,
    LOWPRIV_DISPLAY_NAME,
    admin_client,
    info,
    ok,
    section,
    step,
)

ENV_PATH = EXPERIMENT_ROOT / ".env"


def find_or_create_sp(w) -> iam.ServicePrincipal:
    for sp in w.service_principals.list(
        filter=f'displayName eq "{LOWPRIV_DISPLAY_NAME}"'
    ):
        info(f"reusing existing service principal (application_id={sp.application_id})")
        return sp

    step(f"creating service principal {LOWPRIV_DISPLAY_NAME!r}")
    # No entitlements: not allowed cluster creation, not a workspace admin.
    # It can authenticate and reach SQL, nothing more.
    return w.service_principals.create(
        display_name=LOWPRIV_DISPLAY_NAME,
        active=True,
        entitlements=[iam.ComplexValue(value="workspace-access")],
    )


def main() -> int:
    section("setup: low-privilege caller identity")
    w = admin_client()

    sp = find_or_create_sp(w)
    ok(f"service principal id={sp.id} application_id={sp.application_id}")

    step("minting a workspace-level OAuth secret for the service principal")
    secret = w.service_principal_secrets_proxy.create(service_principal_id=int(sp.id))
    ok(f"secret created (id={secret.id})")

    step("confirming the identity is NOT a workspace admin")
    admin_members = []
    for group in w.groups.list(filter='displayName eq "admins"'):
        admin_members = [m.value for m in (group.members or [])]
    if sp.id in admin_members:
        raise SystemExit(
            "The low-privilege service principal is in the admins group. "
            "That would invalidate every row of the matrix — remove it and re-run."
        )
    ok("caller is a plain workspace user, not an admin")

    step(f"writing credentials to {ENV_PATH.relative_to(EXPERIMENT_ROOT)} (gitignored)")
    ENV_PATH.write_text(
        "# Written by scripts/setup/00_create_lowpriv_principal.py — do not commit.\n"
        f"EXPERIMENT_LOWPRIV_SP_ID={sp.id}\n"
        f"EXPERIMENT_LOWPRIV_CLIENT_ID={sp.application_id}\n"
        f"EXPERIMENT_LOWPRIV_CLIENT_SECRET={secret.secret}\n"
    )
    ok("credentials written")

    step("granting the caller CAN_USE on the warehouse so it can issue statements at all")
    from databricks.sdk.service import sql as sqlservice

    from _common import warehouse_id

    wh = warehouse_id(w)
    w.warehouses.update_permissions(
        warehouse_id=wh,
        access_control_list=[
            sqlservice.WarehouseAccessControlRequest(
                service_principal_name=sp.application_id,
                permission_level=sqlservice.WarehousePermissionLevel.CAN_USE,
            )
        ],
    )
    ok(f"warehouse {wh} CAN_USE granted (compute access only — no data privileges)")

    step("proving the low-privilege identity authenticates as ITSELF, not as the admin")
    import os

    from _common import assert_distinct_identities, lowpriv_client

    os.environ["EXPERIMENT_LOWPRIV_CLIENT_ID"] = sp.application_id or ""
    os.environ["EXPERIMENT_LOWPRIV_CLIENT_SECRET"] = secret.secret or ""
    admin_who, caller_who = assert_distinct_identities(w, lowpriv_client())
    ok(f"admin={admin_who}  caller={caller_who}  (distinct)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
