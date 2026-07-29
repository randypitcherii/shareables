"""Remove everything the experiment created.

Every matrix row must be re-runnable for reproduction, which means the
experiment ends with an empty workspace rather than an orphaned sandbox: the
schema and its functions, the target service principal from row 4, and the
low-privilege caller identity itself.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common import (  # noqa: E402
    EXPERIMENT_ROOT,
    FQ_SCHEMA,
    LOWPRIV_DISPLAY_NAME,
    admin_client,
    info,
    ok,
    run_sql,
    section,
    step,
)

# Both row-4 and row-7 create a target principal; teardown must know about
# every one of them or the next run's pre-check fails on stale state.
TARGET_NAMES = ("udf-delegation-target-sp", "udf-delegation-target-sp-embedded")


def drop_principals(w, display_name: str) -> int:
    dropped = 0
    for sp in w.service_principals.list(filter=f'displayName eq "{display_name}"'):
        w.service_principals.delete(id=sp.id)
        dropped += 1
    return dropped


def main() -> int:
    section("teardown")
    w = admin_client()

    step(f"dropping schema {FQ_SCHEMA} (cascade)")
    outcome = run_sql(w, f"DROP SCHEMA IF EXISTS {FQ_SCHEMA} CASCADE")
    if outcome.succeeded:
        ok("schema dropped")
    else:
        info(f"schema drop reported: {outcome.error_code}: {outcome.error}")

    for name in (*TARGET_NAMES, LOWPRIV_DISPLAY_NAME):
        step(f"dropping service principals named {name!r}")
        count = drop_principals(w, name)
        ok(f"removed {count}")

    env_path = EXPERIMENT_ROOT / ".env"
    if env_path.exists():
        env_path.unlink()
        ok("removed .env")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
