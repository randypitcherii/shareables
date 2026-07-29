"""Remove everything the experiment created.

Every matrix row must be re-runnable for reproduction, which means the
experiment ends with an empty workspace rather than an orphaned sandbox: the
schema and its functions, the metastore-level connections, the broker job and
its notebook, every service principal any row created, and the low-privilege
caller identity itself.

Connections and jobs are the ones worth being explicit about. Connections live
in a flat metastore-wide namespace, so a schema drop does not touch them, and
the broker job is created outside Unity Catalog entirely.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common import (  # noqa: E402
    CONNECTION_NAME,
    CONNECTION_SCIM_NAME,
    EXPERIMENT_ROOT,
    FQ_SCHEMA,
    JOB_NAME,
    JOB_SP_PREFIX,
    LOWPRIV_DISPLAY_NAME,
    admin_client,
    info,
    ok,
    run_sql,
    section,
    step,
)

# Rows 4, 7 and 11 each create something with a name of its own; teardown must
# know about every one of them or the next run's pre-checks fail on stale state.
TARGET_NAMES = ("udf-delegation-target-sp", "udf-delegation-target-sp-embedded")
CONNECTION_NAMES = (CONNECTION_NAME, CONNECTION_SCIM_NAME, "udf_delegation_scim_live_conn")


def drop_principals(w, display_name: str) -> int:
    dropped = 0
    for sp in w.service_principals.list(filter=f'displayName eq "{display_name}"'):
        w.service_principals.delete(id=sp.id)
        dropped += 1
    return dropped


def drop_principals_by_prefix(w, prefix: str) -> list[str]:
    """Row 12 creates principals whose names the caller partly chose, so they are
    matched by the prefix the notebook enforces rather than by an exact name."""
    dropped = []
    for sp in w.service_principals.list():
        if (sp.display_name or "").startswith(prefix):
            w.service_principals.delete(id=sp.id)
            dropped.append(sp.display_name or "")
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

    for name in CONNECTION_NAMES:
        step(f"dropping connection {name}")
        outcome = run_sql(w, f"DROP CONNECTION IF EXISTS `{name}`")
        if outcome.succeeded:
            ok("dropped")
        else:
            info(f"reported: {outcome.error_code}: {outcome.error}")

    step(f"deleting job {JOB_NAME!r} and its notebook")
    for job in w.jobs.list(name=JOB_NAME):
        if job.job_id:
            settings = w.jobs.get(job_id=job.job_id).settings
            w.jobs.delete(job_id=job.job_id)
            ok(f"job {job.job_id} deleted")
            task = (settings.tasks or [None])[0] if settings else None
            path = task.notebook_task.notebook_path if task and task.notebook_task else None
            if path:
                try:
                    w.workspace.delete(path=path)
                    ok(f"notebook {path} deleted")
                except Exception as exc:  # noqa: BLE001 - teardown reports, never aborts
                    info(f"notebook delete reported: {type(exc).__name__}: {exc}")

    step(f"dropping service principals created by the broker job ({JOB_SP_PREFIX}*)")
    dropped = drop_principals_by_prefix(w, JOB_SP_PREFIX)
    ok(f"removed {len(dropped)}: {dropped or 'none'}")

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
