"""Provision the job-as-broker scenario.

Rows 1-11 close the function and connection routes. What both lack is an
execution context with an identity of its own: the SQL UDF has definer's rights
but no grammar for identity administration, the Python UDF has the grammar but
is anonymous, and the connection has a credential but re-checks it against the
invoker. A job has all three — it runs as a named principal, on compute that
holds that principal's credential, executing arbitrary code.

This script creates, as the high-privilege author:

  <workspace>/.../udf_delegation_create_sp   a notebook that creates one service
                                             principal under a fixed name prefix
  udf-delegation-broker-job                  a job running that notebook, whose
                                             run-as identity is the author

and grants the caller CAN_MANAGE_RUN — the permission that allows triggering a
run and nothing else. The caller is deliberately not given CAN_MANAGE, which is
the permission that would let it edit the task, change the run-as identity, or
point the job at different code.

The shape constraint lives in the notebook, not in the platform: the caller
supplies a suffix, the notebook rejects anything that is not a short lowercase
token and prepends the prefix itself. That is the substantive difference from
every earlier row — the constraint is code the caller cannot reach, running as
an identity the caller cannot borrow, rather than a convention the caller is
trusted to follow.

Run-as is the author rather than a second service principal on purpose. The
question is whether a job executes with the run-as identity's privileges when a
different, lower-privileged principal triggers it; that is answered by any
run-as identity the caller is not. Using an identity that already exists avoids
minting a second workspace admin to answer a question that does not need one.

Idempotent: the notebook is overwritten and the job is looked up by name.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from databricks.sdk.service import jobs  # noqa: E402
from databricks.sdk.service.workspace import ImportFormat, Language  # noqa: E402

from _common import (  # noqa: E402
    JOB_NAME,
    JOB_SP_PREFIX,
    admin_client,
    info,
    ok,
    section,
    step,
)

LOWPRIV_APP_ID = os.environ.get("EXPERIMENT_LOWPRIV_CLIENT_ID", "")

NOTEBOOK_SOURCE = f'''# Databricks notebook source
# The delegated action, and the only place its shape is enforced.
#
# The caller supplies `suffix` and nothing else. The name prefix, the
# entitlements, and the fact that exactly one principal is created are decided
# here, in code the caller holds no permission to read or modify, executing as
# the job's run-as identity rather than the caller's.
import json
import re

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam

PREFIX = "{JOB_SP_PREFIX}"
ALLOWED_SUFFIX = re.compile(r"^[a-z0-9-]{{1,24}}$")

dbutils.widgets.text("suffix", "")
suffix = dbutils.widgets.get("suffix")

result = {{"requested_suffix": suffix}}

if not ALLOWED_SUFFIX.match(suffix):
    result["outcome"] = "rejected"
    result["reason"] = "suffix must match ^[a-z0-9-]{{1,24}}$"
    dbutils.notebook.exit(json.dumps(result))

w = WorkspaceClient()
result["run_as"] = w.current_user.me().user_name

name = PREFIX + suffix
try:
    sp = w.service_principals.create(
        display_name=name,
        active=True,
        entitlements=[iam.ComplexValue(value="workspace-access")],
    )
    result["outcome"] = "created"
    result["created_display_name"] = sp.display_name
    result["created_application_id"] = sp.application_id
except Exception as exc:
    result["outcome"] = "failed"
    result["error_type"] = type(exc).__name__
    result["error"] = str(exc)[:400]

dbutils.notebook.exit(json.dumps(result))
'''


def notebook_path(w) -> str:
    me = w.current_user.me().user_name
    return f"/Users/{me}/udf_delegation_create_sp"


def main() -> int:
    section("setup: the job that brokers the delegated action")
    if not LOWPRIV_APP_ID:
        raise SystemExit(
            "EXPERIMENT_LOWPRIV_CLIENT_ID is unset — run 00_create_lowpriv_principal.py first."
        )

    w = admin_client()
    path = notebook_path(w)

    step(f"uploading the notebook to {path}")
    w.workspace.upload(
        path=path,
        content=NOTEBOOK_SOURCE.encode(),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )
    ok("notebook uploaded")

    existing = next(iter(w.jobs.list(name=JOB_NAME)), None)
    if existing and existing.job_id:
        step(f"deleting the previous job {existing.job_id} so the definition is exact")
        w.jobs.delete(job_id=existing.job_id)

    step(f"creating job {JOB_NAME!r} with run-as the author")
    created = w.jobs.create(
        name=JOB_NAME,
        tasks=[
            jobs.Task(
                task_key="create_service_principal",
                notebook_task=jobs.NotebookTask(
                    notebook_path=path,
                    base_parameters={"suffix": ""},
                ),
            )
        ],
        parameters=None,
    )
    job_id = created.job_id
    ok(f"job created (job_id={job_id})")

    step("granting the caller CAN_MANAGE_RUN — trigger only, no edit")
    w.jobs.update_permissions(
        job_id=str(job_id),
        access_control_list=[
            jobs.JobAccessControlRequest(
                service_principal_name=LOWPRIV_APP_ID,
                permission_level=jobs.JobPermissionLevel.CAN_MANAGE_RUN,
            )
        ],
    )
    ok("CAN_MANAGE_RUN granted")

    perms = w.jobs.get_permissions(job_id=str(job_id))
    for acl in perms.access_control_list or []:
        if acl.service_principal_name == LOWPRIV_APP_ID:
            levels = [p.permission_level for p in (acl.all_permissions or [])]
            info(f"caller's effective job permissions: {levels}")
            if jobs.JobPermissionLevel.CAN_MANAGE in levels:
                raise SystemExit(
                    "The caller holds CAN_MANAGE on the job, which would let it "
                    "rewrite the task and invalidate rows 12-13. Revoke and re-run."
                )
    ok("caller can trigger the job and cannot manage it")

    env_line = f"EXPERIMENT_JOB_ID={job_id}\n"
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    text = env_path.read_text() if env_path.exists() else ""
    lines = [x for x in text.splitlines(keepends=True) if not x.startswith("EXPERIMENT_JOB_ID=")]
    env_path.write_text("".join(lines) + env_line)
    ok("job id written to .env")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
