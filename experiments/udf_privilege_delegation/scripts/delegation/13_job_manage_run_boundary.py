"""Row 13 — what does CAN_MANAGE_RUN *not* let the caller do?

Row 12 shows the job delivers the delegated action. That is only half the claim.
Row 6 is the reminder why: the embedded-credential wrapper also delivered the
action, and was still worthless, because the permission that let a caller invoke
it also let them read what it contained. A delegation boundary is defined by
what the grant withholds, not by what it permits.

So this row probes the edges of CAN_MANAGE_RUN as the caller:

  edit the task        can the caller repoint the job at different code?
  change run-as        can the caller swap the identity the job executes as?
  regrant itself       can the caller give itself CAN_MANAGE?
  read the source      can the caller read the notebook the job runs — the row 6
                       question, asked of the object that now holds the logic?
  read run output      can the caller read what a run returned?

The last one is expected to succeed, and it is not a defect — it is the
operational rule that falls out of the design. Whoever can trigger a run can
read what that run printed, so the job's output is part of the caller's
attack surface even though its credential is not. Anything the job prints, the
caller reads.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from databricks.sdk.service import jobs  # noqa: E402

from _common import (  # noqa: E402
    JOB_NAME,
    admin_client,
    assert_distinct_identities,
    fail,
    info,
    lowpriv_client,
    ok,
    section,
    step,
    write_result,
)

ROW = "13"
QUESTION = (
    "Does CAN_MANAGE_RUN withhold the ability to change what the job does, who it "
    "runs as, and what code it executes?"
)


def _attempt(label: str, fn) -> dict:
    """Run one probe; a refusal is the result, not an error."""
    try:
        fn()
        return {"refused": False, "note": "the caller was allowed to do this"}
    except Exception as exc:  # noqa: BLE001 - the refusal is the evidence
        return {
            "refused": True,
            "error_type": type(exc).__name__,
            "error": str(exc)[:300],
        }


def main() -> int:
    section(f"row {ROW}: the edges of CAN_MANAGE_RUN")
    admin = admin_client()
    caller = lowpriv_client()
    admin_who, caller_who = assert_distinct_identities(admin, caller)
    info(f"author={admin_who}  caller={caller_who}")

    job = next(iter(admin.jobs.list(name=JOB_NAME)), None)
    if not job or not job.job_id:
        raise SystemExit(f"Job {JOB_NAME!r} not found — run 03_provision_delegated_job.py first.")
    job_id = job.job_id
    settings = admin.jobs.get(job_id=job_id).settings
    notebook_path = settings.tasks[0].notebook_task.notebook_path  # type: ignore[union-attr,index]

    evidence: dict[str, object] = {"job_id_probed": "redacted-by-shape", "run_as": admin_who}
    must_refuse = {}

    step("caller attempts: repoint the job's task at different code")
    must_refuse["edit_task"] = _attempt(
        "edit_task",
        lambda: caller.jobs.update(
            job_id=job_id,
            new_settings=jobs.JobSettings(
                tasks=[
                    jobs.Task(
                        task_key="create_service_principal",
                        notebook_task=jobs.NotebookTask(notebook_path="/Shared/not_the_authors_code"),
                    )
                ]
            ),
        ),
    )

    step("caller attempts: change the identity the job runs as")
    must_refuse["change_run_as"] = _attempt(
        "change_run_as",
        lambda: caller.jobs.update(
            job_id=job_id,
            new_settings=jobs.JobSettings(
                run_as=jobs.JobRunAs(service_principal_name=caller_who)
            ),
        ),
    )

    step("caller attempts: grant itself CAN_MANAGE on the job")
    must_refuse["regrant_self"] = _attempt(
        "regrant_self",
        lambda: caller.jobs.update_permissions(
            job_id=str(job_id),
            access_control_list=[
                jobs.JobAccessControlRequest(
                    service_principal_name=caller_who,
                    permission_level=jobs.JobPermissionLevel.CAN_MANAGE,
                )
            ],
        ),
    )

    step("caller attempts: read the notebook the job runs (the row 6 question)")
    must_refuse["read_source"] = _attempt(
        "read_source", lambda: caller.workspace.export(path=notebook_path)
    )

    for label, result in must_refuse.items():
        if result["refused"]:
            ok(f"{label}: refused ({result.get('error_type')})")
        else:
            fail(f"{label}: ALLOWED — the grant does not withhold this")
    evidence["withheld_probes"] = must_refuse

    step("caller attempts: read the output of a run it triggered (expected to succeed)")
    runs = list(caller.jobs.list_runs(job_id=job_id, limit=1))
    output_readable = None
    if runs and runs[0].run_id:
        run = caller.jobs.get_run(run_id=runs[0].run_id)
        task_run_id = run.tasks[0].run_id if run.tasks else None
        if task_run_id:
            probe = _attempt(
                "read_run_output", lambda: caller.jobs.get_run_output(run_id=task_run_id)
            )
            output_readable = not probe["refused"]
            evidence["read_run_output"] = probe
            if output_readable:
                info("allowed — anything the job prints is readable by whoever triggers it")
            else:
                info("refused")

    all_withheld = all(r["refused"] for r in must_refuse.values())
    allowed = [k for k, v in must_refuse.items() if not v["refused"]]

    if all_withheld:
        write_result(
            ROW,
            question=QUESTION,
            status="pass",
            finding=(
                "CAN_MANAGE_RUN withholds every probed way of changing what the job "
                "does. The caller could not repoint the task, could not change the "
                "run-as identity, could not grant itself CAN_MANAGE, and could not "
                "read the notebook the job executes — the last being the question "
                "that killed the embedded-credential wrapper in row 6, answered the "
                "other way here. (The source read is refused as ResourceDoesNotExist "
                "rather than PermissionDenied: the workspace hides the object's "
                "existence instead of naming it. Still a refusal, but it is an "
                "existence answer, not an authorization one.) The one thing it does "
                "grant is reading the output of runs it triggers"
                + (
                    ", confirmed live: whatever the job prints, the caller reads, so "
                    "job output is part of the caller's surface even though the job's "
                    "credential is not."
                    if output_readable
                    else "."
                )
                + " Scoped to the probes listed; not a proof that no other edit path "
                "exists."
            ),
            evidence=evidence,
        )
    else:
        write_result(
            ROW,
            question=QUESTION,
            status="fail",
            finding=(
                "CAN_MANAGE_RUN does not withhold what the delegation depends on: the "
                f"caller was allowed to {', '.join(allowed)}. A caller who can change "
                "what the job does holds the run-as identity's privileges in full, not "
                "the one action the author intended to expose."
            ),
            evidence=evidence,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
