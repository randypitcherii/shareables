"""Row 12 — can a caller holding only CAN_MANAGE_RUN cause the privileged action?

This is row 4 asked of a primitive that has what the function model lacked. Row 4
failed because the Python UDF sandbox is anonymous: it had the network and the
code but no identity, so the control plane answered it like any stranger. A job
runs as a named principal on compute holding that principal's credential, so the
question becomes worth asking again — and it is the same question the customer
started with, only pointed at a different object.

Three measurements:

  control     the caller creates a service principal directly, via SCIM. It must
              be refused, or nothing that follows distinguishes delegation from
              the caller simply having had the privilege.
  measurement the caller triggers the job with a well-formed suffix. Success is
              not the run finishing — it is a principal existing afterwards,
              read back by the admin from the SCIM directory, with the name the
              notebook composed rather than the one the caller asked for.
  constraint  the caller triggers the job with a suffix the notebook's rules
              reject. If that also creates something, the shape constraint is
              decorative and this row is no better than row 7.

The caller passes a suffix; the notebook prepends the prefix. Whether that
division holds under a caller that supplies something hostile is the whole
difference between a delegated action and a delegated credential.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from databricks.sdk.service import iam  # noqa: E402

from _common import (  # noqa: E402
    JOB_NAME,
    JOB_SP_PREFIX,
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

ROW = "12"
QUESTION = (
    "Can a caller holding only CAN_MANAGE_RUN cause a service principal to be "
    "created, in a shape the author fixed?"
)

GOOD_SUFFIX = "alpha"
# Uppercase, a slash and a leading dot: rejected by the notebook's rule, and the
# shapes someone would try if they wanted to steer the name somewhere else.
BAD_SUFFIX = "../Evil Name"


def _resolve_job_id(w) -> int:
    job = next(iter(w.jobs.list(name=JOB_NAME)), None)
    if not job or not job.job_id:
        raise SystemExit(f"Job {JOB_NAME!r} not found — run 03_provision_delegated_job.py first.")
    return job.job_id


def _run_and_wait(caller, job_id: int, suffix: str, timeout_s: int = 900) -> dict:
    """Trigger as the caller and return the task's exit payload."""
    waiter = caller.jobs.run_now(
        job_id=job_id, notebook_params={"suffix": suffix}
    )
    run_id = waiter.run_id
    deadline = time.time() + timeout_s
    state = None
    while time.time() < deadline:
        run = caller.jobs.get_run(run_id=run_id)
        state = run.state
        if state and state.life_cycle_state and state.life_cycle_state.value in (
            "TERMINATED",
            "SKIPPED",
            "INTERNAL_ERROR",
        ):
            break
        time.sleep(10)

    record: dict = {
        "suffix_supplied_by_caller": suffix,
        "run_id": run_id,
        "life_cycle_state": state.life_cycle_state.value if state and state.life_cycle_state else None,
        "result_state": state.result_state.value if state and state.result_state else None,
    }

    run = caller.jobs.get_run(run_id=run_id)
    task_run_id = run.tasks[0].run_id if run.tasks else None
    if task_run_id:
        output = caller.jobs.get_run_output(run_id=task_run_id)
        record["notebook_output"] = output.notebook_output.result if output.notebook_output else None
        record["error"] = output.error
    return record


def _directory_contains(admin, display_name: str) -> bool:
    return any(
        admin.service_principals.list(filter=f'displayName eq "{display_name}"')
    )


def main() -> int:
    section(f"row {ROW}: job run-as as a delegation boundary")
    admin = admin_client()
    caller = lowpriv_client()
    admin_who, caller_who = assert_distinct_identities(admin, caller)
    info(f"author={admin_who}  caller={caller_who}")

    job_id = _resolve_job_id(admin)
    evidence: dict[str, object] = {"job_run_as": admin_who}

    # A previous run leaves its principal behind, and the read-back below cannot
    # tell "this run created it" from "last run did". Rows 4 and 7 answer the
    # same hazard by refusing to produce a result; that would make `make matrix`
    # non-repeatable, so this row clears its own prior output instead. The prefix
    # is the row's own, so nothing outside the experiment can match it.
    step("clearing principals left by a previous run of this row")
    stale = [
        sp
        for sp in admin.service_principals.list()
        if (sp.display_name or "").startswith(JOB_SP_PREFIX)
    ]
    for sp in stale:
        admin.service_principals.delete(id=sp.id)
    evidence["stale_principals_removed"] = [sp.display_name for sp in stale]
    ok(f"removed {len(stale)}")

    step("control: the caller tries to create a service principal directly")
    try:
        caller.service_principals.create(
            display_name=f"{JOB_SP_PREFIX}control",
            active=True,
            entitlements=[iam.ComplexValue(value="workspace-access")],
        )
        evidence["caller_direct_scim_create"] = {"refused": False}
        fail("the caller can create service principals directly — this row proves nothing")
    except Exception as exc:  # noqa: BLE001 - the refusal is the evidence
        evidence["caller_direct_scim_create"] = {
            "refused": True,
            "error_type": type(exc).__name__,
            "error": str(exc)[:300],
        }
        ok(f"refused, as required: {type(exc).__name__}")

    step(f"measurement: caller triggers the job with suffix {GOOD_SUFFIX!r}")
    good = _run_and_wait(caller, job_id, GOOD_SUFFIX)
    evidence["run_with_valid_suffix"] = good
    info(f"run {good['run_id']} finished {good['result_state']}")
    info(f"notebook output: {good.get('notebook_output')}")

    expected_name = f"{JOB_SP_PREFIX}{GOOD_SUFFIX}"
    step(f"admin reads the SCIM directory back, looking for {expected_name!r}")
    created = _directory_contains(admin, expected_name)
    evidence["principal_present_on_readback"] = created
    if created:
        ok("the principal exists — the action really happened, as the run-as identity")
    else:
        fail("no such principal; the run did not produce the delegated action")

    step(f"constraint: caller triggers the job with suffix {BAD_SUFFIX!r}")
    bad = _run_and_wait(caller, job_id, BAD_SUFFIX)
    evidence["run_with_rejected_suffix"] = bad
    info(f"notebook output: {bad.get('notebook_output')}")

    rejected = False
    try:
        payload = json.loads(bad.get("notebook_output") or "{}")
        rejected = payload.get("outcome") == "rejected"
    except (TypeError, ValueError):
        rejected = False
    evidence["hostile_suffix_rejected"] = rejected

    step("admin confirms the rejected suffix created nothing")
    stray = [
        sp.display_name
        for sp in admin.service_principals.list()
        if (sp.display_name or "").startswith(JOB_SP_PREFIX)
    ]
    evidence["principals_matching_prefix_after_both_runs"] = sorted(stray)
    info(f"principals carrying the prefix: {sorted(stray)}")

    caller_refused_directly = bool(
        evidence["caller_direct_scim_create"].get("refused")  # type: ignore[union-attr]
    )

    if not caller_refused_directly:
        write_result(
            ROW,
            question=QUESTION,
            status="inconclusive",
            finding=(
                "Could not isolate the question: the caller can create service "
                "principals directly, so causing one through the job is not evidence "
                "of delegation."
            ),
            evidence=evidence,
        )
    elif not created:
        write_result(
            ROW,
            question=QUESTION,
            status="fail",
            finding=(
                "The caller triggered the job and no principal existed afterwards. "
                f"Run finished {good.get('result_state')}; the notebook returned "
                f"{good.get('notebook_output')!r}. The job route did not deliver the "
                "delegated action in this environment."
            ),
            evidence=evidence,
        )
    elif not rejected:
        write_result(
            ROW,
            question=QUESTION,
            status="partial",
            finding=(
                "The delegated action works but its shape does not hold: the caller "
                "caused a principal to be created through the job, and the suffix the "
                "notebook was supposed to reject was not rejected. The mechanism "
                "delegates the action; this implementation of the constraint does not "
                f"constrain it. Directory after both runs: {sorted(stray)}."
            ),
            evidence=evidence,
        )
    elif sorted(stray) != [expected_name]:
        write_result(
            ROW,
            question=QUESTION,
            status="inconclusive",
            finding=(
                "The delegated action happened and the hostile suffix was rejected, "
                "but the directory does not match what the two runs should have "
                f"produced: expected exactly {[expected_name]}, found {sorted(stray)}. "
                "Something outside these two runs is creating principals under the "
                "same prefix, so the shape claim cannot be attributed to this row."
            ),
            evidence=evidence,
        )
    else:
        write_result(
            ROW,
            question=QUESTION,
            status="pass",
            finding=(
                "The job is a real delegation boundary. The caller was refused the "
                "SCIM create directly, then triggered a job it holds only "
                "CAN_MANAGE_RUN on and a service principal existed afterwards — "
                f"named {expected_name!r}, composed by the notebook from a prefix the "
                "caller never supplied. The hostile suffix was rejected by the "
                "notebook's own rule and created nothing, so the shape constraint is "
                "enforced by code running as the privileged identity rather than by "
                "trusting the caller. No credential passed through the caller's hands "
                "at any point: the job's identity lives on the compute, not in "
                "anything the caller can read (contrast rows 6-7)."
            ),
            evidence=evidence,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
