"""Deploy the bundle, run both Lakeflow Jobs orchestration paths, record rows j01-j03.

Both jobs build the same dbt v2 project (full refresh, batch 1) into their own
schema, at the same time. A row is ✅ when the job run succeeds; the evidence
is the run's final state plus, for a failure, the first dbt `[error]` line or
the platform's state message. Every task attempt is recorded, because serverless
retries a failed environment install and the first attempt's error is evidence
too (see j03).
"""

from __future__ import annotations

import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from _common import (
    CATALOG,
    EXPERIMENT_ROOT,
    LANDING_PATH,
    PROFILE,
    client,
    load_results,
    now_iso,
    save_results,
    warehouse_id,
)

ANSI = re.compile(r"\x1b\[[0-9;]*m")
JOBS = {
    "j01": (
        "dbt_v2_native_dbt_task",
        (
            "Native Lakeflow Jobs `dbt_task` (serverless) with the dbt v2 wheel as its "
            "environment dependency: does the whole project build?"
        ),
    ),
    "j02": (
        "dbt_v2_python_runner",
        (
            "Serverless Python task running the dbt v2 binary as the job's `run_as` "
            "identity: does the whole project build?"
        ),
    ),
}


def bundle_vars(w) -> list[str]:
    return [
        "--profile", PROFILE,
        "--var", f"catalog={CATALOG}",
        "--var", f"warehouse_id={warehouse_id(w)}",
        "--var", f"landing_path={LANDING_PATH}",
    ]  # fmt: skip


def job_id(w, key: str) -> int:
    out = subprocess.run(
        ["databricks", "bundle", "summary", "-o", "json", *bundle_vars(w)],
        cwd=EXPERIMENT_ROOT, capture_output=True, text=True, check=True,
    )  # fmt: skip
    import json

    return int(json.loads(out.stdout)["resources"]["jobs"][key]["id"])


def attempt_evidence(w, task: Any) -> str:
    """What one task attempt says about itself: dbt's first error, else the state message."""
    try:
        out = w.jobs.get_run_output(task.run_id)
    except Exception:  # noqa: BLE001
        out = None
    logs = ANSI.sub("", (out.logs or "") if out else "")
    for line in logs.splitlines():
        if line.startswith("[error]"):
            block = logs[logs.index(line) :].splitlines()[:3]
            text = " ".join(x.strip() for x in block)
            return re.sub(r"RequestId=[0-9a-f-]+|pipeline [0-9a-f-]{36}", "…", text)[:400]
    summary = [x for x in logs.splitlines() if x.startswith("Summary:")]
    if summary:
        return summary[-1].strip()
    msg = (task.state.state_message or "").strip()
    m = re.search(r"ERROR: (\S+ is not a supported wheel on this platform)", msg)
    return m.group(1) if m else msg.splitlines()[0][:300] if msg else ""


def run_job(w, key: str) -> dict[str, Any]:
    run = w.jobs.run_now(job_id(w, key))
    run_id = run.run_id
    print(f"  started {key} run {run_id}", flush=True)
    while True:
        r = w.jobs.get_run(run_id)
        if r.state.life_cycle_state.value in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            break
        time.sleep(20)
    attempts = [
        {
            "attempt": t.attempt_number,
            "result": (t.state.result_state.value if t.state.result_state else None),
            "evidence": attempt_evidence(w, t),
        }
        for t in sorted(r.tasks, key=lambda t: t.attempt_number or 0)
    ]
    ok = r.state.result_state is not None and r.state.result_state.value == "SUCCESS"
    return {"ok": ok, "attempts": attempts}


def main() -> None:
    w = client()
    print("deploying bundle", flush=True)
    subprocess.run(["databricks", "bundle", "deploy", *bundle_vars(w)], cwd=EXPERIMENT_ROOT,
                   check=True)  # fmt: skip
    with ThreadPoolExecutor(max_workers=len(JOBS)) as pool:
        futures = {rid: pool.submit(run_job, w, key) for rid, (key, _) in JOBS.items()}
        outcomes = {rid: f.result() for rid, f in futures.items()}

    data = load_results()
    for rid, (key, question) in JOBS.items():
        o = outcomes[rid]
        last = o["attempts"][-1]
        data["rows"][rid] = {
            "question": question,
            "status": "pass" if o["ok"] else "fail",
            "evidence": last["evidence"] or last["result"],
            "attempts": o["attempts"],
            "at": now_iso(),
        }
        print(f"  {'✅' if o['ok'] else '❌'} {rid} {data['rows'][rid]['evidence']}", flush=True)

    # j03: did any first attempt fail on wheel platform mismatch? Recorded so a
    # pinned single-platform wheel URL is visibly a flaky choice on serverless.
    mismatches = [
        a["evidence"] for o in outcomes.values() for a in o["attempts"]
        if "not a supported wheel" in (a["evidence"] or "")
    ]  # fmt: skip
    data["rows"]["j03"] = {
        "question": "With per-platform PEP 508 markers on the wheel URLs, does the serverless "
        "environment install dbt v2 on the first attempt?",
        "status": "fail" if mismatches else "pass",
        "evidence": mismatches[0] if mismatches else "every task attempt installed the wheel",
        "at": now_iso(),
    }
    save_results(data)


if __name__ == "__main__":
    main()
