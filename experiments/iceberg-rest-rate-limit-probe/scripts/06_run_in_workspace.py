"""Run the probe from serverless compute inside each workspace (no laptop network in the way).

Deploy the bundle first (`make job-deploy`). For each workspace label this launches one
run of the bundle's job with the same `--start-at`, so two workspaces ramp in lockstep
for the scope test. Logs land in the workspace's `probe_logs` volume and are pulled
back into data/<RUN_ID>/ (merged into one file when several workspaces ran together).

Usage:
    uv run python scripts/06_run_in_workspace.py --endpoint load_table_spread [--workspaces a,b] [-- --rates 100,200]
"""

from __future__ import annotations

import argparse
import json
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
from _common import DATA_DIR, load_config, workspace_client

JOB_NAME = "iceberg-rest-rate-limit-probe: in-workspace ramp"
START_DELAY_SEC = 240  # serverless cold start; both runs must be up before the ramp starts


def find_job(w) -> int:
    for j in w.jobs.list(name=None):
        if j.settings and j.settings.name and j.settings.name.endswith(JOB_NAME):
            return j.job_id
    raise SystemExit("bundle job not found; run `make job-deploy` first")


def main() -> None:
    argv = sys.argv[1:]
    extra: list[str] = []
    if "--" in argv:
        i = argv.index("--")
        argv, extra = argv[:i], argv[i + 1 :]
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--endpoint", required=True)
    p.add_argument("--workspaces", default="a")
    p.add_argument("--sp-scope", default="", metavar="LABEL=SCOPE",
                   help="in workspace LABEL, call as the service principal stored in secret scope SCOPE")
    args = p.parse_args(argv)
    sp_label, _, sp_scope = args.sp_scope.partition("=")

    cfg = load_config()
    labels = args.workspaces.split(",")
    start_at = time.time() + START_DELAY_SEC
    runs = {}
    for label in labels:
        w = workspace_client(cfg.profile(label))
        cat = cfg.catalog(label)
        # per-mode folder: a solo run and a scope run of one endpoint never overwrite each other
        out_dir = f"/Volumes/{cat}/{cfg.uc_schema}/probe_logs/jobs/{'x'.join(labels)}"
        sets = {"DATABRICKS_PROFILE_A": "runtime"} | {
            f"DATABRICKS_PROFILE_{label.upper()}": f"sp:{sp_scope}" if label == sp_label else "runtime",
            "UC_CATALOG": cat,
            "UC_CATALOG_B": cat,
            "UC_SCHEMA": cfg.uc_schema,
            "PROBE_TABLE_COUNT": str(cfg.table_count),
            "PROBE_MAX_RATE": str(cfg.max_rate),
            "PROBE_MAX_DURATION_SEC": str(cfg.max_duration_sec),
            "PROBE_MAX_CONCURRENCY": str(cfg.max_concurrency),
            "PROBE_CLOUD": cfg.cloud,
            "PROBE_REGION": cfg.region,
            "RUN_ID": cfg.run_id,
        }
        params = ["--endpoint", args.endpoint, "--workspaces", label, "--no-live", "--out-dir", out_dir]
        if len(labels) > 1:
            params += ["--start-at", f"{start_at:.1f}"]
        for k, v in sets.items():
            params += ["--set", f"{k}={v}"]
        params += extra
        run = w.jobs.run_now(job_id=find_job(w), python_params=params)
        runs[label] = (w, run.run_id, out_dir)
        print(f"workspace {label}: run {run.run_id} launched", flush=True)

    tables = []
    for label, (w, run_id, out_dir) in runs.items():
        while True:
            r = w.jobs.get_run(run_id)
            if r.state and r.state.life_cycle_state and r.state.life_cycle_state.value in (
                "TERMINATED", "SKIPPED", "INTERNAL_ERROR"
            ):
                break
            time.sleep(15)
        task_run = r.tasks[0].run_id
        out = w.jobs.get_run_output(task_run)
        print(f"--- workspace {label}: {r.state.result_state} ---")
        print((out.logs or out.error or "")[-3000:])
        if r.state.result_state is None or r.state.result_state.value != "SUCCESS":
            raise SystemExit(f"run in workspace {label} failed: {r.state.state_message}")
        src = f"{out_dir}/{cfg.run_id}/{args.endpoint}-{label}.parquet"
        tag = "job" if len(labels) == 1 else "part"
        local = DATA_DIR / cfg.run_id / f"{args.endpoint}-{label}-{tag}.parquet"
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(w.files.download(src).contents.read())
        meta = w.files.download(src.replace(".parquet", ".meta.json")).contents.read()
        local.with_suffix(".meta.json").write_bytes(meta)
        tables.append(pq.read_table(local))

    if len(tables) > 1:  # one combined file, so the analyzer sees both workspaces in one run
        merged = DATA_DIR / cfg.run_id / f"{args.endpoint}-{'x'.join(labels)}-job.parquet"
        pq.write_table(pa.concat_tables(tables), merged)
        first = DATA_DIR / cfg.run_id / f"{args.endpoint}-{labels[0]}-part.meta.json"
        meta = json.loads(first.read_text()) | {"workspaces": labels}
        merged.with_suffix(".meta.json").write_text(json.dumps(meta, indent=2))
        for label in labels:
            (DATA_DIR / cfg.run_id / f"{args.endpoint}-{label}-part.parquet").unlink()
            (DATA_DIR / cfg.run_id / f"{args.endpoint}-{label}-part.meta.json").unlink()
        print(f"merged -> {merged}")


if __name__ == "__main__":
    main()
