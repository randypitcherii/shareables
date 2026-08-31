"""Launch a Databricks ingest job (path A or B) with a preset ladder of cells.

The DAB defines the jobs (house convention); this script launches runs via the
SDK's run_now with python_params, because every invocation carries a different
cells_json and `bundle run --params` cannot pass JSON (it splits on commas).

Usage: uv run python scripts/01_run_databricks_cells.py <preset>
Presets: scale-a | scale-b | ladder-a | ladder-b
"""

import json
import subprocess
import sys
import time

from _common import EXPERIMENT_ROOT, load_config, record_result, workspace_client

JOB_KEYS = {"a": "path_a_kafka", "b": "path_b_pulsar"}

# Window sizes: enough for >=3 commits on timed triggers (initial + N intervals),
# with slack for source startup.
PRESETS = {
    "scale-a": (
        "a",
        [
            {
                "name": "a_scale_plain",
                "mode": "drain",
                "filter": "off",
                "cluster": "none",
                "starting": "earliest",
            },
            {
                "name": "a_scale_filtered",
                "mode": "drain",
                "filter": "on",
                "cluster": "none",
                "starting": "earliest",
            },
            {
                "name": "a_scale_clustered",
                "mode": "drain",
                "filter": "off",
                "cluster": "auto",
                "starting": "earliest",
            },
        ],
    ),
    "scale-b": (
        "b",
        [
            {
                "name": "b_scale_plain",
                "mode": "drain",
                "filter": "off",
                "cluster": "none",
                "starting": "earliest",
            },
        ],
    ),
    "ladder-a": (
        "a",
        [
            {
                "name": "a_nrt",
                "mode": "window",
                "trigger_sec": 0,
                "window_sec": 480,
                "filter": "off",
                "cluster": "none",
                "starting": "latest",
            },
            {
                "name": "a_t60",
                "mode": "window",
                "trigger_sec": 60,
                "window_sec": 600,
                "filter": "off",
                "cluster": "none",
                "starting": "latest",
            },
            {
                "name": "a_t300",
                "mode": "window",
                "trigger_sec": 300,
                "window_sec": 780,
                "filter": "off",
                "cluster": "none",
                "starting": "latest",
            },
        ],
    ),
    "ladder-b": (
        "b",
        [
            {
                "name": "b_nrt",
                "mode": "window",
                "trigger_sec": 0,
                "window_sec": 480,
                "filter": "off",
                "cluster": "none",
                "starting": "latest",
            },
            {
                "name": "b_t60",
                "mode": "window",
                "trigger_sec": 60,
                "window_sec": 600,
                "filter": "off",
                "cluster": "none",
                "starting": "latest",
            },
        ],
    ),
}


def job_id_for(cfg, key: str) -> int:
    out = subprocess.run(
        [
            "databricks",
            "bundle",
            "summary",
            "--output",
            "json",
            "--profile",
            cfg.databricks_profile,
            "--target",
            "dev",
        ],
        cwd=EXPERIMENT_ROOT / "databricks",
        capture_output=True,
        text=True,
        check=True,
    )
    summary = json.loads(out.stdout)
    return int(summary["resources"]["jobs"][key]["id"])


def main() -> None:
    preset_name = sys.argv[1]
    path, cells = PRESETS[preset_name]
    cfg = load_config()
    job_key = JOB_KEYS[path]
    endpoint = cfg.kafka_bootstrap if path == "a" else cfg.pulsar_service_url

    job_id = job_id_for(cfg, job_key)
    w = workspace_client(cfg)
    params = [
        "kafka" if path == "a" else "pulsar",
        endpoint,
        cfg.pulsar_topic,
        cfg.uc_catalog,
        cfg.uc_schema,
        cfg.run_id,
        json.dumps(cells),
    ]
    print(f"launching job {job_id} ({job_key}) preset={preset_name} run_id={cfg.run_id}")
    waiter = w.jobs.run_now(job_id=job_id, python_params=params)
    run_id = waiter.run_id
    print(f"run started: run_id={run_id}")

    started = time.time()
    while True:
        run = w.jobs.get_run(run_id)
        state = (
            run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else "?"
        )
        result = run.state.result_state.value if run.state and run.state.result_state else None
        if state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            print(f"run finished: {state}/{result} after {round(time.time() - started)}s")
            record_result(
                f"job_run_{preset_name}",
                {
                    "preset": preset_name,
                    "job_key": job_key,
                    "databricks_run_id": run_id,
                    "result_state": result,
                    "duration_sec": round(time.time() - started),
                    "run_page_url": run.run_page_url,
                },
            )
            if result != "SUCCESS":
                raise SystemExit(f"job run failed: {result} ({run.run_page_url})")
            return
        print(f"  ...{state} ({round(time.time() - started)}s)")
        time.sleep(30)


if __name__ == "__main__":
    main()
