"""Submit scripts/spark/matrix_job.py as a one-time run on a pinned DBR job cluster, then
download its raw evidence to results/.spark_raw.json (gitignored; rows 02-04 scrub it)."""

import io
import json
import os
import time
from datetime import timedelta

from _common import CATALOG, ROOT, SCHEMA, VOLUME_PATH, client
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.workspace import ImportFormat

SPARK_VERSION = os.getenv("EXPERIMENT_SPARK_VERSION", "19.x-scala2.13")
NODE_TYPE = os.getenv("EXPERIMENT_NODE_TYPE", "m5d.xlarge")
RAW = ROOT / "results" / ".spark_raw.json"


def _wait(w, run_id: int):
    """Poll by wall clock: the SDK waiter can time out early across laptop sleep."""
    deadline = time.time() + timedelta(minutes=60).total_seconds()
    while time.time() < deadline:
        run = w.jobs.get_run(run_id)
        if run.state and run.state.life_cycle_state in (
            jobs.RunLifeCycleState.TERMINATED,
            jobs.RunLifeCycleState.SKIPPED,
            jobs.RunLifeCycleState.INTERNAL_ERROR,
        ):
            return run
        time.sleep(20)
    w.jobs.cancel_run(run_id)
    raise SystemExit(f"run {run_id} exceeded 60 minutes; cancelled")


def main() -> None:
    w = client()
    me = w.current_user.me().user_name
    ws_dir = f"/Workspace/Users/{me}/.shareables/uc-delta-vs-iceberg"
    w.workspace.mkdirs(ws_dir)
    src = (ROOT / "scripts" / "spark" / "matrix_job.py").read_bytes()
    w.workspace.upload(f"{ws_dir}/matrix_job.py", io.BytesIO(src), format=ImportFormat.AUTO, overwrite=True)
    tag = time.strftime("%Y%m%dT%H%M%S")
    print(f"submitting run {tag} on {SPARK_VERSION} / {NODE_TYPE} ...", flush=True)
    waiter = w.jobs.submit(
        run_name=f"uc-delta-vs-iceberg-{tag}",
        tasks=[
            jobs.SubmitTask(
                task_key="matrix",
                spark_python_task=jobs.SparkPythonTask(
                    python_file=f"{ws_dir}/matrix_job.py",
                    parameters=[CATALOG, SCHEMA, VOLUME_PATH, tag],
                ),
                new_cluster=compute.ClusterSpec(
                    spark_version=SPARK_VERSION,
                    node_type_id=NODE_TYPE,
                    num_workers=0,
                    data_security_mode=compute.DataSecurityMode.SINGLE_USER,
                    single_user_name=me,
                    spark_conf={
                        "spark.databricks.cluster.profile": "singleNode",
                        "spark.master": "local[*]",
                    },
                    custom_tags={"ResourceClass": "SingleNode", "project": "shareables-104"},
                ),
            )
        ],
    )
    run = _wait(w, waiter.run_id)
    state = run.state.result_state if run.state else None
    print(f"run finished: {state}")
    dest = f"{VOLUME_PATH}/out/spark_matrix_{tag}.json"
    raw = json.loads(w.files.download(dest).contents.read())
    RAW.write_text(json.dumps(raw, indent=2, default=str) + "\n")
    print(f"downloaded evidence -> {RAW.relative_to(ROOT)} (dbr={raw.get('dbr')})")
    failed = [k for k, v in raw["cells"].items() if not v["ok"]]
    print(f"{len(raw['cells'])} cells, {len(failed)} failed: {failed}")


if __name__ == "__main__":
    main()
