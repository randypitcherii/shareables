"""Row 08: can jobs pinned to an older DBR LTS (16.4, 17.3) read and write each managed table
shape, or does the table force every reader onto DBR 18+?

Docs: managed Iceberg needs DBR 16.4 LTS+, Iceberg v3 features need DBR 18 LTS+. This row
checks what actually happens per table shape -- including whether managed Delta with Iceberg
reads is a way to keep 16.4 jobs working -- on 16.4 LTS, 17.3 LTS and 18 LTS, run in sequence.

Each table is seeded from DBSQL with insert -> update -> delete, so v3 shapes carry deletion
vectors, then each DBR task attempts read / INSERT / UPDATE / table_changes.
"""

import io
import json
import time

from _common import CATALOG, FQ, ROOT, SCHEMA, VOLUME_PATH, client, must_sql, short, sql, write_result
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.workspace import ImportFormat
from spark_run import NODE_TYPE, _wait

DBRS = {"16.4": "16.4.x-scala2.12", "17.3": "17.3.x-scala2.13", "18": "18.x-scala2.13"}
COLS = "(id BIGINT, cat STRING, v BIGINT)"
TABLES = {
    "dbr_ice_v3": f"{COLS} USING ICEBERG CLUSTER BY (cat) TBLPROPERTIES ('format-version' = '3')",
    "dbr_ice_v2": (
        f"{COLS} USING ICEBERG CLUSTER BY (cat) TBLPROPERTIES ('format-version' = '2', "
        "'iceberg.enableDeletionVectors' = 'false', 'iceberg.enableRowTracking' = 'false')"
    ),
    "dbr_delta_v3reads": (
        f"{COLS} CLUSTER BY (cat) TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported', "
        "'delta.enableIcebergCompatV3' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg', "
        "'delta.enableRowTracking' = 'true')"
    ),
    "dbr_delta_v2reads": (
        f"{COLS} CLUSTER BY (cat) TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true', "
        "'delta.universalFormat.enabledFormats' = 'iceberg')"
    ),
    "dbr_delta_plain": f"{COLS} CLUSTER BY (cat)",
}
LABELS = {
    "dbr_ice_v3": "Managed Iceberg v3 (Liquid)",
    "dbr_ice_v2": "Managed Iceberg v2 (Liquid, DVs + row tracking off)",
    "dbr_delta_v3reads": "Managed Delta, Iceberg v3 reads + catalog commits (this experiment's Delta shape)",
    "dbr_delta_v2reads": "Managed Delta, Iceberg v2 reads (UniForm, no catalog commits)",
    "dbr_delta_plain": "Managed Delta (Liquid, no Iceberg reads)",
}
SEED = [
    "INSERT INTO {t} VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
    "UPDATE {t} SET v = v + 1 WHERE id = 1",
    "DELETE FROM {t} WHERE id = 2",
]


def seed() -> dict:
    ev = {}
    for name, ddl in TABLES.items():
        t = f"{FQ}.{name}"
        must_sql(f"DROP TABLE IF EXISTS {t}")
        r = sql(f"CREATE TABLE {t} {ddl}")
        ev[name] = {"create": r.succeeded, "error": r.error}
        if r.succeeded:
            for s in SEED:
                must_sql(s.format(t=t))
            detail = must_sql(f"DESCRIBE DETAIL {t}").rows[0]
            ev[name]["format"] = detail[0]
            ev[name]["features"] = detail[-3] if len(detail) > 3 else None
    return ev


def run_job() -> dict[str, dict]:
    w = client()
    me = w.current_user.me().user_name
    ws_dir = f"/Workspace/Users/{me}/.shareables/uc-delta-vs-iceberg"
    w.workspace.mkdirs(ws_dir)
    src = (ROOT / "scripts" / "spark" / "legacy_dbr_job.py").read_bytes()
    w.workspace.upload(f"{ws_dir}/legacy_dbr_job.py", io.BytesIO(src), format=ImportFormat.AUTO, overwrite=True)
    tag = time.strftime("%Y%m%dT%H%M%S")
    tasks, prev = [], None
    for label, version in DBRS.items():
        key = "dbr_" + label.replace(".", "_")
        tasks.append(
            jobs.SubmitTask(
                task_key=key,
                depends_on=[jobs.TaskDependency(task_key=prev)] if prev else None,
                run_if=jobs.RunIf.ALL_DONE if prev else None,
                spark_python_task=jobs.SparkPythonTask(
                    python_file=f"{ws_dir}/legacy_dbr_job.py", parameters=[CATALOG, SCHEMA, VOLUME_PATH, tag]
                ),
                new_cluster=compute.ClusterSpec(
                    spark_version=version,
                    node_type_id=NODE_TYPE,
                    num_workers=0,
                    data_security_mode=compute.DataSecurityMode.SINGLE_USER,
                    single_user_name=me,
                    spark_conf={"spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*]"},
                    custom_tags={"ResourceClass": "SingleNode", "project": "shareables-104"},
                ),
            )
        )
        prev = key
    print(f"submitting legacy-DBR run {tag} on {list(DBRS.values())} (sequential) ...", flush=True)
    waiter = w.jobs.submit(run_name=f"uc-delta-vs-iceberg-legacy-dbr-{tag}", tasks=tasks)
    _wait(w, waiter.run_id)
    out = {}
    for entry in w.files.list_directory_contents(f"{VOLUME_PATH}/out"):
        if entry.name.startswith("legacy_dbr_") and entry.name.endswith(f"_{tag}.json"):
            raw = json.loads(w.files.download(entry.path).contents.read())
            out[raw["dbr"]] = raw["cells"]
    (ROOT / "results" / ".legacy_dbr_raw.json").write_text(json.dumps(out, indent=2, default=str) + "\n")
    return out


def main() -> None:
    seeded = seed()
    by_dbr = run_job()
    cells, evidence = {}, {"seed": seeded, "by_dbr": {}}
    for name, label in LABELS.items():
        summary = {}
        for dbr, c in sorted(by_dbr.items()):
            ops = {op: c[f"{name}.{op}"] for op in ("read", "insert", "update", "table_changes")}
            summary[dbr] = {op: ("ok" if v["ok"] else short(v, 200)) for op, v in ops.items()}
        evidence["by_dbr"][name] = summary
        older = {d: s for d, s in summary.items() if not d.startswith("18")}
        read_ok = all(s["read"] == "ok" for s in older.values()) if older else False
        write_ok = all(s["insert"] == "ok" and s["update"] == "ok" for s in older.values()) if older else False
        status = "pass" if read_ok and write_ok else "partial" if read_ok else "fail"
        finding = "; ".join(
            f"{d.split('-')[0]}: read {'ok' if s['read'] == 'ok' else 'FAIL'}, "
            f"write {'ok' if s['insert'] == 'ok' and s['update'] == 'ok' else 'FAIL'}, "
            f"table_changes {'ok' if s['table_changes'] == 'ok' else 'FAIL'}"
            for d, s in sorted(summary.items())
        )
        cells[name] = {"label": label, "status": status, "finding": finding}
    write_result(
        "08_older_dbr_access",
        question="Can DBR 16.4 / 17.3 LTS jobs read and write each managed table shape (vs DBR 18 LTS)?",
        cells=cells,
        evidence=evidence,
    )


if __name__ == "__main__":
    main()
