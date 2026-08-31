"""Liquid clustering read/write benefit: representative queries against the
clustered vs unclustered twins of the same landed dataset, plus a timed
OPTIMIZE FULL to price the maintenance story for write-only streaming tables.

Sequence per table: 5 iterations x 3 queries (as landed) -> timed OPTIMIZE FULL
-> 5 more iterations (post-optimize). Each iteration's SQL carries a unique
comment so the warehouse result cache cannot short-circuit it; the disk cache
stays warm after iteration 1, which mirrors a hot serving pattern. Wall-clock
client-side timing; medians reported.
"""

import json
import statistics
import time
import uuid

from _common import load_config, record_result, run_sql

ITERATIONS = 5

# proj-0000 is the hottest tenant by construction (cubic skew in the generator).
QUERIES = {
    "q1_hot_tenant_slice": (
        "SELECT event_type, COUNT(*) AS events "
        "FROM {t} WHERE project_id = 'proj-0000' GROUP BY event_type"
    ),
    "q2_selective_variant_distinct": (
        "SELECT COUNT(DISTINCT CAST(event:user.user_hash AS STRING)) "
        "FROM {t} WHERE event_type = 'open'"
    ),
    "q3_fullscan_variant_group": (
        "SELECT CAST(event:context.geo.country AS STRING) AS country, COUNT(*) AS events "
        "FROM {t} GROUP BY country ORDER BY events DESC LIMIT 10"
    ),
}


def run_suite(cfg, table: str, phase: str) -> dict:
    out = {}
    for qname, template in QUERIES.items():
        times = []
        for i in range(ITERATIONS):
            sql = f"/* {phase} {qname} iter{i} {uuid.uuid4().hex[:8]} */ " + template.format(
                t=table
            )
            t0 = time.time()
            run_sql(cfg, sql)
            times.append(round(time.time() - t0, 2))
        out[qname] = {
            "iterations_sec": times,
            "median_sec": round(statistics.median(times), 2),
            "min_sec": min(times),
        }
        print(f"{table} [{phase}] {qname}: median {out[qname]['median_sec']}s {times}")
    return out


def table_files(cfg, table: str) -> dict:
    """numFiles/sizeInBytes from DESCRIBE DETAIL, resolved by column name via the
    statement manifest (DESCRIBE DETAIL is not usable as a subquery)."""
    from _common import workspace_client

    w = workspace_client(cfg)
    resp = w.statement_execution.execute_statement(
        warehouse_id=cfg.warehouse_id,
        statement=f"DESCRIBE DETAIL {table}",
        wait_timeout="50s",
    )
    if not (resp.result and resp.result.data_array and resp.manifest):
        return {}
    cols = [c.name for c in resp.manifest.schema.columns]
    row = dict(zip(cols, resp.result.data_array[0]))
    return {"num_files": int(row["numFiles"]), "size_bytes": int(row["sizeInBytes"])}


def main() -> None:
    cfg = load_config()
    tables = {
        "clustered": f"{cfg.uc_catalog}.{cfg.uc_schema}.a_scale_clustered",
        "plain": f"{cfg.uc_catalog}.{cfg.uc_schema}.a_scale_plain",
    }
    results = {}
    for label, fq in tables.items():
        entry = {"table": fq, "files_as_landed": table_files(cfg, fq)}
        entry["reads_as_landed"] = run_suite(cfg, fq, "as-landed")
        t0 = time.time()
        run_sql(cfg, f"OPTIMIZE {fq} FULL")
        entry["optimize_full_sec"] = round(time.time() - t0, 1)
        entry["files_post_optimize"] = table_files(cfg, fq)
        entry["reads_post_optimize"] = run_suite(cfg, fq, "post-optimize")
        results[label] = entry
    results["run_id"] = cfg.run_id
    print(json.dumps(results, indent=2))
    record_result("clustering_reads", results)


if __name__ == "__main__":
    main()
