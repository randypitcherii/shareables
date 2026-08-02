"""Interop — Databricks SQL reading and writing a table CREATEd by StarRocks.

StarRocks created this table through the UC Iceberg REST catalog (battery 02).
That CREATE succeeded, so the table is a genuine UC managed Iceberg table; this
script asks whether the warehouse treats it as a first-class managed table:
identity/format from UC metadata, then bulk INSERT, aggregation, UPDATE, DELETE.

Runs entirely from the operator host — no StarRocks connectivity required, so it
still produces a valid interop row when the StarRocks node cannot reach the
workspace (see the IP-ACL caveat in the README).
"""

import functools
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common import (  # noqa: E402
    AGG_QUERIES,
    CHECKSUM_SQL,
    EVENT_COLUMNS,
    dbsql_cost_usd,
    dbsql_exec,
    gen_select_dbsql,
    load_config,
    record_result,
    section,
    timing_stats,
    warehouse_dbu_per_hour,
    workspace_client,
)

SR_AUTHORED = "events_sr_iceberg"
PROBE_SEQ = 200_000_003


def probe(fn, cost_fn) -> dict:
    try:
        rows, elapsed = fn()
        out = {"status": "ok", "elapsed_ms": round(elapsed * 1000), "cost_usd": cost_fn(elapsed)}
        if rows:
            out["rows"] = rows[0] if len(rows) == 1 else len(rows)
        return out
    except Exception as e:  # noqa: BLE001 — error text is the finding
        return {"status": "error", "error": str(e)[:1500]}


def main() -> None:
    cfg = load_config()
    w = workspace_client(cfg)
    dbu, size = warehouse_dbu_per_hour(cfg, w)
    ident_rows, _ = dbsql_exec(cfg, "SELECT current_user()", w)
    exec_fn = functools.partial(dbsql_exec, cfg, w=w)
    cost_fn = functools.partial(dbsql_cost_usd, dbu, cfg.usd_per_dbu)
    table = f"{cfg.uc_catalog}.{cfg.uc_schema}.{SR_AUTHORED}"

    section("UC metadata for the StarRocks-created table")
    meta = {}
    try:
        t = w.tables.get(table)
        meta = {
            "table_type": str(t.table_type),
            "data_source_format": str(t.data_source_format),
            "owner": t.owner,
        }
    except Exception as e:  # noqa: BLE001
        meta = {"error": str(e)[:500]}
    print(meta)

    ops = {}
    section("DBSQL bulk insert into the StarRocks-created table")
    ops["bulk_insert_100k"] = probe(
        lambda: exec_fn(f"INSERT INTO {table} {EVENT_COLUMNS} {gen_select_dbsql(cfg.row_count)}"),
        cost_fn,
    )
    print(ops["bulk_insert_100k"])

    section("DBSQL read + aggregations")
    ops["checksum"] = probe(lambda: exec_fn(CHECKSUM_SQL.format(table=table)), cost_fn)
    print(ops["checksum"])
    for name, sql in AGG_QUERIES.items():
        timings = []
        failure = None
        for _ in range(cfg.agg_runs):
            try:
                _, elapsed = exec_fn(sql.format(table=table))
                timings.append(elapsed)
            except Exception as e:  # noqa: BLE001
                failure = str(e)[:1200]
                break
        if failure:
            ops[name] = {"status": "error", "error": failure}
        else:
            stats = timing_stats(timings)
            ops[name] = {
                "status": "ok",
                **stats,
                "cost_usd_per_query_p50": cost_fn(stats["p50_ms"] / 1000),
            }
        print(name, ops[name])

    section("DBSQL row-level DML on the StarRocks-created table")
    values = f"({PROBE_SEQ}, 'interop-probe', '2026-01-03 00:00:00', 'dev-x', 'us-east', 1.5, 'p')"
    ops["insert_1"] = probe(
        lambda: exec_fn(f"INSERT INTO {table} {EVENT_COLUMNS} VALUES {values}"), cost_fn
    )
    print(ops["insert_1"])
    ops["update_1"] = probe(
        lambda: exec_fn(f"UPDATE {table} SET value = 2.5 WHERE seq = {PROBE_SEQ}"), cost_fn
    )
    print(ops["update_1"])
    ops["delete_1"] = probe(
        lambda: exec_fn(f"DELETE FROM {table} WHERE seq = {PROBE_SEQ}"), cost_fn
    )
    print(ops["delete_1"])

    record_result(
        "interop_rw_dbsql_on_starrocks_created_iceberg",
        {
            "creator": "starrocks (CREATE TABLE via UC Iceberg REST)",
            "format": "uc-managed-iceberg",
            "accessor": "dbsql-serverless",
            "operation": "read+write",
            "identity": {"dbsql": ident_rows[0][0]},
            "uc_metadata": meta,
            "warehouse_size": size,
            "dbu_per_hour": dbu,
            "ops": ops,
        },
    )


if __name__ == "__main__":
    main()
