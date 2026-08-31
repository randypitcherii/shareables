"""Generic CRUD + aggregation battery, shared by every (engine, format) row.

Each operation is probed independently: success records wall-clock timing and
normalized cost; failure records the server's error text verbatim. A failed op
never aborts the battery — "unsupported, with evidence" is a first-class result.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common import (  # noqa: E402
    AGG_QUERIES,
    CHECKSUM_SQL,
    EVENT_COLUMNS,
    SINGLE_ROW_SEQ,
    SINGLE_ROW_VALUES,
    record_result,
    section,
    timing_stats,
)


def probe(fn, cost_fn) -> dict:
    try:
        rows, elapsed = fn()
        out = {
            "status": "ok",
            "elapsed_ms": round(elapsed * 1000),
            "cost_usd": cost_fn(elapsed),
        }
        if rows:
            out["rows"] = rows[0] if len(rows) == 1 else len(rows)
        return out
    except Exception as e:  # noqa: BLE001 — error text IS the finding
        return {"status": "error", "error": str(e)[:1500]}


def run_battery(
    *,
    row_key: str,
    engine: str,
    fmt: str,
    table: str,
    create_sql: str,
    gen_select: str,
    exec_fn,
    cost_fn,
    identity: dict,
    agg_runs: int,
    row_count: int,
    drop_first: bool = True,
    extra: dict | None = None,
) -> dict:
    ops: dict = {}

    section(f"{row_key}: create {table}")
    if drop_first:
        try:
            exec_fn(f"DROP TABLE IF EXISTS {table}")
        except Exception as e:  # noqa: BLE001
            print(f"pre-drop failed (continuing): {e}")
    ops["create"] = probe(lambda: exec_fn(create_sql), cost_fn)
    print(ops["create"])

    if ops["create"]["status"] == "ok":
        section(f"{row_key}: bulk insert {row_count}")
        ops["bulk_insert_100k"] = probe(
            lambda: exec_fn(f"INSERT INTO {table} {EVENT_COLUMNS} {gen_select}"), cost_fn
        )
        print(ops["bulk_insert_100k"])

        section(f"{row_key}: single-row insert / update / delete")
        ops["insert_1"] = probe(
            lambda: exec_fn(f"INSERT INTO {table} {EVENT_COLUMNS} VALUES {SINGLE_ROW_VALUES}"),
            cost_fn,
        )
        print(ops["insert_1"])
        ops["update_1"] = probe(
            lambda: exec_fn(f"UPDATE {table} SET value = 43.43 WHERE seq = {SINGLE_ROW_SEQ}"),
            cost_fn,
        )
        print(ops["update_1"])
        ops["delete_1"] = probe(
            lambda: exec_fn(f"DELETE FROM {table} WHERE seq = {SINGLE_ROW_SEQ}"), cost_fn
        )
        print(ops["delete_1"])

        # If the single-row delete is unsupported, the probe row may still exist;
        # aggregations tolerate it (they filter nothing) but the checksum below
        # records whatever state the table truly reached.
        section(f"{row_key}: aggregations x{agg_runs}")
        for name, sql in AGG_QUERIES.items():
            timings: list[float] = []
            failure = None
            for _ in range(agg_runs):
                try:
                    _, elapsed = exec_fn(sql.format(table=table))
                    timings.append(elapsed)
                except Exception as e:  # noqa: BLE001
                    failure = str(e)[:1500]
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

        section(f"{row_key}: bulk update / delete (~50% of rows)")
        ops["bulk_update"] = probe(
            lambda: exec_fn(f"UPDATE {table} SET value = value + 1 WHERE seq % 2 = 0"), cost_fn
        )
        print(ops["bulk_update"])
        ops["bulk_delete"] = probe(
            lambda: exec_fn(f"DELETE FROM {table} WHERE seq % 2 = 0"), cost_fn
        )
        print(ops["bulk_delete"])

        section(f"{row_key}: final checksum")
        ops["final_checksum"] = probe(lambda: exec_fn(CHECKSUM_SQL.format(table=table)), cost_fn)
        print(ops["final_checksum"])

    payload = {
        "engine": engine,
        "format": fmt,
        "table": table,
        "identity": identity,
        "ops": ops,
        **(extra or {}),
    }
    record_result(row_key, payload)
    return payload
