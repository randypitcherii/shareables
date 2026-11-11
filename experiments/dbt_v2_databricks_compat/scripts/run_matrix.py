"""Run every local findings-matrix row against a real workspace.

Phases, in order (each row's verdict needs the phases before it):

  1. install + connect   `dbt --version`, `dbt debug` per auth style, `dbt deps`
  2. first build         `dbt build --full-refresh --vars {batch: 1}`
  3. second build        land a second file in the volume, then
                         `dbt build --vars {batch: 2}` (no full refresh), so every
                         incremental strategy, the snapshot, the streaming table,
                         and the materialized view run their *second* pass
  4. read back           SQL against the warehouse proves each feature did what
                         it claims: row counts, clustering keys, table format,
                         tags, constraints, comments
  5. artifacts           `dbt docs generate`, `dbt source freshness`

A row is ✅ only when its dbt nodes succeeded on both passes AND the read-back
matches the expected outcome. Results are merged into
results/matrix_results.json (Jobs rows are written by run_jobs.py).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from _common import (
    FQ_SCHEMA,
    PROJECT_DIR,
    client,
    dbt_env,
    load_results,
    now_iso,
    save_results,
    sql,
)
from setup import land_file
from setup import main as setup_main

DBT = Path(os.environ.get("DBT_V2_BIN", PROJECT_DIR.parent / ".dbt-v2" / "bin" / "dbt"))
ANSI = re.compile(r"\x1b\[[0-9;]*m")


# --------------------------------------------------------------------------- dbt


def dbt(env: dict[str, str], *args: str) -> dict[str, Any]:
    """Run one dbt command; return exit code, cleaned output, and node statuses."""
    run_results = PROJECT_DIR / "target" / "run_results.json"
    run_results.unlink(missing_ok=True)
    proc = subprocess.run(
        [str(DBT), *args, "--profiles-dir", str(PROJECT_DIR)],
        cwd=PROJECT_DIR,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    output = ANSI.sub("", proc.stdout + proc.stderr)
    nodes: dict[str, dict[str, str]] = {}
    if run_results.exists():
        for r in json.loads(run_results.read_text()).get("results", []):
            nodes[r["unique_id"]] = {"status": r["status"], "message": r.get("message") or ""}
    print(f"  $ dbt {' '.join(args)} -> exit {proc.returncode}", flush=True)
    return {"exit": proc.returncode, "output": output, "nodes": nodes}


def first_error(output: str) -> str:
    """The first `[error]` block of dbt's summary, trimmed to one quotable line."""
    lines = [x.strip() for x in output.splitlines()]
    for i, line in enumerate(lines):
        if line.startswith("[error]"):
            block = []
            for x in lines[i:]:
                if block and x.startswith(("[", "Check ", "====")):
                    break
                block.append(x)
            text = " ".join(x for x in block if x)
            return re.sub(r"RequestId=[0-9a-f-]+", "RequestId=…", text)[:400]
    return ""


def node_ids(manifest_tag: str) -> list[str]:
    manifest = json.loads((PROJECT_DIR / "target" / "manifest.json").read_text())
    out = []
    for section in ("nodes", "unit_tests"):
        for uid, node in manifest.get(section, {}).items():
            if manifest_tag in (node.get("tags") or node.get("config", {}).get("tags") or []):
                out.append(uid)
    return sorted(out)


def nodes_ok(build: dict[str, Any], uids: list[str]) -> tuple[bool, str]:
    """All nodes present and succeeded/passed/warned; else the first failure."""
    if not uids:
        return False, "no nodes selected"
    for uid in uids:
        node = build["nodes"].get(uid)
        if node is None:
            return False, f"{uid} did not run"
        if node["status"] not in ("success", "pass", "warn"):
            return False, f"{uid}: {node['status']} — {node['message'][:300]}"
    return True, ""


# ------------------------------------------------------------------- read-backs


def scalar(w, statement: str) -> Any:
    rows = sql(w, statement)
    return rows[0][0] if rows else None


def count(w, table: str) -> int:
    return int(scalar(w, f"SELECT count(*) FROM {FQ_SCHEMA}.{table}"))


def describe_json(w, table: str) -> dict[str, Any]:
    return json.loads(scalar(w, f"DESCRIBE TABLE EXTENDED {FQ_SCHEMA}.{table} AS JSON"))


# ------------------------------------------------------------------------- rows

ROWS: list[dict[str, Any]] = []


def row(rid: str, question: str, tag: str | None = None) -> Callable:
    def register(fn: Callable) -> Callable:
        ROWS.append({"id": rid, "question": question, "tag": tag, "check": fn})
        return fn

    return register


def expect(observed: Any, expected: Any, what: str) -> tuple[str, str]:
    if observed == expected:
        return "pass", f"{what}: {observed!r} (expected {expected!r})"
    return "fail", f"{what}: {observed!r}, expected {expected!r}"


@row("r01", "Does `dbt-oss` install as one wheel with no separate Databricks adapter package?")
def r01(ctx):
    v = ctx["version"]
    ok = v["exit"] == 0 and "dbt-oss" in v["output"]
    return ("pass" if ok else "fail"), v["output"].strip().splitlines()[0] if v["output"] else ""


@row("r02", "Does a bearer token (`token:` profile field) connect? (`dbt debug`)")
def r02(ctx):
    d = ctx["debug_token"]
    return ("pass" if d["exit"] == 0 else "fail"), (
        "dbt debug: All checks passed" if d["exit"] == 0 else first_error(d["output"])
    )


@row("r03", "Does `auth_type: databricks_cli` (reuse the Databricks CLI login) connect?")
def r03(ctx):
    d = ctx["debug_cli"]
    if d["exit"] == 0:
        return "pass", "dbt debug: All checks passed"
    return "fail", first_error(d["output"]) or d["output"].strip()[-300:]


@row("r04", "Do `dbt deps` (dbt_utils from the hub) and `dbt seed` work?", "r04_seed")
def r04(ctx):
    if ctx["deps"]["exit"] != 0:
        return "fail", first_error(ctx["deps"]["output"])
    ok, why = nodes_ok(ctx["build1"], node_ids("r04_seed"))
    if not ok:
        return "fail", why
    return expect(count(ctx["w"], "events"), 8, "events seed rows")


def simple(tag: str, table: str | None = None, rows: int | None = None):
    def check(ctx):
        uids = node_ids(tag)
        for name in ("build1", "build2"):
            ok, why = nodes_ok(ctx[name], uids)
            if not ok:
                return "fail", f"{name}: {why}"
        if table is None:
            return "pass", f"{len(uids)} node(s) succeeded on both builds"
        return expect(count(ctx["w"], table), rows, f"{table} rows after build 2")

    return check


row("r05", "Do `view` models build?", "r05_view")(simple("r05_view", "r05_view", 8))
row("r06", "Do `table` models build?", "r06_table")(simple("r06_table", "r06_table", 2))


@row("r07", "Are `ephemeral` models inlined into their consumer as a CTE?", "r07_ephemeral")
def r07(ctx):
    # dbt v2 reports the ephemeral node itself as `skipped` in run_results.json
    # (it never executes on its own), so only the consumer is status-checked.
    status, why = simple("r07_ephemeral")(
        {**ctx, **{b: _without(ctx[b], "r07_ephemeral_base") for b in ("build1", "build2")}}
    )
    if status != "pass":
        return status, why
    compiled = next((PROJECT_DIR / "target" / "compiled").rglob("r07_ephemeral_consumer.sql"))
    inlined = "__dbt__cte__r07_ephemeral_base" in compiled.read_text()
    return expect(
        (count(ctx["w"], "r07_ephemeral_consumer"), inlined), (3, True), "(rows, CTE inlined)"
    )


def _without(build: dict[str, Any], name: str) -> dict[str, Any]:
    """A build result with one node's status forced to success (see r07)."""
    nodes = {
        u: (dict(n, status="success") if u.endswith("." + name) else n)
        for u, n in build["nodes"].items()
    }
    return {**build, "nodes": nodes}


@row("r08", "Do generic data tests, singular tests, and unit tests run?")
def r08(ctx):
    tests = [
        u for u, n in ctx["build1"]["nodes"].items() if u.split(".")[0] in ("test", "unit_test")
    ]
    failed = [u for u in tests if ctx["build1"]["nodes"][u]["status"] != "pass"]
    kinds = {u.split(".")[0] for u in tests}
    if failed:
        return "fail", f"failed: {failed}"
    if kinds != {"test", "unit_test"}:
        return "fail", f"only saw {kinds}"
    return "pass", f"{len(tests)} tests passed (data + singular + unit)"


@row("r09", "Does a YAML-defined `check` snapshot close the old row on change?", "r09_snapshot")
def r09(ctx):
    status, why = simple("r09_snapshot")(ctx)
    if status != "pass":
        return status, why
    closed = scalar(
        ctx["w"],
        f"SELECT count(*) FROM {FQ_SCHEMA}.r09_snapshot WHERE customer_id = 1 "
        "AND dbt_valid_to IS NOT NULL",
    )
    return expect((count(ctx["w"], "r09_snapshot"), int(closed)), (4, 1), "(rows, closed rows)")


# Incremental strategies. events.csv: batch 1 = ids 1-5; batch 2 = id 2 again
# (amount 20 -> 25), plus ids 6 and 7. Expected outcomes are written out per
# strategy so a silent semantic change shows up as a ❌, not just a crash.


def incremental(tag: str, table: str, expected_rows: int, expected_id2: float | None):
    def check(ctx):
        status, why = simple(tag)(ctx)
        if status != "pass":
            return status, why
        n = count(ctx["w"], table)
        id2 = scalar(ctx["w"], f"SELECT max(amount) FROM {FQ_SCHEMA}.{table} WHERE event_id = 2")
        observed = (n, None if id2 is None else float(id2))
        return expect(
            observed, (expected_rows, expected_id2), "(rows, amount of id 2) after pass 2"
        )

    return check


row("r10", "Incremental `append`: second pass appends only new rows?", "r10_incr_append")(
    incremental("r10_incr_append", "r10_incr_append", 8, 25.0)
)
row("r11", "Incremental `merge` on `unique_key`: second pass upserts?", "r11_incr_merge")(
    incremental("r11_incr_merge", "r11_incr_merge", 7, 25.0)
)
row(
    "r12",
    "Incremental `delete+insert` on `unique_key`: second pass replaces matching keys?",
    "r12_incr_delete_insert",
)(incremental("r12_incr_delete_insert", "r12_incr_delete_insert", 7, 25.0))
row(
    "r13",
    "Incremental `insert_overwrite` by `partition_by`: only touched partitions replaced?",
    "r13_incr_insert_overwrite",
)(incremental("r13_incr_insert_overwrite", "r13_incr_insert_overwrite", 5, 25.0))
row(
    "r14",
    "Incremental `replace_where` with `incremental_predicates`?",
    "r14_incr_replace_where",
)(incremental("r14_incr_replace_where", "r14_incr_replace_where", 7, 20.0))


@row(
    "r15",
    "Incremental `microbatch`: per-batch runs, and a targeted `--event-time-start/end` backfill?",
    "r15_incr_microbatch",
)
def r15(ctx):
    status, why = simple("r15_incr_microbatch")(ctx)
    if status != "pass":
        return status, why
    bf = ctx["backfill"]
    if bf["exit"] != 0:
        return "fail", f"backfill: {first_error(bf['output'])}"
    batches = len(re.findall(r"Succeeded batch", bf["output"]))
    return expect(
        (count(ctx["w"], "r15_incr_microbatch"), batches),
        (8, 4),
        "(rows, batches run) after Jan–Apr backfill at batch 2",
    )


@row(
    "r16",
    "`streaming_table` reading CSVs from a UC volume with `read_files`: picks up new files?",
    "r16_streaming_table",
)
def r16(ctx):
    status, why = simple("r16_streaming_table")(ctx)
    if status != "pass":
        return status, why
    return expect(
        (ctx["st_rows_build1"], count(ctx["w"], "r16_streaming_table")),
        (3, 5),
        "(rows after file 1, rows after file 2)",
    )


@row("r17", "`materialized_view` builds and refreshes on re-run?", "r17_materialized_view")
def r17(ctx):
    status, why = simple("r17_materialized_view")(ctx)
    if status != "pass":
        return status, why
    orders = scalar(ctx["w"], f"SELECT sum(orders) FROM {FQ_SCHEMA}.r17_materialized_view")
    return expect(int(orders), 5, "orders counted by the MV after the second build")


@row(
    "r18",
    "Liquid clustering: `liquid_clustered_by` and `auto_liquid_cluster`?",
    "r18_liquid_clustering",
)
def r18(ctx):
    status, why = simple("r18_liquid_clustering")(ctx)
    if status != "pass":
        return status, why
    w = ctx["w"]
    cols = describe_json(w, "r18_liquid_clustering").get("clustering_columns")
    auto = describe_json(w, "r18_auto_liquid_clustering").get("table_properties", {})
    auto_on = str(auto.get("clusterByAuto", "")).lower()
    return expect(
        (cols, auto_on),
        (["customer_id", "event_date"], "true"),
        "(clustering columns, clusterByAuto)",
    )


@row("r19", "`table_format='iceberg'` creates a managed Iceberg table?", "r19_managed_iceberg")
def r19(ctx):
    status, why = simple("r19_managed_iceberg")(ctx)
    if status != "pass":
        return status, why
    d = describe_json(ctx["w"], "r19_managed_iceberg")
    return expect(str(d.get("provider", "")).lower(), "iceberg", "provider")


@row(
    "r20",
    "`persist_docs`, `databricks_tags` (table + column), and `tblproperties` land in UC?",
    "r20_tags_persist_docs",
)
def r20(ctx):
    status, why = simple("r20_tags_persist_docs")(ctx)
    if status != "pass":
        return status, why
    w = ctx["w"]
    d = describe_json(w, "r20_tags_persist_docs")
    col = next(c for c in d["columns"] if c["name"] == "customer_id")
    tags = {
        r[0]: r[1]
        for r in sql(
            w,
            "SELECT tag_name, tag_value FROM system.information_schema.table_tags "
            f"WHERE catalog_name = '{FQ_SCHEMA.split('.')[0].strip('`')}' "
            f"AND schema_name = '{FQ_SCHEMA.split('.')[1].strip('`')}' "
            "AND table_name = 'r20_tags_persist_docs'",
        )
    }
    col_tags = sql(
        w,
        "SELECT tag_name, tag_value FROM system.information_schema.column_tags "
        f"WHERE schema_name = '{FQ_SCHEMA.split('.')[1].strip('`')}' "
        "AND table_name = 'r20_tags_persist_docs' AND column_name = 'customer_id'",
    )
    observed = (
        bool(d.get("comment")),
        bool(col.get("comment")),
        tags.get("row"),
        [list(r) for r in col_tags],
        d.get("table_properties", {}).get("dbt_v2_compat.row"),
    )
    expected = (True, True, "r20", [["dbt_v2_compat_column", "r20"]], "r20")
    return expect(
        observed, expected, "(table comment, column comment, table tag, column tags, tblproperty)"
    )


@row(
    "r21",
    "Model contracts with `not_null` + `primary_key` constraints?",
    "r21_contract_constraints",
)
def r21(ctx):
    status, why = simple("r21_contract_constraints")(ctx)
    if status != "pass":
        return status, why
    w = ctx["w"]
    schema = FQ_SCHEMA.split(".")[1].strip("`")
    pk = scalar(
        w,
        "SELECT count(*) FROM system.information_schema.table_constraints "
        f"WHERE table_schema = '{schema}' AND table_name = 'r21_contract_constraints' "
        "AND constraint_type = 'PRIMARY KEY'",
    )
    d = describe_json(w, "r21_contract_constraints")
    nullable = next(c for c in d["columns"] if c["name"] == "customer_id").get("nullable")
    return expect((int(pk), nullable), (1, False), "(primary keys, customer_id nullable)")


@row(
    "r22",
    "Python model with `submission_method: serverless_cluster`?",
    "r22_python_model",
)
def r22(ctx):
    return simple("r22_python_model", "r22_python_model", 3)(ctx)


@row("r23", "`metric_view` materialization creates a UC metric view?", "r23_metric_view")
def r23(ctx):
    status, why = simple("r23_metric_view")(ctx)
    if status != "pass":
        return status, why
    d = describe_json(ctx["w"], "r23_metric_view")
    return expect(str(d.get("type", "")).upper(), "METRIC_VIEW", "table type")


row("r24", "Hub package macros (`dbt_utils.generate_surrogate_key`)?", "r24_dbt_utils_package")(
    simple("r24_dbt_utils_package", "r24_dbt_utils_package", 8)
)


@row("r25", "`dbt docs generate` builds the docs site (and v1's `catalog.json`)?")
def r25(ctx):
    d = ctx["docs"]
    target = PROJECT_DIR / "target"
    if d["exit"] != 0 or not (target / "index.html").exists():
        return "fail", first_error(d["output"]) or "no index.html"
    if (target / "catalog.json").exists():
        return "pass", "index.html + catalog.json written"
    parquet = len(list((target / "info_schema").rglob("*.parquet")))
    return "partial", (
        f"static site (index.html) + {parquet} Parquet artifacts under target/info_schema/; "
        "no catalog.json — tools that read v1's catalog.json need a change"
    )


@row("r26", "`dbt source freshness` against a UC table?")
def r26(ctx):
    f = ctx["freshness"]
    return ("pass" if f["exit"] == 0 else "fail"), (
        "source freshness passed" if f["exit"] == 0 else first_error(f["output"])
    )


# ------------------------------------------------------------------------- main


def main() -> None:
    only = set(sys.argv[1:])
    w = client()
    env = dbt_env(w)
    setup_main()
    ctx: dict[str, Any] = {"w": w}

    print("phase 1: install + connect", flush=True)
    ctx["version"] = dbt(env, "--version")
    ctx["debug_token"] = dbt(env, "debug")
    ctx["debug_cli"] = dbt(env, "debug", "--target", "databricks_cli")
    ctx["deps"] = dbt(env, "deps")

    print("phase 2: first build (batch 1, full refresh)", flush=True)
    ctx["build1"] = dbt(env, "build", "--full-refresh", "--vars", "{batch: 1}")
    try:
        ctx["st_rows_build1"] = count(w, "r16_streaming_table")
    except RuntimeError:
        ctx["st_rows_build1"] = None

    print("phase 3: second build (batch 2, second landing file)", flush=True)
    land_file(w, "orders_002.csv")
    ctx["build2"] = dbt(env, "build", "--vars", "{batch: 2}")
    ctx["backfill"] = dbt(
        env, "run", "-s", "r15_incr_microbatch", "--vars", "{batch: 2}",
        "--event-time-start", "2026-01-01", "--event-time-end", "2026-05-01",
    )  # fmt: skip

    print("phase 5: artifacts", flush=True)
    ctx["docs"] = dbt(env, "docs", "generate")
    ctx["freshness"] = dbt(env, "source", "freshness")

    print("phase 4: read back", flush=True)
    data = load_results()
    data["local_run"] = {
        "at": now_iso(),
        "dbt_version": ctx["version"]["output"].strip().splitlines()[0],
        "compute": "serverless SQL warehouse (PRO); Python model on serverless jobs compute",
    }
    for r in ROWS:
        if only and r["id"] not in only:
            continue
        try:
            status, evidence = r["check"](ctx)
        except Exception as exc:  # noqa: BLE001 - a read-back failure is itself evidence
            status, evidence = "fail", f"read-back error: {str(exc).splitlines()[0][:300]}"
        data["rows"][r["id"]] = {
            "question": r["question"],
            "status": status,
            "evidence": evidence,
            "at": now_iso(),
        }
        mark = {"pass": "✅", "fail": "❌", "partial": "◑"}[status]
        print(f"  {mark} {r['id']} {evidence}", flush=True)
    save_results(data)


if __name__ == "__main__":
    main()
