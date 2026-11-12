"""Row 05: can a plain (non-Databricks, non-DuckDB) Iceberg client write through the UC Iceberg
REST catalog? PyIceberg tries read / append / delete / overwrite / upsert against:

- py_iceberg_v3: managed Iceberg v3 (Liquid), Databricks-side UPDATE/DELETE already applied
- py_iceberg_v2: managed Iceberg v2 (Liquid, DVs + row tracking off), same seed history
- py_delta:      managed Delta with Iceberg v3 reads, same seed history
Databricks SQL reads each table back afterwards.
"""

import pyarrow as pa
import pyiceberg
from _common import CATALOG, attempt, host, oauth_token, rows_of, write_result
from pyiceberg.catalog import load_catalog

TABLES = {"py_iceberg_v3": "iceberg", "py_iceberg_v2": "iceberg_v2", "py_delta": "delta"}


def batch(tbl, rows):
    schema = tbl.schema().as_arrow()
    cols = list(zip(*rows, strict=True))
    return pa.Table.from_arrays([pa.array(c, type=f.type) for c, f in zip(cols, schema, strict=True)], schema=schema)


def ops(tbl) -> dict:
    out = {
        "format_version": attempt(lambda: tbl.metadata.format_version),
        "read": attempt(lambda: sorted(tbl.scan().to_arrow().to_pylist(), key=lambda r: r["id"])),
    }
    out["append"] = attempt(lambda: tbl.append(batch(tbl, [(4, "d", 40)])))
    tbl = tbl.refresh()
    out["delete"] = attempt(lambda: tbl.delete("id == 3"))
    tbl = tbl.refresh()
    out["overwrite_filter"] = attempt(lambda: tbl.overwrite(batch(tbl, [(1, "a", 111)]), overwrite_filter="id == 1"))
    tbl = tbl.refresh()
    out["upsert"] = attempt(
        lambda: str(tbl.upsert(batch(tbl, [(4, "d", 44), (5, "e", 50)]), join_cols=["id"]))
    )
    return out


def main() -> None:
    print(f"pyiceberg {pyiceberg.__version__}")
    cat = load_catalog(
        "uc",
        type="rest",
        uri=f"{host()}/api/2.1/unity-catalog/iceberg-rest",
        token=oauth_token(),
        warehouse=CATALOG,
    )
    evidence: dict = {"pyiceberg_version": pyiceberg.__version__}
    for name in TABLES:
        load = attempt(lambda name=name: cat.load_table(f"matrix.{name}"))
        evidence[name] = {"load": {"ok": load["ok"], "error": load.get("error")}}
        if load["ok"]:
            evidence[name].update(ops(cat.load_table(f"matrix.{name}")))
        evidence[name]["databricks_after"] = rows_of(name)
    # read-only probe: a table with both Databricks-written and DuckDB-written deletion vectors
    evidence["read_dml_iceberg_mixed_dvs"] = attempt(
        lambda: len(cat.load_table("matrix.dml_iceberg").scan().to_arrow())
    )

    write_ops = ("append", "delete", "overwrite_filter", "upsert")

    def summarize(name):
        e = evidence[name]
        if not e["load"]["ok"]:
            return "fail", f"load_table failed: {e['load']['error']}"
        okmap = {op: e[op]["ok"] for op in ("read", *write_ops)}
        errs = {op: e[op]["error"][:200] for op in ("read", *write_ops) if not e[op]["ok"]}
        n_ok = sum(okmap[op] for op in write_ops)
        status = "pass" if okmap["read"] and n_ok == len(write_ops) else ("fail" if n_ok == 0 and not okmap["read"] else "partial")
        return status, f"ops={okmap}; databricks reads {e['databricks_after']}; errors={errs}"

    v3s, v3f = summarize("py_iceberg_v3")
    v2s, v2f = summarize("py_iceberg_v2")
    ds, df = summarize("py_delta")
    cells = {
        "delta": {"status": ds, "finding": f"[Delta with Iceberg v3 reads] {df}"},
        "iceberg": {
            "status": "pass" if v3s == "pass" or v2s == "pass" else ("partial" if "partial" in (v3s, v2s) else "fail"),
            "finding": f"[v3] {v3s}: {v3f} || [v2 control] {v2s}: {v2f}",
        },
    }
    write_result(
        "05_pyiceberg_writes",
        question="Can PyIceberg read/append/delete/overwrite/upsert through the UC Iceberg REST catalog?",
        cells=cells,
        evidence=evidence,
    )


if __name__ == "__main__":
    main()
