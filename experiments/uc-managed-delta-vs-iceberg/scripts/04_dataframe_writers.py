"""Row 04: which DataFrame write APIs work against each existing managed table, and what
table format do the create-style writers produce?"""

from _common import short, spark_raw, write_result

WRITERS = [
    "v1_saveAsTable_no_format",
    "v1_saveAsTable_format_delta",
    "v1_saveAsTable_format_iceberg",
    "v1_insertInto",
    "v2_writeTo_append",
]
CREATES = ["create_v1_format_iceberg", "create_v2_using_iceberg", "create_v1_no_format"]


def main() -> None:
    raw = spark_raw()
    c = raw["cells"]
    evidence = {"dbr": raw.get("dbr")}
    cells = {}
    for fmt in ("delta", "iceberg"):
        parts, n_ok, n = [], 0, 0
        for w in [*WRITERS, "DeltaTable_merge"]:
            cell = c[f"writer_{w}_{fmt}"]
            n += 1
            landed = cell["ok"] and (cell["value"].get("rows_landed", 0) == 2 or cell["value"].get("has_999"))
            n_ok += bool(landed)
            parts.append(f"{w}: {'✅' if landed else '❌ ' + short(cell, 160)}")
        evidence[fmt] = {w: c[f"writer_{w}_{fmt}"] for w in [*WRITERS, "DeltaTable_merge"]}
        cells[fmt] = {
            "status": "pass" if n_ok == n else ("fail" if n_ok == 0 else "partial"),
            "finding": "; ".join(parts),
        }
    creates = {k: c[f"writer_{k}"] for k in CREATES}
    evidence["creates"] = creates
    cells["iceberg"]["finding"] += " || creates: " + "; ".join(
        f"{k}: {(v['value']['provider'] if v['ok'] else short(v, 160))}" for k, v in creates.items()
    )
    write_result(
        "04_dataframe_writers",
        question="Do V1 saveAsTable / insertInto, V2 writeTo, and DeltaTable.merge work on each format?",
        cells=cells,
        evidence=evidence,
    )


if __name__ == "__main__":
    main()
