"""Row 02: does automatic change data feed work on each format -- batch, SQL table_changes,
and Structured Streaming readChangeFeed -- and does it capture commits written by DuckDB?

Evidence comes from the DBR job (`make spark`) plus table_changes on the serverless SQL
warehouse. dml_iceberg carries Databricks commits (insert/update/delete) followed by DuckDB
commits (insert/update/delete/merge from row 01); dml_delta carries only Databricks commits
because no DuckDB write to Delta succeeded.
"""

from _common import FQ, short, spark_raw, sql, write_result

# DuckDB-written changes on dml_iceberg (row 01): id 1 -> 111 (UPDATE), id 4 -> 44 (MERGE), id 5 insert
DUCKDB_MARKERS = {("1", "111", "update_postimage"), ("4", "44", "update_postimage"), ("5", "50", "insert")}


def as_set(rows):
    return {(str(r[0]), str(r[1]), str(r[2])) for r in rows}


def main() -> None:
    raw = spark_raw()
    c = raw["cells"]
    evidence = {"dbr": raw.get("dbr"), "spark_version": raw.get("spark_version")}
    cells = {}
    for fmt in ("delta", "iceberg"):
        names = {
            "batch": f"cdf_batch_readChangeFeed_{fmt}",
            "sql": f"cdf_sql_table_changes_{fmt}",
            "stream": f"cdf_stream_readChangeFeed_{fmt}",
            "history": f"cdf_history_{fmt}",
        }
        ev = {k: c[v] for k, v in names.items()}
        dbsql = sql(f"SELECT id, v, _change_type, _commit_version FROM table_changes('{FQ}.dml_{fmt}', 0)")
        ev["dbsql_table_changes"] = {"ok": dbsql.succeeded, "rows": dbsql.rows, "error": dbsql.error}
        evidence[fmt] = ev
        ok = {k: ev[k]["ok"] for k in ("batch", "sql", "stream")}
        note = [
            (
                f"DBR job: batch={short(ev['batch'])}, table_changes={short(ev['sql'])}, "
                f"streaming={short(ev['stream'])}"
            ),
            f"DBSQL warehouse table_changes: {'ok' if dbsql.succeeded else (dbsql.error or '')[:200]}",
        ]
        external = None
        if fmt == "iceberg" and ok["batch"]:
            found = DUCKDB_MARKERS & as_set(ev["batch"]["value"])
            external = found == DUCKDB_MARKERS
            note.append(f"DuckDB-written changes present in CDF: {sorted(found)} of {sorted(DUCKDB_MARKERS)}")
        if all(ok.values()) and external is not False:
            status = "pass"
        elif any(ok.values()) or dbsql.succeeded:
            status = "partial"
        else:
            status = "fail"
        cells[fmt] = {"status": status, "finding": " | ".join(note)}
    write_result(
        "02_change_data_feed",
        question="Does automatic CDF (batch, table_changes, streaming readChangeFeed) work, including on externally written commits?",
        cells=cells,
        evidence=evidence,
    )


if __name__ == "__main__":
    main()
