"""Row 06 (mitigation): rows 01 and 05 show external readers rejecting the deletion-vector files
Databricks writes for Iceberg v3. Does a Liquid Iceberg v3 table with deletion vectors turned
off (so Databricks UPDATE/DELETE rewrite files instead) restore strict external reads -- and
what does it cost (row tracking / automatic CDF)?

Variants (each: create -> insert 3 -> UPDATE -> DELETE from Databricks SQL):
- dv_off:          'iceberg.enableDeletionVectors' = 'false'
- dv_and_rt_off:   deletion vectors and row tracking both off
"""

import duckdb
import pyiceberg
from _common import (
    CATALOG,
    FQ,
    attempt,
    duckdb_attach_iceberg,
    host,
    oauth_token,
    sql,
    write_result,
)
from pyiceberg.catalog import load_catalog

VARIANTS = {
    "dv_off": "'iceberg.enableDeletionVectors' = 'false'",
    "dv_and_rt_off": "'iceberg.enableDeletionVectors' = 'false', 'iceberg.enableRowTracking' = 'false'",
}
SEED = [
    "INSERT INTO {t} VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
    "UPDATE {t} SET v = v + 1 WHERE id = 1",
    "DELETE FROM {t} WHERE id = 2",
]


def main() -> None:
    evidence: dict = {"duckdb_version": duckdb.__version__, "pyiceberg_version": pyiceberg.__version__}
    con = duckdb.connect()
    duckdb_attach_iceberg(con)
    cat = load_catalog(
        "uc", type="rest", uri=f"{host()}/api/2.1/unity-catalog/iceberg-rest", token=oauth_token(), warehouse=CATALOG
    )
    for name, props in VARIANTS.items():
        t = f"{FQ}.mit_{name}"
        sql(f"DROP TABLE IF EXISTS {t}")
        create = sql(
            f"CREATE TABLE {t} (id BIGINT, cat STRING, v BIGINT) USING ICEBERG CLUSTER BY (cat) "
            f"TBLPROPERTIES ('format-version' = '3', {props})"
        )
        ev = {"create": {"ok": create.succeeded, "error": create.error}}
        if create.succeeded:
            ev["seed"] = [
                {"ok": r.succeeded, "error": r.error} for r in (sql(s.format(t=t)) for s in SEED)
            ]
            props_now = sql(f"SHOW TBLPROPERTIES {t}")
            ev["props"] = {k: v for k, v in props_now.rows if k.startswith(("iceberg.", "format-version"))}
            ev["duckdb_strict_read"] = attempt(
                lambda name=name: con.execute(f"SELECT * FROM ice.matrix.mit_{name} ORDER BY id").fetchall()
            )
            ev["duckdb_update"] = attempt(
                lambda name=name: con.execute(f"UPDATE ice.matrix.mit_{name} SET v = v + 100 WHERE id = 3").fetchall()
            )
            ev["pyiceberg_read"] = attempt(
                lambda name=name: len(cat.load_table(f"matrix.mit_{name}").scan().to_arrow())
            )
            cdf = sql(f"SELECT id, v, _change_type, _commit_version FROM table_changes('{t}', 0)")
            ev["dbsql_table_changes"] = {"ok": cdf.succeeded, "rows": cdf.rows, "error": cdf.error}
        evidence[name] = ev

    def verdict(ev):
        if not ev["create"]["ok"]:
            return "fail", f"CREATE rejected: {ev['create']['error']}"
        reads = ev["duckdb_strict_read"]["ok"] and ev["pyiceberg_read"]["ok"]
        bits = [
            f"DuckDB strict read={'ok' if ev['duckdb_strict_read']['ok'] else ev['duckdb_strict_read']['error'][:150]}",
            f"DuckDB UPDATE={'ok' if ev['duckdb_update']['ok'] else ev['duckdb_update']['error'][:400]}",
            f"PyIceberg read={'ok' if ev['pyiceberg_read']['ok'] else ev['pyiceberg_read']['error'][:150]}",
            f"automatic CDF (DBSQL)={'ok' if ev['dbsql_table_changes']['ok'] else (ev['dbsql_table_changes']['error'] or '')[:200]}",
        ]
        full = reads and ev["dbsql_table_changes"]["ok"] and ev["duckdb_update"]["ok"]
        status = "pass" if full else ("partial" if reads else "fail")
        return status, "; ".join(bits)

    s1, f1 = verdict(evidence["dv_off"])
    s2, f2 = verdict(evidence["dv_and_rt_off"])
    best = "pass" if "pass" in (s1, s2) else ("partial" if "partial" in (s1, s2) else "fail")
    write_result(
        "06_iceberg_dv_off_mitigation",
        question="Does turning off deletion vectors on Liquid Iceberg v3 restore strict external reads, and at what cost?",
        cells={
            "delta": {"status": "not_applicable", "finding": "Iceberg-only mitigation."},
            "iceberg": {"status": best, "finding": f"[DVs off] {s1}: {f1} || [DVs + row tracking off] {s2}: {f2}"},
        },
        evidence=evidence,
    )


if __name__ == "__main__":
    main()
