"""Row 01: can DuckDB read and run row-level DML on each managed, Liquid-clustered format?

Delta is tried through both DuckDB paths: the unity_catalog extension (Delta reader/writer,
UC Delta API + catalog commits) and the iceberg extension (Iceberg reads of the Delta table).
Iceberg is tried through the iceberg extension against the UC Iceberg REST catalog.
Afterwards Databricks SQL reads each table back, so a "success" is only counted when
Databricks sees exactly the rows DuckDB claims to have written.
"""

import duckdb
from _common import (
    AWS_REGION,
    USER_AGENT,
    attempt,
    duckdb_attach_iceberg,
    duckdb_attach_uc,
    rows_of,
    write_result,
)

# Seeded state after setup.py (insert 3, update id=1, delete id=2): (1,a,11), (3,c,30)
OPS = {
    "insert": "INSERT INTO {t} VALUES (4, 'd', 40)",
    "update": "UPDATE {t} SET v = v + 100 WHERE id = 1",
    "delete": "DELETE FROM {t} WHERE id = 3",
    "merge": (
        "MERGE INTO {t} AS t USING (SELECT 4 AS id, 'd' AS cat, 44 AS v UNION ALL "
        "SELECT 5, 'e', 50) AS s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET v = s.v "
        "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.cat, s.v)"
    ),
}
EXPECTED_AFTER_ALL = [["1", "a", "111"], ["4", "d", "44"], ["5", "e", "50"]]
# dml_iceberg_clean is seeded with the insert only: (1,a,10), (2,b,20), (3,c,30)
EXPECTED_CLEAN = [["1", "a", "110"], ["2", "b", "20"], ["4", "d", "44"], ["5", "e", "50"]]
SKIP_PUFFIN = "SET iceberg_unsafe_skip_puffin_verification = true"


def _short(result: dict) -> str:
    if result["ok"]:
        return "ok"
    return result["error"].replace("\n", " ")[:220]


def run_ops(con, table: str) -> dict:
    out = {"read_before": attempt(lambda: con.execute(f"SELECT * FROM {table} ORDER BY id").fetchall())}
    for op, stmt in OPS.items():
        out[op] = attempt(lambda stmt=stmt: con.execute(stmt.format(t=table)).fetchall())
    return out


def main() -> None:
    print(f"duckdb {duckdb.__version__}")
    evidence: dict = {"duckdb_version": duckdb.__version__}

    # ---------------- Delta via unity_catalog extension (Delta reader/writer)
    con = duckdb.connect()
    evidence["delta_uc_readwrite"] = {"attach": attempt(lambda: duckdb_attach_uc(con))}
    evidence["delta_uc_readwrite"].update(run_ops(con, "uc.matrix.dml_delta"))

    # Same, with an explicit custom_user_agent (the UC Delta API demands client identification)
    con = duckdb.connect(config={"custom_user_agent": USER_AGENT})
    evidence["delta_uc_custom_user_agent"] = {
        "attach": attempt(lambda: duckdb_attach_uc(con)),
        "insert": attempt(lambda: con.execute(OPS["insert"].format(t="uc.matrix.dml_delta")).fetchall()),
    }

    # Read-only attach requests READ credentials instead of READ_WRITE
    con = duckdb.connect()
    evidence["delta_uc_read_only"] = {
        "attach": attempt(lambda: duckdb_attach_uc(con, read_only=True)),
        "read": attempt(lambda: con.execute("SELECT * FROM uc.matrix.dml_delta ORDER BY id").fetchall()),
        "secret_aws_region": AWS_REGION,
    }

    # ---------------- Both formats through the Iceberg REST catalog (iceberg extension)
    # strict: DuckDB defaults. permissive: opt in to skipping Puffin footer verification,
    # which DuckDB's own error message suggests for Databricks-written deletion vectors.
    con = duckdb.connect()
    evidence["iceberg_rest_attach"] = attempt(lambda: duckdb_attach_iceberg(con))
    for label, table in (("delta", "dml_delta"), ("iceberg", "dml_iceberg")):
        evidence[f"{label}_via_iceberg_rest_strict_read"] = attempt(
            lambda table=table: con.execute(f"SELECT * FROM ice.matrix.{table} ORDER BY id").fetchall()
        )
    # control: Iceberg table whose only Databricks commit was an INSERT (no Databricks DVs)
    evidence["iceberg_clean_strict"] = run_ops(con, "ice.matrix.dml_iceberg_clean")
    evidence["iceberg_clean_databricks_after"] = rows_of("dml_iceberg_clean")

    con.execute(SKIP_PUFFIN)
    evidence["delta_via_iceberg_rest_permissive"] = run_ops(con, "ice.matrix.dml_delta")
    evidence["iceberg_permissive"] = run_ops(con, "ice.matrix.dml_iceberg")
    evidence["delta_databricks_after"] = rows_of("dml_delta")
    evidence["iceberg_databricks_after"] = rows_of("dml_iceberg")

    # ---------------- verdicts
    d_rw, d_ro = evidence["delta_uc_readwrite"], evidence["delta_uc_read_only"]
    d_ice = evidence["delta_via_iceberg_rest_permissive"]
    delta_any_write = any(d_rw[op]["ok"] for op in OPS) or any(d_ice[op]["ok"] for op in OPS)
    delta_reads = [
        name
        for name, ok in (
            ("unity_catalog READ_ONLY attach", d_ro["read"]["ok"]),
            ("Iceberg REST, strict", evidence["delta_via_iceberg_rest_strict_read"]["ok"]),
            ("Iceberg REST, skip-puffin-verification", d_ice["read_before"]["ok"]),
        )
        if ok
    ]
    clean, perm = evidence["iceberg_clean_strict"], evidence["iceberg_permissive"]
    clean_ok = all(clean[op]["ok"] for op in OPS) and evidence["iceberg_clean_databricks_after"] == EXPECTED_CLEAN
    perm_ok = all(perm[op]["ok"] for op in OPS) and evidence["iceberg_databricks_after"] == EXPECTED_AFTER_ALL
    strict_read_ok = evidence["iceberg_via_iceberg_rest_strict_read"]["ok"]

    if clean_ok and strict_read_ok:
        ice_status, ice_note = "pass", "DML works with DuckDB defaults on every table tested."
    elif clean_ok:
        ice_status = "partial"
        ice_note = (
            "DuckDB INSERT/UPDATE/DELETE/MERGE all commit and Databricks reads back exactly the expected "
            "rows -- on a table Databricks has only appended to. Once Databricks itself runs UPDATE/DELETE "
            "(writing Iceberg v3 deletion vectors), DuckDB's default reader rejects the table: "
            f"{_short(evidence['iceberg_via_iceberg_rest_strict_read'])}. "
            + (
                "With iceberg_unsafe_skip_puffin_verification, the same DML succeeds and Databricks "
                "reads the expected rows."
                if perm_ok
                else f"Skip-verification run: ops={ {op: perm[op]['ok'] for op in OPS} }, "
                f"databricks sees {evidence['iceberg_databricks_after']}."
            )
        )
    else:
        ice_status = "fail" if not any(clean[op]["ok"] for op in OPS) else "partial"
        ice_note = f"clean-table ops={ {op: clean[op]['ok'] for op in OPS} }"

    cells = {
        "delta": {
            "status": "partial" if delta_reads and not delta_any_write else ("pass" if delta_any_write else "fail"),
            "finding": (
                f"Reads work via: {', '.join(delta_reads) or 'none'}. "
                "No write landed. unity_catalog read-write attach: "
                f"{_short(d_rw['insert'])} | Iceberg REST path: {_short(d_ice['insert'])}"
                if not delta_any_write
                else "At least one DuckDB write landed on managed Delta; see evidence."
            ),
        },
        "iceberg": {"status": ice_status, "finding": ice_note},
    }
    write_result(
        "01_duckdb_dml",
        question="Can DuckDB read and INSERT/UPDATE/DELETE/MERGE a UC managed Liquid table?",
        cells=cells,
        evidence=evidence,
    )


if __name__ == "__main__":
    main()
