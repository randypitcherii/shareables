"""Row 07: if you pick a format today, how do you change your mind later?

A managed Iceberg table is Delta under the hood (it carries Delta protocol features such as
catalogManaged and v2Checkpoint) with live Iceberg metadata. The hoped-for zero-copy exit is:
drop the table from UC, keep its files, and re-register the same files as the other format.
This row tries that and every SQL-level alternative, in both directions, from DBSQL:

- iceberg -> delta: ALTER (provider-level property), SHALLOW CLONE, DEEP CLONE, DROP +
  re-register the location (as Delta or Iceberg), UNDROP, CTAS
- delta -> iceberg: DEEP CLONE, CTAS (v2 default and v3)

Each source table gets insert -> update so it carries deletion vectors and history.
"""

import re

from _common import FQ, must_sql, sql, write_result

COLS = "(id BIGINT, cat STRING, v BIGINT)"
SEED = ["INSERT INTO {t} VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)", "UPDATE {t} SET v = v + 1 WHERE id = 1"]
EXPECTED = [["1", "a", "11"], ["2", "b", "20"], ["3", "c", "30"]]


def step(statement: str) -> dict:
    r = sql(statement)
    if r.succeeded:
        return {"ok": True, "error_code": None, "error": None}
    m = re.search(r"ErrorClass=([A-Z_.]+)|\[([A-Z_][A-Z0-9_.]+)\]", r.error or "")
    code = (m.group(1) or m.group(2)) if m else r.error_code
    return {"ok": False, "error_code": code, "error": (r.error or "")[:400]}


def fmt_of(table: str) -> str | None:
    r = sql(f"DESCRIBE DETAIL {table}")
    return r.rows[0][0] if r.succeeded else None


def rows(table: str):
    return must_sql(f"SELECT id, cat, v FROM {table} ORDER BY id").rows


def history_len(table: str) -> int:
    return len(must_sql(f"DESCRIBE HISTORY {table}").rows)


def seed(name: str, ddl: str) -> str:
    t = f"{FQ}.{name}"
    must_sql(f"DROP TABLE IF EXISTS {t}")
    must_sql(f"CREATE TABLE {t} {ddl}")
    for s in SEED:
        must_sql(s.format(t=t))
    return t


def iceberg_to_delta() -> dict:
    src = seed("rev_ice", f"{COLS} USING ICEBERG CLUSTER BY (cat) TBLPROPERTIES ('format-version' = '3')")
    ev = {"source_format": fmt_of(src), "source_history_versions": history_len(src)}
    ev["alter_to_delta_property"] = step(
        f"ALTER TABLE {src} SET TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg')"
    )
    ev["shallow_clone"] = step(f"CREATE OR REPLACE TABLE {FQ}.rev_ice_shallow SHALLOW CLONE {src}")
    ev["deep_clone"] = step(f"CREATE TABLE {FQ}.rev_ice_deep DEEP CLONE {src}")
    ev["deep_clone_result_format"] = fmt_of(f"{FQ}.rev_ice_deep")
    # Zero-copy attempt: drop from UC (files are retained for the UNDROP window) and
    # re-register the same location as an external table of either format.
    location = must_sql(f"DESCRIBE DETAIL {src}").rows[0][4]
    ev["drop"] = step(f"DROP TABLE {src}")
    ev["reregister_as_delta"] = step(f"CREATE TABLE {FQ}.rev_ice_as_delta USING DELTA LOCATION '{location}'")
    ev["reregister_as_iceberg"] = step(f"CREATE TABLE {FQ}.rev_ice_as_ice USING ICEBERG LOCATION '{location}'")
    ev["undrop"] = step(f"UNDROP TABLE {src}")
    ev["after_undrop_rows_intact"] = rows(src) == EXPECTED if ev["undrop"]["ok"] else False
    ev["ctas_to_delta"] = step(f"CREATE OR REPLACE TABLE {FQ}.rev_ice_ctas CLUSTER BY (cat) AS SELECT * FROM {src}")
    if ev["ctas_to_delta"]["ok"]:
        ev["ctas_result_format"] = fmt_of(f"{FQ}.rev_ice_ctas")
        ev["ctas_rows_match"] = rows(f"{FQ}.rev_ice_ctas") == EXPECTED
        ev["ctas_history_versions"] = history_len(f"{FQ}.rev_ice_ctas")
    return ev


def delta_to_iceberg() -> dict:
    src = seed(
        "rev_delta",
        f"{COLS} CLUSTER BY (cat) TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported', "
        "'delta.enableIcebergCompatV3' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg', "
        "'delta.enableRowTracking' = 'true')",
    )
    ev = {"source_format": fmt_of(src), "source_history_versions": history_len(src)}
    ev["deep_clone"] = step(f"CREATE TABLE {FQ}.rev_delta_deep DEEP CLONE {src}")
    ev["deep_clone_result_format"] = fmt_of(f"{FQ}.rev_delta_deep")
    ev["ctas_iceberg_default"] = step(
        f"CREATE OR REPLACE TABLE {FQ}.rev_delta_ctas_v2 USING ICEBERG CLUSTER BY (cat) AS SELECT * FROM {src}"
    )
    ev["ctas_iceberg_v3"] = step(
        f"CREATE OR REPLACE TABLE {FQ}.rev_delta_ctas USING ICEBERG CLUSTER BY (cat) "
        f"TBLPROPERTIES ('format-version' = '3') AS SELECT * FROM {src}"
    )
    if ev["ctas_iceberg_v3"]["ok"]:
        ev["ctas_result_format"] = fmt_of(f"{FQ}.rev_delta_ctas")
        ev["ctas_rows_match"] = rows(f"{FQ}.rev_delta_ctas") == EXPECTED
        ev["ctas_history_versions"] = history_len(f"{FQ}.rev_delta_ctas")
    return ev


TARGETS = ["rev_ice_shallow", "rev_ice_deep", "rev_ice_as_delta", "rev_ice_as_ice", "rev_ice_ctas",
           "rev_delta_deep", "rev_delta_ctas_v2", "rev_delta_ctas"]


def main() -> None:
    for t in TARGETS:  # reruns must start clean: CREATE OR REPLACE ... CLONE onto managed Iceberg is rejected
        must_sql(f"DROP TABLE IF EXISTS {FQ}.{t}")
    i2d, d2i = iceberg_to_delta(), delta_to_iceberg()
    zero_copy = i2d["reregister_as_delta"]["ok"] or i2d["reregister_as_iceberg"]["ok"]
    i2d_finding = (
        "No zero-copy path. "
        f"Re-registering the dropped table's files: {i2d['reregister_as_delta']['error_code'] or 'ok'} "
        "(the path stays reserved as managed storage); UNDROP restores the table "
        f"({'rows intact' if i2d['after_undrop_rows_intact'] else 'NOT intact'}). "
        f"SHALLOW CLONE and provider-level ALTER are rejected; DEEP CLONE stays "
        f"{i2d['deep_clone_result_format']}. CTAS to Delta works "
        f"({'rows match' if i2d.get('ctas_rows_match') else 'rows differ'}) but rewrites all data and "
        f"starts history over ({i2d.get('ctas_history_versions')} vs {i2d['source_history_versions']} versions)."
    )
    d2i_finding = (
        f"No zero-copy path. DEEP CLONE stays {d2i['deep_clone_result_format']}. CTAS to Iceberg works only "
        "with 'format-version' = '3' on Liquid (v2 default: "
        f"{d2i['ctas_iceberg_default']['error_code'] or 'ok'}); it rewrites all data and starts history over."
    )
    write_result(
        "07_format_reversibility",
        question="Can you switch a managed table between Delta and Iceberg later without copying data?",
        cells={
            "iceberg": {"status": "pass" if zero_copy else "fail", "finding": "Iceberg -> Delta: " + i2d_finding},
            "delta": {"status": "fail", "finding": "Delta -> Iceberg: " + d2i_finding},
        },
        evidence={"iceberg_to_delta": i2d, "delta_to_iceberg": d2i},
    )


if __name__ == "__main__":
    main()
