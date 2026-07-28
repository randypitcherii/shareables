"""Verify every ingestion path landed a MANAGED Unity Catalog table with the expected rows.

For each candidate table: existence, row count, distinct event_ids (dedup check),
table type (must be MANAGED), and data source format (DELTA / ICEBERG). Results are
merged into results/matrix_results.json under "verification".
"""

import json

from _common import load_config, record_result, run_sql

CANDIDATE_TABLES = {
    "path_a_kop_kafka": "DELTA",
    "path_b_native_pulsar": "DELTA",
    "path_c_pyiceberg_rest": "ICEBERG",
}


def verify_table(cfg, table: str, expected_format: str) -> dict:
    fq = f"{cfg.uc_catalog}.{cfg.uc_schema}.{table}"
    meta = run_sql(
        cfg,
        f"""
        SELECT table_type, data_source_format
        FROM {cfg.uc_catalog}.information_schema.tables
        WHERE table_schema = '{cfg.uc_schema}' AND table_name = '{table}'
        """,
    )
    if not meta:
        return {"status": "MISSING"}
    table_type, fmt = meta[0][0], meta[0][1]
    counts = run_sql(
        cfg,
        f"SELECT COUNT(*), COUNT(DISTINCT event_id), MIN(seq), MAX(seq) FROM {fq}",
    )[0]
    return {
        "status": "OK",
        "table_type": table_type,
        "data_source_format": fmt,
        "is_managed": table_type == "MANAGED",
        "format_matches": fmt == expected_format,
        "row_count": int(counts[0]),
        "distinct_event_ids": int(counts[1]),
        "seq_range": [int(counts[2]), int(counts[3])],
    }


def main() -> None:
    cfg = load_config()
    if not cfg.warehouse_id:
        raise SystemExit("DATABRICKS_WAREHOUSE_ID is required for verification queries")
    results = {
        name: verify_table(cfg, name, expected)
        for name, expected in CANDIDATE_TABLES.items()
    }
    print(json.dumps(results, indent=2))
    record_result("verification", results)


if __name__ == "__main__":
    main()
