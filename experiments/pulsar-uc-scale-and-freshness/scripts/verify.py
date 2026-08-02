"""Collect and verify everything the live runs produced.

1. Pull Spark cell summaries (written by the job into <schema>.ingest_cells)
   into results/matrix_results.json under "spark_cells".
2. Verify each landed table: MANAGED, expected format, row counts, distinct
   event ids, and (Delta paths) that the `event` column is a real VARIANT.
3. Compute exact per-event freshness (commit wall-clock minus event_ts) for the
   ladder cells by joining rows' batch_id to the batch commit log; report all
   batches and steady-state (first batch excluded — with starting=latest the
   first micro-batch is warm-up).
4. Compute filter volume-reduction from input vs landed counts.
5. Emit modeled 15-min/hourly freshness cells (labeled), validating the model
   against the measured 1-min and 5-min cells first.
"""

import json

from _common import load_config, modeled_freshness_p95_sec, record_result, run_sql

DELTA_CELLS = {
    "a_scale_plain": "kafka",
    "a_scale_filtered": "kafka",
    "a_scale_clustered": "kafka",
    "b_scale_plain": "pulsar",
    "a_nrt": "kafka",
    "a_t60": "kafka",
    "a_t300": "kafka",
    "b_nrt": "pulsar",
    "b_t60": "pulsar",
}
ICEBERG_CELLS = ["c_scale_plain", "c_scale_filtered", "c_nrt", "c_t60"]
LADDER_CELLS = ["a_nrt", "a_t60", "a_t300", "b_nrt", "b_t60"]


def collect_spark_cells(cfg) -> dict:
    rows = run_sql(
        cfg,
        f"SELECT cell, summary FROM {cfg.uc_catalog}.{cfg.uc_schema}.ingest_cells "
        f"WHERE run_id = '{cfg.run_id}'",
    )
    out = {}
    for cell, summary in rows:
        out[cell] = json.loads(summary)
    return out


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
    entry = {
        "status": "OK",
        "table_type": table_type,
        "data_source_format": fmt,
        "is_managed": table_type == "MANAGED",
        "format_matches": fmt == expected_format,
        "row_count": int(counts[0]),
        "distinct_event_ids": int(counts[1]),
        "seq_range": [int(counts[2]), int(counts[3])] if counts[2] is not None else None,
    }
    if expected_format == "DELTA":
        vcol = run_sql(
            cfg,
            f"""
            SELECT full_data_type FROM {cfg.uc_catalog}.information_schema.columns
            WHERE table_schema = '{cfg.uc_schema}' AND table_name = '{table}'
              AND column_name = 'event'
            """,
        )
        entry["event_column_type"] = vcol[0][0] if vcol else None
        entry["event_is_variant"] = bool(vcol) and "VARIANT" in vcol[0][0].upper()
    return entry


def freshness_for_cell(cfg, cell: str) -> dict:
    fq = f"{cfg.uc_catalog}.{cfg.uc_schema}.{cell}"
    batches = f"{cfg.uc_catalog}.{cfg.uc_schema}.ingest_batches"

    def pct_query(exclude_first: bool) -> dict:
        excl = (
            f"AND t.batch_id > (SELECT MIN(batch_id) FROM {batches} "
            f"WHERE run_id = '{cfg.run_id}' AND cell = '{cell}')"
            if exclude_first
            else ""
        )
        rows = run_sql(
            cfg,
            f"""
            SELECT
              COUNT(*),
              percentile_approx(b.commit_ts_ms - unix_millis(t.event_ts), 0.5),
              percentile_approx(b.commit_ts_ms - unix_millis(t.event_ts), 0.95),
              MAX(b.commit_ts_ms - unix_millis(t.event_ts))
            FROM {fq} t
            JOIN {batches} b
              ON t.batch_id = b.batch_id AND b.cell = '{cell}' AND b.run_id = '{cfg.run_id}'
            WHERE 1=1 {excl}
            """,
        )
        n, p50, p95, mx = rows[0]
        if not n or int(n) == 0:
            return {"count": 0}
        return {
            "count": int(n),
            "p50_sec": round(float(p50) / 1000, 2),
            "p95_sec": round(float(p95) / 1000, 2),
            "max_sec": round(float(mx) / 1000, 2),
        }

    return {"all_batches": pct_query(False), "steady_state": pct_query(True)}


def main() -> None:
    cfg = load_config()
    if not cfg.warehouse_id:
        raise SystemExit("DATABRICKS_WAREHOUSE_ID is required for verification queries")

    spark_cells = collect_spark_cells(cfg)
    record_result("spark_cells", spark_cells)

    verification = {}
    for cell, _src in DELTA_CELLS.items():
        verification[cell] = verify_table(cfg, cell, "DELTA")
    for cell in ICEBERG_CELLS:
        verification[cell] = verify_table(cfg, cell, "ICEBERG")
    record_result("verification", verification)

    freshness = {}
    for cell in LADDER_CELLS:
        if verification.get(cell, {}).get("status") == "OK":
            freshness[cell] = freshness_for_cell(cfg, cell)
            print(f"freshness {cell}: {json.dumps(freshness[cell])}")
    record_result("freshness_measured", freshness)

    # Filter reduction (path A): input vs landed on the filtered drain.
    filt = spark_cells.get("a_scale_filtered")
    if filt and filt.get("input_rows"):
        record_result(
            "filter_reduction_path_a",
            {
                "input_rows": filt["input_rows"],
                "landed_rows": filt["landed_rows"],
                "volume_reduction_pct": round(
                    100 * (1 - filt["landed_rows"] / filt["input_rows"]), 1
                ),
            },
        )

    # Modeled 15-min / hourly freshness, validated against measured 1/5-min cells.
    plain = spark_cells.get("a_scale_plain")
    if plain and plain.get("elapsed_sec"):
        drain_rate = plain["landed_rows"] / plain["elapsed_sec"]
        rate = 5000  # ladder producer rate (events/s)
        modeled = {
            "model": "p95 ~= 0.95*T + (rate*T)/drain_rate; see _common.modeled_freshness_p95_sec",
            "inputs": {"drain_rows_per_sec_measured": round(drain_rate), "producer_rate": rate},
            "validation_against_measured": {},
            "cells": {},
            "label": "modeled",
        }
        for t_sec, name in [(60, "t60"), (300, "t300")]:
            pred = modeled_freshness_p95_sec(t_sec, drain_rate, rate)
            meas = freshness.get(f"a_{name}", {}).get("steady_state", {}).get("p95_sec")
            modeled["validation_against_measured"][f"a_{name}"] = {
                "predicted_p95_sec": round(pred, 1),
                "measured_p95_sec": meas,
            }
        for t_sec, name in [(900, "15min"), (3600, "hourly")]:
            modeled["cells"][name] = {
                "predicted_p95_sec": round(modeled_freshness_p95_sec(t_sec, drain_rate, rate), 1)
            }
        record_result("freshness_modeled", modeled)

    print(json.dumps(verification, indent=2))


if __name__ == "__main__":
    main()
