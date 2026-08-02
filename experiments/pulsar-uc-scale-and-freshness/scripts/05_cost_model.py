"""Cost and performance model: measured throughput at evaluation scale,
extrapolated to the target workload (~250k events/s at ~1KB).

Every number is tagged "measured" (cites its results key) or "extrapolated"
(linear scaling from measured per-node throughput — real estates should pilot
before committing; coordination overhead, skew, and broker fan-out are not
linear). Rates are embedded so the arithmetic is reproducible; update RATES
when list prices move.
"""

import json
import math

from _common import RESULTS_PATH, record_result

TARGET_EVENTS_PER_SEC = 250_000

# List prices, us-east-1, on-demand, 2026-08 (see README sources note).
RATES = {
    "dbu_usd_jobs_premium": 0.15,  # Jobs Compute, premium tier, AWS
    "m5d_xlarge_jobs_dbu_per_hr": 0.75,  # Databricks AWS instance-DBU table
    "m5d_xlarge_ec2_usd_per_hr": 0.226,
    "m6i_2xlarge_ec2_usd_per_hr": 0.384,  # broker VM (shared by all paths)
    "c6i_xlarge_ec2_usd_per_hr": 0.17,  # external-writer host (4 vCPU)
    "s3_put_usd_per_1k": 0.005,
    "s3_storage_usd_per_gb_month": 0.023,
}


def load_results() -> dict:
    return json.loads(RESULTS_PATH.read_text())


def spark_path_model(measured_rows_per_sec: float, source_key: str) -> dict:
    """Both Spark paths ran single-node m5d.xlarge (4 vCPU); linear extrapolation."""
    nodes = max(1, math.ceil(TARGET_EVENTS_PER_SEC / measured_rows_per_sec))
    dbu_hr = nodes * RATES["m5d_xlarge_jobs_dbu_per_hr"]
    usd_hr = dbu_hr * RATES["dbu_usd_jobs_premium"] + nodes * RATES["m5d_xlarge_ec2_usd_per_hr"]
    return {
        "measured_rows_per_sec_single_node": round(measured_rows_per_sec),
        "measured_from": source_key,
        "extrapolated_nodes_at_target": nodes,
        "extrapolated_usd_per_hr_continuous": round(usd_hr, 2),
        "extrapolated_usd_per_month_continuous": round(usd_hr * 730, 0),
        "label": "extrapolated",
        "notes": (
            "Linear scale-out from one m5d.xlarge worker-equivalent; assumes the broker "
            "side fans out (partitions >= cores) and no skew. Scheduled (non-continuous) "
            "triggers cut cost roughly by duty cycle: usd/hr x (processing_time / interval)."
        ),
    }


def path_c_model(measured_rows_per_sec: float, commits_per_hr: float, source_key: str) -> dict:
    procs = max(1, math.ceil(TARGET_EVENTS_PER_SEC / measured_rows_per_sec))
    hosts = max(1, math.ceil(procs / 4))  # ~4 writer processes per c6i.xlarge (1/vCPU)
    # Each Iceberg commit is O(few) S3 PUTs (data file(s) + metadata + manifest).
    s3_puts_hr = commits_per_hr * procs * 5
    usd_hr = (
        hosts * RATES["c6i_xlarge_ec2_usd_per_hr"]
        + (s3_puts_hr / 1000) * RATES["s3_put_usd_per_1k"]
    )
    return {
        "measured_rows_per_sec_single_process": round(measured_rows_per_sec),
        "measured_from": source_key,
        "extrapolated_writer_processes_at_target": procs,
        "extrapolated_writer_hosts_c6i_xlarge": hosts,
        "extrapolated_usd_per_hr_continuous": round(usd_hr, 2),
        "extrapolated_usd_per_month_continuous": round(usd_hr * 730, 0),
        "label": "extrapolated",
        "notes": (
            "Single-process Python writer scaled by process count (one consumer per "
            "topic partition subset). Excludes Databricks-side cost entirely (none in the "
            "ingest path) but a real estate pays a silver-hop job to convert JSON->VARIANT. "
            "Commit-rate drives both S3 PUTs and REST-catalog load; at 250k events/s the "
            "catalog-commit path itself needs validation (concurrent-commit serialization)."
        ),
    }


def main() -> None:
    results = load_results()

    def cell_rate(key: str, field: str = "landed_rows") -> float | None:
        # Spark cells: summaries live in results["spark_cells"][cell]
        cell = results.get("spark_cells", {}).get(key)
        if cell and cell.get("elapsed_sec"):
            return cell[field] / cell["elapsed_sec"]
        return None

    model: dict = {"target_events_per_sec": TARGET_EVENTS_PER_SEC, "rates": RATES}

    a_rate = cell_rate("a_scale_plain")
    if a_rate:
        model["path_a_kafka"] = spark_path_model(a_rate, "spark_cells.a_scale_plain")
    b_rate = cell_rate("b_scale_plain")
    if b_rate:
        model["path_b_pulsar"] = spark_path_model(b_rate, "spark_cells.b_scale_plain")

    c = results.get("cell_c_scale_plain")
    if c and c.get("elapsed_sec"):
        c_rate = c["rows_written"] / c["elapsed_sec"]
        commits_per_hr = 3600 / 60  # modeled at a 60s commit cadence in production
        model["path_c_pyiceberg"] = path_c_model(c_rate, commits_per_hr, "cell_c_scale_plain")

    model["shared_broker_note"] = (
        "Broker-side cost (here one m6i.2xlarge, "
        f"${RATES['m6i_2xlarge_ec2_usd_per_hr']}/hr) belongs to the existing Pulsar "
        "estate in every path and is excluded from per-path comparisons."
    )
    print(json.dumps(model, indent=2))
    record_result("cost_model", model)


if __name__ == "__main__":
    main()
