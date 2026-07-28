"""Path C — external writer: Pulsar consumer -> PyIceberg -> UC managed Iceberg table.

No Databricks compute in the ingest path. The writer authenticates to the Unity
Catalog Iceberg REST endpoint (OAuth, credential vending) and appends Arrow
batches to a MANAGED Iceberg table. Requires:
  - metastore external data access enabled
  - EXTERNAL USE SCHEMA granted on the target schema to the writer principal
"""

import json
import time

import pulsar
import pyarrow as pa
from _common import databricks_config, latency_stats_ms, load_config, record_result
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError

TABLE_NAME = "path_c_pyiceberg_rest"
BATCH_SIZE = 5000
READ_TIMEOUT_MS = 15000

ARROW_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string()),
        pa.field("seq", pa.int64()),
        pa.field("event_ts", pa.timestamp("us")),
        pa.field("device_id", pa.string()),
        pa.field("region", pa.string()),
        pa.field("value", pa.float64()),
        pa.field("payload_json", pa.string()),
        pa.field("ingest_ts", pa.timestamp("us")),
    ]
)


def open_catalog(cfg):
    dbx = databricks_config(cfg)
    return RestCatalog(
        "uc",
        uri=f"{dbx.host}/api/2.1/unity-catalog/iceberg-rest",
        token=dbx.oauth_token().access_token,
        warehouse=cfg.uc_catalog,
    )


def ensure_table(catalog, cfg):
    ident = (cfg.uc_schema, TABLE_NAME)
    try:
        return catalog.load_table(ident)
    except NoSuchTableError:
        return catalog.create_table(ident, schema=ARROW_SCHEMA)


def rows_to_arrow(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=ARROW_SCHEMA)


def main() -> None:
    cfg = load_config()
    catalog = open_catalog(cfg)
    table = ensure_table(catalog, cfg)

    client = pulsar.Client(cfg.pulsar_service_url)
    reader = client.create_reader(cfg.pulsar_topic, pulsar.MessageId.earliest)

    rows: list[dict] = []
    total = 0
    commit_latencies: list[float] = []
    batch_event_ts: list[int] = []
    appends = 0
    started = time.time()

    def flush():
        nonlocal rows, total, appends
        if not rows:
            return
        table.append(rows_to_arrow(rows))
        commit_ms = time.time() * 1000
        commit_latencies.extend(commit_ms - ts for ts in batch_event_ts)
        appends += 1
        total += len(rows)
        print(f"appended batch of {len(rows)} (total {total})")
        rows = []
        batch_event_ts.clear()

    while total + len(rows) < cfg.event_count:
        try:
            msg = reader.read_next(READ_TIMEOUT_MS)
        except Exception:
            print("no more messages within timeout; flushing")
            break
        event = json.loads(msg.data())
        now_us = int(time.time() * 1_000_000)
        rows.append(
            {
                "event_id": event["event_id"],
                "seq": event["seq"],
                "event_ts": event["event_ts"] * 1000,  # ms -> us
                "device_id": event["device_id"],
                "region": event["region"],
                "value": event["value"],
                "payload_json": json.dumps(event["payload"], separators=(",", ":")),
                "ingest_ts": now_us,
            }
        )
        batch_event_ts.append(event["event_ts"])
        if len(rows) >= BATCH_SIZE:
            flush()

    flush()
    elapsed = time.time() - started
    client.close()

    summary = {
        "table": f"{cfg.uc_catalog}.{cfg.uc_schema}.{TABLE_NAME}",
        "rows_written": total,
        "iceberg_commits": appends,
        "elapsed_sec": round(elapsed, 1),
        "throughput_rows_per_sec": round(total / elapsed) if elapsed else 0,
        "event_to_commit_latency": latency_stats_ms(commit_latencies),
        "batch_size": BATCH_SIZE,
    }
    print(json.dumps(summary, indent=2))
    record_result("path_c_pyiceberg_rest", summary)


if __name__ == "__main__":
    main()
