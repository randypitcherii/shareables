"""Path C — external writer: Pulsar consumer -> PyIceberg -> UC managed Iceberg.

No Databricks compute in the ingest path. Extends the prior experiment's writer
with pre-table filtering, timed continuous ("window") runs with configurable
commit intervals, and exact event->queryable freshness (commit wall-clock minus
event_ts for every row in the commit).

VARIANT floor (documented, not landed here): PyIceberg cannot write Iceberg v3
VARIANT today, so the payload lands as a JSON string; conversion to VARIANT
belongs in a silver hop. Iceberg v3 VARIANT also needs a recent Iceberg library
(apache/iceberg#14655 fix) on the write side and DBR-18-class readers.

Usage:
  uv run python scripts/03_path_c_writer.py --cell c_scale_plain --mode drain
  uv run python scripts/03_path_c_writer.py --cell c_scale_filtered --mode drain --filter on
  uv run python scripts/03_path_c_writer.py --cell c_nrt --mode window \
      --window-sec 480 --commit-interval 5
  uv run python scripts/03_path_c_writer.py --cell c_t60 --mode window \
      --window-sec 600 --commit-interval 60
"""

import argparse
import json
import time

import pulsar
import pyarrow as pa
from _common import (
    KEEP_EVENT_TYPES,
    databricks_config,
    latency_stats_ms,
    load_config,
    record_result,
)
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import CommitFailedException, NoSuchTableError

BATCH_ROWS_CAP = 20000  # rows per append in drain mode (memory bound, not a timer)
READ_TIMEOUT_MS = 15000
COMMIT_RETRIES = 5

ARROW_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string()),
        pa.field("seq", pa.int64()),
        pa.field("event_ts", pa.timestamp("us")),
        pa.field("event_type", pa.string()),
        pa.field("project_id", pa.string()),
        pa.field("event_json", pa.string()),
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


def ensure_table(catalog, cfg, name):
    ident = (cfg.uc_schema, name)
    try:
        catalog.drop_table(ident)
    except NoSuchTableError:
        pass
    return catalog.create_table(ident, schema=ARROW_SCHEMA)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cell", required=True)
    ap.add_argument("--mode", choices=["drain", "window"], required=True)
    ap.add_argument("--filter", choices=["on", "off"], default="off")
    ap.add_argument(
        "--commit-interval", type=float, default=5.0, help="window mode: seconds between commits"
    )
    ap.add_argument("--window-sec", type=int, default=480)
    ap.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="drain mode: stop after N consumed (0 = until topic empty)",
    )
    args = ap.parse_args()

    cfg = load_config()
    catalog = open_catalog(cfg)
    table = ensure_table(catalog, cfg, args.cell)

    client = pulsar.Client(cfg.pulsar_service_url)
    # Consumer (not Reader): readers do not span partitioned topics; a fresh
    # exclusive subscription with an explicit initial position does.
    initial = (
        pulsar.InitialPosition.Earliest if args.mode == "drain" else pulsar.InitialPosition.Latest
    )
    consumer = client.subscribe(
        cfg.pulsar_topic,
        subscription_name=f"eval-{cfg.run_id}-{args.cell}",
        initial_position=initial,
        receiver_queue_size=10000,
    )

    filter_on = args.filter == "on"
    rows: list[dict] = []
    rows_event_ts_ms: list[float] = []
    consumed = 0
    written = 0
    commits = 0
    freshness_ms: list[float] = []
    conflicts = 0
    started = time.time()
    deadline = started + args.window_sec if args.mode == "window" else None
    last_commit = started

    def flush():
        nonlocal rows, rows_event_ts_ms, written, commits, conflicts
        if not rows:
            return
        batch = pa.Table.from_pylist(rows, schema=ARROW_SCHEMA)
        # UC background services (automatic maintenance) can commit between our
        # appends; refresh and retry on conflict — required client behavior.
        for attempt in range(COMMIT_RETRIES):
            try:
                table.append(batch)
                break
            except CommitFailedException:
                conflicts += 1
                if attempt == COMMIT_RETRIES - 1:
                    raise
                print(f"commit conflict; refreshing and retrying ({attempt + 1})")
                table.refresh()
        commit_ms = time.time() * 1000
        freshness_ms.extend(commit_ms - ts for ts in rows_event_ts_ms)
        commits += 1
        written += len(rows)
        print(f"commit {commits}: {len(rows)} rows (total {written})", flush=True)
        rows = []
        rows_event_ts_ms = []

    while True:
        now = time.time()
        if deadline is not None and now >= deadline:
            break
        if args.mode == "drain" and args.max_events and consumed >= args.max_events:
            break
        try:
            msg = consumer.receive(READ_TIMEOUT_MS)
        except Exception:
            if args.mode == "drain":
                print("no more messages within timeout; flushing")
                break
            continue
        consumer.acknowledge(msg)
        event = json.loads(msg.data())
        consumed += 1
        if filter_on and event.get("event_type") not in KEEP_EVENT_TYPES:
            continue
        now_us = int(time.time() * 1_000_000)
        rows.append(
            {
                "event_id": event["event_id"],
                "seq": event["seq"],
                "event_ts": event["event_ts"] * 1000,  # ms -> us
                "event_type": event.get("event_type"),
                "project_id": event.get("project_id"),
                "event_json": json.dumps(event, separators=(",", ":")),
                "ingest_ts": now_us,
            }
        )
        rows_event_ts_ms.append(event["event_ts"])
        if args.mode == "drain":
            if len(rows) >= BATCH_ROWS_CAP:
                flush()
        else:
            if time.time() - last_commit >= args.commit_interval:
                flush()
                last_commit = time.time()

    flush()
    elapsed = time.time() - started
    try:
        consumer.unsubscribe()
    except Exception:
        pass
    client.close()

    summary = {
        "table": f"{cfg.uc_catalog}.{cfg.uc_schema}.{args.cell}",
        "mode": args.mode,
        "filter": args.filter,
        "commit_interval_sec": args.commit_interval if args.mode == "window" else None,
        "window_sec": args.window_sec if args.mode == "window" else None,
        "rows_consumed": consumed,
        "rows_written": written,
        "volume_reduction_pct": round(100 * (1 - written / consumed), 1) if consumed else None,
        "iceberg_commits": commits,
        "commit_conflicts_retried": conflicts,
        "elapsed_sec": round(elapsed, 1),
        "consume_throughput_rows_per_sec": round(consumed / elapsed) if elapsed else 0,
        "write_throughput_rows_per_sec": round(written / elapsed) if elapsed else 0,
        "freshness": latency_stats_ms(freshness_ms) if args.mode == "window" else None,
        "run_id": cfg.run_id,
    }
    print(json.dumps(summary, indent=2))
    record_result(f"cell_{args.cell}", summary)


if __name__ == "__main__":
    main()
