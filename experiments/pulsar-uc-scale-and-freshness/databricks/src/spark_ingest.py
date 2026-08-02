"""Unified Databricks Structured Streaming ingest for paths A (Kafka/KoP) and B
(native pulsar connector), writing UC MANAGED Delta tables with a real VARIANT
column, parameterized by "cells" so one job run (one cluster) can execute a whole
measurement ladder.

argv: source endpoint topic catalog schema run_id cells_json

cells_json is a JSON list of cell dicts:
  name         table name suffix (also the results key)
  mode         "drain" (bounded, trigger availableNow) | "window" (timed live run)
  trigger_sec  window mode only: 0 = near-real-time (back-to-back batches),
               N = processingTime trigger every N seconds
  window_sec   window mode only: how long the query runs
  filter       "on" | "off" — pre-table predicate keeping business event types
  cluster      "auto" | "none" — liquid clustering seeded + CLUSTER BY AUTO
  starting     "earliest" | "latest"

Filtering happens INSIDE foreachBatch so input rows vs landed rows are both
measured exactly. Every batch appends a metrics row (commit wall-clock, counts)
to <schema>.ingest_batches; verify.py joins rows' event_ts to their batch's
commit_ts for exact per-event freshness. Cell summaries land in
<schema>.ingest_cells.

Checkpoints are reset per cell — reusing a checkpoint against a rebuilt broker
silently ingests nothing while the table still holds old rows (prior-experiment
finding). max_concurrent_runs=1 on the job keeps runs from clobbering state.
"""

import json
import sys
import time

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

KEEP_EVENT_TYPES = ("send", "open", "click", "purchase", "bounce", "unsubscribe")

BASE_COLUMNS = (
    "event_id STRING, seq BIGINT, event_ts TIMESTAMP, event_type STRING, "
    "project_id STRING, event VARIANT, batch_id BIGINT, ingest_ts TIMESTAMP"
)


def parse_events(raw_df, value_col):
    """Promote filter/cluster columns to typed fields; keep the whole event as VARIANT."""
    return raw_df.select(F.col(value_col).cast("string").alias("raw")).select(
        F.get_json_object("raw", "$.event_id").alias("event_id"),
        F.get_json_object("raw", "$.seq").cast("bigint").alias("seq"),
        F.timestamp_millis(F.get_json_object("raw", "$.event_ts").cast("bigint")).alias("event_ts"),
        F.get_json_object("raw", "$.event_type").alias("event_type"),
        F.get_json_object("raw", "$.project_id").alias("project_id"),
        F.expr("try_parse_json(raw)").alias("event"),
    )


def build_source(spark, source, endpoint, topic, starting):
    if source == "kafka":
        # Kafka clients address the topic by its short name (tenant/namespace implied).
        short_topic = topic.rsplit("/", 1)[-1]
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", endpoint)
            .option("subscribe", short_topic)
            .option("startingOffsets", starting)
            .load()
        ), "value"
    if source == "pulsar":
        return (
            spark.readStream.format("pulsar")
            .option("service.url", endpoint)
            .option("topics", topic)
            .option("startingOffsets", starting)
            .load()
        ), "value"
    raise ValueError(f"unknown source {source}")


def run_cell(spark, cell, source, endpoint, topic, catalog, schema, run_id):
    name = cell["name"]
    mode = cell["mode"]
    trigger_sec = int(cell.get("trigger_sec", 0))
    window_sec = int(cell.get("window_sec", 0))
    filter_on = cell.get("filter", "off") == "on"
    cluster = cell.get("cluster", "none")
    starting = cell.get("starting", "earliest")

    table = f"{catalog}.{schema}.{name}"
    checkpoint = f"/Volumes/{catalog}/{schema}/checkpoints/{name}"
    batches_table = f"{catalog}.{schema}.ingest_batches"

    spark.sql(f"DROP TABLE IF EXISTS {table}")
    DBUtils(spark).fs.rm(checkpoint, True)
    spark.sql(f"DELETE FROM {batches_table} WHERE run_id = '{run_id}' AND cell = '{name}'")

    ddl = f"CREATE TABLE {table} ({BASE_COLUMNS})"
    if cluster == "auto":
        # Seed liquid clustering on the obvious read-path columns, then hand
        # column choice to AUTO (predictive optimization keeps learning).
        ddl += " CLUSTER BY (project_id, event_type, event_ts)"
    spark.sql(ddl)
    if cluster == "auto":
        spark.sql(f"ALTER TABLE {table} CLUSTER BY AUTO")

    raw_df, value_col = build_source(spark, source, endpoint, topic, starting)
    parsed = parse_events(raw_df, value_col)

    totals = {"input_rows": 0, "landed_rows": 0, "batches": 0}

    def handle_batch(batch_df, batch_id):
        batch_df.persist()
        input_rows = batch_df.count()
        out = batch_df
        if filter_on:
            out = out.where(F.col("event_type").isin(*KEEP_EVENT_TYPES))
        # lit() of a Python int is INT; the table column is BIGINT and Delta
        # refuses to merge them (DELTA_FAILED_TO_MERGE_FIELDS, seen live).
        out = out.withColumn("batch_id", F.lit(batch_id).cast("bigint")).withColumn(
            "ingest_ts", F.current_timestamp()
        )
        out.write.format("delta").mode("append").saveAsTable(table)
        commit_ts_ms = time.time() * 1000  # queryable time: the append has committed
        landed = spark.sql(f"SELECT COUNT(*) FROM {table} WHERE batch_id = {batch_id}").collect()[
            0
        ][0]
        batch_df.unpersist()
        spark.createDataFrame(
            [(run_id, name, int(batch_id), commit_ts_ms, int(input_rows), int(landed))],
            "run_id STRING, cell STRING, batch_id BIGINT, commit_ts_ms DOUBLE, "
            "input_rows BIGINT, landed_rows BIGINT",
        ).write.mode("append").saveAsTable(batches_table)
        totals["input_rows"] += int(input_rows)
        totals["landed_rows"] += int(landed)
        totals["batches"] += 1

    writer = parsed.writeStream.option("checkpointLocation", checkpoint).foreachBatch(handle_batch)

    started_ms = time.time() * 1000
    if mode == "drain":
        query = writer.trigger(availableNow=True).start()
        query.awaitTermination()
    elif mode == "window":
        if trigger_sec > 0:
            writer = writer.trigger(processingTime=f"{trigger_sec} seconds")
        query = writer.start()
        time.sleep(window_sec)
        query.stop()
        query.awaitTermination()
    else:
        raise ValueError(f"unknown mode {mode}")
    elapsed = time.time() - started_ms / 1000

    summary = {
        "run_id": run_id,
        "cell": name,
        "source": source,
        "mode": mode,
        "trigger_sec": trigger_sec,
        "window_sec": window_sec,
        "filter": "on" if filter_on else "off",
        "cluster": cluster,
        "starting": starting,
        "started_ts_ms": started_ms,
        "elapsed_sec": round(elapsed, 1),
        "input_rows": totals["input_rows"],
        "landed_rows": totals["landed_rows"],
        "batches": totals["batches"],
    }
    spark.createDataFrame(
        [(run_id, name, json.dumps(summary))],
        "run_id STRING, cell STRING, summary STRING",
    ).write.mode("append").saveAsTable(f"{catalog}.{schema}.ingest_cells")
    print("CELL_SUMMARY: " + json.dumps(summary), flush=True)


def main() -> None:
    source, endpoint, topic, catalog, schema, run_id, cells_json = sys.argv[1:8]
    cells = json.loads(cells_json)

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.checkpoints")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.ingest_batches ("
        "run_id STRING, cell STRING, batch_id BIGINT, commit_ts_ms DOUBLE, "
        "input_rows BIGINT, landed_rows BIGINT)"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.ingest_cells ("
        "run_id STRING, cell STRING, summary STRING)"
    )

    for cell in cells:
        run_cell(spark, cell, source, endpoint, topic, catalog, schema, run_id)


if __name__ == "__main__":
    main()
