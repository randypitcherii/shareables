"""Path B — Databricks native Pulsar connector (Public Preview, DBR 14.1+),
writing a Unity Catalog MANAGED Delta table.

Same parse + sink as path A; only the source differs (format("pulsar") over the
Pulsar binary protocol instead of Kafka-protocol via KoP). Preview status means
this path is a fast-follow, not the GA-critical path.
"""

import json
import sys
import time

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("seq", LongType()),
        StructField("event_ts", LongType()),
        StructField("device_id", StringType()),
        StructField("region", StringType()),
        StructField("value", DoubleType()),
        StructField("payload", StringType()),
    ]
)


def parse_events(raw_df, value_col="value"):
    # Kept identical to path A's parse so the paths differ ONLY in source connector.
    parsed = (
        raw_df.select(F.col(value_col).cast("string").alias("raw"))
        .withColumn("j", F.from_json("raw", EVENT_SCHEMA))
        .select(
            "j.event_id",
            "j.seq",
            F.timestamp_millis(F.col("j.event_ts")).alias("event_ts"),
            "j.device_id",
            "j.region",
            "j.value",
            F.col("j.payload").alias("payload_json"),
        )
        .withColumn("ingest_ts", F.current_timestamp())
    )
    return parsed


def main() -> None:
    service_url, topic, catalog, schema = sys.argv[1:5]
    table = f"{catalog}.{schema}.path_b_native_pulsar"

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.checkpoints")

    # Each evaluation run is a bounded drain of a freshly produced topic, so it
    # must start from clean state. Reusing the checkpoint across runs is not
    # merely stale — if the broker was rebuilt, the stored offsets refer to a
    # topic instance that no longer exists, and the run silently ingests nothing
    # while the table still holds the previous run's rows. That reads as success.
    checkpoint = f"/Volumes/{catalog}/{schema}/checkpoints/path_b"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    DBUtils(spark).fs.rm(checkpoint, True)

    raw = (
        spark.readStream.format("pulsar")
        .option("service.url", service_url)
        .option("topics", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    started = time.time()
    query = (
        parse_events(raw)
        .writeStream.trigger(availableNow=True)
        .option("checkpointLocation", checkpoint)
        .toTable(table)
    )
    query.awaitTermination()
    elapsed = time.time() - started

    count = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]
    print(json.dumps({"table": table, "rows": count, "drain_elapsed_sec": round(elapsed, 1)}))


if __name__ == "__main__":
    main()
