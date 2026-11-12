"""Runs ON a Databricks job cluster (DBR 19.x). Produces raw evidence for rows 02-04.

Usage (spark_python_task parameters): <catalog> <schema> <volume_path> <run_tag>
Writes <volume_path>/out/spark_matrix.json. Every cell records success or the verbatim error;
nothing here decides pass/fail -- the local 02/03/04 scripts interpret the evidence.
"""

import json
import sys
import time
import traceback

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
CATALOG, SCHEMA, VOLUME_PATH, RUN_TAG = sys.argv[1:5]
FQ = f"`{CATALOG}`.`{SCHEMA}`"
CKPT = f"{VOLUME_PATH}/checkpoints/{RUN_TAG}"
OUT: dict = {"run_tag": RUN_TAG, "cells": {}}

DELTA_PROPS = (
    "'delta.feature.catalogManaged' = 'supported', 'delta.enableIcebergCompatV3' = 'true', "
    "'delta.universalFormat.enabledFormats' = 'iceberg', 'delta.enableRowTracking' = 'true'"
)


STREAM_TIMEOUT_S = 300


class StreamTimeout(Exception):
    pass


def await_or_stop(q):
    """availableNow streams must terminate on their own; a hang is recorded, not waited out."""
    if not q.awaitTermination(STREAM_TIMEOUT_S):
        progress = q.lastProgress
        q.stop()
        raise StreamTimeout(f"stream did not finish within {STREAM_TIMEOUT_S}s; lastProgress={progress}")


def cell(name, fn):
    print("start", name, flush=True)
    t0 = time.time()
    try:
        OUT["cells"][name] = {"ok": True, "value": fn()}
    except Exception as exc:  # noqa: BLE001 -- failures are the evidence
        OUT["cells"][name] = {
            "ok": False,
            "error_class": type(exc).__name__,
            "error": str(exc)[:1500],
            "trace_tail": traceback.format_exc()[-600:],
        }
    OUT["cells"][name]["seconds"] = round(time.time() - t0, 1)
    print(name, "ok" if OUT["cells"][name]["ok"] else "FAILED", flush=True)


def create(name, fmt):
    spark.sql(f"DROP TABLE IF EXISTS {FQ}.{name}")
    cols = "(id BIGINT, cat STRING, v BIGINT)"
    if fmt == "delta":
        spark.sql(f"CREATE TABLE {FQ}.{name} {cols} CLUSTER BY (cat) TBLPROPERTIES ({DELTA_PROPS})")
    else:
        spark.sql(
            f"CREATE TABLE {FQ}.{name} {cols} USING ICEBERG CLUSTER BY (cat) "
            "TBLPROPERTIES ('format-version' = '3')"
        )


def rows(name):
    return [list(r) for r in spark.sql(f"SELECT id, cat, v FROM {FQ}.{name} ORDER BY id").collect()]


def provider(name):
    d = spark.sql(f"DESCRIBE DETAIL {FQ}.{name}").collect()[0].asDict()
    ext = spark.sql(f"DESCRIBE TABLE EXTENDED {FQ}.{name}").collect()
    prov = [r["data_type"] for r in ext if r["col_name"] == "Provider"]
    return {"format": d.get("format"), "provider": prov[0] if prov else None}


def df(ids):
    return spark.createDataFrame([(i, f"c{i % 3}", i * 10) for i in ids], "id BIGINT, cat STRING, v BIGINT")


def run_stream(stream_writer, name):
    q = stream_writer.option("checkpointLocation", f"{CKPT}/{name}").trigger(availableNow=True)
    return q


# ------------------------------------------------------------------ environment
OUT["spark_version"] = spark.version
OUT["dbr"] = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "unknown")

# ------------------------------------------------------------------ row 02: change data feed
CDF_COLS = "id, v, _change_type, _commit_version"
for fmt in ("delta", "iceberg"):
    t = f"{FQ}.dml_{fmt}"
    cell(
        f"cdf_history_{fmt}",
        lambda t=t: [
            {k: str(r.asDict().get(k)) for k in ("version", "operation", "engineInfo")}
            for r in spark.sql(f"DESCRIBE HISTORY {t}").collect()
        ],
    )
    cell(
        f"cdf_batch_readChangeFeed_{fmt}",
        lambda t=t: [
            list(r)
            for r in spark.read.option("readChangeFeed", "true")
            .option("startingVersion", 0)
            .table(t)
            .selectExpr(*CDF_COLS.split(", "))
            .orderBy("_commit_version", "id", "_change_type")
            .collect()
        ],
    )
    cell(
        f"cdf_sql_table_changes_{fmt}",
        lambda t=t: [
            list(r)
            for r in spark.sql(
                f"SELECT {CDF_COLS} FROM table_changes('{t}', 0) ORDER BY _commit_version, id"
            ).collect()
        ],
    )

    def _cdf_stream(t=t, fmt=fmt):
        sink = f"cdf_stream_sink_{fmt}"
        spark.sql(f"DROP TABLE IF EXISTS {FQ}.{sink}")
        q = run_stream(
            spark.readStream.option("readChangeFeed", "true")
            .option("startingVersion", 0)
            .table(t)
            .selectExpr("id", "v", "_change_type", "_commit_version")
            .writeStream,
            sink,
        ).toTable(f"{FQ}.{sink}")
        await_or_stop(q)
        return [
            list(r)
            for r in spark.sql(
                f"SELECT * FROM {FQ}.{sink} ORDER BY _commit_version, id, _change_type"
            ).collect()
        ]

    cell(f"cdf_stream_readChangeFeed_{fmt}", _cdf_stream)

# ------------------------------------------------------------------ row 03: structured streaming
create("stream_src_delta", "delta")
create("stream_src_iceberg", "iceberg")
spark.sql(f"INSERT INTO {FQ}.stream_src_delta VALUES (1,'a',10),(2,'b',20),(3,'c',30)")
spark.sql(f"INSERT INTO {FQ}.stream_src_iceberg VALUES (1,'a',10),(2,'b',20),(3,'c',30)")
for sink, fmt in (
    ("stream_sink_iceberg_default", "iceberg"),
    ("stream_sink_iceberg_formatted", "iceberg"),
    ("stream_sink_delta", "delta"),
    ("stream_sink_from_iceberg", "delta"),
    ("stream_sink_iceberg_from_iceberg", "iceberg"),
):
    create(sink, fmt)

STREAMS = {
    # name: (source table, sink table, writer format or None)
    "sink_iceberg_default_writer": ("stream_src_delta", "stream_sink_iceberg_default", None),
    "sink_iceberg_format_iceberg": ("stream_src_delta", "stream_sink_iceberg_formatted", "iceberg"),
    "control_delta_to_delta": ("stream_src_delta", "stream_sink_delta", None),
    "source_iceberg_plain_to_delta": ("stream_src_iceberg", "stream_sink_from_iceberg", None),
    "iceberg_to_iceberg": ("stream_src_iceberg", "stream_sink_iceberg_from_iceberg", "iceberg"),
}


def _stream_once(src, sink, fmt, name):
    w = spark.readStream.table(f"{FQ}.{src}").writeStream
    if fmt:
        w = w.format(fmt)
    q = run_stream(w, name).toTable(f"{FQ}.{sink}")
    await_or_stop(q)


for name, (src, sink, fmt) in STREAMS.items():

    def _first(src=src, sink=sink, fmt=fmt, name=name):
        _stream_once(src, sink, fmt, name)
        return {"sink_rows": rows(sink)}

    cell(f"stream_{name}_first", _first)

# append to sources, re-run each stream: proves incremental progress, not a re-snapshot
print("appending to stream sources", flush=True)
spark.sql(f"INSERT INTO {FQ}.stream_src_delta VALUES (4,'d',40),(5,'e',50)")
spark.sql(f"INSERT INTO {FQ}.stream_src_iceberg VALUES (4,'d',40),(5,'e',50)")
for name, (src, sink, fmt) in STREAMS.items():
    if not OUT["cells"][f"stream_{name}_first"]["ok"]:
        continue

    def _second(src=src, sink=sink, fmt=fmt, name=name):
        _stream_once(src, sink, fmt, name)
        return {"sink_rows": rows(sink)}

    cell(f"stream_{name}_incremental", _second)

# ------------------------------------------------------------------ row 04: DataFrame writers
create("w_delta", "delta")
create("w_iceberg", "iceberg")
WRITERS = {
    "v1_saveAsTable_no_format": lambda t, d: d.write.mode("append").saveAsTable(t),
    "v1_saveAsTable_format_delta": lambda t, d: d.write.format("delta")
    .mode("append")
    .saveAsTable(t),
    "v1_saveAsTable_format_iceberg": lambda t, d: d.write.format("iceberg")
    .mode("append")
    .saveAsTable(t),
    "v1_insertInto": lambda t, d: d.write.insertInto(t),
    "v2_writeTo_append": lambda t, d: d.writeTo(t).append(),
}
start = 100
for fmt in ("delta", "iceberg"):
    t = f"{FQ}.w_{fmt}"
    for wname, fn in WRITERS.items():
        start += 10
        ids = [start, start + 1]

        def _write(t=t, fn=fn, ids=ids, fmt=fmt):
            fn(t, df(ids))
            got = [r for r in rows(f"w_{fmt}") if r[0] in ids]
            return {"rows_landed": len(got), "provider_after": provider(f"w_{fmt}")}

        cell(f"writer_{wname}_{fmt}", _write)

    def _merge(t=t, fmt=fmt, start=start):
        from delta.tables import DeltaTable

        src = df([start, 999])  # one match, one insert
        (
            DeltaTable.forName(spark, t)
            .alias("t")
            .merge(src.alias("s"), "t.id = s.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        return {"has_999": any(r[0] == 999 for r in rows(f"w_{fmt}"))}

    cell(f"writer_DeltaTable_merge_{fmt}", _merge)

CREATES = {
    "create_v1_format_iceberg": lambda t: df([1, 2]).write.format("iceberg").saveAsTable(t),
    "create_v2_using_iceberg": lambda t: df([1, 2]).writeTo(t).using("iceberg").create(),
    "create_v1_no_format": lambda t: df([1, 2]).write.saveAsTable(t),
}
for cname, fn in CREATES.items():
    tname = f"w_{cname}"

    def _create(tname=tname, fn=fn):
        spark.sql(f"DROP TABLE IF EXISTS {FQ}.{tname}")
        fn(f"{FQ}.{tname}")
        return {"provider": provider(tname), "rows": len(rows(tname))}

    cell(f"writer_{cname}", _create)

# ------------------------------------------------------------------ write evidence
dest = f"{VOLUME_PATH}/out/spark_matrix_{RUN_TAG}.json"
import os

os.makedirs(os.path.dirname(dest), exist_ok=True)
with open(dest, "w") as f:
    json.dump(OUT, f, indent=2, default=str)
print("wrote", dest)
