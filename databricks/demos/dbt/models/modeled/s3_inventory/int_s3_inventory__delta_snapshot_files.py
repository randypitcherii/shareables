"""Which data files each Delta table references: the one job done in Python.

Everything else is SQL in int_s3_inventory__delta_file_status: joining to the
inventory, classifying files, and pricing. Python is used here only because SQL
can't loop over tables asking each one "which files does your snapshot
reference?". Spark can answer that from the transaction log alone, without
reading any data files:

    spark.read.option("versionAsOf", v).table(name).inputFiles()

For every Delta table root in an inventoried bucket, it emits file lists:

    current  the snapshot as of the bucket's inventory time, so files written or
             removed after the inventory was taken aren't misclassified.
    window   the snapshot just BEFORE each commit that removed files inside the
             retention window (now - delta.deletedFileRetentionDuration). Those
             files were removed recently enough that VACUUM must keep them.

Why not one time-travel read at the retention boundary? Databricks refuses to
time travel to any version committed before now - deletedFileRetentionDuration
(DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION). So the
version that was live at the boundary is unreadable. The files removed by the
FIRST in-window commit therefore can't be named. Its removed-file count, from
DESCRIBE HISTORY, goes to SQL as `window_ambiguous_removed_files`, so SQL
reports those files as unproven instead of guessing.

Output grain: one row per (table_uri, snapshot_kind, file_uri). A table with no
files, or whose log can't be read, still gets one row (file_uri NULL) carrying
list_status, so SQL can report it rather than drop it.

Real-inventory mode only. In sample mode (DBT_S3_INVENTORY_TABLE unset),
dbt_project.yml disables this model and the seed
s3_inventory_sample__delta_snapshot_files stands in for it.
"""

import re
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

SCHEMA = StructType(
    [
        StructField("table_uri", StringType()),
        StructField("table_full_name", StringType()),
        StructField("snapshot_kind", StringType()),
        StructField("file_uri", StringType()),
        StructField("snapshot_at", TimestampType()),
        StructField("horizon_at", TimestampType()),
        StructField("retention_duration", StringType()),
        StructField("window_ambiguous_removed_files", IntegerType()),
        StructField("list_status", StringType()),
    ]
)

DEFAULT_RETENTION = "interval 7 days"  # Delta's default deletedFileRetentionDuration
# Stay clear of the time-travel cutoff, which moves while this model runs.
CUTOFF_MARGIN = timedelta(minutes=10)
# Bound log reads for chatty tables. Past this, SQL reports leftovers as unproven.
MAX_WINDOW_SNAPSHOTS = 100
# Tables checked concurrently (each check is a few small log-only calls).
PARALLEL_TABLES = 16
# Operations that never remove files, even when they report no metrics.
APPEND_ONLY = {"CREATE TABLE", "WRITE", "STREAMING UPDATE", "SET TBLPROPERTIES", "ADD COLUMNS", "CHANGE COLUMN"}


def retention(properties):
    """'interval 7 days' / '168 hours' -> (raw, timedelta). Delta's default when unset."""
    raw = (properties or {}).get("delta.deletedFileRetentionDuration") or DEFAULT_RETENTION
    units = {"week": "weeks", "day": "days", "hour": "hours", "minute": "minutes", "second": "seconds"}
    total = timedelta()
    for amount, unit in re.findall(r"(\d+)\s*(week|day|hour|minute|second)", raw.lower()):
        total += timedelta(**{units[unit]: int(amount)})
    return raw, total or timedelta(days=7)


def removed_files(commit):
    """Files a commit removed, from its operationMetrics. -1 = unknown."""
    if commit["version"] == 0:  # nothing exists before the first commit
        return 0
    counts = [int(v) for k, v in (commit["operationMetrics"] or {}).items()
              if re.search(r"removed.*files|files.*removed", k, re.I) and str(v).isdigit()]
    if counts:
        return max(counts)
    mode = (commit["operationParameters"] or {}).get("mode", "")
    return 0 if commit["operation"] in APPEND_ONLY and mode != "Overwrite" else -1


def quoted(name):
    return ".".join(f"`{part}`" for part in name.split("."))


def table_files(session, root, now):
    """All file-list rows for one Delta table root."""
    rows = []
    # UC tables by name (governed access); unregistered Delta roots by path
    target = quoted(root.table_full_name) if root.table_full_name else f"delta.`{root.table_uri}`"
    snapshot_at = root.inventory_snapshot_at
    try:
        detail = session.sql(f"DESCRIBE DETAIL {target}").collect()[0]
        raw_retention, window = retention(detail["properties"])
        horizon_at = now - window  # VACUUM run now keeps files removed after this
        in_window = sorted(
            (c for c in session.sql(f"DESCRIBE HISTORY {target}").collect()
             if c["timestamp"] >= horizon_at + CUTOFF_MARGIN),
            key=lambda c: c["version"],
        )
        ambiguous = removed_files(in_window[0]) if in_window else 0

        def files(version=None, timestamp=None):
            reader = session.read.format("delta")
            if version is not None:
                reader = reader.option("versionAsOf", version)
            elif timestamp is not None:
                reader = reader.option("timestampAsOf", timestamp.strftime("%Y-%m-%d %H:%M:%S"))
            df = reader.table(root.table_full_name) if root.table_full_name else reader.load(root.table_uri)
            return df.inputFiles()  # metadata only: read from the log, no data scanned

        def emit(kind, file_list, status="ok"):
            base = (root.table_uri, root.table_full_name, kind)
            tail = (snapshot_at, horizon_at, raw_retention, ambiguous, status)
            for f in file_list or [None]:
                rows.append((*base, f, *tail))

        # current = as of the inventory, when that version is still readable
        readable = snapshot_at >= horizon_at + CUTOFF_MARGIN and snapshot_at < detail["lastModified"]
        if snapshot_at < detail["createdAt"]:
            emit("current", [])
        else:
            emit("current", files(timestamp=snapshot_at) if readable else files(),
                 "ok" if readable or snapshot_at >= detail["lastModified"] else "ok: current is latest")

        # window = the snapshot before each later in-window commit that removed files
        removing = [c for c in in_window[1:] if removed_files(c) != 0]
        for commit in removing[:MAX_WINDOW_SNAPSHOTS]:
            emit("window", files(version=commit["version"] - 1))
        if len(removing) > MAX_WINDOW_SNAPSHOTS:
            emit("window", [], "window_truncated")
    except Exception as error:  # no access, not Delta, dropped since the inventory ...
        rows = [(root.table_uri, root.table_full_name, "current", None, snapshot_at, None, None, None,
                 f"error: {str(error)[:200]}")]
    return rows


def model(dbt, session):
    dbt.config(materialized="table")

    roots = (
        dbt.ref("int_s3_inventory__table_paths")
        .filter("data_source_format = 'DELTA' and inventory_snapshot_at is not null")
        .select("table_uri", "table_full_name", "inventory_snapshot_at")
        .collect()
    )
    now = session.sql("select current_timestamp() as now").collect()[0]["now"]

    # each table is a handful of small log-only calls: run them concurrently
    with ThreadPoolExecutor(max_workers=PARALLEL_TABLES) as pool:
        per_table = list(pool.map(lambda root: table_files(session, root, now), roots))

    return session.createDataFrame([row for rows in per_table for row in rows], SCHEMA)
