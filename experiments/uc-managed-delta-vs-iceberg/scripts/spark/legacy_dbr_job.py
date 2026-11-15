"""Row 08 job: what can an older DBR LTS do with each managed table shape? Runs once per pinned
DBR (one task each). Every operation is attempted; failures are evidence, never exceptions.

Args: catalog schema volume_path tag
"""

import json
import sys
import traceback

from pyspark.sql import SparkSession

catalog, schema, volume_path, tag = sys.argv[1:5]
spark = SparkSession.builder.getOrCreate()
fq = f"`{catalog}`.`{schema}`"
dbr = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "unknown")
TABLES = ["dbr_ice_v3", "dbr_ice_v2", "dbr_delta_v3reads", "dbr_delta_v2reads", "dbr_delta_plain"]


def attempt(fn):
    try:
        return {"ok": True, "value": fn()}
    except Exception as exc:  # noqa: BLE001
        msg = str(exc).split("\nJVM stacktrace")[0]
        return {"ok": False, "error_class": type(exc).__name__, "error": msg[:1500]}


def run(stmt):
    spark.sql(stmt).collect()
    return "ok"


cells = {}
for t in TABLES:
    name = f"{fq}.{t}"
    cells[f"{t}.read"] = attempt(
        lambda name=name: [list(r) for r in spark.sql(f"SELECT id, cat, v FROM {name} ORDER BY id").collect()]
    )
    cells[f"{t}.insert"] = attempt(lambda name=name: run(f"INSERT INTO {name} VALUES (100, 'z', 1)"))
    cells[f"{t}.update"] = attempt(lambda name=name: run(f"UPDATE {name} SET v = v + 1 WHERE id = 100"))
    cells[f"{t}.table_changes"] = attempt(
        lambda name=name: spark.sql(f"SELECT count(*) FROM table_changes('{name.replace('`', '')}', 0)").collect()[0][0]
    )

out = {"dbr": dbr, "tag": tag, "cells": cells}
path = f"{volume_path}/out/legacy_dbr_{dbr.split('-')[0]}_{tag}.json"
try:
    dbutils.fs.put(path, json.dumps(out, default=str), True)  # noqa: F821 -- Databricks global
except Exception:  # noqa: BLE001
    traceback.print_exc()
print(json.dumps({k: v["ok"] for k, v in cells.items()}))
