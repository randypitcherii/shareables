"""Battery row 4 — Databricks serverless SQL writing UC MANAGED DELTA (the native path)."""

import functools
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _battery import run_battery  # noqa: E402
from _common import (  # noqa: E402
    dbsql_cost_usd,
    dbsql_exec,
    gen_select_dbsql,
    load_config,
    warehouse_dbu_per_hour,
    workspace_client,
)

TABLE_BASENAME = "events_dbsql_delta"


def main() -> None:
    cfg = load_config()
    w = workspace_client(cfg)
    dbu, size = warehouse_dbu_per_hour(cfg, w)
    rows, _ = dbsql_exec(cfg, "SELECT current_user(), current_version()", w)
    identity = {"user": rows[0][0], "version": str(rows[0][1])}

    table = f"{cfg.uc_catalog}.{cfg.uc_schema}.{TABLE_BASENAME}"
    create_sql = f"""
CREATE TABLE {table} (
  seq BIGINT,
  event_id STRING,
  event_ts TIMESTAMP_NTZ,
  device_id STRING,
  region STRING,
  value DOUBLE,
  payload STRING
)
"""

    run_battery(
        row_key="battery_04_dbsql_uc_managed_delta",
        engine="dbsql-serverless",
        fmt="uc-managed-delta",
        table=table,
        create_sql=create_sql,
        gen_select=gen_select_dbsql(cfg.row_count),
        exec_fn=functools.partial(dbsql_exec, cfg, w=w),
        cost_fn=lambda s: dbsql_cost_usd(dbu, cfg.usd_per_dbu, s),
        identity=identity,
        agg_runs=cfg.agg_runs,
        row_count=cfg.row_count,
        extra={"warehouse_size": size, "dbu_per_hour": dbu},
    )


if __name__ == "__main__":
    main()
