"""Battery row 1 — StarRocks writing its NATIVE format (primary-key OLAP table).

The baseline: the incumbent serving path. Full CRUD is expected to work; the
numbers here anchor what the lake formats are traded against.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _battery import run_battery  # noqa: E402
from _common import (  # noqa: E402
    gen_select_starrocks,
    load_config,
    sr_exec,
    starrocks_conn,
    starrocks_cost_usd,
)
from _uc_catalog import sr_identity  # noqa: E402

TABLE = "sr_eval.events_native"

CREATE_SQL = f"""
CREATE TABLE {TABLE} (
  seq BIGINT NOT NULL,
  event_id VARCHAR(64),
  event_ts DATETIME,
  device_id VARCHAR(32),
  region VARCHAR(16),
  value DOUBLE,
  payload VARCHAR(256)
) PRIMARY KEY (seq)
DISTRIBUTED BY HASH(seq) BUCKETS 8
PROPERTIES ("replication_num" = "1")
"""


def main() -> None:
    cfg = load_config()
    conn = starrocks_conn(cfg)
    sr_exec(conn, "CREATE DATABASE IF NOT EXISTS sr_eval")

    run_battery(
        row_key="battery_01_starrocks_native",
        engine=f"starrocks-{sr_identity(conn)['version']}",
        fmt="starrocks-native-primary-key",
        table=TABLE,
        create_sql=CREATE_SQL,
        gen_select=gen_select_starrocks(cfg.row_count),
        exec_fn=lambda sql: sr_exec(conn, sql),
        cost_fn=lambda s: starrocks_cost_usd(cfg, s),
        identity=sr_identity(conn),
        agg_runs=cfg.agg_runs,
        row_count=cfg.row_count,
    )
    conn.close()


if __name__ == "__main__":
    main()
