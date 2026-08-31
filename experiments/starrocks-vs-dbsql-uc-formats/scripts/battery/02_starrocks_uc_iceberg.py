"""Battery row 2 — StarRocks writing UC MANAGED ICEBERG via the Iceberg REST catalog.

CREATE/CTAS/INSERT are documented; UPDATE/DELETE are probed and their verbatim
rejection (if any) recorded — that boundary is a core deliverable of the grid.

The created table is intentionally left in place: interop scripts read and
write it from Databricks SQL afterwards.
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
from _uc_catalog import UC_ICEBERG_CATALOG, create_uc_iceberg_catalog, sr_identity  # noqa: E402

TABLE_BASENAME = "events_sr_iceberg"


def main() -> None:
    cfg = load_config()
    conn = starrocks_conn(cfg)
    catalog_info = create_uc_iceberg_catalog(conn, cfg)
    print(f"UC Iceberg REST catalog ready: {catalog_info}")

    table = f"{UC_ICEBERG_CATALOG}.{cfg.uc_schema}.{TABLE_BASENAME}"
    create_sql = f"""
CREATE TABLE {table} (
  seq BIGINT,
  event_id VARCHAR(64),
  event_ts DATETIME,
  device_id VARCHAR(32),
  region VARCHAR(16),
  value DOUBLE,
  payload VARCHAR(256)
)
"""

    run_battery(
        row_key="battery_02_starrocks_uc_managed_iceberg",
        engine=f"starrocks-{sr_identity(conn)['version']}",
        fmt="uc-managed-iceberg",
        table=table,
        create_sql=create_sql,
        gen_select=gen_select_starrocks(cfg.row_count),
        exec_fn=lambda sql: sr_exec(conn, sql),
        cost_fn=lambda s: starrocks_cost_usd(cfg, s),
        identity=sr_identity(conn),
        agg_runs=cfg.agg_runs,
        row_count=cfg.row_count,
        extra={"uc_catalog_info": catalog_info},
    )
    conn.close()


if __name__ == "__main__":
    main()
