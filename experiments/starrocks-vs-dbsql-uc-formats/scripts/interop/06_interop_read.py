"""Interop — StarRocks READING tables created and filled by Databricks SQL.

Creates two tables from the warehouse:
  interop_ice   — UC managed Iceberg
  interop_delta — UC managed Delta with Iceberg reads enabled (UniForm metadata),
                  the ONLY path by which an Iceberg REST client can see managed Delta.

Then reads both from StarRocks through the UC Iceberg REST catalog and demands
exact row-count + checksum agreement. A row is ✅ only on full agreement.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common import (  # noqa: E402
    CHECKSUM_SQL,
    EVENT_COLUMNS,
    dbsql_exec,
    gen_select_dbsql,
    load_config,
    record_result,
    section,
    sr_exec,
    starrocks_conn,
    timing_stats,
    workspace_client,
)
from _uc_catalog import UC_ICEBERG_CATALOG, create_uc_iceberg_catalog, sr_identity  # noqa: E402

ICE = "interop_ice"
DELTA = "interop_delta"


def norm(row) -> list[int]:
    return [int(v) for v in row]


def main() -> None:
    cfg = load_config()
    w = workspace_client(cfg)
    rows, _ = dbsql_exec(cfg, "SELECT current_user()", w)
    dbsql_user = rows[0][0]

    section("DBSQL: create + fill interop tables")
    fqn = f"{cfg.uc_catalog}.{cfg.uc_schema}"
    cols = (
        "seq BIGINT, event_id STRING, event_ts TIMESTAMP_NTZ, device_id STRING, "
        "region STRING, value DOUBLE, payload STRING"
    )
    dbsql_exec(cfg, f"DROP TABLE IF EXISTS {fqn}.{ICE}", w)
    dbsql_exec(cfg, f"CREATE TABLE {fqn}.{ICE} ({cols}) USING ICEBERG", w)
    dbsql_exec(cfg, f"DROP TABLE IF EXISTS {fqn}.{DELTA}", w)
    dbsql_exec(
        cfg,
        f"""CREATE TABLE {fqn}.{DELTA} ({cols}) TBLPROPERTIES (
            'delta.enableIcebergCompatV2' = 'true',
            'delta.universalFormat.enabledFormats' = 'iceberg')""",
        w,
    )
    gen = gen_select_dbsql(cfg.row_count)
    dbsql_exec(cfg, f"INSERT INTO {fqn}.{ICE} {EVENT_COLUMNS} {gen}", w)
    dbsql_exec(cfg, f"INSERT INTO {fqn}.{DELTA} {EVENT_COLUMNS} {gen}", w)
    print("both tables filled")

    checks = {}
    for t in (ICE, DELTA):
        rows, _ = dbsql_exec(cfg, CHECKSUM_SQL.format(table=f"{fqn}.{t}"), w)
        checks[t] = norm(rows[0])
        print(f"DBSQL checksum {t}: {checks[t]}")

    section("StarRocks: read both through UC Iceberg REST")
    conn = starrocks_conn(cfg)
    catalog_info = create_uc_iceberg_catalog(conn, cfg)
    identity = {"dbsql": dbsql_user, "starrocks": sr_identity(conn)}

    for t, key in ((ICE, "interop_read_dbsql_iceberg_to_starrocks"),
                   (DELTA, "interop_read_dbsql_delta_to_starrocks")):
        sr_table = f"{UC_ICEBERG_CATALOG}.{cfg.uc_schema}.{t}"
        payload = {
            "creator": "dbsql-serverless",
            "format": "uc-managed-iceberg" if t == ICE else "uc-managed-delta-uniform",
            "accessor": "starrocks",
            "operation": "read",
            "identity": identity,
            "uc_catalog_info": catalog_info,
            "dbsql_checksum": checks[t],
        }
        try:
            timings = []
            sr_check = None
            for _ in range(3):
                rows, elapsed = sr_exec(conn, CHECKSUM_SQL.format(table=sr_table))
                sr_check = norm(rows[0])
                timings.append(elapsed)
            payload["starrocks_checksum"] = sr_check
            payload["read_timing"] = timing_stats(timings)
            payload["status"] = "ok" if sr_check == checks[t] else "checksum_mismatch"
        except Exception as e:  # noqa: BLE001 — error text is the finding
            payload["status"] = "error"
            payload["error"] = str(e)[:1500]
        print(key, payload.get("status"), payload.get("error", ""))
        record_result(key, payload)

    conn.close()


if __name__ == "__main__":
    main()
