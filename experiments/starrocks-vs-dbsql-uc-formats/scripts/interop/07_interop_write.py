"""Interop — cross-engine WRITES to tables the other engine created.

1. StarRocks -> DBSQL-created managed ICEBERG table: INSERT / UPDATE / DELETE
   probes, each verified (or refuted) from the warehouse side.
2. StarRocks -> DBSQL-created managed DELTA table (UniForm, via Iceberg REST):
   write probes expected to be rejected — the rejection text is the evidence.
3. DBSQL -> StarRocks-created managed ICEBERG table (battery 02's table):
   read checksum agreement + INSERT / UPDATE / DELETE probes.

Requires 06_interop_read.py (creates the interop tables) and battery 02
(creates the StarRocks-authored table) to have run first.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common import (  # noqa: E402
    CHECKSUM_SQL,
    EVENT_COLUMNS,
    dbsql_exec,
    load_config,
    record_result,
    section,
    sr_exec,
    starrocks_conn,
    workspace_client,
)
from _uc_catalog import UC_ICEBERG_CATALOG, create_uc_iceberg_catalog, sr_identity  # noqa: E402

ICE = "interop_ice"
DELTA = "interop_delta"
SR_AUTHORED = "events_sr_iceberg"

PROBE_BASE = 200_000_000


def norm(row) -> list[int]:
    return [int(v) for v in row]


def probe_writes(exec_fn, table: str, base_seq: int) -> dict:
    """INSERT/UPDATE/DELETE one probe row; every outcome (incl. rejection) is data."""
    out = {}
    values = f"({base_seq}, 'interop-probe', '2026-01-03 00:00:00', 'dev-x', 'us-east', 1.5, 'p')"
    for op, sql in (
        ("insert_1", f"INSERT INTO {table} {EVENT_COLUMNS} VALUES {values}"),
        ("update_1", f"UPDATE {table} SET value = 2.5 WHERE seq = {base_seq}"),
        ("delete_1", f"DELETE FROM {table} WHERE seq = {base_seq}"),
    ):
        try:
            _, elapsed = exec_fn(sql)
            out[op] = {"status": "ok", "elapsed_ms": round(elapsed * 1000)}
        except Exception as e:  # noqa: BLE001
            out[op] = {"status": "error", "error": str(e)[:1200]}
    return out


def main() -> None:
    cfg = load_config()
    w = workspace_client(cfg)
    rows, _ = dbsql_exec(cfg, "SELECT current_user()", w)
    conn = starrocks_conn(cfg)
    catalog_info = create_uc_iceberg_catalog(conn, cfg)
    identity = {"dbsql": rows[0][0], "starrocks": sr_identity(conn)}
    fqn = f"{cfg.uc_catalog}.{cfg.uc_schema}"

    # --- 1. StarRocks writes into the DBSQL-created ICEBERG table ---
    section("StarRocks -> DBSQL-created managed Iceberg: write probes")
    sr_ice = f"{UC_ICEBERG_CATALOG}.{cfg.uc_schema}.{ICE}"
    ops = probe_writes(lambda sql: sr_exec(conn, sql), sr_ice, PROBE_BASE + 1)
    # Warehouse-side verification: how many probe rows actually landed/survived?
    vrows, _ = dbsql_exec(
        cfg, f"SELECT COUNT(*) FROM {fqn}.{ICE} WHERE seq = {PROBE_BASE + 1}", w
    )
    record_result(
        "interop_write_starrocks_to_dbsql_iceberg",
        {
            "creator": "dbsql-serverless",
            "format": "uc-managed-iceberg",
            "accessor": "starrocks",
            "operation": "write",
            "identity": identity,
            "uc_catalog_info": catalog_info,
            "ops": ops,
            "probe_rows_remaining_dbsql_view": int(vrows[0][0]),
        },
    )
    print(ops)

    # --- 2. StarRocks writes into the DBSQL-created DELTA (UniForm) table ---
    section("StarRocks -> DBSQL-created managed Delta (via Iceberg REST): write probes")
    sr_delta = f"{UC_ICEBERG_CATALOG}.{cfg.uc_schema}.{DELTA}"
    ops = probe_writes(lambda sql: sr_exec(conn, sql), sr_delta, PROBE_BASE + 2)
    vrows, _ = dbsql_exec(
        cfg, f"SELECT COUNT(*) FROM {fqn}.{DELTA} WHERE seq = {PROBE_BASE + 2}", w
    )
    record_result(
        "interop_write_starrocks_to_dbsql_delta",
        {
            "creator": "dbsql-serverless",
            "format": "uc-managed-delta-uniform",
            "accessor": "starrocks",
            "operation": "write",
            "identity": identity,
            "uc_catalog_info": catalog_info,
            "ops": ops,
            "probe_rows_remaining_dbsql_view": int(vrows[0][0]),
        },
    )
    print(ops)

    # --- 3. DBSQL reads + writes the StarRocks-created ICEBERG table ---
    section("DBSQL -> StarRocks-created managed Iceberg: read agreement + write probes")
    payload = {
        "creator": "starrocks",
        "format": "uc-managed-iceberg",
        "accessor": "dbsql-serverless",
        "operation": "read+write",
        "identity": identity,
    }
    try:
        # StarRocks CREATEd this table but could not bulk-INSERT into it (see
        # battery 02); fill it from the warehouse so read-agreement is over real
        # data. This is itself a write-interop datapoint (DBSQL DML on an
        # externally-created managed Iceberg table).
        from _common import gen_select_dbsql

        count_rows, _ = dbsql_exec(cfg, f"SELECT COUNT(*) FROM {fqn}.{SR_AUTHORED}", w)
        if int(count_rows[0][0]) == 0:
            _, fill_elapsed = dbsql_exec(
                cfg,
                f"INSERT INTO {fqn}.{SR_AUTHORED} {EVENT_COLUMNS} "
                f"{gen_select_dbsql(cfg.row_count)}",
                w,
            )
            payload["dbsql_bulk_fill_elapsed_ms"] = round(fill_elapsed * 1000)
    except Exception as e:  # noqa: BLE001
        payload["dbsql_bulk_fill_error"] = str(e)[:1200]
    try:
        sr_rows, _ = sr_exec(
            conn, CHECKSUM_SQL.format(table=f"{UC_ICEBERG_CATALOG}.{cfg.uc_schema}.{SR_AUTHORED}")
        )
        db_rows, elapsed = dbsql_exec(cfg, CHECKSUM_SQL.format(table=f"{fqn}.{SR_AUTHORED}"), w)
        payload["starrocks_checksum"] = norm(sr_rows[0])
        payload["dbsql_checksum"] = norm(db_rows[0])
        payload["dbsql_read_elapsed_ms"] = round(elapsed * 1000)
        payload["read_status"] = (
            "ok" if payload["starrocks_checksum"] == payload["dbsql_checksum"]
            else "checksum_mismatch"
        )
    except Exception as e:  # noqa: BLE001
        payload["read_status"] = "error"
        payload["read_error"] = str(e)[:1500]
    payload["ops"] = probe_writes(
        lambda sql: dbsql_exec(cfg, sql, w), f"{fqn}.{SR_AUTHORED}", PROBE_BASE + 3
    )
    record_result("interop_rw_dbsql_on_starrocks_iceberg", payload)
    print(payload.get("read_status"), payload["ops"])

    conn.close()


if __name__ == "__main__":
    main()
