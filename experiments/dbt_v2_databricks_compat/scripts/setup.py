"""Create the schema, the landing volume, the first landing file, and the
freshness source. Idempotent; resets the landing folder to one file so the
streaming-table row always starts from a known state."""

from __future__ import annotations

import io

from _common import FQ_SCHEMA, LANDING_FILES, LANDING_PATH, VOLUME, client, sql


def main() -> None:
    w = client()
    sql(w, f"CREATE SCHEMA IF NOT EXISTS {FQ_SCHEMA}")
    sql(w, f"CREATE VOLUME IF NOT EXISTS {FQ_SCHEMA}.`{VOLUME}`")
    sql(
        w,
        f"CREATE OR REPLACE TABLE {FQ_SCHEMA}.heartbeat AS "
        "SELECT 1 AS id, current_timestamp() AS loaded_at",
    )
    # Snapshots ignore --full-refresh by design, so history from an earlier run
    # would leak into r09's counts. Start it from nothing.
    sql(w, f"DROP TABLE IF EXISTS {FQ_SCHEMA}.r09_snapshot")
    orders = f"{LANDING_PATH}/orders"
    from databricks.sdk.errors import NotFound

    try:
        for entry in w.files.list_directory_contents(orders):
            w.files.delete(entry.path)
    except NotFound:
        print(f"{orders} absent: first run")
    land_file(w, "orders_001.csv")
    print(f"setup ok: {FQ_SCHEMA}, landing at {orders}")


def land_file(w, name: str) -> None:
    data = (LANDING_FILES / name).read_bytes()
    w.files.upload(f"{LANDING_PATH}/orders/{name}", io.BytesIO(data), overwrite=True)


if __name__ == "__main__":
    main()
