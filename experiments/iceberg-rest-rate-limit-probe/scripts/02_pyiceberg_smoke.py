"""PyIceberg sanity check against the UC Iceberg REST catalog (before any load).

Confirms the catalog, schema, EXTERNAL USE SCHEMA grant, and credential vending all
work for the probe principal, and records how PyIceberg surfaces a throttle: it maps
HTTP 429 to a plain RESTError / ServerError with no automatic retry, so any backoff
is the caller's job.
"""

import json
import time

import pyiceberg
from _common import IRC_PATH, RESULTS_DIR, host_and_headers, load_config, stamp, write_json
from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    cfg = load_config()
    host, headers = host_and_headers(cfg.profile("a"))
    token = headers["Authorization"].split(" ", 1)[1]
    t = time.perf_counter()
    cat = RestCatalog("uc", uri=f"{host}{IRC_PATH}", token=token, warehouse=cfg.catalog("a"))
    config_ms = (time.perf_counter() - t) * 1000
    t = time.perf_counter()
    tables = cat.list_tables(cfg.uc_schema)
    list_ms = (time.perf_counter() - t) * 1000
    t = time.perf_counter()
    tbl = cat.load_table((cfg.uc_schema, "t000"))
    load_ms = (time.perf_counter() - t) * 1000
    rows = tbl.scan().to_arrow().num_rows
    result = {
        "stamp": stamp(cfg, IRC_PATH),
        "pyiceberg_version": pyiceberg.__version__,
        "tables_listed": len(tables),
        "t000_rows_via_vended_credentials": rows,
        "latency_ms": {"config": round(config_ms), "list_tables": round(list_ms),
                       "load_table": round(load_ms)},
    }
    print(json.dumps(result, indent=2))
    write_json(RESULTS_DIR / "pyiceberg_smoke.json", result)


if __name__ == "__main__":
    main()
