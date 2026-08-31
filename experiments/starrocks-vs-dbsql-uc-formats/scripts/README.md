# Scripts

Config comes from `${APP_ENV:-dev}.env` at the experiment root (copy
`template.env`). Every script writes machine-readable evidence into
`results/matrix_results.json` keyed by matrix row.

Prerequisites: `make install`, `make auth-check`, the StarRocks VM up
(`make tf-apply`, outputs into `dev.env`), `make setup-uc`, then `make verify`
before any battery.

| Script | What it does |
|---|---|
| `verify.py` | End-to-end auth proof: DBSQL identity + version, warehouse size/DBU basis, UC Iceberg REST `/v1/config` status, StarRocks identity + version. Run before anything else. |
| `00_setup_uc.py` | Creates the evaluation schema and grants `EXTERNAL USE SCHEMA` (required for Iceberg REST credential vending). |
| `battery/01_starrocks_native.py` | CRUD + aggregation battery on a StarRocks primary-key table (baseline). |
| `battery/02_starrocks_uc_iceberg.py` | Same battery, StarRocks writing UC managed Iceberg via the Iceberg REST catalog + vended credentials. Leaves its table for interop 07. |
| `battery/03_starrocks_uc_delta.py` | Evidence script: StarRocks → UC managed Delta is unsupported; records live catalog-creation rejections + doc grounds. |
| `battery/04_dbsql_delta.py` | Same battery, serverless SQL warehouse on managed Delta. |
| `battery/05_dbsql_iceberg.py` | Same battery, serverless SQL warehouse on managed Iceberg. |
| `interop/06_interop_read.py` | DBSQL creates + fills managed Iceberg and managed Delta (UniForm) tables; StarRocks reads both; exact checksum agreement required. |
| `interop/07_interop_write.py` | Cross-engine write probes: StarRocks → DBSQL-created Iceberg and Delta tables; DBSQL → StarRocks-created Iceberg table (read agreement + INSERT/UPDATE/DELETE). |

The battery is one shared runner (`battery/_battery.py`): every op is probed
independently, failures record the server's error verbatim, and a row is only
recorded with a proven engine identity. Timings are client wall-clock from the
operator machine for both engines; costs use the documented model in
`template.env` (EC2 $/hr for StarRocks, warehouse DBU/hr × $/DBU for DBSQL).

## Re-running a cell in a different environment

A cell that failed for environmental reasons has to be re-runnable without
erasing what the first attempt found. Two optional environment variables control
that, both honored by `record_result`:

| Variable | Effect |
|---|---|
| `RESULT_KEY_SUFFIX` | Appended to every row key the run writes, so the rerun lands beside the original row instead of on top of it. |
| `RESULT_RUN_NOTE` | Stored on each row as `run_note` — say which environment produced it. |

The rows suffixed `__rerun_no_ip_acl` come from re-running the StarRocks→UC cells
on a workspace with no IP access list; the unsuffixed rows are the first attempt,
kept intact.
