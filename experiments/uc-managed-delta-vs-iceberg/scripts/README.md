# Experiment scripts

`_common.py` runs UC SQL on a serverless warehouse, refuses PATs, attaches DuckDB to Unity Catalog through both of its paths, scrubs evidence, and writes `results/matrix_results.json`. `spark/matrix_job.py` runs on a Databricks job cluster and only gathers raw evidence. The numbered scripts turn that evidence into matrix rows.

## Prerequisites

- An OAuth/SSO Databricks CLI profile (PATs are refused) and a serverless SQL warehouse.
- **External data access** enabled on the metastore. `setup.py` grants `EXTERNAL USE SCHEMA` to the caller, because owning a schema does not include it.
- A writable path under a UC external location in your own bucket (`EXPERIMENT_STORAGE_ROOT`). It becomes the catalog's managed location. Default Storage won't work, because external engines need credential vending.
- Permission to create classic job clusters on DBR 19+ (automatic CDF needs DBR 19), and to create and drop a catalog.

## Lifecycle

| Script | Purpose |
|---|---|
| `verify.py` | Checks that REST and SQL resolve to the same principal, that the warehouse is reachable, that external data access is on, and that one real Iceberg REST catalog call succeeds. |
| `setup.py` | Creates the catalog, schema and evidence volume, plus Liquid tables. `dml_delta`, `dml_iceberg`, `py_iceberg_v3`, `py_iceberg_v2` and `py_delta` each get insert → update → delete from Databricks. `dml_iceberg_clean` gets insert only (the control). Each table is dropped and recreated, because catalog commits can't be toggled through `CREATE OR REPLACE`. |
| `01_duckdb_dml.py` | Row 01. DuckDB reads plus INSERT/UPDATE/DELETE/MERGE on both formats, through the `unity_catalog` path (read-write, custom User-Agent, read-only) and the `iceberg` REST path (strict, then with `iceberg_unsafe_skip_puffin_verification`). Databricks SQL reads every table back afterwards. |
| `spark_run.py` | Uploads `spark/matrix_job.py`, submits a one-time run on a single-node DBR 19 cluster, and downloads raw evidence to `results/.spark_raw.json` (gitignored). |
| `02_change_data_feed.py` | Row 02. Automatic CDF on the `dml_*` tables: batch `readChangeFeed`, `table_changes()`, streaming `readChangeFeed`, and DBSQL `table_changes()`. It also checks whether DuckDB's commits from row 01 appear in the feed. |
| `03_structured_streaming.py` | Row 03. Each format as a streaming sink (default writer and `.format('iceberg')`) and as a plain streaming source. Every stream runs twice with `availableNow`, with an append in between. |
| `04_dataframe_writers.py` | Row 04. V1 `saveAsTable` (no format, `format('delta')`, `format('iceberg')`), `insertInto`, V2 `writeTo().append()`, `DeltaTable.forName().merge()`, plus which format the create-style writers produce. |
| `05_pyiceberg_writes.py` | Row 05. PyIceberg read/append/delete/overwrite/upsert against Iceberg v3, an Iceberg v2 control, and Delta with Iceberg reads. |
| `cleanup.py` | Drops the experiment catalog (CASCADE). |

Statuses per cell: `pass` means every operation tested worked. `partial` means some did, and the finding says which. `fail` means none did. `inconclusive` is reserved for infrastructure failures that never reached the capability under test.

Order matters: row 02 reads the commits that row 01 made, so run `make matrix` (or keep the same order).
