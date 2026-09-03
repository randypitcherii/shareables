# Experiment scripts

`_common.py` runs UC SQL, refuses PATs, scrubs evidence, and writes `results/matrix_results.json`. `exits.py` holds the catalog-free Python clients (deltalake, duckdb, pyiceberg) that stand in for "the destination"; they never talk to Unity Catalog and are unit-tested against local tables.

## Prerequisites

- OAuth/SSO Databricks CLI profile and a serverless SQL warehouse
- A writable UC external location in your own S3 bucket (`EXPERIMENT_EXTERNAL_ROOT`)
- An AWS SSO profile that can read/write that bucket directly
- Permission to create and drop a catalog

## Lifecycle

| Script | Purpose |
|---|---|
| `verify.py` | Live identity, external-location, and direct-S3 preflight. |
| `setup.py` | Creates a catalog with `storage_root` under the external location, then a UniForm managed Delta table and a managed Iceberg table (3 rows each). |
| `exit_01_zero_catalog_delta.py` | Rows 1a/1b: read each table's `_delta_log` from S3 with deltalake and duckdb; no catalog, no copy. |
| `exit_03_iceberg_new_catalog.py` | Rows 3a/3b: register each table's newest `metadata.json` in a fresh pyiceberg SQLite catalog, read, then append a snapshot in place. |
| `exit_02_uc_external_reregister.py` | Rows 2a/2b: `CREATE TABLE … LOCATION` over the managed path, live and after `DROP TABLE`, plus a copied-outside-`__unitystorage` control. Runs last because it drops the sources. |
| `cleanup.py` | Drops the experiment catalog. Object-storage files are left for the owner. |

Statuses: `pass` every client succeeded with all rows · `partial` at least one client succeeded · `fail` no no-copy path worked · `inconclusive` reserved for infrastructure failures that never reached the capability under test.
