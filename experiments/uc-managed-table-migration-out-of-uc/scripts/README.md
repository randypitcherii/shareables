# Experiment scripts

Every matrix row has one numbered script. The shared `_common.py` module runs SQL, refuses PATs, scrubs evidence, and writes `results/matrix_results.json`.

## Prerequisites

- An OAuth/SSO Databricks CLI profile
- A serverless SQL warehouse
- A catalog where the caller can create an isolated schema
- A dedicated prefix under a writable UC external location
- An authenticated AWS SSO profile with access to S3, Glue, and Athena
- A temporary Glue database name and Athena result prefix
- An isolated UC catalog name and managed storage root under the external location

## Lifecycle

| Script | Purpose |
|---|---|
| `verify.py` | Makes live identity and catalog calls, then confirms the external root is governed by a visible writable external location. |
| `setup.py` | Creates three-row managed Delta and managed Iceberg source tables. |
| `migrations/01_delta_to_uncataloged_path.py` | Copies managed Delta to a path-based Delta table. |
| `migrations/02_delta_to_uc_external.py` | Copies managed Delta into a UC external table. |
| `migrations/03_delta_unregister_reregister.py` | Tests the no-copy `UNREGISTER TABLE` claim. |
| `migrations/04_managed_iceberg_export.py` | Copies managed Iceberg through Parquet staging into AWS Glue Iceberg, then verifies an Athena update. |
| `migrations/05_delta_to_glue_athena.py` | Records Athena's rejection of the default Delta protocol and verifies a compatibility-targeted Delta copy. |
| `migrations/06_customer_storage_delta_no_copy.py` | Creates a customer-rooted UC catalog and compares default vs portability-targeted managed Delta registration without copying data. |
| `migrations/07_customer_storage_iceberg_no_copy.py` | Registers managed Iceberg metadata/files in Glue without a copy, writes through Athena, and proves the catalog pointers diverge. |
| `cleanup.py` | Drops the isolated UC schema/catalog and temporary Glue database. It does not delete uncataloged object-storage files. |

Storage authorization failures are `inconclusive`, not capability failures. A parse or platform refusal is a `fail`. This prevents a broken cloud credential from producing a believable portability verdict.
