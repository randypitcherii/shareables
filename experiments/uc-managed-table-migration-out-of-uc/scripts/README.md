# Experiment scripts

Every matrix row has one numbered script. The shared `_common.py` module runs SQL, refuses PATs, scrubs evidence, and writes `results/matrix_results.json`.

## Prerequisites

- An OAuth/SSO Databricks CLI profile
- A serverless SQL warehouse
- A catalog where the caller can create an isolated schema
- A dedicated prefix under a writable UC external location
- For row 4, an independent Iceberg catalog (not configured in the first run)

## Lifecycle

| Script | Purpose |
|---|---|
| `verify.py` | Makes live identity and catalog calls, then confirms the external root is governed by a visible writable external location. |
| `setup.py` | Creates three-row managed Delta and managed Iceberg source tables. |
| `migrations/01_delta_to_uncataloged_path.py` | Copies managed Delta to a path-based Delta table. |
| `migrations/02_delta_to_uc_external.py` | Copies managed Delta into a UC external table. |
| `migrations/03_delta_unregister_reregister.py` | Tests the no-copy `UNREGISTER TABLE` claim. |
| `migrations/04_managed_iceberg_export.py` | Refuses to assert portability without an independent catalog destination. |
| `cleanup.py` | Drops the isolated UC schema. It does not delete uncataloged object-storage files. |

Storage authorization failures are `inconclusive`, not capability failures. A parse or platform refusal is a `fail`. This prevents a broken cloud credential from producing a believable portability verdict.
