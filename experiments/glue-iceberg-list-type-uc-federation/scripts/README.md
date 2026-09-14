# scripts/

Every script is thin: config + clients + result writing live in `_common.py`;
the write → inspect-SD → probe-UC loop lives in `matrix/_matrix_common.py`.

## Prerequisites

- `make tf-apply` done and its `env_snippet` pasted into `dev.env`
- Databricks OAuth/SSO profile with metastore-admin-level privileges
  (`CREATE SERVICE CREDENTIAL`, `CREATE STORAGE CREDENTIAL`, `CREATE CONNECTION`,
  `CREATE CATALOG`) — needed once, by `00_setup_uc.py`
- AWS SSO profile that can call Glue, S3, STS, and Athena in the target account

## Lifecycle scripts

| Script | Does |
|---|---|
| `00_setup_uc.py` | Creates (idempotently) the service credential, storage credential, external location, Glue connection, and foreign catalog with `storage_root` |
| `01_teardown_uc.py` | Drops those in reverse order and drops every experiment table from Glue |
| `verify.py` | One real call per surface: DBSQL identity, AWS caller, Glue `GetDatabase`, SigV4 `GET /v1/config` on the Glue REST endpoint, `SHOW SCHEMAS` on the foreign catalog |

## Matrix scripts (`matrix/`)

Each writes ONE key into `results/matrix_results.json`. Each is re-runnable:
tables are reused if present and a fresh append commit is always made.

| # | Script | Writer | Shape | Records |
|---|---|---|---|---|
| 01 | `01_baseline_primitives_rest.py` | rest | primitives | SD types, UC describe + count |
| 02 | `02_list_primitive_rest.py` | rest | list_primitive | SD token (`list<` vs `array<`), UC error class, `reproduced` flag |
| 03 | `03_list_primitive_glue_native.py` | glue | list_primitive | same, for the client-side writer |
| 04 | `04_type_shape_sweep.py` | both | all six | per-cell SD complex types + UC verdict/error |
| 05 | `05_failure_surfaces.py` | rest | list_primitive | 8 SQL surfaces, each with its own error class |
| 06 | `06_sd_patch_durability.py` | rest | list_primitive | before-patch / after-patch / after-next-commit SD + UC verdict |
| 07 | `07_athena_cross_reader.py` | rest | list_primitive | Athena `COUNT`, sample, `DESCRIBE` |
| 08 | `08_uc_side_overrides.py` | rest | list_primitive | `REFRESH`, `read_files`, optional bypass `CREATE TABLE`/`VIEW` |

Row 08's bypass attempts need `UC_BYPASS_CATALOG` / `UC_BYPASS_SCHEMA` pointing
at a writable catalog.schema; without them those attempts are skipped and noted.

## Writer paths

| key | pyiceberg catalog | Writes the Glue SD |
|---|---|---|
| `rest` | `type=rest`, `uri=https://glue.<region>.amazonaws.com/iceberg`, SigV4 | the AWS service |
| `glue` | `type=glue` | the client (`array<…>` for LIST, like the Java converter) |

## Type shapes (`_common.SHAPES`)

`primitives`, `list_primitive`, `list_struct`, `map_primitive`,
`struct_with_list`, `nested_struct_required`. Each has a pyiceberg `Schema`
builder and a matching one-row generator; `tests/test_common.py` proves every
shape round-trips through Arrow before anything touches the cloud.

## Failure output is data

Probes never `raise_for_status()` or retry on type errors. The bracketed error
condition (e.g. `INTERNAL_ERROR_INVALID_HIVE_COLUMN_TYPE`) is extracted into
`error_class` and the first 1500 chars of the server message are kept. A
`TABLE_OR_VIEW_NOT_FOUND` immediately after a create is treated as "not synced
yet" and retried up to 6×10 s — anything else is recorded on the first attempt.

## Redaction

`record_result()` redacts on the way out: bucket, account id, role ARN, any
`arn:aws:*::<12 digits>:…`, workspace hosts, emails. Do not bypass it.
