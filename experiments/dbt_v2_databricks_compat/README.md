# What works with dbt v2 on Databricks today?

**The question.** dbt v2 (the Rust engine, shipped as `dbt-oss`, Databricks
adapter built in) is GA. A team on Databricks wants to move a medallion project
to it now. That project has streaming tables over files in a UC volume, and the
team wants Lakeflow Jobs as the orchestrator, not Airflow or Dagster. What works
locally? What works inside a job? Which Jobs task should they use while the
dedicated dbt v2 task type is still on the roadmap?

**The answer, in one line.** Nearly everything works. 24 of 26 local rows pass
and both Jobs paths build the whole project. The **native `dbt_task` already
runs dbt v2** if you swap its environment dependency to the `dbt-oss` wheel. The
gaps:

- `auth_type: databricks_cli` does not exist in v2.
- `docs generate` no longer writes `catalog.json`.
- A serverless environment pinned to one platform's wheel fails on the other
  architecture.

Run date: **2026-11-09**, dbt-oss **2.0.11**, the latest v2 release (2026-11-03).
The workspace was AWS Databricks with Unity Catalog. SQL ran on a serverless SQL
warehouse (PRO). The Python model and both jobs ran on serverless jobs compute,
environment version 3. Each row below was produced by a script in `scripts/`,
which writes to `results/matrix_results.json`. That file is the source of truth;
the table is generated from it (`make readme`).

## Findings matrix

`r` rows run dbt from a laptop against the warehouse (`make matrix`). Each one
is checked by **reading the result back from Unity Catalog**, not by trusting
the exit code. `j` rows deploy this directory as a bundle and run the project
inside Lakeflow Jobs (`make jobs`).

<!-- MATRIX:START -->
| # | Question | Result | Evidence |
|---|---|---|---|
| r01 | Does `dbt-oss` install as one wheel with no separate Databricks adapter package? | ✅ | dbt-oss 2.0.11 |
| r02 | Does a bearer token (`token:` profile field) connect? (`dbt debug`) | ✅ | dbt debug: All checks passed |
| r03 | Does `auth_type: databricks_cli` (reuse the Databricks CLI login) connect? | ❌ | [error] [AuthenticationFailed (dbt1011)]: dbt was unable to connect to the database configured for the `databricks` adapter. The following error was returned: Serde YAML Error: missing field `token` |
| r04 | Do `dbt deps` (dbt_utils from the hub) and `dbt seed` work? | ✅ | events seed rows: 8 (expected 8) |
| r05 | Do `view` models build? | ✅ | r05_view rows after build 2: 8 (expected 8) |
| r06 | Do `table` models build? | ✅ | r06_table rows after build 2: 2 (expected 2) |
| r07 | Are `ephemeral` models inlined into their consumer as a CTE? | ✅ | (rows, CTE inlined): (3, True) (expected (3, True)) |
| r08 | Do generic data tests, singular tests, and unit tests run? | ✅ | 5 tests passed (data + singular + unit) |
| r09 | Does a YAML-defined `check` snapshot close the old row on change? | ✅ | (rows, closed rows): (4, 1) (expected (4, 1)) |
| r10 | Incremental `append`: second pass appends only new rows? | ✅ | (rows, amount of id 2) after pass 2: (8, 25.0) (expected (8, 25.0)) |
| r11 | Incremental `merge` on `unique_key`: second pass upserts? | ✅ | (rows, amount of id 2) after pass 2: (7, 25.0) (expected (7, 25.0)) |
| r12 | Incremental `delete+insert` on `unique_key`: second pass replaces matching keys? | ✅ | (rows, amount of id 2) after pass 2: (7, 25.0) (expected (7, 25.0)) |
| r13 | Incremental `insert_overwrite` by `partition_by`: only touched partitions replaced? | ✅ | (rows, amount of id 2) after pass 2: (5, 25.0) (expected (5, 25.0)) |
| r14 | Incremental `replace_where` with `incremental_predicates`? | ✅ | (rows, amount of id 2) after pass 2: (7, 20.0) (expected (7, 20.0)) |
| r15 | Incremental `microbatch`: per-batch runs, and a targeted `--event-time-start/end` backfill? | ✅ | (rows, batches run) after Jan–Apr backfill at batch 2: (8, 4) (expected (8, 4)) |
| r16 | `streaming_table` reading CSVs from a UC volume with `read_files`: picks up new files? | ✅ | (rows after file 1, rows after file 2): (3, 5) (expected (3, 5)) |
| r17 | `materialized_view` builds and refreshes on re-run? | ✅ | orders counted by the MV after the second build: 5 (expected 5) |
| r18 | Liquid clustering: `liquid_clustered_by` and `auto_liquid_cluster`? | ✅ | (clustering columns, clusterByAuto): (['customer_id', 'event_date'], 'true') (expected (['customer_id', 'event_date'], 'true')) |
| r19 | `table_format='iceberg'` creates a managed Iceberg table? | ✅ | provider: 'iceberg' (expected 'iceberg') |
| r20 | `persist_docs`, `databricks_tags` (table + column), and `tblproperties` land in UC? | ✅ | (table comment, column comment, table tag, column tags, tblproperty): (True, True, 'r20', [['dbt_v2_compat_column', 'r20']], 'r20') (expected (True, True, 'r20', [['dbt_v2_compat_column', 'r20']], 'r20')) |
| r21 | Model contracts with `not_null` + `primary_key` constraints? | ✅ | (primary keys, customer_id nullable): (1, False) (expected (1, False)) |
| r22 | Python model with `submission_method: serverless_cluster`? | ✅ | r22_python_model rows after build 2: 3 (expected 3) |
| r23 | `metric_view` materialization creates a UC metric view? | ✅ | table type: 'METRIC_VIEW' (expected 'METRIC_VIEW') |
| r24 | Hub package macros (`dbt_utils.generate_surrogate_key`)? | ✅ | r24_dbt_utils_package rows after build 2: 8 (expected 8) |
| r25 | `dbt docs generate` builds the docs site (and v1's `catalog.json`)? | ◑ | static site (index.html) + 38 Parquet artifacts under target/info_schema/; no catalog.json — tools that read v1's catalog.json need a change |
| r26 | `dbt source freshness` against a UC table? | ✅ | source freshness passed |
| j01 | Native Lakeflow Jobs `dbt_task` (serverless) with the dbt v2 wheel as its environment dependency: does the whole project build? | ✅ | Summary: 29 total \| 27 success \| 1 warn \| 1 no-op |
| j02 | Serverless Python task running the dbt v2 binary as the job's `run_as` identity: does the whole project build? | ✅ | Summary: 29 total \| 27 success \| 1 warn \| 1 no-op |
| j03 | With per-platform PEP 508 markers on the wheel URLs, does the serverless environment install dbt v2 on the first attempt? | ✅ | every task attempt installed the wheel |
<!-- MATRIX:END -->

Status vocabulary: ✅ works as claimed · ❌ does not, with evidence ·
◑ partially.

## Key findings

### The native dbt task already runs dbt v2

The Jobs `dbt_task` installs `dbt-databricks>=1.0.0,<2.0.0` by default, and a
dedicated dbt v2 task type is on the roadmap. Neither stops you today. The task
runs whatever `dbt` its serverless environment puts on `PATH`. Make the
environment dependency the `dbt-oss` wheel, and the task does the rest:

- It writes a `profiles.yml` from `warehouse_id`, `catalog`, and `schema`.
- It injects a token for the job's `run_as` identity.
- It runs your commands, and v2 accepts the generated profile unchanged.

j01 built all 29 nodes this way. That covers seeds, every incremental strategy,
the snapshot, a streaming table over a UC volume, a materialized view, a metric
view, and a Python model.

This is measured behaviour, not a supported configuration. The task was not
designed around v2, so treat j01 as "works today" and re-run `make jobs` on
each new release. If that risk is unacceptable, j02 is the conservative path. It
is a plain serverless Python task that runs the same binary, about 80 lines in
`scripts/jobs_dbt_v2_runner.py`. It authenticates as the job's `run_as`
identity from the task's ambient credentials, with no stored secret, and it
gives you full control over profile, flags, and retries.

### Serverless jobs compute is not always x86_64

The first Jobs runs pinned one wheel, the `manylinux_2_28_x86_64` build. Both
jobs' first attempts failed with `ERROR_WHEEL_INSTALLATION: … is not a supported
wheel on this platform`. The automatic retry landed on a compatible host and
went on to run dbt. The runner now prints its platform, and it reported
`Linux/aarch64`. Serverless can land a task on either architecture.

The fix is in `databricks.yml`. List **both** wheels, each with a PEP 508
platform marker:

```text
dbt-oss @ https://…/dbt_oss-2.0.11-cp310-abi3-manylinux_2_28_x86_64.whl ; platform_machine == 'x86_64'
dbt-oss @ https://…/dbt_oss-2.0.11-cp310-abi3-manylinux_2_28_aarch64.whl ; platform_machine == 'aarch64'
```

pip installs only the one that matches. With markers, every attempt in j01–j03
installed on the first try. Once a release reaches PyPI, `dbt-oss==<version>`
alone resolves the right wheel. The GitHub URLs are only needed to test a
release the day it ships, because package mirrors lag.

### Two v1 habits to drop

- **`auth_type: databricks_cli` is gone** (r03). In v2 the valid `auth_type`
  values are `oauth`, `token`, `env-oidc`, and `file-oidc`. Anything else falls
  back to token auth and fails on the missing `token` field. Locally, export
  `databricks auth token` into the profile's `token`. In Jobs, the native task
  injects the token, and the runner script fetches it at runtime.
- **No `catalog.json`** (r25). `dbt docs generate` still builds the docs site.
  The catalog now lands as Parquet under `target/info_schema/`. Anything that
  parses v1's `catalog.json` (lineage scrapers, catalog sync jobs) needs a
  change.

### Harmless differences you will see in logs

- A contract `primary_key` with no `name` produces warning `dbt1071`. v2 then
  generates a hashed constraint name, and the constraint lands (r21).
- An ephemeral model reports `skipped` in `run_results.json`, because it never
  runs on its own. The compiled consumer inlines it as `__dbt__cte__<name>`
  (r07). A harness that counts `skipped` as failure will misreport ephemeral
  models.

## What this does not test

- **The full `dbt` distribution wheel.** Only `dbt_oss`, the Apache-2.0 subset,
  was tested. `DBT_V2_DIST=dbt make matrix` reruns the local rows against the full one. The jobs pin `dbt-oss` in `databricks.yml`.
- **Advanced streaming-table and materialized-view options.** Refresh schedules
  and policies and AUTO CDC are known open parity items for v2 on Databricks.
  r16 and r17 cover the default create-and-refresh path only.
- **Azure.** The run was on AWS. The adapter and Jobs surfaces are the same
  across clouds, but that is an expectation, not a measurement.
- **Scale.** The data is a handful of rows per model; this tests correctness,
  not throughput.

## Run it

```bash
cp template.env .env     # set catalog (and optionally warehouse / profile)
make all                 # install dbt v2, set up, local matrix, Jobs rows, README
make teardown            # drop every schema it created, destroy the bundle
```

`make help` lists each step. The local matrix takes about 4 minutes, and the
Jobs rows about 5. You need `CREATE SCHEMA` on the catalog, a serverless SQL
warehouse, and serverless jobs compute.

| Path | What it is |
|---|---|
| `dbt_project/models/rows/` | One model per matrix row, named for its row |
| `landing_files/` | The two CSV drops the streaming table ingests |
| `scripts/run_matrix.py` | Local rows: runs dbt in phases, reads each result back from UC |
| `scripts/run_jobs.py` | Jobs rows: deploys the bundle and runs both jobs at once |
| `scripts/jobs_dbt_v2_runner.py` | The Python-task runner (j02) |
| `databricks.yml`, `resources/` | The bundle: both job definitions |
| `results/matrix_results.json` | Source of truth for the table above |
