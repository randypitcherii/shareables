# Managed Delta vs managed Iceberg under Unity Catalog: what each format actually unlocks

**The question.** You've decided on UC managed tables with Liquid clustering. Should they be **Delta** or **Iceberg**? The usual reasons to stay on Delta are change data feed, Structured Streaming, and existing `df.write…saveAsTable` code. The usual reason to pick Iceberg is that outside engines like DuckDB can write to it. Both have shifted recently, so this experiment measures where the line sits today.

**Answer (2026-11-02).**
- **Databricks features no longer rule Iceberg out.** Automatic CDF, streaming reads and streaming writes all work on managed Iceberg v3. The code changes involved are small and listed below.
- **Iceberg is the only format outside engines can write.** Managed Delta is read-only from DuckDB and PyIceberg.
- **The real catch is cross-engine deletion vectors.** Once *Databricks* runs `UPDATE`/`DELETE`/`MERGE` on an Iceberg v3 table, DuckDB and PyIceberg refuse to read it under their default settings ([research note](docs/research/2026-11-02-iceberg-v3-deletion-vector-files.md)).

Setup: one real AWS workspace, an isolated UC catalog in a customer-owned S3 prefix, synthetic 3-row tables, all clustered with `CLUSTER BY (cat)`.
- **Delta column:** managed Delta with catalog commits, Iceberg v3 reads (UniForm) and row tracking.
- **Iceberg column:** managed Iceberg `format-version = 3`.

Clients:
- serverless SQL warehouse (DBSQL 2026.42)
- a single-node classic job cluster on DBR 19 (Spark 4.2.0)
- DuckDB 2.0.0 (`iceberg` + `unity_catalog` extensions)
- PyIceberg 0.12.0

Every cell traces to a numbered script and an entry in [`results/matrix_results.json`](results/matrix_results.json).

## Findings matrix

| # | Capability | Managed Delta | Managed Iceberg v3 | Evidence |
|---|---|:---:|:---:|---|
| 01 | DuckDB read + `INSERT`/`UPDATE`/`DELETE`/`MERGE` | ◑ read-only | ◑ | `01_duckdb_dml.py` |
| 02 | Automatic CDF: batch, `table_changes()`, streaming `readChangeFeed`, DBSQL | ✅ | ✅ includes DuckDB-written commits | `02_change_data_feed.py` |
| 03 | Structured Streaming sink + plain streaming source (incremental) | ✅ | ◑ sink needs `.format("iceberg")` | `03_structured_streaming.py` |
| 04 | DataFrame writers into an existing table | ✅ | ◑ `saveAsTable` w/o `format("iceberg")` fails | `04_dataframe_writers.py` |
| 05 | PyIceberg read/append/delete/overwrite/upsert via Iceberg REST catalog | ❌ | ◑ v2 only, partial | `05_pyiceberg_writes.py` |
| 06 | Mitigation: Liquid Iceberg v3 with deletion vectors **off** | n/a | ◑ external reads restored; DuckDB DML lost | `06_iceberg_dv_off_mitigation.py` |

Legend:
- ✅ every operation tested worked
- ◑ some operations worked (the details below say which)
- ❌ none worked
- ❓ inconclusive (no cell this run)

## Row details

### 01: DuckDB 2.0 DML

- **Iceberg:**
  - **Clean table:** on a table Databricks has only appended to, DuckDB's `INSERT`, `UPDATE`, `DELETE` and `MERGE INTO` all commit through the UC Iceberg REST catalog. Databricks SQL reads back exactly the expected rows. Liquid clustering doesn't get in the way.
  - **After Databricks DML:** once Databricks has run `UPDATE`/`DELETE` on the table, DuckDB's default reader fails with `Puffin file … has invalid trailing magic`.
  - **With the opt-in setting:** after `SET iceberg_unsafe_skip_puffin_verification = true`, the same DML succeeds and Databricks reads the correct result.
- **Delta:**
  - **Reads work two ways:**
    - the `unity_catalog` extension attached `READ_ONLY`. The secret needs an explicit `AWS_REGION`; without it DuckDB builds an `s3.null` hostname.
    - the `iceberg` extension, but only with the Puffin skip setting, because the Delta table's Iceberg metadata lists the same deletion-vector files.
  - **Writes are refused on both paths:**
    - **`unity_catalog` read-write attach:** the UC Delta API rejects READ_WRITE credentials with `The UC Delta API requires clients to identify the calling application in the User-Agent header … is insufficient`. That also breaks plain `SELECT` on a read-write attach. Setting DuckDB's `custom_user_agent` doesn't help. External managed-Delta writes are in Public Preview and appear to be limited to recognized connectors.
    - **Iceberg REST path:** it vends read-only credentials for Delta, so writes fail with S3 `PutObject` denied.

### 02: Automatic change data feed

Both formats pass every CDF read path on DBR 19 and on the serverless warehouse. On Iceberg v3, the commits DuckDB made in row 01 show up in the feed with the correct update pre-images and post-images. In `DESCRIBE HISTORY`, Iceberg REST commits appear as `Kernel-…/Iceberg REST Catalog`.

### 03: Structured Streaming

- **Sink, default writer:** writing to a managed Iceberg sink without a format fails with `[MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED] Managed Iceberg tables do not support Delta streaming writes.`
- **Sink, `.format("iceberg")`:** works and makes incremental progress (3 → 5 rows across two `availableNow` runs).
- **Plain source:** `spark.readStream.table(<iceberg>)` works incrementally, with no CDF needed.
- **Iceberg → Iceberg:** works with `.format("iceberg")`.

### 04: DataFrame writers

| Writer → existing table | Delta | Iceberg |
|---|:---:|:---:|
| `df.write.mode("append").saveAsTable(t)` (no format) | ✅ | ❌ `Managed Iceberg tables do not support Delta writes.` |
| `df.write.format("delta")…saveAsTable(t)` | ✅ | ❌ same error |
| `df.write.format("iceberg")…saveAsTable(t)` | ✅ ¹ | ✅ |
| `df.write.insertInto(t)` | ✅ | ✅ |
| `df.writeTo(t).append()` | ✅ | ✅ |
| `DeltaTable.forName(spark, t).merge(…)` | ✅ | ✅ |

¹ On an existing Delta table, `format("iceberg")` is effectively ignored: the rows land and the table stays Delta.

Creating tables: `format("iceberg").saveAsTable` and `writeTo().using("iceberg").create()` both create managed Iceberg. A plain `saveAsTable` creates Delta.

### 05: PyIceberg 0.12

- **Iceberg v3:** reads fail on the Databricks deletion-vector files (`Incorrect magic bytes, expected b'PFA1'`). Writes fail inside PyIceberg itself, because 0.12 can't write v3 manifests (`Cannot write manifest for table version: 3`).
- **Iceberg v2 control:** read, `append` and `delete` work. UC rejects `overwrite` and `upsert` with `Adding multiple snapshots in a single update is not supported`, because PyIceberg commits a delete snapshot and an append snapshot together.
- **Delta with Iceberg reads:** reads fail on the same deletion-vector files, and writes fail because the vended credentials are read-only.

### 06: Mitigation, deletion vectors off

- **What it fixes:** `'iceberg.enableDeletionVectors' = 'false'` on a Liquid v3 table makes Databricks `UPDATE`/`DELETE` rewrite files instead. Strict DuckDB and PyIceberg reads then work, and automatic CDF still works.
- **What it costs:** DuckDB `UPDATE` on that table fails at commit with **HTTP 500** from the Iceberg REST catalog, because DuckDB only writes merge-on-read deletes.
- **Row tracking stays on:** v3 can't turn it off (`IcebergCompatV3 requires … 'delta.enableRowTracking' … 'true'`).

### Setup-time finding

Liquid clustering on a managed Iceberg **v2** table is rejected unless deletion vectors and row tracking are both off (`MANAGED_ICEBERG_ATTEMPTED_TO_ENABLE_CLUSTERING_WITHOUT_DISABLING_DVS_OR_ROW_TRACKING`). The error says this reduces concurrency control.

## What this means for the format choice

- **Pick Iceberg v3 if outside engines need to write.** Delta isn't writable from DuckDB or PyIceberg today. Plan for the deletion-vector problem by doing one of these:
  - keep Databricks-side changes on shared tables append-only
  - have DuckDB readers set `iceberg_unsafe_skip_puffin_verification`
  - turn deletion vectors off and give up DuckDB DML (row 06)
- **CDF and streaming no longer favor Delta**, provided you're on DBR 19+ or DBSQL and your streaming writes use `.format("iceberg")`.
- **Expect to touch every `saveAsTable` that lacks `format("iceberg")`.** `insertInto`, `writeTo().append()` and `DeltaTable.merge` work unchanged.
- **Pick Delta (with Iceberg reads) if outside engines only read,** and you want Delta-only features this experiment didn't test (generated columns, CHECK constraints, shallow clone). Outside readers still hit the deletion-vector problem on tables Databricks has updated.

## Not tested

- Iceberg Java / Spark-with-Iceberg-runtime, Trino and Snowflake as external clients.
- BigQuery federation.
- Private-preview features.
- Performance.
- Whether the deletion-vector framing behavior is specific to this workspace's release.

## Run it

Copy `.env.example` to `.env`. You need an OAuth profile, a serverless SQL warehouse, metastore external data access turned on, a writable external-location path (not Default Storage), and permission to run DBR 19 job clusters.

```bash
make sync
make verify
make matrix     # setup -> 01 -> spark job (~15 min) -> 02 -> 03 -> 04 -> 05 -> 06
make check
make cleanup
```

See [`scripts/README.md`](scripts/README.md) for what each script does.

Safety:
- OAuth only; PATs are refused.
- Hosts, identities, buckets, account ids, table ids and request ids are scrubbed before evidence is written.
- `results/.spark_raw.json` (unscrubbed) is gitignored.
