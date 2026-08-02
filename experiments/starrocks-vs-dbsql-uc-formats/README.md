# StarRocks vs Databricks SQL across Unity Catalog table formats

A live evaluation of which table formats each engine can **write** under Unity
Catalog (UC) governance, what each operation costs in latency and dollars, and
whether the two engines can actually read and write each other's tables.

The goal is a clarity grid, not a workaround hunt. **A "not supported" cell with
a verbatim engine error or a doc citation is a first-class result.**

Versions pinned for this run: **StarRocks 4.0.13** (`starrocks/allin1-ubuntu`,
single-node allin1 on an m5.2xlarge) and **StarRocks 4.1.3** (re-probed, same
outcomes); **Databricks serverless SQL** `dbsql_version 2026.20`, X-Small
warehouse; UC Iceberg REST catalog at `/api/2.1/unity-catalog/iceberg-rest`.

---

## Findings

### 1. StarRocks cannot do row-level DML on Iceberg at all — this is the headline

StarRocks 4.0.13 and 4.1.3 both reject `UPDATE` and `DELETE` against any Iceberg
catalog table, before any storage or permission check is reached:

```
UPDATE ... -> (1064) table events_sr_iceberg does not support update
DELETE ... -> (1064) Getting analyzing error. Detail message:
              Table of iceberg catalog doesn't support [DELETE].
```

This is an engine capability boundary, not an environment artifact: it is a
planner-stage rejection, identical on both versions, and it matches the
published feature matrix, which lists only CREATE DATABASE/TABLE, CTAS, and
INSERT INTO/OVERWRITE as Iceberg write features. Position and equality deletes
appear in that matrix as *read* capabilities (reading delete files another engine
wrote), which is easy to mistake for delete support.

**Consequence:** any workload needing corrections, GDPR-style deletes, or upserts
on the served table cannot use StarRocks-on-Iceberg as the writer. StarRocks'
own native primary-key table does all of it (see the grid) — the DML capability
belongs to the native format, not to StarRocks-on-the-lake.

### 2. StarRocks cannot address UC managed Delta at all

Delta write support is absent from the StarRocks feature matrix, and more
fundamentally there is no way to point a StarRocks Delta catalog at UC. Both
metastore types were attempted live:

```
"hive.metastore.type" = "unity" -> hive metastore type [unity] is not supported.
"hive.metastore.type" = "rest"  -> hive metastore type [rest] is not supported.
```

StarRocks' Delta Lake catalog accepts only Hive Metastore and AWS Glue. The only
way StarRocks can see a UC managed Delta table is through the Iceberg REST
endpoint via UniForm-style Iceberg metadata — that is, as an Iceberg table, and
read-only.

### 3. StarRocks CREATE against UC works, and produces a real managed Iceberg table

`CREATE TABLE` through the Iceberg REST catalog with OAuth2 + vended credentials
succeeded (3.0–3.5 s). The result is a genuine UC managed table that Databricks
then treats as first-class: the warehouse bulk-inserted 100k rows into it,
aggregated it, and ran `UPDATE`/`DELETE` on it, all successfully.

`DESCRIBE DETAIL` reports `format = iceberg` for it, the same as a
`CREATE TABLE ... USING ICEBERG` table authored by Databricks. Note a reporting
nuance: the UC Tables API reports `data_source_format = DELTA` for *every* table
in the schema, including the Databricks-authored Iceberg one — so that API field
cannot be used to tell managed Iceberg from managed Delta. Use `DESCRIBE DETAIL`.

### 4. StarRocks INSERT into UC managed storage was blocked by storage policy

`INSERT` (single-row and 100k) failed at the object-store layer, not the catalog
layer:

```
S3: Fail to create multipart upload for object <table_root>/<uuid>_0_0_0.parquet:
User: arn:aws:sts::<acct>:assumed-role/<uc-vended-role> is not authorized to
perform: s3:PutObject on resource "<table_root>/<uuid>_0_0_0.parquet"
because no service control policy allows the s3:PutObject action
```

Two distinct contributors, separated by direct probing:

- **Write path shape.** StarRocks writes data files directly under the table
  root (`<root>/<uuid>_0_0_0.parquet`). By default it routes writes through a
  spill writer first, which targets `<root>/load_spill/`. Disabling the BE
  setting `enable_connector_sink_spill` moved the failure from the `load_spill/`
  prefix to the parquet object itself — same denial, different key. StarRocks
  reads `write.data.path` from table properties (confirmed in FE source), and UC
  does publish that property, so the path itself is not obviously wrong.
- **Credential scope.** Credentials vended by the UC *API*
  (`temporary-table-credentials`, `READ_WRITE`) allowed `PutObject`, directory
  markers, and multipart create against the same prefixes when exercised from an
  allowlisted host. PyIceberg, using the same Iceberg REST path, appended to the
  StarRocks-created table successfully. So the table and prefix are writable in
  principle; the denial is specific to the session the StarRocks sink used.

**This cell is therefore recorded as "blocked in this environment", not as a
clean engine verdict.** See the caveat below — it is entangled with the workspace
IP ACL, which we could not modify.

### 5. Interop reads from StarRocks were blocked by IP-bound access, not by format

StarRocks reading Databricks-created tables (both managed Iceberg and managed
Delta via UniForm) failed with S3 403 during scan planning, while reading Iceberg
manifest files:

```
org.apache.iceberg.aws.s3.BaseS3File.getObjectMetadata -> S3Exception:
Forbidden (Status Code: 403)
```

The catalog layer worked throughout — StarRocks resolved namespaces and table
metadata over the REST endpoint. What failed was the data-plane fetch. The
decisive contrast: **PyIceberg, using the same UC Iceberg REST endpoint and the
same vended-credential mechanism, read both tables successfully from an
allowlisted host.** The workspace enforces an IP access list, and the StarRocks
node's address was not on it; the REST calls only reached UC because they were
relayed from an allowlisted address, which leaves the node's own S3 requests
unallowlisted.

**Read this as an environment finding with a real lesson**: connecting an
external engine to UC is not only a catalog-credential exercise. The engine's own
egress must be admitted by the workspace network policy, and relaying only the
catalog traffic is not sufficient, because the data-plane credentials are bound
to the context that requested them.

### 6. On latency and cost, the two engines are not in the same class

For 100k-row tables, StarRocks answers aggregations in **130–200 ms p50** and the
serverless warehouse in **490–600 ms p50**, roughly a 3–4× gap, with the
warehouse's first query in a series 1.6–1.9× its own steady-state (serverless
warm-up is visible in every battery). Per-query cost runs
**~$0.000014 (StarRocks) vs ~$0.0006 (DBSQL)** under the stated model — but that
comparison is an artifact of accounting, not efficiency: StarRocks is charged
only for the wall-clock slice of an always-on m5.2xlarge, while the warehouse is
charged full warehouse-seconds per statement. A continuously-running StarRocks
node costs $0.384/hr whether queried or not; the warehouse costs nothing idle.
The crossover depends entirely on duty cycle, which this experiment did not
measure.

Where the engines genuinely differ is **latency floor**: sub-200 ms single-stream
aggregations on 100k rows are routine for StarRocks native and were never
observed from the serverless warehouse.

---

## Support grid — engine × format × operation

Timings are client wall-clock p50 (aggregations, 7 runs) or single-run elapsed
(DDL/DML). Cost per the model in `template.env`.

| Operation | StarRocks 4.0.13 native (PK table) | StarRocks 4.0.13 → UC managed Iceberg | StarRocks → UC managed Delta | DBSQL serverless → UC managed Delta | DBSQL serverless → UC managed Iceberg |
|---|---|---|---|---|---|
| CREATE | ✅ 285 ms | ✅ 3 500 ms | ❌ no UC metastore type | ✅ 2 845 ms | ✅ 3 410 ms |
| INSERT 1 row | ✅ 159 ms | ⚠️ blocked (S3 PutObject denied) | ❌ | ✅ 1 855 ms | ✅ 2 013 ms |
| UPDATE 1 row | ✅ 311 ms | ❌ **"does not support update"** | ❌ | ✅ 4 989 ms | ✅ 2 805 ms |
| DELETE 1 row | ✅ 167 ms | ❌ **"doesn't support [DELETE]"** | ❌ | ✅ 1 796 ms | ✅ 2 247 ms |
| Bulk INSERT 100k | ✅ 1 340 ms | ⚠️ blocked (same denial) | ❌ | ✅ 4 317 ms | ✅ 3 429 ms |
| Bulk UPDATE (~50k) | ✅ 683 ms | ❌ unsupported | ❌ | ✅ 2 532 ms | ✅ 2 687 ms |
| Bulk DELETE (~50k) | ✅ 273 ms | ❌ unsupported | ❌ | ✅ 1 636 ms | ✅ 2 744 ms |
| Agg: group-by | ✅ 202 ms | ✅ 131 ms | ❌ | ✅ 601 ms | ✅ 530 ms |
| Agg: filter + aggregate | ✅ 140 ms | ✅ 133 ms | ❌ | ✅ 547 ms | ✅ 510 ms |
| Agg: high-card distinct | ✅ 175 ms | ✅ 133 ms | ❌ | ✅ 548 ms | ✅ 534 ms |

✅ supported and measured · ❌ rejected by the engine (verbatim error in
`results/matrix_results.json`) · ⚠️ blocked by this environment, not a clean
engine verdict

StarRocks-on-Iceberg aggregation timings are over an empty table (the INSERT was
blocked), so they measure planning and catalog overhead, not scan throughput —
they are **not** comparable to the other aggregation columns. All other numbers
are over 100k rows.

### Cost per operation class

| Measure | StarRocks (m5.2xlarge, $0.384/hr) | DBSQL serverless (X-Small, 6 DBU/hr × $0.70) |
|---|---|---|
| $ per 100k-row bulk insert | $0.000143 (native) | $0.005036 (Delta) · $0.004000 (Iceberg) |
| $ per aggregation query (p50) | $0.000015–0.000022 | $0.000595–0.000701 |
| Idle cost | $0.384/hr continuous | $0 (auto-stop) |

Assumptions: us-east-1 on-demand EC2, AWS serverless SQL list price, each
statement charged the full warehouse-seconds it occupies, single-node StarRocks
with no EBS/S3/network charges attributed. Duty cycle is not modeled and
dominates any real comparison.

## Interop matrix — creator × format × accessor

| Creator | Format | Accessor | Read | Write |
|---|---|---|---|---|
| DBSQL | UC managed Iceberg | StarRocks | ⚠️ blocked (S3 403, IP-bound creds) | not reached |
| DBSQL | UC managed Delta (UniForm) | StarRocks | ⚠️ blocked (S3 403, IP-bound creds) | ❌ no Delta write path exists |
| DBSQL | UC managed Iceberg | PyIceberg (allowlisted host) | ✅ verified | ✅ append verified |
| StarRocks (CREATE via IRC) | UC managed Iceberg | DBSQL | ✅ 100k rows, checksum agrees | ✅ bulk insert 8 770 ms, UPDATE 3 521 ms, DELETE 2 476 ms |
| StarRocks (CREATE via IRC) | UC managed Iceberg | StarRocks | ✅ metadata + aggregations | ⚠️ INSERT blocked; UPDATE/DELETE ❌ unsupported |

The one clean cross-engine result: **a table StarRocks created through UC's
Iceberg REST catalog is fully usable by Databricks SQL** — bulk load, aggregate,
row-level update and delete all succeeded on it, and `DESCRIBE DETAIL` confirms
it is genuine Iceberg. Catalog-level interop works. What does not work is
StarRocks' side of the DML contract.

## Caveats

- **The workspace enforced an IP access list** that did not include the StarRocks
  node, and we did not have authorization to modify it. Catalog traffic was
  relayed from an allowlisted host so the REST layer could be exercised; the
  node's own S3 requests remained unallowlisted, which is the direct cause of
  every ⚠️ cell. **Rerunning with the engine's egress IP allowlisted is required
  before treating StarRocks→UC INSERT or StarRocks reads of UC tables as
  unsupported.** The ❌ cells (UPDATE/DELETE, Delta catalog) are unaffected —
  they are planner-level rejections that never touch storage.
- StarRocks ran as a single-node `allin1` container (FE+BE, one m5.2xlarge,
  shared-nothing). A production shared-data deployment would change absolute
  throughput and cost, though not the support surface.
- Aggregation timings are single-stream from one operator machine. No
  concurrency or QPS testing was performed, so nothing here speaks to behavior
  under sustained load.
- The 100k-row scale exercises the support surface and the latency floor. It is
  far too small to say anything about scan performance at production volumes.
- Costs are list-price models, not billed amounts.

## Layout

```
docs/plans/     pre-registered plan: scope decisions, expectations, fail-closed rules
scripts/        verify.py, 00_setup_uc.py, battery/ (5 rows), interop/ (3 scripts)
results/        matrix_results.json — every timing and verbatim error
terraform/      single-node StarRocks on EC2; optional customer-managed-storage rig
tests/          unit tests for the cost model, generators, and fail-closed recording
```

Run `make help` for the command surface. `make verify` proves every auth surface
with a real call before any battery runs; `make teardown-check` proves no
evaluation instances remain.
