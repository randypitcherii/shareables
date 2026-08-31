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

**The StarRocks→UC column was re-run** on a second workspace that enforces no IP
access list, using the same StarRocks pin (4.0.13-b55eab7) and the same
m5.2xlarge allin1 rig. That rerun replaced two ⚠️ environment-blocked cells with
real engine verdicts — see finding 4 and finding 5. Its warehouse was a 2X-Small
(the first run's was X-Small), so the rerun is authoritative for every
StarRocks-side number and is **not** used for any DBSQL timing.

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

### 4. StarRocks INSERT into UC managed Iceberg works — the earlier denial was the network policy

On the first workspace, `INSERT` (single-row and 100k) failed at the object-store
layer with an `s3:PutObject` denial, and we recorded it as blocked rather than
unsupported. **The rerun on a workspace with no IP access list settles it: the
INSERT is supported.**

```
INSERT ... VALUES (1 row)  -> ok,   4 317 ms
INSERT ... SELECT (100k)   -> ok,   9 615 ms
```

The written data is correct and immediately first-class to Databricks: the
warehouse and StarRocks agree exactly on `COUNT(*)`, `SUM(seq)`, a scaled value
sum, and `COUNT(DISTINCT device_id)` — `[100001, 5100050001, 4999954242, 5001]`
from both engines over the same table.

Before running anything else on the new workspace we probed the data plane from
the StarRocks node itself, using credentials vended by the UC Iceberg REST
`loadTable` call: `HEAD` of the current `metadata.json`, `ListObjectsV2` on the
table prefix, `PutObject` under the table root, and `CreateMultipartUpload` (the
exact call the StarRocks sink makes) **all succeeded**. That is the surface that
previously returned 403, so the difference is the workspace network policy, not
StarRocks and not the write-path shape.

**Consequence:** StarRocks can be a writer into UC managed Iceberg for
append-shaped workloads. The DML boundary in finding 1 is unchanged and is what
actually constrains the design.

### 5. StarRocks reads of Databricks-created tables work, in both formats

With the node's own egress admitted, StarRocks reads Databricks-SQL-created
tables through the UC Iceberg REST catalog with exact checksum agreement:

| Table as created by Databricks SQL | StarRocks read | Checksum vs warehouse |
|---|---|---|
| UC managed **Iceberg** | ✅ p50 1 265 ms | exact agreement |
| UC managed **Delta** + UniForm Iceberg metadata | ✅ p50 567 ms | exact agreement |

`DESCRIBE DETAIL` confirms the second table really is `format = delta` — so the
read genuinely traversed UniForm's Iceberg metadata over a managed Delta table
rather than silently hitting an Iceberg table.

The UniForm bridge remains the **only** path, and it remains read-only:
re-probed on this workspace, StarRocks' Delta Lake catalog still rejects both
candidate metastore types (`unity`, `rest` — finding 2), so there is no direct
StarRocks↔UC Delta connector to write through.

**The lesson that survives from the first attempt:** connecting an external
engine to UC is not only a catalog-credential exercise. The engine's own egress
must be admitted by the workspace network policy. Relaying only the catalog
traffic is not sufficient, because the vended data-plane credentials are bound to
the context that requested them — which is exactly why the first run saw a
working catalog layer and a 403 data layer.

### 6. On latency and cost, the two engines are not in the same class

For 100k-row tables, StarRocks **native** answers aggregations in **140–200 ms
p50** and the serverless warehouse in **490–600 ms p50**, roughly a 3–4× gap,
with the
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

**The rerun sharpens this, and it is the single most decision-relevant number in
the experiment.** That latency floor belongs to StarRocks' *native* format, not
to StarRocks. Reading the same 100k rows as UC managed Iceberg, StarRocks
answered in **580–677 ms p50** — 3–4× its own native timings, and squarely in the
same band as the serverless warehouse (490–600 ms). Whatever advantage StarRocks
has here is bought by loading into its own storage, not by pointing it at the
lake.

---

## Support grid — engine × format × operation

Timings are client wall-clock p50 (aggregations, 7 runs) or single-run elapsed
(DDL/DML). Cost per the model in `template.env`.

| Operation | StarRocks 4.0.13 native (PK table) | StarRocks 4.0.13 → UC managed Iceberg | StarRocks → UC managed Delta | DBSQL serverless → UC managed Delta | DBSQL serverless → UC managed Iceberg |
|---|---|---|---|---|---|
| CREATE | ✅ 285 ms | ✅ 2 389 ms | ❌ no UC metastore type | ✅ 2 845 ms | ✅ 3 410 ms |
| INSERT 1 row | ✅ 159 ms | ✅ 4 317 ms | ❌ | ✅ 1 855 ms | ✅ 2 013 ms |
| UPDATE 1 row | ✅ 311 ms | ❌ **"does not support update"** | ❌ | ✅ 4 989 ms | ✅ 2 805 ms |
| DELETE 1 row | ✅ 167 ms | ❌ **"doesn't support [DELETE]"** | ❌ | ✅ 1 796 ms | ✅ 2 247 ms |
| Bulk INSERT 100k | ✅ 1 340 ms | ✅ 9 615 ms | ❌ | ✅ 4 317 ms | ✅ 3 429 ms |
| Bulk UPDATE (~50k) | ✅ 683 ms | ❌ unsupported | ❌ | ✅ 2 532 ms | ✅ 2 687 ms |
| Bulk DELETE (~50k) | ✅ 273 ms | ❌ unsupported | ❌ | ✅ 1 636 ms | ✅ 2 744 ms |
| Agg: group-by | ✅ 202 ms | ✅ 580 ms | ❌ | ✅ 601 ms | ✅ 530 ms |
| Agg: filter + aggregate | ✅ 140 ms | ✅ 677 ms | ❌ | ✅ 547 ms | ✅ 510 ms |
| Agg: high-card distinct | ✅ 175 ms | ✅ 641 ms | ❌ | ✅ 548 ms | ✅ 534 ms |

✅ supported and measured · ❌ rejected by the engine (verbatim error in
`results/matrix_results.json`)

The whole StarRocks → UC managed Iceberg column is from the ACL-free rerun
(`*__rerun_no_ip_acl` rows in the results JSON), so it is internally consistent
and every cell is over the same 100k-row table. The first run's blocked column is
preserved unchanged at the original row keys. Note that these aggregations are
now over real data — in the first run they ran against an empty table and read
131–133 ms, which measured planning overhead only. **StarRocks reading its own
lake table is ~3–4× slower than StarRocks reading its native format** (580–677 ms
vs 140–202 ms) and lands in the same range as the serverless warehouse.

### Cost per operation class

| Measure | StarRocks (m5.2xlarge, $0.384/hr) | DBSQL serverless (X-Small, 6 DBU/hr × $0.70) |
|---|---|---|
| $ per 100k-row bulk insert | $0.000143 (native) · $0.001026 (UC Iceberg) | $0.005036 (Delta) · $0.004000 (Iceberg) |
| $ per aggregation query (p50) | $0.000015–0.000022 (native) · $0.000062–0.000072 (UC Iceberg) | $0.000595–0.000701 |
| Idle cost | $0.384/hr continuous | $0 (auto-stop) |

Assumptions: us-east-1 on-demand EC2, AWS serverless SQL list price, each
statement charged the full warehouse-seconds it occupies, single-node StarRocks
with no EBS/S3/network charges attributed. Duty cycle is not modeled and
dominates any real comparison.

## Interop matrix — creator × format × accessor

| Creator | Format | Accessor | Read | Write |
|---|---|---|---|---|
| DBSQL | UC managed Iceberg | StarRocks | ✅ p50 1 265 ms, checksum agrees | ❌ UPDATE/DELETE unsupported (INSERT works) |
| DBSQL | UC managed Delta (UniForm) | StarRocks | ✅ p50 567 ms, checksum agrees | ❌ no Delta write path exists |
| DBSQL | UC managed Iceberg | PyIceberg | ✅ verified | ✅ append verified |
| StarRocks (CREATE + INSERT via IRC) | UC managed Iceberg | DBSQL | ✅ 100 001 rows, checksum agrees | ✅ bulk insert 8 770 ms, UPDATE 3 521 ms, DELETE 2 476 ms |
| StarRocks (CREATE + INSERT via IRC) | UC managed Iceberg | StarRocks | ✅ metadata + aggregations | ✅ INSERT; UPDATE/DELETE ❌ unsupported |

Interop is now clean in **both** directions, and the remaining boundary is a
single one. A table StarRocks creates and fills through UC's Iceberg REST catalog
is fully usable by Databricks SQL — bulk load, aggregate, row-level update and
delete all succeed on it, and `DESCRIBE DETAIL` confirms it is genuine Iceberg.
Going the other way, StarRocks reads Databricks-authored managed Iceberg *and*
managed Delta (through UniForm) with exact checksum agreement. **The only thing
that does not work is StarRocks' side of the row-level DML contract** — appends
in, no corrections.

## Caveats

- **Two workspaces, and the network policy was the difference.** The first run's
  workspace enforced an IP access list that did not include the StarRocks node
  and that we could not modify; catalog traffic was relayed from an allowlisted
  host, so the REST layer worked while the node's own S3 requests were denied.
  Everything that was ⚠️ has since been re-run on a second workspace with **no IP
  access list configured**, same StarRocks pin and same instance type, and all of
  it passed. Before re-running the cells we proved the data plane directly from
  the engine node — vended-credential `HEAD`, `ListObjectsV2`, `PutObject` and
  `CreateMultipartUpload` all returned success where the first workspace returned
  403. The ❌ cells (UPDATE/DELETE, Delta catalog) were never in question: they
  are planner-level rejections that never touch storage, and they reproduce
  identically on both workspaces.
- **The rerun's warehouse was a 2X-Small**, the first run's an X-Small, so no
  DBSQL timing in this README comes from the rerun. The rerun's warehouse was
  used only to author interop tables and to verify checksums.
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
