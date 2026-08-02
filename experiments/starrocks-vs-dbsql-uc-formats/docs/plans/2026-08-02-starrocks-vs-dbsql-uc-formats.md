# Plan — StarRocks vs Databricks SQL across Unity Catalog table formats

Date: 2026-08-02. Status: executed same day (live run).

## Question

An organization choosing a serving engine wants to know, with live evidence:

1. Which table formats can **StarRocks** write to when Unity Catalog (UC) governs the
   data — its native format, UC managed Iceberg, UC managed Delta?
2. How does **Databricks serverless SQL** compare on the same operations against
   managed Iceberg and managed Delta?
3. Does the "format doesn't matter" claim hold for **interop** — can each engine
   read (and write) tables the other engine created?
4. What are the measured **latency and cost** tradeoffs per operation class?

A "not supported" cell with a live error or doc citation is a first-class result.
This experiment does NOT look for workarounds — it maps the support surface as it
exists today.

## What research established (pre-registration of expectations)

Pinned versions: **StarRocks 4.0.13** (`starrocks/allin1-ubuntu:4.0.13`, released
2026-07-16, latest patch of the current stable 4.0 branch), Databricks serverless
SQL warehouse (channel current), UC Iceberg REST catalog endpoint
`/api/2.1/unity-catalog/iceberg-rest`.

- StarRocks Iceberg catalogs support `"iceberg.catalog.type" = "rest"` with
  OAuth2 bearer token (`iceberg.catalog.security = "oauth2"`,
  `iceberg.catalog.oauth2.token`) and vended credentials
  (`iceberg.catalog.vended-credentials-enabled`, default true). Writes documented:
  CREATE DATABASE/TABLE (v3.1+), CTAS, INSERT INTO / INSERT OVERWRITE (v3.1+),
  Parquet sink only. UPDATE/DELETE against Iceberg are **not** in the
  feature-support matrix (position/equality *deletes* are read-side features);
  4.0 release notes are ambiguous — settled live below.
- StarRocks Delta Lake catalog supports **Hive metastore and AWS Glue only** — no
  UC metastore type — and lists **no write features** (read-only integration).
  Expectation: StarRocks → UC managed Delta is unsupported on two independent
  grounds (no UC Delta catalog, no Delta writer).
- UC's Iceberg REST catalog serves managed Iceberg tables read/write with
  credential vending, and serves managed **Delta** tables **read-only** as Iceberg
  when Iceberg reads (UniForm-style metadata) are enabled on the table.
- Databricks serverless SQL: full DML on managed Delta and managed Iceberg (both GA).

## Scope decisions (what was added / cut and why)

Yardstick: maximize clarity for a serving-engine + format recommendation for a
latency-sensitive (sub-second target), moderately concurrent serving workload
with a GA-only bar and a preference for native Iceberg over UniForm.

**Added**
- Repeated-run aggregation timings (7 runs, report p50/min) rather than a single
  shot — single-shot numbers are noise for a latency-target decision, and cold vs
  warm behavior is itself a finding for serverless-vs-provisioned engines.
- StarRocks native-format battery as the baseline row (its primary-key table is
  the incumbent serving path; the grid is meaningless without it).
- An explicit UniForm read path (managed Delta exposed through the Iceberg REST
  endpoint) because it is the ONLY path by which StarRocks can see managed Delta
  — the grid must show that plainly rather than leave the Delta column blank.
- Identity + row-count/checksum agreement checks on every interop row (fail-closed:
  a row is only ✅ if both engines agree on count and checksum).

**Cut**
- Streaming sources entirely (prior experiment covered ingestion; this one is
  about the serving/format surface). Bulk = 100k-row batches.
- StarRocks shared-data (S3-backed) mode — one allin1 node answers the support
  and small-scale latency questions; shared-data changes ops, not the support
  grid. Noted as a caveat on absolute StarRocks numbers.
- QPS/concurrency load testing — support + single-stream latency + cost is the
  clarity target here; a load harness would double the rig for numbers that
  depend heavily on sizing choices we are not evaluating.
- Databricks writing to StarRocks native format (no such integration exists in
  either direction; a doc-cited row, not a rig).
- MERGE / upsert batteries — UPDATE/DELETE cells already establish the DML
  boundary; MERGE adds no new support information (StarRocks has no MERGE INTO
  for Iceberg — open upstream issue #73684).

## The grid to fill

Capability battery — per (engine, format): CREATE, INSERT 1 row, UPDATE 1 row,
DELETE 1 row, bulk INSERT 100k, bulk UPDATE (~50k rows), bulk DELETE (~50k rows),
and three aggregations over the 100k table (group-by, filter+aggregate,
high-cardinality distinct), each timed.

| Engine | Format | Path |
|---|---|---|
| StarRocks 4.0.13 | native (primary-key OLAP table) | local |
| StarRocks 4.0.13 | UC managed Iceberg | UC Iceberg REST + vended credentials |
| StarRocks 4.0.13 | UC managed Delta | expected unsupported — record evidence |
| DBSQL serverless | UC managed Delta | native |
| DBSQL serverless | UC managed Iceberg | native |

Interop matrix — creator × format × accessor × read/write, with row-count +
checksum agreement (count, SUM(seq), SUM(CAST(value*100 AS BIGINT)),
COUNT(DISTINCT device_id)):

| Creator | Format | Accessor | Read | Write |
|---|---|---|---|---|
| DBSQL | managed Iceberg | StarRocks | test | test (INSERT via REST) |
| DBSQL | managed Delta | StarRocks | test (Iceberg-reads path only) | test (expect ❌) |
| StarRocks (via REST) | managed Iceberg | DBSQL | test | test |

## Cost model (stated assumptions)

- StarRocks: m5.2xlarge us-east-1 on-demand = $0.384/hr → $/op = 0.384 × elapsed_hr.
  Assumes single-node allin1; no EBS/S3/network charges material at this scale.
- DBSQL serverless: warehouse size read live from the API; AWS serverless SQL
  list price $0.70/DBU-hr × warehouse DBU rating × elapsed. This charges each op
  for the full warehouse-seconds it occupies — the honest serving-cost lens.
  Auto-stop means idle time is not billed but is also not charged to any op.
- Normalized: $ per 100k-row bulk op and $ per aggregation query.

## Rig

- Terraform (AWS profile `fe-sandbox-keys` → SSO-derived keys, us-east-1):
  one m5.2xlarge, Ubuntu 24.04 AMI, docker + `starrocks/allin1-ubuntu:4.0.13`,
  SG ingress on 9030 (MySQL protocol) + 8030/8040 (FE/BE HTTP) restricted to the
  operator CIDRs (sandbox rejects literal 0.0.0.0/0; use the two /1 halves only
  if an open evaluation is intended), optional SSH /32 from day one for logs.
- Databricks: profile `DEFAULT`, serverless warehouse, catalog + fresh schema
  `starrocks_uc_eval`; `EXTERNAL USE SCHEMA` granted to the operator principal
  for REST-catalog access. OAuth token minted per run (`databricks auth token`)
  and passed to StarRocks catalog properties; run completes inside token TTL.
- All timings are client-side wall-clock from the same operator machine, so both
  engines carry comparable network overhead; recorded in results JSON.

## Fail-closed rules

- `verify.py` proves both identities live (DBSQL `SELECT current_user()`,
  StarRocks `SELECT current_user()` + version) before any battery row is written.
- Reachability ≠ authorization: HTTP/SQL errors are recorded verbatim as
  findings, never retried into silence.
- Interop rows are only ✅ when count AND checksum agree across engines.
- Every ❌ cell records the exact error text or the doc citation that closes it.
