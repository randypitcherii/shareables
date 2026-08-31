# Plan — Pulsar → UC managed tables: scale, freshness, filtering, VARIANT, clustering

Date: 2026-08-02. Extends `experiments/pulsar-to-uc-managed-tables/` (the "prior rig"),
which proved three viable paths land UC managed tables from one Pulsar topic with a
100k × 512-byte flat-JSON bounded drain. This experiment closes the prior README's
open coverage rows that matter most for a production decision: realistic ~1KB nested
payloads landed as VARIANT, pre-table filtering, measured event→queryable freshness
under continuous production across a trigger ladder, liquid clustering impact, larger
load, and a cost model extrapolated to the target workload (~250k events/s at ~1KB).

The yardstick for every scoping decision below: **what maximizes clarity for a final
recommendations write-up mapping a production Pulsar shop's pains and priorities to
solutions with tradeoffs.** Directional honesty beats fake benchmarks.

## What the research sharpened (generic wording; repo is public)

- The target workload is ~250k append-only events/s at ~1KB with complex, nested,
  heterogeneous JSON the team wants as VARIANT; a separate upsert-heavy stream is
  parked for a later phase.
- **GA + compliance is one universal gate**, not a per-path score. The native Pulsar
  connector (path B) has been confirmed to have *no GA plan*, which effectively
  disqualifies it for the critical path; it is still measured here because it is the
  cleanest possible Spark-source baseline and calibrates what path A gives up.
- Formal freshness target is minutes-level (≤5–10 min p95, event→queryable); a looser
  "hourly is fine" consumer also exists. So the trigger ladder must cover
  near-real-time through hourly, with real freshness measured under **continuous
  production** — the prior rig documented that a bounded backlog drain measures
  backlog age, not freshness.
- The full firehose is far larger than what should land: **in-stream filtering before
  the table** is a hard requirement. The shop's own sketch is a broker-side function
  that filters/projects the raw topic; the Databricks-side equivalent is a predicate
  before the sink.
- No producer changes; the incumbent is a live Flink pipeline, so the bar for every
  path is "substantially cheaper, simpler, or more reliable," which makes the cost
  model a first-class deliverable.
- Iceberg VARIANT has real version floors: Iceberg v3 + a recent Iceberg library
  (apache/iceberg#14655 fixed) on the write side, DBR 18+ class readers; PyIceberg
  cannot write VARIANT today. Delta VARIANT is GA (DBR 15.3+). Also relevant, from
  vendor/field threads: managed Iceberg has no GA streaming/CDF read (batch
  high-water-mark instead), external writers must handle vended-credential refresh
  past ~1h, and VARIANT columns can't be clustered/partitioned or used for data
  skipping — promoted typed columns carry the layout.

## Dimensions and how each is executed

### 1. Realistic payloads → VARIANT
New generator: ~1KB average (natural size jitter, no random padding), heterogeneous
per event type — arrays of objects (e.g. items/attachments), optional keys, mixed
types (string/int/bool/null/nested), a skewed multi-tenant `project_id`, and a noise
mix (~65% of volume is `debug_log` / `heartbeat` / `internal_metric` types that should
never land). Paths A/B parse the raw bytes with `parse_json()` → a real VARIANT column
plus promoted typed columns (`event_id`, `seq`, `event_ts`, `event_type`,
`project_id`) for filtering/clustering. Path C lands the payload as a JSON string and
documents the Iceberg-v3/PyIceberg VARIANT floor (conversion to VARIANT belongs in
silver).

### 2. Pre-table filtering
A `FILTER_MODE` flag on every path: `off` (land everything) vs `on` (keep only the
six business event types, ~35% of volume). A/B: predicate on the promoted
`event_type` before the sink, applied pre-shuffle in the same micro-batch. C:
consumer-side predicate before rows enter the Arrow batch. Measured: input vs landed
rows, volume reduction, drain-time change. Broker-side Pulsar Functions and
vendor-managed filtering are **documented, not run** (see cuts).

### 3. Freshness ladder under continuous production
Producer runs **on the broker VM** (SSH + docker) at a steady controlled rate for the
whole window while consumers run concurrently — freshness is event→queryable, not
backlog age. Per-batch metrics side-table: every micro-batch tags rows with its batch
id; after the batch's write completes the job records `(batch_id, commit_ts, rows)`;
exact per-event freshness = `commit_ts − event_ts` computed in SQL afterwards →
p50/p95 per path per trigger.

Live-measured cells: A × {near-real-time, 1 min, 5 min}, B × {near-real-time, 1 min},
C × {near-real-time (per-batch commit), 60 s commit interval}. The 15-min and hourly
cells are **modeled, clearly labeled**: for a T-interval trigger, an event waits
U(0,T) plus processing of a T-sized backlog, so p95 ≈ 0.95·T + P(T) where P(T) comes
from the measured drain rate; the model is validated against the measured 1-min and
5-min cells before being trusted for 15 min/hourly. Rationale: a live hourly cell
costs >2 h of wall-clock and broker spend to confirm arithmetic that the 1/5-min
cells already validate.

To keep cluster-spinup overhead from eating the budget, each Databricks path runs its
whole trigger ladder inside **one job run** (one cluster): the driver script cycles
trigger configs sequentially, each with its own table + fresh checkpoint, while the
producer runs continuously. Clock skew between the VM (chrony/NTP) and Databricks is
tens of ms — negligible at seconds-scale freshness; noted in results.

### 4. Liquid clustering
On the largest landed dataset (path A's scale drain), land the same backlog twice:
into a table created with `CLUSTER BY AUTO` seeded on obvious columns
(`project_id`, `event_type`, day of `event_ts`) vs an unclustered twin. Measured:
write-side drain-time delta, then read-side medians (5 iterations each) on three
representative queries via the SQL warehouse — tenant+time-range aggregation,
high-cardinality distinct, VARIANT field extraction. `OPTIMIZE FULL` run explicitly
and timed, with the maintenance story for write-only streaming tables documented
(automatic optimization is usage-driven and may never trigger on a write-only bronze
table). Clustering is exercised on path A only — path B shares the identical Delta
sink (same machinery, no new information), and path C's PyIceberg writer cannot set
clustering (post-hoc `ALTER TABLE ... CLUSTER BY` via SQL is noted for managed
Iceberg).

### 5. Cost and performance model
`scripts/05_cost_model.py` computes, from measured throughput: cores needed at 250k
events/s per path, DBU/hr and $/hr for jobs compute (classic, published AWS premium
rates), EC2 for the external writer, S3 request+storage for path C's commit cadence,
and the trigger-interval tradeoff (always-on vs scheduled batch). Every extrapolated
number is labeled `extrapolated`; every measured number cites its run id. Rates used
are written into the results JSON so the arithmetic is reproducible.

### 6. Scale-up
Broker: `m6i.2xlarge` (8 vCPU / 32 GB) with a 200 GB / 250 MB/s gp3 root volume
(vs t3.large + 50 GB before), 4-partition topic (`defaultNumPartitions=4`), SSH
required (producer runs on-VM, so laptop WAN bandwidth is out of the loop). Scale
drain target: **2M events × ~1KB (~2 GB)**, produced at the max sustainable on-VM
rate. Ceiling rationale: one broker VM is the honest evaluation-infra limit — beyond
this, numbers stop being about the ingest paths and start being about broker
clustering, which is not the question. 2M × 1KB is 20× the prior row count and ~80×
the prior bytes, enough to expose per-path throughput separation and give the cost
model a real slope. Databricks side stays single-node (m5d.xlarge) so per-core
arithmetic stays clean; the cost model, not a bigger test cluster, carries the
extrapolation to 250k events/s.

## Explicitly added / cut

**Added** (vs the minimum required scope):
- Per-batch commit-time metrics side-table → *exact* per-event freshness in SQL,
  instead of approximating from `ingest_ts`.
- Single-job-run trigger ladder (one cluster per path) to keep live wall-clock and
  spend proportionate.
- Noise-mix generator design tied to the filtering dimension (the same events feed
  both dimensions; no second corpus).
- Rates and inputs embedded in the cost-model output for reproducibility.

**Cut or bounded, and why:**
- 15-min and hourly freshness cells: modeled, not run live (validated model;
  >2 h wall-clock to confirm arithmetic). Labeled in every artifact.
- Broker-side Pulsar Functions filtering: documented only. The rig runs standalone
  with `--no-functions-worker`; enabling the functions worker adds rig risk and the
  measured Databricks-side answer (predicate before sink) is unchanged. GA/ops
  implications of broker-side filtering are written up in the README.
- Path D (Kafka Connect Iceberg sink) and vendor-managed broker-side Iceberg write:
  documented candidates, not run (no Connect worker / vendor account in the rig).
- Clustering on paths B/C: B is byte-identical Delta sink machinery; C can't set it
  at create time from PyIceberg. Documented instead.
- Serving-side query benchmarks (the UC1–UC29-style suite), external-engine reads,
  and the upsert/profile stream: separate evaluations, per the prior split.
- Databricks-side multi-node scaling runs: the cost model extrapolates from clean
  single-node per-core numbers instead of burning budget on mid-size clusters that
  answer neither the per-core nor the 250k question.

## Run plan (live phase)

| Run | What | Output |
|---|---|---|
| R1 | Produce 2M × ~1KB on-VM; bounded drain per path (A, B availableNow; C batch loop), filter off | throughput at scale per path |
| R2 | Filter drain: re-drain the same 2M backlog with `FILTER_MODE=on` for A and C | volume reduction, drain-time delta |
| R3 | Freshness ladder: producer 5k events/s continuous; A ladder {NRT, 1m, 5m}, B ladder {NRT, 1m}, C {NRT, 60s} | p50/p95 freshness per cell |
| R4 | Clustering: drain 2M backlog into clustered vs plain twin (path A sink); 3 read queries × 5 iters each; timed `OPTIMIZE FULL` | write delta, read medians, maintenance cost |
| R5 | Cost model from R1–R4 measurements | $/hr per path at eval scale and extrapolated 250k events/s |

Teardown (`terraform destroy` + AWS describe-instances verification by project tag)
is mandatory before delivery.

## Environment

- Databricks: profile `DEFAULT`, warehouse `f6dd72df81d69f03`, catalog
  `fe_randy_pitcher_workspace_catalog`, **new schema `pulsar_uc_scale_eval`**.
- AWS: sandbox account via SSO profile, us-east-1; the sandbox rejects a literal
  `0.0.0.0/0` ingress CIDR — use the two /1 halves.
- Reused broker fixes from the prior rig: `advertisedListeners` (never
  `advertisedAddress`), `brokerEntryMetadataInterceptors` before first produce,
  `narExtractionDirectory` off /tmp with uid-10000 ownership, KoP pinned to
  Pulsar 3.1.1 / KoP 3.1.1.1.
- Sharp edges honored: checkpoints reset per run, `max_concurrent_runs: 1`,
  PyIceberg refresh-and-retry on `CommitFailedException`.
