# Modular Decision Grid — Streaming Ingestion & Serving (Template)

> **The frame:** with an open table format on object storage in the middle, ingestion compute and query compute are decoupled. You are not choosing one monolithic product — you are making **four independent module decisions**, each on its own criteria. A weak answer in one module doesn't force you to abandon a strong answer in another.

```
┌────────────┐   ┌───────────────────┐   ┌──────────────────────────┐   ┌────────────────┐
│ 1. Bus     │ → │ 2. Ingestion      │ → │ 3. Storage & table       │ → │ 4. Serving     │
│    egress  │   │    compute        │   │    maintenance           │   │    engine      │
└────────────┘   └───────────────────┘   └──────────────────────────┘   └────────────────┘
                                          open format = the contract
                                          between modules 2 and 4
```

Monolithic real-time OLAP stacks (bus → proprietary storage → coupled query engine) collapse modules 2–4 into one decision. Score them in the [scorecard](evaluation_scorecard.md) as a single column, but use this grid to make the comparison explicit: what do you give up in each module by coupling them?

---

## Module 1 — Bus egress

**Decision:** how events leave `{{message_bus}}` and reach ingestion compute.

| Candidate | Notes |
|---|---|
| Native consumer API from ingestion engine | e.g., Spark/Flink source connector for the bus protocol |
| Kafka-protocol compatibility layer | if the bus speaks Kafka protocol, Kafka-ecosystem connectors apply |
| Bus-native tiered/offload to object storage | bus writes directly to storage; ingestion reads files |
| Connector framework sink | Kafka Connect–style sink into storage or a staging topic |

**Measure:**
- Sustained egress throughput at `{{events_per_sec_peak}}` without consumer lag growth
- Delivery semantics (exactly-once vs at-least-once) and dedup burden pushed downstream
- Replay: can you re-read `{{replay_window}}` of history without impacting live consumers?
- Egress cost per unit of data at `{{events_per_sec_steady}}`

## Module 2 — Ingestion compute

**Decision:** what turns the event stream into committed open-format table writes.

| Candidate | Notes |
|---|---|
| Spark Structured Streaming (managed or self-run) | includes managed declarative-pipeline offerings built on it |
| Apache Flink | strongest for low-latency / complex event-time processing |
| Connector-framework direct sink | e.g., open-format sink connectors; minimal transform capability |
| Managed ingestion service | vendor-managed bus→table ingestion |

**Measure:**
- Freshness contribution: micro-batch/commit interval → event-to-commit latency at p95
- Sustained throughput at `{{events_per_sec_peak}}` with `{{event_size_avg}}` events
- Upsert/CDC support if `{{write_pattern}}` requires it (merge performance under load)
- Small-file behavior at high commit frequency (interacts directly with Module 3)
- Cost at steady state; autoscaling behavior between `{{events_per_sec_steady}}` and peak
- **GA-only check:** the passing configuration uses no preview/beta features

## Module 3 — Storage & table maintenance

**Decision:** table format, catalog, and who keeps fast-arriving tables healthy.

| Candidate | Notes |
|---|---|
| Delta Lake on `{{object_store}}` | with automatic optimization/clustering where GA |
| Apache Iceberg on `{{object_store}}` | with a maintenance service or scheduled jobs |
| Format-interop layer | write once, read as either format, where GA |

**Measure:**
- Compaction keeping pace: file count and average file size after `{{test_duration}}` at peak ingest
- Query latency drift: row-2 scorecard latencies at hour 0 vs hour N without manual intervention
- Retention enforcement: hot window `{{hot_window}}`, total `{{total_retention}}`, vacuum/expiry behavior
- External-read contract: second engine (`{{external_reader}}`) reads live tables via the catalog (scorecard row 5)
- Storage + maintenance compute cost per month at steady state

## Module 4 — Serving engine

**Decision:** what answers queries at `{{query_latency_slo}}` / `{{query_latency_percentile}}` under `{{qps_peak}}`.

| Candidate | Notes |
|---|---|
| Lakehouse SQL engine (e.g., Databricks SQL, Trino/Presto-family) | queries the open tables in place; no second copy |
| Real-time OLAP engine reading open formats externally (e.g., StarRocks, ClickHouse, Druid, Pinot) | engine stays, storage stays open — check external-read maturity |
| Real-time OLAP engine with internal ingestion | second copy of the hot window; scores as a coupled 2–4 stack |
| Serving-optimized cache/store over the lakehouse | for point-lookup/API classes only |

**Measure:**
- Query latency p50/p95/p99 per query class, **under live ingest**, at `{{test_concurrency}}` (scorecard row 2)
- Concurrency scaling: latency at 1× vs `{{qps_peak}}` load
- Nested/semi-structured support for `{{schema_shape}}` (scorecard row 4)
- Freshness at the serving layer: does the engine see new rows immediately, or add its own sync delay?
- Cost per month at `{{qps_steady}}`, including any second-copy storage

---

## Decision record

| Module | Chosen option | Runner-up | Deciding criterion | Fast-follows (preview/beta, off critical path) |
|---|---|---|---|---|
| 1. Bus egress | | | | |
| 2. Ingestion compute | | | | |
| 3. Storage & maintenance | | | | |
| 4. Serving engine | | | | |
