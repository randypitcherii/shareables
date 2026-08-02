# Pre-table filtering: where the predicate can live, and what each spot costs

The workload shape that motivates this: the raw firehose topic is far larger than
what should land in the lakehouse (~65% noise in this experiment's generator, by
design). Four places can drop the noise; the experiment measures the first two live
and documents the rest.

## 1. Consumer-side predicate, Databricks paths (A/B) — measured live

A `WHERE event_type IN (...)` on the promoted column before the sink, applied inside
the same micro-batch that parses the payload. Zero extra moving parts, GA, and the
filter is versioned with the pipeline code. The cost profile is the honest caveat:
the full firehose still crosses the network to the ingest cluster, is parsed, and is
*then* dropped — you pay egress + parse compute for bytes that never land. Storage
and downstream costs drop by the full reduction. See `filter_reduction_path_a` and
the `a_scale_filtered` vs `a_scale_plain` drain times in the results JSON.

## 2. Consumer-side predicate, external writer (path C) — measured live

Same predicate applied in the Python consumer before rows enter the Arrow batch.
Identical cost shape (full firehose consumed, reduced landing), no Databricks
compute involved.

## 3. Broker-side Pulsar Functions — documented only

A lightweight function deployed on the Pulsar cluster reads the raw topic and
republishes only matching/projected events to a curated topic; every downstream
consumer then drains the *reduced* stream. This is the genuine shift-left: network
and ingest compute shrink by the reduction factor, and multiple downstream consumers
share one filtered stream.

Why it is documented rather than run here: this rig runs Pulsar standalone with
`--no-functions-worker` (the functions worker is another JVM with its own state
store, a meaningful reliability surface on a single-node eval broker), and the
Databricks-side answer — predicate before sink — is unchanged either way.

Ops/GA implications to weigh for production:
- Pulsar Functions is a GA feature of Apache Pulsar and of hosted Pulsar vendors,
  but it is **owned by the broker team, not the data team** — filter changes ride
  the messaging estate's deploy process, not the lakehouse repo.
- It doubles the stored volume for the kept subset (raw topic + curated topic)
  unless raw retention is shortened.
- Failure modes move upstream: a wedged function stalls the curated topic while the
  raw topic keeps growing; monitoring must cover function backlog, not just
  consumer lag.
- Hosted-vendor function runtimes bill per-execution/CPU; at firehose rates this is
  a real line item to price against the consumer-side parse cost it saves.

## 4. Vendor-managed broker-side lakehouse write — documented only

Hosted Pulsar vendors offer a broker-side feature that writes topic data directly to
Iceberg via a REST catalog endpoint (the same UC Iceberg REST + credential-vending
machinery path C exercises externally). Combined with a broker-side function (3),
this reaches "filtered stream lands in UC with no consumer infrastructure at all."
Path C's measurements are the closest live stand-in for its commit/conflict
behavior: the coexistence findings (UC automatic maintenance commits mid-stream,
refresh-and-retry required) apply to any external REST-catalog writer, vendor or
DIY. Version floors to check with the vendor: VARIANT writing needs Iceberg v3 and
an Iceberg library carrying the apache/iceberg#14655 fix; readers of v3 VARIANT need
a DBR-18-class engine. Until then: land JSON strings, convert to VARIANT in silver
(the pattern this experiment demonstrates for path C).

## Rule of thumb

Filter placement is a cost/ownership tradeoff, not a capability gap: every path can
filter. Consumer-side is simplest and keeps the predicate with the pipeline; go
broker-side when (a) egress/ingest compute on the noise fraction is the dominating
cost, or (b) several downstream consumers want the same curated stream.
