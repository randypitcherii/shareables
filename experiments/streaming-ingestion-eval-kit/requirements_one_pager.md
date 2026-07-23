# Requirements One-Pager — Streaming Ingestion & Serving (Template)

> Fill in every `{{placeholder}}` with a real number from the workload owner, or mark it `TBD` with an owner and a date to resolve it. A requirement without a number and a percentile is an opinion, not a requirement.

## Workload profile

| Requirement | Value | Notes |
|---|---|---|
| Message bus / source | `{{message_bus}}` | e.g., Kafka, Pulsar, Kinesis, Event Hubs |
| Event size (avg / p95) | `{{event_size_avg}}` / `{{event_size_p95}}` | bytes, post-serialization |
| Event rate, steady state | `{{events_per_sec_steady}}` events/sec | |
| Event rate, peak | `{{events_per_sec_peak}}` events/sec | include burst duration |
| Write pattern | `{{write_pattern}}` | append-only vs upsert/CDC — changes the architecture materially |
| Number of topics / streams | `{{topic_count}}` | and partition counts if known |
| Schema shape | `{{schema_shape}}` | flat, nested/semi-structured, evolving? |

## Freshness

| Requirement | Value | Notes |
|---|---|---|
| Freshness target (event → queryable) | `{{freshness_slo}}` at `{{freshness_percentile}}` | e.g., "≤ N minutes at p95" — target + percentile, always together |
| Freshness measurement point | `{{freshness_measurement}}` | where the clock starts (bus publish time?) and stops (row visible to a query?) |

## Query serving

| Requirement | Value | Notes |
|---|---|---|
| Query latency SLO | `{{query_latency_slo}}` at `{{query_latency_percentile}}` | e.g., "≤ N ms at p95" for the dashboard/API query class |
| Query classes | `{{query_classes}}` | point lookups, aggregations, joins, full scans — SLO per class if they differ |
| QPS, steady state | `{{qps_steady}}` | |
| QPS, peak | `{{qps_peak}}` | |
| Concurrency | `{{concurrent_users}}` | users / API clients at peak |

## Retention

| Requirement | Value | Notes |
|---|---|---|
| Hot / queryable window | `{{hot_window}}` | the window the latency SLO applies to |
| Total retention | `{{total_retention}}` | full history, any tier is acceptable |
| Cold-tier query expectation | `{{cold_query_expectation}}` | is slower-but-cheaper acceptable outside the hot window? |

## Cost

| Requirement | Value | Notes |
|---|---|---|
| Annual cost ceiling (all-in) | `{{annual_cost_ceiling}}` | compute + storage + bus egress + licenses + ops headcount if counted |
| Cost accounting boundary | `{{cost_boundary}}` | what's in and out of the ceiling |

## Non-functional requirements

- **Open storage**: data lands in an open table format (`{{table_format}}`, e.g., Delta Lake or Apache Iceberg) on object storage (`{{object_store}}`), externally readable by engines other than the primary one.
- **GA-only**: the recommended path uses only generally-available features. Preview/beta capabilities may appear as fast-follows, never on the critical path.
- **Replay / backfill**: historical replay from the bus (or from storage) must coexist with live ingestion without violating the freshness SLO. Expected replay window: `{{replay_window}}`.
- **Ops burden**: target operating model is `{{ops_model}}` (e.g., fully managed, managed with escape hatches, self-hosted). Headcount available: `{{ops_headcount}}`.

## Parked / out of scope (this round)

| Item | Why parked | Revisit |
|---|---|---|
| `{{parked_item_1}}` | `{{parked_reason_1}}` | `{{parked_revisit_1}}` |

## Open questions / TBD

| Question | Owner | Needed by |
|---|---|---|
| `{{open_question_1}}` | `{{owner_1}}` | `{{date_1}}` |
