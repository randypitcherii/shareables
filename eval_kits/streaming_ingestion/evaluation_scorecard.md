# Evaluation Scorecard — Streaming Ingestion & Serving (Template)

> One column per candidate stack. Every row is a **measured** result against the same dataset, the same event rate, and the same query set — no vendor benchmark numbers, no extrapolation. Targets come from the [requirements one-pager](requirements_one_pager.md).

## Test conditions (record once, apply to every column)

| Condition | Value |
|---|---|
| Dataset / generator | `{{test_dataset}}` (event size `{{event_size_avg}}`, schema `{{schema_shape}}`) |
| Sustained ingest rate during tests | `{{test_events_per_sec}}` events/sec for `{{test_duration}}` |
| Query set | `{{query_set}}` (per query class in the one-pager) |
| Query concurrency during latency runs | `{{test_concurrency}}` |
| Measurement window | `{{measurement_window}}` (exclude warm-up) |

## Scorecard

| # | Criterion | Target | How measured | Candidate A: `{{option_a}}` | Candidate B: `{{option_b}}` | Candidate C: `{{option_c}}` |
|---|---|---|---|---|---|---|
| 1 | Freshness p95 (event → queryable) | `{{freshness_slo}}` at p95 | Timestamp at bus publish vs first query that returns the row; sample continuously under sustained load | | | |
| 2 | Query latency p50 / p95 / p99 | `{{query_latency_slo}}` at `{{query_latency_percentile}}` | Run `{{query_set}}` at `{{test_concurrency}}` concurrency **while ingest is running**; report all three percentiles | | | |
| 3 | Sustained ingest throughput | ≥ `{{events_per_sec_peak}}` events/sec | Hold peak rate for `{{test_duration}}`; confirm no consumer lag growth and no freshness degradation | | | |
| 4 | Semi-structured / nested-data query support | `{{nested_data_requirement}}` | Run the nested-field query subset; note syntax gaps, flattening requirements, and performance vs flat equivalents | | | |
| 5 | Open-format external read verification | Second engine reads the live tables | Query the same tables from an independent engine (`{{external_reader}}`); confirm row counts and recent data match | | | |
| 6 | Replay / backfill coexistence | Freshness SLO holds during replay | Replay `{{replay_window}}` of history while live ingest continues; measure row 1 during the replay | | | |
| 7 | Ops burden | `{{ops_model}}` | Count: components to run, upgrade owner, failure modes exercised (kill a node/worker), pages generated during the test week | | | |
| 8 | All-in monthly cost | ≤ `{{annual_cost_ceiling}}` / 12 | Meter actual test spend, scale linearly to steady-state rate; itemize compute, storage, bus egress, licenses | | | |

## Scoring guidance

- **Record raw numbers first**, verdicts second. A cell should read like "measured X (target `{{freshness_slo}}`) ✅", not just "✅".
- Rows 1–3 and 6 are **under-load measurements** — a candidate that only hits its numbers with ingest paused fails the row.
- Row 5 is pass/fail: if the "open" tables can't be read by a second engine in practice, the stack is not open.
- Row 7 is qualitative but evidence-based — list the incidents and interventions from the test period.
- Disqualify any candidate whose passing configuration depends on preview/beta features (GA-only rule); note the feature as a fast-follow instead.

## Result summary

| Candidate | Rows passed | Rows failed | Disqualifiers | Recommendation |
|---|---|---|---|---|
| `{{option_a}}` | | | | |
| `{{option_b}}` | | | | |
| `{{option_c}}` | | | | |
