# 🌊 Pulsar → UC Managed Tables: Scale, Freshness, Filtering, VARIANT, Clustering

**The question:** the [prior experiment](../pulsar-to-uc-managed-tables/) proved three
paths land Unity Catalog **managed** tables from one Apache Pulsar topic. This one
answers what a production Pulsar shop asks next: does each path hold up with
**realistic ~1KB nested payloads landed as VARIANT**, **pre-table filtering**, real
**event→queryable freshness** across a trigger ladder under continuous production,
**liquid clustering**, 20× the load, and what does each path **cost** at a target
workload of ~250k events/s?

Plan and scope decisions: [`docs/plan.md`](docs/plan.md). All numbers below are from
[`results/matrix_results.json`](results/matrix_results.json), live run 2026-08-02:
one Pulsar 3.1.1 + KoP broker on `m6i.2xlarge` (4-partition topic), single-node
`m5d.xlarge` Databricks job clusters (DBR 16.4), events generated **on the broker
VM** (~1KB avg nested heterogeneous JSON, ~65% noise event types by design).

The paths (unchanged from the prior experiment's rig, scaled up):

| Path | Route | Table | GA status |
|---|---|---|---|
| **A** | KoP (Kafka protocol) → Structured Streaming Kafka source | managed Delta | GA on the Databricks side; KoP itself is archived OSS (vendor-hosted Kafka endpoints avoid this) |
| **B** | native `format("pulsar")` connector | managed Delta | Public Preview, **confirmed no GA plan** — fails a GA-only gate |
| **C** | Pulsar consumer → PyIceberg → UC Iceberg REST | managed Iceberg | GA (managed Iceberg + credential vending), no Databricks compute in path |

## Findings matrix

| # | Question | Result | Evidence |
|---|---|---|---|
| 1 | ~1KB nested heterogeneous payloads land as **VARIANT** | ✅ paths A/B (real `VARIANT` column verified on all 9 Delta cells); ◑ path C lands JSON string (PyIceberg cannot write Iceberg v3 VARIANT yet — silver-hop conversion; see [variant-floors](docs/research/variant-floors.md)) | `verification.*.event_is_variant` |
| 2 | Throughput at 2M × ~1KB, single node | ✅ A: **22.2k rows/s** · B: **23.1k rows/s** · C: **2.7k rows/s** (single Python process over WAN) | `spark_cells.*_scale_plain`, `cell_c_scale_plain` |
| 3 | Pre-table filtering, volume reduction | ✅ **65.0%** reduction on A and C (699,231 of 2M land, byte-identical counts); filtered drain 13% faster than plain on A, 38% on C | `filter_reduction_path_a`, `cell_c_scale_filtered` |
| 4 | Freshness, near-real-time trigger | ✅ A: **p50 7.7s / p95 13.6s** @5k ev/s · B: **p50 12.0s / p95 17.5s** @~9.6k ev/s · C: **p50 13.1s / p95 18.8s** @~1.8k ev/s (sustainable) — C @5k ev/s falls behind: p95 150s and growing | `freshness_measured`, `cell_c_nrt*` |
| 5 | Freshness, 1-min trigger | ✅ A: **p95 71.8s** · B: **p95 76.1s** · C (60s commits): lag-bound p95 137s at over-capacity input | `freshness_measured` |
| 6 | Freshness, 5-min trigger | ✅ A: **p95 337s** | `freshness_measured.a_t300` |
| 7 | Freshness, 15-min / hourly | ◑ **modeled** (labeled): p95 ≈ **17.6 min / 70.5 min**; model validated against measured 1-min (70.5 vs 71.8) and 5-min (352.6 vs 337.2) cells | `freshness_modeled` |
| 8 | Liquid clustering, write-side cost | ✅ clustered drain 105s vs plain 90s (**+16%**) | `spark_cells.a_scale_*` |
| 9 | Liquid clustering, read-side benefit | ❓ not measurable at this scale — 2M rows land in 4–8 files (~470MB); all queries ~2.5s, warehouse-latency-dominated. Needs a 100×-file table to show data skipping | `clustering_reads` |
| 10 | Maintenance on write-only streaming tables | ✅ `OPTIMIZE FULL` explicit and cheap here (3.9–9.5s); automatic optimization is usage-driven, so write-only bronze needs an explicit plan | `clustering_reads.*.optimize_full_sec` |
| 11 | No loss / no duplication at 2M scale | ✅ 2,000,000 distinct `event_id`s, full seq range, on A, B, and C | `verification` |
| 12 | External writer credential lifecycle | ✅ hit live: vended S3 credential failed mid-run (`ACCESS_DENIED` after 82 clean commits); recovery = new OAuth token + reload table. Retry logic now required client behavior, alongside `CommitFailedException` refresh-retry and transient S3 timeout backoff | `03_path_c_writer.py`, run logs |
| 13 | Preview pulsar connector reliability | ◑ ladder run 1 died with `Failed to commit cursor` (partition-2) mid-stream; identical retry succeeded. Intermittent — exactly the class of risk a no-GA-timeline preview carries | `job_run_ladder-b` (run 911928214642507) |
| 14 | Cost at ~250k ev/s (extrapolated, labeled) | ◑ A: **~$4.06/hr** (12 × m5d.xlarge jobs compute) · B: ~$3.72/hr · C: ~$4.05/hr (23 × c6i.xlarge + S3 PUTs, no DBU) — see caveats in `cost_model.notes` | `cost_model` |
| 15 | Batching vs freshness tradeoff (path C) | ✅ 60s commits lifted C throughput 2.7k→4.4k rows/s (commit amortization) at the cost of minute-scale freshness | `cell_c_t60` |

## Key findings

**Freshness is trigger-arithmetic plus processing, and now that's demonstrated.**
Measured p95 tracked `0.95×T + processing` within 5% at 1-min and 5-min triggers, so
the 15-min and hourly cells are modeled, not run — and the near-real-time cells show
the floor: **~8–14s p95 end-to-end** (produce → broker → parse → VARIANT → managed
Delta commit) on GA components with no tuning. A minutes-level freshness SLA
(≤5–10 min p95) is comfortably met by paths A and B at any trigger ≤5 min, and by
path C only when the writer has throughput headroom (see next).

**The external writer's ceiling is the story for path C.** A single Python
consumer/writer sustains ~2.7k events/s (5s commits) or ~4.4k (60s commits); driven
at 5k ev/s it falls behind and freshness degrades without bound. Its freshness is
healthy (p95 ~19s) only below capacity. Scaling C is horizontal (partition the
topic, run N writers), and every writer must implement three retry behaviors we hit
live: commit-conflict refresh, **vended-credential reauth**, and transient S3
timeout backoff. This generalizes to any REST-catalog external writer, including
vendor broker-side lakehouse writers.

**Filtering works everywhere; placement is cost/ownership, not capability.** The
same 65% reduction landed identically on Databricks-side and external-writer
filtering. Consumer-side filtering still pays to move and parse the noise;
broker-side options (Pulsar Functions, vendor curated topics) shift that left —
tradeoffs in [docs/research/filtering-and-broker-side-options.md](docs/research/filtering-and-broker-side-options.md).

**VARIANT lands today only on the Delta paths.** All nine Delta cells carry a real
`VARIANT` column parsed in-stream; the Iceberg path needs a silver hop until the
Iceberg v3 VARIANT floor clears (writer library + DBR-18-class readers). Promoted
typed columns (`project_id`, `event_type`, `event_ts`) carry filtering and
clustering, since VARIANT columns cannot.

**Clustering: pay-now benefits-later.** +16% write cost at ingest; no measurable
read effect at 2M rows because the table is only 4–8 files. The recommendation
stands (cluster on the promoted read-path columns, `CLUSTER BY AUTO` to keep
learning), but the read-side payoff needs production-scale file counts to
demonstrate — directional honesty over a fake benchmark.

**Preview-connector risk became concrete.** Path B is the fastest and simplest
source and it failed one of two identical runs with an intermittent cursor-commit
error. For a GA-only estate this is corroboration, not just policy: the GA gate and
the observed flakiness point the same direction (path A on the Databricks side, or a
vendor-supported Kafka endpoint on the broker side).

**Cost parity at target scale is close enough that ops model decides.** All three
paths extrapolate to roughly $3.7–4.1/hr of ingest compute at 250k ev/s (before the
silver hop for C, and excluding the broker estate). The real differentiators are the
things measured above: freshness ceiling, retry burden, GA/compliance posture, and
who owns the moving parts — not the compute bill.

### Caveats (read before quoting numbers)

- Single-broker, single-node rig: throughputs are per-node slopes for the cost
  model, not capacity benchmarks. Extrapolations assume linear fan-out and no skew.
- Path B's ladder ran while two producer windows overlapped (~9.6k ev/s input vs
  A's 5k) — it kept up; freshness cells remain comparable, input rates differ.
- Path A's 5-min cell lost its producer partway through the final batch (producer
  window expired), so its last-batch input rate dipped; p95 matches the model.
- Path C ran over WAN from a laptop; its absolute throughput is a floor. Co-locate
  writers with the broker in production.
- DBU/instance rates in `cost_model.rates` are 2026-08 public list prices; the
  m5d.xlarge DBU rate is the published instance-table value — re-verify before
  external use.

## Running it

```bash
make install && make auth-check
make tf-apply          # m6i.2xlarge broker VM: Pulsar 3.1.1 + KoP, 4-partition topic
cp template.env dev.env   # fill from `terraform output`
make pulsar-health && make deploy-jobs

make produce-remote                       # 2M x ~1KB backlog, produced ON the VM
make run-scale-a run-scale-b              # drains: plain/filtered/clustered (A), plain (B)
make run-scale-c run-scale-c-filtered     # external writer drains

make produce-remote-timed DURATION_SEC=3300 EVENT_RATE_PER_SEC=5000 &   # continuous
make run-ladder-a run-ladder-b            # freshness ladders (concurrent with producer)
make run-ladder-c-nrt run-ladder-c-t60

make run-reads verify cost-model
make tf-destroy        # the broker is ephemeral; ALWAYS tear it down
```

Path C prerequisites (one-time, workspace admin): metastore external data access
enabled and `GRANT EXTERNAL USE SCHEMA ON SCHEMA <catalog>.<schema> TO <principal>`.

## Security posture

Same as the prior experiment: the broker is **unauthenticated plaintext**, an
ephemeral rig for synthetic data only. Choose exposure via `allowed_ingress_cidrs`,
destroy when done. Production needs TLS + token/OAuth on Pulsar and SASL on KoP.

## Structure

House experiment shape: Makefile surface, uv-managed Python, terraform for ephemeral
infra, DAB-defined jobs (profile auth, no PATs), numbered scripts + `verify.py`,
committed results JSON, tiered pytest markers.
