# 🌊 Pulsar → UC Managed Tables: Scale, Freshness, Filtering, VARIANT, Clustering

**The question:** the [prior experiment](../pulsar-to-uc-managed-tables/) proved three
paths land Unity Catalog **managed** tables from one Apache Pulsar topic. This one
answers what a production Pulsar shop asks next: does each path hold up with
**realistic ~1KB nested payloads landed as VARIANT**, **pre-table filtering**, real
**event→queryable freshness** across a trigger ladder under continuous production,
**liquid clustering**, 20× the load, and what does each path **cost** at a target
workload of ~250k events/s?

Plan and scope decisions: [`docs/plan.md`](docs/plan.md). Results:
[`results/matrix_results.json`](results/matrix_results.json).

The paths (unchanged from the prior experiment's rig, scaled up):

| Path | Route | Table | GA status |
|---|---|---|---|
| **A** | KoP (Kafka protocol) → Structured Streaming Kafka source | managed Delta | GA on the Databricks side; KoP itself is archived OSS (vendor-hosted Kafka endpoints avoid this) |
| **B** | native `format("pulsar")` connector | managed Delta | Public Preview, **confirmed no GA plan** — fails a GA-only gate |
| **C** | Pulsar consumer → PyIceberg → UC Iceberg REST | managed Iceberg | GA (managed Iceberg + credential vending), no Databricks compute in path |

## Findings

*(populated from `results/matrix_results.json` after the live run — placeholder until
the run completes)*

## Running it

```bash
make install && make auth-check
make tf-apply          # m6i.2xlarge broker VM: Pulsar 3.1.1 + KoP, 4-partition topic
cp template.env dev.env   # fill from `terraform output`
make pulsar-health && make deploy-jobs

make produce-remote                       # 2M x ~1KB backlog, produced ON the VM
make run-scale-a run-scale-b              # drains: plain/filtered/clustered (A), plain (B)
make run-scale-c run-scale-c-filtered     # external writer drains

make produce-remote-timed DURATION_SEC=... EVENT_RATE_PER_SEC=5000 &   # continuous
make run-ladder-a run-ladder-b            # freshness ladders (concurrent with producer)
make run-ladder-c-nrt run-ladder-c-t60

make run-reads verify cost-model
make tf-destroy        # the broker is ephemeral; ALWAYS tear it down
```

## Security posture

Same as the prior experiment: the broker is **unauthenticated plaintext**, an
ephemeral rig for synthetic data only. Choose exposure via `allowed_ingress_cidrs`,
destroy when done. Production needs TLS + token/OAuth on Pulsar and SASL on KoP.

## Structure

House experiment shape: Makefile surface, uv-managed Python, terraform for ephemeral
infra, DAB-defined jobs (profile auth, no PATs), numbered scripts + `verify.py`,
committed results JSON, tiered pytest markers.
