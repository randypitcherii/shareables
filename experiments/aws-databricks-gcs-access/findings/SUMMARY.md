# Reading GCS Data from AWS Databricks — Summary

## TL;DR

- **It works**, but only outside Unity Catalog governance. Classic/job compute supports full Spark-native GCS reads; serverless is limited to the Python SDK (small datasets only).
- **UC external locations cannot point at GCS from AWS.** All five creative workarounds failed — this is an architectural limitation, not a config issue.
- **Governed cross-cloud access is possible** via Delta Sharing or Lakehouse Federation (BigQuery), but these paths were not yet tested.

---

## Results at a Glance

| Approach | Serverless | Classic / Job |
|----------|:----------:|:-------------:|
| HMAC keys via `s3a://` | :x: | :white_check_mark: |
| Python SDK (`google-cloud-storage`) | :white_check_mark: | :white_check_mark: |
| GCS Connector (`gs://`) | :x: | :white_check_mark: |
| Mixed GCS+S3: bucket-specific `s3a` | :x: | :white_check_mark: |
| Mixed GCS+S3: Python SDK + Spark SQL | :white_check_mark: | :white_check_mark: |
| Mixed GCS+S3: `gs://` + `s3a://` | :x: | :white_check_mark: |
| UC external location (all 5 variants) | :x: | :x: |

**Why do serverless approaches fail?** Serverless blocks all `spark.hadoop.*` config overrides. The Python SDK sidesteps this by using plain HTTP — no Hadoop involved.

---

## What Should I Recommend to My Customer?

**Do they need governed access (UC lineage, audit, access control)?**
- Recommend **Delta Sharing**: a GCP Databricks workspace shares tables to the AWS workspace. GA, cross-cloud, full UC governance.
- Or recommend **Lakehouse Federation via BigQuery**: create a BigQuery foreign catalog on AWS backed by GCS external tables. No GCP Databricks workspace needed.
- *Neither has been tested in this experiment yet — see "Future Investigation" below.*

**Do they need large-scale reads (10GB+)?**
- Use **classic or job compute** with `s3a://` (HMAC) or `gs://` (GCS connector) configured at the cluster level.
- Reads are fully distributed across Spark executors — scales with cluster size.
- For mixed GCS + S3 workloads, use **bucket-specific `s3a` config** to isolate GCS creds from S3.

**Do they need quick ad-hoc reads on serverless?**
- Use the **Python SDK** (`google-cloud-storage`). Practical limit is ~1 GB before driver memory becomes a risk.
- No predicate pushdown or partition pruning — it downloads everything.

**Do they need mixed GCS + S3 access?**
- Classic/job: bucket-specific `s3a` config (cleanest isolation).
- Serverless: Python SDK for GCS, normal Spark SQL for S3.

---

## What Definitely Does Not Work

| Approach | Why It Fails |
|----------|-------------|
| Any `spark.hadoop.*` config on serverless | Blocked with `CONFIG_NOT_AVAILABLE` — no exceptions |
| UC external locations pointing at GCS from AWS | UC credential types and URL routing are hardcoded per cloud provider |
| `r2://` redirected to GCS endpoint | UC validates R2 keys server-side and rejects GCS HMAC keys |
| `gs://` URL in UC on AWS | Accepted at creation time, but queries fail — UC uses AWS IAM, not GCS creds |
| Runtime `spark.conf.set()` for Hadoop configs | Does not propagate to Hadoop filesystem layer; must be cluster-level `spark_conf` |

---

## Future Investigation

These are untested but likely viable paths to **governed** cross-cloud access:

1. **Delta Sharing (GCP provider to AWS recipient)** — GA, supported, full UC governance on the recipient side. Requires a GCP Databricks workspace.
2. **Lakehouse Federation via BigQuery** — Create a BigQuery foreign catalog on AWS pointing at GCS-backed external tables. GA, no GCP workspace needed, UC-governed.
3. **Databricks-to-Databricks workspace federation** — AWS workspace creates a foreign catalog mirroring a GCP workspace's UC catalog. More networking complexity than Delta Sharing.
4. **Performance benchmarks** — The Python SDK path (recommended for serverless) has no benchmarks yet. Testing at 100 MB / 1 GB / 10 GB would quantify its practical ceiling.
5. **Write-path testing** — This experiment only tested reads. Bidirectional access needs separate validation.

---

*Full details: see `2026-03-10-experiment-results.md`, `2026-03-10-distributed-reads-assessment.md`, and `2026-03-10-adversarial-review.md` in this directory.*
