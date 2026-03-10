# Adversarial Review: AWS Databricks GCS Access Experiment

**Date:** 2026-03-10
**Reviewer:** Adversarial analysis
**Status:** Review of experiment dated 2026-03-10

---

## Executive Summary

The experiment tested 5 approaches (with sub-variants) for reading GCS data from AWS Databricks. While thorough in what it covered, the experiment has significant gaps: at least 6 viable approaches were never tested, several conclusions rest on incomplete evidence, and the recommendations ignore critical real-world constraints around performance, cost, and bidirectional access. The most important omission is **Lakehouse Federation via BigQuery** and **Delta Sharing from a GCP workspace**, both of which could provide UC-governed access — directly contradicting the conclusion that "UC governance is impossible."

---

## Gap Analysis

| # | Gap | Severity | Category | Recommendation |
|---|-----|----------|----------|----------------|
| 1 | Delta Sharing (GCP provider -> AWS recipient) never tested | **Critical** | Untested approach | Test immediately — this is a GA, supported cross-cloud path with full UC governance |
| 2 | Lakehouse Federation via BigQuery foreign catalog never tested | **Critical** | Untested approach | Test — BigQuery federation from AWS is GA and could query GCS-backed BigQuery external tables |
| 3 | Databricks-to-Databricks workspace federation never tested | **High** | Untested approach | Test — AWS workspace can create a foreign catalog pointing at a GCP workspace's UC catalog |
| 4 | Job clusters marked "untested" for all approaches | **High** | Incomplete testing | Job clusters can have different spark_conf behavior; must be validated |
| 5 | Only DBR 16.4 tested; no version matrix | **Medium** | Incomplete testing | At minimum test DBR 15.4 LTS and latest serverless version |
| 6 | No performance benchmarks for Python SDK approach | **High** | Missing analysis | The recommended serverless path loads everything into driver memory — critical for large datasets |
| 7 | No write-path testing (only reads) | **Medium** | Incomplete scope | Customer may need bidirectional access |
| 8 | UC Volumes not tested as a governed wrapper | **Medium** | Untested approach | External volumes on classic could provide partial governance |
| 9 | DBFS mounts (legacy) not tested | **Low** | Untested approach | Deprecated but might work for quick-and-dirty classic access |
| 10 | Init scripts not tested for classic cluster config | **Medium** | Incomplete testing | Alternative to spark_conf for injecting GCS credentials |
| 11 | Arrow/Pandas direct reads bypassing Spark not tested | **Medium** | Untested approach | Could offer better performance than Python SDK + createDataFrame |
| 12 | No cost analysis of any approach | **Medium** | Missing analysis | Cross-cloud egress, compute costs, and BigQuery query costs matter |
| 13 | Conclusion overstates "UC governance is impossible" | **Critical** | Weak conclusion | Delta Sharing and Lakehouse Federation both provide UC governance |
| 14 | No data replication / ETL pattern evaluated | **Medium** | Architecture gap | Scheduled sync to S3 may be simplest long-term solution |
| 15 | Instance profile with GCS access not explored | **Low** | Untested approach | Unlikely to work but not documented as tested |

---

## Detailed Analysis of Untested Approaches

### 1. Delta Sharing from GCP Workspace (LIKELY VIABLE — Critical Gap)

**What it is:** A GCP Databricks workspace natively accesses GCS via UC external locations. The GCP workspace acts as a Delta Sharing *provider*, sharing tables/views to the AWS workspace as a *recipient*. The AWS workspace sees shared data as a read-only catalog under UC governance.

**Why it was missed:** The experiment focused exclusively on direct GCS access from the AWS workspace. Delta Sharing was mentioned in passing in the recommendations but never tested.

**Feasibility: HIGH**
- Delta Sharing Databricks-to-Databricks is GA across AWS, Azure, and GCP
- Cross-cloud sharing (GCP provider to AWS recipient) is explicitly supported
- Provides full UC governance: access control, audit logging, lineage
- Works on both serverless and classic compute on the recipient side
- Supports tables, views, volumes, and AI models as of 2025

**Limitations:**
- Requires a GCP Databricks workspace (additional cost and operational overhead)
- Data is read-only on the recipient side
- Latency depends on Delta Sharing's caching and materialization behavior
- Provider must actively manage shares and recipients

**Verdict: WORTH TESTING — should be the #1 recommended governed approach**

### 2. Lakehouse Federation via BigQuery Foreign Catalog (LIKELY VIABLE — Critical Gap)

**What it is:** Create a BigQuery foreign catalog in the AWS Databricks workspace using Lakehouse Federation. In BigQuery, create external tables backed by GCS data (BigLake external tables support Parquet, ORC, Avro, CSV, JSON, and Delta Lake formats). Query the GCS data through the BigQuery federation layer.

**Why it was missed:** The experiment only tested direct storage-layer access patterns. Federation through an intermediary database was not considered.

**Feasibility: HIGH**
- BigQuery Lakehouse Federation from AWS Databricks is GA
- BigQuery external tables can point directly at GCS paths (no data movement)
- The foreign catalog provides UC governance (access control, lineage) on the Databricks side
- Authentication uses a GCP service account key JSON — the experiment already has one provisioned
- Works on both serverless and classic compute

**Limitations:**
- Adds BigQuery as a dependency (cost, ops overhead)
- Query performance depends on BigQuery's external table scan performance
- BigQuery external tables have query limitations vs native BigQuery tables
- BigQuery charges for bytes scanned on external table queries
- Read-only (BigQuery external tables over GCS are generally read-only)

**Verdict: WORTH TESTING — provides UC governance without a GCP Databricks workspace**

### 3. Databricks-to-Databricks Workspace Federation (LIKELY VIABLE)

**What it is:** Create a Lakehouse Federation connection from the AWS workspace to a GCP Databricks workspace. Create a foreign catalog mirroring the GCP workspace's UC catalog. Query GCS-backed tables through the federation layer.

**Why it was missed:** Similar to Delta Sharing — the experiment focused on direct access only.

**Feasibility: MEDIUM-HIGH**
- Databricks-to-Databricks federation is documented and supported
- Requires network connectivity between workspaces (potentially complex for cross-cloud)
- The GCP workspace must expose a SQL warehouse or cluster endpoint accessible from AWS
- Provides UC governance on the AWS side via foreign catalog

**Limitations:**
- Requires a GCP Databricks workspace (same as Delta Sharing path)
- Network connectivity between AWS and GCP may require VPN/peering/PrivateLink
- Query performance depends on the GCP workspace's compute
- More complex networking setup than Delta Sharing

**Verdict: WORTH TESTING — but Delta Sharing is simpler for the same outcome**

### 4. UC Volumes as a Governed Wrapper (POSSIBLY VIABLE)

**What it is:** On classic compute, after reading GCS data via one of the working approaches (s3a://, gs://, or Python SDK), write the data to a UC external volume backed by S3. This doesn't solve *reading* from GCS under UC, but it governs the data once ingested.

**Why it was missed:** Volumes were not considered as part of the access pattern.

**Feasibility: MEDIUM**
- This is really an ingestion pattern, not a direct access pattern
- External volumes provide UC governance for the landed data
- Could be automated as a scheduled job

**Limitations:**
- Data is copied, not federated — introduces latency and staleness
- Requires a pipeline to keep data in sync
- Doesn't solve real-time or near-real-time access requirements

**Verdict: WORTH CONSIDERING as part of a production architecture, not as a direct access method**

### 5. Arrow/Pandas Direct Reads (WORTH TESTING)

**What it is:** Instead of `google-cloud-storage` SDK -> `spark.createDataFrame()`, use `gcsfs` + `pyarrow` or `pandas` to read Parquet/Delta files directly into Arrow tables or Pandas DataFrames. Then convert to Spark DataFrames if needed.

**Why it was missed:** The experiment tested one Python SDK pattern but didn't explore optimized variants.

**Feasibility: HIGH (for performance improvement)**
- `gcsfs` provides a filesystem interface to GCS compatible with `pyarrow.parquet.read_table()`
- Arrow-native reads avoid CSV parsing overhead
- `pyarrow` can read Parquet files with predicate pushdown and column pruning
- `delta-rs` (the Rust Delta Lake library) supports GCS natively via `deltalake` Python package
- Could significantly outperform the CSV-focused Python SDK approach

**Limitations:**
- Still loads into driver memory (same fundamental constraint as Python SDK)
- No UC governance
- Requires additional dependencies (`gcsfs`, `pyarrow`, potentially `deltalake`)

**Verdict: WORTH TESTING — likely a meaningful performance improvement over the current Python SDK approach**

### 6. DBFS Mounts (UNLIKELY TO WORK on AWS)

**What it is:** Legacy `dbutils.fs.mount()` to mount a GCS bucket via `gs://` on an AWS workspace.

**Feasibility: LOW**
- DBFS mounts for `gs://` are documented only for GCP Databricks workspaces
- AWS workspaces would need the GCS Hadoop connector at the DBFS layer, which is unlikely to be configured
- DBFS mounts are deprecated and do not work with Unity Catalog
- Classic-only (no serverless support)

**Verdict: DEFINITELY WON'T WORK on AWS — skip**

### 7. Instance Profiles with GCS Access (WON'T WORK)

**What it is:** Configure an AWS instance profile that has cross-cloud credentials to GCS.

**Feasibility: NONE**
- AWS IAM instance profiles authenticate to AWS services only
- There is no IAM federation path from an EC2 instance profile to GCP GCS
- GCP Workload Identity Federation could theoretically bridge AWS IAM to GCP SA, but Databricks' Hadoop filesystem layer doesn't support this chain

**Verdict: DEFINITELY WON'T WORK — skip**

### 8. Data Replication to S3 (VIABLE — Architecture Alternative)

**What it is:** Instead of reading from GCS at query time, replicate GCS data to S3 on a schedule using:
- GCP Storage Transfer Service (native GCS -> S3 sync)
- A Databricks job on the GCP workspace that reads GCS and writes to S3
- An intermediary service (Cloud Function / Lambda) that syncs data

**Feasibility: HIGH**
- GCP Storage Transfer Service is a managed, zero-code solution
- Data lands in S3 under full UC governance
- No cross-cloud access complexity at query time

**Limitations:**
- Data staleness (batch sync)
- S3 storage costs (duplication)
- Cross-cloud egress costs from GCP
- Operational overhead of maintaining sync pipeline

**Verdict: LIKELY VIABLE — the most production-ready architecture for large-scale, governed access**

---

## Weak Conclusions in the Original Experiment

### 1. "UC governance is impossible" — OVERSTATED

The experiment only tested *direct storage-layer* UC patterns (external locations, storage credentials). It did not test:
- **Delta Sharing** — provides full UC governance on shared data
- **Lakehouse Federation via BigQuery** — provides UC foreign catalog governance
- **Databricks-to-Databricks federation** — provides UC foreign catalog governance

The correct conclusion should be: "UC *external locations* cannot point directly at GCS from AWS. However, UC-governed access is achievable via Delta Sharing or Lakehouse Federation."

### 2. Job clusters were never tested

Every row in the results matrix shows "untested" for job clusters. Job clusters can behave differently from interactive clusters:
- Different init script execution paths
- Different default spark_conf inheritance
- Potentially different serverless behavior for workflow jobs vs notebook serverless

This is a gap because the customer likely needs a production job, not an interactive notebook.

### 3. Only one DBR version tested (16.4)

The experiment used `16.4.x-scala2.12`. Databricks frequently adds cross-cloud features in new runtimes (e.g., R2 support required DBR 14.3+). Testing on the latest DBR and the latest serverless runtime version could reveal new capabilities.

### 4. Classic cluster spark.conf.set() failure — was it truly conclusive?

The experiment found that `spark.conf.set()` at runtime doesn't propagate to the Hadoop filesystem layer via Databricks Connect. However:
- Was this tested with a notebook running *directly* on the cluster (not via Connect)?
- Init scripts were never tested as an alternative config injection path
- Cluster environment variables (`SPARK_DAEMON_JAVA_OPTS`) were not explored

### 5. UC Approach 5 permission errors — were some tests inconclusive?

Several UC tests hit permission-related errors. In some cases, `skip_validation=True` was used to bypass creation checks, meaning the failures were at *query time* not *creation time*. It is worth distinguishing between:
- "UC fundamentally cannot route to GCS" (architectural)
- "Our specific IAM role doesn't have GCS access" (configuration)

The analysis correctly identifies this as architectural, but the experiment logs should make this distinction explicit.

---

## Missing Real-World Considerations

### Performance

The Python SDK approach (recommended for serverless) reads *everything into driver memory* via HTTP, then calls `spark.createDataFrame()`. For datasets larger than a few GB:
- Driver OOM is a real risk
- No partition pruning or predicate pushdown at the GCS level
- `createDataFrame()` serializes from Python to JVM — a known bottleneck
- No benchmarks were provided for any dataset size

**Recommendation:** Benchmark with 100MB, 1GB, 10GB, and 100GB datasets. Compare Python SDK vs Arrow-native reads vs `deltalake` (delta-rs) for Parquet and Delta formats.

### Write Path

The experiment only tested *reads*. If the customer needs to write back to GCS (e.g., processed results, exports), the approaches need re-evaluation. The Python SDK can write, but `google-cloud-storage` uploads are single-threaded and not suitable for large writes without additional tooling.

### Cost Implications

| Approach | Key Cost Factors |
|----------|-----------------|
| Python SDK (serverless) | Serverless DBU cost + GCP egress |
| s3a/gs:// (classic) | Classic cluster cost + GCP egress |
| Delta Sharing (GCP->AWS) | GCP workspace cost + Delta Sharing materialization + egress |
| BigQuery federation | BigQuery on-demand query cost (bytes scanned) + egress |
| Data replication to S3 | S3 storage + GCP egress + sync pipeline compute |

No cost comparison was provided in the experiment.

### Incremental / CDC Patterns

The experiment tested full-file reads. Real-world patterns often require:
- Incremental reads (new files since last checkpoint)
- Change data capture from GCS (object notifications -> trigger pipeline)
- Streaming from GCS (Spark Structured Streaming with GCS as a source)

None of these were tested or discussed.

---

## Recommended Next Experiments (Prioritized)

### Tier 1: High Likelihood of Success — Test Immediately

| # | Experiment | Expected Outcome | Effort |
|---|-----------|-------------------|--------|
| 1 | **Delta Sharing: GCP workspace as provider, AWS workspace as recipient** | Full UC-governed read access to GCS-backed tables on the AWS side | Medium — requires a GCP Databricks workspace |
| 2 | **Lakehouse Federation: BigQuery foreign catalog on AWS with GCS-backed external tables** | UC-governed read access via BigQuery federation; no GCP Databricks workspace needed | Low — only needs a BigQuery dataset with external tables |
| 3 | **Arrow/Pandas direct reads** (`gcsfs` + `pyarrow` + `deltalake`) on serverless | Faster ungoverned reads than current Python SDK approach | Low — just new Python dependencies |
| 4 | **Performance benchmarks** for Python SDK approach at 100MB, 1GB, 10GB | Quantify the practical limits of the recommended serverless path | Low |

### Tier 2: Worth Testing — Fill Gaps

| # | Experiment | Expected Outcome | Effort |
|---|-----------|-------------------|--------|
| 5 | **Job cluster validation** for approaches 1-4 | Confirm classic results hold for production job clusters | Low |
| 6 | **Databricks-to-Databricks workspace federation** (AWS -> GCP foreign catalog) | UC-governed access, but may need VPN/networking | Medium-High |
| 7 | **Data replication via GCP Storage Transfer Service** to S3 | Fully governed S3-native access with batch latency | Medium |
| 8 | **Write-path testing** (Python SDK upload, s3a:// write, gs:// write) | Determine if bidirectional access works | Low |
| 9 | **DBR version matrix** (15.4 LTS, 16.4, latest serverless) | Check if newer runtimes add cross-cloud capabilities | Low |

### Tier 3: Low Priority — Edge Cases

| # | Experiment | Expected Outcome | Effort |
|---|-----------|-------------------|--------|
| 10 | **Init scripts** for credential injection on classic | Alternative to spark_conf, might enable runtime flexibility | Low |
| 11 | **Streaming reads** from GCS via Structured Streaming (classic) | Validate near-real-time patterns | Medium |
| 12 | **`deltalake` (delta-rs) native GCS reads** on serverless | Pure-Python Delta reader, no Spark dependency for reads | Low |

---

## Revised Conclusion

The original experiment's conclusion that "UC governance is impossible" applies only to *direct storage-layer access via UC external locations*. At least two GA Databricks features — **Delta Sharing** and **Lakehouse Federation via BigQuery** — provide UC-governed paths to GCS data from an AWS workspace that were never tested. These should be the immediate focus of follow-up experimentation before advising the customer that governance requires "lobbying Databricks."

---

## Sources

- [Databricks Lakehouse Federation overview (AWS)](https://docs.databricks.com/aws/en/query-federation/)
- [Run federated queries on BigQuery from AWS](https://docs.databricks.com/aws/en/query-federation/bigquery)
- [Run federated queries on another Databricks workspace (AWS)](https://docs.databricks.com/aws/en/query-federation/databricks)
- [Delta Sharing overview (AWS)](https://docs.databricks.com/aws/en/delta-sharing/)
- [Delta Sharing Databricks-to-Databricks (GCP provider)](https://docs.databricks.com/gcp/en/delta-sharing/share-data-databricks)
- [Delta Sharing setup for providers (GCP)](https://docs.databricks.com/gcp/en/delta-sharing/set-up)
- [Delta Sharing recipient access (AWS)](https://docs.databricks.com/aws/en/delta-sharing/recipient)
- [Private Cross-Cloud Delta Sharing using Databricks](https://medium.com/databricks-platform-sme/private-cross-cloud-delta-sharing-using-databricks-bc6239d134c6)
- [What's New with Data Sharing — Summer 2025](https://www.databricks.com/blog/whats-new-data-sharing-and-collaboration-summer-2025)
- [Unity Catalog Volumes (AWS)](https://docs.databricks.com/aws/en/volumes/)
- [Serverless compute limitations (AWS)](https://docs.databricks.com/aws/en/compute/serverless/limitations)
- [DBFS mounts (legacy, AWS)](https://docs.databricks.com/aws/en/dbfs/mounts)
- [BigLake external tables for Delta Lake (GCP)](https://docs.cloud.google.com/bigquery/docs/create-delta-lake-table)
- [Databricks cross-cloud federation community discussion](https://community.databricks.com/t5/data-engineering/can-databricks-federation-policy-support-cross-cloud/td-p/132460)
