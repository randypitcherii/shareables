# AWS Databricks → GCS Access: Experiment Results

**Date:** 2026-03-10
**Author:** Randy Pitcher
**Status:** Complete

---

## Problem Statement

A user needs to read data stored in Google Cloud Storage (GCS) from an AWS-hosted Databricks workspace. This experiment evaluates five approaches across both serverless and classic compute to determine viable cross-cloud data access patterns.

---

## Results Matrix

| # | Approach | Serverless | Classic | Notes |
|---|----------|:----------:|:-------:|-------|
| 1 | HMAC / S3-compatible (`s3a://`) | FAIL | PASS | Serverless blocks `spark.hadoop.*` overrides |
| 2 | Python `google-cloud-storage` SDK | PASS | PASS | Bypasses Spark entirely; uses HTTP |
| 3 | GCS Connector JAR (`gs://`) | FAIL | PASS | GCS connector JAR is pre-installed on Databricks clusters |
| 4a | Bucket-specific `s3a` config (mixed GCS + S3) | FAIL | PASS | Cleanest isolation for mixed access on classic |
| 4b | Python SDK + Spark SQL (mixed GCS + S3) | PASS | PASS | Only mixed-access path on serverless |
| 4c | `gs://` + `s3a://` (mixed GCS + S3) | FAIL | PASS | Requires cluster-level spark_conf |
| 5a | UC External Location — `s3://` + IAM role | FAIL | FAIL | IAM role routes to AWS S3, not GCS |
| 5b | UC External Location — `r2://` → GCS endpoint | FAIL | FAIL | UC validates R2 keys server-side; rejects GCS HMAC keys |
| 5c | UC External Location — R2 credential + `s3://` URL | FAIL | FAIL | Same R2 key validation failure |
| 5d | UC External Location — `gs://` on AWS | FAIL | FAIL | Accepted with `skip_validation`, but LIST fails (IAM credential error) |
| 5e | UC External Location — Creative URLs (`s3a://`, embedded endpoints) | FAIL | FAIL | `s3a://` silently converted to `s3://`; all fail at query time |

---

## Detailed Findings

### Approach 1: HMAC / S3-Compatible Access (`s3a://`)

GCS supports S3-compatible access via HMAC keys and the `s3a://` protocol. This requires setting `spark.hadoop.fs.s3a.endpoint` and related Hadoop configs.

- **Serverless:** `CONFIG_NOT_AVAILABLE` — serverless compute blocks all `spark.hadoop.*` overrides at runtime.
- **Classic:** Works when credentials are set as **cluster-level `spark_conf`** properties. Runtime `spark.conf.set()` calls do **not** work; configs must be present at cluster startup.

### Approach 2: Python `google-cloud-storage` SDK

Uses the native GCS Python client library, bypassing Spark's I/O layer entirely and communicating with GCS over HTTPS.

- **Serverless:** Works. The SDK runs as a standard Python HTTP client with no dependency on Spark's Hadoop filesystem layer.
- **Classic:** Works identically.

### Approach 3: GCS Connector JAR (`gs://`)

Uses the Hadoop-compatible `gs://` filesystem provided by the GCS connector JAR.

- **Serverless:** `CONFIG_NOT_AVAILABLE` — same `spark.hadoop.*` restriction as Approach 1.
- **Classic:** Works. The GCS connector JAR is **pre-installed** on Databricks clusters, so no library installation is required. Credentials are configured via cluster-level `spark_conf`.

### Approach 4: Mixed GCS + S3 Coexistence

Real-world scenarios often require reading from GCS while continuing to use S3-backed tables.

- **4a. Bucket-specific `s3a` config:** Uses `fs.s3a.bucket.<BUCKETNAME>.<setting>` to scope GCS HMAC credentials to a specific bucket name without affecting default S3 access. Serverless blocks these configs; classic works cleanly with **zero interference** to existing S3 access.
- **4b. Python SDK + Spark SQL:** Reads GCS via the Python SDK (Approach 2), then registers data as a Spark DataFrame or temp view for SQL queries. The only mixed-access strategy that works on serverless.
- **4c. `gs://` + `s3a://`:** Combines the GCS connector (Approach 3) with native S3 access. Works on classic only.

**Key finding:** On serverless, the Python SDK is the **only** mixed-access path. On classic, bucket-specific `s3a` config (4a) is the cleanest solution — it isolates GCS credentials per-bucket with no risk of disrupting S3 access.

### Approach 5: Unity Catalog External Location Tricks

Attempted to leverage UC's storage credential and external location abstractions to route requests to GCS from an AWS workspace. **All sub-approaches failed.**

- **5a.** `s3://` URL with an IAM role — the IAM role naturally routes to AWS S3, not GCS.
- **5b.** `r2://` URL pointed at the GCS endpoint — UC validates R2 secret keys server-side and rejects GCS HMAC keys.
- **5c.** R2 storage credential with an `s3://` URL — same R2 key validation failure.
- **5d.** `gs://` URL on an AWS workspace — surprisingly accepted with `skip_validation`, but queries fail because the underlying credential is an IAM role bound to AWS.
- **5e.** Creative URL schemes (`s3a://`, embedded endpoints) — `s3a://` is silently converted to `s3://` internally; all attempts fail at query time.

**Root cause:** Unity Catalog's credential types, URL scheme validation, and internal storage clients are hardcoded per cloud provider. There is no custom endpoint support, making it impossible to redirect an AWS workspace's storage layer to GCS.

---

## Recommendations

### 1. Serverless Compute

Use the **Python `google-cloud-storage` SDK** (Approach 2). This is the only working path on serverless. Note that data accessed this way is **outside Unity Catalog governance** — there is no read auditing, lineage tracking, or access control via UC.

### 2. Classic Compute — Simple GCS Read

Use either **HMAC/`s3a://`** (Approach 1) or the **`gs://` connector** (Approach 3) with cluster-level `spark_conf`. Both are straightforward and support Spark-native reads. The `gs://` connector has the advantage of not requiring the S3-compatibility layer.

### 3. Classic Compute — Mixed GCS + S3 Access

Use **bucket-specific `s3a` config** (Approach 4a). The `fs.s3a.bucket.<BUCKETNAME>.*` pattern scopes GCS HMAC credentials to a single bucket name, leaving all other S3 access untouched. This is the cleanest isolation strategy.

### 4. Unity Catalog Governance

Cross-cloud GCS access from an AWS workspace is **not currently possible** under UC governance. For governed access, consider:

- **Delta Sharing** from a GCP-hosted Databricks workspace to the AWS workspace
- **Lobbying Databricks** for AWS → GCS cross-cloud external location support (currently, only GCP → S3 read-only access exists as a cross-cloud feature)

---

## Summary

Cross-cloud GCS access from AWS Databricks is achievable but constrained. Serverless is limited to the Python SDK (no Spark-native reads, no UC governance). Classic compute offers full Spark-native support via cluster-level configuration. Unity Catalog cannot broker cross-cloud storage access today — governed cross-cloud patterns require Delta Sharing or future platform enhancements.
