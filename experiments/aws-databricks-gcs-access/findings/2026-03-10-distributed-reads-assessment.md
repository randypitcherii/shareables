# Assessment: Distributed GCS Reads for Large Datasets

**Date:** 2026-03-10
**Status:** Complete
**Bead:** shareables-byt

---

## Question

For large-scale, regular GCS data access from AWS Databricks, should we:
1. Build a custom distributed reader using Arrow/Pandas (`gcsfs` + `pyarrow` + `deltalake`)?
2. Use native Spark reads with the working Hadoop S3A or GCS connector approaches?

## Answer

**Use native Spark reads on classic/job compute. Don't build custom distribution.**

On classic and job clusters, `spark.read.format("delta").load("s3a://bucket/path")` with bucket-specific S3A config is already fully distributed across Spark workers. Each executor inherits the Hadoop config from the cluster definition and reads partitions in parallel. This is the standard, optimized path — no custom code needed.

## Why Not Arrow/Pandas?

| Factor | Native Spark | Arrow/Pandas Custom |
|--------|-------------|-------------------|
| Distribution | Automatic across executors | Driver-only (single node) |
| Predicate pushdown | Yes (via Spark optimizer) | Partial (pyarrow on Parquet only) |
| Delta Lake support | Full (time travel, Z-order, liquid clustering) | Limited (delta-rs reads only, no advanced features) |
| Memory model | Executor memory, spills to disk | Driver memory only, OOM risk |
| Maintenance | Zero — standard Spark | Custom code to maintain |
| UC governance | No (non-UC path) | No (non-UC path) |

## When Arrow/Pandas IS Appropriate

- **Small datasets on serverless** (< 1GB): The Python SDK approach works fine
- **Metadata-only operations**: Listing files, reading schemas, sampling
- **Non-Spark consumers**: If data feeds into a Pandas/ML pipeline that doesn't need Spark

## Recommended Approach by Scenario

| Scenario | Compute | Approach |
|----------|---------|----------|
| Large data, regular reads | Classic/Job cluster | Native Spark via `s3a://` with bucket-specific config |
| Large data, needs UC governance | Classic/Job cluster | Delta Sharing or Lakehouse Federation (separate experiment) |
| Small data, ad-hoc | Serverless | Python SDK (`google-cloud-storage`) |
| Mixed GCS + S3 in same job | Classic/Job cluster | Bucket-specific s3a config (Approach 4a) |

## Scaling Characteristics of Working Approaches

### s3a:// with bucket-specific config (Approach 1/4a on classic/job)
- **Scales with cluster size** — more executors = more parallel partition reads
- **Supports all Spark data sources** — Delta, Parquet, CSV, JSON, etc.
- **Partition pruning works** — Spark pushes predicates down to skip unnecessary files
- **Limitations**: No UC governance, requires cluster-level Hadoop config

### gs:// connector (Approach 3 on classic/job)
- **Same scaling as s3a://** — native Spark distributed reads
- **GCS-native protocol** — potentially better performance than S3-compat layer
- **Pre-installed on Databricks** — no custom JARs needed

### Python SDK (Approach 2 on serverless)
- **Single-node only** — reads into driver memory via HTTP
- **Practical limit ~1GB** before driver OOM risk
- **No Spark optimization** — no predicate pushdown, no partition pruning
- **Good for**: small datasets, serverless where Hadoop config is blocked
