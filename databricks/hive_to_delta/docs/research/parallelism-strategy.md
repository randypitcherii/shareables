# Parallelism Strategy Research (htd-j5m)

Research into whether the current `ThreadPoolExecutor` approach should be replaced with Spark-native or other parallelism strategies for bulk table conversions.

**Date:** 2026-02-17
**Status:** Complete
**Recommendation:** Keep ThreadPoolExecutor (current approach)

---

## Current Implementation Summary

The `convert()` and `convert_tables()` functions use `ThreadPoolExecutor` from Python's standard library via `run_parallel()` in `parallel.py`. Each table conversion involves:

1. Schema inference (read one parquet file via Spark OR use Glue metadata via boto3)
2. Delta log JSON generation (CPU-bound, trivial)
3. Delta log write to S3 (single `put_object` call via boto3)
4. 3 SQL statements via `spark.sql()` (CREATE SCHEMA IF NOT EXISTS, DROP TABLE IF EXISTS, CREATE TABLE)

The file discovery phase (Glue API + S3 listing) happens **before** parallel execution, sequentially per table.

---

## Bottleneck Analysis for 100-Table Conversion

Based on the timing estimates in `ARCHITECTURE.md` and the code:

| Operation | Per-Table Latency | Bottleneck Type | Parallelizable? |
|-----------|-------------------|-----------------|-----------------|
| Glue metadata fetch | ~100-200ms | I/O (network) | Done before parallel phase |
| S3 file listing | ~50-100ms per partition | I/O (network) | Done before parallel phase |
| Schema inference (Spark) | ~500-1000ms | I/O (Spark query) | Yes (threads) |
| Schema inference (Glue) | ~0ms (already fetched) | N/A | N/A |
| Delta log generation | ~1-5ms | CPU (trivial) | Yes |
| Delta log S3 write | ~50-100ms | I/O (S3 put) | Yes (threads) |
| CREATE SCHEMA IF NOT EXISTS | ~200-500ms | I/O (Spark SQL) | Yes (threads) |
| DROP TABLE IF EXISTS | ~200-500ms | I/O (Spark SQL) | Yes (threads) |
| CREATE TABLE | ~200-500ms | I/O (Spark SQL) | Yes (threads) |

**Key insight:** The actual per-table conversion work is almost entirely I/O-bound. The CPU work (JSON generation) is negligible. The biggest latency contributors are the three `spark.sql()` DDL calls, at ~600-1500ms combined per table.

**Secondary insight:** The file discovery phase (Glue + S3 listing) runs sequentially before the parallel phase and may actually be a larger bottleneck for tables with many partitions. This is worth parallelizing separately.

---

## Approach Comparison

### 1. ThreadPoolExecutor (Current) -- RECOMMENDED

**How it works:** Python threads share the process and GIL. Each thread calls `_convert_one_table()` which uses the shared `spark` session for SQL calls and creates boto3 clients for S3 writes.

**Pros:**
- Simple, standard library, no extra dependencies
- Ideal for I/O-bound work -- GIL is released during network I/O
- `spark.sql()` calls release the GIL while waiting for the server response
- Shared SparkSession avoids serialization overhead
- Easy error handling with `future.result()`
- Progress reporting via callbacks

**Cons:**
- GIL prevents true CPU parallelism (irrelevant here -- CPU work is trivial)
- All workers share process memory (low risk given ~250KB per table)
- Thread count limited by driver node resources

**Verdict:** Already the right choice. The workload is I/O-bound, and threads handle this efficiently.

### 2. spark.sparkContext.parallelize + mapPartitions -- NOT VIABLE

**How it would work:** Distribute table conversion work items across Spark executors.

**Why it fails:**
- **`spark.sql()` is driver-only.** SparkContext and SparkSession cannot be used from executor code. Since Spark 3.1, attempting to create or use SparkContext on executors throws an exception. Each table conversion needs 3 DDL calls via `spark.sql()`, which means the core registration step cannot run on executors.
- **boto3 S3 writes could technically run on executors**, but the SQL registration cannot, so you'd need a split architecture (executors do S3 writes, driver does SQL) which adds complexity for minimal gain.
- The per-table S3 write is a single `put_object` -- there's no large data transfer to parallelize across executors.

**Verdict:** Not viable. The `spark.sql()` driver-only constraint is a hard blocker.

### 3. ProcessPoolExecutor -- WORSE THAN THREADS

**How it would work:** Spawn separate Python processes, each handling a subset of tables.

**Why it's worse:**
- **SparkSession cannot be serialized or shared across processes.** Each process would need its own SparkSession/connection, multiplying cluster resource consumption.
- **boto3 clients cannot be shared across processes** either -- each process needs its own clients.
- Serialization overhead for function arguments and results.
- Higher memory footprint (full process per worker vs lightweight thread).
- More complex error handling and result collection.
- The workload is I/O-bound, so multiprocessing provides no advantage over threading.

**Verdict:** Strictly worse than ThreadPoolExecutor for this workload.

### 4. asyncio with aiobotocore -- MARGINAL GAIN, HIGH COMPLEXITY

**How it would work:** Use asyncio event loop for S3 writes, combined with threaded Spark SQL calls.

**Analysis:**
- The S3 write is a single `put_object` per table -- async would save maybe 10-50ms per table from connection overhead, negligible at scale.
- `spark.sql()` has no async API, so you'd need `asyncio.to_thread()` wrappers anyway.
- Performance benchmarks show aioboto3 is comparable to threaded boto3 at scale (2.78s vs 2.89s for 1000 calls), with slightly worse performance for small batches.
- Adds aiobotocore/aioboto3 dependency.
- Mixed async/sync code (asyncio for S3, threads for Spark) is error-prone.

**Verdict:** Not worth the complexity. Marginal performance gain doesn't justify the added dependency and code complexity.

### 5. Spark Structured Streaming / foreachBatch -- NOT APPLICABLE

**How it would work:** Use Spark's streaming batch patterns to process tables.

**Why it doesn't fit:**
- Structured Streaming is designed for continuous data processing, not one-shot DDL operations.
- `foreachBatch` provides a DataFrame per micro-batch -- there's no DataFrame of "tables to convert."
- The conversion work is metadata operations (S3 writes, DDL), not data transformations.
- Would require significant architectural changes for no benefit.

**Verdict:** Wrong tool for the job.

---

## SparkSession Thread Safety Analysis

### Is SparkSession thread-safe for concurrent `spark.sql()` calls?

**Yes, with caveats.**

- SparkSession is designed to be shared across threads. The [SPARK-15135](https://issues.apache.org/jira/browse/SPARK-15135) JIRA specifically addressed making SparkSession thread-safe.
- Multiple threads can submit `spark.sql()` calls concurrently. These become separate Spark jobs that execute independently.
- **The caveat:** Metadata mutations to the session itself (e.g., changing configs, setting database context) are not thread-safe. In the hive_to_delta case, each thread only calls `spark.sql()` with fully-qualified table names, so there's no shared mutable state concern.
- Databricks Connect sessions support concurrent operations with session isolation.

### Is boto3 thread-safe for concurrent S3 operations?

**Yes, with the right pattern.**

- boto3 **clients** (e.g., `s3_client = boto3.client("s3")`) are thread-safe for API calls.
- boto3 **sessions** and **resources** are NOT thread-safe.
- **Current code concern:** `_convert_one_table()` calls `write_delta_log()` which calls `write_to_s3()` which calls `get_s3_client()` -- creating a new client per call. This is safe but wasteful. Creating the client is NOT thread-safe (the `boto3.client()` call uses the default session internally).

**Recommendation for boto3:** Create one S3 client in the main thread and pass it to workers, or create a dedicated `boto3.Session()` per thread and derive clients from that.

---

## Potential Improvements (Within Current Approach)

Rather than switching parallelism strategies, these improvements would yield better results:

### 1. Parallelize the Discovery/Listing Phase

Currently, file discovery runs sequentially before parallel conversion:

```python
# Current: sequential discovery
for table in tables:
    files = listing.list_files(spark, table)  # sequential!
    table_files.append((table, files))

# Then: parallel conversion
raw_results = run_parallel(_do_convert, table_files, max_workers=max_workers)
```

The S3 listing phase (especially for tables with many partitions) could be parallelized with its own ThreadPoolExecutor pass, potentially cutting total wall time significantly for partition-heavy tables.

### 2. Fix boto3 Client Creation Thread Safety

Create a shared S3 client or use per-thread sessions:

```python
# Option A: shared client (created once in main thread)
s3_client = boto3.client("s3", region_name=region)
# Pass to workers...

# Option B: per-thread session
import threading
_thread_local = threading.local()
def get_thread_safe_s3_client(region):
    if not hasattr(_thread_local, "s3_client"):
        session = boto3.Session()
        _thread_local.s3_client = session.client("s3", region_name=region)
    return _thread_local.s3_client
```

### 3. Batch the CREATE SCHEMA Call

The `CREATE SCHEMA IF NOT EXISTS` call is identical for all tables in a bulk conversion. Execute it once before the parallel phase instead of in every thread:

```python
# Before parallel execution
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")

# Then in _convert_one_table, skip the CREATE SCHEMA call
```

This eliminates one DDL round-trip per table.

### 4. Tune max_workers Based on Workload

- For < 10 tables: `max_workers=4` (current default) is fine
- For 10-50 tables: `max_workers=8` works well
- For 50-200 tables: `max_workers=10-16` -- bounded by Spark driver memory for concurrent query tracking
- For 200+ tables: Consider batching into groups of 50-100 with a brief pause between batches to avoid overwhelming the metastore

---

## Recommendation

**Keep ThreadPoolExecutor.** It is the correct choice for this workload.

The conversion pipeline is I/O-bound (network calls to S3, Glue, and Databricks SQL), and Python threads handle I/O-bound parallelism effectively since the GIL is released during network waits. None of the alternative approaches offer meaningful advantages, and most introduce significant drawbacks (Spark executor limitations, process serialization overhead, async complexity).

The highest-impact improvements are within the current architecture:
1. Parallelize the discovery/listing phase (likely the biggest win)
2. Hoist `CREATE SCHEMA IF NOT EXISTS` out of the per-table loop
3. Fix boto3 client creation to be properly thread-safe
4. Provide guidance on `max_workers` tuning for large-scale conversions

---

## Sources

- [SPARK-15135: Make SparkSession thread safe](https://issues.apache.org/jira/browse/SPARK-15135)
- [SPARK-26555: Thread safety issue with createDataset](https://issues.apache.org/jira/browse/SPARK-26555)
- [SparkContext driver-only limitation (Microsoft Q&A)](https://learn.microsoft.com/en-us/answers/questions/941566/sparkcontext-should-only-be-created-and-accessed-o)
- [Databricks Connect async queries documentation](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/python/async)
- [Databricks Delta Lake best practices](https://docs.databricks.com/aws/en/delta/best-practices)
- [Databricks isolation levels and write conflicts](https://docs.databricks.com/aws/en/optimizations/isolation-level)
- [boto3 client thread safety documentation](https://boto3.amazonaws.com/v1/documentation/api/1.19.0/guide/clients.html)
- [boto3 client creation thread safety issue #2750](https://github.com/boto/boto3/issues/2750)
- [aioboto3 performance comparison issue #359](https://github.com/terricain/aioboto3/issues/359)
- [Concurrency in Spark (Russell Spitzer)](https://www.russellspitzer.com/2017/02/27/Concurrency-In-Spark/)
- [Parallel ingest with Spark multithreading (Dustin Vannoy)](https://dustinvannoy.com/2022/05/06/parallel-ingest-spark-notebook/)
- [Parallelizing non-distributed tasks in Spark (Miles Cole)](https://milescole.dev/data-engineering/2024/10/11/Parallelizing-Non-Distributed-Tasks.html)
- [Databricks Community: multiprocessing/threads for parallel queries](https://community.databricks.com/t5/data-engineering/is-it-possible-to-use-multiprocessing-or-threads-to-submit/td-p/17402)
- [Databricks Community: exploring parallelism for multiple tables](https://community.databricks.com/t5/data-engineering/exploring-parallelism-for-multiple-tables/td-p/117068)
