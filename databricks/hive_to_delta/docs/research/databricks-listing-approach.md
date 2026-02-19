# Research: DatabricksListing Strategy (htd-1s4)

**Date:** 2026-02-17
**Goal:** Replace boto3-based `S3Listing` with a Databricks-native `DatabricksListing` class that implements the `Listing` protocol (`list_files(spark, table) -> list[ParquetFileInfo]`).

---

## Candidate Comparison

| Criteria | dbutils.fs.ls() | Hadoop FileSystem API | spark.read + input_file_name() | UC Volumes API |
|---|---|---|---|---|
| **Recursive listing** | Manual recursion required | `listFiles(path, true)` -- single S3 LIST call | Automatic via Spark | Only for volume paths |
| **Parallelization** | None (driver-only, serial) | None (driver-side JVM call) | Spark-distributed | None |
| **10K files** | Slow (sequential per-dir) | Fast (single flat LIST) | Fast but overhead of reading footers | N/A for external tables |
| **100K files** | Very slow | Fast | Moderate (Spark task overhead) | N/A |
| **1M files** | Impractical | Manageable with pagination | Feasible but expensive | N/A |
| **Auth model** | Workspace credentials / UC external locations | Inherits Spark Hadoop config (instance profile, UC credentials) | Same as Spark read | UC permissions only |
| **Returns file size** | Yes (FileInfo.size) | Yes (FileStatus.getLen()) | No -- would need separate call | Yes |
| **API stability** | Stable but "dbutils on executors" caveat | Hadoop public API, extremely stable | Spark SQL public API | Newer, volumes-only |
| **Error handling** | Python exceptions | Java exceptions via Py4J | Spark exceptions | REST API errors |
| **Works with s3:// paths** | Yes (s3a:// scheme) | Yes | Yes | No (volumes only) |
| **Partition value extraction** | Parse from path strings | Parse from path strings | Parse from path strings | N/A |

---

## Candidate Deep Dives

### 1. dbutils.fs.ls()

**How it works:** Returns a list of `FileInfo` namedtuples with fields `path`, `name`, `size`, `modificationTime`. Non-recursive -- only lists immediate children of a directory. Recursive listing requires manual tree-walking in Python.

**Pros:**
- Simple API, well-known in Databricks ecosystem
- Returns file size directly
- Uses workspace credentials automatically (instance profile, UC external locations)

**Cons:**
- Non-recursive: must walk directory tree manually, issuing one HTTP round-trip per directory level per partition
- Runs only on the driver -- cannot parallelize across executors
- For a partitioned table with 1000 partitions and 3 partition levels, this is 3000+ sequential API calls
- Cannot be called from executors (documented limitation)
- Slow for large directory hierarchies -- documented performance issues at scale

**Verdict:** Not recommended. Serial directory-walking over S3 is the same bottleneck we have with boto3, just with a different client library.

### 2. Hadoop FileSystem API (spark._jvm)

**How it works:** Access Hadoop's `FileSystem` class through PySpark's Py4J bridge. The critical method is `listFiles(path, recursive=true)`, which for S3A performs a flat `LIST` operation -- enumerating all objects under a prefix in a single paginated API call, regardless of directory depth.

**Setup:**
```python
jvm = spark.sparkContext._jvm
uri = jvm.java.net.URI.create("s3a://bucket/path/")
conf = spark.sparkContext._jsc.hadoopConfiguration()
fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
```

**Key methods:**
- `fs.listFiles(path, recursive=True)` -- returns `RemoteIterator<LocatedFileStatus>` with path, length, modification time. For S3A this is O(files) with async page fetch, not O(directories).
- `fs.listStatus(path)` -- non-recursive, returns `FileStatus[]`. Less useful for our case.
- `fs.globStatus(pathPattern)` -- supports glob patterns but still directory-walking internally.

**Pros:**
- `listFiles(path, true)` is the fastest way to enumerate S3 objects -- uses a single flat S3 LIST call with prefix scanning
- Returns file size (`getLen()`) and modification time
- Inherits all Spark/Hadoop auth configuration (instance profiles, UC external locations, STS credentials)
- Extremely stable API (Hadoop FileSystem has been public API for 15+ years)
- No additional dependencies -- available on every Databricks cluster
- Paginated automatically by the S3A connector

**Cons:**
- Runs on the driver JVM, not distributed across executors (but the S3 LIST API is inherently serial per-prefix anyway)
- Py4J bridge means Java exceptions need careful handling
- Uses private PySpark API (`spark.sparkContext._jvm`) -- not officially supported by Databricks, but universally used and unlikely to break

**Verdict:** Recommended approach. The flat recursive listing via S3A is fundamentally more efficient than directory-walking, and it returns all the metadata we need.

### 3. spark.read.parquet() + input_file_name()

**How it works:**
```python
files_df = (spark.read.parquet("s3a://bucket/table_location/")
            .select(input_file_name().alias("file_path"))
            .distinct())
```

**Pros:**
- Distributed -- Spark can scan partitions in parallel across executors
- Automatically discovers all files Spark considers part of the dataset
- Handles partition discovery natively

**Cons:**
- Does NOT return file size -- `input_file_name()` only gives the path. We need size for the Delta log's `add` actions.
- To get file size, we'd need a second pass (Hadoop FS calls per file), which defeats the purpose
- Reads parquet footers for every file (I/O overhead beyond just listing)
- Higher overhead: spins up Spark jobs, tasks, stages just to list files
- Overkill for file discovery -- we don't need to read any data

**Verdict:** Not recommended. The missing file size is a dealbreaker since `ParquetFileInfo` requires `size`, and the Delta log `add` action requires `size`. The overhead of reading parquet footers is also wasteful when we only need file metadata.

### 4. Unity Catalog Volumes API

**How it works:** The Files API (`/api/2.0/fs/directories` and `/api/2.0/fs/files`) or `dbutils.fs.ls("/Volumes/catalog/schema/volume/...")` for files stored in UC volumes.

**Pros:**
- Modern Databricks-native API
- Integrated with UC permissions

**Cons:**
- Only works for files stored in UC Volumes (`/Volumes/...` paths)
- Our use case is external Hive tables on S3 (`s3://` paths) -- volumes are irrelevant
- Not designed for listing external table data

**Verdict:** Not applicable. This is designed for managed volume storage, not for discovering files in external Hive table locations.

---

## Recommendation

**Use the Hadoop FileSystem API via `spark._jvm`**, specifically `fs.listFiles(path, recursive=true)`.

**Rationale:**
1. **Performance:** For S3, `listFiles(path, recursive=true)` translates to a flat `S3 LIST` API call with prefix filtering -- O(files) with async pagination, not O(directories). This is fundamentally faster than the boto3 approach (which does the same flat LIST but from outside the cluster) or dbutils (which walks directories serially).
2. **File size included:** `LocatedFileStatus.getLen()` returns file size, which we need for `ParquetFileInfo.size` and the Delta log.
3. **Zero new dependencies:** Available on every Databricks cluster. No boto3, no SDK installs, no IAM credential passing.
4. **Auth is automatic:** Inherits whatever auth the Spark cluster has configured -- instance profiles, UC external locations, STS roles. No credential management needed in our code.
5. **Protocol compatibility:** Easy to implement `list_files(spark, table) -> list[ParquetFileInfo]` since the Hadoop API gives us everything we need from `spark` alone.

---

## Pseudocode: Recommended Implementation

```python
@dataclass
class DatabricksListing:
    """List parquet files using Databricks-native Hadoop FileSystem API.

    Uses the cluster's configured credentials (instance profile, UC external
    locations) -- no boto3 or AWS credentials needed.
    """

    def list_files(self, spark: Any, table: TableInfo) -> list[ParquetFileInfo]:
        """List parquet files for a table via Hadoop FileSystem.

        Uses fs.listFiles(path, recursive=True) which performs a flat S3 LIST
        operation -- O(files) regardless of directory depth.
        """
        location = table.location.rstrip("/")

        # Convert s3:// to s3a:// (Hadoop's S3 connector scheme)
        if location.startswith("s3://"):
            location = "s3a://" + location[5:]

        jvm = spark.sparkContext._jvm
        conf = spark.sparkContext._jsc.hadoopConfiguration()

        uri = jvm.java.net.URI.create(location)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
        path = jvm.org.apache.hadoop.fs.Path(location)

        # Flat recursive listing -- single S3 LIST call for all files
        iterator = fs.listFiles(path, True)  # recursive=True

        files: list[ParquetFileInfo] = []
        while iterator.hasNext():
            status = iterator.next()
            file_path = status.getPath().toString()

            # Filter to .parquet files only
            if not file_path.endswith(".parquet"):
                continue

            # Normalize back to s3:// for consistency with rest of codebase
            if file_path.startswith("s3a://"):
                file_path = "s3://" + file_path[6:]

            # Parse partition values from path if table is partitioned
            partition_values = (
                _parse_partition_values(file_path, table.partition_keys)
                if table.partition_keys
                else {}
            )

            files.append(
                ParquetFileInfo(
                    path=file_path,
                    size=status.getLen(),
                    partition_values=partition_values,
                )
            )

        return files
```

### Key Design Decisions

1. **s3:// to s3a:// conversion:** Hadoop uses the `s3a://` scheme for its S3 connector. Our codebase uses `s3://` everywhere (in `ParquetFileInfo.path`, `TableInfo.location`). The conversion is done at the boundary -- convert to s3a:// for the Hadoop call, normalize back to s3:// in the returned paths.

2. **Single flat listing:** We do NOT walk directories. `listFiles(path, true)` on S3A does a single paginated `LIST` call. This handles partitioned tables with arbitrary depth without extra round-trips.

3. **Partition parsing reuse:** We reuse the existing `_parse_partition_values()` function from `listing.py` to extract partition key=value pairs from file paths, keeping behavior consistent with `InventoryListing`.

4. **No Glue dependency:** Unlike `S3Listing`, `DatabricksListing` does not need Glue to discover partition locations. It simply lists all files under the table root and parses partition values from the paths. This is simpler and eliminates the Glue API dependency.

5. **Error handling:** Py4J will raise `Py4JJavaError` for Java exceptions. These should be caught and wrapped in Python exceptions with clear messages.

---

## Sources

- [Databricks dbutils reference (AWS)](https://docs.databricks.com/aws/en/dev-tools/databricks-utils)
- [Hadoop FileSystem API - listFiles, listStatus](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/filesystem.html)
- [HADOOP-15192: S3A listStatus excessively slow](https://issues.apache.org/jira/browse/HADOOP-15192)
- [HADOOP-10634: Add recursive list APIs to FileSystem](https://issues.apache.org/jira/browse/HADOOP-10634)
- [HADOOP-17400: Optimize S3A for maximum performance in directory listings](https://issues.apache.org/jira/browse/HADOOP-17400)
- [Hadoop S3A performance guide](https://hadoop.apache.org/docs/r3.4.0/hadoop-aws/tools/hadoop-aws/performance.html)
- [Comprehensive guide to listing files via PySpark](https://www.perceptivebits.com/a-comprehensive-guide-to-finding-files-via-spark/)
- [Spark input_file_name() API docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.input_file_name.html)
- [Unity Catalog volumes documentation](https://docs.databricks.com/aws/en/volumes/)
- [Databricks working with files](https://docs.databricks.com/aws/en/files/)
