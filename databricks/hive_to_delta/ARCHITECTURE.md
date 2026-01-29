# Architecture

This document explains how `hive_to_delta` works internally, key design decisions, and trade-offs made during development.

## Problem Statement

### Limitations of CONVERT TO DELTA

The standard `CONVERT TO DELTA` command in Databricks has critical limitations when migrating Hive tables from AWS Glue:

```sql
-- This silently loses data!
CONVERT TO DELTA parquet.`s3://bucket/table/` PARTITIONED BY (region STRING)
```

**What goes wrong:**
1. Only scans files under the specified root path
2. Ignores partitions at external S3 locations
3. No error is raised - data is silently missing
4. Cannot handle cross-bucket or cross-region partitions

### Why This Matters

In real-world AWS environments, Hive tables often have "exotic" partition layouts:

- **External partitions** - Some partitions stored outside the table root
- **Cross-bucket** - Partitions in different S3 buckets
- **Cross-region** - Partitions in different AWS regions
- **Scattered paths** - No consistent directory structure

These patterns emerge from:
- Data lake evolution and migrations
- Multi-account AWS architectures
- Organizational boundaries (team A owns bucket A, team B owns bucket B)
- Historical decisions that are costly to undo

## Solution Overview

`hive_to_delta` manually generates Delta transaction logs with absolute S3 paths, allowing Delta Lake to reference files across multiple buckets and regions without copying data.

### High-Level Flow

```
AWS Glue Metadata → S3 File Scanning → Delta Log Generation → Unity Catalog Registration
       ↓                    ↓                     ↓                        ↓
  Table schema      File paths at all    _delta_log/000...000.json    CREATE TABLE
  Partition info    partition locations  with absolute S3 paths       USING DELTA
```

## Core Components

### 1. Glue Metadata Fetching (`glue.py`)

Retrieves table and partition metadata from AWS Glue Data Catalog.

**Key functions:**
- `get_glue_table_metadata()` - Fetches table schema, storage location, partition keys
- `get_glue_partitions()` - Gets all partition locations and values (with pagination)
- `list_glue_tables()` - Discovers tables matching glob patterns

**Design decision:** Uses boto3 directly instead of Spark's Glue catalog integration for:
- Better control over pagination
- No dependency on Spark Glue connector configuration
- Clearer error handling

### 2. S3 File Scanning (`s3.py`)

Scans S3 locations to find Parquet files in each partition.

**Key functions:**
- `scan_partition_files()` - Lists `.parquet` files at given S3 paths
- `parse_s3_path()` - Extracts bucket and key from S3 URLs
- `get_s3_client()` - Creates boto3 S3 client for a region

**Why absolute paths?**

Consider a cross-bucket table:
```
Table root:   s3://bucket-a/tables/orders/
Partition 1:  s3://bucket-a/tables/orders/region=us/file1.parquet  ✓ Under root
Partition 2:  s3://bucket-b/archive/orders/region=eu/file2.parquet  ✗ Different bucket!
```

Standard Delta logs use relative paths (`region=us/file1.parquet`), which only work for files under the table root. Absolute paths allow references to any S3 location.

**File scanning strategy:**
- Uses S3 `list_objects_v2` with pagination for efficiency
- Filters for `.parquet` extension only
- Preserves partition values for schema enforcement
- Handles cross-region by creating region-specific S3 clients

### 3. Delta Log Generation (`delta_log.py`)

Creates Delta transaction log JSON files with the proper format.

**Key functions:**
- `build_delta_schema()` - Converts Glue schema to Delta schema format
- `generate_delta_log()` - Creates Delta log JSON with add actions
- `write_delta_log()` - Writes log to S3 as `_delta_log/00000000000000000000.json`

**Delta Log Format:**

Each transaction log is newline-delimited JSON with specific action types:

```json
{"commitInfo":{"timestamp":1234567890,"operation":"Manual Conversion"}}
{"metaData":{"id":"uuid","format":{"provider":"parquet"},"schemaString":"...", "partitionColumns":["region"]}}
{"add":{"path":"s3://bucket-b/archive/orders/region=eu/file2.parquet","partitionValues":{"region":"eu"},"size":1024,"modificationTime":1234567890}}
```

**Critical design decision - Absolute vs Relative Paths:**

Standard Delta tables use relative paths:
```json
{"add":{"path":"region=eu/file2.parquet"}}
```

We use absolute paths:
```json
{"add":{"path":"s3://bucket-b/archive/orders/region=eu/file2.parquet"}}
```

This allows Delta to reference files outside the table root, but requires:
- External locations in Unity Catalog for each bucket
- Storage credentials with access to all buckets

**Schema translation:**

Glue uses Hive types, Delta uses Spark SQL types:
- `bigint` → `long`
- `string` → `string`
- `int` → `integer`
- Complex types preserved as-is

### 4. Parallel Execution (`parallel.py`)

Orchestrates concurrent table conversions using ThreadPoolExecutor.

**Key functions:**
- `run_parallel()` - Executes conversion function across multiple tables
- `create_summary()` - Generates human-readable conversion summary

**Design decision - Threads vs Processes:**

Uses threads instead of multiprocessing because:
- Bottleneck is I/O (S3, Glue API calls), not CPU
- Threads share memory for Spark session
- Simpler error handling and result collection
- No serialization overhead

**Concurrency control:**

`max_workers` parameter controls parallelism. Recommended values:
- **4** - Safe default for most use cases
- **8-10** - For high-bandwidth environments
- **1-2** - For rate-limited AWS accounts or development

### 5. External Vacuum (`vacuum.py`)

Cleans up orphaned files at absolute S3 paths that standard VACUUM cannot reach.

**The Problem:**

After UPDATE/DELETE operations on tables with external partitions:
1. Delta writes new files to table root
2. Old files at external paths are marked removed in Delta log
3. Standard VACUUM only scans table root directory
4. External files remain orphaned forever

**The Solution:**

External vacuum:
1. Reads Delta transaction log from S3
2. Identifies remove actions with absolute S3 paths
3. Filters for files outside table root
4. Checks files are past retention period
5. Deletes files via boto3 (with dry-run mode)

**Key functions:**
- `get_delta_log_entries()` - Reads all Delta log JSON files
- `parse_remove_actions()` - Extracts removed file paths
- `parse_add_actions()` - Gets currently active files
- `find_orphaned_external_paths()` - Identifies files safe to delete
- `delete_s3_files()` - Batch deletes via S3 API

**Safety mechanisms:**
- Minimum 7-day retention period enforced
- Dry-run mode by default
- Only vacuums files with absolute paths outside table root
- Skips currently active files
- Batch deletion with error handling

### 6. Data Models (`models.py`)

Dataclasses for type-safe result handling.

**ConversionResult:**
```python
@dataclass
class ConversionResult:
    source_table: str
    target_table: str
    success: bool
    file_count: int = 0
    delta_log_location: str = ""
    error: str | None = None
    duration_seconds: float = 0.0
```

**VacuumResult:**
```python
@dataclass
class VacuumResult:
    table_name: str
    orphaned_files: list[str]
    deleted_files: list[str]
    dry_run: bool
    error: str | None = None
```

## Design Decisions

### 1. Databricks Connect vs REST API

**Choice:** Databricks Connect

**Why:**
- Portable - runs from any Python environment
- Uses Spark SQL for table registration (better compatibility)
- Access to DataFrame API if needed
- Simpler authentication (token-based)

**Trade-offs:**
- Requires cluster/SQL warehouse
- Network latency for API calls
- Heavier dependency than REST API

### 2. Manual Delta Log vs CONVERT TO DELTA

**Choice:** Manual Delta log generation

**Why:**
- Only way to support absolute S3 paths
- Handles cross-bucket/region scenarios
- More control over conversion process
- Doesn't require data scanning or statistics collection

**Trade-offs:**
- More complex implementation
- No automatic statistics (file sizes, record counts)
- Must manage Delta log versioning
- Requires deeper Delta protocol knowledge

### 3. Absolute Paths vs Data Copy

**Choice:** Absolute paths (no data copy)

**Why:**
- Zero data movement cost
- Near-instant migration
- Original data remains in place
- No storage duplication

**Trade-offs:**
- Requires external locations for each bucket
- Read performance may vary across regions
- VACUUM requires custom external vacuum
- More complex Unity Catalog setup

### 4. boto3 Direct vs Spark File APIs

**Choice:** boto3 for S3 operations

**Why:**
- Direct S3 API access with better error handling
- Cross-region support with region-specific clients
- Fine-grained control over pagination
- Standard AWS SDK patterns

**Trade-offs:**
- Requires AWS credentials configuration
- Separate from Spark's credential management
- More dependencies

### 5. Bulk Conversion Pattern

**Choice:** ThreadPoolExecutor with configurable workers

**Why:**
- Simple, standard library solution
- Efficient for I/O-bound operations
- Easy to reason about error handling
- Progress reporting via callbacks

**Trade-offs:**
- GIL limits CPU parallelism (not an issue here)
- All workers share same process memory
- Less isolation than multiprocessing

## Data Flow

### Single Table Conversion

```
1. get_glue_table_metadata()
   ↓ (table location, schema, partition keys)

2. get_glue_partitions()
   ↓ (list of partition locations and values)

3. scan_partition_files()
   ↓ (list of absolute S3 paths to .parquet files)

4. build_delta_schema()
   ↓ (Delta-compatible schema JSON)

5. generate_delta_log()
   ↓ (Delta log JSON with add actions)

6. write_delta_log()
   ↓ (writes to s3://bucket/table/_delta_log/00000000000000000000.json)

7. spark.sql(CREATE TABLE ... USING DELTA LOCATION ...)
   ↓ (registers in Unity Catalog)

8. ConversionResult (success/failure details)
```

### Bulk Parallel Conversion

```
convert_tables(tables=["t1","t2","t3"], max_workers=2)
  ↓
ThreadPoolExecutor.submit()
  ├─→ Thread 1: convert_single_table("t1")
  │     ↓ (completes)
  │   Thread 1: convert_single_table("t3")
  │     ↓ (completes)
  │
  └─→ Thread 2: convert_single_table("t2")
        ↓ (completes)
  ↓
Collect results from all threads
  ↓
[ConversionResult, ConversionResult, ConversionResult]
  ↓
create_summary() → Printed summary
```

### External Vacuum Flow

```
1. spark.sql(DESCRIBE DETAIL table)
   ↓ (get table location)

2. get_delta_log_entries()
   ↓ (read all _delta_log/*.json files)

3. parse_remove_actions() + parse_add_actions()
   ↓ (extract removed and active file paths)

4. find_orphaned_external_paths()
   ↓ (filter for absolute paths outside table root, past retention)

5. delete_s3_files()
   ↓ (batch delete via S3 API, or dry-run)

6. VacuumResult (orphaned files, deleted files)
```

## Delta Protocol Details

### Transaction Log Structure

Delta uses a log-structured approach where each transaction appends a new JSON file:

```
table_root/
├── _delta_log/
│   ├── 00000000000000000000.json  ← Initial conversion
│   ├── 00000000000000000001.json  ← First update
│   ├── 00000000000000000002.json  ← Second update
│   └── 00000000000000000010.checkpoint.parquet
└── <data files>
```

Each JSON file contains newline-delimited actions:
- `commitInfo` - Metadata about the commit
- `metaData` - Table schema and properties (version 0 only)
- `protocol` - Delta protocol version (version 0 only)
- `add` - File added to table
- `remove` - File removed from table

### Add Actions with Absolute Paths

Standard add action (relative path):
```json
{
  "add": {
    "path": "region=us/date=2024-01-01/part-00000.parquet",
    "partitionValues": {"region": "us", "date": "2024-01-01"},
    "size": 1234567,
    "modificationTime": 1234567890000,
    "dataChange": true
  }
}
```

Our add action (absolute path):
```json
{
  "add": {
    "path": "s3://external-bucket/archive/region=eu/date=2024-01-01/part-00000.parquet",
    "partitionValues": {"region": "eu", "date": "2024-01-01"},
    "size": 1234567,
    "modificationTime": 1234567890000,
    "dataChange": true
  }
}
```

Delta Lake resolves absolute paths directly, bypassing the table root location.

### Why Standard VACUUM Fails

Standard VACUUM algorithm:
```python
1. List all files under table_root/
2. Read Delta log to get active files
3. Identify files in filesystem but not in log
4. Delete if past retention period
```

With absolute paths:
- External files (e.g., `s3://bucket-b/file.parquet`) are not under `table_root/`
- VACUUM never sees these files during filesystem scan
- Files remain orphaned even after removal from Delta log

External vacuum solves this by:
1. Reading Delta log directly (not filesystem)
2. Finding remove actions with absolute paths
3. Filtering for paths outside table root
4. Deleting via S3 API

## Unity Catalog Integration

### External Locations Requirement

For each S3 bucket referenced in Delta logs:
```sql
CREATE EXTERNAL LOCATION bucket_a_location
WITH (
  URL = 's3://bucket-a/',
  STORAGE CREDENTIAL my_credential
);

CREATE EXTERNAL LOCATION bucket_b_location
WITH (
  URL = 's3://bucket-b/',
  STORAGE CREDENTIAL my_credential
);
```

Unity Catalog uses these to validate access when querying tables.

### Why External Locations Matter

Without external locations:
```sql
SELECT * FROM main.bronze.orders
-- ERROR: External location not found for s3://bucket-b/
```

With external locations:
```sql
SELECT * FROM main.bronze.orders
-- ✓ Success - reads from both bucket-a and bucket-b
```

### Table Registration

After writing Delta log, register in Unity Catalog:
```sql
CREATE TABLE main.bronze.orders
USING DELTA
LOCATION 's3://bucket-a/tables/orders/'
```

Unity Catalog reads `_delta_log/` to discover:
- Schema
- Partitioning
- File locations (including absolute paths to bucket-b)

## Performance Considerations

### Parallel Conversion

Bottlenecks by operation:
- **Glue API calls** - ~100-200ms per table
- **S3 list operations** - ~50-100ms per partition
- **Delta log writes** - ~50-100ms per table
- **Unity Catalog registration** - ~200-500ms per table

With 4 workers:
- 100 tables takes ~5-10 minutes (vs 20-40 minutes sequential)
- Throughput: ~10-20 tables/minute

### Cross-Region Performance

Reading from different regions incurs:
- **Same region** - 1-5ms latency
- **Cross-region (US)** - 50-100ms latency
- **Cross-region (intercontinental)** - 150-300ms latency

Impact on queries:
- Small queries (few files) - Minimal impact
- Large scans (many files) - 2-3x slower

Mitigation:
- Use `OPTIMIZE` to consolidate files to table root
- Schedule cross-region queries during off-peak hours
- Consider data replication for frequently accessed tables

### Memory Usage

Per-table memory footprint:
- Glue metadata - ~10KB
- Partition list - ~1KB per partition
- File list - ~100 bytes per file
- Delta log generation - ~200 bytes per file

Example: 1000 files, 50 partitions = ~250KB per table

Bulk conversion with 10 workers, 1000 files each = ~2.5MB total (negligible)

## Error Handling

### Conversion Errors

All errors are caught and returned in `ConversionResult`:
```python
ConversionResult(
    source_table="problematic_table",
    target_table="main.bronze.problematic_table",
    success=False,
    error="NoSuchBucket: The specified bucket does not exist",
)
```

This allows bulk conversions to continue even if individual tables fail.

### Vacuum Safety

Multiple safety layers:
1. **Minimum retention** - 7 days enforced (prevents accidental deletion)
2. **Dry-run default** - Must explicitly set `dry_run=False`
3. **Active file check** - Never deletes files still in use
4. **Batch deletion** - Continues on partial failures

### AWS API Errors

Common errors and handling:
- **Throttling** - boto3 retries automatically with exponential backoff
- **Permission denied** - Immediate failure with clear error message
- **Bucket not found** - Caught and returned in result
- **Network errors** - Retried by boto3 (up to 3 attempts)

## Testing Strategy

### Integration Tests

Tests run against real infrastructure:
- Deployed via Terraform
- Uses actual Databricks workspace and S3 buckets
- Validates end-to-end workflows

Test scenarios:
- **Standard layout** - All partitions under table root
- **Cross-bucket** - Partitions in us-east-1a and us-east-1b
- **Cross-region** - Partitions in us-east-1 and us-west-2
- **Non-partitioned** - Single location table

Each scenario validates:
- Conversion success
- File count matches expected
- All Delta operations (SELECT, INSERT, UPDATE, DELETE, OPTIMIZE)
- Standard VACUUM works for table root files
- External vacuum works for external files

### Why Integration Tests Only

Unit tests would require:
- Mocking boto3 (complex, fragile)
- Mocking Spark (very complex)
- Fake Delta log parsing
- Limited value vs integration tests

Integration tests provide:
- True validation against real services
- Confidence in production behavior
- Catch SDK/API changes
- Test cross-service interactions

## Future Enhancements

Potential improvements:

1. **Checkpoint generation** - For tables with many transactions, generate checkpoints to speed up log reads
2. **Statistics collection** - Gather file-level stats (row counts, min/max) for query optimization
3. **Incremental conversion** - Support adding new partitions to already-converted tables
4. **Schema evolution** - Handle schema changes between Glue and Delta
5. **Compression codec detection** - Auto-detect and set proper compression in Delta metadata
6. **Dry-run mode for conversion** - Validate before actually converting
7. **Progress callbacks** - Finer-grained progress reporting during conversion
8. **Retry logic** - Automatic retry for transient failures
9. **Metadata caching** - Cache Glue metadata for repeated conversions

## References

- [Delta Lake Transaction Log Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
- [Unity Catalog External Locations](https://docs.databricks.com/en/connect/unity-catalog/external-locations.html)
- [AWS Glue Data Catalog API](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-tables.html)
- [Databricks Connect](https://docs.databricks.com/en/dev-tools/databricks-connect-ref.html)

## Related Research

See [hive_table_experiments](../hive_table_experiments/README.md) for:
- Initial research validating the approach
- Comparison of CONVERT TO DELTA vs manual log generation
- Performance benchmarks
- VACUUM behavior analysis
