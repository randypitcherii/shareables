# Composable Hive-to-Delta Pipeline

## Problem

The library currently hardcodes a Glue → S3 API → Delta pipeline. Users with S3 Inventory data, Databricks-native auth, or tables discoverable via Unity Catalog / hive_metastore can't use it without going through Glue and boto3.

## Design

Decompose the pipeline into two pluggable axes:

1. **Discovery** — where table metadata (name, location, schema, partition keys) comes from
2. **Listing** — how parquet files are enumerated for a given table

The downstream pipeline (schema inference → Delta log generation → UC registration) stays the same regardless of source.

### Two-Tier API

**Tier 1: `convert_table()`** — simple, manual, single-table. The bite-sized entry point.

```python
from hive_to_delta import convert_table

result = convert_table(
    spark=spark,
    files_df=inventory_df,               # DataFrame: file_path (string), size (long)
    table_location="s3://bucket/events",
    target_catalog="analytics",
    target_schema="bronze",
    target_table="events",
    partition_columns=["year", "month"],  # optional
)
```

**Tier 2: `convert()`** — automated, bulk, composable strategies.

```python
from hive_to_delta import convert
from hive_to_delta.discovery import GlueDiscovery, UCDiscovery
from hive_to_delta.listing import S3Listing, DatabricksListing, InventoryListing

# Glue + boto3 (current behavior, new API)
results = convert(
    spark=spark,
    discovery=GlueDiscovery(database="my_db", pattern="dim_*", region="us-east-1"),
    listing=S3Listing(region="us-east-1"),
    target_catalog="analytics",
    target_schema="bronze",
)

# UC discovery + Databricks-native listing (no AWS creds)
results = convert(
    spark=spark,
    discovery=UCDiscovery(allow=["hive_metastore.my_db.*"]),
    listing=DatabricksListing(),
    target_catalog="analytics",
    target_schema="bronze",
)

# UC discovery + S3 Inventory
results = convert(
    spark=spark,
    discovery=UCDiscovery(allow=["hive_metastore.analytics.*"]),
    listing=InventoryListing(files_df=inventory_df),
    target_catalog="analytics",
    target_schema="bronze",
)
```

### Strategy Protocols

```python
from typing import Protocol

class Discovery(Protocol):
    def discover(self, spark) -> list[TableInfo]:
        """Return tables to convert with metadata."""
        ...

class Listing(Protocol):
    def list_files(self, spark, table: TableInfo) -> list[ParquetFileInfo]:
        """Return parquet files for one table."""
        ...
```

### Discovery Implementations

**GlueDiscovery** — wraps existing glue.py logic:
- `database: str` — Glue database name
- `pattern: str | None` — glob pattern for table filtering
- `region: str` — AWS region
- Returns `TableInfo` with columns populated (enables Glue-based schema inference)
- Partition keys come from Glue table metadata

**UCDiscovery** — queries Spark catalog:
- `allow: list[str]` — required FQN patterns (e.g., `["hive_metastore.my_db.*"]`)
- `deny: list[str]` — optional, defaults to `["*.information_schema.*"]`
- Uses `spark.catalog.listTables()`, `spark.catalog.listColumns()` for partition detection
- Returns `TableInfo` with partition_keys populated, columns=None (use Spark schema inference)
- Table location from `DESCRIBE TABLE EXTENDED`

### Listing Implementations

**S3Listing** — wraps existing s3.py logic:
- `region: str` — AWS region for boto3 client
- Handles cross-bucket/cross-region partitions via `scan_partition_files()`
- Returns `ParquetFileInfo` with partition_values from directory structure

**DatabricksListing** — uses Databricks auth (research needed for best approach):
- No AWS creds required — uses cluster/warehouse auth
- Candidate approaches: `dbutils.fs.ls()`, Spark filesystem APIs
- Returns `ParquetFileInfo` same as S3Listing

**InventoryListing** — user-provided DataFrame:
- `files_df: DataFrame` — must have `file_path` (string) and `size` (long) columns
- Validates schema on construction
- If the table has partition_keys, parses `key=value/` segments from file paths
- Returns `ParquetFileInfo` with partition_values populated (or empty if no partition_keys)

### Data Models

```python
@dataclass
class TableInfo:
    name: str                                      # Table name
    location: str                                  # S3 root path
    target_table_name: str | None = None           # Override for UC table name
    columns: list[dict[str, str]] | None = None    # Glue-style columns (if available)
    partition_keys: list[str] = field(default_factory=list)
```

`ParquetFileInfo` and `ConversionResult` are unchanged.

### Schema Inference (two paths)

**Path A — Glue columns available** (`TableInfo.columns is not None`):
- Use existing `build_delta_schema()` with `GLUE_TO_DELTA_TYPE_MAP`

**Path B — No columns** (`TableInfo.columns is None`):
- Read one parquet file: `spark.read.parquet(files[0].path).schema`
- New function `build_delta_schema_from_spark(spark_schema: StructType) -> dict`:
  - `StructField.dataType.simpleString()` produces Delta-compatible type strings
  - Wrap in same `{"type": "struct", "fields": [...]}` format

Both paths produce identical output format consumed by `generate_delta_log()`.

### Internal Pipeline

Both `convert_table()` and `convert()` feed into a shared internal function:

```
_convert_one_table(spark, table_info, files, target_catalog, target_schema, region) -> ConversionResult
    1. Infer schema (Path A or Path B based on table_info.columns)
    2. generate_delta_log(files, schema, partition_columns, table_location)
    3. write_delta_log(delta_log_content, table_location, region)
    4. CREATE SCHEMA IF NOT EXISTS target_catalog.target_schema
    5. CREATE TABLE target_catalog.target_schema.table_name USING DELTA LOCATION ...
    6. Return ConversionResult
```

`convert_table()` is a thin wrapper:
1. Validate files_df has required columns
2. Collect DataFrame → build `ParquetFileInfo` list (parsing partition values from paths if `partition_columns` provided)
3. Build `TableInfo` from explicit parameters
4. Call `_convert_one_table()`

`convert()` orchestrates bulk:
1. `discovery.discover(spark)` → list of `TableInfo`
2. For each table: `listing.list_files(spark, table)` → list of `ParquetFileInfo`
3. Parallel execution of `_convert_one_table()` for each (table, files) pair
4. Return list of `ConversionResult`

### Module Structure (after refactor)

```
hive_to_delta/
├── __init__.py          # Public API: convert_table, convert, strategy classes
├── converter.py         # Orchestration: convert_table(), convert(), _convert_one_table()
├── discovery.py         # GlueDiscovery, UCDiscovery, Discovery protocol
├── listing.py           # S3Listing, DatabricksListing, InventoryListing, Listing protocol
├── schema.py            # build_delta_schema_from_glue(), build_delta_schema_from_spark()
├── delta_log.py         # generate_delta_log(), write_delta_log(), build_add_action()
├── models.py            # ParquetFileInfo, ConversionResult, VacuumResult, TableInfo
├── parallel.py          # run_parallel(), ConversionSummary, create_summary()
├── s3.py                # Low-level: parse_s3_path(), write_to_s3()
├── vacuum.py            # vacuum_external_files() (unchanged)
```

Changes from current:
- `glue.py` → absorbed into `discovery.py` (GlueDiscovery wraps existing functions)
- S3 listing logic from `s3.py` → moves to `listing.py` (S3Listing)
- `s3.py` retains low-level utilities (parse_s3_path, write_to_s3)
- Schema logic from `delta_log.py` → moves to `schema.py`
- `delta_log.py` retains log generation and writing
- `TableInfo` added to `models.py`

### Partition Handling for Manual Path

When `convert_table()` is called with `partition_columns`:
- Parse `key=value/` segments from each file's `file_path`
- Populate `ParquetFileInfo.partition_values` accordingly
- These flow through to the Delta log as partition metadata

When called without `partition_columns`:
- All files get `partition_values={}`
- Table registered as non-partitioned
- Data still readable (Spark reads parquet content regardless of directory structure)
- **Research needed**: correctness when partition columns exist only in directory paths, not in parquet files

### What's NOT Changing

- `delta_log.py` core: `generate_delta_log()`, `write_delta_log()`, `build_add_action()` — unchanged
- `parallel.py` — unchanged (run_parallel, ConversionSummary)
- `vacuum.py` — unchanged
- `models.py` — ParquetFileInfo, ConversionResult, VacuumResult unchanged (TableInfo added)
- Cross-bucket/cross-region support — preserved (absolute paths in add actions)

### Research Beads (not blocking implementation)

1. **Partition handling trade-offs** — what happens when files in key=value/ dirs are registered without partition metadata? Correctness when partition columns only in paths?
2. **DatabricksListing implementation** — dbutils.fs.ls vs Spark filesystem APIs vs other options. Parallelization approach.
3. **Parallelism for convert()** — current ThreadPoolExecutor adequate? Spark-native alternatives?

### Testing Strategy

- Unit tests for each strategy (mock Glue/S3/Spark as needed)
- Unit tests for `build_delta_schema_from_spark()`
- Unit tests for `convert_table()` with mocked internals
- Integration tests for `convert_table()` with real Spark + S3
- Existing integration tests should continue passing after refactor (convert_single_table wraps new internals or tests updated to new API)
