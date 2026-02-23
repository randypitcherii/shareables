# Composable Hive-to-Delta Pipeline

Implemented in PR #16. The pipeline decomposes into two pluggable axes:

1. **Discovery** -- where table metadata (name, location, schema, partition keys) comes from
2. **Listing** -- how parquet files are enumerated for a given table

The downstream pipeline (schema inference, Delta log generation, UC registration) is shared regardless of source.

## Two-Tier API

**Tier 1: `convert_table()`** -- single-table, manual entry point.

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

**Tier 2: `convert()`** -- bulk, composable strategies.

```python
from hive_to_delta import convert
from hive_to_delta.discovery import GlueDiscovery, UCDiscovery
from hive_to_delta.listing import S3Listing, InventoryListing

# Glue + boto3
results = convert(
    spark=spark,
    discovery=GlueDiscovery(database="my_db", pattern="dim_*", region="us-east-1"),
    listing=S3Listing(region="us-east-1", glue_database="my_db"),
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

## Strategy Protocols

```python
class Discovery(Protocol):
    def discover(self, spark) -> list[TableInfo]: ...

class Listing(Protocol):
    def list_files(self, spark, table: TableInfo) -> list[ParquetFileInfo]: ...
```

## Discovery Implementations

**GlueDiscovery** -- wraps `glue.py` functions:
- `database: str`, `pattern: str | None`, `region: str`
- Returns `TableInfo` with `columns` populated (Glue-based schema inference)
- Partition keys extracted from Glue table metadata

**UCDiscovery** -- queries Spark SQL catalog:
- `allow: list[str]` -- FQN glob patterns (e.g., `["hive_metastore.my_db.*"]`)
- `deny: list[str]` -- defaults to `["*.information_schema.*"]`
- Uses `SHOW CATALOGS`, `SHOW SCHEMAS IN`, `SHOW TABLES IN` for enumeration
- Uses `spark.catalog.listColumns()` for partition key detection
- Table location from `DESCRIBE TABLE EXTENDED`
- Returns `TableInfo` with `columns=None` (schema inferred from parquet at conversion time)

## Listing Implementations

**S3Listing** -- boto3-based file scanning:
- `region: str`, `glue_database: str | None`
- For partitioned tables with a `glue_database`, fetches partition locations from Glue
- Otherwise scans the table root location
- Handles cross-bucket/cross-region partitions via `scan_partition_files()`

**InventoryListing** -- user-provided DataFrame:
- `files_df: DataFrame` -- must have `file_path` (string) and `size` (long) columns
- Validates schema on construction
- Filters files by table location prefix
- Parses `key=value/` segments from file paths for partition values

## Schema Inference

Handled in `schema.py`. Two paths based on `TableInfo.columns`:

**Path A -- Glue columns available** (`columns is not None`):
- `build_delta_schema_from_glue()` with `GLUE_TO_DELTA_TYPE_MAP`
- Complex types (containing `<`) parsed by a recursive descent parser (`_parse_hive_type()`) into Delta protocol JSON objects

**Path B -- No columns** (`columns is None`):
- Reads one parquet file via `spark.read.parquet(files[0].path).schema`
- `build_delta_schema_from_spark()` uses `jsonValue()` on each field's dataType
- Partition columns not present in parquet files are added as `string` type

Both paths produce `{"type": "struct", "fields": [...]}` consumed by `generate_delta_log()`.

## Internal Pipeline

Both `convert_table()` and `convert()` feed into `_convert_one_table()`:

```
_convert_one_table(spark, table_info, files, target_catalog, target_schema, region)
    1. Delete any existing _delta_log/ at the table location
    2. Infer schema (Path A or Path B)
    3. generate_delta_log(files, schema, partition_columns, table_location)
    4. write_delta_log(delta_log_content, table_location, region)
    5. CREATE SCHEMA IF NOT EXISTS
    6. DROP TABLE IF EXISTS + CREATE TABLE ... USING DELTA LOCATION ...
    7. Return ConversionResult
```

## Module Structure

```
hive_to_delta/
├── __init__.py      # Public API exports
├── converter.py     # convert_table(), convert(), _convert_one_table(), legacy wrappers
├── discovery.py     # Discovery protocol, GlueDiscovery, UCDiscovery
├── listing.py       # Listing protocol, S3Listing, InventoryListing
├── schema.py        # build_delta_schema_from_glue(), build_delta_schema_from_spark()
├── delta_log.py     # generate_delta_log(), write_delta_log(), build_add_action()
├── models.py        # TableInfo, ParquetFileInfo, ConversionResult, VacuumResult
├── glue.py          # Low-level Glue API: list_glue_tables(), get_glue_table_metadata()
├── s3.py            # Low-level S3: parse_s3_path(), write_to_s3(), scan_partition_files()
├── parallel.py      # run_parallel(), ConversionSummary, create_summary()
├── vacuum.py        # vacuum_external_files()
```

## Future Work

- **DatabricksListing** -- file listing via Databricks-native auth (no AWS creds). Candidates: `dbutils.fs.ls()`, Spark filesystem APIs.
- **Parallelism** -- evaluate Spark-native parallelism vs current ThreadPoolExecutor for large-scale `convert()` runs.
