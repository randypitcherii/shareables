# hive_to_delta

Bulk registration of AWS Glue Hive tables as external Delta tables in Unity Catalog.

This package generates Delta transaction logs manually for existing Parquet data, enabling registration in Unity Catalog without data movement or conversion.

## Features

- **Manual Delta log generation** - Creates `_delta_log` directly from Glue metadata and S3 file scans
- **Parallel bulk conversion** - Convert multiple tables concurrently with configurable worker count
- **External vacuum handler** - Cleans up orphaned files at absolute S3 paths that standard VACUUM cannot reach
- **Cross-bucket support** - Handles partitions spread across different S3 buckets and regions

## Installation

```bash
pip install /path/to/databricks/hive_to_delta
```

Or with dev dependencies:

```bash
pip install "/path/to/databricks/hive_to_delta[dev]"
```

## Quick Start

### Convert tables

```python
from hive_to_delta import convert_tables

results = convert_tables(
    spark=spark,
    glue_database="my_glue_db",
    tables=["orders", "customers"],  # or use glob pattern: "dim_*"
    target_catalog="main",
    target_schema="bronze",
    aws_region="us-east-1",
    max_workers=4,
)

for r in results:
    if r.success:
        print(f"{r.source_table}: {r.file_count} files in {r.duration_seconds:.1f}s")
    else:
        print(f"{r.source_table}: FAILED - {r.error}")
```

### Vacuum external files

```python
from hive_to_delta import vacuum_external_files

result = vacuum_external_files(
    spark=spark,
    table_name="main.bronze.orders",
    retention_hours=168,  # 7 days
    dry_run=True,  # set False to delete
    region="us-east-1",
)

print(f"Found {len(result.orphaned_files)} orphaned files")
```

## API Reference

### convert_tables

```python
def convert_tables(
    spark,
    glue_database: str,
    tables: list[str] | str,
    target_catalog: str,
    target_schema: str,
    aws_region: str = "us-east-1",
    max_workers: int = 4,
) -> list[ConversionResult]
```

| Parameter | Description |
|-----------|-------------|
| `spark` | Active SparkSession with Unity Catalog access |
| `glue_database` | AWS Glue database containing source tables |
| `tables` | List of table names or glob pattern (e.g., `"dim_*"`, `"*_fact"`) |
| `target_catalog` | Unity Catalog catalog name |
| `target_schema` | Unity Catalog schema name |
| `aws_region` | AWS region for Glue/S3 operations |
| `max_workers` | Maximum concurrent conversion threads |

### vacuum_external_files

```python
def vacuum_external_files(
    spark,
    table_name: str,
    retention_hours: float = 168,
    dry_run: bool = True,
    region: str = "us-east-1",
) -> VacuumResult
```

| Parameter | Description |
|-----------|-------------|
| `spark` | SparkSession (used to get table location) |
| `table_name` | Fully qualified table name (`catalog.schema.table`) |
| `retention_hours` | Minimum file age before deletion (default: 7 days) |
| `dry_run` | If `True`, only identify files without deleting |
| `region` | AWS region for S3 operations |

### list_glue_tables

```python
def list_glue_tables(
    glue_database: str,
    pattern: str | None = None,
    region: str = "us-east-1",
) -> list[str]
```

| Parameter | Description |
|-----------|-------------|
| `glue_database` | AWS Glue database to list tables from |
| `pattern` | Optional glob pattern (`*`, `?`, `[seq]`, `[!seq]`) |
| `region` | AWS region for Glue API |

## Requirements

- **Python** >= 3.10
- **Databricks Connect** >= 15.1.0
- **boto3** >= 1.35.0
- **AWS credentials** configured (via environment, profile, or IAM role)
- **Unity Catalog** access with permissions to create tables
- **S3 access** to source data locations

## Testing

Tests require Databricks Connect and AWS infrastructure.

### Environment variables

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"
export DATABRICKS_CLUSTER_ID="your-cluster-id"

export AWS_REGION="us-east-1"
export HIVE_TO_DELTA_TEST_GLUE_DATABASE="hive_to_delta_test"
export HIVE_TO_DELTA_TEST_CATALOG="main"
export HIVE_TO_DELTA_TEST_SCHEMA="hive_to_delta_tests"
```

### Terraform infrastructure

Deploy test infrastructure (S3 buckets, Glue database, test tables):

```bash
cd terraform
terraform init
terraform apply
```

### Run tests

```bash
# Install dev dependencies
pip install ".[dev]"

# Run all tests
pytest

# Run specific test categories
pytest -m standard          # Standard partition layout tests
pytest -m cross_bucket      # Cross-bucket partition tests
pytest -m cross_region      # Cross-region partition tests

# Parallel execution
pytest -n 4
```
