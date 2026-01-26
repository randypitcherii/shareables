# hive_to_delta

Bulk registration of AWS Glue Hive tables as external Delta tables in Unity Catalog.

This package generates Delta transaction logs manually for existing Parquet data, enabling registration in Unity Catalog without data movement or conversion.

## Overview

When migrating from AWS Glue to Databricks Unity Catalog, the standard `CONVERT TO DELTA` command has limitations:
- Only scans files under the table root path
- Silently ignores partitions at external S3 locations
- Cannot handle partitions spread across buckets or regions

`hive_to_delta` solves these problems by reading Glue metadata and generating Delta transaction logs with absolute S3 paths, allowing you to register tables in Unity Catalog without moving or copying data.

## Use Case

This package is ideal for:
- **Glue to Unity Catalog migrations** where data must remain in place
- **Cross-bucket Hive tables** with partitions in different S3 buckets
- **Cross-region scenarios** where partitions span multiple AWS regions
- **Exotic partition layouts** with external or scattered partition paths
- **Large-scale migrations** requiring bulk conversion of many tables

## Key Features

- **Manual Delta log generation** - Creates `_delta_log` directly from Glue metadata and S3 file scans
- **Parallel bulk conversion** - Convert multiple tables concurrently with configurable worker count
- **Pattern matching** - Use glob patterns to select tables for conversion (e.g., `"dim_*"`, `"*_fact"`)
- **External vacuum handler** - Cleans up orphaned files at absolute S3 paths that standard VACUUM cannot reach
- **Cross-bucket/region support** - Handles partitions spread across different S3 buckets and regions
- **No data movement** - Data stays in original location; only metadata is migrated
- **Progress reporting** - Real-time feedback during bulk conversions with summary statistics

## Installation

Install from the package directory:

```bash
pip install /path/to/databricks/hive_to_delta
```

Or with dev dependencies for testing:

```bash
pip install "/path/to/databricks/hive_to_delta[dev]"
```

## Quick Start

### Prerequisites

Before using this package, ensure you have:

1. **Databricks workspace** with Unity Catalog enabled
2. **AWS Glue database** with Hive tables pointing to Parquet data in S3
3. **AWS credentials** configured (environment variables, profile, or IAM role)
4. **Unity Catalog external locations** created for each S3 bucket containing data
5. **Databricks Connect** or notebook environment with Spark access

### Basic Conversion Example

Convert specific tables from Glue to Unity Catalog:

```python
from databricks.connect import DatabricksSession
from hive_to_delta import convert_tables

# Create Spark session (Databricks Connect or notebook)
spark = DatabricksSession.builder.getOrCreate()

# Convert specific tables
results = convert_tables(
    spark=spark,
    glue_database="my_glue_db",
    tables=["orders", "customers", "products"],
    target_catalog="main",
    target_schema="bronze",
    aws_region="us-east-1",
    max_workers=4,
)

# Check results
for r in results:
    if r.success:
        print(f"✓ {r.source_table}: {r.file_count} files in {r.duration_seconds:.1f}s")
    else:
        print(f"✗ {r.source_table}: {r.error}")
```

### Bulk Conversion with Pattern Matching

Convert all tables matching a glob pattern:

```python
from hive_to_delta import convert_tables

# Convert all dimension tables
results = convert_tables(
    spark=spark,
    glue_database="analytics",
    tables="dim_*",  # Pattern: dim_customer, dim_product, etc.
    target_catalog="main",
    target_schema="dimensions",
    aws_region="us-east-1",
    max_workers=8,  # More workers for parallel processing
)

# Summary is printed automatically
# Or access summary programmatically:
from hive_to_delta import create_summary
summary = create_summary(results)
print(summary)
```

### External Vacuum for Cross-Bucket Tables

After performing UPDATE/DELETE operations on tables with cross-bucket partitions, use external vacuum to clean up orphaned files:

```python
from hive_to_delta import vacuum_external_files

# Dry run first to see what would be deleted
result = vacuum_external_files(
    spark=spark,
    table_name="main.bronze.orders",
    retention_hours=168,  # 7 days minimum
    dry_run=True,
    region="us-east-1",
)

print(f"Found {len(result.orphaned_files)} orphaned files")
for file in result.orphaned_files[:5]:  # Show first 5
    print(f"  - {file}")

# Actually delete files (be careful!)
if len(result.orphaned_files) > 0:
    result = vacuum_external_files(
        spark=spark,
        table_name="main.bronze.orders",
        retention_hours=168,
        dry_run=False,  # Actually delete
        region="us-east-1",
    )
    print(f"Deleted {len(result.deleted_files)} files")
```

### Advanced Configuration

```python
from hive_to_delta import convert_single_table, list_glue_tables

# List available tables with pattern
available_tables = list_glue_tables(
    glue_database="my_database",
    pattern="fact_*",
    region="us-east-1",
)
print(f"Found {len(available_tables)} fact tables")

# Convert single table with custom name
result = convert_single_table(
    spark=spark,
    glue_database="legacy_db",
    table_name="old_orders_table",
    target_catalog="main",
    target_schema="bronze",
    target_table_name="orders",  # Rename during conversion
    aws_region="us-east-1",
)

if result.success:
    print(f"Table registered at: {result.target_table}")
    print(f"Delta log created at: {result.delta_log_location}")
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

## How It Works

1. **Fetch metadata** from AWS Glue Data Catalog (table schema, partition keys, locations)
2. **Scan S3** for Parquet files in each partition across all buckets/regions
3. **Generate Delta log** with absolute S3 paths for each file
4. **Write Delta log** to `_delta_log/` in the table root directory
5. **Register table** in Unity Catalog using `CREATE TABLE ... USING DELTA LOCATION`

The key difference from `CONVERT TO DELTA` is that this approach uses absolute paths in the Delta transaction log, allowing files to be referenced across multiple S3 buckets and regions.

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design decisions and internal implementation.

## Requirements

### Runtime Requirements

- **Python** >= 3.10
- **Databricks Connect** >= 15.1.0 (or Databricks notebook environment)
- **boto3** >= 1.35.0

### AWS Requirements

- **AWS credentials** configured (via environment, profile, or IAM role)
- **IAM permissions** for:
  - `glue:GetTable`, `glue:GetPartitions`, `glue:GetTables` on source database
  - `s3:ListBucket`, `s3:GetObject` on source data buckets
  - `s3:PutObject` on table root for writing Delta logs

### Databricks Requirements

- **Unity Catalog** enabled in your workspace
- **External locations** created for each S3 bucket containing source data
- **Storage credential** with IAM role that has access to all source buckets
- **Schema permissions** to create tables in target catalog/schema

## Infrastructure Deployment

For production use or testing, you need to deploy supporting AWS infrastructure.

See [terraform/README.md](terraform/README.md) for detailed deployment instructions.

### Quick Terraform Deployment

```bash
cd terraform

# Configure variables
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your values

# Deploy infrastructure
terraform init
terraform apply

# Get outputs for Databricks configuration
terraform output role_arn
terraform output bucket_east_1a_uri
```

This creates:
- S3 buckets for testing (multi-region setup)
- IAM role for Unity Catalog storage credential
- Glue database and test tables
- IAM user for local development

## Testing

Tests require Databricks Connect, AWS infrastructure, and appropriate permissions.

### 1. Deploy Test Infrastructure

```bash
cd terraform
terraform init
terraform apply
```

### 2. Configure Environment

Set required environment variables:

```bash
# Databricks configuration
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"
export DATABRICKS_CLUSTER_ID="your-cluster-id"

# AWS configuration
export AWS_REGION="us-east-1"

# Test configuration
export HIVE_TO_DELTA_TEST_GLUE_DATABASE="hive_to_delta_test"
export HIVE_TO_DELTA_TEST_CATALOG="main"
export HIVE_TO_DELTA_TEST_SCHEMA="hive_to_delta_tests"
```

### 3. Create Unity Catalog Resources

In Databricks, create external locations for the test buckets:

```sql
-- Create storage credential (use role_arn from terraform output)
CREATE STORAGE CREDENTIAL htd_test_cred
WITH (AWS_IAM_ROLE = 'arn:aws:iam::ACCOUNT:role/htd-test-role');

-- Create external locations for each bucket
CREATE EXTERNAL LOCATION htd_east_1a
WITH (URL = 's3://htd-test-east-1a/', STORAGE_CREDENTIAL htd_test_cred);

CREATE EXTERNAL LOCATION htd_east_1b
WITH (URL = 's3://htd-test-east-1b/', STORAGE_CREDENTIAL htd_test_cred);

CREATE EXTERNAL LOCATION htd_west_2
WITH (URL = 's3://htd-test-west-2/', STORAGE_CREDENTIAL htd_test_cred);
```

### 4. Run Tests

```bash
# Install dev dependencies
pip install ".[dev]"

# Run all tests
pytest

# Run specific test categories
pytest -m standard          # Standard partition layout tests
pytest -m cross_bucket      # Cross-bucket partition tests
pytest -m cross_region      # Cross-region partition tests

# Run with verbose output
pytest -v

# Run tests in parallel
pytest -n 4

# Run specific test file
pytest tests/test_convert.py
```

### Test Scenarios

The test suite validates:
- **Standard partitions** - All partitions under table root
- **Cross-bucket partitions** - Partitions in different S3 buckets (same region)
- **Cross-region partitions** - Partitions in different AWS regions
- **Non-partitioned tables** - Single location without partitioning
- **Delta operations** - SELECT, INSERT, UPDATE, DELETE, OPTIMIZE
- **Vacuum operations** - Standard VACUUM and external vacuum

## Troubleshooting

### "No parquet files found in table or partitions"

**Cause:** The S3 locations don't contain `.parquet` files, or AWS credentials lack read permissions.

**Solutions:**
- Verify S3 paths in Glue contain `.parquet` files (not `.snappy.parquet` or other extensions)
- Check AWS credentials have `s3:ListBucket` and `s3:GetObject` permissions
- Confirm table isn't empty in Glue

### "Failed to get table location" when vacuuming

**Cause:** Table doesn't exist in Unity Catalog or Spark session lacks access.

**Solutions:**
- Verify table name is fully qualified (`catalog.schema.table`)
- Confirm table exists: `spark.sql("SHOW TABLES IN catalog.schema")`
- Check Spark session has read permissions on the table

### "Permission denied" writing Delta log

**Cause:** AWS credentials lack write permissions to table root location.

**Solutions:**
- Ensure AWS credentials have `s3:PutObject` permission on table root bucket
- For IAM role-based access, verify trust policy allows Databricks to assume the role
- Check bucket policies don't block write access

### "External location not found" when querying converted table

**Cause:** Unity Catalog doesn't have external locations configured for S3 buckets containing data.

**Solutions:**
- Create external locations for each bucket: `CREATE EXTERNAL LOCATION ... URL 's3://bucket/'`
- Verify storage credential has access to all buckets
- Check external location permissions: `SHOW GRANT ON EXTERNAL LOCATION location_name`

### Conversion succeeds but row count is wrong

**Cause:** Some partitions weren't scanned, or files are corrupt.

**Solutions:**
- Check for partitions at external paths not covered by external locations
- Verify all partition locations are accessible
- Compare file count in conversion result vs expected number
- Run `DESCRIBE DETAIL table_name` and check `numFiles`

### VACUUM doesn't clean up old files

**Cause:** Standard VACUUM only cleans files under the table root. Files at absolute external paths require external vacuum.

**Solution:**
```python
from hive_to_delta import vacuum_external_files

result = vacuum_external_files(
    spark=spark,
    table_name="catalog.schema.table",
    retention_hours=168,
    dry_run=True,  # Test first
)
```

### Parallel conversion fails with "Too many open files"

**Cause:** High `max_workers` opens too many S3 connections simultaneously.

**Solutions:**
- Reduce `max_workers` (try 4 or fewer)
- Increase system file descriptor limit: `ulimit -n 4096`

### Cross-region queries are slow

**Cause:** Data is in different AWS regions from the Databricks workspace.

**Solutions:**
- This is expected behavior due to cross-region data transfer
- Consider using `OPTIMIZE` to consolidate files in table root (same region as workspace)
- For read-heavy tables, consider copying data to workspace region

## Common Patterns

### Incremental Migration

Migrate tables in batches to manage risk:

```python
# Batch 1: Small, non-critical tables
batch1 = ["table1", "table2", "table3"]
results = convert_tables(spark, "glue_db", batch1, "main", "bronze")

# Verify batch 1 before proceeding
# Then batch 2, batch 3, etc.
```

### Migration with Validation

Validate row counts after conversion:

```python
from hive_to_delta import convert_single_table

result = convert_single_table(
    spark, "glue_db", "orders", "main", "bronze"
)

if result.success:
    # Get row count from Unity Catalog
    uc_count = spark.sql(f"SELECT COUNT(*) FROM {result.target_table}").collect()[0][0]

    # Compare with Glue source (if accessible)
    # glue_count = spark.sql("SELECT COUNT(*) FROM glue_catalog.glue_db.orders").collect()[0][0]

    print(f"UC table has {uc_count} rows")
```

### Post-Migration Cleanup

After successful migration, clean up Glue tables:

```python
import boto3

glue = boto3.client("glue", region_name="us-east-1")

# Only delete after confirming Unity Catalog tables work correctly!
# glue.delete_table(DatabaseName="glue_db", Name="orders")
```

## Contributing

Contributions are welcome. Please follow these guidelines:

### Development Setup

```bash
# Clone repository
git clone <repo-url>
cd databricks/hive_to_delta

# Install in editable mode with dev dependencies
pip install -e ".[dev]"

# Run linter
ruff check .

# Format code
ruff format .
```

### Running Tests

```bash
# Deploy test infrastructure first (one-time)
cd terraform && terraform apply && cd ..

# Set environment variables (see Testing section)

# Run tests
pytest -v

# Run specific test
pytest tests/test_convert.py::test_convert_standard_table -v
```

### Code Style

- Follow PEP 8 style guidelines
- Use type hints for function parameters and returns
- Add docstrings to all public functions
- Keep functions focused and single-purpose
- Use descriptive variable names

### Pull Request Process

1. Create a feature branch from `main`
2. Make your changes with clear, focused commits
3. Add or update tests for new functionality
4. Ensure all tests pass
5. Update documentation (README, docstrings, ARCHITECTURE.md if relevant)
6. Submit pull request with description of changes

### Reporting Issues

When reporting issues, include:
- Python version and package version
- Databricks Connect version
- Error message and full stack trace
- Minimal reproducible example
- Expected vs actual behavior

## License

This project is provided as-is for use with Databricks and AWS services.

## Related Projects

- [hive_table_experiments](../hive_table_experiments/) - Research and validation of Hive-to-Delta migration approaches
