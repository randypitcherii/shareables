# hive_to_delta

Register AWS Glue Hive tables as external Delta tables in Databricks Unity Catalog—without moving data.

**Quick Links:**
- [Quick Start Guide](QUICKSTART.md) - Get running in 15 minutes
- [Migration Checklist](MIGRATION_CHECKLIST.md) - Production migration guide
- [Configuration Guide](#configuration-guide) - Detailed setup instructions
- [API Reference](#api-reference) - Function documentation
- [Composable API](#composable-api) - Pluggable discovery + listing strategies
- [Troubleshooting](#troubleshooting) - Common errors and fixes
- [Architecture](ARCHITECTURE.md) - How it works internally

**Table of Contents:**
- [Why This Package?](#why-this-package)
- [When to Use This](#when-to-use-this)
- [Installation](#installation)
- [Configuration Guide](#configuration-guide)
- [Quick Start](#quick-start)
- [Composable API](#composable-api)
- [API Reference](#api-reference)
- [Usage Scenarios](#usage-scenarios)
- [How It Works](#how-it-works)
- [Troubleshooting](#troubleshooting)
- [Testing](#testing)
- [Limitations and Best Practices](#limitations-and-best-practices)

## Why This Package?

Databricks' `CONVERT TO DELTA` fails when partitions span multiple S3 buckets or AWS regions. This package solves that by:

- Generating Delta transaction logs from Glue metadata + S3 file scans
- Writing absolute S3 paths that work across buckets and regions
- Registering tables in Unity Catalog as external Delta tables

No data movement. Metadata only.

## When to Use This

Use `hive_to_delta` when:
- Your Hive table partitions span multiple S3 buckets or regions
- `CONVERT TO DELTA` silently loses partition data
- You want zero-copy migration from Glue to Unity Catalog
- You need to preserve existing S3 locations without data movement

Don't use this if:
- All partitions are under a single S3 location (use standard `CONVERT TO DELTA`)
- You're migrating from a non-Glue catalog
- You need to reorganize data during migration

## Installation

```bash
pip install "hive_to_delta @ git+https://github.com/randypitcherii/shareables.git#subdirectory=databricks/hive_to_delta"

# With development dependencies
pip install "hive_to_delta[dev] @ git+https://github.com/randypitcherii/shareables.git#subdirectory=databricks/hive_to_delta"
```

**Requirements:**
- Python >= 3.10
- Databricks workspace with Unity Catalog enabled
- AWS credentials with Glue and S3 access
- SQL Warehouse or Databricks Connect cluster

## Quick Start

### Simple API - Single Table from File List

```python
from databricks.connect import DatabricksSession
from hive_to_delta import convert_table

spark = DatabricksSession.builder.getOrCreate()

# You provide the file list as a Spark DataFrame
files_df = spark.read.format("csv").load("s3://my-inventory/files.csv")
# DataFrame must have columns: file_path (string), size (long)

result = convert_table(
    spark=spark,
    files_df=files_df,
    table_location="s3://my-bucket/my-table/",
    target_catalog="main",
    target_schema="bronze",
    target_table="my_table",
    partition_columns=["year", "month"],
    aws_region="us-east-1",
)
```

### Composable API - Pluggable Discovery + Listing

```python
from hive_to_delta import convert, GlueDiscovery, S3Listing

# Choose your strategies
discovery = GlueDiscovery(database="my_glue_db", pattern="dim_*", region="us-east-1")
listing = S3Listing(region="us-east-1", glue_database="my_glue_db")

# Convert all matching tables
results = convert(
    spark=spark,
    discovery=discovery,
    listing=listing,
    target_catalog="main",
    target_schema="bronze",
    max_workers=4,
)
```

### Using InventoryListing for Pre-Built File Lists

```python
from hive_to_delta import convert, GlueDiscovery, InventoryListing

# Load S3 inventory or pre-built file DataFrame
files_df = spark.read.parquet("s3://my-bucket/inventory/")

# InventoryListing wraps a DataFrame with file_path + size columns
listing = InventoryListing(files_df)
discovery = GlueDiscovery(database="my_glue_db", pattern="*", region="us-east-1")

results = convert(
    spark=spark,
    discovery=discovery,
    listing=listing,
    target_catalog="main",
    target_schema="bronze",
)
```

### Legacy API - Single Table

```python
from databricks.connect import DatabricksSession
from hive_to_delta import convert_single_table

# Create Spark session
spark = DatabricksSession.builder.getOrCreate()

# Convert one table
result = convert_single_table(
    spark=spark,
    glue_database="my_glue_db",
    table_name="orders",
    target_catalog="main",
    target_schema="bronze",
    aws_region="us-east-1",
)

if result.success:
    print(f"Converted {result.file_count} files")
    print(f"Table: {result.target_table}")
else:
    print(f"Failed: {result.error}")
```

### Bulk Conversion - Multiple Tables

```python
from databricks.connect import DatabricksSession
from hive_to_delta import convert_tables, create_summary

spark = DatabricksSession.builder.getOrCreate()

# Convert multiple tables in parallel
results = convert_tables(
    spark=spark,
    glue_database="my_glue_db",
    tables=["orders", "customers", "products"],  # List of table names
    target_catalog="main",
    target_schema="bronze",
    aws_region="us-east-1",
    max_workers=4,
)

# Check results
summary = create_summary(results)
print(f"Converted {summary.succeeded}/{summary.total_tables} tables")
print(f"Total files: {summary.total_files}")
print(f"Duration: {summary.total_duration_seconds:.1f}s")
```

### Pattern Matching

```python
# Convert all tables matching a glob pattern
results = convert_tables(
    spark=spark,
    glue_database="my_glue_db",
    tables="dim_*",  # Glob patterns: *, ?, [seq]
    target_catalog="main",
    target_schema="bronze",
    aws_region="us-east-1",
    max_workers=4,
)
```

### Using SQL Warehouse for Cross-Bucket Tables

```python
from hive_to_delta import convert_tables, create_summary
from hive_to_delta.connection import SqlWarehouseConnection

# Use SQL Warehouse instead of Databricks Connect
with SqlWarehouseConnection(warehouse_id="your_warehouse_id") as conn:
    spark = conn.spark_session

    results = convert_tables(
        spark=spark,
        glue_database="multi_bucket_db",
        tables=["cross_bucket_table"],
        target_catalog="main",
        target_schema="bronze",
        aws_region="us-east-1",
    )
```

## Composable API

v0.2.0 introduces a two-tier API alongside the legacy functions:

| Tier | Function | Use Case |
|------|----------|----------|
| Simple | `convert_table()` | Single table, you provide the file list |
| Composable | `convert()` | Bulk conversion with pluggable discovery + listing |
| Legacy | `convert_single_table()`, `convert_tables()` | Original Glue-only interface (still supported) |

**Discovery strategies** find tables to convert:
- `GlueDiscovery` -- queries AWS Glue Data Catalog
- `UCDiscovery` -- queries Unity Catalog / hive_metastore

**Listing strategies** build file lists:
- `S3Listing` -- scans S3 directly (with optional Glue partition awareness)
- `InventoryListing` -- wraps a pre-built DataFrame of file paths + sizes

Mix and match any discovery with any listing strategy.

## API Reference

### convert_table

Converts a single table from a DataFrame of file paths. Tier 1 simple API.

```python
convert_table(
    spark: SparkSession,
    files_df: DataFrame,              # Must have file_path (string) + size (long)
    table_location: str,              # S3 root path for table data
    target_catalog: str,
    target_schema: str,
    target_table: str,                # Name in Unity Catalog
    partition_columns: list[str] = None,
    aws_region: str = "us-east-1",
) -> ConversionResult
```

### convert

Converts tables using pluggable discovery and listing strategies. Tier 2 composable API.

```python
convert(
    spark: SparkSession,
    discovery: Discovery,             # GlueDiscovery, UCDiscovery, or custom
    listing: Listing,                 # S3Listing, InventoryListing, or custom
    target_catalog: str,
    target_schema: str,
    aws_region: str = "us-east-1",
    max_workers: int = 4,
    print_summary: bool = True,
) -> list[ConversionResult]
```

### GlueDiscovery

Discovers tables by querying the AWS Glue Data Catalog.

```python
GlueDiscovery(
    database: str,                    # Glue database name
    pattern: str = None,              # Glob pattern to filter table names
    region: str = "us-east-1",
)
# discovery.discover(spark) -> list[TableInfo]
```

### UCDiscovery

Discovers tables from Unity Catalog or hive_metastore.

```python
UCDiscovery(
    allow: list[str],                 # FQN glob patterns: ["hive_metastore.my_db.*"]
    deny: list[str] = ["*.information_schema.*"],
)
# discovery.discover(spark) -> list[TableInfo]
```

### S3Listing

Lists parquet files by scanning S3 directly.

```python
S3Listing(
    region: str = "us-east-1",
    glue_database: str = None,        # If set, uses Glue partitions for discovery
)
# listing.list_files(spark, table_info) -> list[ParquetFileInfo]
```

### InventoryListing

Lists parquet files from a pre-built inventory DataFrame.

```python
InventoryListing(
    files_df: DataFrame,              # Must have file_path (string) + size (long)
)
# listing.list_files(spark, table_info) -> list[ParquetFileInfo]
# Filters to files under table_info.location automatically
```

### validate_files_df

Validates that a DataFrame has the required columns and types for file listing.

```python
validate_files_df(files_df: DataFrame) -> None
# Raises ValueError if file_path (string) or size (int/bigint/long) columns are missing
```

### convert_single_table (legacy)

Converts one table from Glue to Unity Catalog.

```python
convert_single_table(
    spark: SparkSession,
    glue_database: str,          # Source Glue database name
    table_name: str,              # Source table name
    target_catalog: str,          # Unity Catalog name (e.g., "main")
    target_schema: str,           # Target schema name (e.g., "bronze")
    aws_region: str = "us-east-1",
    target_table_name: str = None,  # Optional: rename during conversion
) -> ConversionResult

# Returns ConversionResult with:
#   - success: bool
#   - file_count: int
#   - delta_log_location: str
#   - error: str | None
#   - duration_seconds: float
```

**Example:**
```python
result = convert_single_table(
    spark=spark,
    glue_database="analytics",
    table_name="user_events_2024",
    target_catalog="main",
    target_schema="bronze",
    target_table_name="events",  # Rename to "events"
)
```

### convert_tables

Converts multiple tables in parallel.

```python
convert_tables(
    spark: SparkSession,
    glue_database: str,
    tables: list[str] | str,  # List of names or glob pattern
    target_catalog: str,
    target_schema: str,
    aws_region: str = "us-east-1",
    max_workers: int = 4,     # Parallel conversion workers
) -> list[ConversionResult]
```

**Pattern Examples:**
- `tables=["t1", "t2", "t3"]` - Specific tables
- `tables="dim_*"` - All tables starting with "dim_"
- `tables="*_2024"` - All tables ending with "_2024"
- `tables="fact_[abc]*"` - Tables starting with "fact_a", "fact_b", or "fact_c"

**Parallelism Guidelines:**
- `max_workers=4` - Safe default
- `max_workers=8` - High-bandwidth environments
- `max_workers=1` - Rate-limited accounts or debugging

### vacuum_external_files

Cleans orphaned files at absolute S3 paths. Standard `VACUUM` cannot reach files outside the table's root location.

```python
vacuum_external_files(
    spark: SparkSession,
    table_name: str,              # Fully qualified (catalog.schema.table)
    retention_hours: float = 168,  # 7 days minimum
    dry_run: bool = True,         # Default: simulate only
    region: str = "us-east-1",
) -> VacuumResult

# Returns VacuumResult with:
#   - table_name: str
#   - orphaned_files: list[str]  # Files that would be deleted
#   - deleted_files: list[str]    # Files actually deleted (if dry_run=False)
#   - dry_run: bool
#   - error: str | None
```

**Example:**
```python
# First, check what would be deleted
result = vacuum_external_files(
    spark=spark,
    table_name="main.bronze.orders",
    retention_hours=168,
    dry_run=True,  # Simulate only
)
print(f"Would delete {len(result.orphaned_files)} files")

# Then actually delete
result = vacuum_external_files(
    spark=spark,
    table_name="main.bronze.orders",
    retention_hours=168,
    dry_run=False,  # Actually delete
)
print(f"Deleted {len(result.deleted_files)} files")
```

### list_glue_tables

Discovers tables in a Glue database.

```python
list_glue_tables(
    glue_database: str,
    pattern: str | None = None,  # Optional glob pattern
    region: str = "us-east-1",
) -> list[str]
```

**Example:**
```python
# List all tables
all_tables = list_glue_tables("my_glue_db")

# List tables matching pattern
dim_tables = list_glue_tables("my_glue_db", pattern="dim_*")
```

## Configuration Guide

### Step 1: Set Up AWS Credentials

Configure AWS credentials for CLI access:

```bash
# Option 1: AWS SSO (recommended for Databricks Field Eng)
aws configure sso

# Option 2: Traditional credentials
aws configure

# Verify access
aws sts get-caller-identity
aws glue get-databases --query 'DatabaseList[].Name'
```

**Required AWS Permissions:**

| Permission | Purpose |
|------------|---------|
| `glue:GetDatabase`, `glue:GetTable`, `glue:GetPartitions` | Read Hive metadata |
| `s3:ListBucket`, `s3:GetObject` | Scan for Parquet files |
| `s3:PutObject` | Write Delta transaction logs |

### Step 2: Create IAM Role for Unity Catalog

Create an IAM role with two trust relationships:

**Trust Policy:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "YOUR_UC_EXTERNAL_ID"
        }
      }
    },
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::790110701330:role/serverless-customer-resource-role"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "databricks-serverless-YOUR_WORKSPACE_ID"
        }
      }
    }
  ]
}
```

**Permission Policy:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket-1/*",
        "arn:aws:s3:::your-bucket-1",
        "arn:aws:s3:::your-bucket-2/*",
        "arn:aws:s3:::your-bucket-2"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetPartitions"
      ],
      "Resource": "*"
    }
  ]
}
```

Or use the Terraform module in `/terraform` to automatically provision this infrastructure.

### Step 3: Configure Unity Catalog

**Create Storage Credential:**
```bash
# Get your IAM role ARN
export ROLE_ARN="arn:aws:iam::123456789012:role/your-uc-role"

# Create storage credential
databricks storage-credentials create your_credential \
  --aws-iam-role-arn "$ROLE_ARN"

# Get the external ID that Unity Catalog generated
databricks storage-credentials get your_credential --output json | \
  jq -r '.aws_iam_role.external_id'
```

**Update IAM trust policy with the external ID from above.**

**Create External Locations:**

You need an external location for each S3 bucket containing data:

```bash
# For each bucket with table data
databricks external-locations create location_bucket1 \
  --url "s3://your-bucket-1/" \
  --credential-name your_credential

databricks external-locations create location_bucket2 \
  --url "s3://your-bucket-2/" \
  --credential-name your_credential
```

### Step 4: Configure Databricks Compute

**Option A: SQL Warehouse (Required for cross-bucket/region tables)**

Create or configure a SQL Warehouse with an instance profile:

```bash
# Create instance profile wrapping your IAM role
aws iam create-instance-profile --instance-profile-name htd-instance-profile
aws iam add-role-to-instance-profile \
  --instance-profile-name htd-instance-profile \
  --role-name your-uc-role

# Configure workspace
databricks warehouses set-workspace-warehouse-config \
  --instance-profile-arn "arn:aws:iam::123456789012:instance-profile/htd-instance-profile" \
  --security-policy DATA_ACCESS_CONTROL
```

**Option B: Databricks Connect (Only for single-location tables)**

Install and configure:
```bash
pip install databricks-connect

# Configure with your workspace
databricks-connect configure
```

Note: Databricks Connect cannot resolve credentials for cross-bucket queries. Use SQL Warehouse for production workloads.

### Step 5: Set Up Python Environment

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install package
pip install "hive_to_delta @ git+https://github.com/randypitcherii/shareables.git#subdirectory=databricks/hive_to_delta"

# Verify installation
python -c "from hive_to_delta import convert_tables; print('Ready!')"
```

---

# Troubleshooting

## Quick Diagnostics

Run these commands to identify issues:

```bash
# 1. Verify AWS credentials
aws sts get-caller-identity

# 2. Test S3 access
aws s3 ls s3://your-bucket/ --region us-east-1

# 3. Check Glue database
aws glue get-database --name your_db --region us-east-1

# 4. List Glue tables
aws glue get-tables --database-name your_db --region us-east-1 \
  --query 'TableList[].Name' --output table

# 5. Check SQL Warehouse status
databricks warehouses get <warehouse_id> --output json | \
  jq '{state, health, num_clusters}'

# 6. Verify external locations exist
databricks external-locations list --output json | \
  jq '.[] | {name, url, credential_name}'
```

## Common Errors

For comprehensive debugging guidance including real error messages, root cause analysis, and step-by-step solutions, see **[DEBUGGING.md](DEBUGGING.md)**.

### Quick Reference

| Error | Quick Fix | Details |
|-------|-----------|---------|
| `NoCredentialsError` or `TokenRetrievalError` | `aws sso login --profile <your-profile>` | [Link](DEBUGGING.md#issue-expired-sso-tokens) |
| `PERMISSION_DENIED: credentialName = None` | Update IAM trust policy external ID | [Link](DEBUGGING.md#instance-profile-configuration) |
| SQL Warehouse won't start | Configure workspace instance profile | [Link](DEBUGGING.md#issue-sql-warehouse-wont-start-or-shows-degraded) |
| `LOCATION_OVERLAP` | Reuse existing table or use different S3 path | [Link](DEBUGGING.md#issue-location_overlap-error) |
| Cross-bucket queries fail | Use SQL Warehouse instead of Databricks Connect | [Link](DEBUGGING.md#sql-warehouse-vs-databricks-connect) |
| `Column 'X' cannot be resolved` | Check actual schema with `DESCRIBE TABLE` | [Link](DEBUGGING.md#issue-column-name-errors-in-insertupdate-statements) |
| `CANNOT_SHALLOW_CLONE_NON_UC_MANAGED_TABLE` | Expected for external tables | [Link](DEBUGGING.md#issue-shallow-clone-fails-for-external-tables) |
| External location not found | Create external location for bucket | [Link](DEBUGGING.md#unity-catalog-location-errors) |

See [DEBUGGING.md](DEBUGGING.md) for detailed diagnostics with real error messages and verification steps.

---

**Legacy error documentation below (consolidated in DEBUGGING.md):**

### `PERMISSION_DENIED: credentialName = None`

**Root Cause:** IAM trust policy external ID mismatch.

Unity Catalog generates a unique external ID when you create a storage credential. Your IAM role's trust policy must contain this exact ID.

**Diagnose:**
```bash
# Get the external ID Unity Catalog expects
databricks storage-credentials get your_credential --output json | \
  jq -r '.aws_iam_role.external_id'
# Output: 1234-5678-abcd

# Get the external ID in your IAM trust policy
aws iam get-role --role-name your-uc-role | \
  jq '.Role.AssumeRolePolicyDocument.Statement[0].Condition.StringEquals["sts:ExternalId"]'
# Output: "wrong-id"
```

**Fix:**
1. Update IAM trust policy with correct external ID
2. Verify both trust statements (Unity Catalog master role + serverless role)
3. Wait 30 seconds for propagation
4. Retry query

**Example Fix:**
```bash
# Update trust policy (see Configuration Guide for full JSON)
aws iam update-assume-role-policy \
  --role-name your-uc-role \
  --policy-document file://trust-policy.json
```

### SQL Warehouse Won't Start or Shows DEGRADED

**Symptom:**
- Warehouse stuck in "STARTING" state
- Health shows "DEGRADED"
- Queries fail with credential errors

**Root Cause:** Missing instance profile or serverless trust policy.

**Diagnose:**
```bash
# Check workspace configuration
databricks warehouses get-workspace-warehouse-config --output json

# Expected output should include:
# "instance_profile_arn": "arn:aws:iam::123456789:instance-profile/your-profile"

# Check IAM trust policy has serverless statement
aws iam get-role --role-name your-uc-role | \
  jq '.Role.AssumeRolePolicyDocument.Statement[] | select(.Principal.AWS | contains("790110701330"))'
```

**Fix:**
```bash
# 1. Create instance profile (if missing)
aws iam create-instance-profile --instance-profile-name htd-profile
aws iam add-role-to-instance-profile \
  --instance-profile-name htd-profile \
  --role-name your-uc-role

# 2. Update IAM trust policy with serverless statement
# Add to trust policy:
{
  "Effect": "Allow",
  "Principal": {
    "AWS": "arn:aws:iam::790110701330:role/serverless-customer-resource-role"
  },
  "Action": "sts:AssumeRole",
  "Condition": {
    "StringEquals": {
      "sts:ExternalId": "databricks-serverless-YOUR_WORKSPACE_ID"
    }
  }
}

# 3. Configure workspace
databricks warehouses set-workspace-warehouse-config \
  --instance-profile-arn "arn:aws:iam::123456789:instance-profile/htd-profile" \
  --security-policy DATA_ACCESS_CONTROL

# 4. Restart warehouse
databricks warehouses stop <warehouse_id>
databricks warehouses start <warehouse_id>
```

### Cross-Bucket Queries Fail (Single Bucket Works)

**Symptom:**
```python
# This works
spark.sql("SELECT * FROM main.bronze.single_bucket_table").show()

# This fails with credential error
spark.sql("SELECT * FROM main.bronze.cross_bucket_table").show()
```

**Root Cause:** Databricks Connect cannot resolve credentials for multiple buckets.

**Fix:** Use SQL Warehouse instead of Databricks Connect.

```python
# Before (Databricks Connect - won't work)
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

# After (SQL Warehouse - works)
from hive_to_delta.connection import SqlWarehouseConnection
with SqlWarehouseConnection(warehouse_id="abc123") as conn:
    spark = conn.spark_session
    result = convert_tables(...)
```

### External Location Not Found

**Symptom:**
```
Error: External location not found for URL s3://some-bucket/
```

**Root Cause:** Unity Catalog has no external location registered for that bucket.

**Diagnose:**
```bash
# Check which buckets have external locations
databricks external-locations list --output json | \
  jq -r '.[] | "\(.name): \(.url)"'

# Find tables with partitions in missing bucket
aws glue get-partitions --database-name your_db --table-name your_table | \
  jq -r '.Partitions[].StorageDescriptor.Location' | \
  grep -o 's3://[^/]*' | sort -u
```

**Fix:**
```bash
# Create external location for each missing bucket
databricks external-locations create location_for_bucket \
  --url "s3://missing-bucket/" \
  --credential-name your_credential

# Verify
databricks external-locations get location_for_bucket
```

### AWS SSO Session Expired

**Symptom:**
```
Error: The SSO session associated with this profile has expired or is invalid.
```

**Fix:**
```bash
# Re-authenticate
aws sso login --profile your-profile

# Verify
aws sts get-caller-identity --profile your-profile
```

### File Not Found During Conversion

**Symptom:**
```
ConversionResult(success=False, error="NoSuchKey: The specified key does not exist")
```

**Root Cause:** Glue partition metadata points to files that don't exist in S3.

**Diagnose:**
```bash
# Check partition metadata
aws glue get-partition \
  --database-name your_db \
  --table-name your_table \
  --partition-values 2024-01-01 | \
  jq -r '.Partition.StorageDescriptor.Location'

# Check if files exist at that location
aws s3 ls s3://bucket/path/to/partition/ --region us-east-1
```

**Fix:**
- Remove invalid partition from Glue: `aws glue delete-partition ...`
- Or move missing files to expected location
- Or update partition location in Glue

### Schema Mismatch After Conversion

**Symptom:**
```
Error: Column 'x' not found in table
```

**Root Cause:** Glue schema doesn't match actual Parquet files.

**Fix:**
```python
# Force schema refresh after conversion
spark.sql("REFRESH TABLE main.bronze.your_table")

# Check actual vs expected schema
spark.sql("DESCRIBE TABLE main.bronze.your_table").show()
```

## Configuration Checklist

Before running conversions, verify:

| Component | Check | Command |
|-----------|-------|---------|
| AWS Credentials | Can assume role | `aws sts get-caller-identity` |
| S3 Access | Can read buckets | `aws s3 ls s3://bucket/` |
| Glue Access | Can read metadata | `aws glue get-table --database-name db --name table` |
| IAM Trust Policy | Has UC external ID | `aws iam get-role --role-name role` |
| IAM Trust Policy | Has serverless statement | `aws iam get-role --role-name role` |
| Instance Profile | Created and assigned | `databricks warehouses get-workspace-warehouse-config` |
| Storage Credential | Created in Unity Catalog | `databricks storage-credentials list` |
| External Locations | Created for all buckets | `databricks external-locations list` |
| SQL Warehouse | Running and healthy | `databricks warehouses get warehouse_id` |

## Getting Help

If you encounter issues not covered here:

1. Check recent git commit messages for similar issues
2. Review the architecture documentation in `ARCHITECTURE.md`
3. Look at test cases in `tests/` for working examples
4. Enable verbose logging:
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

---

# Testing

## Running Tests

```bash
# By category (no infrastructure required)
make test-unit           # Unit tests (~7s)
make test-composition    # Composition tests (~7s)

# Infrastructure tests (requires Databricks + AWS)
make auth-check          # Verify authentication first
make test-infrastructure # Composable pipeline tests (~47s)

# Legacy scenario tests (requires Databricks + AWS)
make test-standard       # Single-location tables
make test-cross-bucket   # Multi-bucket partitions
make test-cross-region   # Multi-region partitions

# All tests
make test                # Everything
```

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `HIVE_TO_DELTA_TEST_WAREHOUSE_ID` | auto-discovered | SQL Warehouse for cross-bucket tests |
| `HIVE_TO_DELTA_TEST_GLUE_DATABASE` | `hive_to_delta_test` | Source Hive database |
| `HIVE_TO_DELTA_TEST_CATALOG` | `fe_randy_pitcher_workspace_catalog` | Target Unity Catalog |
| `HIVE_TO_DELTA_TEST_SCHEMA` | `hive_to_delta_tests` | Target schema |
| `DATABRICKS_CONFIG_PROFILE` | none | Databricks CLI profile |
| `AWS_PROFILE` | none | AWS credentials profile |

## Test Infrastructure

### Required AWS Resources

Deploy via Terraform:

```bash
cd terraform
terraform init
terraform apply
```

This creates:
- IAM role (`htd-role`) with S3/Glue/LakeFormation permissions
- Instance profile (`htd-instance-profile`)
- S3 buckets: `htd-east-1a`, `htd-east-1b`, `htd-west-2`
- Glue database with test tables

### Required Databricks Resources

```bash
# Create storage credential
databricks storage-credentials create \
  --name htd_storage_credential \
  --aws-iam-role '{"role_arn": "arn:aws:iam::ACCOUNT:role/htd-role"}'

# Create external locations (one per bucket)
databricks external-locations create \
  --name htd_east_1a \
  --url "s3://htd-east-1a/" \
  --credential-name htd_storage_credential

# Configure workspace for SQL Warehouses
databricks warehouses set-workspace-warehouse-config \
  --instance-profile-arn "arn:aws:iam::ACCOUNT:instance-profile/htd-instance-profile" \
  --security-policy DATA_ACCESS_CONTROL
```

### Test Tables

Create test tables in Glue:

```bash
python scripts/setup_integration_test_tables.py
```

| Table | Description |
|-------|-------------|
| `standard_table` | All partitions in `s3://htd-east-1a/standard/` |
| `cross_bucket_table` | Partitions split between `htd-east-1a` and `htd-east-1b` |
| `cross_region_table` | Partitions in `us-east-1` and `us-west-2` |

## Test Categories

| Category | Compute | What It Tests |
|----------|---------|---------------|
| Standard | Databricks Connect | Single-location tables, basic conversion |
| Cross-bucket | SQL Warehouse | Multi-bucket credential resolution |
| Cross-region | SQL Warehouse | Multi-region data access |

Cross-bucket and cross-region tests use `SqlWarehouseConnection` (Thrift protocol) because Databricks Connect cannot resolve credentials for external paths.

## IAM Trust Policy Requirements

Your IAM role needs two trust statements:

**1. Unity Catalog Master Role** (for storage credentials):
```json
{
  "Effect": "Allow",
  "Principal": {"AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-..."},
  "Action": "sts:AssumeRole",
  "Condition": {"StringEquals": {"sts:ExternalId": "<UC_EXTERNAL_ID>"}}
}
```

**2. Serverless Compute** (for SQL Warehouses):
```json
{
  "Effect": "Allow",
  "Principal": {"AWS": "arn:aws:iam::790110701330:role/serverless-customer-resource-role"},
  "Action": "sts:AssumeRole",
  "Condition": {"StringEquals": {"sts:ExternalId": "databricks-serverless-<WORKSPACE_ID>"}}
}
```

Get the UC external ID from your storage credential:
```bash
databricks storage-credentials get htd_storage_credential --output json | jq '.aws_iam_role.external_id'
```

---

## Usage Scenarios

### Scenario 1: Standard Single-Bucket Table

All partitions in one S3 location:

```python
from databricks.connect import DatabricksSession
from hive_to_delta import convert_single_table

spark = DatabricksSession.builder.getOrCreate()

result = convert_single_table(
    spark=spark,
    glue_database="analytics",
    table_name="user_events",
    target_catalog="main",
    target_schema="bronze",
)
```

### Scenario 2: Cross-Bucket Table

Partitions split across multiple S3 buckets in the same region:

```python
# Setup: Ensure external locations exist for both buckets
# databricks external-locations create loc_bucket_a --url s3://bucket-a/
# databricks external-locations create loc_bucket_b --url s3://bucket-b/

from hive_to_delta.connection import SqlWarehouseConnection
from hive_to_delta import convert_single_table

# Must use SQL Warehouse for cross-bucket
with SqlWarehouseConnection(warehouse_id="your_warehouse_id") as conn:
    result = convert_single_table(
        spark=conn.spark_session,
        glue_database="multi_location_db",
        table_name="scattered_table",
        target_catalog="main",
        target_schema="bronze",
    )
```

### Scenario 3: Cross-Region Table

Partitions in different AWS regions:

```python
# Setup: Ensure external locations exist for buckets in both regions
# databricks external-locations create loc_east --url s3://bucket-east/
# databricks external-locations create loc_west --url s3://bucket-west/

from hive_to_delta.connection import SqlWarehouseConnection
from hive_to_delta import convert_single_table

with SqlWarehouseConnection(warehouse_id="your_warehouse_id") as conn:
    result = convert_single_table(
        spark=conn.spark_session,
        glue_database="global_db",
        table_name="worldwide_data",
        target_catalog="main",
        target_schema="bronze",
        aws_region="us-east-1",  # Primary region
    )
```

### Scenario 4: Bulk Migration with Pattern

Migrate all dimension tables:

```python
from databricks.connect import DatabricksSession
from hive_to_delta import convert_tables, create_summary

spark = DatabricksSession.builder.getOrCreate()

# Convert all tables starting with "dim_"
results = convert_tables(
    spark=spark,
    glue_database="analytics",
    tables="dim_*",
    target_catalog="main",
    target_schema="bronze",
    max_workers=8,  # Parallel conversion
)

# Report results
summary = create_summary(results)
print(f"Success: {summary.succeeded}/{summary.total_tables}")
print(f"Failed: {summary.failed}")
print(f"Duration: {summary.total_duration_seconds:.1f}s")

# Check failures
for result in results:
    if not result.success:
        print(f"{result.source_table}: {result.error}")
```

### Scenario 5: Post-Conversion Cleanup

After modifying data, clean up old files:

```python
from databricks.connect import DatabricksSession
from hive_to_delta import vacuum_external_files

spark = DatabricksSession.builder.getOrCreate()

# First, run standard VACUUM for table root
spark.sql("VACUUM main.bronze.orders RETAIN 168 HOURS")

# Then clean up external partitions
result = vacuum_external_files(
    spark=spark,
    table_name="main.bronze.orders",
    retention_hours=168,  # 7 days
    dry_run=True,  # Check first
)

print(f"Would delete {len(result.orphaned_files)} external files")

# If looks good, actually delete
if len(result.orphaned_files) > 0:
    result = vacuum_external_files(
        spark=spark,
        table_name="main.bronze.orders",
        retention_hours=168,
        dry_run=False,  # Actually delete
    )
    print(f"Deleted {len(result.deleted_files)} files")
```

---

## How It Works

`hive_to_delta` performs a metadata-only migration by generating Delta transaction logs that reference files at their original S3 locations.

**Step-by-step process:**

1. **Fetch metadata** from AWS Glue
   - Table schema (columns, types)
   - Partition keys
   - Storage locations for each partition

2. **Scan S3** for Parquet files
   - Lists files in each partition location
   - Supports cross-bucket and cross-region partitions
   - Collects file sizes and modification times

3. **Generate Delta log**
   - Converts Glue schema to Delta format
   - Creates add actions with absolute S3 paths
   - Writes `_delta_log/00000000000000000000.json`

4. **Register in Unity Catalog**
   - Executes `CREATE TABLE ... USING DELTA LOCATION`
   - Unity Catalog reads Delta log to discover schema and files

**Key Difference from `CONVERT TO DELTA`:**

Standard approach:
```json
{"add": {"path": "region=us/file.parquet"}}  // Relative path
```

This package:
```json
{"add": {"path": "s3://bucket-b/external/region=eu/file.parquet"}}  // Absolute path
```

Absolute paths allow Delta to reference files across multiple buckets and regions without data movement.

## Limitations and Best Practices

### Limitations

**Supported:**
- Parquet format only
- Partitioned and non-partitioned tables
- Cross-bucket and cross-region partitions
- Standard Hive partition naming (`key=value`)

**Not Supported:**
- Non-Parquet formats (ORC, Avro, CSV)
- Deeply nested partition structures
- Tables with complex types requiring schema evolution
- Incremental partition additions (must reconvert entire table)

### Best Practices

**Performance:**
- Use `max_workers=4-8` for bulk conversions
- Enable S3 transfer acceleration for cross-region tables
- Run conversions during off-peak hours for large tables
- Consider data locality when querying cross-region tables

**Security:**
- Use AWS SSO instead of long-lived access keys
- Rotate external IDs periodically
- Apply least-privilege IAM policies
- Enable S3 bucket versioning for safety

**Maintenance:**
- Run external vacuum after UPDATE/DELETE operations
- Monitor orphaned file counts
- Document which tables have external partitions
- Keep external location mappings updated

**Testing:**
- Always test with a single table first
- Verify external locations before bulk conversion
- Use dry-run mode for vacuum operations
- Check query performance after conversion

**Cost Optimization:**
- Use S3 Intelligent-Tiering for infrequently accessed partitions
- Consider consolidating cross-region data with OPTIMIZE
- Clean up old files regularly with vacuum
- Monitor cross-region data transfer costs

## Contributing

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run linter
ruff check .

# Format code
ruff format .

# Run tests
make test
```

## Frequently Asked Questions

### Does this copy my data?

No. `hive_to_delta` only writes metadata (Delta transaction logs). Your Parquet files remain at their original S3 locations.

### What happens if I delete a converted table?

The Delta metadata is deleted from Unity Catalog, but your original Parquet files remain untouched in S3.

### Can I convert the same table multiple times?

Yes, but each conversion overwrites the Delta log. If you've made changes (INSERT, UPDATE, DELETE) to the Delta table, you'll lose those changes.

### Do I need to stop using the Glue table?

No. The original Glue table continues to work. The Delta table is a new registration pointing to the same files.

### What about partitions added after conversion?

`hive_to_delta` doesn't support incremental updates. You must reconvert the entire table to pick up new partitions.

### Can I use this with Databricks notebooks?

Yes. Replace `DatabricksSession` with the notebook's existing `spark` session:

```python
from hive_to_delta import convert_single_table

# In a Databricks notebook
result = convert_single_table(
    spark=spark,  # Notebook's built-in spark session
    glue_database="my_db",
    table_name="my_table",
    target_catalog="main",
    target_schema="bronze",
)
```

### When should I use SQL Warehouse vs Databricks Connect?

**Use SQL Warehouse when:**
- Tables have partitions in multiple S3 buckets
- Tables have partitions in multiple AWS regions
- Running in production

**Use Databricks Connect when:**
- All partitions are in a single S3 location
- Running locally for development
- Need faster iteration without warehouse startup time

### Will this work with non-Parquet formats?

No. Only Parquet format is supported. ORC, Avro, CSV, and other formats are not supported.

### How do I handle schema changes?

Schema changes require reconverting the table. Delta doesn't automatically sync schema from Glue.

### What if my table has no partitions?

Non-partitioned tables work fine. The package will scan the table root location for all Parquet files.

### Can I use this with AWS Lake Formation?

Yes, but ensure your IAM role has Lake Formation permissions in addition to S3/Glue permissions.

### Does this support nested partitions?

Standard Hive partitioning (`key=value`) is supported. Deeply nested non-Hive partitions are not supported.

### What about table statistics?

Delta statistics are not generated during conversion. Run `ANALYZE TABLE` after conversion to compute statistics:

```sql
ANALYZE TABLE main.bronze.my_table COMPUTE STATISTICS;
```

### Can I rename tables during conversion?

Yes, use the `target_table_name` parameter:

```python
result = convert_single_table(
    spark=spark,
    glue_database="analytics",
    table_name="old_ugly_name_2024",
    target_catalog="main",
    target_schema="bronze",
    target_table_name="nice_clean_name",
)
```

### What's the performance impact of cross-region tables?

Cross-region queries are 2-3x slower due to network latency. Consider:
- Using `OPTIMIZE` to consolidate files to a single region
- Caching frequently accessed data
- Replicating cross-region data to local buckets

## Related Documentation

- **Quick Start**: See [QUICKSTART.md](QUICKSTART.md) for 15-minute setup
- **Architecture**: See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design decisions
- **Testing**: See [TESTING.md](TESTING.md) for test inventory and execution
- **Terraform Setup**: See [terraform/README.md](terraform/README.md) for infrastructure provisioning
- **Test Infrastructure Setup**: See [scripts/README.md](scripts/README.md) for AWS/Glue test data configuration
- **API Details**: See inline docstrings in `hive_to_delta/` modules

## Support and Contributing

**Found a bug?** Open an issue with:
- Error message and full stack trace
- Code snippet to reproduce
- AWS region and Databricks workspace version

**Want to contribute?**
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

**Development setup:**
```bash
git clone https://github.com/randypitcherii/shareables.git
cd shareables/databricks/hive_to_delta
python -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
make test
```

## License

MIT License - see LICENSE file for details.
