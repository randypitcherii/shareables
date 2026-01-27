# hive_to_delta

Register AWS Glue Hive tables as external Delta tables in Databricks Unity Catalog—without moving data.

## Why This Package?

Databricks' `CONVERT TO DELTA` fails when partitions span multiple S3 buckets or AWS regions. This package solves that by:

- Generating Delta transaction logs from Glue metadata + S3 file scans
- Writing absolute S3 paths that work across buckets and regions
- Registering tables in Unity Catalog as external Delta tables

No data movement. Metadata only.

## Installation

```bash
pip install hive_to_delta

# With development dependencies
pip install "hive_to_delta[dev]"
```

**Requirements:**
- Python >= 3.10
- Databricks workspace with Unity Catalog
- AWS credentials with Glue and S3 access

## Quick Start

```python
from databricks.connect import DatabricksSession
from hive_to_delta import convert_tables, create_summary

spark = DatabricksSession.builder.getOrCreate()

# Convert tables matching a pattern
results = convert_tables(
    spark=spark,
    glue_database="my_glue_db",
    tables="dim_*",  # Glob pattern: *, ?, [seq]
    target_catalog="main",
    target_schema="bronze",
    aws_region="us-east-1",
    max_workers=4,
)

# Check results
summary = create_summary(results)
print(f"Converted {summary.succeeded}/{summary.total_tables} tables")
```

## API Reference

### convert_tables

Converts multiple tables in parallel.

```python
convert_tables(
    spark,
    glue_database: str,
    tables: list[str] | str,  # List or glob pattern
    target_catalog: str,
    target_schema: str,
    aws_region: str = "us-east-1",
    max_workers: int = 4,
) -> list[ConversionResult]
```

### convert_single_table

Converts one table with optional rename.

```python
convert_single_table(
    spark,
    glue_database: str,
    table_name: str,
    target_catalog: str,
    target_schema: str,
    aws_region: str = "us-east-1",
    target_table_name: str = None,
) -> ConversionResult
```

### vacuum_external_files

Cleans orphaned files at absolute S3 paths. Standard `VACUUM` cannot reach files outside the table's root location.

```python
vacuum_external_files(
    spark,
    table_name: str,
    retention_hours: float = 168,  # 7 days minimum
    dry_run: bool = True,
    region: str = "us-east-1",
) -> VacuumResult
```

### list_glue_tables

Discovers tables in a Glue database.

```python
list_glue_tables(
    glue_database: str,
    pattern: str | None = None,
    region: str = "us-east-1",
) -> list[str]
```

## Prerequisites

### AWS

| Permission | Purpose |
|------------|---------|
| `glue:GetTable`, `glue:GetPartitions` | Read table metadata |
| `s3:ListBucket`, `s3:GetObject` | Scan for Parquet files |
| `s3:PutObject` | Write Delta transaction logs |

### Databricks

- Unity Catalog enabled
- Storage credential linked to your IAM role
- External locations for each S3 bucket containing data

---

# Debugging Guide

## Common Errors

### `PERMISSION_DENIED: credentialName = None`

**Cause:** IAM trust policy external ID doesn't match Unity Catalog's expected value.

Unity Catalog generates a unique external ID when you create a storage credential. Your IAM role's trust policy must include this exact ID.

**Diagnose:**
```bash
# Get Unity Catalog's external ID
databricks storage-credentials get <credential_name> --output json | jq '.aws_iam_role.external_id'

# Get IAM trust policy external ID
aws iam get-role --role-name <role_name> --query 'Role.AssumeRolePolicyDocument'
```

**Fix:** Update IAM trust policy with the correct external ID, then re-apply.

### SQL Warehouse Won't Start or Shows DEGRADED

**Cause:** Missing instance profile or serverless compute trust policy.

**Diagnose:**
```bash
# Check workspace config
databricks warehouses get-workspace-warehouse-config --output json

# Check warehouse health
databricks warehouses get <warehouse_id> --output json | jq '{state, health}'
```

**Fix:**
1. Create instance profile wrapping your IAM role
2. Add serverless compute trust policy:
   - Principal: `arn:aws:iam::790110701330:role/serverless-customer-resource-role`
   - External ID: `databricks-serverless-<WORKSPACE_ID>`
3. Configure workspace with instance profile

### Cross-Bucket Queries Fail (but standard queries work)

**Cause:** Databricks Connect cannot resolve credentials for multiple buckets.

**Fix:** Use SQL Warehouse for cross-bucket/cross-region queries. Only SQL Warehouses with instance profiles support credential fallback.

### External Location Not Found

**Cause:** Unity Catalog lacks external locations for some S3 buckets.

**Fix:**
```sql
CREATE EXTERNAL LOCATION my_bucket
WITH (URL = 's3://my-bucket/', STORAGE_CREDENTIAL my_cred);
```

## Quick Health Checks

Run these in order to isolate issues:

```bash
# 1. AWS credentials
aws sts get-caller-identity

# 2. S3 access
aws s3 ls s3://your-bucket/

# 3. Glue tables
aws glue get-tables --database-name your_db --query 'TableList[].Name'

# 4. SQL Warehouse health
databricks warehouses get <id> --output json | jq '{state, health}'

# 5. External locations
databricks external-locations list --output json | jq '.[].name'
```

## Configuration Checklist

| Component | What Goes Wrong |
|-----------|-----------------|
| IAM Trust Policy | External ID mismatch; missing serverless statement |
| Instance Profile | Not assigned to workspace |
| Storage Credential | Wrong IAM role ARN; external ID not in trust policy |
| External Locations | Missing for some buckets; wrong credential |

---

# Testing

## Running Tests

```bash
# All tests
make test

# By category
make test-standard       # Single-location tables (~30s)
make test-cross-bucket   # Multi-bucket partitions
make test-cross-region   # Multi-region partitions

# Options
make test-verbose        # Extra verbose output
make test-parallel       # Run with 4 workers
make test-collect        # Show what will run
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

## How It Works

1. **Fetch metadata** from AWS Glue (schema, partition keys, locations)
2. **Scan S3** for Parquet files across all partitions
3. **Generate Delta log** with absolute S3 paths
4. **Write `_delta_log/`** to table root directory
5. **Register table** in Unity Catalog via `CREATE TABLE ... USING DELTA LOCATION`

The key difference from `CONVERT TO DELTA`: absolute paths in the Delta log allow files across multiple buckets and regions.

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
