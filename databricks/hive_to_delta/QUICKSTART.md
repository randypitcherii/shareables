# Quick Start Guide

Get up and running with `hive_to_delta` in 15 minutes.

## Prerequisites Check

Before starting, verify you have:

```bash
# Python 3.10+
python --version

# AWS CLI configured
aws sts get-caller-identity

# Access to Databricks workspace
databricks workspace list
```

## 1. Install Package

```bash
pip install hive_to_delta
```

## 2. Set Up Infrastructure (One-Time)

### Option A: Use Terraform (Recommended)

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars

# Edit terraform.tfvars with your values:
# - project_prefix (e.g., "htd-myproject")
# - uc_external_id (get from next step)
# - glue_database_name

terraform init
terraform apply
```

### Option B: Manual Setup

**Create IAM Role:**
```bash
# 1. Create role with this trust policy
aws iam create-role \
  --role-name hive-to-delta-role \
  --assume-role-policy-document file://trust-policy.json

# 2. Attach permissions
aws iam put-role-policy \
  --role-name hive-to-delta-role \
  --policy-name HiveToDeltaPermissions \
  --policy-document file://permissions-policy.json
```

**Create Storage Credential in Databricks:**
```bash
export ROLE_ARN="arn:aws:iam::123456789:role/hive-to-delta-role"

databricks storage-credentials create htd_credential \
  --aws-iam-role-arn "$ROLE_ARN"

# Get external ID
databricks storage-credentials get htd_credential --output json | \
  jq -r '.aws_iam_role.external_id'
```

**Update IAM Trust Policy with External ID:**
```bash
# Update trust policy with the external ID from above
aws iam update-assume-role-policy \
  --role-name hive-to-delta-role \
  --policy-document file://trust-policy-updated.json
```

**Create External Locations:**
```bash
# For each S3 bucket containing data
databricks external-locations create htd_bucket1 \
  --url "s3://your-bucket-1/" \
  --credential-name htd_credential

databricks external-locations create htd_bucket2 \
  --url "s3://your-bucket-2/" \
  --credential-name htd_credential
```

**Configure SQL Warehouse:**
```bash
# Create instance profile
aws iam create-instance-profile --instance-profile-name htd-profile
aws iam add-role-to-instance-profile \
  --instance-profile-name htd-profile \
  --role-name hive-to-delta-role

# Configure workspace
databricks warehouses set-workspace-warehouse-config \
  --instance-profile-arn "arn:aws:iam::123456789:instance-profile/htd-profile" \
  --security-policy DATA_ACCESS_CONTROL
```

## 3. Convert Your First Table

Create a Python script:

```python
# convert_table.py
from databricks.connect import DatabricksSession
from hive_to_delta import convert_single_table

# Connect to Databricks
spark = DatabricksSession.builder.getOrCreate()

# Convert one table
result = convert_single_table(
    spark=spark,
    glue_database="your_glue_database",
    table_name="your_table_name",
    target_catalog="main",
    target_schema="bronze",
    aws_region="us-east-1",
)

if result.success:
    print(f"Success! Converted {result.file_count} files")
    print(f"Table: {result.target_table}")
    print(f"Duration: {result.duration_seconds:.1f}s")
else:
    print(f"Failed: {result.error}")
```

Run it:
```bash
python convert_table.py
```

## 4. Verify the Conversion

Query your new table:

```sql
-- In Databricks SQL or notebook
SELECT * FROM main.bronze.your_table_name LIMIT 10;

-- Check partition count
SELECT COUNT(DISTINCT partition_col) FROM main.bronze.your_table_name;

-- View table details
DESCRIBE DETAIL main.bronze.your_table_name;
```

## 5. Convert Multiple Tables

For bulk conversion:

```python
# convert_bulk.py
from databricks.connect import DatabricksSession
from hive_to_delta import convert_tables, create_summary

spark = DatabricksSession.builder.getOrCreate()

# Convert all tables matching pattern
results = convert_tables(
    spark=spark,
    glue_database="your_glue_database",
    tables="dim_*",  # Or ["table1", "table2", "table3"]
    target_catalog="main",
    target_schema="bronze",
    aws_region="us-east-1",
    max_workers=4,
)

# Print summary
summary = create_summary(results)
print(f"Converted: {summary.succeeded}/{summary.total_tables}")
print(f"Total files: {summary.total_files}")
print(f"Duration: {summary.total_duration_seconds:.1f}s")

# Show failures
for result in results:
    if not result.success:
        print(f"FAILED: {result.source_table} - {result.error}")
```

## 6. Handle Cross-Bucket Tables

If your table has partitions in multiple buckets, use SQL Warehouse:

```python
# convert_cross_bucket.py
from hive_to_delta.connection import SqlWarehouseConnection
from hive_to_delta import convert_single_table

# Get warehouse ID
# databricks warehouses list --output json | jq -r '.[0].id'

with SqlWarehouseConnection(warehouse_id="your_warehouse_id") as conn:
    result = convert_single_table(
        spark=conn.spark_session,
        glue_database="your_glue_database",
        table_name="cross_bucket_table",
        target_catalog="main",
        target_schema="bronze",
    )

    print(f"Success: {result.success}")
```

## Troubleshooting Quick Fixes

**AWS credentials not working:**
```bash
aws sso login --profile your-profile
export AWS_PROFILE=your-profile
```

**External ID mismatch:**
```bash
# Get correct external ID
databricks storage-credentials get htd_credential --output json | \
  jq -r '.aws_iam_role.external_id'

# Update IAM trust policy with this ID
```

**External location not found:**
```bash
# List current locations
databricks external-locations list

# Create missing location
databricks external-locations create location_name \
  --url "s3://missing-bucket/" \
  --credential-name htd_credential
```

**SQL Warehouse not starting:**
```bash
# Check warehouse status
databricks warehouses get <warehouse_id> --output json | jq '{state, health}'

# Restart if needed
databricks warehouses stop <warehouse_id>
databricks warehouses start <warehouse_id>
```

## Next Steps

- Read the full README for advanced usage
- Check `ARCHITECTURE.md` for how it works internally
- Review `scripts/README.md` for test setup
- See `terraform/README.md` for infrastructure details

## Common Workflows

**Daily development:**
```bash
# Activate environment
source venv/bin/activate

# Convert tables
python convert_tables.py

# Query in Databricks SQL
databricks sql query "SELECT * FROM main.bronze.my_table LIMIT 5"
```

**Maintenance:**
```bash
# Run standard vacuum (files under table root)
databricks sql query "VACUUM main.bronze.my_table RETAIN 168 HOURS"

# Run external vacuum (files outside table root)
python vacuum_external.py
```

**Monitoring:**
```bash
# Check table sizes
databricks sql query "
SELECT
  table_name,
  size_in_bytes / 1024 / 1024 / 1024 as size_gb,
  num_files
FROM main.bronze.table_metadata
ORDER BY size_gb DESC
LIMIT 10
"
```
