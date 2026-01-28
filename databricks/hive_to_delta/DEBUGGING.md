# Debugging Guide

This guide documents the actual issues encountered while getting all 94 tests passing, with real error messages and step-by-step solutions. Use this when troubleshooting test failures or infrastructure setup.

## Table of Contents

- [AWS Credentials Issues](#aws-credentials-issues)
- [Instance Profile Configuration](#instance-profile-configuration)
- [Unity Catalog Location Errors](#unity-catalog-location-errors)
- [Test Schema Mismatches](#test-schema-mismatches)
- [Shallow Clone Limitations](#shallow-clone-limitations)
- [SQL Warehouse vs Databricks Connect](#sql-warehouse-vs-databricks-connect)
- [Quick Diagnostic Commands](#quick-diagnostic-commands)

---

## AWS Credentials Issues

### Issue: Wrong AWS Profile / Account ID Mismatch

**Symptom:**
```
botocore.exceptions.NoCredentialsError: Unable to locate credentials
```
or
```
An error occurred (AccessDenied) when calling GetObject: Account ID mismatch
```

**Root Cause:**

You're using the wrong AWS profile, or your SSO credentials have expired. The test infrastructure expects specific AWS account credentials that match the Databricks workspace configuration.

**How to Diagnose:**

```bash
# Check which AWS account you're using
aws sts get-caller-identity

# Expected output should show:
# Account: 471112853095 (Field Engineering Sandbox)
# Arn: arn:aws:sts::471112853095:assumed-role/...
```

**Solution:**

1. **Set the correct AWS profile:**
   ```bash
   export AWS_PROFILE=fe-sandbox  # or your specific profile name
   ```

2. **Verify the profile is configured in `~/.aws/config`:**
   ```ini
   [profile fe-sandbox]
   sso_start_url = https://databricks.awsapps.com/start
   sso_region = us-east-1
   sso_account_id = 471112853095
   sso_role_name = AWSAdministratorAccess
   region = us-east-1
   ```

3. **Verify credentials are working:**
   ```bash
   aws sts get-caller-identity
   aws s3 ls s3://htd-east-1a/
   ```

### Issue: Expired SSO Tokens

**Symptom:**
```
botocore.exceptions.TokenRetrievalError: Error when retrieving token from sso:
Token has expired and refresh failed
```

**Root Cause:**

AWS SSO tokens expire after a certain period (usually 12 hours). You need to re-authenticate.

**Solution:**

```bash
# Re-authenticate with AWS SSO
aws sso login --profile fe-sandbox

# Verify it worked
aws sts get-caller-identity
```

**How to Verify:**

```bash
# Test S3 access to the test buckets
aws s3 ls s3://htd-east-1a/
aws s3 ls s3://htd-east-1b/
aws s3 ls s3://htd-west-2/

# Test Glue access
aws glue get-database --name hive_to_delta_test
aws glue get-tables --database-name hive_to_delta_test --query 'TableList[].Name'
```

---

## Instance Profile Configuration

### Issue: SQL Warehouse Won't Start or Shows DEGRADED

**Symptom:**
```
SQL Warehouse state: STARTING (stuck)
SQL Warehouse health: DEGRADED
```
or test error:
```
AssertionError: SQL Warehouse connection required for cross-bucket queries.
Set HIVE_TO_DELTA_TEST_WAREHOUSE_ID environment variable.
```

**Root Cause:**

Serverless SQL Warehouses require an instance profile to be configured at the workspace level. Without it, the warehouse cannot start or access S3 resources properly.

**What is an Instance Profile?**

An instance profile is an AWS resource that wraps an IAM role, allowing EC2 instances (or in this case, Databricks compute) to assume the role and access AWS services. For serverless SQL Warehouses, Databricks needs:

1. An IAM role with S3/Glue permissions
2. An instance profile that wraps that role
3. Workspace-level configuration linking the instance profile
4. A trust policy allowing serverless compute to assume the role

**How to Diagnose:**

```bash
# Check if workspace has instance profile configured
databricks warehouses get-workspace-warehouse-config --output json

# Expected output should show:
# {
#   "instance_profile_arn": "arn:aws:iam::ACCOUNT:instance-profile/htd-instance-profile",
#   ...
# }

# Check warehouse health
databricks warehouses get <warehouse_id> --output json | jq '{state, health}'

# Expected:
# {
#   "state": "RUNNING",
#   "health": {
#     "status": "HEALTHY"
#   }
# }
```

**Solution:**

**Step 1: Create the instance profile (if not exists)**

In your Terraform configuration (`terraform/iam.tf`):

```hcl
# Create instance profile
resource "aws_iam_instance_profile" "htd_instance_profile" {
  name = "htd-instance-profile"
  role = aws_iam_role.htd_role.name
}
```

Apply:
```bash
cd terraform
terraform apply
```

**Step 2: Add serverless compute trust policy**

Your IAM role needs to trust Databricks serverless compute. Add this statement to the trust policy:

```json
{
  "Effect": "Allow",
  "Principal": {
    "AWS": "arn:aws:iam::790110701330:role/serverless-customer-resource-role"
  },
  "Action": "sts:AssumeRole",
  "Condition": {
    "StringEquals": {
      "sts:ExternalId": "databricks-serverless-<WORKSPACE_ID>"
    }
  }
}
```

**Get your workspace ID:**
```bash
databricks current-user me --output json | jq -r '.active_workspace_id'
```

**Step 3: Configure workspace with instance profile**

```bash
# Get instance profile ARN from Terraform output
cd terraform
terraform output instance_profile_arn

# Configure workspace
databricks warehouses set-workspace-warehouse-config \
  --instance-profile-arn "arn:aws:iam::471112853095:instance-profile/htd-instance-profile" \
  --security-policy DATA_ACCESS_CONTROL
```

**Step 4: Restart warehouse**

```bash
# Get warehouse ID
databricks warehouses list --output json | jq '.warehouses[] | {id, name, state}'

# Stop and start the warehouse
databricks warehouses stop <warehouse_id>
databricks warehouses start <warehouse_id>

# Check health
databricks warehouses get <warehouse_id> --output json | jq '{state, health}'
```

**How to Verify:**

```bash
# Warehouse should be RUNNING and HEALTHY
databricks warehouses get <warehouse_id> --output json | jq '{state, health}'

# Set the environment variable for tests
export HIVE_TO_DELTA_TEST_WAREHOUSE_ID=<warehouse_id>

# Run cross-bucket tests
make test-cross-bucket
```

---

## Unity Catalog Location Errors

### Issue: LOCATION_OVERLAP Error

**Symptom:**
```
[LOCATION_OVERLAP] Cannot create table ('catalog.schema.new_table').
The location 's3://htd-east-1a/standard/' overlaps with the location of table
'catalog.schema.existing_table' (s3://htd-east-1a/standard/).
```

**Root Cause:**

Unity Catalog does not allow multiple **external** tables to share the same S3 location. When you try to create a second external table pointing to the same S3 path, Unity Catalog raises a `LOCATION_OVERLAP` error. This is expected behavior to prevent metadata conflicts.

**Important:** Managed tables can share locations, but external tables (created with `USING DELTA LOCATION`) cannot.

**How to Diagnose:**

```bash
# Check if a table already exists at that location
databricks tables list --catalog <catalog> --schema <schema> --output json | \
  jq '.[] | {name, table_type, storage_location}'

# Look for tables with matching storage_location
```

**Solution:**

**Option 1: Reuse the existing table** (recommended for tests)

Instead of creating a new table at the same location, use the table that was already created:

```python
# In test_operations.py - BAD approach:
def test_operations(spark, ...):
    # This creates a NEW external table at the same location - ERROR!
    result = convert_single_table(
        spark=spark,
        glue_database="hive_to_delta_test",
        table_name="standard_table",
        target_catalog=catalog,
        target_schema=schema,
        target_table_name="standard_table_ops",  # New table, same location
    )

# GOOD approach:
def test_operations(spark, ...):
    # Reuse the table created by test_convert.py
    existing_table = f"{catalog}.{schema}.standard_table"

    try:
        count = spark.sql(f"SELECT COUNT(*) FROM {existing_table}").collect()[0]["cnt"]
        if count > 0:
            # Table exists and has data - use it!
            return existing_table
    except Exception:
        # Table doesn't exist - create it
        result = convert_single_table(...)
```

**Option 2: Use different S3 locations**

If you need multiple tables, ensure each points to a different S3 path:

```python
# Table 1: s3://htd-east-1a/standard/
# Table 2: s3://htd-east-1a/standard_copy/  # Different path
# Table 3: s3://htd-east-1a/standard_test/  # Different path
```

**Option 3: Drop the existing table first**

```sql
-- This removes the metadata, freeing up the location
DROP TABLE IF EXISTS catalog.schema.existing_table;
```

**How to Verify:**

```bash
# After fixing, verify the table is accessible
databricks sql-warehouse query \
  --warehouse-id <id> \
  --query "SELECT COUNT(*) FROM catalog.schema.table_name"
```

---

## Test Schema Mismatches

### Issue: Column Name Errors in INSERT/UPDATE Statements

**Symptom:**
```
[UNRESOLVED_COLUMN] Column 'is_active' cannot be resolved
```
or
```
pyspark.errors.exceptions.captured.AnalysisException:
Column 'is_active' does not exist. Available columns: id, name, value, created_at, date
```

**Root Cause:**

Test code attempted to INSERT or UPDATE using column names that don't exist in the actual table schema. This happened because the test was written with an assumed schema that didn't match the tables created by `setup_fresh_hive_tables.py`.

**How to Diagnose:**

```sql
-- Check the actual table schema
DESCRIBE TABLE catalog.schema.table_name;

-- Or use Spark
spark.sql("DESCRIBE TABLE catalog.schema.table_name").show()
```

**Solution:**

**Step 1: Get the actual schema**

```python
# In your test
detail = spark.sql(f"DESCRIBE {table}").collect()
columns = {row["col_name"]: row["data_type"] for row in detail
           if row["col_name"] and not row["col_name"].startswith("#")}
print(f"Available columns: {list(columns.keys())}")
```

**Step 2: Update INSERT/UPDATE statements to match**

```python
# BAD - assumes wrong schema
spark.sql(f"""
    INSERT INTO {table}
    VALUES (1, 'test', 100.0, true)  -- Assumes: id, name, value, is_active
""")

# GOOD - matches actual schema: id, name, value, created_at, date
partition_col, partition_val = "date", "2024-01-01"

spark.sql(f"""
    INSERT INTO {table}
    SELECT 999999 as id,
           'test_insert' as name,
           999.99 as value,
           current_timestamp() as created_at,
           '{partition_val}' as {partition_col}
""")
```

**Common Schemas in This Project:**

```sql
-- Standard/Cross-bucket/Cross-region tables
CREATE TABLE table_name (
    id BIGINT,
    name STRING,
    value DOUBLE,
    created_at TIMESTAMP,
    date STRING  -- partition column
)
PARTITIONED BY (date)
```

**How to Verify:**

```bash
# Run the specific test
pytest tests/test_operations.py::TestDeltaOperations::test_insert -v

# Should pass without column errors
```

---

## Shallow Clone Limitations

### Issue: Shallow Clone Fails for External Tables

**Symptom:**
```
[CANNOT_SHALLOW_CLONE_NON_UC_MANAGED_TABLE_AS_SOURCE_OR_TARGET]
Shallow clone is only supported for the MANAGED table type
```

**Root Cause:**

Unity Catalog only supports shallow cloning for **managed** tables. External tables (like our converted Hive-to-Delta tables created with `USING DELTA LOCATION`) cannot be shallow cloned. This is a Unity Catalog design limitation.

**Why This Limitation Exists:**

Shallow clones create a new table that references the same data files as the source table. For managed tables, Unity Catalog controls the data lifecycle. For external tables, Unity Catalog only manages metadata, so it cannot guarantee referential integrity for shallow clones.

**How to Diagnose:**

```sql
-- Check table type
DESCRIBE DETAIL catalog.schema.table_name;

-- Look for:
-- type: EXTERNAL (cannot shallow clone)
-- type: MANAGED (can shallow clone)
```

**Solution:**

This is expected behavior, not a bug. Update tests to verify this behavior rather than skip it:

```python
# BAD approach - skip the test
@pytest.mark.skip(reason="External tables don't support shallow clone")
def test_shallow_clone_external():
    ...

# GOOD approach - verify expected behavior
def test_shallow_clone_external_table_fails(spark, ...):
    """Verify shallow clone fails for external tables (expected behavior)."""

    source_table = f"{catalog}.{schema}.cross_bucket_table"  # External table
    target_table = f"{catalog}.{schema}.cross_bucket_clone"

    spark.sql(f"DROP TABLE IF EXISTS {target_table}")

    # Shallow clone should fail for external tables
    from pyspark.errors.exceptions.connect import AnalysisException

    with pytest.raises(AnalysisException) as exc_info:
        spark.sql(f"""
            CREATE TABLE {target_table}
            SHALLOW CLONE {source_table}
        """)

    # Verify the error is about external tables
    error_message = str(exc_info.value)
    assert "CANNOT_SHALLOW_CLONE_NON_UC_MANAGED_TABLE_AS_SOURCE_OR_TARGET" in error_message or \
           "Shallow clone is only supported for the MANAGED table type" in error_message

    print("Confirmed: Shallow clone correctly fails for external table")
```

**Workarounds:**

If you need to clone an external table, use one of these alternatives:

1. **Deep Clone** (copies data):
   ```sql
   CREATE TABLE target_table
   DEEP CLONE source_table;
   ```

2. **Create a new external table** at the same location:
   ```sql
   -- Both tables reference the same data
   CREATE TABLE new_table
   USING DELTA
   LOCATION 's3://bucket/path/';
   ```
   Note: This will fail with LOCATION_OVERLAP unless you drop the original table first.

3. **Convert to managed table first**:
   ```sql
   -- Create managed table from external
   CREATE TABLE managed_table AS
   SELECT * FROM external_table;

   -- Now you can shallow clone the managed table
   CREATE TABLE clone_table
   SHALLOW CLONE managed_table;
   ```

**How to Verify:**

```bash
# Test should pass by verifying the expected error
pytest tests/test_shallow_clone.py::TestShallowCloneCrossBucket::test_shallow_clone_external_table_fails -v
```

---

## SQL Warehouse vs Databricks Connect

### Issue: Cross-Bucket/Cross-Region Queries Fail with Databricks Connect

**Symptom:**
```
An error occurred (403) when calling GetObject: Forbidden
```
or
```
org.apache.spark.SparkException: Job aborted due to stage failure:
Could not read from S3 location
```

**Root Cause:**

Databricks Connect (serverless) has different credential resolution behavior than SQL Warehouses:

- **Databricks Connect**: Uses a single credential context, cannot resolve credentials for paths outside the primary bucket
- **SQL Warehouse with Instance Profile**: Can assume different roles/credentials for different S3 buckets via the instance profile

**When This Happens:**

Your tables have partitions in multiple S3 buckets (cross-bucket scenario) or regions (cross-region scenario). Queries need to read from `s3://bucket-A/` and `s3://bucket-B/` in the same query.

**How to Diagnose:**

```bash
# Check table locations
databricks sql-warehouse query \
  --warehouse-id <id> \
  --query "DESCRIBE DETAIL catalog.schema.cross_bucket_table" | \
  jq '.location'

# Check partition locations
databricks sql-warehouse query \
  --warehouse-id <id> \
  --query "SHOW PARTITIONS catalog.schema.cross_bucket_table" | \
  jq '.[].location'

# If partitions span multiple buckets -> need SQL Warehouse
```

**Solution:**

**Step 1: Configure SQL Warehouse (see Instance Profile Configuration above)**

```bash
export HIVE_TO_DELTA_TEST_WAREHOUSE_ID=<warehouse_id>
```

**Step 2: Use SQL Warehouse connection in tests**

```python
@pytest.mark.cross_bucket
def test_cross_bucket_query(spark, sql_warehouse_connection, ...):
    """Test cross-bucket queries using SQL Warehouse."""

    # Use SQL Warehouse for queries
    if sql_warehouse_connection is None:
        pytest.fail(
            "SQL Warehouse connection required for cross-bucket queries. "
            "Set HIVE_TO_DELTA_TEST_WAREHOUSE_ID environment variable."
        )

    # Query via SQL Warehouse
    count = sql_warehouse_connection.get_count(table_name)
    sample = sql_warehouse_connection.get_sample(table_name, limit=5)

    assert count > 0
    assert len(sample) > 0
```

**Step 3: Mark tests appropriately**

```python
# Standard tables - can use Databricks Connect
@pytest.mark.standard
def test_standard_table(spark, ...):
    result = spark.sql(f"SELECT * FROM {table}").collect()

# Cross-bucket/region - must use SQL Warehouse
@pytest.mark.cross_bucket
def test_cross_bucket_table(sql_warehouse_connection, ...):
    result = sql_warehouse_connection.get_sample(table)
```

**How to Verify:**

```bash
# Run tests by category
make test-standard        # Uses Databricks Connect
make test-cross-bucket    # Uses SQL Warehouse
make test-cross-region    # Uses SQL Warehouse
```

---

## Quick Diagnostic Commands

Run these commands in order to isolate infrastructure issues:

### 1. AWS Credentials
```bash
# Check identity
aws sts get-caller-identity

# Expected: Account 471112853095
# If wrong account or error: aws sso login --profile fe-sandbox

# Test S3 access
aws s3 ls s3://htd-east-1a/
aws s3 ls s3://htd-east-1b/
aws s3 ls s3://htd-west-2/

# Test Glue access
aws glue get-database --name hive_to_delta_test
aws glue get-tables --database-name hive_to_delta_test --query 'TableList[].Name'
```

### 2. Databricks Workspace Configuration
```bash
# Check storage credential
databricks storage-credentials get htd_storage_credential --output json | \
  jq '{name, aws_iam_role}'

# Get external ID (needed for IAM trust policy)
databricks storage-credentials get htd_storage_credential --output json | \
  jq -r '.aws_iam_role.external_id'

# Check external locations
databricks external-locations list --output json | \
  jq '.[] | {name, url, credential_name}'

# Verify workspace instance profile
databricks warehouses get-workspace-warehouse-config --output json | \
  jq '{instance_profile_arn, security_policy}'
```

### 3. SQL Warehouse Health
```bash
# List warehouses
databricks warehouses list --output json | jq '.warehouses[] | {id, name, state}'

# Check specific warehouse
export WAREHOUSE_ID=<your-warehouse-id>
databricks warehouses get $WAREHOUSE_ID --output json | \
  jq '{name, state, health, instance_profile_arn}'

# Expected:
# {
#   "state": "RUNNING",
#   "health": {
#     "status": "HEALTHY"
#   }
# }

# If not RUNNING/HEALTHY:
databricks warehouses stop $WAREHOUSE_ID
databricks warehouses start $WAREHOUSE_ID
```

### 4. IAM Configuration
```bash
# Check IAM role trust policy
aws iam get-role --role-name htd-role --query 'Role.AssumeRolePolicyDocument'

# Should include:
# 1. Unity Catalog master role (with external ID from storage credential)
# 2. Serverless compute role (with databricks-serverless-<WORKSPACE_ID>)

# Check instance profile exists
aws iam list-instance-profiles --query "InstanceProfiles[?InstanceProfileName=='htd-instance-profile']"
```

### 5. Test Table Availability
```bash
# Check test tables exist in Glue
aws glue get-tables --database-name hive_to_delta_test --query 'TableList[].Name'

# Expected: standard_table, cross_bucket_table, cross_region_table

# Check converted tables in Unity Catalog
export CATALOG=fe_randy_pitcher_workspace_catalog
export SCHEMA=hive_to_delta_tests

databricks tables list --catalog $CATALOG --schema $SCHEMA --output json | \
  jq '.[] | {name, table_type, storage_location}'
```

### 6. Run Tests
```bash
# Set environment variables
export AWS_PROFILE=fe-sandbox
export DATABRICKS_CONFIG_PROFILE=fe
export HIVE_TO_DELTA_TEST_WAREHOUSE_ID=<warehouse-id>
export HIVE_TO_DELTA_TEST_CATALOG=fe_randy_pitcher_workspace_catalog
export HIVE_TO_DELTA_TEST_SCHEMA=hive_to_delta_tests
export HIVE_TO_DELTA_TEST_GLUE_DATABASE=hive_to_delta_test

# Run all tests
make test

# Or run by category
make test-standard       # Should pass with any configuration
make test-cross-bucket   # Requires SQL Warehouse
make test-cross-region   # Requires SQL Warehouse
```

---

## Configuration Checklist

Before running tests, verify all these are configured correctly:

| Component | Check | How to Verify |
|-----------|-------|---------------|
| **AWS Credentials** | Correct profile, not expired | `aws sts get-caller-identity` |
| **S3 Buckets** | Exist and accessible | `aws s3 ls s3://htd-east-1a/` |
| **Glue Database** | Exists with test tables | `aws glue get-tables --database-name hive_to_delta_test` |
| **IAM Role** | Has S3/Glue permissions | `aws iam get-role --role-name htd-role` |
| **IAM Trust Policy** | Includes UC + serverless statements | `aws iam get-role --role-name htd-role \| jq '.Role.AssumeRolePolicyDocument'` |
| **Instance Profile** | Created and linked to role | `aws iam get-instance-profile --instance-profile-name htd-instance-profile` |
| **Storage Credential** | Points to IAM role | `databricks storage-credentials get htd_storage_credential` |
| **External Locations** | Created for all buckets | `databricks external-locations list` |
| **Workspace Config** | Instance profile assigned | `databricks warehouses get-workspace-warehouse-config` |
| **SQL Warehouse** | RUNNING + HEALTHY | `databricks warehouses get <id>` |
| **Environment Variables** | All set correctly | `env \| grep HIVE_TO_DELTA` |

---

## Common Error Messages Reference

Quick lookup for error messages you might encounter:

| Error Message | Section | Quick Fix |
|--------------|---------|-----------|
| `NoCredentialsError` | [AWS Credentials](#aws-credentials-issues) | `aws sso login --profile fe-sandbox` |
| `TokenRetrievalError` | [AWS Credentials](#aws-credentials-issues) | `aws sso login --profile fe-sandbox` |
| `Account ID mismatch` | [AWS Credentials](#aws-credentials-issues) | `export AWS_PROFILE=fe-sandbox` |
| `SQL Warehouse DEGRADED` | [Instance Profile](#instance-profile-configuration) | Configure workspace instance profile |
| `LOCATION_OVERLAP` | [Unity Catalog](#unity-catalog-location-errors) | Reuse existing table or use different location |
| `Column 'X' cannot be resolved` | [Test Schema](#test-schema-mismatches) | Check actual schema with `DESCRIBE TABLE` |
| `CANNOT_SHALLOW_CLONE_NON_UC_MANAGED_TABLE` | [Shallow Clone](#shallow-clone-limitations) | Expected for external tables - verify error instead |
| `403 Forbidden` (S3) | [SQL Warehouse vs Connect](#sql-warehouse-vs-databricks-connect) | Use SQL Warehouse for cross-bucket queries |
| `credentialName = None` | Instance Profile + Storage Credential | Verify IAM trust policy external ID matches UC |

---

## Additional Resources

- **README.md**: Quick start guide and API reference
- **TEST_RESULTS_FINAL.md**: Summary of test improvements and fixes
- **ARCHITECTURE.md**: System design and component interactions
- **Databricks Unity Catalog Docs**: https://docs.databricks.com/data-governance/unity-catalog/
- **AWS IAM Instance Profiles**: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2_instance-profiles.html
