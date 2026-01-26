# Integration Test Setup Scripts

Scripts for creating and verifying AWS Glue test tables for hive_to_delta integration tests.

## Prerequisites

- AWS Profile: `aws-sandbox-field-eng_databricks-sandbox-admin`
- AWS Region: `us-east-1` (primary)
- Python environment with dependencies installed

## Test Scenarios

### Scenario 1: Standard Table (`test_table_standard`)
**Single bucket, single region**

- Database: `hive_to_delta_test`
- Table Location: `s3://htd-east-1a/standard/`
- Partitions:
  - `date=2024-01-01` → `s3://htd-east-1a/standard/date=2024-01-01/`
  - `date=2024-01-02` → `s3://htd-east-1a/standard/date=2024-01-02/`

### Scenario 2: Cross-Bucket Table (`test_table_cross_bucket`)
**Multiple buckets, same region**

- Database: `hive_to_delta_test`
- Table Location: `s3://htd-east-1a/cross_bucket/`
- Partitions:
  - `date=2024-01-01` → `s3://htd-east-1a/cross_bucket/date=2024-01-01/` (bucket A)
  - `date=2024-01-02` → `s3://htd-east-1b/cross_bucket/date=2024-01-02/` (bucket B)

### Scenario 3: Cross-Region Table (`test_table_cross_region`)
**Different regions**

- Database: `hive_to_delta_test`
- Table Location: `s3://htd-east-1a/cross_region/`
- Partitions:
  - `date=2024-01-01` → `s3://htd-east-1a/cross_region/date=2024-01-01/` (us-east-1)
  - `date=2024-01-02` → `s3://htd-west-2/cross_region/date=2024-01-02/` (us-west-2)

## Schema

All tables share the same schema:

| Column | Type | Description |
|--------|------|-------------|
| `id` | bigint | Sequential ID (1-5 per partition) |
| `name` | string | Record name with partition value embedded |
| `value` | double | Numeric value (10.5, 21.0, 31.5, 42.0, 52.5) |
| `created_at` | timestamp | Timestamp matching partition date |

**Partition Key:** `date` (string)

## Usage

### Create Test Tables

Run the setup script to create all test tables and data:

```bash
# From project root
python scripts/setup_integration_test_tables.py
```

This will:
1. Create or verify the Glue database `hive_to_delta_test`
2. Delete existing test tables (for fresh start)
3. Create 3 Glue tables with proper schema
4. Generate parquet data (5 rows per partition)
5. Upload parquet files to appropriate S3 locations
6. Register partitions in Glue with correct S3 locations

### Verify Test Tables

Run the verification script to check all tables and data:

```bash
# From project root
python scripts/verify_test_tables.py
```

This will:
1. Check each table exists in Glue
2. Verify table schema and location
3. List all partitions
4. Read parquet files from S3
5. Display row counts and sample data

### Clean Up

To remove all test tables:

```bash
# Using AWS CLI
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws glue delete-table --database-name hive_to_delta_test --table-name test_table_standard --region us-east-1

AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws glue delete-table --database-name hive_to_delta_test --table-name test_table_cross_bucket --region us-east-1

AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws glue delete-table --database-name hive_to_delta_test --table-name test_table_cross_region --region us-east-1
```

To remove S3 data:

```bash
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws s3 rm s3://htd-east-1a/standard/ --recursive --region us-east-1

AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws s3 rm s3://htd-east-1a/cross_bucket/ --recursive --region us-east-1

AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws s3 rm s3://htd-east-1b/cross_bucket/ --recursive --region us-east-1

AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws s3 rm s3://htd-east-1a/cross_region/ --recursive --region us-east-1

AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws s3 rm s3://htd-west-2/cross_region/ --recursive --region us-west-2
```

## Files

- `setup_integration_test_tables.py` - Creates test tables and data
- `verify_test_tables.py` - Verifies tables and data are correct
- `setup_fresh_hive_tables.py` - Legacy script with 5 scenarios (standard, scattered, cross-bucket, cross-region, recursive)

## Configuration

Both scripts use the following configuration:

```python
AWS_PROFILE = "aws-sandbox-field-eng_databricks-sandbox-admin"
DEFAULT_REGION = "us-east-1"
DATABASE_NAME = "hive_to_delta_test"
BUCKET_EAST_1A = "htd-east-1a"
BUCKET_EAST_1B = "htd-east-1b"
BUCKET_WEST_2 = "htd-west-2"
```

To modify these values, edit the constants at the top of each script.

## Expected Output

### Setup Script

```
Creating Integration Test Tables in AWS Glue
============================================================
AWS Profile: aws-sandbox-field-eng_databricks-sandbox-admin
AWS Region: us-east-1
Glue Database: hive_to_delta_test
S3 Buckets: htd-east-1a, htd-east-1b, htd-west-2
============================================================

Setting up: test_table_standard
Created Glue table: hive_to_delta_test.test_table_standard
Uploaded 5 rows to s3://htd-east-1a/standard/date=2024-01-01/data.parquet
Added partition {'date': '2024-01-01'} -> s3://htd-east-1a/standard/date=2024-01-01/
...

All tables created successfully!
Database: hive_to_delta_test
Total tables: 3
```

### Verification Script

```
VERIFICATION: Integration Test Tables
======================================================================

Table: test_table_standard
  Location: s3://htd-east-1a/standard/
  Columns: id, name, value, created_at
  Partition Keys: date
  Total Partitions: 2

  Partition: ['2024-01-01']
    Location: s3://htd-east-1a/standard/date=2024-01-01/
    Status: OK
    Row Count: 5
    Sample Names: record_2024-01-01_1, record_2024-01-01_2, record_2024-01-01_3
...
```

## Integration with Tests

These tables can be used in integration tests to verify:

1. Basic Hive-to-Delta conversion
2. Partition discovery and migration
3. Cross-bucket data handling
4. Cross-region data handling
5. Schema inference and validation
6. Data integrity after conversion

Example test usage:

```python
def test_convert_standard_table(spark):
    # Use test_table_standard for basic conversion test
    result = convert_table(
        glue_database="hive_to_delta_test",
        glue_table="test_table_standard",
        unity_catalog="main",
        unity_schema="test",
    )
    assert result.status == "success"
    assert result.rows_migrated == 10
```

## Notes

- The scripts use explicit AWS profile configuration in all boto3 calls
- Cross-region uploads automatically use the correct region for each bucket
- Existing tables are deleted before recreation to ensure clean state
- Each partition contains unique, identifiable data for verification
- All data files are named `data.parquet` following Hive conventions
