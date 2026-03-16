# Quick Reference - Test Tables Commands

## AWS CLI Commands

All commands assume AWS profile: `aws-sandbox-field-eng_databricks-sandbox-admin`

### List All Test Tables

```bash
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws glue get-tables \
  --database-name hive_to_delta_test \
  --region us-east-1 \
  --query 'TableList[*].[Name, StorageDescriptor.Location]' \
  --output table
```

### Get Table Details

```bash
# Standard table
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws glue get-table \
  --database-name hive_to_delta_test \
  --table-name test_table_standard \
  --region us-east-1

# Cross-bucket table
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws glue get-table \
  --database-name hive_to_delta_test \
  --table-name test_table_cross_bucket \
  --region us-east-1

# Cross-region table
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws glue get-table \
  --database-name hive_to_delta_test \
  --table-name test_table_cross_region \
  --region us-east-1
```

### List Partitions

```bash
# Standard table partitions
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws glue get-partitions \
  --database-name hive_to_delta_test \
  --table-name test_table_standard \
  --region us-east-1 \
  --query 'Partitions[*].[Values[0], StorageDescriptor.Location]' \
  --output table

# Cross-bucket table partitions
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws glue get-partitions \
  --database-name hive_to_delta_test \
  --table-name test_table_cross_bucket \
  --region us-east-1 \
  --query 'Partitions[*].[Values[0], StorageDescriptor.Location]' \
  --output table

# Cross-region table partitions
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws glue get-partitions \
  --database-name hive_to_delta_test \
  --table-name test_table_cross_region \
  --region us-east-1 \
  --query 'Partitions[*].[Values[0], StorageDescriptor.Location]' \
  --output table
```

### List S3 Files

```bash
# Standard table files
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws s3 ls s3://htd-east-1a/standard/ --recursive --region us-east-1

# Cross-bucket table files
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws s3 ls s3://htd-east-1a/cross_bucket/ --recursive --region us-east-1

AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws s3 ls s3://htd-east-1b/cross_bucket/ --recursive --region us-east-1

# Cross-region table files
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws s3 ls s3://htd-east-1a/cross_region/ --recursive --region us-east-1

AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws s3 ls s3://htd-west-2/cross_region/ --recursive --region us-west-2
```

### Download Sample Parquet File

```bash
# Download and inspect a parquet file
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws s3 cp s3://htd-east-1a/standard/date=2024-01-01/data.parquet /tmp/test.parquet --region us-east-1

# Use Python to read it
python -c "
import pyarrow.parquet as pq
table = pq.read_table('/tmp/test.parquet')
print(table.to_pandas())
"
```

## Python Quick Commands

### Read Parquet from S3

```python
import boto3
import pyarrow.parquet as pq
import io

# Configure boto3
session = boto3.Session(
    profile_name='aws-sandbox-field-eng_databricks-sandbox-admin',
    region_name='us-east-1'
)
s3 = session.client('s3')

# Read parquet
response = s3.get_object(
    Bucket='htd-east-1a',
    Key='standard/date=2024-01-01/data.parquet'
)
buffer = io.BytesIO(response['Body'].read())
table = pq.read_table(buffer)

# Display data
print(table.to_pandas())
```

### Query Glue Table

```python
import boto3

session = boto3.Session(
    profile_name='aws-sandbox-field-eng_databricks-sandbox-admin',
    region_name='us-east-1'
)
glue = session.client('glue')

# Get table
response = glue.get_table(
    DatabaseName='hive_to_delta_test',
    Name='test_table_standard'
)

print(f"Location: {response['Table']['StorageDescriptor']['Location']}")
print(f"Columns: {response['Table']['StorageDescriptor']['Columns']}")
```

### List All Partitions

```python
import boto3

session = boto3.Session(
    profile_name='aws-sandbox-field-eng_databricks-sandbox-admin',
    region_name='us-east-1'
)
glue = session.client('glue')

# Get partitions
response = glue.get_partitions(
    DatabaseName='hive_to_delta_test',
    TableName='test_table_standard'
)

for partition in response['Partitions']:
    print(f"{partition['Values']} -> {partition['StorageDescriptor']['Location']}")
```

## Test Data Structure

### Standard Table
```
hive_to_delta_test.test_table_standard
├── s3://htd-east-1a/standard/
│   ├── date=2024-01-01/data.parquet (5 rows)
│   └── date=2024-01-02/data.parquet (5 rows)
└── Total: 10 rows
```

### Cross-Bucket Table
```
hive_to_delta_test.test_table_cross_bucket
├── s3://htd-east-1a/cross_bucket/
│   └── date=2024-01-01/data.parquet (5 rows)
├── s3://htd-east-1b/cross_bucket/
│   └── date=2024-01-02/data.parquet (5 rows)
└── Total: 10 rows
```

### Cross-Region Table
```
hive_to_delta_test.test_table_cross_region
├── s3://htd-east-1a/cross_region/ (us-east-1)
│   └── date=2024-01-01/data.parquet (5 rows)
├── s3://htd-west-2/cross_region/ (us-west-2)
│   └── date=2024-01-02/data.parquet (5 rows)
└── Total: 10 rows
```

## Sample Data

Each partition contains 5 rows with this structure:

```
id | name                    | value | created_at
---|-------------------------|-------|--------------------
1  | record_2024-01-01_1     | 10.5  | 2024-01-01 12:00:00
2  | record_2024-01-01_2     | 21.0  | 2024-01-01 12:00:00
3  | record_2024-01-01_3     | 31.5  | 2024-01-01 12:00:00
4  | record_2024-01-01_4     | 42.0  | 2024-01-01 12:00:00
5  | record_2024-01-01_5     | 52.5  | 2024-01-01 12:00:00
```

The `name` field embeds the partition date for easy identification.

## Cleanup Commands

### Delete Tables

```bash
for table in test_table_standard test_table_cross_bucket test_table_cross_region; do
  AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
  aws glue delete-table \
    --database-name hive_to_delta_test \
    --table-name $table \
    --region us-east-1
done
```

### Delete S3 Data

```bash
# Standard table
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws s3 rm s3://htd-east-1a/standard/ --recursive --region us-east-1

# Cross-bucket table
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws s3 rm s3://htd-east-1a/cross_bucket/ --recursive --region us-east-1

AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws s3 rm s3://htd-east-1b/cross_bucket/ --recursive --region us-east-1

# Cross-region table
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws s3 rm s3://htd-east-1a/cross_region/ --recursive --region us-east-1

AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws s3 rm s3://htd-west-2/cross_region/ --recursive --region us-west-2
```

## Troubleshooting

### Check AWS Credentials

```bash
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin aws sts get-caller-identity
```

### Verify S3 Bucket Access

```bash
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin aws s3 ls s3://htd-east-1a/ --region us-east-1
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin aws s3 ls s3://htd-east-1b/ --region us-east-1
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin aws s3 ls s3://htd-west-2/ --region us-west-2
```

### Verify Glue Database

```bash
AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
aws glue get-database \
  --name hive_to_delta_test \
  --region us-east-1
```
