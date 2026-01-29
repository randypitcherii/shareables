# Migration Checklist

Use this checklist when migrating Hive tables from AWS Glue to Unity Catalog in production.

## Pre-Migration

### 1. Infrastructure Readiness

- [ ] IAM role created with S3, Glue, and LakeFormation permissions
- [ ] IAM trust policy includes Unity Catalog external ID
- [ ] IAM trust policy includes serverless compute statement (for SQL Warehouses)
- [ ] Instance profile created and wrapping the IAM role
- [ ] Storage credential created in Unity Catalog
- [ ] External ID in IAM matches Unity Catalog storage credential
- [ ] External locations created for all S3 buckets containing data
- [ ] SQL Warehouse configured with instance profile
- [ ] SQL Warehouse in RUNNING state and health is HEALTHY

**Verify:**
```bash
# Check IAM role
aws iam get-role --role-name your-role | jq '.Role.AssumeRolePolicyDocument'

# Check storage credential
databricks storage-credentials get your_credential

# Check external locations
databricks external-locations list

# Check SQL Warehouse
databricks warehouses get <warehouse_id> --output json | jq '{state, health}'
```

### 2. Source Data Validation

- [ ] All Glue tables accessible via AWS CLI
- [ ] Parquet files exist at partition locations
- [ ] S3 buckets readable with current credentials
- [ ] Partition metadata in Glue is accurate
- [ ] No pending schema changes in source tables
- [ ] Data quality validated in source tables

**Verify:**
```bash
# List Glue tables
aws glue get-tables --database-name your_db --region us-east-1 | \
  jq -r '.TableList[].Name'

# Check partition count
aws glue get-partitions --database-name your_db --table-name your_table | \
  jq '.Partitions | length'

# Sample partition location
aws glue get-partition \
  --database-name your_db \
  --table-name your_table \
  --partition-values 2024-01-01 | \
  jq -r '.Partition.StorageDescriptor.Location'

# Verify files exist
aws s3 ls s3://bucket/path/to/partition/
```

### 3. Target Environment Setup

- [ ] Unity Catalog catalog exists
- [ ] Unity Catalog schema exists
- [ ] Naming convention defined for converted tables
- [ ] Access controls planned for new tables
- [ ] Data retention policies documented
- [ ] Rollback plan prepared

**Verify:**
```bash
# Check catalog
databricks catalogs get your_catalog

# Check schema
databricks schemas get your_catalog.your_schema

# Test table creation
databricks sql query "
CREATE TABLE IF NOT EXISTS your_catalog.your_schema.test_table
USING DELTA
LOCATION 's3://test-bucket/test/'
"
```

## Migration Execution

### 4. Test Migration

- [ ] Single table converted successfully
- [ ] Converted table queryable in Unity Catalog
- [ ] Row counts match between Glue and Unity Catalog
- [ ] Partition counts match
- [ ] Query performance acceptable
- [ ] Standard Delta operations work (INSERT, UPDATE, DELETE)

**Test script:**
```python
from databricks.connect import DatabricksSession
from hive_to_delta import convert_single_table

spark = DatabricksSession.builder.getOrCreate()

# Convert test table
result = convert_single_table(
    spark=spark,
    glue_database="your_db",
    table_name="test_table",
    target_catalog="main",
    target_schema="test_schema",
)

assert result.success, f"Conversion failed: {result.error}"
print(f"Converted {result.file_count} files in {result.duration_seconds:.1f}s")

# Query test
df = spark.sql("SELECT COUNT(*) as cnt FROM main.test_schema.test_table")
count = df.collect()[0]['cnt']
print(f"Row count: {count}")
```

### 5. Bulk Migration

- [ ] List of tables to migrate prepared
- [ ] Tables prioritized by importance/usage
- [ ] Migration scheduled during low-usage window
- [ ] Monitoring setup for migration progress
- [ ] Error handling and logging configured
- [ ] Stakeholders notified of migration window

**Migration script:**
```python
import logging
from databricks.connect import DatabricksSession
from hive_to_delta import convert_tables, create_summary

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler()
    ]
)

spark = DatabricksSession.builder.getOrCreate()

# Migrate tables in batches
batches = [
    ["critical_table_1", "critical_table_2"],
    ["important_table_*"],  # Pattern-based
    ["other_tables_*"],
]

for i, tables in enumerate(batches, 1):
    logging.info(f"Starting batch {i}/{len(batches)}: {tables}")

    results = convert_tables(
        spark=spark,
        glue_database="your_db",
        tables=tables,
        target_catalog="main",
        target_schema="prod",
        max_workers=4,
    )

    summary = create_summary(results)
    logging.info(f"Batch {i} complete: {summary.succeeded}/{summary.total_tables} succeeded")

    # Log failures
    for result in results:
        if not result.success:
            logging.error(f"FAILED: {result.source_table} - {result.error}")
```

### 6. Validation

For each converted table:

- [ ] Row count matches source
- [ ] Partition count matches source
- [ ] Sample queries return expected results
- [ ] Schema matches source
- [ ] Table metadata correct (location, format, partitions)

**Validation script:**
```python
import boto3

def validate_table(spark, glue_db, glue_table, uc_table):
    # Get Glue partition count
    glue = boto3.client('glue', region_name='us-east-1')
    glue_partitions = glue.get_partitions(
        DatabaseName=glue_db,
        TableName=glue_table
    )
    glue_partition_count = len(glue_partitions['Partitions'])

    # Get Unity Catalog partition count
    uc_partition_count = spark.sql(f"""
        SELECT COUNT(DISTINCT partition_col) as cnt
        FROM {uc_table}
    """).collect()[0]['cnt']

    # Get row counts
    uc_row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {uc_table}").collect()[0]['cnt']

    print(f"Table: {uc_table}")
    print(f"  Glue partitions: {glue_partition_count}")
    print(f"  UC partitions: {uc_partition_count}")
    print(f"  UC rows: {uc_row_count}")
    print(f"  Match: {glue_partition_count == uc_partition_count}")

    return glue_partition_count == uc_partition_count
```

## Post-Migration

### 7. Cutover

- [ ] Application connection strings updated
- [ ] Users notified of new table locations
- [ ] Documentation updated with Unity Catalog paths
- [ ] Access controls applied to new tables
- [ ] Monitoring dashboards updated
- [ ] Alerts configured for new tables

### 8. Optimization

- [ ] Run `ANALYZE TABLE` to compute statistics
- [ ] Run `OPTIMIZE` to consolidate small files
- [ ] Enable Delta optimizations (auto-optimize, auto-vacuum)
- [ ] Configure Z-ordering on commonly filtered columns
- [ ] Set up retention policies

**Optimization script:**
```python
tables = [
    "main.prod.table1",
    "main.prod.table2",
    "main.prod.table3",
]

for table in tables:
    # Analyze
    spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS")

    # Optimize
    spark.sql(f"OPTIMIZE {table}")

    # Z-order (if applicable)
    spark.sql(f"OPTIMIZE {table} ZORDER BY (commonly_filtered_column)")

    # Enable auto-optimize
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')")
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true')")
```

### 9. Cleanup

- [ ] Verify Glue tables still accessible (keep as backup)
- [ ] Document original Glue table locations
- [ ] Plan for eventual Glue table deprecation
- [ ] Remove test tables and schemas
- [ ] Archive migration logs
- [ ] Update runbooks with new procedures

### 10. Monitoring

Set up monitoring for:

- [ ] Query performance on converted tables
- [ ] Table sizes and file counts
- [ ] External file orphan counts
- [ ] Vacuum operations
- [ ] Access patterns and usage
- [ ] Error rates

**Sample monitoring query:**
```sql
-- Table health check
SELECT
  table_catalog,
  table_schema,
  table_name,
  size_in_bytes / 1024 / 1024 / 1024 as size_gb,
  num_files,
  CASE
    WHEN num_files > 10000 THEN 'WARNING: Too many small files'
    WHEN size_in_bytes = 0 THEN 'ERROR: Empty table'
    ELSE 'OK'
  END as health_status
FROM system.information_schema.tables
WHERE table_catalog = 'main'
  AND table_schema = 'prod'
ORDER BY size_gb DESC;
```

## Rollback Procedure

If issues occur:

1. **Stop using Unity Catalog tables**
   - Revert application connection strings
   - Point queries back to Glue tables

2. **Investigate issues**
   - Check logs: `migration.log`
   - Verify credentials and permissions
   - Test individual table queries

3. **Fix and retry**
   - Resolve identified issues
   - Drop problematic tables: `DROP TABLE IF EXISTS catalog.schema.table`
   - Reconvert: `convert_single_table(...)`

4. **Document lessons learned**
   - Update this checklist
   - Share findings with team

## Maintenance Schedule

### Daily
- [ ] Monitor query performance
- [ ] Check for failed queries
- [ ] Review error logs

### Weekly
- [ ] Check table file counts
- [ ] Review table sizes
- [ ] Identify tables needing optimization

### Monthly
- [ ] Run standard VACUUM (table root files)
- [ ] Run external vacuum (external partition files)
- [ ] Review and update access controls
- [ ] Archive old migration logs

**Vacuum script:**
```python
from hive_to_delta import vacuum_external_files

tables = [
    "main.prod.table1",
    "main.prod.table2",
]

for table in tables:
    # Standard vacuum (table root)
    spark.sql(f"VACUUM {table} RETAIN 168 HOURS")

    # External vacuum (external partitions)
    result = vacuum_external_files(
        spark=spark,
        table_name=table,
        retention_hours=168,
        dry_run=False,
    )
    print(f"{table}: Deleted {len(result.deleted_files)} external files")
```

## Success Criteria

Migration is successful when:

- [ ] All tables converted without errors
- [ ] All validation checks pass
- [ ] Query performance meets SLAs
- [ ] Users successfully accessing new tables
- [ ] No increase in error rates
- [ ] Rollback plan tested and ready
- [ ] Team trained on new procedures
- [ ] Documentation complete and accurate

## Contact and Escalation

**Issues during migration:**
1. Check troubleshooting guide: [README.md#troubleshooting](README.md#troubleshooting)
2. Review architecture: [ARCHITECTURE.md](ARCHITECTURE.md)
3. Check test examples: [tests/test_convert.py](tests/test_convert.py)
4. Contact: [Add your team contact info]

**Escalation path:**
1. Data Engineer on-call
2. Platform team lead
3. Databricks support (if under contract)
