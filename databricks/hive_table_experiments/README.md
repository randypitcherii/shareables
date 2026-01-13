# Hive-to-Delta Migration: Evaluation Results

## Executive Summary

**No-copy migration of exotic Hive partition layouts to Delta Lake + Unity Catalog IS possible.**

| Partition Layout | CONVERT TO DELTA | Manual Delta Log | Data Copy Required |
|------------------|------------------|------------------|-------------------|
| Standard (all under root) | ✅ Works | ✅ Works | No |
| Scattered (external paths) | ❌ Loses data | ✅ Works | No |
| Cross-bucket | ❌ Loses data | ✅ Works | No |
| Cross-region | ❌ Loses data | ✅ Works | No |
| Recursive (nested dirs) | ❌ Fails | ✅ Works | No |

---

## What Works

### Manual Delta Log with Absolute S3 Paths

When external locations are properly configured, Delta tables can reference files across multiple S3 buckets using absolute paths in the transaction log.

| Operation | Via Cluster | Via Serverless SQL | Notes |
|-----------|-------------|-------------------|-------|
| SELECT | ✅ | ✅ | All partitions accessible |
| INSERT | ✅ | ✅ | New files go to table root |
| UPDATE | ✅ | ✅ | Reads external, writes to root |
| DELETE | ✅ | ✅ | Reads external, writes to root |
| OPTIMIZE | ✅ | ✅ | Compacts to table root |
| VACUUM | ⚠️ Partial | ⚠️ Partial | See limitation below |

### Requirements

1. **External location** for each S3 bucket referenced in the Delta log
2. **Storage credential** with IAM access to all buckets (can be shared)
3. **No pre-existing `_delta_log/`** at the source Parquet locations

---

## What Doesn't Work

### CONVERT TO DELTA with External Partitions

CONVERT TO DELTA only scans files under the specified table root path. It silently ignores partitions stored at external locations.

```
# This LOSES the us-west partition if it's at an external S3 path
CONVERT TO DELTA parquet.`s3://bucket/table/` PARTITIONED BY (region STRING)
```

**Result:** Table converts successfully but external partition data is missing. No error is raised.

### VACUUM on External Files

**VACUUM cannot delete files at absolute S3 paths outside the table root.**

VACUUM scans the filesystem under the table location to find orphaned files. Files referenced via absolute paths in other buckets are outside its scan scope.

| File Location | VACUUM Deletes? |
|---------------|-----------------|
| Under table root | ✅ Yes |
| External bucket/path | ❌ No |

**Impact:** After UPDATE/DELETE operations, old files at external locations become orphaned (marked removed in Delta log but not physically deleted).

**Solution:** Use the `external_vacuum` module to clean up orphaned external files:

```python
from hive_table_experiments import vacuum_external_files_pure_python

# Dry run first
result = vacuum_external_files_pure_python(
    table_location="s3://bucket/path/to/delta/table/",
    retention_hours=0,
    dry_run=True
)
print(f"Would delete {len(result.orphaned_files)} files")

# Actually delete
result = vacuum_external_files_pure_python(..., dry_run=False)
```

---

## Migration Decision Tree

```
Is table partitioned?
├── No → CONVERT TO DELTA
└── Yes → Are ALL partitions under the table root?
    ├── Yes → CONVERT TO DELTA
    └── No → Manual Delta Log
              1. Create external locations for all S3 buckets
              2. Generate Delta log with absolute paths
              3. Register table in UC
```

---

## Migration Workflow

### Standard Tables (CONVERT TO DELTA)

```sql
-- 1. Convert in place
CONVERT TO DELTA parquet.`s3://bucket/table/` PARTITIONED BY (col STRING);

-- 2. Register in UC
CREATE TABLE catalog.schema.table USING DELTA LOCATION 's3://bucket/table/';

-- 3. Verify row count matches original
SELECT COUNT(*) FROM catalog.schema.table;
```

### Exotic Tables (Manual Delta Log)

```python
from hive_table_experiments import convert_hive_to_delta_uc

# Converts Hive table to Delta with absolute S3 paths
result = convert_hive_to_delta_uc(
    spark=spark,
    glue_database="my_database",
    table_name="my_table",
    uc_catalog="my_catalog",
    uc_schema="my_schema",
)

print(f"Converted {result.total_files} files, {result.total_rows} rows")
```

**Prerequisites:**
```sql
-- Create storage credential
CREATE STORAGE CREDENTIAL hive_migration_cred
WITH (AWS_IAM_ROLE = 'arn:aws:iam::ACCOUNT:role/migration-role');

-- Create external location for each bucket
CREATE EXTERNAL LOCATION bucket_a URL 's3://bucket-a/'
WITH (STORAGE CREDENTIAL hive_migration_cred);

CREATE EXTERNAL LOCATION bucket_b URL 's3://bucket-b/'
WITH (STORAGE CREDENTIAL hive_migration_cred);
```

---

## Post-Migration Considerations

### Read/Write Access

| Client | Read After Migration | Write After Migration |
|--------|---------------------|----------------------|
| Delta/UC | ✅ Full access | ✅ Full access |
| Hive/Glue | ⚠️ Works (ignores `_delta_log`) | ❌ Corrupts Delta table |

**Recommendation:** Migrate all writers to Delta/UC. Readers can transition gradually.

### Storage Cleanup

For tables with external partitions that receive UPDATE/DELETE operations:
- New data files go to the table root
- Old external files become orphaned over time
- Run `external_vacuum` periodically to clean up

For read-only or INSERT-only tables, no cleanup needed.

### Cold Storage (Glacier)

Manual Delta Log approach is safe for cold storage - it reads only Glue metadata, not data files.

---

## Tools Provided

| Module | Purpose |
|--------|---------|
| `delta_converter.py` | Convert Hive tables to Delta with absolute S3 paths |
| `external_vacuum.py` | Clean up orphaned files that VACUUM can't reach |
| `validation.py` | Validate conversions and S3 contents |

---

## Test Results

Validated scenarios with fresh test data:

| Scenario | Partitions | Conversion | All Ops via Serverless |
|----------|------------|------------|------------------------|
| standard | 2 (under root) | ✅ | ✅ |
| scattered | 2 (1 external path) | ✅ | ✅ |
| cross_bucket | 2 (different buckets) | ✅ | ✅ |
| cross_region | 2 (us-east-1, us-west-2) | ✅ | ✅ |
| recursive | 3 (nested dirs) | ✅ | ✅ |

All operations tested: SELECT, UPDATE, DELETE, OPTIMIZE, VACUUM, external_vacuum.
