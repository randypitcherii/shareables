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

## Migration Workflow

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
| Delta/UC | ✅ Full access | ‼️ Full access, BUT likely to corrupt Hive reads |
| Hive/Glue | ✅ Works (ignores `_delta_log`) | ⚠️ Works, but likely to corrupt delta metadata (until you regenerate it, so not a huge deal) |

**Recommendation:** 
- Feel free to register hive tables as delta tables without worrying about breaking hive readers or writers
- Direct writes to the delta table will be a 1 way door, so do not do this until you are certain you will no longer be reading or writing with hive table connections
- Hive reads and writes are safe, but you may need to regenerate delta metadata based on the nature of changes
    - Hive appends won't corrupt the delta metadata, but the delta table won't show the new records until metadata is regenerated
    - Any other Hive write operation besides append will corrupt the delta metadata.
    - Delta metadata corruption in this case sounds scarier than it is - it just means the delta metadata will point to data files that either don't exist anymore or are no longer part of the hive table due to dynamic partition changes.
    - Delta metadata is reliable and simple to regenerate. It may be expensive to do constantly, but you'll have to run the library for yourself to determine that. There's no reason other than time and cost that you couldn't just schedule metadata refreshes after your hive writers are finished though 💪
- ✨ In a VERY cool twist, you can shallow clone the registered delta tables you convert!
      - this is extremely powerful as it allows you to "branch" your hive tables safely.
      - for migrations, you may want to shallow clone your delta tables and use those shallow clones to accept full read/write operations safely without impacting your hive table processes.
      - this let's you do pre-production validation of your migrated transformation pipelines and actually compare your outputs against live data with formal tests, like with dbt for example ✅

### Storage Cleanup

For tables with external partitions that receive UPDATE/DELETE operations:
- New data files go to the table root
- Old external files become orphaned over time
- Run `external_vacuum` periodically to clean up

For read-only or INSERT-only tables, no cleanup needed unless you perform any OPTIMIZE operations. OPTIMIZE often rewrites smaller files into a larger single file, which will look like deletes of the smaller files. Those smaller files are candidates for vacuum cleanup just like any traditional delete operation.

### Cold Storage (Glacier)

Manual Delta Log approach is safe for cold storage - it reads only Glue metadata, not data files.

‼️ You should still think twice before registering these tables. Do NOT register these tables until you have a rock solid access control pattern to guarantee no accidental reads against these cold storage data files are possible by accident. Your costs will skyrocket faster than you can say "cold storage object retrieval fees".

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
| scattered | 2 (1 partition in the same bucket but outside the table root) | ✅ | ✅ |
| cross_bucket | 2 (different buckets) | ✅ | ✅ |
| cross_region | 2 (us-east-1, us-west-2) | ✅ | ✅ |
| recursive | 3 (with non-hive-partitioned nested dirs) | ✅ | ✅ |

All operations tested: SELECT, UPDATE, DELETE, OPTIMIZE, VACUUM, external_vacuum.

‼️ Note - VACUUM calls will complete without error even when they fail to actually vacuum files outside of the table root. 
🧠 Tip - S3 Asset Inventory datasets can help a TON with monitoring and auditing this process. It is well worth enabling and applying a few simple charts on top of this data set during your migration.
