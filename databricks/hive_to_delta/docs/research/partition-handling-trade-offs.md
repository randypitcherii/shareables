# Partition Handling Trade-offs for hive_to_delta

**Research ID:** htd-2ha
**Date:** 2026-02-17

## Context

The `hive_to_delta` library registers existing parquet files as Delta tables by writing a Delta log (`_delta_log/00000000000000000000.json`). When source Hive tables use `key=value/` directory partitioning, we need to decide how to handle partition metadata in the generated Delta log.

This document investigates what happens when files in `key=value/` directories are registered as **non-partitioned** Delta tables, and what trade-offs are involved.

---

## Question 1: Does Spark still read data correctly without partition metadata?

**Answer: Yes, with a critical caveat.**

Spark can read all parquet files referenced in the Delta log regardless of whether partition columns are declared. The Delta log contains explicit file paths (`add` actions), so Spark finds and reads every file. All rows from the parquet files themselves will be returned.

**The caveat:** Hive-style partitioning typically stores partition column values **only in the directory path**, not inside the parquet files. These are "virtual columns" that Spark infers from the directory structure during partition discovery. If the Delta log does not declare partition columns, and the parquet files do not contain those columns internally, then **the partition columns will be entirely missing from the table schema**.

For example, if files are at `s3://bucket/table/year=2024/month=01/data.parquet` and the parquet file itself does not contain `year` or `month` columns, a non-partitioned Delta table will:
- Return all rows from the data columns stored in the parquet files
- **Not include `year` or `month` columns at all** -- those values are lost

This is because Delta Lake reads data using metadata from the transaction log, not by performing directory-based partition discovery like vanilla Spark parquet reading does.

**Sources:**
- [Spark Parquet Partition Discovery](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
- [Pros and Cons of Hive-style Partitioning - Delta Lake](https://delta.io/blog/pros-cons-hive-style-partionining/)
- [Converting from Parquet to Delta Lake - Delta Lake](https://delta.io/blog/2022-09-23-convert-parquet-to-delta/)

---

## Question 2: Performance implications without partition metadata

**Answer: No partition pruning, but data skipping still works partially.**

Without partition columns in the Delta log metadata:

- **Partition pruning is impossible.** Delta Lake uses `partitionColumns` from the metadata action and `partitionValues` from each `add` action to skip entire file groups. Without this metadata, queries like `WHERE year = '2024'` cannot eliminate files at the partition level.

- **Data skipping via file-level stats still works** for columns that exist in the parquet files. Delta Lake automatically collects min/max statistics for the first N columns (default 32) in each file. Queries filtering on these columns can still skip files whose min/max ranges don't match.

- **Z-ORDER and Dynamic File Pruning (DFP)** can partially compensate for missing partition pruning on non-partitioned tables. DFP is "especially efficient when running join queries on non-partitioned tables." However, this requires running `OPTIMIZE ZORDER BY` after table creation, which requires the columns to exist in the data.

- **Net impact:** For tables where partition columns are missing from the schema entirely (Question 1 caveat), you can't even write a `WHERE year = '2024'` filter because the column doesn't exist. For tables where partition values ARE in the parquet files, you lose partition pruning but retain file-level data skipping -- queries will be slower for large tables but still functional.

**Sources:**
- [Faster SQL Queries with Dynamic File Pruning - Databricks](https://www.databricks.com/blog/2020/04/30/faster-sql-queries-on-delta-lake-with-dynamic-file-pruning.html)
- [Boost Delta Lake Performance with Data Skipping and Z-Order - Salesforce Engineering](https://engineering.salesforce.com/boost-delta-lake-performance-with-data-skipping-and-z-order-75c7e6c59133/)
- [Best Practices - Delta Lake](https://docs.delta.io/best-practices/)
- [Delta Lake Optimizations](https://docs.delta.io/optimizations-oss/)

---

## Question 3: How to retroactively add partitioning

**Answer: You cannot add partition columns to an existing Delta table. Full rewrite is required.**

Delta Lake does not support `ALTER TABLE ... ADD PARTITION` or any mechanism to retroactively change the partition scheme of an existing table. The partitioning structure is set at table creation time in the Delta log metadata and is immutable.

To add partitioning after the fact, users must:

1. **Create a new partitioned table** and copy data into it:
   ```sql
   CREATE TABLE new_table
   USING delta
   PARTITIONED BY (year, month)
   AS SELECT * FROM old_table
   ```

2. **Or use `REPLACE TABLE`**:
   ```sql
   REPLACE TABLE my_table
   USING DELTA
   PARTITIONED BY (year, month)
   AS SELECT * FROM my_table
   ```

3. **Or read and rewrite** with `.partitionBy()` in PySpark.

All approaches require a full data rewrite, which is expensive for large tables. This makes getting the partition configuration right at registration time very important.

**Sources:**
- [How to add partition for existing Delta table - Databricks Community](https://community.databricks.com/t5/data-engineering/how-to-add-the-partition-for-an-existing-delta-table/td-p/27333)
- [Adding and Deleting Partitions in Delta Lake - Delta Lake](https://delta.io/blog/2023-01-18-add-remove-partition-delta-lake/)
- [Update Table Schema - Databricks](https://docs.databricks.com/aws/en/delta/update-schema)

---

## Question 4: Correctness when partition columns only exist in directory paths

**Answer: Those columns will be missing from the Delta table schema.**

This is the most critical finding. In Hive-style partitioning:

- Partition column values are stored **only in the directory path** (e.g., `year=2024/month=01/`), not as columns inside the parquet files.
- When Spark reads raw parquet with partition discovery, it automatically adds these virtual columns to the DataFrame.
- **Delta Lake does NOT perform partition discovery from directory paths.** It relies entirely on the transaction log metadata.

If you register files as a non-partitioned Delta table:
- The `partitionColumns` in the metadata action will be `[]`
- The `partitionValues` in each `add` action will be `{}`
- The schema will not include the partition columns
- **Those columns and their values are effectively lost** -- queries cannot access `year` or `month` at all

If you register files WITH partition columns declared:
- The `partitionColumns` in metadata will list them (e.g., `["year", "month"]`)
- Each `add` action includes `partitionValues` (e.g., `{"year": "2024", "month": "01"}`)
- Delta reconstructs these columns at read time from the `partitionValues` map
- The columns appear in the schema and are queryable

**This is not just a performance issue -- it's a correctness issue.** Omitting partition columns means losing data.

**Sources:**
- [Converting from Parquet to Delta Lake fails - Databricks KB](https://kb.databricks.com/delta/parquet-to-delta-fails)
- [CONVERT TO DELTA - Databricks](https://docs.databricks.com/en/sql/language-manual/delta-convert-to-delta.html)
- [Spark Parquet Partition Discovery](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)

---

## Validation: What does CONVERT TO DELTA do?

Databricks' own `CONVERT TO DELTA` command **refuses to proceed** if it detects `key=value/` directories but no partition schema is specified. It throws:

```
AnalysisException: Expecting 0 partition column(s): [], but found 1 partition column(s): [<column_name>] from parsing the file name.
```

This confirms that Databricks considers registering partitioned parquet as non-partitioned to be an error condition, not a valid alternative.

**Source:** [Converting from Parquet to Delta Lake fails - Databricks KB](https://kb.databricks.com/delta/parquet-to-delta-fails)

---

## Recommendation for hive_to_delta

### Partition columns should be included by default when detected.

Rationale:

1. **Correctness:** Omitting partition columns from Hive-style partitioned tables causes **data loss** -- those column values become inaccessible. This is not a performance trade-off; it's a broken table.

2. **Databricks precedent:** `CONVERT TO DELTA` treats this as an error, not a warning. Our library should be at least as cautious.

3. **No recovery path:** You cannot retroactively add partitioning to a Delta table. Users who register without partitions must do a full rewrite to fix it.

### Specific implementation recommendations:

- **Auto-detect partition columns** from `key=value/` directory structure during file discovery (the library already captures `partition_keys` on `TableInfo` and `partition_values` on `ParquetFileInfo`).

- **Warn users loudly** if `key=value/` directories are detected but no partition columns are specified. The warning should explain that partition column data will be lost.

- **Consider failing by default** (like `CONVERT TO DELTA` does) when `key=value/` directories are detected without matching partition column configuration. Provide an explicit opt-out flag like `ignore_partitions=True` for users who know what they're doing.

- **Include partition columns in the Delta schema.** When partition columns are specified, ensure they appear in both the `schemaString` and `partitionColumns` of the metadata action, and that `partitionValues` are populated on each `add` action (the library already does this when `partition_keys` is provided).
