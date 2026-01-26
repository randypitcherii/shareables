# Integration Test Coverage for hive_to_delta

This document summarizes the comprehensive integration test suite for the hive_to_delta package.

## Test Files Overview

### 1. conftest.py
**Purpose**: Pytest configuration and shared fixtures for all tests.

**Key Fixtures**:
- `spark`: Databricks Connect SparkSession (session-scoped)
- `glue_database`: Test Glue database name from environment
- `aws_region`: AWS region for Glue/S3 operations
- `s3_clients`: S3 clients for multiple regions (us-east-1, us-west-2)
- `target_catalog`: Unity Catalog name for test tables
- `target_schema`: Unity Catalog schema name for test tables
- `cleanup_tables`: Table cleanup fixture (drops tables after test module)
- `sql_warehouse_id`: SQL warehouse ID for cross-bucket/region queries
- `query_via_sql_warehouse()`: Helper function to execute SQL via SQL Warehouse API

**Environment Variables**:
- `DATABRICKS_CONFIG_PROFILE`: Databricks profile to use
- `DATABRICKS_CLUSTER_ID`: Cluster ID for classic clusters
- `HIVE_TO_DELTA_TEST_GLUE_DATABASE`: Glue database (default: "hive_to_delta_test")
- `AWS_REGION`: AWS region (default: "us-east-1")
- `HIVE_TO_DELTA_TEST_CATALOG`: UC catalog (default: "fe_randy_pitcher_workspace_catalog")
- `HIVE_TO_DELTA_TEST_SCHEMA`: UC schema (default: "hive_to_delta_tests")
- `HIVE_TO_DELTA_TEST_WAREHOUSE_ID`: SQL warehouse ID for cross-bucket/region tests

---

### 2. test_convert.py (8 tests)
**Purpose**: Integration tests for Hive to Delta conversion functionality.

**Test Scenarios**:
- `standard`: Tables with all partitions in single S3 location
- `cross_bucket`: Tables with partitions across multiple S3 buckets
- `cross_region`: Tables with partitions across multiple AWS regions

**Test Cases**:

#### Single Table Conversion (3 tests)
- `test_convert_single_table_standard`: Convert standard table
- `test_convert_single_table_cross_bucket`: Convert cross-bucket table
- `test_convert_single_table_cross_region`: Convert cross-region table

**Validates**:
- ConversionResult.success is True
- Delta log written to S3 (_delta_log/00000000000000000000.json exists)
- Table is queryable (via spark.sql or SQL Warehouse API)
- File count > 0

#### Bulk Conversion (2 tests - SKIPPED)
- `test_convert_tables_bulk`: Convert multiple tables with max_workers=2
- `test_convert_tables_pattern`: Convert tables matching glob pattern "test_*"

**Note**: Bulk tests are skipped pending creation of bulk test tables in Glue.

#### Scenario Workflows (3 tests)
- `TestStandardScenario::test_standard_conversion_workflow`: Full workflow for standard tables
- `TestCrossBucketScenario::test_cross_bucket_conversion_workflow`: Full workflow for cross-bucket tables
- `TestCrossRegionScenario::test_cross_region_conversion_workflow`: Full workflow for cross-region tables

---

### 3. test_vacuum.py (10 tests)
**Purpose**: Tests for vacuum_external_files() which cleans up orphaned files at absolute S3 paths.

**Test Classes**:

#### TestVacuumExternalDryRun (3 tests)
- `test_vacuum_external_dry_run_finds_orphaned_files`: Identifies orphaned external files
- `test_vacuum_external_dry_run_returns_result`: Returns VacuumResult with dry_run=True
- `test_vacuum_external_dry_run_skips_active_files`: Does not flag currently active files

#### TestVacuumExternalDelete (2 tests)
- `test_vacuum_external_delete_removes_files`: Actually deletes orphaned files when dry_run=False
- `test_vacuum_external_delete_handles_multiple_buckets`: Handles deletions across multiple S3 buckets

#### TestVacuumRespectsRetention (5 tests)
- `test_vacuum_respects_retention_skips_recent_files`: Files within retention period are not deleted
- `test_vacuum_respects_retention_deletes_old_files`: Files outside retention period are marked for deletion
- `test_vacuum_respects_custom_retention`: Custom retention periods are honored
- `test_vacuum_skips_internal_paths`: Files inside table root are skipped (standard VACUUM handles these)
- `test_vacuum_enforces_minimum_retention_period`: Rejects retention periods < 168 hours (7 days)

**Key Behaviors Tested**:
- External vacuum only handles files with absolute S3 paths OUTSIDE table root
- Standard VACUUM handles files under table root
- Retention period filtering (default: 7 days / 168 hours)
- Multi-bucket and multi-region file detection
- Error handling for invalid tables and parameters

---

### 4. test_operations.py (15 tests)
**Purpose**: Integration tests for Delta operations on converted tables.

**Test Class**: TestDeltaOperations

**Setup**: Uses `converted_tables` fixture which:
1. Converts Glue tables to Delta format once per module
2. Returns mapping of scenario -> UC table name
3. Automatically cleans up tables after module

**Test Cases** (each parameterized across 3 scenarios: standard, cross_bucket, cross_region):

#### SELECT Tests (3 tests)
- `test_select`: Verify basic SELECT and COUNT queries work
  - Validates row count > 0
  - Retrieves sample rows
  - Verifies data structure

#### INSERT Tests (3 tests)
- `test_insert`: Verify INSERT creates new parquet files
  - Checks row count increases
  - Verifies parquet file count
  - Validates inserted row is readable
  - Files written to table root location

#### UPDATE Tests (3 tests)
- `test_update`: Verify UPDATE with copy-on-write semantics
  - Inserts test row
  - Updates values
  - Validates changes are visible
  - Cleans up test data

#### OPTIMIZE Tests (3 tests)
- `test_optimize`: Verify OPTIMIZE compacts files
  - Creates small files via multiple inserts
  - Runs OPTIMIZE
  - Validates file count reduced or unchanged
  - Verifies data integrity preserved

#### VACUUM Tests (3 tests)
- `test_vacuum`: Verify standard VACUUM works
  - Creates old file versions via UPDATE
  - Runs VACUUM with default retention (7 days)
  - Verifies data integrity
  - Note: Uses default retention since retentionDurationCheck cannot be disabled via Databricks Connect serverless

**Total Coverage**: 5 operations × 3 scenarios = 15 tests

---

### 5. test_shallow_clone.py (9 tests)
**Purpose**: Tests for shallow clone functionality on converted Delta tables.

**Test Classes**:

#### TestShallowCloneStandard (3 tests)
- `test_shallow_clone_standard_creates_clone`: Create shallow clone of standard table
  - Verifies clone exists
  - Validates data count matches source
  - Checks CLONE operation in history
- `test_shallow_clone_standard_preserves_schema`: Schema preservation
  - Compares column names and types
  - Validates all columns match
- `test_shallow_clone_standard_partitioned_table`: Partitioned table cloning
  - Verifies partition columns preserved
  - Validates partitioning structure

#### TestShallowCloneCrossBucket (2 tests)
- `test_shallow_clone_cross_bucket_creates_clone`: Clone table with external bucket data
  - Validates clone is queryable
  - Verifies row counts match
- `test_shallow_clone_cross_bucket_preserves_file_references`: File reference behavior (unit test with mocks)

#### TestShallowCloneQueryable (4 tests)
- `test_shallow_clone_queryable_select`: SELECT queries on clones
  - Basic SELECT
  - WHERE predicates
  - Aggregations (SUM)
- `test_shallow_clone_queryable_with_predicates`: Predicate pushdown
  - Partition predicates
  - Date predicates
- `test_shallow_clone_queryable_aggregations`: Complex aggregations
  - GROUP BY with SUM and COUNT
  - Multiple categories
- `test_shallow_clone_queryable_joins`: Table joins
  - JOIN between clone and dimension table
  - Validates join results

---

## Test Execution

### Run All Tests
```bash
cd databricks/hive_to_delta
source .venv/bin/activate
pytest tests/ -v
```

### Run Specific Test File
```bash
pytest tests/test_convert.py -v
pytest tests/test_vacuum.py -v
pytest tests/test_operations.py -v
pytest tests/test_shallow_clone.py -v
```

### Run Tests by Marker
```bash
# Standard scenario tests only
pytest tests/ -v -m standard

# Cross-bucket tests only
pytest tests/ -v -m cross_bucket

# Cross-region tests only
pytest tests/ -v -m cross_region
```

### Run Tests in Parallel
```bash
# Use pytest-xdist for parallel execution
pytest tests/ -v -n 4
```

---

## Test Markers

Defined in `pyproject.toml`:
- `standard`: Standard partition layout tests
- `cross_bucket`: Cross-bucket partition tests
- `cross_region`: Cross-region partition tests

---

## Integration Test Requirements

These tests require:

1. **Databricks Workspace**:
   - Unity Catalog enabled
   - Databricks Connect configured
   - SQL Warehouse (for cross-bucket/cross-region tests)

2. **AWS Resources**:
   - AWS Glue database with test tables
   - S3 buckets with test data
   - IAM credentials with Glue/S3 access

3. **Test Tables in Glue**:
   - `standard_table`: Single-location partitioned table
   - `cross_bucket_table`: Multi-bucket partitioned table
   - `cross_region_table`: Multi-region partitioned table

4. **Environment Configuration**:
   - `.env` file or environment variables set
   - Databricks profile configured in `~/.databrickscfg`

---

## Test Coverage Summary

| Test File | Test Count | Purpose |
|-----------|------------|---------|
| test_convert.py | 8 | Hive to Delta conversion |
| test_vacuum.py | 10 | External vacuum for orphaned files |
| test_operations.py | 15 | Delta operations (SELECT, INSERT, UPDATE, OPTIMIZE, VACUUM) |
| test_shallow_clone.py | 9 | Shallow clone creation and queries |
| **TOTAL** | **42** | **Comprehensive integration testing** |

---

## Known Limitations

1. **Bulk Conversion Tests**: Currently skipped pending creation of bulk test tables in Glue
2. **VACUUM Retention**: test_operations.py uses default 7-day retention since retentionDurationCheck cannot be disabled via Databricks Connect serverless
3. **Cross-Bucket/Cross-Region Queries**: Require SQL Warehouse with instance profile + fallback enabled on external locations (Databricks Connect credential resolution doesn't support this)

---

## Next Steps

To run the full test suite:

1. Set up AWS Glue test tables using `scripts/setup_fresh_hive_tables.py`
2. Configure environment variables in `.env`
3. Run tests: `pytest tests/ -v`
4. For parallel execution: `pytest tests/ -v -n 4`
5. Review test results and validate all scenarios pass

---

## References

- [Databricks Connect Documentation](https://docs.databricks.com/en/dev-tools/databricks-connect.html)
- [Delta Lake Documentation](https://docs.delta.io/latest/index.html)
- [AWS Glue API Reference](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api.html)
- [pytest Documentation](https://docs.pytest.org/)
