# Testing Guide

Integration test suite for the `hive_to_delta` package.

## Quick Start

```bash
cd databricks/hive_to_delta

# All tests
make test

# By category (no infrastructure required)
make test-unit           # Unit tests (~7s)
make test-composition    # Composition tests (~7s)

# Infrastructure tests (requires Databricks + AWS)
make auth-check          # Verify authentication first
make test-infrastructure # Composable pipeline tests (~47s)

# Legacy scenario tests (requires Databricks + AWS)
make test-standard       # Single-location tables
make test-cross-bucket   # Multi-bucket partitions
make test-cross-region   # Multi-region partitions

# Options
make test-verbose        # Extra verbose output
make test-parallel       # Run with 4 workers
make test-collect        # Show what will run
```

## Test Inventory

| Test File | Count | Infra Required? | Purpose |
|-----------|-------|-----------------|---------|
| `test_models.py` | 3 | No | TableInfo dataclass |
| `test_schema.py` | 7 | No | Schema inference (Glue + Spark) |
| `test_discovery.py` | 16 | No | Discovery strategies |
| `test_listing.py` | 14 | No | Listing strategies |
| `test_converter.py` | 23 | No | Converter pipeline |
| `test_delta_log.py` | 37 | No | Delta log generation |
| `test_vacuum.py` | 9 | No | External vacuum |
| `test_composition.py` | 20 | No | Multi-module composition |
| `test_infrastructure.py` | 17 | Yes | Real Databricks + AWS |
| `test_convert.py` | 8 | Yes | Legacy conversion |
| `test_operations.py` | 15 | Yes | Delta operations |
| `test_shallow_clone.py` | 9 | Yes | Shallow clones |

## Test Categories

| Category | Make Target | Infra Required | What It Tests |
|----------|-------------|----------------|---------------|
| Unit | `make test-unit` | No | Models, schema, discovery, listing, converter, delta log, vacuum |
| Composition | `make test-composition` | No | Multi-module integration without real infrastructure |
| Infrastructure | `make test-infrastructure` | Yes | End-to-end composable pipeline against real Databricks + AWS |
| Standard | `make test-standard` | Yes | Legacy single-location table conversion |
| Cross-bucket | `make test-cross-bucket` | Yes | Legacy multi-bucket partition conversion |
| Cross-region | `make test-cross-region` | Yes | Legacy multi-region partition conversion |

## Test Markers

Run tests by scenario:

```bash
pytest tests/ -v -m standard       # Single-location tables
pytest tests/ -v -m cross_bucket   # Multi-bucket tables
pytest tests/ -v -m cross_region   # Multi-region tables
```

Markers are defined in `pyproject.toml`.

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `HIVE_TO_DELTA_TEST_WAREHOUSE_ID` | auto-discovered | SQL Warehouse for cross-bucket tests |
| `HIVE_TO_DELTA_TEST_GLUE_DATABASE` | `hive_to_delta_test` | Source Hive database |
| `HIVE_TO_DELTA_TEST_CATALOG` | `fe_randy_pitcher_workspace_catalog` | Target Unity Catalog |
| `HIVE_TO_DELTA_TEST_SCHEMA` | `hive_to_delta_tests` | Target schema |
| `DATABRICKS_CONFIG_PROFILE` | none | Databricks CLI profile |
| `AWS_PROFILE` | none | AWS credentials profile |

## Test Infrastructure Requirements

### AWS Resources (via Terraform)

```bash
cd terraform && terraform apply
```

Creates:
- IAM role (`htd-role`) with S3/Glue/LakeFormation permissions
- Instance profile (`htd-instance-profile`)
- S3 buckets: `htd-east-1a`, `htd-east-1b`, `htd-west-2`
- Glue database with test tables

### Databricks Resources

- Storage credential (`htd_storage_credential`) pointing to `htd-role`
- External locations for each S3 bucket
- Workspace configured with instance profile for SQL Warehouses

### Test Tables in Glue

| Table | Description |
|-------|-------------|
| `standard_table` | All partitions in `s3://htd-east-1a/standard/` |
| `cross_bucket_table` | Partitions split between `htd-east-1a` and `htd-east-1b` |
| `cross_region_table` | Partitions in `us-east-1` and `us-west-2` |

Create with: `python scripts/setup_integration_test_tables.py`

## Compute Requirements

| Test Category | Compute | Why |
|---------------|---------|-----|
| Standard | Databricks Connect | Faster, single credential |
| Cross-bucket | SQL Warehouse | Multi-bucket credential resolution |
| Cross-region | SQL Warehouse | Multi-region data access |

Cross-bucket and cross-region tests require SQL Warehouse because Databricks Connect cannot resolve credentials for external paths across multiple buckets.

## Known Limitations

1. **Bulk Conversion Tests**: Skipped pending creation of bulk test tables in Glue
2. **VACUUM Retention**: Operations tests use default 7-day retention (retentionDurationCheck cannot be disabled via Databricks Connect serverless)
3. **Cross-Bucket/Region Queries**: Require SQL Warehouse with instance profile + fallback enabled on external locations

## Test File Details

### conftest.py - Fixtures

Key fixtures:
- `spark`: Databricks Connect SparkSession (session-scoped)
- `glue_database`: Test Glue database name from environment
- `s3_clients`: S3 clients for multiple regions
- `sql_warehouse_id`: SQL warehouse ID for cross-bucket/region queries
- `cleanup_tables`: Automatic table cleanup after test module

### test_convert.py - Conversion Tests

- Single table conversion (standard, cross-bucket, cross-region)
- ConversionResult validation
- Delta log verification in S3
- Table queryability verification

### test_vacuum.py - External Vacuum Tests

- Dry-run mode (identifies without deleting)
- Actual deletion mode
- Retention period filtering (7 days minimum)
- Cross-bucket orphaned file detection
- Internal path exclusion (handled by standard VACUUM)

### test_operations.py - Delta Operations Tests

Each operation tested across all 3 scenarios:
- SELECT queries
- INSERT operations
- UPDATE operations
- OPTIMIZE command
- VACUUM command

### test_shallow_clone.py - Clone Tests

- Shallow clone creation
- Schema preservation
- Partitioned table cloning
- Clone queryability (SELECT, predicates, aggregations, JOINs)

## Troubleshooting

### SQL Warehouse won't start

Check instance profile configuration:
```bash
databricks warehouses get-workspace-warehouse-config
```

See [DEBUGGING.md](DEBUGGING.md) for IAM trust policy requirements.

### Cross-bucket tests fail with credential errors

Ensure external locations have `fallback` enabled and the IAM role has both:
1. Unity Catalog master role trust statement
2. Serverless compute trust statement

### Tests hang waiting for warehouse

Warehouse may be in DEGRADED state. Check:
```bash
databricks warehouses get <warehouse_id> --output json | jq '{state, health}'
```
