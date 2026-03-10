"""Test CONVERT TO DELTA conversion method across all 7 scenarios.

CONVERT TO DELTA:
- In-place conversion that adds _delta_log to existing Parquet data
- Expected: Works for standard layouts, may fail or require data copy for exotic layouts
- READS DATA FILES: Yes (schema inference from Parquet)
- DATA COPY: No for standard, Yes for exotic (must consolidate partitions)

This test evaluates whether CONVERT TO DELTA can handle:
1. standard - Partitions under table root (should work)
2. recursive - Nested subdirectories (needs recursive scan)
3. scattered - Partitions outside table root (likely fails/requires copy)
4. cross_bucket - Partitions across S3 buckets (likely fails)
5. cross_region - Partitions across AWS regions (likely fails)
6. shared_a - Shared partition with another table (conflict risk)
7. shared_b - Shared partition with another table (conflict risk)
"""

import time

import pytest

from hive_table_experiments import (
    record_conversion_result,
    get_conversion_source_info,
)
from hive_table_experiments.scenarios import ALL_SCENARIOS, SCENARIO_BY_BASE_NAME

from .conftest import get_glue_source_table, get_uc_target_table


def get_partition_clause(scenario_name: str) -> str:
    """Get PARTITIONED BY clause for CONVERT TO DELTA.

    CONVERT TO DELTA requires explicit partition column specification
    when converting partitioned Parquet data.

    Args:
        scenario_name: Scenario base name

    Returns:
        PARTITIONED BY clause or empty string
    """
    scenario = SCENARIO_BY_BASE_NAME.get(scenario_name)
    if not scenario or not scenario.partitions:
        return ""

    # Get partition keys
    partition_keys = list(set(p.partition_key for p in scenario.partitions))
    if not partition_keys:
        return ""

    # Build PARTITIONED BY clause - assume STRING type for partition columns
    partition_cols = ", ".join(f"{key} STRING" for key in partition_keys)
    return f"PARTITIONED BY ({partition_cols})"


# Scenarios to test
SCENARIOS = [s.base_name for s in ALL_SCENARIOS]


class TestConvertToDelta:
    """Test CONVERT TO DELTA across all scenarios."""

    @pytest.fixture(scope="class")
    def conversion_method(self):
        """Conversion method under test."""
        return "convert"

    @pytest.mark.parametrize("scenario", SCENARIOS)
    def test_convert_to_delta(
        self,
        spark,
        scenario: str,
        conversion_method: str,
        conversion_matrix,
        glue_database,
        uc_catalog,
        uc_schema,
    ):
        """Test CONVERT TO DELTA for a single scenario.

        This test:
        1. Gets the source Glue table info
        2. Counts rows in source table
        3. Attempts CONVERT TO DELTA
        4. Records success/failure and metrics
        5. If successful, verifies row count matches

        Args:
            spark: Databricks Connect session
            scenario: Scenario base name
            conversion_method: Always "convert" for this test
            conversion_matrix: Matrix for recording results
            glue_database: Glue database name
            uc_catalog: UC catalog for target
            uc_schema: UC schema for target
        """
        # Get source table info
        source_info = get_conversion_source_info(scenario, conversion_method)
        source_table = get_glue_source_table(scenario, conversion_method, glue_database)
        target_table = get_uc_target_table(scenario, conversion_method, uc_catalog, uc_schema)
        s3_path = source_info["s3_path"]

        print(f"\n{'='*60}")
        print(f"Testing CONVERT TO DELTA: {scenario}")
        print(f"Source: {source_table}")
        print(f"Target: {target_table}")
        print(f"S3 Path: {s3_path}")
        print(f"{'='*60}")

        # Count rows in source
        try:
            row_count_before = spark.sql(f"SELECT COUNT(*) as cnt FROM {source_table}").collect()[0]["cnt"]
            print(f"Source row count: {row_count_before}")
        except Exception as e:
            print(f"Failed to count source rows: {e}")
            record_conversion_result(
                scenario=scenario,
                method=conversion_method,
                success=False,
                error_message=f"Failed to read source table: {str(e)}",
                requires_data_copy=False,
                reads_data_files=False,
                source_table=source_table,
            )
            pytest.skip(f"Source table not readable: {e}")

        # Attempt CONVERT TO DELTA
        start_time = time.time()
        success = False
        error_message = None
        row_count_after = None
        requires_data_copy = False

        try:
            # First, try direct CONVERT TO DELTA on the Parquet location
            # This is the most efficient approach if it works
            partition_clause = get_partition_clause(scenario)
            convert_sql = f"""
                CONVERT TO DELTA parquet.`{s3_path}` {partition_clause}
            """
            print(f"Executing: {convert_sql}")
            spark.sql(convert_sql)

            # Create UC table pointing to the converted Delta location
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {target_table}
                USING DELTA
                LOCATION '{s3_path}'
            """
            print(f"Registering in UC: {create_table_sql}")
            spark.sql(create_table_sql)

            # Verify row count
            row_count_after = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table}").collect()[0]["cnt"]
            success = True
            print(f"✓ CONVERT TO DELTA succeeded! Row count: {row_count_after}")

        except Exception as e:
            error_message = str(e)
            print(f"✗ CONVERT TO DELTA failed: {error_message}")

            # Check if the error indicates data copy would be required
            error_lower = error_message.lower()
            if any(x in error_lower for x in [
                "multiple locations",
                "different locations",
                "external",
                "partition",
                "not supported",
                "cannot convert",
            ]):
                requires_data_copy = True
                print("  → This layout would require data consolidation/copy")

        execution_time_ms = int((time.time() - start_time) * 1000)

        # CONVERT TO DELTA always reads data files for schema inference
        reads_data_files = True

        # Record result
        record_conversion_result(
            scenario=scenario,
            method=conversion_method,
            success=success,
            error_message=error_message,
            requires_data_copy=requires_data_copy,
            reads_data_files=reads_data_files,
            source_table=source_table,
            target_table=target_table if success else None,
            row_count_before=row_count_before,
            row_count_after=row_count_after,
            execution_time_ms=execution_time_ms,
            notes=f"CONVERT TO DELTA {'succeeded' if success else 'failed'} for {scenario}",
        )

        # Assertion for test framework
        if success:
            assert row_count_after == row_count_before, \
                f"Row count mismatch: {row_count_after} != {row_count_before}"
        else:
            # Mark as expected failure for exotic scenarios
            # These are expected to fail - we're documenting what works
            if scenario in ["scattered", "cross_bucket", "cross_region", "shared_a", "shared_b"]:
                pytest.xfail(f"CONVERT TO DELTA expected to fail for exotic layout: {scenario}")
            else:
                pytest.fail(f"CONVERT TO DELTA unexpectedly failed for {scenario}: {error_message}")


class TestConvertToDeltaCleanup:
    """Cleanup converted tables after testing."""

    @pytest.fixture(scope="class", autouse=True)
    def cleanup_converted_tables(self, spark, uc_catalog, uc_schema):
        """Clean up converted Delta tables after tests complete."""
        yield

        print("\n\nCleaning up converted tables...")
        for scenario in SCENARIOS:
            target_table = get_uc_target_table(scenario, "convert", uc_catalog, uc_schema)
            try:
                spark.sql(f"DROP TABLE IF EXISTS {target_table}")
                print(f"  Dropped {target_table}")
            except Exception as e:
                print(f"  Failed to drop {target_table}: {e}")
