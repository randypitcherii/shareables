"""Test SHALLOW CLONE conversion method across all 7 scenarios.

SHALLOW CLONE:
- Creates a metadata-only copy of a Delta table
- Requires a Delta source table (two-step: first convert Parquet to Delta)
- No physical data copy - just references existing files
- Expected: Works if underlying Delta conversion works

READS DATA FILES: No (metadata only operation)
DATA COPY: No (creates references to existing files)

Test approach:
1. First attempt CONVERT TO DELTA on the source Parquet
2. If conversion succeeds, create SHALLOW CLONE
3. Track whether the full pipeline works for each scenario

For exotic layouts, this tests whether:
- If CONVERT TO DELTA fails, SHALLOW CLONE cannot be used at all
- If CONVERT TO DELTA succeeds, SHALLOW CLONE inherits any layout issues
"""

import time

import pytest

from hive_table_experiments import (
    record_conversion_result,
    get_conversion_source_info,
)
from hive_table_experiments.scenarios import ALL_SCENARIOS, SCENARIO_BY_BASE_NAME

from .conftest import get_glue_source_table


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


def get_intermediate_delta_table(scenario: str, uc_catalog: str, uc_schema: str) -> str:
    """Get intermediate Delta table name (from CONVERT step)."""
    return f"{uc_catalog}.{uc_schema}.{scenario}_clone_intermediate"


def get_clone_target_table(scenario: str, uc_catalog: str, uc_schema: str) -> str:
    """Get SHALLOW CLONE target table name."""
    return f"{uc_catalog}.{uc_schema}.{scenario}_delta_clone"


class TestShallowClone:
    """Test SHALLOW CLONE across all scenarios.

    SHALLOW CLONE requires a Delta source, so this is a two-step test:
    1. Convert source Parquet to Delta (intermediate table)
    2. SHALLOW CLONE the intermediate table to final target
    """

    @pytest.fixture(scope="class")
    def conversion_method(self):
        """Conversion method under test."""
        return "clone"

    @pytest.mark.parametrize("scenario", SCENARIOS)
    def test_shallow_clone(
        self,
        spark,
        scenario: str,
        conversion_method: str,
        conversion_matrix,
        glue_database,
        uc_catalog,
        uc_schema,
    ):
        """Test SHALLOW CLONE for a single scenario.

        This test:
        1. Gets the source Glue table info
        2. Counts rows in source table
        3. Converts source to Delta (intermediate)
        4. Creates SHALLOW CLONE from intermediate
        5. Records success/failure and metrics

        Args:
            spark: Databricks Connect session
            scenario: Scenario base name
            conversion_method: Always "clone" for this test
            conversion_matrix: Matrix for recording results
            glue_database: Glue database name
            uc_catalog: UC catalog for target
            uc_schema: UC schema for target
        """
        # Get source table info (uses "clone" method's dedicated source table)
        source_info = get_conversion_source_info(scenario, conversion_method)
        source_table = get_glue_source_table(scenario, conversion_method, glue_database)
        intermediate_table = get_intermediate_delta_table(scenario, uc_catalog, uc_schema)
        target_table = get_clone_target_table(scenario, uc_catalog, uc_schema)
        s3_path = source_info["s3_path"]

        print(f"\n{'='*60}")
        print(f"Testing SHALLOW CLONE: {scenario}")
        print(f"Source: {source_table}")
        print(f"Intermediate (Delta): {intermediate_table}")
        print(f"Target (Clone): {target_table}")
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

        start_time = time.time()
        success = False
        error_message = None
        row_count_after = None
        requires_data_copy = False
        reads_data_files = False  # SHALLOW CLONE doesn't read data files

        try:
            # Step 1: Convert source Parquet to Delta (intermediate)
            # This step DOES read data files for schema inference
            print("\nStep 1: Converting source to Delta (intermediate)...")
            partition_clause = get_partition_clause(scenario)
            convert_sql = f"""
                CONVERT TO DELTA parquet.`{s3_path}` {partition_clause}
            """
            spark.sql(convert_sql)

            # Register as intermediate table
            create_intermediate_sql = f"""
                CREATE TABLE IF NOT EXISTS {intermediate_table}
                USING DELTA
                LOCATION '{s3_path}'
            """
            spark.sql(create_intermediate_sql)
            print(f"  ✓ Created intermediate Delta table: {intermediate_table}")

            # Step 2: Create SHALLOW CLONE
            # This step does NOT read data files
            print("\nStep 2: Creating SHALLOW CLONE...")

            # Drop target if exists
            spark.sql(f"DROP TABLE IF EXISTS {target_table}")

            # Create shallow clone with a different location
            clone_location = s3_path.rstrip("/") + "_clone/"
            clone_sql = f"""
                CREATE TABLE {target_table}
                SHALLOW CLONE {intermediate_table}
                LOCATION '{clone_location}'
            """
            print(f"  Executing: {clone_sql}")
            spark.sql(clone_sql)

            # Verify row count
            row_count_after = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table}").collect()[0]["cnt"]
            success = True
            print(f"✓ SHALLOW CLONE succeeded! Row count: {row_count_after}")

            # Note: The intermediate step (CONVERT TO DELTA) reads files
            # But SHALLOW CLONE itself does not
            reads_data_files = True  # Overall pipeline reads files during convert step

        except Exception as e:
            error_message = str(e)
            print(f"✗ SHALLOW CLONE pipeline failed: {error_message}")

            # Check if failure was in convert step (data copy needed)
            error_lower = error_message.lower()
            if any(x in error_lower for x in [
                "multiple locations",
                "different locations",
                "external",
                "cannot convert",
            ]):
                requires_data_copy = True
                reads_data_files = True  # Would have read files if convert had worked
                print("  → Failure was in CONVERT step - exotic layout not supported")

        execution_time_ms = int((time.time() - start_time) * 1000)

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
            notes=f"SHALLOW CLONE (via CONVERT) {'succeeded' if success else 'failed'} for {scenario}",
        )

        # Assertion for test framework
        if success:
            assert row_count_after == row_count_before, \
                f"Row count mismatch: {row_count_after} != {row_count_before}"
        else:
            # Mark as expected failure for exotic scenarios
            if scenario in ["scattered", "cross_bucket", "cross_region", "shared_a", "shared_b"]:
                pytest.xfail(f"SHALLOW CLONE expected to fail for exotic layout: {scenario}")
            else:
                pytest.fail(f"SHALLOW CLONE unexpectedly failed for {scenario}: {error_message}")


class TestShallowCloneCleanup:
    """Cleanup tables after testing."""

    @pytest.fixture(scope="class", autouse=True)
    def cleanup_tables(self, spark, uc_catalog, uc_schema):
        """Clean up intermediate and clone tables after tests complete."""
        yield

        print("\n\nCleaning up SHALLOW CLONE tables...")
        for scenario in SCENARIOS:
            intermediate = get_intermediate_delta_table(scenario, uc_catalog, uc_schema)
            target = get_clone_target_table(scenario, uc_catalog, uc_schema)
            for table in [target, intermediate]:
                try:
                    spark.sql(f"DROP TABLE IF EXISTS {table}")
                    print(f"  Dropped {table}")
                except Exception as e:
                    print(f"  Failed to drop {table}: {e}")
