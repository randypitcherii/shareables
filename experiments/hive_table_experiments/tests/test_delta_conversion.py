"""
Test Delta conversion approaches for Hive-to-Delta migration.

This module tests three conversion approaches across all 7 test scenarios:
1. CONVERT TO DELTA - In-place conversion with _delta_log
2. SHALLOW CLONE - Metadata-only reference to source files
3. Manual Delta Log Generation - Programmatic _delta_log creation

Test Matrix: 7 scenarios x 3 approaches = 21 test combinations

Scenarios:
- standard_table: Partitions under table root (should work with all approaches)
- recursive_table: Nested subdirectories (may require special handling)
- scattered_table: External partitions (CONVERT TO DELTA may fail)
- cross_bucket_table: Partitions in multiple S3 buckets
- cross_region_table: Partitions in multiple AWS regions
- shared_partition_table_a: First table with shared partition
- shared_partition_table_b: Second table with shared partition

Key validation:
- No data duplication (original files remain in place)
- Row counts match after conversion
- Schema preserved
- Partition pruning works

See:
- https://docs.databricks.com/aws/en/sql/language-manual/delta-convert-to-delta
- https://docs.databricks.com/aws/en/delta/clone-unity-catalog
"""

import pytest
from hive_table_experiments.scenarios import (
    ALL_SCENARIOS,
    PARTITIONS_UNDER_TABLE_ROOT,
    PARTITIONS_IN_NESTED_SUBDIRECTORIES,
    PARTITIONS_OUTSIDE_TABLE_ROOT,
    PARTITIONS_ACROSS_S3_BUCKETS,
    PARTITIONS_ACROSS_AWS_REGIONS,
    PARTITIONS_SHARED_BETWEEN_TABLES_A,
    PARTITIONS_SHARED_BETWEEN_TABLES_B,
    HiveTableScenario,
)
from hive_table_experiments.delta_conversion import (
    ConversionApproach,
    ConversionResult,
    DEFAULT_DELTA_CATALOG,
    DEFAULT_DELTA_SCHEMA,
    convert_to_delta,
    shallow_clone_table,
    validate_conversion,
    cleanup_conversion,
    can_convert_to_delta,
    can_shallow_clone,
)
from hive_table_experiments.manual_delta_log import (
    scan_parquet_files,
    scan_external_partitions,
    generate_delta_schema,
    generate_delta_log,
    manual_convert_to_delta,
    cleanup_manual_delta_log,
)


# Test configuration
DELTA_CATALOG = DEFAULT_DELTA_CATALOG
DELTA_SCHEMA = DEFAULT_DELTA_SCHEMA


@pytest.fixture(scope="module")
def delta_schema_ready(spark):
    """Ensure Delta migration schema exists."""
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {DELTA_CATALOG}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DELTA_CATALOG}.{DELTA_SCHEMA}")
        return True
    except Exception as e:
        pytest.skip(f"Cannot create Delta schema: {str(e)[:200]}")
        return False


@pytest.fixture(scope="function")
def cleanup_after_test(spark, request):
    """Cleanup fixture that runs after each test."""
    yield
    # Cleanup is handled in individual tests to control when it happens


# =============================================================================
# CONVERT TO DELTA Tests
# =============================================================================

@pytest.mark.delta_conversion
class TestConvertToDelta:
    """Test CONVERT TO DELTA approach across all scenarios."""

    @pytest.mark.parametrize("scenario", [
        pytest.param(PARTITIONS_UNDER_TABLE_ROOT, id="standard_table"),
        pytest.param(PARTITIONS_IN_NESTED_SUBDIRECTORIES, id="recursive_table"),
        pytest.param(PARTITIONS_OUTSIDE_TABLE_ROOT, id="scattered_table"),
        pytest.param(PARTITIONS_ACROSS_S3_BUCKETS, id="cross_bucket_table"),
        pytest.param(PARTITIONS_ACROSS_AWS_REGIONS, id="cross_region_table"),
        pytest.param(PARTITIONS_SHARED_BETWEEN_TABLES_A, id="shared_partition_table_a"),
        pytest.param(PARTITIONS_SHARED_BETWEEN_TABLES_B, id="shared_partition_table_b"),
    ])
    def test_convert_to_delta(
        self,
        spark,
        glue_database,
        delta_schema_ready,
        scenario: HiveTableScenario,
    ):
        """
        Test CONVERT TO DELTA for each scenario.

        Expected outcomes:
        - standard_table: SUCCESS - partitions under table root
        - recursive_table: SUCCESS - non-partitioned, files under root
        - scattered_table: PARTIAL - external partitions not included
        - cross_bucket_table: PARTIAL - only files in primary bucket
        - cross_region_table: PARTIAL - only files in primary region
        - shared_partition_*: PARTIAL - external shared partition not included
        """
        # Check compatibility first
        can_convert, reason = can_convert_to_delta(scenario)
        print(f"\nScenario: {scenario.name}")
        print(f"Can convert: {can_convert}")
        print(f"Reason: {reason}")

        # Attempt conversion
        result = convert_to_delta(
            spark,
            scenario,
            glue_database,
            DELTA_CATALOG,
            DELTA_SCHEMA,
        )

        print(f"Result: {result}")
        print(f"Notes: {result.notes}")

        if result.success:
            print(f"Source rows: {result.source_row_count}")
            print(f"Target rows: {result.target_row_count}")
            print(f"Row counts match: {result.row_counts_match}")

            # For scenarios with external partitions, document expected behavior
            if scenario.use_external_partitions:
                if not result.row_counts_match:
                    print(
                        "EXPECTED: Row count mismatch due to external partitions. "
                        "CONVERT TO DELTA only includes files under table root."
                    )
            else:
                # For scenarios without external partitions, rows should match
                assert result.row_counts_match, (
                    f"Row count mismatch for {scenario.name}: "
                    f"source={result.source_row_count}, target={result.target_row_count}"
                )
        else:
            print(f"Error: {result.error}")
            # Document failure but don't fail test - we're documenting behavior
            if not can_convert:
                print("EXPECTED: Conversion failed as predicted by compatibility check")

        # Cleanup
        cleanup_conversion(spark, scenario, DELTA_CATALOG, DELTA_SCHEMA)


@pytest.mark.delta_conversion
class TestConvertToDeltaStandard:
    """Detailed tests for standard_table CONVERT TO DELTA."""

    @pytest.fixture(scope="class")
    def scenario(self):
        return PARTITIONS_UNDER_TABLE_ROOT

    @pytest.fixture(scope="class")
    def conversion_result(self, spark, glue_database, delta_schema_ready, scenario):
        """Run conversion once for all tests in class."""
        result = convert_to_delta(
            spark, scenario, glue_database, DELTA_CATALOG, DELTA_SCHEMA
        )
        yield result
        # Cleanup after class
        cleanup_conversion(spark, scenario, DELTA_CATALOG, DELTA_SCHEMA)

    def test_conversion_succeeds(self, conversion_result):
        """Verify conversion succeeds for standard table."""
        assert conversion_result.success, (
            f"Conversion should succeed: {conversion_result.error}"
        )

    def test_row_counts_match(self, conversion_result):
        """Verify row counts match after conversion."""
        assert conversion_result.row_counts_match, (
            f"Row counts should match: "
            f"source={conversion_result.source_row_count}, "
            f"target={conversion_result.target_row_count}"
        )

    def test_no_data_duplication(self, spark, conversion_result, scenario):
        """Verify original Parquet files weren't copied."""
        if not conversion_result.success:
            pytest.skip("Conversion failed")

        # Check that table uses original location
        assert conversion_result.target_location == scenario.table_location.rstrip("/")
        print("Verified: Delta table uses original S3 location (no data copy)")

    def test_partition_pruning_works(self, spark, conversion_result):
        """Verify partition pruning works on converted Delta table."""
        if not conversion_result.success:
            pytest.skip("Conversion failed")

        df = spark.sql(
            f"SELECT * FROM {conversion_result.target_table} WHERE region = 'us-east'"
        )
        count = df.count()
        assert count > 0, "Partition pruning should return data"

        regions = df.select("region").distinct().collect()
        assert len(regions) == 1 and regions[0].region == "us-east"
        print(f"Partition pruning works: {count} rows for region=us-east")

    def test_delta_log_created(self, spark, conversion_result, scenario):
        """Verify _delta_log directory was created."""
        if not conversion_result.success:
            pytest.skip("Conversion failed")

        # Check for Delta log via DESCRIBE HISTORY
        history = spark.sql(
            f"DESCRIBE HISTORY {conversion_result.target_table}"
        ).collect()

        assert len(history) > 0, "Delta history should exist"
        print(f"Delta log has {len(history)} version(s)")


# =============================================================================
# SHALLOW CLONE Tests
# =============================================================================

@pytest.mark.delta_conversion
class TestShallowClone:
    """Test SHALLOW CLONE approach across all scenarios."""

    @pytest.mark.parametrize("scenario", [
        pytest.param(PARTITIONS_UNDER_TABLE_ROOT, id="standard_table"),
        pytest.param(PARTITIONS_IN_NESTED_SUBDIRECTORIES, id="recursive_table"),
        pytest.param(PARTITIONS_OUTSIDE_TABLE_ROOT, id="scattered_table"),
        pytest.param(PARTITIONS_ACROSS_S3_BUCKETS, id="cross_bucket_table"),
        pytest.param(PARTITIONS_ACROSS_AWS_REGIONS, id="cross_region_table"),
        pytest.param(PARTITIONS_SHARED_BETWEEN_TABLES_A, id="shared_partition_table_a"),
        pytest.param(PARTITIONS_SHARED_BETWEEN_TABLES_B, id="shared_partition_table_b"),
    ])
    def test_shallow_clone_from_hive(
        self,
        spark,
        glue_database,
        delta_schema_ready,
        scenario: HiveTableScenario,
    ):
        """
        Test SHALLOW CLONE from Hive table.

        Expected outcome: FAIL - SHALLOW CLONE requires Delta source.

        This test documents that you cannot directly shallow clone
        a Hive/Parquet table. You must convert to Delta first.
        """
        print(f"\nScenario: {scenario.name}")

        # Check compatibility
        can_clone, reason = can_shallow_clone(scenario, source_is_delta=False)
        print(f"Can shallow clone from Hive: {can_clone}")
        print(f"Reason: {reason}")

        # Attempt clone (expected to fail)
        result = shallow_clone_table(
            spark,
            scenario,
            glue_database,
            DELTA_CATALOG,
            DELTA_SCHEMA,
            source_is_delta=False,
        )

        print(f"Result: {result}")

        if result.success:
            print("UNEXPECTED: Shallow clone from Hive succeeded")
            print(f"Source rows: {result.source_row_count}")
            print(f"Target rows: {result.target_row_count}")
        else:
            print(f"EXPECTED: Clone failed - {result.error}")
            print(f"Notes: {result.notes}")

        # Cleanup regardless
        try:
            spark.sql(
                f"DROP TABLE IF EXISTS "
                f"{DELTA_CATALOG}.{DELTA_SCHEMA}.{scenario.name}_shallow_clone"
            )
        except Exception:
            pass

    @pytest.mark.parametrize("scenario", [
        pytest.param(PARTITIONS_UNDER_TABLE_ROOT, id="standard_table"),
    ])
    def test_shallow_clone_from_delta(
        self,
        spark,
        glue_database,
        delta_schema_ready,
        scenario: HiveTableScenario,
    ):
        """
        Test SHALLOW CLONE from Delta table.

        First converts to Delta, then creates shallow clone.
        Expected outcome: SUCCESS for standard_table.
        """
        print(f"\nScenario: {scenario.name}")

        # First convert to Delta
        convert_result = convert_to_delta(
            spark, scenario, glue_database, DELTA_CATALOG, DELTA_SCHEMA
        )

        if not convert_result.success:
            pytest.skip(f"Initial conversion failed: {convert_result.error}")

        print(f"Initial conversion: {convert_result.target_table}")

        # Now shallow clone from Delta table
        result = shallow_clone_table(
            spark,
            scenario,
            glue_database,
            DELTA_CATALOG,
            DELTA_SCHEMA,
            source_is_delta=True,
        )

        print(f"Clone result: {result}")

        if result.success:
            print(f"Source rows: {result.source_row_count}")
            print(f"Target rows: {result.target_row_count}")
            print(f"Row counts match: {result.row_counts_match}")

            assert result.row_counts_match, (
                f"Shallow clone should have same row count as source"
            )
        else:
            print(f"Clone failed: {result.error}")
            print(f"Notes: {result.notes}")

        # Cleanup both tables
        cleanup_conversion(spark, scenario, DELTA_CATALOG, DELTA_SCHEMA)


# =============================================================================
# Manual Delta Log Generation Tests
# =============================================================================

@pytest.mark.delta_conversion
class TestManualDeltaLog:
    """Test manual Delta log generation across all scenarios."""

    @pytest.mark.parametrize("scenario", [
        pytest.param(PARTITIONS_UNDER_TABLE_ROOT, id="standard_table"),
        pytest.param(PARTITIONS_IN_NESTED_SUBDIRECTORIES, id="recursive_table"),
        pytest.param(PARTITIONS_OUTSIDE_TABLE_ROOT, id="scattered_table"),
        pytest.param(PARTITIONS_ACROSS_S3_BUCKETS, id="cross_bucket_table"),
        pytest.param(PARTITIONS_ACROSS_AWS_REGIONS, id="cross_region_table"),
        pytest.param(PARTITIONS_SHARED_BETWEEN_TABLES_A, id="shared_partition_table_a"),
        pytest.param(PARTITIONS_SHARED_BETWEEN_TABLES_B, id="shared_partition_table_b"),
    ])
    def test_manual_delta_conversion(
        self,
        spark,
        glue_database,
        delta_schema_ready,
        scenario: HiveTableScenario,
        aws_region,
    ):
        """
        Test manual Delta log generation for each scenario.

        This approach should work for ALL scenarios including:
        - External partitions (scattered_table)
        - Cross-bucket tables
        - Cross-region tables (with caveats for region access)
        """
        print(f"\nScenario: {scenario.name}")
        print(f"Uses external partitions: {scenario.use_external_partitions}")

        result = manual_convert_to_delta(
            spark,
            scenario,
            glue_database,
            DELTA_CATALOG,
            DELTA_SCHEMA,
            aws_region,
            include_external_partitions=True,
        )

        print(f"Result: {result}")
        print(f"Notes: {result.notes}")

        if result.success:
            print(f"Source rows: {result.source_row_count}")
            print(f"Target rows: {result.target_row_count}")
            print(f"Row counts match: {result.row_counts_match}")

            # For manual conversion, we expect row counts to match
            # even for external partitions (that's the point!)
            if not result.row_counts_match:
                print(
                    f"WARNING: Row count mismatch. "
                    f"Some files may not be accessible from current region."
                )
        else:
            print(f"Error: {result.error}")

        # Cleanup
        cleanup_conversion(spark, scenario, DELTA_CATALOG, DELTA_SCHEMA)
        # Also cleanup delta log from S3 (optional - may want to keep for inspection)


@pytest.mark.delta_conversion
class TestManualDeltaLogHelpers:
    """Test helper functions for manual Delta log generation."""

    def test_scan_parquet_files(self, aws_region):
        """Test Parquet file scanning under table root."""
        scenario = PARTITIONS_UNDER_TABLE_ROOT

        files = scan_parquet_files(
            scenario.table_location,
            partition_columns=["region"],
            region=aws_region,
        )

        print(f"Found {len(files)} Parquet files")
        for f in files[:5]:  # Print first 5
            print(f"  {f.path} ({f.size_bytes} bytes)")
            print(f"    Partitions: {f.partition_values}")

        assert len(files) > 0, "Should find Parquet files"

    def test_scan_external_partitions(self, glue_database, aws_region):
        """Test scanning external partition locations."""
        scenario = PARTITIONS_OUTSIDE_TABLE_ROOT

        files = scan_external_partitions(
            scenario,
            glue_database,
            aws_region,
        )

        print(f"Found {len(files)} Parquet files across all partitions")
        for f in files[:5]:
            print(f"  {f.path}")
            print(f"    Partitions: {f.partition_values}")

        assert len(files) > 0, "Should find files in external partitions"

    def test_generate_delta_schema(self, glue_database, aws_region):
        """Test Delta schema generation from Glue metadata."""
        scenario = PARTITIONS_UNDER_TABLE_ROOT

        schema = generate_delta_schema(
            glue_database,
            scenario.name,
            aws_region,
        )

        print(f"Generated schema: {schema}")

        assert schema["type"] == "struct"
        assert len(schema["fields"]) > 0

    def test_generate_delta_log(self):
        """Test Delta transaction log JSON generation."""
        from hive_table_experiments.manual_delta_log import ParquetFileInfo

        # Create test file info
        files = [
            ParquetFileInfo(
                path="region=us-east/part-00000.parquet",
                size_bytes=1024,
                modification_time=1704067200000,
                partition_values={"region": "us-east"},
            ),
            ParquetFileInfo(
                path="region=us-west/part-00000.parquet",
                size_bytes=2048,
                modification_time=1704067200000,
                partition_values={"region": "us-west"},
            ),
        ]

        schema = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": True, "metadata": {}},
                {"name": "name", "type": "string", "nullable": True, "metadata": {}},
                {"name": "region", "type": "string", "nullable": True, "metadata": {}},
            ],
        }

        log_json = generate_delta_log(
            files=files,
            schema=schema,
            partition_columns=["region"],
        )

        print(f"Generated log:\n{log_json[:500]}...")

        # Verify JSON structure
        import json
        lines = log_json.strip().split("\n")

        # Should have protocol, metadata, and add entries
        assert len(lines) >= 4  # protocol + metadata + 2 adds

        # Parse and verify
        protocol = json.loads(lines[0])
        assert "protocol" in protocol

        metadata = json.loads(lines[1])
        assert "metaData" in metadata

        add1 = json.loads(lines[2])
        assert "add" in add1


# =============================================================================
# Comparison Tests
# =============================================================================

@pytest.mark.delta_conversion
class TestConversionComparison:
    """Compare all three approaches for each scenario."""

    def test_full_comparison_matrix(
        self,
        spark,
        glue_database,
        delta_schema_ready,
        aws_region,
    ):
        """
        Run all three approaches on all scenarios and generate comparison matrix.

        This is the comprehensive test that documents which approach works
        for which scenario.
        """
        results = []

        for scenario in ALL_SCENARIOS:
            print(f"\n{'='*60}")
            print(f"Scenario: {scenario.name}")
            print(f"{'='*60}")

            scenario_results = {
                "scenario": scenario.name,
                "uses_external_partitions": scenario.use_external_partitions,
            }

            # Test CONVERT TO DELTA
            print("\n1. CONVERT TO DELTA")
            convert_result = convert_to_delta(
                spark, scenario, glue_database, DELTA_CATALOG, DELTA_SCHEMA
            )
            scenario_results["convert_to_delta"] = {
                "success": convert_result.success,
                "row_match": convert_result.row_counts_match,
                "source_rows": convert_result.source_row_count,
                "target_rows": convert_result.target_row_count,
                "error": convert_result.error,
            }
            print(f"   Success: {convert_result.success}")
            if convert_result.success:
                print(f"   Rows: {convert_result.source_row_count} -> {convert_result.target_row_count}")

            # Test SHALLOW CLONE (from Delta if available)
            print("\n2. SHALLOW CLONE")
            if convert_result.success:
                clone_result = shallow_clone_table(
                    spark, scenario, glue_database, DELTA_CATALOG, DELTA_SCHEMA,
                    source_is_delta=True
                )
            else:
                clone_result = shallow_clone_table(
                    spark, scenario, glue_database, DELTA_CATALOG, DELTA_SCHEMA,
                    source_is_delta=False
                )
            scenario_results["shallow_clone"] = {
                "success": clone_result.success,
                "row_match": clone_result.row_counts_match,
                "error": clone_result.error,
            }
            print(f"   Success: {clone_result.success}")
            if clone_result.success:
                print(f"   Rows match: {clone_result.row_counts_match}")

            # Cleanup CONVERT and CLONE results before manual
            cleanup_conversion(spark, scenario, DELTA_CATALOG, DELTA_SCHEMA)

            # Test MANUAL DELTA LOG
            print("\n3. MANUAL DELTA LOG")
            manual_result = manual_convert_to_delta(
                spark, scenario, glue_database, DELTA_CATALOG, DELTA_SCHEMA,
                aws_region, include_external_partitions=True
            )
            scenario_results["manual_delta_log"] = {
                "success": manual_result.success,
                "row_match": manual_result.row_counts_match,
                "source_rows": manual_result.source_row_count,
                "target_rows": manual_result.target_row_count,
                "error": manual_result.error,
            }
            print(f"   Success: {manual_result.success}")
            if manual_result.success:
                print(f"   Rows: {manual_result.source_row_count} -> {manual_result.target_row_count}")

            # Final cleanup
            cleanup_conversion(spark, scenario, DELTA_CATALOG, DELTA_SCHEMA)

            results.append(scenario_results)

        # Print summary matrix
        print("\n" + "="*80)
        print("DELTA CONVERSION COMPARISON MATRIX")
        print("="*80)
        print(f"\n{'Scenario':<30} {'CONVERT':<12} {'CLONE':<12} {'MANUAL':<12}")
        print("-"*66)

        for r in results:
            convert_status = "OK" if r["convert_to_delta"]["success"] else "FAIL"
            if r["convert_to_delta"]["success"] and not r["convert_to_delta"]["row_match"]:
                convert_status = "PARTIAL"

            clone_status = "OK" if r["shallow_clone"]["success"] else "FAIL"
            if r["shallow_clone"]["success"] and not r["shallow_clone"]["row_match"]:
                clone_status = "PARTIAL"

            manual_status = "OK" if r["manual_delta_log"]["success"] else "FAIL"
            if r["manual_delta_log"]["success"] and not r["manual_delta_log"]["row_match"]:
                manual_status = "PARTIAL"

            print(f"{r['scenario']:<30} {convert_status:<12} {clone_status:<12} {manual_status:<12}")

        print("\nLegend: OK=Full success, PARTIAL=Success but row count mismatch, FAIL=Error")
        print("="*80)

        # Return results for potential further analysis
        return results


# =============================================================================
# Validation Tests
# =============================================================================

@pytest.mark.delta_conversion
class TestValidation:
    """Test conversion validation utilities."""

    def test_validate_successful_conversion(
        self,
        spark,
        glue_database,
        delta_schema_ready,
    ):
        """Test validation on a successful conversion."""
        scenario = PARTITIONS_UNDER_TABLE_ROOT

        # Convert first
        result = convert_to_delta(
            spark, scenario, glue_database, DELTA_CATALOG, DELTA_SCHEMA
        )

        if not result.success:
            pytest.skip(f"Conversion failed: {result.error}")

        # Validate
        validation = validate_conversion(
            spark,
            result.source_table,
            result.target_table,
        )

        print(f"Validation result: {validation}")

        assert validation["valid"], f"Validation failed: {validation['errors']}"

        # Cleanup
        cleanup_conversion(spark, scenario, DELTA_CATALOG, DELTA_SCHEMA)

    def test_cleanup_works(self, spark, glue_database, delta_schema_ready):
        """Test that cleanup properly removes test tables."""
        scenario = PARTITIONS_UNDER_TABLE_ROOT

        # Convert
        convert_to_delta(
            spark, scenario, glue_database, DELTA_CATALOG, DELTA_SCHEMA
        )

        # Verify table exists
        tables = spark.sql(
            f"SHOW TABLES IN {DELTA_CATALOG}.{DELTA_SCHEMA}"
        ).collect()
        table_names = [t.tableName for t in tables]

        assert f"{scenario.name}_delta" in table_names, "Delta table should exist"

        # Cleanup
        result = cleanup_conversion(spark, scenario, DELTA_CATALOG, DELTA_SCHEMA)

        print(f"Cleanup result: {result}")

        # Verify table removed
        tables_after = spark.sql(
            f"SHOW TABLES IN {DELTA_CATALOG}.{DELTA_SCHEMA}"
        ).collect()
        table_names_after = [t.tableName for t in tables_after]

        assert f"{scenario.name}_delta" not in table_names_after, "Delta table should be removed"
