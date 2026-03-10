"""
Test Unity Catalog external table registration.

This module tests registering Glue Hive tables as Unity Catalog external tables
and compares query results between hive_metastore and unity_catalog versions.

Key questions being validated:
- Can UC external tables point to same S3 locations as Glue tables?
- How does UC handle partition metadata (infer vs explicit)?
- Do external partition locations (outside table root) work in UC?
- Cross-bucket/cross-region scenarios - same behavior as Glue?

Important UC limitation:
Unity Catalog requires all partitions to be contained within the table's
LOCATION directory. External partitions (outside table root) are NOT supported.

See:
- https://docs.databricks.com/aws/en/tables/external
- https://docs.databricks.com/aws/en/tables/external-partition-discovery
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
)
from hive_table_experiments.uc_helpers import (
    DEFAULT_UC_CATALOG,
    DEFAULT_UC_SCHEMA,
    ensure_uc_catalog_and_schema,
    register_uc_external_table,
    compare_table_schemas,
    compare_row_counts,
    compare_partition_counts,
    full_table_comparison,
    scenario_supports_uc,
    get_glue_partition_locations,
    check_partitions_under_table_root,
    generate_uc_create_table_sql,
)


# Test configuration
UC_CATALOG = DEFAULT_UC_CATALOG
UC_SCHEMA = DEFAULT_UC_SCHEMA


@pytest.fixture(scope="module")
def uc_table(glue_database):
    """
    Factory fixture for generating fully-qualified UC table names.

    Usage:
        def test_something(uc_table):
            table = uc_table("standard_table")
            spark.sql(f"SELECT * FROM {table}")

    Returns:
        Callable: Function that takes table name and returns qualified name
    """
    def _make_uc_table_name(table_name: str) -> str:
        return f"{UC_CATALOG}.{UC_SCHEMA}.{table_name}"

    return _make_uc_table_name


@pytest.fixture(scope="module")
def uc_ready(spark):
    """
    Ensure UC catalog and schema are available.

    This fixture checks if Unity Catalog is properly configured.
    Tests will be skipped if UC is not available.
    """
    try:
        # Try to access UC catalog
        spark.sql(f"USE CATALOG {UC_CATALOG}")
        return True
    except Exception as e:
        error_msg = str(e)
        if "does not exist" in error_msg or "CATALOG_NOT_FOUND" in error_msg:
            pytest.skip(
                f"UC catalog '{UC_CATALOG}' not found. "
                "Create it manually or run ensure_uc_catalog_and_schema()"
            )
        elif "permission" in error_msg.lower() or "privilege" in error_msg.lower():
            pytest.skip(f"Insufficient UC permissions: {error_msg[:200]}")
        else:
            pytest.skip(f"UC not available: {error_msg[:200]}")
    return False


@pytest.mark.uc_registration
class TestUCSetup:
    """Test UC catalog and schema setup."""

    def test_uc_catalog_exists(self, spark):
        """Verify UC catalog exists or can be created."""
        try:
            result = spark.sql(f"SHOW CATALOGS LIKE '{UC_CATALOG}'").collect()
            if len(result) > 0:
                print(f"UC catalog '{UC_CATALOG}' exists")
            else:
                # Try to create it
                spark.sql(f"CREATE CATALOG IF NOT EXISTS {UC_CATALOG}")
                print(f"Created UC catalog '{UC_CATALOG}'")
        except Exception as e:
            pytest.skip(f"Cannot access or create UC catalog: {str(e)[:200]}")

    def test_uc_schema_exists(self, spark, uc_ready):
        """Verify UC schema exists or can be created."""
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")
            print(f"UC schema '{UC_CATALOG}.{UC_SCHEMA}' is ready")
        except Exception as e:
            pytest.skip(f"Cannot create UC schema: {str(e)[:200]}")


@pytest.mark.uc_registration
class TestStandardTableRegistration:
    """
    Test UC registration for standard_table (partitions under table root).

    This is the baseline scenario that should work with UC.
    """

    @pytest.fixture(scope="class")
    def scenario(self):
        return PARTITIONS_UNDER_TABLE_ROOT

    @pytest.fixture(scope="class")
    def hive_table(self, glue_table, scenario):
        return glue_table(scenario.name)

    @pytest.fixture(scope="class")
    def uc_table_name(self, uc_table, scenario):
        return uc_table(scenario.name)

    def test_scenario_is_uc_compatible(self, scenario):
        """Verify this scenario should work with UC."""
        is_supported, reason = scenario_supports_uc(scenario)
        assert is_supported, f"Standard table should be UC compatible: {reason}"
        print(f"Scenario compatibility: {reason}")

    def test_register_in_uc(self, spark, scenario, glue_database, uc_table_name, uc_ready):
        """Register the Glue table as UC external table."""
        result = register_uc_external_table(
            spark,
            scenario,
            glue_database,
            UC_CATALOG,
            UC_SCHEMA,
        )

        assert result.success, f"Registration failed: {result.error}"
        print(f"Registered {result.uc_table_name}")
        if result.notes:
            print(f"Notes: {result.notes}")

    def test_schema_matches(self, spark, hive_table, uc_table_name, uc_ready):
        """Verify schema matches between Hive and UC versions."""
        differences = compare_table_schemas(spark, hive_table, uc_table_name)

        if differences:
            print(f"Schema differences: {differences}")
        assert len(differences) == 0, f"Schema mismatch: {differences}"

    def test_row_counts_match(self, spark, hive_table, uc_table_name, uc_ready):
        """Verify row counts match between Hive and UC versions."""
        hive_count, uc_count = compare_row_counts(spark, hive_table, uc_table_name)

        print(f"Row counts - Hive: {hive_count}, UC: {uc_count}")
        assert hive_count == uc_count, (
            f"Row count mismatch: Hive={hive_count}, UC={uc_count}"
        )

    def test_partition_counts_match(self, spark, hive_table, uc_table_name, uc_ready):
        """Verify partition counts match between Hive and UC versions."""
        hive_parts, uc_parts = compare_partition_counts(spark, hive_table, uc_table_name)

        print(f"Partition counts - Hive: {hive_parts}, UC: {uc_parts}")
        assert hive_parts == uc_parts, (
            f"Partition count mismatch: Hive={hive_parts}, UC={uc_parts}"
        )

    def test_partition_pruning_works(self, spark, uc_table_name, uc_ready):
        """Verify partition pruning works in UC table."""
        df = spark.sql(
            f"SELECT * FROM {uc_table_name} WHERE region = 'us-east'"
        )
        count = df.count()

        assert count > 0, "No data found for region=us-east"

        # Verify all rows have correct partition value
        regions = df.select("region").distinct().collect()
        assert len(regions) == 1
        assert regions[0].region == "us-east"

        print(f"Partition pruning works: {count} rows for region=us-east")

    def test_sample_query_results_match(self, spark, hive_table, uc_table_name, uc_ready):
        """Compare sample query results between Hive and UC."""
        query = "SELECT id, name, region FROM {} ORDER BY id LIMIT 5"

        hive_df = spark.sql(query.format(hive_table))
        uc_df = spark.sql(query.format(uc_table_name))

        hive_data = [row.asDict() for row in hive_df.collect()]
        uc_data = [row.asDict() for row in uc_df.collect()]

        assert hive_data == uc_data, (
            f"Query results differ:\nHive: {hive_data}\nUC: {uc_data}"
        )
        print("Sample query results match between Hive and UC")


@pytest.mark.uc_registration
class TestRecursiveTableRegistration:
    """
    Test UC registration for recursive_table (nested subdirectories).

    This scenario has no partitions but requires recursive file discovery.
    Known issue: Databricks may not scan nested subdirectories by default.
    """

    @pytest.fixture(scope="class")
    def scenario(self):
        return PARTITIONS_IN_NESTED_SUBDIRECTORIES

    @pytest.fixture(scope="class")
    def hive_table(self, glue_table, scenario):
        return glue_table(scenario.name)

    @pytest.fixture(scope="class")
    def uc_table_name(self, uc_table, scenario):
        return uc_table(scenario.name)

    def test_scenario_properties(self, scenario):
        """Document scenario properties."""
        assert scenario.recursive_scan_required
        assert len(scenario.partitions) == 0
        print(f"Scenario: {scenario.description}")

    def test_register_in_uc(self, spark, scenario, glue_database, uc_table_name, uc_ready):
        """Register the recursive table in UC."""
        result = register_uc_external_table(
            spark,
            scenario,
            glue_database,
            UC_CATALOG,
            UC_SCHEMA,
        )

        # May succeed but with limited data visibility
        if not result.success:
            pytest.skip(f"Registration failed: {result.error}")

        print(f"Registered {result.uc_table_name}")

    def test_row_counts_comparison(self, spark, hive_table, uc_table_name, uc_ready):
        """
        Compare row counts - expect potential discrepancy.

        UC may not recursively scan nested directories, so UC count
        may be lower than Hive count.
        """
        hive_count, uc_count = compare_row_counts(spark, hive_table, uc_table_name)

        print(f"Row counts - Hive: {hive_count}, UC: {uc_count}")

        if hive_count != uc_count:
            print(
                "EXPECTED: Row count mismatch due to recursive scan limitations. "
                "UC may not discover files in nested subdirectories."
            )
        # Don't assert equality - document behavior


@pytest.mark.uc_registration
class TestScatteredTableRegistration:
    """
    Test UC registration for scattered_table (external partitions).

    IMPORTANT: Unity Catalog does NOT support external partitions.
    Partitions outside the table root will not be accessible.

    This test documents the expected limitation.
    """

    @pytest.fixture(scope="class")
    def scenario(self):
        return PARTITIONS_OUTSIDE_TABLE_ROOT

    @pytest.fixture(scope="class")
    def hive_table(self, glue_table, scenario):
        return glue_table(scenario.name)

    @pytest.fixture(scope="class")
    def uc_table_name(self, uc_table, scenario):
        return uc_table(scenario.name)

    def test_scenario_has_external_partitions(self, scenario, glue_database, aws_region):
        """Verify scenario has partitions outside table root."""
        partition_locs = get_glue_partition_locations(
            glue_database, scenario.name, aws_region
        )
        all_under_root, external_locs = check_partitions_under_table_root(
            scenario.table_location, partition_locs
        )

        assert not all_under_root, "Expected external partitions"
        print(f"External partition locations: {external_locs}")

    def test_uc_compatibility_check(self, scenario):
        """Verify UC compatibility check correctly identifies limitation."""
        is_supported, reason = scenario_supports_uc(scenario)

        # Should NOT be supported
        assert not is_supported, "UC should not support external partitions"
        print(f"Compatibility check: {reason}")

    def test_register_in_uc_limited(self, spark, scenario, glue_database, uc_table_name, uc_ready):
        """
        Register scattered table in UC.

        Registration may succeed, but only partitions under table root
        will be accessible.
        """
        result = register_uc_external_table(
            spark,
            scenario,
            glue_database,
            UC_CATALOG,
            UC_SCHEMA,
        )

        # Registration may succeed but with warnings
        print(f"Registration result: success={result.success}")
        if result.notes:
            print(f"Notes: {result.notes}")
        if result.error:
            print(f"Error: {result.error}")

    def test_row_count_discrepancy(self, spark, hive_table, uc_table_name, uc_ready):
        """
        Document row count discrepancy due to external partitions.

        UC should see fewer rows than Hive because external partitions
        are not accessible.
        """
        hive_count, uc_count = compare_row_counts(spark, hive_table, uc_table_name)

        print(f"Row counts - Hive: {hive_count}, UC: {uc_count}")

        if uc_count < hive_count:
            print(
                "EXPECTED: UC sees fewer rows because external partitions "
                "(outside table root) are not accessible in UC."
            )
        elif uc_count == 0:
            print(
                "EXPECTED: UC sees 0 rows - all data may be in external partitions."
            )

    def test_partition_count_discrepancy(self, spark, hive_table, uc_table_name, uc_ready):
        """Document partition count discrepancy."""
        hive_parts, uc_parts = compare_partition_counts(spark, hive_table, uc_table_name)

        print(f"Partition counts - Hive: {hive_parts}, UC: {uc_parts}")

        if uc_parts < hive_parts:
            print(
                "EXPECTED: UC sees fewer partitions because external partitions "
                "cannot be discovered."
            )


@pytest.mark.uc_registration
class TestCrossBucketTableRegistration:
    """
    Test UC registration for cross_bucket_table.

    Tests whether UC external table can span multiple S3 buckets.
    Expectation: UC tables have a single LOCATION, so cross-bucket
    partitions may not be accessible.
    """

    @pytest.fixture(scope="class")
    def scenario(self):
        return PARTITIONS_ACROSS_S3_BUCKETS

    @pytest.fixture(scope="class")
    def hive_table(self, glue_table, scenario):
        return glue_table(scenario.name)

    @pytest.fixture(scope="class")
    def uc_table_name(self, uc_table, scenario):
        return uc_table(scenario.name)

    def test_has_cross_bucket_partitions(self, scenario):
        """Verify partitions span multiple buckets."""
        buckets = set()
        for p in scenario.partitions:
            # Extract bucket from s3://bucket/...
            bucket = p.s3_path.split("/")[2]
            buckets.add(bucket)

        assert len(buckets) > 1, "Expected partitions in multiple buckets"
        print(f"Partitions span buckets: {buckets}")

    def test_uc_compatibility(self, scenario):
        """Check UC compatibility for cross-bucket scenario."""
        is_supported, reason = scenario_supports_uc(scenario)
        print(f"UC compatibility: supported={is_supported}, reason={reason}")

    def test_register_in_uc(self, spark, scenario, glue_database, uc_table_name, uc_ready):
        """Attempt registration of cross-bucket table."""
        result = register_uc_external_table(
            spark,
            scenario,
            glue_database,
            UC_CATALOG,
            UC_SCHEMA,
        )

        print(f"Registration: success={result.success}")
        if result.notes:
            print(f"Notes: {result.notes}")
        if result.error:
            print(f"Error: {result.error}")

    def test_row_count_comparison(self, spark, hive_table, uc_table_name, uc_ready):
        """Compare row counts - document cross-bucket behavior."""
        hive_count, uc_count = compare_row_counts(spark, hive_table, uc_table_name)

        print(f"Row counts - Hive: {hive_count}, UC: {uc_count}")

        if uc_count < hive_count:
            print(
                "UC sees fewer rows - partitions in other buckets "
                "may not be accessible from single LOCATION."
            )

    def test_query_each_partition(self, spark, uc_table_name, uc_ready):
        """Test querying each partition individually."""
        for region in ["us-east", "us-west"]:
            try:
                df = spark.sql(
                    f"SELECT COUNT(*) as count FROM {uc_table_name} "
                    f"WHERE region = '{region}'"
                )
                count = df.collect()[0].count
                print(f"  region={region}: {count} rows")
            except Exception as e:
                print(f"  region={region}: FAILED - {str(e)[:100]}")


@pytest.mark.uc_registration
class TestCrossRegionTableRegistration:
    """
    Test UC registration for cross_region_table.

    Tests whether UC external table can span AWS regions.
    """

    @pytest.fixture(scope="class")
    def scenario(self):
        return PARTITIONS_ACROSS_AWS_REGIONS

    @pytest.fixture(scope="class")
    def hive_table(self, glue_table, scenario):
        return glue_table(scenario.name)

    @pytest.fixture(scope="class")
    def uc_table_name(self, uc_table, scenario):
        return uc_table(scenario.name)

    def test_has_cross_region_partitions(self, scenario):
        """Verify partitions span AWS regions."""
        # Check for different region indicators in bucket names
        locations = [p.s3_path for p in scenario.partitions]
        print(f"Partition locations: {locations}")

        # Buckets with 'east' and 'west' in names
        has_east = any("east" in loc for loc in locations)
        has_west = any("west" in loc for loc in locations)

        assert has_east and has_west, "Expected partitions in different regions"

    def test_register_in_uc(self, spark, scenario, glue_database, uc_table_name, uc_ready):
        """Attempt registration of cross-region table."""
        result = register_uc_external_table(
            spark,
            scenario,
            glue_database,
            UC_CATALOG,
            UC_SCHEMA,
        )

        print(f"Registration: success={result.success}")
        if result.notes:
            print(f"Notes: {result.notes}")

    def test_row_count_and_latency(self, spark, hive_table, uc_table_name, uc_ready):
        """Test row counts and note any latency from cross-region access."""
        import time

        # Time the UC query
        start = time.time()
        uc_df = spark.sql(f"SELECT COUNT(*) as count FROM {uc_table_name}")
        uc_count = uc_df.collect()[0].count
        uc_time = time.time() - start

        # Time the Hive query
        start = time.time()
        hive_df = spark.sql(f"SELECT COUNT(*) as count FROM {hive_table}")
        hive_count = hive_df.collect()[0].count
        hive_time = time.time() - start

        print(f"Hive: {hive_count} rows in {hive_time:.2f}s")
        print(f"UC: {uc_count} rows in {uc_time:.2f}s")


@pytest.mark.uc_registration
class TestSharedPartitionTablesRegistration:
    """
    Test UC registration for shared_partition tables.

    Tests whether multiple UC tables can point to same partition data.
    """

    @pytest.fixture(scope="class")
    def scenario_a(self):
        return PARTITIONS_SHARED_BETWEEN_TABLES_A

    @pytest.fixture(scope="class")
    def scenario_b(self):
        return PARTITIONS_SHARED_BETWEEN_TABLES_B

    @pytest.fixture(scope="class")
    def hive_table_a(self, glue_table, scenario_a):
        return glue_table(scenario_a.name)

    @pytest.fixture(scope="class")
    def hive_table_b(self, glue_table, scenario_b):
        return glue_table(scenario_b.name)

    @pytest.fixture(scope="class")
    def uc_table_a(self, uc_table, scenario_a):
        return uc_table(scenario_a.name)

    @pytest.fixture(scope="class")
    def uc_table_b(self, uc_table, scenario_b):
        return uc_table(scenario_b.name)

    def test_tables_share_partition(self, scenario_a, scenario_b):
        """Verify both tables reference same shared partition location."""
        a_locs = {p.s3_path for p in scenario_a.partitions}
        b_locs = {p.s3_path for p in scenario_b.partitions}

        shared = a_locs.intersection(b_locs)
        assert len(shared) > 0, "Expected shared partition locations"
        print(f"Shared partition locations: {shared}")

    def test_register_both_tables(
        self, spark, scenario_a, scenario_b, glue_database, uc_ready
    ):
        """Register both shared partition tables in UC."""
        for scenario in [scenario_a, scenario_b]:
            result = register_uc_external_table(
                spark,
                scenario,
                glue_database,
                UC_CATALOG,
                UC_SCHEMA,
            )
            print(f"{scenario.name}: success={result.success}")
            if result.notes:
                print(f"  Notes: {result.notes}")

    def test_shared_partition_data_visible(
        self, spark, uc_table_a, uc_table_b, uc_ready
    ):
        """
        Test if shared partition data is visible from both UC tables.

        Both tables should see the 'shared' partition data if UC supports this.
        """
        for table_name, partition_val in [
            (uc_table_a, "shared"),
            (uc_table_b, "shared"),
        ]:
            try:
                df = spark.sql(
                    f"SELECT COUNT(*) as count FROM {table_name} "
                    f"WHERE region = '{partition_val}'"
                )
                count = df.collect()[0].count
                print(f"{table_name} region={partition_val}: {count} rows")
            except Exception as e:
                print(f"{table_name} region={partition_val}: FAILED - {str(e)[:100]}")


@pytest.mark.uc_registration
class TestAllScenariosComparison:
    """
    Summary comparison across all scenarios.

    Runs full comparison for each scenario and generates summary report.
    """

    def test_full_comparison_report(self, spark, glue_database, uc_ready):
        """Generate comparison report for all scenarios."""
        results = []

        for scenario in ALL_SCENARIOS:
            # First check UC compatibility
            is_compatible, compat_reason = scenario_supports_uc(scenario)

            # Try to register
            reg_result = register_uc_external_table(
                spark,
                scenario,
                glue_database,
                UC_CATALOG,
                UC_SCHEMA,
            )

            if reg_result.success:
                # Run full comparison
                comparison = full_table_comparison(
                    spark, scenario, glue_database, UC_CATALOG, UC_SCHEMA
                )

                results.append({
                    "scenario": scenario.name,
                    "uc_compatible": is_compatible,
                    "registered": True,
                    "schema_match": comparison.schema_match,
                    "row_count_match": comparison.row_count_match,
                    "hive_rows": comparison.hive_row_count,
                    "uc_rows": comparison.uc_row_count,
                    "notes": comparison.notes or compat_reason,
                })
            else:
                results.append({
                    "scenario": scenario.name,
                    "uc_compatible": is_compatible,
                    "registered": False,
                    "schema_match": None,
                    "row_count_match": None,
                    "hive_rows": None,
                    "uc_rows": None,
                    "notes": reg_result.error or compat_reason,
                })

        # Print summary report
        print("\n" + "=" * 80)
        print("UC REGISTRATION COMPARISON REPORT")
        print("=" * 80)

        for r in results:
            print(f"\n{r['scenario']}:")
            print(f"  UC Compatible: {r['uc_compatible']}")
            print(f"  Registered: {r['registered']}")
            if r['registered']:
                print(f"  Schema Match: {r['schema_match']}")
                print(f"  Row Count Match: {r['row_count_match']}")
                print(f"  Rows - Hive: {r['hive_rows']}, UC: {r['uc_rows']}")
            if r['notes']:
                print(f"  Notes: {r['notes']}")

        print("\n" + "=" * 80)

        # All tests should complete without error (documenting behavior)
        assert len(results) == len(ALL_SCENARIOS)
