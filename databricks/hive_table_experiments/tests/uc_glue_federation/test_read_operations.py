"""Read operation tests for uc_glue_federation access method.

Tests SELECT operations via Unity Catalog federation to Glue.
Note: Federation is READ-ONLY - no write tests in this module.
"""

import pytest
from hive_table_experiments import (
    OperationResult,
    ALL_SCENARIOS,
    UC_GLUE_FEDERATION,
)


class TestGlueFederationReadOperations:
    """Test read operations via UC Glue federation."""

    @pytest.mark.parametrize(
        "scenario",
        [s for s in ALL_SCENARIOS if s.supports_access_method(UC_GLUE_FEDERATION)[0]],
        ids=lambda s: s.base_name,
    )
    def test_select_all(self, spark, results_matrix, scenario, access_config):
        """Test SELECT * returns expected row count."""
        table = access_config.get_fully_qualified_table(scenario.base_name)

        try:
            df = spark.sql(f"SELECT * FROM {table}")
            count = df.count()

            results_matrix.record_result(
                scenario=scenario.base_name,
                access_method=access_config.table_suffix,
                operation="select_all",
                result=OperationResult.OK if count > 0 else OperationResult.ZERO_ROWS,
                row_count=count,
            )

            assert count > 0, f"Expected rows but got {count}"

        except Exception as e:
            results_matrix.record_result(
                scenario=scenario.base_name,
                access_method=access_config.table_suffix,
                operation="select_all",
                result=OperationResult.FAIL,
                error=str(e)[:200],
            )
            pytest.fail(f"Query failed: {e}")

    @pytest.mark.parametrize(
        "scenario",
        [s for s in ALL_SCENARIOS if s.supports_access_method(UC_GLUE_FEDERATION)[0] and s.partitions],
        ids=lambda s: s.base_name,
    )
    def test_partition_filter(self, spark, results_matrix, scenario, access_config):
        """Test partition filtering returns expected results."""
        table = access_config.get_fully_qualified_table(scenario.base_name)
        partition = scenario.partitions[0]

        try:
            query = f"""
                SELECT * FROM {table}
                WHERE {partition.partition_key} = '{partition.partition_value}'
            """
            df = spark.sql(query)
            count = df.count()

            results_matrix.record_result(
                scenario=scenario.base_name,
                access_method=access_config.table_suffix,
                operation="select_filtered",
                result=OperationResult.OK if count > 0 else OperationResult.ZERO_ROWS,
                row_count=count,
            )

            assert count > 0, f"Expected rows but got {count}"

        except Exception as e:
            results_matrix.record_result(
                scenario=scenario.base_name,
                access_method=access_config.table_suffix,
                operation="select_filtered",
                result=OperationResult.FAIL,
                error=str(e)[:200],
            )
            pytest.fail(f"Query failed: {e}")


class TestGlueFederationWriteRestrictions:
    """Verify that federation is read-only."""

    @pytest.mark.parametrize(
        "scenario",
        [s for s in ALL_SCENARIOS if s.supports_access_method(UC_GLUE_FEDERATION)[0]][:1],
        ids=lambda s: s.base_name,
    )
    def test_insert_fails(self, spark, results_matrix, scenario, access_config):
        """Verify INSERT is not supported via federation."""
        table = access_config.get_fully_qualified_table(scenario.base_name)

        try:
            spark.sql(f"""
                INSERT INTO {table}
                VALUES (9999, 'test_fed', 0.0, true, 'test')
            """)
            # If we get here, write succeeded unexpectedly
            results_matrix.record_result(
                scenario=scenario.base_name,
                access_method=access_config.table_suffix,
                operation="insert",
                result=OperationResult.OK,
                notes="INSERT unexpectedly succeeded",
            )
            pytest.fail("INSERT should fail on read-only federation")

        except Exception as e:
            # Expected - federation is read-only
            results_matrix.record_result(
                scenario=scenario.base_name,
                access_method=access_config.table_suffix,
                operation="insert",
                result=OperationResult.NOT_SUPPORTED,
                notes="Federation is read-only as expected",
            )
