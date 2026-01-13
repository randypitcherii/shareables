"""Read operation tests for glue_as_hive_metastore access method.

Tests SELECT operations across all compatible scenarios using the hive_metastore catalog.
"""

import pytest
from hive_table_experiments import (
    OperationResult,
    ALL_SCENARIOS,
    GLUE_AS_HIVE,
)


class TestGlueHiveReadOperations:
    """Test read operations via glue_as_hive_metastore."""

    @pytest.mark.parametrize(
        "scenario",
        [s for s in ALL_SCENARIOS if s.supports_access_method(GLUE_AS_HIVE)[0]],
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
        [s for s in ALL_SCENARIOS if s.supports_access_method(GLUE_AS_HIVE)[0] and s.partitions],
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
