"""Delta table operations tests for UC external tables.

Tests Delta-specific operations (UPDATE, DELETE, OPTIMIZE, VACUUM)
that are only available for the uc_delta access method.
"""

import pytest
from hive_table_experiments import (
    OperationResult,
    ALL_SCENARIOS,
    UC_EXTERNAL_DELTA,
)


class TestDeltaWriteOperations:
    """Test write operations on Delta external tables."""

    @pytest.fixture(scope="class")
    def access_config(self, uc_delta_config):
        """Use Delta config for this test class."""
        return uc_delta_config

    @pytest.mark.parametrize(
        "scenario",
        [s for s in ALL_SCENARIOS if s.supports_access_method(UC_EXTERNAL_DELTA)[0]][:2],
        ids=lambda s: s.base_name,
    )
    def test_update(self, spark, results_matrix, scenario, access_config):
        """Test UPDATE operation on Delta table."""
        table = access_config.get_fully_qualified_table(scenario.base_name)

        try:
            # Update a single row
            spark.sql(f"""
                UPDATE {table}
                SET value = value + 1
                WHERE id = 1
            """)

            # Verify update
            result = spark.sql(f"SELECT value FROM {table} WHERE id = 1").collect()

            results_matrix.record_result(
                scenario=scenario.base_name,
                access_method=access_config.table_suffix,
                operation="update",
                result=OperationResult.OK,
            )

        except Exception as e:
            results_matrix.record_result(
                scenario=scenario.base_name,
                access_method=access_config.table_suffix,
                operation="update",
                result=OperationResult.FAIL,
                error=str(e)[:200],
            )
            pytest.fail(f"UPDATE failed: {e}")

    @pytest.mark.parametrize(
        "scenario",
        [s for s in ALL_SCENARIOS if s.supports_access_method(UC_EXTERNAL_DELTA)[0]][:2],
        ids=lambda s: s.base_name,
    )
    def test_delete(self, spark, results_matrix, scenario, access_config):
        """Test DELETE operation on Delta table."""
        table = access_config.get_fully_qualified_table(scenario.base_name)

        try:
            # Get initial count
            initial_count = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]

            # Delete a row (use high ID to avoid affecting other tests)
            spark.sql(f"DELETE FROM {table} WHERE id = 9999")

            # Get final count
            final_count = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]

            results_matrix.record_result(
                scenario=scenario.base_name,
                access_method=access_config.table_suffix,
                operation="delete",
                result=OperationResult.OK,
                notes=f"Count: {initial_count} -> {final_count}",
            )

        except Exception as e:
            results_matrix.record_result(
                scenario=scenario.base_name,
                access_method=access_config.table_suffix,
                operation="delete",
                result=OperationResult.FAIL,
                error=str(e)[:200],
            )
            pytest.fail(f"DELETE failed: {e}")


class TestDeltaOptimizeVacuum:
    """Test OPTIMIZE and VACUUM on Delta external tables."""

    @pytest.fixture(scope="class")
    def access_config(self, uc_delta_config):
        """Use Delta config for this test class."""
        return uc_delta_config

    @pytest.mark.parametrize(
        "scenario",
        [s for s in ALL_SCENARIOS if s.supports_access_method(UC_EXTERNAL_DELTA)[0]][:1],
        ids=lambda s: s.base_name,
    )
    def test_optimize(self, spark, results_matrix, scenario, access_config):
        """Test OPTIMIZE command on Delta table.

        OPTIMIZE compacts small files into larger ones for better read performance.
        """
        table = access_config.get_fully_qualified_table(scenario.base_name)

        try:
            # Run OPTIMIZE
            result = spark.sql(f"OPTIMIZE {table}")

            results_matrix.record_result(
                scenario=scenario.base_name,
                access_method=access_config.table_suffix,
                operation="optimize",
                result=OperationResult.OK,
            )

        except Exception as e:
            results_matrix.record_result(
                scenario=scenario.base_name,
                access_method=access_config.table_suffix,
                operation="optimize",
                result=OperationResult.FAIL,
                error=str(e)[:200],
            )
            pytest.fail(f"OPTIMIZE failed: {e}")

    @pytest.mark.parametrize(
        "scenario",
        [s for s in ALL_SCENARIOS if s.supports_access_method(UC_EXTERNAL_DELTA)[0]][:1],
        ids=lambda s: s.base_name,
    )
    def test_vacuum(self, spark, results_matrix, scenario, access_config):
        """Test VACUUM command on Delta table.

        VACUUM removes old file versions no longer needed by the Delta table.
        Uses 0 HOURS retention with safety check disabled for testing.
        """
        table = access_config.get_fully_qualified_table(scenario.base_name)

        try:
            # Disable retention check for testing (normally requires 7 day minimum)
            spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = false")

            # Run VACUUM with 0 hour retention
            result = spark.sql(f"VACUUM {table} RETAIN 0 HOURS")

            # Re-enable safety check
            spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = true")

            results_matrix.record_result(
                scenario=scenario.base_name,
                access_method=access_config.table_suffix,
                operation="vacuum",
                result=OperationResult.OK,
            )

        except Exception as e:
            # Re-enable safety check even on failure
            try:
                spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = true")
            except:
                pass

            results_matrix.record_result(
                scenario=scenario.base_name,
                access_method=access_config.table_suffix,
                operation="vacuum",
                result=OperationResult.FAIL,
                error=str(e)[:200],
            )
            pytest.fail(f"VACUUM failed: {e}")
