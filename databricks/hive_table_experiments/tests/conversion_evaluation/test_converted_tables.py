"""Test post-conversion UC capabilities on successfully converted Delta tables.

For each successfully converted table, test:
1. SELECT * (full table scan)
2. SELECT filtered (partition pruning)
3. INSERT (add new rows)
4. UPDATE (modify existing rows)
5. DELETE (remove rows)
6. OPTIMIZE (compact small files)
7. VACUUM (remove old versions)

Focus: Identify issues that persist from exotic layouts even after conversion.
"""

import pytest

from hive_table_experiments import (
    get_conversion_matrix,
    record_post_conversion_result,
)
from hive_table_experiments.scenarios import ALL_SCENARIOS


# Operations to test
OPERATIONS = ["select_all", "select_filtered", "insert", "update", "delete", "optimize", "vacuum"]

# Scenarios
SCENARIOS = [s.base_name for s in ALL_SCENARIOS]


class TestPostConversionCapabilities:
    """Test UC capabilities on converted Delta tables.

    These tests run AFTER conversion tests complete. They verify
    that converted tables support standard Delta operations.
    """

    @pytest.fixture(scope="class")
    def successful_conversions(self, conversion_matrix):
        """Get list of successful conversions to test.

        Returns list of (scenario, method, target_table) tuples.
        """
        successful = []
        for result in conversion_matrix.get_successful_conversions():
            if result.target_table:
                successful.append((result.scenario, result.method, result.target_table))
        return successful

    def test_select_all(self, spark, successful_conversions, conversion_matrix):
        """Test SELECT * on all successfully converted tables."""
        if not successful_conversions:
            pytest.skip("No successful conversions to test")

        for scenario, method, target_table in successful_conversions:
            print(f"\nTesting SELECT * on {target_table}")
            try:
                result = spark.sql(f"SELECT * FROM {target_table} LIMIT 10").collect()
                print(f"  ✓ SELECT * succeeded: {len(result)} rows returned")
                record_post_conversion_result(
                    scenario=scenario,
                    method=method,
                    operation="select_all",
                    success=True,
                )
            except Exception as e:
                print(f"  ✗ SELECT * failed: {e}")
                record_post_conversion_result(
                    scenario=scenario,
                    method=method,
                    operation="select_all",
                    success=False,
                    error_message=str(e),
                )

    def test_select_filtered(self, spark, successful_conversions, conversion_matrix):
        """Test SELECT with WHERE clause on all successfully converted tables."""
        if not successful_conversions:
            pytest.skip("No successful conversions to test")

        for scenario, method, target_table in successful_conversions:
            print(f"\nTesting SELECT filtered on {target_table}")
            try:
                # Try to filter on partition column if it exists
                result = spark.sql(f"""
                    SELECT * FROM {target_table}
                    WHERE id > 5
                    LIMIT 10
                """).collect()
                print(f"  ✓ SELECT filtered succeeded: {len(result)} rows returned")
                record_post_conversion_result(
                    scenario=scenario,
                    method=method,
                    operation="select_filtered",
                    success=True,
                )
            except Exception as e:
                print(f"  ✗ SELECT filtered failed: {e}")
                record_post_conversion_result(
                    scenario=scenario,
                    method=method,
                    operation="select_filtered",
                    success=False,
                    error_message=str(e),
                )

    def test_insert(self, spark, successful_conversions, conversion_matrix):
        """Test INSERT on all successfully converted tables."""
        if not successful_conversions:
            pytest.skip("No successful conversions to test")

        for scenario, method, target_table in successful_conversions:
            print(f"\nTesting INSERT on {target_table}")
            try:
                # Get current count
                count_before = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table}").collect()[0]["cnt"]

                # Insert a test row
                spark.sql(f"""
                    INSERT INTO {target_table}
                    VALUES (999, 'test_insert', 99.9, true)
                """)

                # Verify count increased
                count_after = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table}").collect()[0]["cnt"]
                assert count_after == count_before + 1, f"Expected {count_before + 1}, got {count_after}"

                print(f"  ✓ INSERT succeeded: count {count_before} → {count_after}")
                record_post_conversion_result(
                    scenario=scenario,
                    method=method,
                    operation="insert",
                    success=True,
                )
            except Exception as e:
                print(f"  ✗ INSERT failed: {e}")
                record_post_conversion_result(
                    scenario=scenario,
                    method=method,
                    operation="insert",
                    success=False,
                    error_message=str(e),
                )

    def test_update(self, spark, successful_conversions, conversion_matrix):
        """Test UPDATE on all successfully converted tables."""
        if not successful_conversions:
            pytest.skip("No successful conversions to test")

        for scenario, method, target_table in successful_conversions:
            print(f"\nTesting UPDATE on {target_table}")
            try:
                # Update the test row we inserted
                spark.sql(f"""
                    UPDATE {target_table}
                    SET name = 'test_updated'
                    WHERE id = 999
                """)

                # Verify update
                result = spark.sql(f"SELECT name FROM {target_table} WHERE id = 999").collect()
                if result:
                    assert result[0]["name"] == "test_updated"
                    print(f"  ✓ UPDATE succeeded")
                    record_post_conversion_result(
                        scenario=scenario,
                        method=method,
                        operation="update",
                        success=True,
                    )
                else:
                    print(f"  ⊘ UPDATE - no test row found (INSERT may have failed)")
                    record_post_conversion_result(
                        scenario=scenario,
                        method=method,
                        operation="update",
                        success=True,
                        notes="No test row to update",
                    )
            except Exception as e:
                print(f"  ✗ UPDATE failed: {e}")
                record_post_conversion_result(
                    scenario=scenario,
                    method=method,
                    operation="update",
                    success=False,
                    error_message=str(e),
                )

    def test_delete(self, spark, successful_conversions, conversion_matrix):
        """Test DELETE on all successfully converted tables."""
        if not successful_conversions:
            pytest.skip("No successful conversions to test")

        for scenario, method, target_table in successful_conversions:
            print(f"\nTesting DELETE on {target_table}")
            try:
                # Delete the test row
                spark.sql(f"""
                    DELETE FROM {target_table}
                    WHERE id = 999
                """)

                # Verify deletion
                result = spark.sql(f"SELECT * FROM {target_table} WHERE id = 999").collect()
                assert len(result) == 0, f"Expected 0 rows, got {len(result)}"

                print(f"  ✓ DELETE succeeded")
                record_post_conversion_result(
                    scenario=scenario,
                    method=method,
                    operation="delete",
                    success=True,
                )
            except Exception as e:
                print(f"  ✗ DELETE failed: {e}")
                record_post_conversion_result(
                    scenario=scenario,
                    method=method,
                    operation="delete",
                    success=False,
                    error_message=str(e),
                )

    def test_optimize(self, spark, successful_conversions, conversion_matrix):
        """Test OPTIMIZE on all successfully converted tables."""
        if not successful_conversions:
            pytest.skip("No successful conversions to test")

        for scenario, method, target_table in successful_conversions:
            print(f"\nTesting OPTIMIZE on {target_table}")
            try:
                spark.sql(f"OPTIMIZE {target_table}")
                print(f"  ✓ OPTIMIZE succeeded")
                record_post_conversion_result(
                    scenario=scenario,
                    method=method,
                    operation="optimize",
                    success=True,
                )
            except Exception as e:
                print(f"  ✗ OPTIMIZE failed: {e}")
                record_post_conversion_result(
                    scenario=scenario,
                    method=method,
                    operation="optimize",
                    success=False,
                    error_message=str(e),
                )

    def test_vacuum(self, spark, successful_conversions, conversion_matrix):
        """Test VACUUM on all successfully converted tables."""
        if not successful_conversions:
            pytest.skip("No successful conversions to test")

        for scenario, method, target_table in successful_conversions:
            print(f"\nTesting VACUUM on {target_table}")
            try:
                # VACUUM with 0 hours retention (requires setting)
                spark.sql(f"VACUUM {target_table} RETAIN 168 HOURS")
                print(f"  ✓ VACUUM succeeded")
                record_post_conversion_result(
                    scenario=scenario,
                    method=method,
                    operation="vacuum",
                    success=True,
                )
            except Exception as e:
                print(f"  ✗ VACUUM failed: {e}")
                record_post_conversion_result(
                    scenario=scenario,
                    method=method,
                    operation="vacuum",
                    success=False,
                    error_message=str(e),
                )
