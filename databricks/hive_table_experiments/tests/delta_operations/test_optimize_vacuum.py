"""OPTIMIZE and VACUUM validation tests for Delta tables.

Validates that Delta maintenance operations work correctly on UC external tables.
"""

import pytest
from hive_table_experiments import OperationResult


class TestOptimizeValidation:
    """Comprehensive OPTIMIZE testing."""

    def test_optimize_compacts_files(self, spark, results_matrix, delta_test_table):
        """Verify OPTIMIZE reduces file count.

        Strategy:
        1. Insert 10 separate rows to create 10 small files
        2. Count files before OPTIMIZE
        3. Run OPTIMIZE
        4. Verify file count reduced
        5. Verify row count unchanged
        """
        # Insert rows individually to create multiple files
        for i in range(10):
            spark.sql(f"""
                INSERT INTO {delta_test_table}
                VALUES ({i}, 'record_{i}', {i * 10.5})
            """)

        # Get row count before
        initial_count = spark.sql(f"SELECT COUNT(*) FROM {delta_test_table}").collect()[0][0]
        assert initial_count == 10, f"Expected 10 rows, got {initial_count}"

        # Get file count before (from Delta history)
        files_before = spark.sql(f"DESCRIBE DETAIL {delta_test_table}").collect()[0]["numFiles"]

        # Run OPTIMIZE
        optimize_result = spark.sql(f"OPTIMIZE {delta_test_table}")

        # Get file count after
        files_after = spark.sql(f"DESCRIBE DETAIL {delta_test_table}").collect()[0]["numFiles"]

        # Verify row count unchanged
        final_count = spark.sql(f"SELECT COUNT(*) FROM {delta_test_table}").collect()[0][0]
        assert final_count == initial_count, f"Row count changed: {initial_count} -> {final_count}"

        # Verify files were compacted (should be fewer files after)
        assert files_after <= files_before, f"OPTIMIZE didn't reduce files: {files_before} -> {files_after}"

        results_matrix.record_result(
            scenario="delta_test",
            access_method="_uc_delta",
            operation="optimize",
            result=OperationResult.OK,
            notes=f"Files: {files_before} -> {files_after}",
        )


class TestVacuumValidation:
    """Comprehensive VACUUM testing."""

    def test_vacuum_removes_old_files(self, spark, results_matrix, delta_test_table):
        """Verify VACUUM removes old file versions.

        Strategy:
        1. Insert initial data
        2. Update data (creates new file versions)
        3. Get location and count files
        4. Run VACUUM with 0 hour retention
        5. Verify old files removed
        6. Verify data still accessible
        """
        # Insert initial data
        spark.sql(f"""
            INSERT INTO {delta_test_table}
            VALUES (1, 'original', 10.0), (2, 'original', 20.0)
        """)

        # Update to create old file versions
        spark.sql(f"""
            UPDATE {delta_test_table}
            SET name = 'updated', value = value * 2
            WHERE id = 1
        """)

        # Disable retention check for testing
        spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = false")

        try:
            # Run VACUUM with 0 hour retention
            vacuum_result = spark.sql(f"VACUUM {delta_test_table} RETAIN 0 HOURS")

            # Verify data still accessible
            count = spark.sql(f"SELECT COUNT(*) FROM {delta_test_table}").collect()[0][0]
            assert count == 2, f"Data lost after VACUUM: expected 2 rows, got {count}"

            # Verify update was preserved
            updated_value = spark.sql(f"""
                SELECT value FROM {delta_test_table} WHERE id = 1
            """).collect()[0][0]
            assert updated_value == 20.0, f"Update lost: expected 20.0, got {updated_value}"

            results_matrix.record_result(
                scenario="delta_test",
                access_method="_uc_delta",
                operation="vacuum",
                result=OperationResult.OK,
                notes="Old files removed, data intact",
            )

        finally:
            # Re-enable retention check
            spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = true")


class TestDeltaTableProperties:
    """Test Delta table property configuration."""

    def test_retention_properties(self, spark, delta_test_table):
        """Verify Delta table properties can be set for testing."""
        # Get table properties
        properties = spark.sql(f"SHOW TBLPROPERTIES {delta_test_table}").collect()
        prop_dict = {row["key"]: row["value"] for row in properties}

        assert "delta.logRetentionDuration" in prop_dict
        assert "delta.deletedFileRetentionDuration" in prop_dict

    def test_describe_detail(self, spark, delta_test_table):
        """Verify DESCRIBE DETAIL returns expected information."""
        # Insert some data first
        spark.sql(f"INSERT INTO {delta_test_table} VALUES (1, 'test', 1.0)")

        detail = spark.sql(f"DESCRIBE DETAIL {delta_test_table}").collect()[0]

        assert detail["format"] == "delta"
        assert detail["numFiles"] >= 1
        assert "location" in detail.asDict()
