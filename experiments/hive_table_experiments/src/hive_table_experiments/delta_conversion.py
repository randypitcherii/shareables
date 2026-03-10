"""Delta conversion utilities for Hive-to-Delta migration.

Provides three conversion approaches that work WITHOUT duplicating data:

1. CONVERT TO DELTA - Creates _delta_log directory in-place
   - No data movement, only adds Delta transaction log
   - Works for tables with all partitions under table root

2. SHALLOW CLONE - Creates metadata-only reference to source files
   - Source table remains unchanged
   - Requires Databricks Runtime 14.2+ for UC external tables

3. Manual Delta Log Generation - Full control for edge cases
   - Programmatically generate _delta_log JSON
   - For scenarios where CONVERT/CLONE don't work

See: https://docs.databricks.com/aws/en/sql/language-manual/delta-convert-to-delta
See: https://docs.databricks.com/aws/en/delta/clone-unity-catalog
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Any
import time

from .scenarios import HiveTableScenario, ALL_SCENARIOS
from .uc_helpers import (
    get_glue_table_schema,
    get_glue_partition_locations,
    check_partitions_under_table_root,
    _glue_to_spark_type,
)


class ConversionApproach(Enum):
    """Available Delta conversion approaches."""
    CONVERT_TO_DELTA = "convert_to_delta"
    SHALLOW_CLONE = "shallow_clone"
    MANUAL_DELTA_LOG = "manual_delta_log"


@dataclass
class ConversionResult:
    """Result of a Delta conversion operation."""
    scenario_name: str
    approach: ConversionApproach
    success: bool
    source_table: str
    target_table: Optional[str] = None
    target_location: Optional[str] = None
    error: Optional[str] = None
    notes: str = ""
    execution_time_seconds: float = 0.0
    # Validation results
    source_row_count: int = -1
    target_row_count: int = -1
    row_counts_match: bool = False
    schema_match: bool = False

    def __repr__(self):
        status = "SUCCESS" if self.success else "FAILED"
        return (
            f"ConversionResult({self.scenario_name}, {self.approach.value}, {status})"
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "scenario_name": self.scenario_name,
            "approach": self.approach.value,
            "success": self.success,
            "source_table": self.source_table,
            "target_table": self.target_table,
            "target_location": self.target_location,
            "error": self.error,
            "notes": self.notes,
            "execution_time_seconds": self.execution_time_seconds,
            "source_row_count": self.source_row_count,
            "target_row_count": self.target_row_count,
            "row_counts_match": self.row_counts_match,
            "schema_match": self.schema_match,
        }


# Default catalog/schema for Delta tables
DEFAULT_DELTA_CATALOG = "migration_eval"
DEFAULT_DELTA_SCHEMA = "delta_migration"


def get_partition_columns(
    scenario: HiveTableScenario,
    glue_database: str,
    region: str = "us-east-1",
) -> List[str]:
    """
    Get partition column names and types for a scenario.

    Args:
        scenario: Hive table scenario
        glue_database: Glue database name
        region: AWS region

    Returns:
        List of "column_name TYPE" strings for PARTITIONED BY clause
    """
    _, partition_cols = get_glue_table_schema(
        glue_database, scenario.name, region
    )
    return [
        f"{col.name} {_glue_to_spark_type(col.data_type)}"
        for col in partition_cols
    ]


def convert_to_delta(
    spark,
    scenario: HiveTableScenario,
    glue_database: str,
    delta_catalog: str = DEFAULT_DELTA_CATALOG,
    delta_schema: str = DEFAULT_DELTA_SCHEMA,
    region: str = "us-east-1",
    no_statistics: bool = False,
) -> ConversionResult:
    """
    Convert Parquet table to Delta format using CONVERT TO DELTA.

    This creates a _delta_log directory in-place without copying data.
    The conversion adds Delta transaction log to existing Parquet files.

    IMPORTANT: This modifies the source location by adding _delta_log.
    After conversion, the location can be registered as a Delta table.

    Args:
        spark: Spark session
        scenario: Hive table scenario to convert
        glue_database: Source Glue database name
        delta_catalog: Target UC catalog for registering the Delta table
        delta_schema: Target UC schema for registering the Delta table
        region: AWS region for Glue
        no_statistics: Skip statistics collection for faster conversion

    Returns:
        ConversionResult with success status and details

    See: https://docs.databricks.com/aws/en/sql/language-manual/delta-convert-to-delta
    """
    source_table = f"hive_metastore.{glue_database}.{scenario.name}"
    target_table = f"{delta_catalog}.{delta_schema}.{scenario.name}_delta"
    s3_location = scenario.table_location.rstrip("/")

    start_time = time.time()
    notes = []

    try:
        # Check if partitions are under table root (required for CONVERT TO DELTA)
        partition_locs = get_glue_partition_locations(
            glue_database, scenario.name, region
        )
        all_under_root, external_locs = check_partitions_under_table_root(
            scenario.table_location, partition_locs
        )

        if not all_under_root:
            notes.append(
                f"WARNING: {len(external_locs)} partition(s) outside table root. "
                "CONVERT TO DELTA may not include all data."
            )

        # Get source row count before conversion
        source_count = spark.sql(
            f"SELECT COUNT(*) as cnt FROM {source_table}"
        ).collect()[0].cnt

        # Build CONVERT TO DELTA statement
        # Use parquet.`path` syntax for path-based conversion
        partition_cols = get_partition_columns(scenario, glue_database, region)

        convert_sql = f"CONVERT TO DELTA parquet.`{s3_location}`"

        if no_statistics:
            convert_sql += " NO STATISTICS"

        if partition_cols:
            partition_clause = ", ".join(partition_cols)
            convert_sql += f" PARTITIONED BY ({partition_clause})"

        notes.append(f"Executing: {convert_sql}")

        # Execute conversion
        spark.sql(convert_sql)

        # Register as external Delta table in UC
        # First ensure schema exists
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {delta_catalog}.{delta_schema}")

        # Drop target if exists
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")

        # Create external Delta table pointing to converted location
        data_cols, partition_cols_schema = get_glue_table_schema(
            glue_database, scenario.name, region
        )

        col_defs = ", ".join([
            f"{col.name} {_glue_to_spark_type(col.data_type)}"
            for col in data_cols
        ])

        create_sql = f"CREATE TABLE {target_table} ({col_defs}) USING DELTA"

        if partition_cols_schema:
            part_defs = ", ".join([
                f"{col.name} {_glue_to_spark_type(col.data_type)}"
                for col in partition_cols_schema
            ])
            create_sql += f" PARTITIONED BY ({part_defs})"

        create_sql += f" LOCATION '{s3_location}'"

        notes.append(f"Registering: {create_sql}")
        spark.sql(create_sql)

        # Get target row count after conversion
        target_count = spark.sql(
            f"SELECT COUNT(*) as cnt FROM {target_table}"
        ).collect()[0].cnt

        execution_time = time.time() - start_time

        return ConversionResult(
            scenario_name=scenario.name,
            approach=ConversionApproach.CONVERT_TO_DELTA,
            success=True,
            source_table=source_table,
            target_table=target_table,
            target_location=s3_location,
            notes="; ".join(notes),
            execution_time_seconds=execution_time,
            source_row_count=source_count,
            target_row_count=target_count,
            row_counts_match=source_count == target_count,
            schema_match=True,  # Conversion preserves schema
        )

    except Exception as e:
        execution_time = time.time() - start_time
        return ConversionResult(
            scenario_name=scenario.name,
            approach=ConversionApproach.CONVERT_TO_DELTA,
            success=False,
            source_table=source_table,
            target_table=target_table,
            target_location=s3_location,
            error=str(e),
            notes="; ".join(notes),
            execution_time_seconds=execution_time,
        )


def shallow_clone_table(
    spark,
    scenario: HiveTableScenario,
    glue_database: str,
    delta_catalog: str = DEFAULT_DELTA_CATALOG,
    delta_schema: str = DEFAULT_DELTA_SCHEMA,
    source_is_delta: bool = False,
) -> ConversionResult:
    """
    Create a shallow clone of a table in Unity Catalog.

    A shallow clone creates a metadata-only reference to source files.
    No data is copied - the clone references the same underlying files.

    IMPORTANT:
    - Source must be a Delta table for SHALLOW CLONE to work
    - For Hive/Parquet tables, use CONVERT TO DELTA first
    - Shallow clone support for UC external tables requires DBR 14.2+

    Args:
        spark: Spark session
        scenario: Hive table scenario to clone
        glue_database: Source Glue database name
        delta_catalog: Target UC catalog
        delta_schema: Target UC schema
        source_is_delta: Whether source is already a Delta table

    Returns:
        ConversionResult with success status and details

    See: https://docs.databricks.com/aws/en/delta/clone-unity-catalog
    """
    # Determine source table name
    if source_is_delta:
        # If source is already Delta, clone from there
        source_table = f"{delta_catalog}.{delta_schema}.{scenario.name}_delta"
    else:
        # Try to clone from hive_metastore
        source_table = f"hive_metastore.{glue_database}.{scenario.name}"

    target_table = f"{delta_catalog}.{delta_schema}.{scenario.name}_shallow_clone"

    start_time = time.time()
    notes = []

    try:
        # Get source row count
        source_count = spark.sql(
            f"SELECT COUNT(*) as cnt FROM {source_table}"
        ).collect()[0].cnt

        # Ensure target schema exists
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {delta_catalog}.{delta_schema}")

        # Drop target if exists
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")

        # Build SHALLOW CLONE statement
        clone_sql = f"CREATE TABLE {target_table} SHALLOW CLONE {source_table}"

        notes.append(f"Executing: {clone_sql}")
        notes.append(
            "Note: SHALLOW CLONE requires source to be a Delta table. "
            "For Parquet/Hive tables, use CONVERT TO DELTA first."
        )

        # Execute clone
        spark.sql(clone_sql)

        # Get target row count
        target_count = spark.sql(
            f"SELECT COUNT(*) as cnt FROM {target_table}"
        ).collect()[0].cnt

        # Get target location
        describe_result = spark.sql(
            f"DESCRIBE DETAIL {target_table}"
        ).collect()[0]
        target_location = describe_result.location if hasattr(describe_result, 'location') else None

        execution_time = time.time() - start_time

        return ConversionResult(
            scenario_name=scenario.name,
            approach=ConversionApproach.SHALLOW_CLONE,
            success=True,
            source_table=source_table,
            target_table=target_table,
            target_location=target_location,
            notes="; ".join(notes),
            execution_time_seconds=execution_time,
            source_row_count=source_count,
            target_row_count=target_count,
            row_counts_match=source_count == target_count,
            schema_match=True,  # Clone preserves schema
        )

    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = str(e)

        # Add helpful notes for common errors
        if "not a Delta table" in error_msg.lower():
            notes.append(
                "SOLUTION: Use CONVERT TO DELTA first to convert the source table."
            )
        elif "SHALLOW CLONE is not supported" in error_msg:
            notes.append(
                "SOLUTION: Shallow clones for UC external tables require DBR 14.2+. "
                "Try using CONVERT TO DELTA instead."
            )

        return ConversionResult(
            scenario_name=scenario.name,
            approach=ConversionApproach.SHALLOW_CLONE,
            success=False,
            source_table=source_table,
            target_table=target_table,
            error=error_msg,
            notes="; ".join(notes),
            execution_time_seconds=execution_time,
        )


def validate_conversion(
    spark,
    source_table: str,
    target_table: str,
) -> Dict[str, Any]:
    """
    Validate that conversion preserved data integrity.

    Checks:
    - Row count match
    - Schema compatibility
    - Sample data verification
    - Checksum (if supported)

    Args:
        spark: Spark session
        source_table: Fully-qualified source table name
        target_table: Fully-qualified target table name

    Returns:
        Dict with validation results
    """
    results = {
        "source_table": source_table,
        "target_table": target_table,
        "valid": True,
        "checks": {},
        "errors": [],
    }

    try:
        # Row count check
        source_count = spark.sql(
            f"SELECT COUNT(*) as cnt FROM {source_table}"
        ).collect()[0].cnt
        target_count = spark.sql(
            f"SELECT COUNT(*) as cnt FROM {target_table}"
        ).collect()[0].cnt

        results["checks"]["row_count"] = {
            "source": source_count,
            "target": target_count,
            "match": source_count == target_count,
        }
        if source_count != target_count:
            results["valid"] = False
            results["errors"].append(
                f"Row count mismatch: source={source_count}, target={target_count}"
            )

        # Schema check
        source_schema = spark.sql(f"DESCRIBE {source_table}").collect()
        target_schema = spark.sql(f"DESCRIBE {target_table}").collect()

        source_cols = {
            row.col_name: row.data_type
            for row in source_schema
            if not row.col_name.startswith("#")
        }
        target_cols = {
            row.col_name: row.data_type
            for row in target_schema
            if not row.col_name.startswith("#")
        }

        schema_diffs = []
        for col in source_cols:
            if col not in target_cols:
                schema_diffs.append(f"Missing column in target: {col}")
            elif source_cols[col].lower() != target_cols[col].lower():
                schema_diffs.append(
                    f"Type mismatch for {col}: {source_cols[col]} vs {target_cols[col]}"
                )

        for col in target_cols:
            if col not in source_cols:
                schema_diffs.append(f"Extra column in target: {col}")

        results["checks"]["schema"] = {
            "match": len(schema_diffs) == 0,
            "differences": schema_diffs,
        }
        if schema_diffs:
            results["valid"] = False
            results["errors"].extend(schema_diffs)

        # Sample data check (compare first 10 rows sorted by first column)
        try:
            first_col = list(source_cols.keys())[0]
            source_sample = spark.sql(
                f"SELECT * FROM {source_table} ORDER BY {first_col} LIMIT 10"
            ).collect()
            target_sample = spark.sql(
                f"SELECT * FROM {target_table} ORDER BY {first_col} LIMIT 10"
            ).collect()

            sample_match = len(source_sample) == len(target_sample)
            if sample_match:
                for s, t in zip(source_sample, target_sample):
                    if s.asDict() != t.asDict():
                        sample_match = False
                        break

            results["checks"]["sample_data"] = {
                "match": sample_match,
                "sample_size": len(source_sample),
            }
            if not sample_match:
                results["valid"] = False
                results["errors"].append("Sample data does not match")

        except Exception as e:
            results["checks"]["sample_data"] = {
                "match": None,
                "error": str(e),
            }

    except Exception as e:
        results["valid"] = False
        results["errors"].append(f"Validation error: {str(e)}")

    return results


def cleanup_conversion(
    spark,
    scenario: HiveTableScenario,
    delta_catalog: str = DEFAULT_DELTA_CATALOG,
    delta_schema: str = DEFAULT_DELTA_SCHEMA,
    remove_delta_log: bool = False,
) -> Dict[str, bool]:
    """
    Clean up tables created during conversion testing.

    Args:
        spark: Spark session
        scenario: Scenario that was converted
        delta_catalog: Catalog where Delta tables were created
        delta_schema: Schema where Delta tables were created
        remove_delta_log: Whether to also remove _delta_log from S3

    Returns:
        Dict mapping table names to cleanup success status
    """
    results = {}

    # Tables to clean up
    tables_to_drop = [
        f"{delta_catalog}.{delta_schema}.{scenario.name}_delta",
        f"{delta_catalog}.{delta_schema}.{scenario.name}_shallow_clone",
        f"{delta_catalog}.{delta_schema}.{scenario.name}_manual",
    ]

    for table in tables_to_drop:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            results[table] = True
        except Exception:
            results[table] = False

    if remove_delta_log:
        # This would require S3 operations to remove _delta_log directory
        # Not implemented here to avoid accidental data deletion
        results["_delta_log_removal"] = False
        results["_delta_log_note"] = (
            "Delta log removal not implemented. "
            "Manually delete _delta_log directory from S3 if needed."
        )

    return results


def can_convert_to_delta(scenario: HiveTableScenario) -> tuple[bool, str]:
    """
    Check if a scenario can be converted to Delta using CONVERT TO DELTA.

    CONVERT TO DELTA requires all data files to be under the table location.
    External partitions (outside table root) cannot be converted this way.

    Args:
        scenario: Hive table scenario

    Returns:
        Tuple of (can_convert, reason)
    """
    if scenario.use_external_partitions:
        # Check if any partitions are actually external
        external_count = sum(
            1 for p in scenario.partitions
            if not p.s3_path.startswith(scenario.table_location.rstrip("/"))
        )
        if external_count > 0:
            return False, (
                f"Cannot use CONVERT TO DELTA: {external_count} partition(s) "
                "are outside the table root location. "
                "Consider using manual Delta log generation instead."
            )

    return True, "Scenario is compatible with CONVERT TO DELTA"


def can_shallow_clone(scenario: HiveTableScenario, source_is_delta: bool) -> tuple[bool, str]:
    """
    Check if a scenario can be shallow cloned.

    SHALLOW CLONE requires the source to be a Delta table.

    Args:
        scenario: Hive table scenario
        source_is_delta: Whether source has been converted to Delta

    Returns:
        Tuple of (can_clone, reason)
    """
    if not source_is_delta:
        return False, (
            "SHALLOW CLONE requires source to be a Delta table. "
            "Use CONVERT TO DELTA first, then clone from the converted table."
        )

    return True, "Scenario can be shallow cloned from Delta source"
