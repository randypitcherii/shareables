"""Unity Catalog external table registration utilities.

Provides helpers for:
- Creating UC external tables from Glue table metadata
- Getting table locations from Glue
- Comparing schemas between hive_metastore and UC tables
- Partition comparison utilities

Note: Unity Catalog requires all partitions to be contained within the
table's LOCATION directory. External partitions (outside table root)
are NOT supported in UC, unlike Glue/Hive metastore.

See: https://docs.databricks.com/aws/en/tables/external-partition-discovery
"""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple
import boto3

from .scenarios import HiveTableScenario, ALL_SCENARIOS
from .validation import get_glue_client, get_table_location, get_glue_partitions


@dataclass
class SchemaColumn:
    """Represents a table column."""
    name: str
    data_type: str
    is_partition: bool = False


@dataclass
class TableComparisonResult:
    """Result of comparing two tables."""
    hive_table: str
    uc_table: str
    schema_match: bool
    row_count_match: bool
    hive_row_count: int
    uc_row_count: int
    schema_differences: List[str]
    notes: str = ""


@dataclass
class UCRegistrationResult:
    """Result of registering a table in Unity Catalog."""
    scenario_name: str
    success: bool
    uc_table_name: str
    error: Optional[str] = None
    notes: str = ""


# Default UC catalog and schema for test tables
DEFAULT_UC_CATALOG = "migration_eval"
DEFAULT_UC_SCHEMA = "hive_migration"


def get_glue_table_schema(
    database: str,
    table: str,
    region: str = "us-east-1",
) -> Tuple[List[SchemaColumn], List[SchemaColumn]]:
    """
    Get table schema from Glue, separating data and partition columns.

    Args:
        database: Glue database name
        table: Table name
        region: AWS region

    Returns:
        Tuple of (data_columns, partition_columns)
    """
    glue_client = get_glue_client(region)

    try:
        response = glue_client.get_table(DatabaseName=database, Name=table)
        table_info = response["Table"]

        # Data columns from StorageDescriptor
        data_columns = [
            SchemaColumn(
                name=col["Name"],
                data_type=col["Type"],
                is_partition=False,
            )
            for col in table_info["StorageDescriptor"].get("Columns", [])
        ]

        # Partition columns from PartitionKeys
        partition_columns = [
            SchemaColumn(
                name=col["Name"],
                data_type=col["Type"],
                is_partition=True,
            )
            for col in table_info.get("PartitionKeys", [])
        ]

        return data_columns, partition_columns

    except Exception as e:
        raise RuntimeError(f"Failed to get schema for {database}.{table}: {e}")


def get_glue_table_location(
    database: str,
    table: str,
    region: str = "us-east-1",
) -> Optional[str]:
    """
    Get the S3 location for a Glue table.

    Args:
        database: Glue database name
        table: Table name
        region: AWS region

    Returns:
        S3 location string or None if not found
    """
    return get_table_location(get_glue_client(region), database, table)


def get_glue_partition_locations(
    database: str,
    table: str,
    region: str = "us-east-1",
) -> List[Dict[str, Any]]:
    """
    Get all partition locations for a Glue table.

    Args:
        database: Glue database name
        table: Table name
        region: AWS region

    Returns:
        List of dicts with 'values' and 'location' keys
    """
    glue_client = get_glue_client(region)
    partitions = get_glue_partitions(glue_client, database, table)

    return [
        {
            "values": p["Values"],
            "location": p["StorageDescriptor"]["Location"],
        }
        for p in partitions
    ]


def check_partitions_under_table_root(
    table_location: str,
    partition_locations: List[Dict[str, Any]],
) -> Tuple[bool, List[str]]:
    """
    Check if all partitions are under the table root location.

    Unity Catalog requires all partitions to be contained within
    the LOCATION directory. This function checks that constraint.

    Args:
        table_location: Table root S3 path
        partition_locations: List of partition location dicts

    Returns:
        Tuple of (all_under_root, list_of_external_locations)
    """
    # Normalize table location (remove trailing slash for comparison)
    root = table_location.rstrip("/")

    external_partitions = []
    for p in partition_locations:
        loc = p["location"].rstrip("/")
        if not loc.startswith(root + "/") and loc != root:
            external_partitions.append(p["location"])

    return len(external_partitions) == 0, external_partitions


def generate_uc_create_table_sql(
    scenario: HiveTableScenario,
    glue_database: str,
    uc_catalog: str = DEFAULT_UC_CATALOG,
    uc_schema: str = DEFAULT_UC_SCHEMA,
    region: str = "us-east-1",
) -> str:
    """
    Generate CREATE TABLE SQL for Unity Catalog from Glue table metadata.

    Args:
        scenario: Hive table scenario
        glue_database: Source Glue database
        uc_catalog: Target UC catalog
        uc_schema: Target UC schema
        region: AWS region for Glue

    Returns:
        SQL statement for creating UC external table
    """
    data_cols, partition_cols = get_glue_table_schema(
        glue_database, scenario.name, region
    )
    table_location = scenario.table_location

    # Build column definitions
    col_defs = []
    for col in data_cols:
        col_defs.append(f"    {col.name} {_glue_to_spark_type(col.data_type)}")

    # Build partition column definitions
    partition_defs = []
    for col in partition_cols:
        partition_defs.append(f"{col.name} {_glue_to_spark_type(col.data_type)}")

    # Build SQL
    uc_table_name = f"{uc_catalog}.{uc_schema}.{scenario.name}"

    sql = f"CREATE TABLE IF NOT EXISTS {uc_table_name} (\n"
    sql += ",\n".join(col_defs)
    sql += "\n)\nUSING PARQUET\n"

    if partition_defs:
        sql += f"PARTITIONED BY ({', '.join(partition_defs)})\n"

    sql += f"LOCATION '{table_location}'"

    return sql


def _glue_to_spark_type(glue_type: str) -> str:
    """
    Convert Glue/Hive data type to Spark SQL type.

    Args:
        glue_type: Glue data type string

    Returns:
        Spark SQL data type string
    """
    # Most types are the same, handle any special cases
    type_map = {
        "bigint": "BIGINT",
        "int": "INT",
        "integer": "INT",
        "smallint": "SMALLINT",
        "tinyint": "TINYINT",
        "double": "DOUBLE",
        "float": "FLOAT",
        "string": "STRING",
        "boolean": "BOOLEAN",
        "binary": "BINARY",
        "timestamp": "TIMESTAMP",
        "date": "DATE",
        "decimal": "DECIMAL",
    }
    return type_map.get(glue_type.lower(), glue_type.upper())


def register_uc_external_table(
    spark,
    scenario: HiveTableScenario,
    glue_database: str,
    uc_catalog: str = DEFAULT_UC_CATALOG,
    uc_schema: str = DEFAULT_UC_SCHEMA,
    region: str = "us-east-1",
    drop_if_exists: bool = True,
) -> UCRegistrationResult:
    """
    Register a Glue table as a Unity Catalog external table.

    Args:
        spark: Spark session
        scenario: Hive table scenario
        glue_database: Source Glue database
        uc_catalog: Target UC catalog
        uc_schema: Target UC schema
        region: AWS region
        drop_if_exists: Drop existing table first

    Returns:
        UCRegistrationResult with success status and details
    """
    uc_table_name = f"{uc_catalog}.{uc_schema}.{scenario.name}"

    try:
        # Check for external partitions (UC limitation)
        partition_locations = get_glue_partition_locations(
            glue_database, scenario.name, region
        )
        all_under_root, external_locs = check_partitions_under_table_root(
            scenario.table_location, partition_locations
        )

        notes = ""
        if not all_under_root:
            notes = (
                f"WARNING: {len(external_locs)} partition(s) outside table root. "
                "UC may not see all data. External locations: "
                f"{external_locs[:3]}..."
            )

        # Drop existing table if requested
        if drop_if_exists:
            spark.sql(f"DROP TABLE IF EXISTS {uc_table_name}")

        # Generate and execute CREATE TABLE
        create_sql = generate_uc_create_table_sql(
            scenario, glue_database, uc_catalog, uc_schema, region
        )
        spark.sql(create_sql)

        # Repair partitions to discover Hive-style partitions
        spark.sql(f"MSCK REPAIR TABLE {uc_table_name} SYNC PARTITIONS")

        return UCRegistrationResult(
            scenario_name=scenario.name,
            success=True,
            uc_table_name=uc_table_name,
            notes=notes,
        )

    except Exception as e:
        return UCRegistrationResult(
            scenario_name=scenario.name,
            success=False,
            uc_table_name=uc_table_name,
            error=str(e),
        )


def compare_table_schemas(
    spark,
    hive_table: str,
    uc_table: str,
) -> List[str]:
    """
    Compare schemas between hive_metastore and UC tables.

    Args:
        spark: Spark session
        hive_table: Fully-qualified hive_metastore table name
        uc_table: Fully-qualified UC table name

    Returns:
        List of schema differences (empty if schemas match)
    """
    differences = []

    try:
        hive_df = spark.sql(f"DESCRIBE TABLE {hive_table}")
        uc_df = spark.sql(f"DESCRIBE TABLE {uc_table}")

        hive_cols = {
            row.col_name: row.data_type
            for row in hive_df.collect()
            if not row.col_name.startswith("#")
        }
        uc_cols = {
            row.col_name: row.data_type
            for row in uc_df.collect()
            if not row.col_name.startswith("#")
        }

        # Check for missing columns
        for col in hive_cols:
            if col not in uc_cols:
                differences.append(f"Column '{col}' missing in UC table")

        for col in uc_cols:
            if col not in hive_cols:
                differences.append(f"Column '{col}' missing in Hive table")

        # Check for type mismatches
        for col in hive_cols:
            if col in uc_cols:
                if hive_cols[col].lower() != uc_cols[col].lower():
                    differences.append(
                        f"Type mismatch for '{col}': "
                        f"Hive={hive_cols[col]}, UC={uc_cols[col]}"
                    )

    except Exception as e:
        differences.append(f"Error comparing schemas: {e}")

    return differences


def compare_row_counts(
    spark,
    hive_table: str,
    uc_table: str,
) -> Tuple[int, int]:
    """
    Compare row counts between hive_metastore and UC tables.

    Args:
        spark: Spark session
        hive_table: Fully-qualified hive_metastore table name
        uc_table: Fully-qualified UC table name

    Returns:
        Tuple of (hive_count, uc_count)
    """
    try:
        hive_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {hive_table}").collect()[0].cnt
    except Exception:
        hive_count = -1

    try:
        uc_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {uc_table}").collect()[0].cnt
    except Exception:
        uc_count = -1

    return hive_count, uc_count


def compare_partition_counts(
    spark,
    hive_table: str,
    uc_table: str,
) -> Tuple[int, int]:
    """
    Compare partition counts between hive_metastore and UC tables.

    Args:
        spark: Spark session
        hive_table: Fully-qualified hive_metastore table name
        uc_table: Fully-qualified UC table name

    Returns:
        Tuple of (hive_partition_count, uc_partition_count)
    """
    try:
        hive_parts = spark.sql(f"SHOW PARTITIONS {hive_table}").count()
    except Exception:
        hive_parts = 0

    try:
        uc_parts = spark.sql(f"SHOW PARTITIONS {uc_table}").count()
    except Exception:
        uc_parts = 0

    return hive_parts, uc_parts


def full_table_comparison(
    spark,
    scenario: HiveTableScenario,
    glue_database: str,
    uc_catalog: str = DEFAULT_UC_CATALOG,
    uc_schema: str = DEFAULT_UC_SCHEMA,
) -> TableComparisonResult:
    """
    Perform full comparison between Hive and UC versions of a table.

    Args:
        spark: Spark session
        scenario: Hive table scenario
        glue_database: Glue database name
        uc_catalog: UC catalog name
        uc_schema: UC schema name

    Returns:
        TableComparisonResult with comparison details
    """
    hive_table = f"hive_metastore.{glue_database}.{scenario.name}"
    uc_table = f"{uc_catalog}.{uc_schema}.{scenario.name}"

    # Compare schemas
    schema_diffs = compare_table_schemas(spark, hive_table, uc_table)

    # Compare row counts
    hive_count, uc_count = compare_row_counts(spark, hive_table, uc_table)

    notes = []
    if scenario.use_external_partitions:
        notes.append(
            "This scenario uses external partitions which UC may not support"
        )
    if scenario.recursive_scan_required:
        notes.append(
            "This scenario requires recursive scanning"
        )

    return TableComparisonResult(
        hive_table=hive_table,
        uc_table=uc_table,
        schema_match=len(schema_diffs) == 0,
        row_count_match=hive_count == uc_count,
        hive_row_count=hive_count,
        uc_row_count=uc_count,
        schema_differences=schema_diffs,
        notes="; ".join(notes) if notes else "",
    )


def ensure_uc_catalog_and_schema(
    spark,
    catalog: str = DEFAULT_UC_CATALOG,
    schema: str = DEFAULT_UC_SCHEMA,
) -> Tuple[bool, str]:
    """
    Ensure UC catalog and schema exist for test tables.

    Note: This may require elevated permissions.

    Args:
        spark: Spark session
        catalog: UC catalog name
        schema: UC schema name

    Returns:
        Tuple of (success, message)
    """
    try:
        # Check/create catalog
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

        # Check/create schema
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

        return True, f"Catalog {catalog} and schema {schema} are ready"

    except Exception as e:
        return False, f"Failed to create catalog/schema: {e}"


def scenario_supports_uc(scenario: HiveTableScenario) -> Tuple[bool, str]:
    """
    Check if a scenario is compatible with UC external table requirements.

    Unity Catalog requires all partitions to be under the table root location.

    Args:
        scenario: Hive table scenario

    Returns:
        Tuple of (is_supported, reason)
    """
    if scenario.use_external_partitions:
        # Check if partitions are actually external
        external_count = sum(
            1 for p in scenario.partitions
            if not p.s3_path.startswith(scenario.table_location.rstrip("/"))
        )
        if external_count > 0:
            return False, (
                f"UC does not support external partitions. "
                f"{external_count} partition(s) are outside table root."
            )

    return True, "Scenario is compatible with UC"
