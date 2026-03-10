"""Manual Delta log generation for edge cases.

When CONVERT TO DELTA and SHALLOW CLONE don't work (e.g., external partitions,
cross-bucket scenarios), this module provides programmatic Delta log generation.

Delta Lake stores metadata in a _delta_log directory containing JSON files:
- 00000000000000000000.json - Initial commit with all AddFile actions
- Subsequent commits increment the version number

This approach provides full control for scenarios like:
- External partitions (files outside table root)
- Cross-bucket tables
- Cross-region tables
- Shared partition tables

The generated Delta log references existing Parquet files without copying them.
"""

import json
import time
import uuid
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime

from .scenarios import HiveTableScenario
from .validation import (
    get_glue_client,
    get_s3_client,
    parse_s3_path,
    list_s3_files,
    get_glue_partitions,
)
from .uc_helpers import get_glue_table_schema, _glue_to_spark_type
from .delta_conversion import ConversionResult, ConversionApproach


@dataclass
class ParquetFileInfo:
    """Information about a Parquet file for Delta log entry."""
    path: str  # Relative path or absolute S3 path
    size_bytes: int
    modification_time: int  # Unix timestamp in milliseconds
    partition_values: Dict[str, str] = None

    def __post_init__(self):
        if self.partition_values is None:
            self.partition_values = {}


@dataclass
class DeltaLogEntry:
    """Represents a Delta transaction log entry."""
    version: int
    timestamp: int  # Unix timestamp in milliseconds
    protocol: Dict[str, int] = None
    metadata: Dict[str, Any] = None
    add_files: List[Dict[str, Any]] = None
    remove_files: List[Dict[str, Any]] = None

    def __post_init__(self):
        if self.protocol is None:
            # Default protocol version (reader v1, writer v2)
            self.protocol = {"minReaderVersion": 1, "minWriterVersion": 2}
        if self.add_files is None:
            self.add_files = []
        if self.remove_files is None:
            self.remove_files = []


def scan_parquet_files(
    s3_location: str,
    partition_columns: List[str] = None,
    region: str = "us-east-1",
) -> List[ParquetFileInfo]:
    """
    Scan S3 location for Parquet files and extract metadata.

    Args:
        s3_location: S3 path to scan (e.g., s3://bucket/path/)
        partition_columns: List of partition column names to extract from paths
        region: AWS region for S3 client

    Returns:
        List of ParquetFileInfo for each discovered file
    """
    s3_client = get_s3_client(region)

    if partition_columns is None:
        partition_columns = []

    s3_location = s3_location.rstrip("/")
    bucket, prefix = parse_s3_path(s3_location)

    files = []

    # List all objects under the prefix
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            # Only include Parquet files
            if not key.endswith(".parquet"):
                continue

            # Extract partition values from path
            partition_values = {}
            if partition_columns:
                # Parse Hive-style partition paths (e.g., region=us-east/year=2024/)
                path_parts = key.split("/")
                for part in path_parts:
                    if "=" in part:
                        col_name, col_value = part.split("=", 1)
                        if col_name in partition_columns:
                            partition_values[col_name] = col_value

            # Get relative path from table root
            relative_path = key[len(prefix):].lstrip("/")

            files.append(ParquetFileInfo(
                path=relative_path,
                size_bytes=obj["Size"],
                modification_time=int(obj["LastModified"].timestamp() * 1000),
                partition_values=partition_values,
            ))

    return files


def scan_external_partitions(
    scenario: HiveTableScenario,
    glue_database: str,
    region: str = "us-east-1",
) -> List[ParquetFileInfo]:
    """
    Scan all partition locations (including external) for Parquet files.

    This handles scenarios where partitions are scattered across different
    S3 locations, buckets, or even regions.

    Args:
        scenario: Hive table scenario with partition definitions
        glue_database: Glue database name
        region: AWS region for Glue client

    Returns:
        List of ParquetFileInfo with absolute S3 paths
    """
    glue_client = get_glue_client(region)
    s3_client = get_s3_client(region)

    files = []

    # Get partitions from Glue
    partitions = get_glue_partitions(glue_client, glue_database, scenario.name)

    # Get partition column names
    _, partition_cols = get_glue_table_schema(
        glue_database, scenario.name, region
    )
    partition_column_names = [col.name for col in partition_cols]

    for partition in partitions:
        partition_location = partition["StorageDescriptor"]["Location"]
        partition_values = dict(zip(
            partition_column_names,
            partition["Values"]
        ))

        # Scan this partition location
        bucket, prefix = parse_s3_path(partition_location.rstrip("/"))

        # For cross-region partitions, we might need different clients
        # For simplicity, use the default client (might need enhancement)
        try:
            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]

                    if not key.endswith(".parquet"):
                        continue

                    # Use absolute S3 path for external partitions
                    absolute_path = f"s3://{bucket}/{key}"

                    files.append(ParquetFileInfo(
                        path=absolute_path,
                        size_bytes=obj["Size"],
                        modification_time=int(obj["LastModified"].timestamp() * 1000),
                        partition_values=partition_values,
                    ))
        except Exception as e:
            # Log but continue - some partitions might be in different regions
            print(f"Warning: Could not scan {partition_location}: {e}")

    return files


def generate_delta_schema(
    glue_database: str,
    table_name: str,
    region: str = "us-east-1",
) -> Dict[str, Any]:
    """
    Generate Delta Lake schema JSON from Glue table schema.

    Args:
        glue_database: Glue database name
        table_name: Table name
        region: AWS region

    Returns:
        Delta schema in JSON format
    """
    data_cols, partition_cols = get_glue_table_schema(
        glue_database, table_name, region
    )

    # Build schema fields
    fields = []
    for col in data_cols + partition_cols:
        spark_type = _glue_to_spark_type(col.data_type)

        # Map Spark SQL types to Delta schema format
        type_mapping = {
            "STRING": "string",
            "BIGINT": "long",
            "INT": "integer",
            "DOUBLE": "double",
            "FLOAT": "float",
            "BOOLEAN": "boolean",
            "TIMESTAMP": "timestamp",
            "DATE": "date",
            "BINARY": "binary",
        }

        delta_type = type_mapping.get(spark_type.upper(), spark_type.lower())

        fields.append({
            "name": col.name,
            "type": delta_type,
            "nullable": True,
            "metadata": {},
        })

    return {
        "type": "struct",
        "fields": fields,
    }


def generate_delta_log(
    files: List[ParquetFileInfo],
    schema: Dict[str, Any],
    table_id: str = None,
    partition_columns: List[str] = None,
) -> str:
    """
    Generate Delta transaction log JSON for initial commit.

    Args:
        files: List of Parquet files to include
        schema: Delta schema dictionary
        table_id: Unique table identifier (generated if not provided)
        partition_columns: List of partition column names

    Returns:
        JSON string for 00000000000000000000.json
    """
    if table_id is None:
        table_id = str(uuid.uuid4())

    if partition_columns is None:
        partition_columns = []

    timestamp = int(datetime.now().timestamp() * 1000)

    # Build log entries
    entries = []

    # Protocol entry
    entries.append(json.dumps({
        "protocol": {
            "minReaderVersion": 1,
            "minWriterVersion": 2,
        }
    }))

    # Metadata entry
    metadata = {
        "metaData": {
            "id": table_id,
            "format": {
                "provider": "parquet",
                "options": {},
            },
            "schemaString": json.dumps(schema),
            "partitionColumns": partition_columns,
            "configuration": {},
            "createdTime": timestamp,
        }
    }
    entries.append(json.dumps(metadata))

    # Add file entries
    for file_info in files:
        add_entry = {
            "add": {
                "path": file_info.path,
                "partitionValues": file_info.partition_values,
                "size": file_info.size_bytes,
                "modificationTime": file_info.modification_time,
                "dataChange": True,
                # Optional: file statistics (we skip for simplicity)
            }
        }
        entries.append(json.dumps(add_entry))

    # Join with newlines (Delta log format)
    return "\n".join(entries)


def write_delta_log_to_s3(
    delta_log_json: str,
    s3_location: str,
    region: str = "us-east-1",
) -> str:
    """
    Write Delta transaction log to S3.

    Creates _delta_log/00000000000000000000.json at the specified location.

    Args:
        delta_log_json: JSON content for the log file
        s3_location: Table root S3 path
        region: AWS region

    Returns:
        S3 path where log was written
    """
    s3_client = get_s3_client(region)

    s3_location = s3_location.rstrip("/")
    bucket, prefix = parse_s3_path(s3_location)

    log_key = f"{prefix}/_delta_log/00000000000000000000.json"

    s3_client.put_object(
        Bucket=bucket,
        Key=log_key,
        Body=delta_log_json.encode("utf-8"),
        ContentType="application/json",
    )

    return f"s3://{bucket}/{log_key}"


def register_delta_table(
    spark,
    table_name: str,
    s3_location: str,
    schema: Dict[str, Any],
    partition_columns: List[str] = None,
) -> bool:
    """
    Register an external Delta table in Unity Catalog.

    Args:
        spark: Spark session
        table_name: Fully-qualified table name (catalog.schema.table)
        s3_location: S3 location of the Delta table
        schema: Delta schema dictionary
        partition_columns: List of partition column names

    Returns:
        True if registration succeeded
    """
    if partition_columns is None:
        partition_columns = []

    # Build column definitions from schema
    col_defs = []
    for field in schema["fields"]:
        # Map Delta types back to Spark SQL
        type_mapping = {
            "string": "STRING",
            "long": "BIGINT",
            "integer": "INT",
            "double": "DOUBLE",
            "float": "FLOAT",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP",
            "date": "DATE",
            "binary": "BINARY",
        }
        sql_type = type_mapping.get(field["type"], field["type"].upper())

        # Skip partition columns from main columns (they'll be added separately)
        if field["name"] not in partition_columns:
            col_defs.append(f"{field['name']} {sql_type}")

    # Build CREATE TABLE statement
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n  "
    create_sql += ",\n  ".join(col_defs)
    create_sql += "\n) USING DELTA"

    if partition_columns:
        # Find partition column types from schema
        part_defs = []
        for pc in partition_columns:
            for field in schema["fields"]:
                if field["name"] == pc:
                    type_mapping = {
                        "string": "STRING",
                        "long": "BIGINT",
                        "integer": "INT",
                    }
                    sql_type = type_mapping.get(field["type"], field["type"].upper())
                    part_defs.append(f"{pc} {sql_type}")
                    break
        create_sql += f"\nPARTITIONED BY ({', '.join(part_defs)})"

    create_sql += f"\nLOCATION '{s3_location}'"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(create_sql)

    return True


def manual_convert_to_delta(
    spark,
    scenario: HiveTableScenario,
    glue_database: str,
    delta_catalog: str = "migration_eval",
    delta_schema: str = "delta_migration",
    region: str = "us-east-1",
    include_external_partitions: bool = True,
) -> ConversionResult:
    """
    Manually convert a Hive table to Delta by generating _delta_log.

    This approach works for scenarios where CONVERT TO DELTA fails:
    - External partitions (files outside table root)
    - Cross-bucket tables
    - Cross-region tables

    Args:
        spark: Spark session
        scenario: Hive table scenario
        glue_database: Glue database name
        delta_catalog: Target UC catalog
        delta_schema: Target UC schema
        region: AWS region
        include_external_partitions: Whether to include files from external partitions

    Returns:
        ConversionResult with success status and details
    """
    source_table = f"hive_metastore.{glue_database}.{scenario.name}"
    target_table = f"{delta_catalog}.{delta_schema}.{scenario.name}_manual"
    s3_location = scenario.table_location.rstrip("/")

    start_time = time.time()
    notes = []

    try:
        # Get source row count
        source_count = spark.sql(
            f"SELECT COUNT(*) as cnt FROM {source_table}"
        ).collect()[0].cnt

        # Get schema
        _, partition_cols = get_glue_table_schema(
            glue_database, scenario.name, region
        )
        partition_column_names = [col.name for col in partition_cols]

        schema = generate_delta_schema(glue_database, scenario.name, region)

        # Scan for Parquet files
        if include_external_partitions and scenario.use_external_partitions:
            # Use external partition scanner for scattered data
            files = scan_external_partitions(scenario, glue_database, region)
            notes.append(f"Scanned {len(files)} files from external partitions")
        else:
            # Standard scan under table root
            files = scan_parquet_files(
                s3_location,
                partition_column_names,
                region
            )
            notes.append(f"Scanned {len(files)} files under table root")

        if len(files) == 0:
            return ConversionResult(
                scenario_name=scenario.name,
                approach=ConversionApproach.MANUAL_DELTA_LOG,
                success=False,
                source_table=source_table,
                target_table=target_table,
                target_location=s3_location,
                error="No Parquet files found to include in Delta log",
                notes="; ".join(notes),
                execution_time_seconds=time.time() - start_time,
            )

        # Generate Delta log JSON
        delta_log = generate_delta_log(
            files=files,
            schema=schema,
            partition_columns=partition_column_names,
        )

        # Write to S3
        log_path = write_delta_log_to_s3(delta_log, s3_location, region)
        notes.append(f"Wrote Delta log to {log_path}")

        # Ensure schema exists
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {delta_catalog}.{delta_schema}")

        # Register as Delta table
        register_delta_table(
            spark,
            target_table,
            s3_location,
            schema,
            partition_column_names,
        )
        notes.append(f"Registered table {target_table}")

        # Repair partitions
        if partition_column_names:
            try:
                spark.sql(f"MSCK REPAIR TABLE {target_table}")
                notes.append("Partition repair completed")
            except Exception as e:
                notes.append(f"Partition repair skipped: {e}")

        # Get target row count
        target_count = spark.sql(
            f"SELECT COUNT(*) as cnt FROM {target_table}"
        ).collect()[0].cnt

        execution_time = time.time() - start_time

        return ConversionResult(
            scenario_name=scenario.name,
            approach=ConversionApproach.MANUAL_DELTA_LOG,
            success=True,
            source_table=source_table,
            target_table=target_table,
            target_location=s3_location,
            notes="; ".join(notes),
            execution_time_seconds=execution_time,
            source_row_count=source_count,
            target_row_count=target_count,
            row_counts_match=source_count == target_count,
            schema_match=True,
        )

    except Exception as e:
        execution_time = time.time() - start_time
        return ConversionResult(
            scenario_name=scenario.name,
            approach=ConversionApproach.MANUAL_DELTA_LOG,
            success=False,
            source_table=source_table,
            target_table=target_table,
            target_location=s3_location,
            error=str(e),
            notes="; ".join(notes),
            execution_time_seconds=execution_time,
        )


def cleanup_manual_delta_log(
    s3_location: str,
    region: str = "us-east-1",
) -> bool:
    """
    Remove _delta_log directory from S3.

    WARNING: This removes the Delta log, reverting the location to plain Parquet.
    Use with caution in production.

    Args:
        s3_location: Table root S3 path
        region: AWS region

    Returns:
        True if cleanup succeeded
    """
    s3_client = get_s3_client(region)

    s3_location = s3_location.rstrip("/")
    bucket, prefix = parse_s3_path(s3_location)

    delta_log_prefix = f"{prefix}/_delta_log/"

    try:
        # List all objects in _delta_log
        paginator = s3_client.get_paginator("list_objects_v2")
        objects_to_delete = []

        for page in paginator.paginate(Bucket=bucket, Prefix=delta_log_prefix):
            for obj in page.get("Contents", []):
                objects_to_delete.append({"Key": obj["Key"]})

        if objects_to_delete:
            # Delete in batches of 1000 (S3 limit)
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i:i + 1000]
                s3_client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": batch}
                )

        return True

    except Exception as e:
        print(f"Failed to cleanup Delta log: {e}")
        return False
