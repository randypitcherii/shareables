"""Reusable Hive-to-Delta conversion with Unity Catalog registration.

This module provides a clean, reusable function for converting Hive tables
(backed by Parquet in AWS Glue) to Delta tables registered in Unity Catalog.

Key Features:
- Uses Manual Delta Log approach (not CONVERT TO DELTA)
- Handles partitioned and non-partitioned tables
- Supports external partitions (files outside table root)
- Writes absolute S3 paths in Delta transaction log
- Registers tables in Unity Catalog

Usage:
    from hive_table_experiments.delta_converter import (
        convert_hive_to_delta_uc,
        validate_delta_table,
    )

    result = convert_hive_to_delta_uc(
        spark=spark,
        glue_database="my_glue_db",
        table_name="my_table",
        uc_catalog="my_catalog",
        uc_schema="my_schema",
    )

    if result.success:
        validation = validate_delta_table(spark, result.target_table)
        print(f"Row count: {validation.row_count}")
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .validation import (
    get_glue_client,
    get_s3_client,
    get_glue_partitions,
    get_table_location,
    parse_s3_path,
)


@dataclass
class ConversionResult:
    """Result of a Hive-to-Delta conversion operation."""

    success: bool
    source_table: str
    target_table: str
    target_location: str
    error: Optional[str] = None
    notes: List[str] = field(default_factory=list)
    file_count: int = 0
    partition_columns: List[str] = field(default_factory=list)
    delta_log_path: Optional[str] = None

    def __repr__(self) -> str:
        status = "SUCCESS" if self.success else "FAILED"
        return f"ConversionResult({self.source_table} -> {self.target_table}, {status})"


@dataclass
class ValidationResult:
    """Result of validating a Delta table."""

    success: bool
    table_name: str
    row_count: int = -1
    partition_count: int = 0
    partition_columns: List[str] = field(default_factory=list)
    error: Optional[str] = None
    sample_data: Optional[List[Dict[str, Any]]] = None

    def __repr__(self) -> str:
        status = "VALID" if self.success else "INVALID"
        return f"ValidationResult({self.table_name}, {status}, rows={self.row_count})"


@dataclass
class ParquetFileInfo:
    """Information about a Parquet file for Delta log entry."""

    path: str  # Absolute S3 path (e.g., s3://bucket/path/file.parquet)
    size_bytes: int
    modification_time: int  # Unix timestamp in milliseconds
    partition_values: Dict[str, str] = field(default_factory=dict)


@dataclass
class GlueTableMetadata:
    """Metadata extracted from a Glue table."""

    database: str
    table_name: str
    location: str
    data_columns: List[Dict[str, str]]  # [{"name": "col", "type": "string"}, ...]
    partition_columns: List[Dict[str, str]]
    partitions: List[Dict[str, Any]]  # From get_partitions


def _glue_type_to_delta_type(glue_type: str) -> str:
    """
    Convert Glue/Hive data type to Delta schema type.

    Args:
        glue_type: Glue data type string (e.g., "string", "bigint")

    Returns:
        Delta schema type string
    """
    type_mapping = {
        "string": "string",
        "bigint": "long",
        "int": "integer",
        "integer": "integer",
        "smallint": "short",
        "tinyint": "byte",
        "double": "double",
        "float": "float",
        "boolean": "boolean",
        "timestamp": "timestamp",
        "date": "date",
        "binary": "binary",
        "decimal": "decimal(38,18)",
    }
    return type_mapping.get(glue_type.lower(), glue_type.lower())


def _glue_type_to_spark_sql(glue_type: str) -> str:
    """
    Convert Glue/Hive data type to Spark SQL type.

    Args:
        glue_type: Glue data type string

    Returns:
        Spark SQL type string (for CREATE TABLE)
    """
    type_mapping = {
        "string": "STRING",
        "bigint": "BIGINT",
        "int": "INT",
        "integer": "INT",
        "smallint": "SMALLINT",
        "tinyint": "TINYINT",
        "double": "DOUBLE",
        "float": "FLOAT",
        "boolean": "BOOLEAN",
        "timestamp": "TIMESTAMP",
        "date": "DATE",
        "binary": "BINARY",
        "decimal": "DECIMAL(38,18)",
    }
    return type_mapping.get(glue_type.lower(), glue_type.upper())


def get_glue_table_metadata(
    glue_database: str,
    table_name: str,
    region: str = "us-east-1",
) -> GlueTableMetadata:
    """
    Extract metadata from a Glue table.

    Args:
        glue_database: AWS Glue database name
        table_name: Table name in Glue
        region: AWS region

    Returns:
        GlueTableMetadata with schema and partition information

    Raises:
        RuntimeError: If table not found or Glue API fails
    """
    glue_client = get_glue_client(region)

    try:
        response = glue_client.get_table(DatabaseName=glue_database, Name=table_name)
        table_info = response["Table"]

        # Extract data columns
        data_columns = [
            {"name": col["Name"], "type": col["Type"]}
            for col in table_info["StorageDescriptor"].get("Columns", [])
        ]

        # Extract partition columns
        partition_columns = [
            {"name": col["Name"], "type": col["Type"]}
            for col in table_info.get("PartitionKeys", [])
        ]

        # Get table location
        location = table_info["StorageDescriptor"]["Location"]

        # Get partitions
        partitions = get_glue_partitions(glue_client, glue_database, table_name)

        return GlueTableMetadata(
            database=glue_database,
            table_name=table_name,
            location=location,
            data_columns=data_columns,
            partition_columns=partition_columns,
            partitions=partitions,
        )

    except Exception as e:
        raise RuntimeError(
            f"Failed to get metadata for {glue_database}.{table_name}: {e}"
        )


def scan_partition_files(
    metadata: GlueTableMetadata,
    region: str = "us-east-1",
) -> List[ParquetFileInfo]:
    """
    Scan all partition locations for Parquet files.

    For partitioned tables, scans each partition location from Glue.
    Uses ABSOLUTE S3 paths, which is required for external partitions.

    Args:
        metadata: Glue table metadata with partition info
        region: AWS region

    Returns:
        List of ParquetFileInfo with absolute S3 paths
    """
    s3_client = get_s3_client(region)
    files = []
    partition_col_names = [col["name"] for col in metadata.partition_columns]

    if not metadata.partitions:
        # Non-partitioned table: scan the table location directly
        return scan_location_for_parquet(
            s3_client,
            metadata.location,
            partition_values={},
        )

    # Partitioned table: scan each partition location
    for partition in metadata.partitions:
        partition_location = partition["StorageDescriptor"]["Location"]
        partition_values = dict(zip(partition_col_names, partition["Values"]))

        partition_files = scan_location_for_parquet(
            s3_client,
            partition_location,
            partition_values,
        )
        files.extend(partition_files)

    return files


def scan_location_for_parquet(
    s3_client,
    s3_location: str,
    partition_values: Dict[str, str],
) -> List[ParquetFileInfo]:
    """
    Scan a single S3 location for Parquet files.

    Args:
        s3_client: Boto3 S3 client
        s3_location: S3 path to scan
        partition_values: Partition values for these files

    Returns:
        List of ParquetFileInfo with absolute S3 paths
    """
    files = []
    s3_location = s3_location.rstrip("/")
    bucket, prefix = parse_s3_path(s3_location)

    # Ensure prefix ends with / for listing
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]

                # Only include Parquet files
                if not key.endswith(".parquet"):
                    continue

                # Use absolute S3 path
                absolute_path = f"s3://{bucket}/{key}"

                files.append(
                    ParquetFileInfo(
                        path=absolute_path,
                        size_bytes=obj["Size"],
                        modification_time=int(obj["LastModified"].timestamp() * 1000),
                        partition_values=partition_values.copy(),
                    )
                )
    except Exception as e:
        print(f"Warning: Could not scan {s3_location}: {e}")

    return files


def build_delta_schema(metadata: GlueTableMetadata) -> Dict[str, Any]:
    """
    Build Delta Lake schema JSON from Glue metadata.

    Args:
        metadata: Glue table metadata

    Returns:
        Delta schema dictionary for metaData.schemaString
    """
    fields = []

    # Add data columns
    for col in metadata.data_columns:
        fields.append(
            {
                "name": col["name"],
                "type": _glue_type_to_delta_type(col["type"]),
                "nullable": True,
                "metadata": {},
            }
        )

    # Add partition columns
    for col in metadata.partition_columns:
        fields.append(
            {
                "name": col["name"],
                "type": _glue_type_to_delta_type(col["type"]),
                "nullable": True,
                "metadata": {},
            }
        )

    return {"type": "struct", "fields": fields}


def generate_delta_log_json(
    files: List[ParquetFileInfo],
    schema: Dict[str, Any],
    partition_columns: List[str],
    table_id: Optional[str] = None,
) -> str:
    """
    Generate Delta transaction log JSON content.

    Creates the initial commit (version 0) with:
    - Protocol action (reader v1, writer v2)
    - Metadata action (schema, partition columns, table ID)
    - Add actions for each Parquet file

    Args:
        files: List of Parquet files to include
        schema: Delta schema dictionary
        partition_columns: List of partition column names
        table_id: Unique table ID (generated if not provided)

    Returns:
        JSON string for 00000000000000000000.json (newline-delimited)
    """
    if table_id is None:
        table_id = str(uuid.uuid4())

    timestamp = int(datetime.now().timestamp() * 1000)

    entries = []

    # Protocol action
    entries.append(
        json.dumps(
            {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
            separators=(",", ":"),
        )
    )

    # Metadata action
    metadata_action = {
        "metaData": {
            "id": table_id,
            "format": {"provider": "parquet", "options": {}},
            "schemaString": json.dumps(schema, separators=(",", ":")),
            "partitionColumns": partition_columns,
            "configuration": {},
            "createdTime": timestamp,
        }
    }
    entries.append(json.dumps(metadata_action, separators=(",", ":")))

    # Add file actions (one per file)
    for file_info in files:
        add_action = {
            "add": {
                "path": file_info.path,
                "partitionValues": file_info.partition_values,
                "size": file_info.size_bytes,
                "modificationTime": file_info.modification_time,
                "dataChange": True,
            }
        }
        entries.append(json.dumps(add_action, separators=(",", ":")))

    # Delta log format: one JSON object per line
    return "\n".join(entries)


def write_delta_log_to_s3(
    delta_log_json: str,
    delta_table_location: str,
    region: str = "us-east-1",
) -> str:
    """
    Write Delta transaction log to S3.

    Creates _delta_log/00000000000000000000.json at the specified location.

    Args:
        delta_log_json: JSON content for the log file
        delta_table_location: S3 location for the Delta table root
        region: AWS region

    Returns:
        S3 path where log was written
    """
    s3_client = get_s3_client(region)

    delta_table_location = delta_table_location.rstrip("/")
    bucket, prefix = parse_s3_path(delta_table_location)

    log_key = f"{prefix}/_delta_log/00000000000000000000.json"

    s3_client.put_object(
        Bucket=bucket,
        Key=log_key,
        Body=delta_log_json.encode("utf-8"),
        ContentType="application/json",
    )

    return f"s3://{bucket}/{log_key}"


def register_delta_table_in_uc(
    spark,
    target_table: str,
    delta_location: str,
    schema: Dict[str, Any],
    partition_columns: List[str],
) -> None:
    """
    Register an external Delta table in Unity Catalog.

    Args:
        spark: Spark session
        target_table: Fully-qualified table name (catalog.schema.table)
        delta_location: S3 location of the Delta table
        schema: Delta schema dictionary
        partition_columns: List of partition column names

    Raises:
        Exception: If table creation fails
    """
    # Build column definitions (excluding partition columns)
    col_defs = []
    for field in schema["fields"]:
        if field["name"] not in partition_columns:
            sql_type = _delta_type_to_spark_sql(field["type"])
            col_defs.append(f"{field['name']} {sql_type}")

    # Build CREATE TABLE SQL
    create_sql = f"CREATE TABLE IF NOT EXISTS {target_table} (\n"
    create_sql += ",\n".join(f"  {col}" for col in col_defs)
    create_sql += "\n)\nUSING DELTA"

    if partition_columns:
        # Get partition column types from schema
        part_defs = []
        for pc in partition_columns:
            for field in schema["fields"]:
                if field["name"] == pc:
                    sql_type = _delta_type_to_spark_sql(field["type"])
                    part_defs.append(f"{pc} {sql_type}")
                    break
        create_sql += f"\nPARTITIONED BY ({', '.join(part_defs)})"

    create_sql += f"\nLOCATION '{delta_location}'"

    # Drop existing table and create new
    spark.sql(f"DROP TABLE IF EXISTS {target_table}")
    spark.sql(create_sql)


def _delta_type_to_spark_sql(delta_type: str) -> str:
    """Convert Delta schema type to Spark SQL type."""
    type_mapping = {
        "string": "STRING",
        "long": "BIGINT",
        "integer": "INT",
        "short": "SMALLINT",
        "byte": "TINYINT",
        "double": "DOUBLE",
        "float": "FLOAT",
        "boolean": "BOOLEAN",
        "timestamp": "TIMESTAMP",
        "date": "DATE",
        "binary": "BINARY",
    }
    # Handle decimal with precision
    if delta_type.startswith("decimal"):
        return delta_type.upper()
    return type_mapping.get(delta_type.lower(), delta_type.upper())


def convert_hive_to_delta_uc(
    spark,
    glue_database: str,
    table_name: str,
    uc_catalog: str,
    uc_schema: str,
    delta_location: Optional[str] = None,
    region: str = "us-east-1",
) -> ConversionResult:
    """
    Convert a Hive table (Glue) to a Delta table registered in Unity Catalog.

    This function:
    1. Reads Glue table metadata (schema, partitions, locations)
    2. Scans all partition locations for Parquet files
    3. Creates a Delta transaction log with ABSOLUTE S3 paths
    4. Writes the log to S3 at _delta_log/00000000000000000000.json
    5. Registers the table as an external Delta table in Unity Catalog

    Args:
        spark: Active Spark session with Unity Catalog access
        glue_database: AWS Glue database name containing the source table
        table_name: Name of the source table in Glue
        uc_catalog: Unity Catalog catalog for the target table
        uc_schema: Unity Catalog schema for the target table
        delta_location: S3 location for Delta table (defaults to Glue table location)
        region: AWS region for Glue/S3 operations

    Returns:
        ConversionResult with success/failure details

    Example:
        >>> result = convert_hive_to_delta_uc(
        ...     spark=spark,
        ...     glue_database="my_glue_db",
        ...     table_name="events",
        ...     uc_catalog="analytics",
        ...     uc_schema="delta_tables",
        ... )
        >>> if result.success:
        ...     print(f"Converted {result.file_count} files")
        ...     print(f"Delta table: {result.target_table}")
    """
    source_table = f"hive_metastore.{glue_database}.{table_name}"
    target_table = f"{uc_catalog}.{uc_schema}.{table_name}"
    notes = []

    try:
        # Step 1: Get Glue table metadata
        notes.append("Reading Glue table metadata...")
        metadata = get_glue_table_metadata(glue_database, table_name, region)

        # Use provided location or default to Glue table location
        target_location = delta_location or metadata.location.rstrip("/")
        partition_col_names = [col["name"] for col in metadata.partition_columns]

        notes.append(
            f"Found {len(metadata.data_columns)} data columns, "
            f"{len(metadata.partition_columns)} partition columns"
        )

        # Step 2: Scan for Parquet files
        notes.append("Scanning partition locations for Parquet files...")
        files = scan_partition_files(metadata, region)

        if not files:
            return ConversionResult(
                success=False,
                source_table=source_table,
                target_table=target_table,
                target_location=target_location,
                error="No Parquet files found in table or partitions",
                notes=notes,
            )

        notes.append(f"Found {len(files)} Parquet files across all partitions")

        # Step 3: Build Delta schema
        schema = build_delta_schema(metadata)

        # Step 4: Generate Delta transaction log
        notes.append("Generating Delta transaction log with absolute S3 paths...")
        delta_log_json = generate_delta_log_json(
            files=files,
            schema=schema,
            partition_columns=partition_col_names,
        )

        # Step 5: Write Delta log to S3
        notes.append(f"Writing _delta_log to {target_location}...")
        log_path = write_delta_log_to_s3(delta_log_json, target_location, region)
        notes.append(f"Delta log written to {log_path}")

        # Step 6: Ensure UC schema exists
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {uc_catalog}.{uc_schema}")

        # Step 7: Register in Unity Catalog
        notes.append(f"Registering table in Unity Catalog as {target_table}...")
        register_delta_table_in_uc(
            spark=spark,
            target_table=target_table,
            delta_location=target_location,
            schema=schema,
            partition_columns=partition_col_names,
        )
        notes.append("Table registered successfully")

        return ConversionResult(
            success=True,
            source_table=source_table,
            target_table=target_table,
            target_location=target_location,
            notes=notes,
            file_count=len(files),
            partition_columns=partition_col_names,
            delta_log_path=log_path,
        )

    except Exception as e:
        notes.append(f"ERROR: {str(e)}")
        return ConversionResult(
            success=False,
            source_table=source_table,
            target_table=target_table,
            target_location=delta_location or "",
            error=str(e),
            notes=notes,
        )


def validate_delta_table(
    spark,
    table_name: str,
    sample_rows: int = 5,
) -> ValidationResult:
    """
    Validate that a Delta table is readable and return basic statistics.

    Performs the following checks:
    - Table exists and is queryable
    - Row count
    - Partition information
    - Sample data retrieval

    Args:
        spark: Spark session
        table_name: Fully-qualified table name (catalog.schema.table)
        sample_rows: Number of sample rows to retrieve (0 to skip)

    Returns:
        ValidationResult with row count, partition info, and optional sample data

    Example:
        >>> result = validate_delta_table(spark, "analytics.delta.events")
        >>> if result.success:
        ...     print(f"Table has {result.row_count} rows")
        ...     print(f"Partitioned by: {result.partition_columns}")
    """
    try:
        # Get row count
        row_count = spark.sql(
            f"SELECT COUNT(*) as cnt FROM {table_name}"
        ).collect()[0].cnt

        # Get partition info from DESCRIBE
        partition_columns = []
        partition_count = 0

        try:
            describe_df = spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}")
            describe_rows = describe_df.collect()

            # Find partition columns in describe output
            in_partition_section = False
            for row in describe_rows:
                if row.col_name == "# Partition Information":
                    in_partition_section = True
                    continue
                if in_partition_section and row.col_name and not row.col_name.startswith("#"):
                    partition_columns.append(row.col_name)
                if row.col_name == "# Detailed Table Information":
                    in_partition_section = False
        except Exception:
            pass

        # Get partition count if partitioned
        if partition_columns:
            try:
                partition_count = spark.sql(
                    f"SHOW PARTITIONS {table_name}"
                ).count()
            except Exception:
                pass

        # Get sample data
        sample_data = None
        if sample_rows > 0:
            try:
                sample_df = spark.sql(f"SELECT * FROM {table_name} LIMIT {sample_rows}")
                sample_data = [row.asDict() for row in sample_df.collect()]
            except Exception:
                pass

        return ValidationResult(
            success=True,
            table_name=table_name,
            row_count=row_count,
            partition_count=partition_count,
            partition_columns=partition_columns,
            sample_data=sample_data,
        )

    except Exception as e:
        return ValidationResult(
            success=False,
            table_name=table_name,
            error=str(e),
        )


def cleanup_delta_log(
    s3_location: str,
    region: str = "us-east-1",
) -> bool:
    """
    Remove _delta_log directory from S3, reverting to plain Parquet.

    WARNING: This removes the Delta log, making the location no longer
    a valid Delta table. Use with caution.

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
        paginator = s3_client.get_paginator("list_objects_v2")
        objects_to_delete = []

        for page in paginator.paginate(Bucket=bucket, Prefix=delta_log_prefix):
            for obj in page.get("Contents", []):
                objects_to_delete.append({"Key": obj["Key"]})

        if objects_to_delete:
            # Delete in batches of 1000 (S3 limit)
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i : i + 1000]
                s3_client.delete_objects(Bucket=bucket, Delete={"Objects": batch})

        return True

    except Exception as e:
        print(f"Failed to cleanup Delta log: {e}")
        return False
