"""Delta log generation for Hive to Delta table conversion.

This module generates Delta transaction logs to register existing Parquet files
as Delta tables without copying data. Delta Lake stores metadata in a _delta_log
directory containing JSON files where each line is a separate action.

The generated Delta log references existing Parquet files in-place, enabling
zero-copy migration from Hive to Delta format.
"""

import json
import uuid
from datetime import datetime
from typing import Any

from hive_to_delta.models import ParquetFileInfo
from hive_to_delta.s3 import parse_s3_path, write_to_s3


# Mapping of AWS Glue/Hive types to Delta Lake types
GLUE_TO_DELTA_TYPE_MAP: dict[str, str] = {
    # String types
    "string": "string",
    "char": "string",
    "varchar": "string",
    # Numeric types
    "tinyint": "byte",
    "smallint": "short",
    "int": "integer",
    "integer": "integer",
    "bigint": "long",
    "float": "float",
    "double": "double",
    "decimal": "decimal",
    # Boolean
    "boolean": "boolean",
    # Date/Time types
    "date": "date",
    "timestamp": "timestamp",
    # Binary
    "binary": "binary",
    # Complex types (simplified mapping)
    "array": "array",
    "map": "map",
    "struct": "struct",
}


def _normalize_glue_type(glue_type: str) -> str:
    """Normalize Glue type string for mapping.

    Handles parameterized types like decimal(10,2), varchar(255), etc.
    """
    glue_type = glue_type.lower().strip()

    # Handle parameterized types - extract base type
    if "(" in glue_type:
        base_type = glue_type.split("(")[0]
        # For decimal, preserve the full type specification
        if base_type == "decimal":
            return glue_type  # Return full decimal(p,s)
        return base_type

    return glue_type


def _map_glue_to_delta_type(glue_type: str) -> str:
    """Map a single Glue type to Delta type."""
    normalized = _normalize_glue_type(glue_type)

    # Handle decimal with precision/scale
    if normalized.startswith("decimal"):
        return normalized  # Keep as decimal(p,s)

    return GLUE_TO_DELTA_TYPE_MAP.get(normalized, "string")


def build_delta_schema(glue_columns: list[dict[str, str]]) -> dict[str, Any]:
    """Build Delta schema from Glue column definitions.

    Args:
        glue_columns: List of column dicts with 'Name' and 'Type' keys
                     (as returned by Glue API)

    Returns:
        Delta schema dict with 'type': 'struct' and 'fields' list
    """
    fields = []

    for col in glue_columns:
        col_name = col.get("Name", col.get("name", ""))
        col_type = col.get("Type", col.get("type", "string"))

        delta_type = _map_glue_to_delta_type(col_type)

        fields.append({
            "name": col_name,
            "type": delta_type,
            "nullable": True,
            "metadata": {},
        })

    return {
        "type": "struct",
        "fields": fields,
    }


def build_add_action(
    file_info: ParquetFileInfo,
    table_location: str,
) -> dict[str, Any]:
    """Build add action for a file in Delta log.

    Files inside the table root use relative paths.
    Files outside the table root use absolute S3 paths.

    Args:
        file_info: Parquet file info with path, size, partition values
        table_location: Table root S3 location (e.g., s3://bucket/path)

    Returns:
        Dict representing the 'add' action for Delta log
    """
    table_location = table_location.rstrip("/")
    file_path = file_info.path

    # Determine if file is inside table root
    if file_path.startswith("s3://"):
        # Absolute path - check if inside table root
        if file_path.startswith(table_location + "/"):
            # Inside table root - use relative path
            relative_path = file_path[len(table_location) + 1:]
            path_for_delta = relative_path
        else:
            # Outside table root - must use absolute path
            path_for_delta = file_path
    else:
        # Already relative path
        path_for_delta = file_path

    return {
        "add": {
            "path": path_for_delta,
            "partitionValues": file_info.partition_values,
            "size": file_info.size,
            "modificationTime": int(datetime.now().timestamp() * 1000),
            "dataChange": True,
        }
    }


def generate_delta_log(
    files: list[ParquetFileInfo],
    schema: dict[str, Any],
    partition_columns: list[str],
    table_location: str,
) -> str:
    """Generate full Delta log JSON content for initial commit.

    Creates the content for 00000000000000000000.json with:
    - Protocol action (reader v1, writer v2)
    - Metadata action (schema, partition columns, table ID)
    - Add actions for each file

    Args:
        files: List of Parquet files to include
        schema: Delta schema dict (from build_delta_schema)
        partition_columns: List of partition column names
        table_location: Table root S3 location

    Returns:
        JSON string (newline-delimited) for the Delta log file
    """
    table_id = str(uuid.uuid4())
    timestamp = int(datetime.now().timestamp() * 1000)

    entries = []

    # Protocol action
    protocol = {
        "protocol": {
            "minReaderVersion": 1,
            "minWriterVersion": 2,
        }
    }
    entries.append(json.dumps(protocol, separators=(",", ":")))

    # Metadata action
    metadata = {
        "metaData": {
            "id": table_id,
            "format": {
                "provider": "parquet",
                "options": {},
            },
            "schemaString": json.dumps(schema, separators=(",", ":")),
            "partitionColumns": partition_columns,
            "configuration": {},
            "createdTime": timestamp,
        }
    }
    entries.append(json.dumps(metadata, separators=(",", ":")))

    # Add actions for each file
    for file_info in files:
        add_action = build_add_action(file_info, table_location)
        entries.append(json.dumps(add_action, separators=(",", ":")))

    return "\n".join(entries)


def write_delta_log(
    delta_log_content: str,
    table_location: str,
    region: str = "us-east-1",
) -> str:
    """Write Delta log to S3 as _delta_log/00000000000000000000.json.

    Args:
        delta_log_content: JSON content for the Delta log file
        table_location: Table root S3 location
        region: AWS region for S3 client

    Returns:
        S3 path where Delta log was written
    """
    table_location = table_location.rstrip("/")
    delta_log_path = f"{table_location}/_delta_log/00000000000000000000.json"

    write_to_s3(delta_log_content, delta_log_path, region)

    return delta_log_path
