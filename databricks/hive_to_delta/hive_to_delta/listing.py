"""Listing strategies for discovering parquet files.

Provides different strategies for building file lists:
- S3Listing: Scans S3 directly (with optional Glue partition awareness)
- InventoryListing: Uses a pre-built DataFrame of file paths and sizes
"""

import re
from dataclasses import dataclass, field
from typing import Any, Optional, Protocol, runtime_checkable

from hive_to_delta.glue import get_glue_partitions
from hive_to_delta.models import ParquetFileInfo, TableInfo
from hive_to_delta.s3 import scan_partition_files


@runtime_checkable
class Listing(Protocol):
    """Protocol for listing parquet files belonging to a table."""

    def list_files(self, spark: Any, table: TableInfo) -> list[ParquetFileInfo]:
        ...


def _parse_partition_values(file_path: str, partition_keys: list[str]) -> dict[str, str]:
    """Parse partition key=value segments from a file path.

    Uses regex to extract key=value pairs from the path, then maps each
    requested partition key to its value. Keys not found in the path get
    an empty string.

    Args:
        file_path: Full file path potentially containing key=value/ segments.
        partition_keys: List of partition column names to extract.

    Returns:
        Dict mapping each partition key to its parsed value (or empty string).
    """
    parsed = dict(re.findall(r"([^/]+)=([^/]+)", file_path))
    return {key: parsed.get(key, "") for key in partition_keys}


@dataclass
class S3Listing:
    """List parquet files by scanning S3 directly.

    For partitioned tables with a glue_database configured, fetches partition
    metadata from Glue to discover partition locations. Otherwise scans the
    table root location.

    Attributes:
        region: AWS region for S3 and Glue clients.
        glue_database: Optional Glue database name for partition discovery.
    """

    region: str = "us-east-1"
    glue_database: Optional[str] = None

    def list_files(self, spark: Any, table: TableInfo) -> list[ParquetFileInfo]:
        """List parquet files for a table by scanning S3.

        Args:
            spark: Spark session (unused, kept for protocol compatibility).
            table: Table metadata including location and partition keys.

        Returns:
            List of ParquetFileInfo for discovered parquet files.
        """
        if table.partition_keys and self.glue_database:
            partitions = get_glue_partitions(
                self.glue_database, table.name, region=self.region
            )
            partition_locations: list[tuple[str, dict[str, str]]] = []
            for p in partitions:
                location = p["StorageDescriptor"]["Location"]
                values = dict(zip(table.partition_keys, p["Values"]))
                partition_locations.append((location, values))
            return scan_partition_files(partition_locations, region=self.region)
        else:
            # Non-partitioned or no Glue database: scan root
            return scan_partition_files(
                [(table.location, {})], region=self.region
            )


def validate_files_df(files_df: Any) -> None:
    """Validate that a DataFrame has the required columns and types for file listing.

    Args:
        files_df: Spark DataFrame to validate.

    Raises:
        ValueError: If required columns are missing or have wrong types.
    """
    columns = list(files_df.columns)
    if "file_path" not in columns:
        raise ValueError(
            f"DataFrame must have 'file_path' column, got columns: {columns}"
        )
    if "size" not in columns:
        raise ValueError(
            f"DataFrame must have 'size' column, got columns: {columns}"
        )

    dtype_map = {name: dtype for name, dtype in files_df.dtypes}
    if dtype_map["file_path"] != "string":
        raise ValueError(
            f"'file_path' column must be string type, got: {dtype_map['file_path']}"
        )
    if dtype_map["size"] not in ("int", "bigint", "long"):
        raise ValueError(
            f"'size' column must be int/bigint/long type, got: {dtype_map['size']}"
        )


class InventoryListing:
    """List parquet files from a pre-built inventory DataFrame.

    The DataFrame must have:
    - file_path (string): Full S3 path to each parquet file.
    - size (int/bigint/long): File size in bytes.

    Attributes:
        files_df: Spark DataFrame containing file inventory.
    """

    def __init__(self, files_df: Any) -> None:
        """Initialize with a validated inventory DataFrame.

        Args:
            files_df: Spark DataFrame with file_path and size columns.

        Raises:
            ValueError: If required columns are missing or have wrong types.
        """
        validate_files_df(files_df)
        self.files_df = files_df

    def list_files(self, spark: Any, table: TableInfo) -> list[ParquetFileInfo]:
        """Build ParquetFileInfo list from inventory DataFrame.

        Filters rows to only include files under the table's location prefix,
        so this works correctly when the DataFrame contains files for multiple tables.

        Args:
            spark: Spark session (unused, kept for protocol compatibility).
            table: Table metadata including location and partition keys.

        Returns:
            List of ParquetFileInfo built from matching DataFrame rows.
        """
        location_prefix = table.location.rstrip("/") + "/"
        rows = self.files_df.collect()
        files = []
        for row in rows:
            if not row.file_path.startswith(location_prefix):
                continue
            partition_values = (
                _parse_partition_values(row.file_path, table.partition_keys)
                if table.partition_keys
                else {}
            )
            files.append(
                ParquetFileInfo(
                    path=row.file_path,
                    size=row.size,
                    partition_values=partition_values,
                )
            )
        return files
