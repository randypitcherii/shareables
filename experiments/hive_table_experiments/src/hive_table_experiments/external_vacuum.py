"""External VACUUM: Cleanup orphaned files at absolute S3 paths.

When Delta tables use the Manual Delta Log approach with absolute S3 paths
pointing to external buckets, standard VACUUM cannot clean up orphaned files
because it only scans the table root directory.

This module provides utilities to:
1. Read Delta transaction log and identify "remove" actions with absolute paths
2. Filter for files past the retention period
3. Delete those files via boto3 (with dry_run mode)

## Problem Background

When UPDATE/DELETE runs on tables with external partitions:
1. Delta marks the old file as REMOVED in the transaction log
2. Delta creates new files at the table root (relative paths)
3. Standard VACUUM scans only the table root directory
4. External files are never seen or deleted by VACUUM

## Solution

This module reads the Delta log directly to find remove actions,
identifies absolute paths outside the table root, and deletes them
using the S3 API.

## Usage

### Via Spark (Databricks notebook/cluster)

```python
from hive_table_experiments.external_vacuum import (
    vacuum_external_files,
    get_orphaned_external_files_spark
)

# Find orphaned files (dry run)
orphans = vacuum_external_files(
    spark,
    table_name="catalog.schema.table",
    retention_hours=0,
    dry_run=True
)

# Actually delete them
deleted = vacuum_external_files(
    spark,
    table_name="catalog.schema.table",
    retention_hours=0,
    dry_run=False
)
```

### Via Pure Python (no Spark required)

```python
from hive_table_experiments.external_vacuum import (
    vacuum_external_files_pure_python,
    get_delta_log_from_s3,
    find_orphaned_external_paths
)

# Get orphaned files from S3-based Delta log
orphans = vacuum_external_files_pure_python(
    table_location="s3://bucket/path/to/table/",
    retention_hours=0,
    dry_run=True,
    region="us-east-1"
)
```

## References

- [Delta Lake Protocol - Remove Action](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
- [VACUUM Limitation with Absolute Paths](../docs/VACUUM_ABSOLUTE_PATHS_LIMITATION.md)
"""

import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any, Set
from urllib.parse import unquote

from .validation import get_s3_client, parse_s3_path


# -----------------------------------------------------------------------------
# Data Classes
# -----------------------------------------------------------------------------

@dataclass
class RemoveAction:
    """Represents a Delta Lake remove action from the transaction log."""
    path: str  # Relative or absolute path
    deletion_timestamp: int  # Unix timestamp in milliseconds
    data_change: bool = True
    extended_file_metadata: bool = False
    partition_values: Dict[str, str] = field(default_factory=dict)
    size: Optional[int] = None
    version: int = 0  # Transaction log version where this remove occurred

    @property
    def deletion_datetime(self) -> datetime:
        """Convert deletion timestamp to datetime."""
        return datetime.fromtimestamp(self.deletion_timestamp / 1000)

    @property
    def is_absolute_path(self) -> bool:
        """Check if the path is an absolute S3 path."""
        return self.path.startswith("s3://") or self.path.startswith("s3a://")

    @property
    def age_hours(self) -> float:
        """Age of the remove action in hours."""
        now_ms = int(datetime.now().timestamp() * 1000)
        return (now_ms - self.deletion_timestamp) / (1000 * 60 * 60)


@dataclass
class AddAction:
    """Represents a Delta Lake add action from the transaction log."""
    path: str
    size: int
    modification_time: int
    data_change: bool = True
    partition_values: Dict[str, str] = field(default_factory=dict)
    version: int = 0


@dataclass
class ExternalVacuumResult:
    """Result of an external vacuum operation."""
    table_name: str
    table_location: str
    retention_hours: float
    dry_run: bool
    orphaned_files: List[str]
    deleted_files: List[str]
    failed_deletions: List[Dict[str, str]]  # path -> error message
    total_size_bytes: int
    execution_time_seconds: float
    notes: List[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """True if all identified orphaned files were deleted (or dry_run)."""
        if self.dry_run:
            return True
        return len(self.failed_deletions) == 0

    def __repr__(self) -> str:
        if self.dry_run:
            return (
                f"ExternalVacuumResult(DRY RUN - "
                f"{len(self.orphaned_files)} files would be deleted, "
                f"{self.total_size_bytes:,} bytes)"
            )
        return (
            f"ExternalVacuumResult("
            f"{len(self.deleted_files)} deleted, "
            f"{len(self.failed_deletions)} failed)"
        )


# -----------------------------------------------------------------------------
# Delta Log Reading Functions
# -----------------------------------------------------------------------------

def get_delta_log_from_s3(
    table_location: str,
    region: str = "us-east-1"
) -> List[Dict[str, Any]]:
    """
    Read all Delta transaction log entries from S3.

    Reads both JSON commit files (000...000.json) and parquet checkpoints.
    For simplicity, this implementation only reads JSON files.

    Args:
        table_location: S3 path to the Delta table root
        region: AWS region for S3 client

    Returns:
        List of parsed JSON objects from the transaction log
    """
    s3_client = get_s3_client(region)
    table_location = table_location.rstrip("/")
    bucket, prefix = parse_s3_path(table_location)

    delta_log_prefix = f"{prefix}/_delta_log/"

    entries = []

    # List all files in _delta_log
    paginator = s3_client.get_paginator("list_objects_v2")
    json_files = []

    for page in paginator.paginate(Bucket=bucket, Prefix=delta_log_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Only process JSON commit files (not checkpoints or crc files)
            if key.endswith(".json") and not key.endswith(".crc"):
                json_files.append(key)

    # Sort by version number (filename is version padded to 20 digits)
    json_files.sort()

    # Read each JSON file
    for key in json_files:
        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read().decode("utf-8")

            # Extract version from filename
            filename = key.split("/")[-1]
            version = int(filename.replace(".json", ""))

            # Each line is a separate JSON object
            for line in content.strip().split("\n"):
                if line:
                    entry = json.loads(line)
                    entry["_version"] = version
                    entries.append(entry)
        except Exception as e:
            print(f"Warning: Failed to read {key}: {e}")

    return entries


def parse_remove_actions(
    delta_log_entries: List[Dict[str, Any]]
) -> List[RemoveAction]:
    """
    Extract remove actions from Delta log entries.

    Args:
        delta_log_entries: List of parsed Delta log JSON objects

    Returns:
        List of RemoveAction objects
    """
    removes = []

    for entry in delta_log_entries:
        if "remove" in entry:
            remove = entry["remove"]
            # URL-decode the path (Delta encodes special characters)
            path = unquote(remove.get("path", ""))

            removes.append(RemoveAction(
                path=path,
                deletion_timestamp=remove.get("deletionTimestamp", 0),
                data_change=remove.get("dataChange", True),
                extended_file_metadata=remove.get("extendedFileMetadata", False),
                partition_values=remove.get("partitionValues", {}),
                size=remove.get("size"),
                version=entry.get("_version", 0),
            ))

    return removes


def parse_add_actions(
    delta_log_entries: List[Dict[str, Any]]
) -> List[AddAction]:
    """
    Extract add actions from Delta log entries.

    Args:
        delta_log_entries: List of parsed Delta log JSON objects

    Returns:
        List of AddAction objects
    """
    adds = []

    for entry in delta_log_entries:
        if "add" in entry:
            add = entry["add"]
            path = unquote(add.get("path", ""))

            adds.append(AddAction(
                path=path,
                size=add.get("size", 0),
                modification_time=add.get("modificationTime", 0),
                data_change=add.get("dataChange", True),
                partition_values=add.get("partitionValues", {}),
                version=entry.get("_version", 0),
            ))

    return adds


def find_orphaned_external_paths(
    remove_actions: List[RemoveAction],
    add_actions: List[AddAction],
    table_location: str,
    retention_hours: float = 168,  # 7 days default
) -> List[RemoveAction]:
    """
    Find removed files that are at absolute external paths and past retention.

    A file is considered orphaned if:
    1. It has a remove action in the log
    2. The path is absolute (starts with s3://)
    3. The path is OUTSIDE the table location (different bucket or prefix)
    4. There is no subsequent add action for the same path
    5. The deletion timestamp is older than the retention period

    Args:
        remove_actions: List of remove actions from the log
        add_actions: List of add actions from the log
        table_location: S3 path to the table root
        retention_hours: Minimum age in hours for a file to be vacuumed

    Returns:
        List of RemoveAction objects for orphaned external files
    """
    table_location = table_location.rstrip("/")

    # Track the latest action version for each path (add or remove)
    # A file is currently active only if its latest action was an ADD
    path_to_latest_add_version: Dict[str, int] = {}
    path_to_latest_remove_version: Dict[str, int] = {}

    for add in add_actions:
        current_version = path_to_latest_add_version.get(add.path, -1)
        if add.version > current_version:
            path_to_latest_add_version[add.path] = add.version

    for remove in remove_actions:
        current_version = path_to_latest_remove_version.get(remove.path, -1)
        if remove.version > current_version:
            path_to_latest_remove_version[remove.path] = remove.version

    orphaned = []

    for remove in remove_actions:
        # Skip non-absolute paths
        if not remove.is_absolute_path:
            continue

        # Check if path is outside table location
        # (file at table root would have a relative path, not absolute)
        # An absolute path that starts with the table location could be
        # inside the table root but stored with full path - still skip
        if remove.path.startswith(table_location):
            continue

        # Check retention period
        if remove.age_hours < retention_hours:
            continue

        # Check if file was re-added AFTER this specific remove action
        latest_add_version = path_to_latest_add_version.get(remove.path, -1)
        if latest_add_version > remove.version:
            # File was re-added after this remove, skip
            continue

        # Check if this remove is the latest action for this path
        # (handles case where same file was removed multiple times)
        latest_remove_version = path_to_latest_remove_version.get(remove.path, -1)
        if remove.version < latest_remove_version:
            # This is an older remove action, skip (we'll process the latest one)
            continue

        orphaned.append(remove)

    return orphaned


# -----------------------------------------------------------------------------
# S3 Deletion Functions
# -----------------------------------------------------------------------------

def delete_s3_files(
    paths: List[str],
    region: str = "us-east-1",
    dry_run: bool = True
) -> Dict[str, List]:
    """
    Delete files from S3.

    Args:
        paths: List of full S3 paths to delete
        region: AWS region (note: cross-region deletes work)
        dry_run: If True, don't actually delete

    Returns:
        Dict with 'deleted' and 'failed' lists
    """
    result = {
        "deleted": [],
        "failed": []
    }

    if dry_run:
        result["deleted"] = paths
        return result

    s3_client = get_s3_client(region)

    # Group by bucket for batch deletion
    bucket_to_keys: Dict[str, List[str]] = {}
    for path in paths:
        try:
            bucket, key = parse_s3_path(path)
            if bucket not in bucket_to_keys:
                bucket_to_keys[bucket] = []
            bucket_to_keys[bucket].append(key)
        except ValueError as e:
            result["failed"].append({"path": path, "error": str(e)})

    # Delete from each bucket
    for bucket, keys in bucket_to_keys.items():
        # S3 delete_objects can handle up to 1000 keys at a time
        for i in range(0, len(keys), 1000):
            batch = keys[i:i + 1000]
            delete_objects = [{"Key": k} for k in batch]

            try:
                response = s3_client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": delete_objects, "Quiet": False}
                )

                # Track successes
                for deleted in response.get("Deleted", []):
                    result["deleted"].append(f"s3://{bucket}/{deleted['Key']}")

                # Track failures
                for error in response.get("Errors", []):
                    result["failed"].append({
                        "path": f"s3://{bucket}/{error['Key']}",
                        "error": error.get("Message", "Unknown error")
                    })

            except Exception as e:
                # If batch delete fails, mark all as failed
                for key in batch:
                    result["failed"].append({
                        "path": f"s3://{bucket}/{key}",
                        "error": str(e)
                    })

    return result


def get_file_sizes(
    paths: List[str],
    region: str = "us-east-1"
) -> Dict[str, int]:
    """
    Get file sizes for S3 paths.

    Args:
        paths: List of S3 paths
        region: AWS region

    Returns:
        Dict mapping path to size in bytes
    """
    s3_client = get_s3_client(region)
    sizes = {}

    for path in paths:
        try:
            bucket, key = parse_s3_path(path)
            response = s3_client.head_object(Bucket=bucket, Key=key)
            sizes[path] = response["ContentLength"]
        except Exception:
            sizes[path] = 0

    return sizes


# -----------------------------------------------------------------------------
# Main Vacuum Functions
# -----------------------------------------------------------------------------

def vacuum_external_files_pure_python(
    table_location: str,
    retention_hours: float = 168,
    dry_run: bool = True,
    region: str = "us-east-1",
) -> ExternalVacuumResult:
    """
    Vacuum orphaned external files using pure Python (no Spark required).

    This reads the Delta log from S3, identifies orphaned files at absolute
    paths outside the table root, and deletes them.

    Args:
        table_location: S3 path to the Delta table root
        retention_hours: Minimum age in hours for file to be deleted (default 7 days)
        dry_run: If True, only identify files without deleting
        region: AWS region for S3 operations

    Returns:
        ExternalVacuumResult with details of the operation
    """
    start_time = time.time()
    notes = []

    table_location = table_location.rstrip("/")
    notes.append(f"Table location: {table_location}")
    notes.append(f"Retention: {retention_hours} hours")

    # Read Delta log
    try:
        entries = get_delta_log_from_s3(table_location, region)
        notes.append(f"Read {len(entries)} Delta log entries")
    except Exception as e:
        return ExternalVacuumResult(
            table_name="",
            table_location=table_location,
            retention_hours=retention_hours,
            dry_run=dry_run,
            orphaned_files=[],
            deleted_files=[],
            failed_deletions=[{"path": "N/A", "error": f"Failed to read Delta log: {e}"}],
            total_size_bytes=0,
            execution_time_seconds=time.time() - start_time,
            notes=notes,
        )

    # Parse actions
    remove_actions = parse_remove_actions(entries)
    add_actions = parse_add_actions(entries)
    notes.append(f"Found {len(remove_actions)} remove actions, {len(add_actions)} add actions")

    # Find orphaned external files
    orphaned = find_orphaned_external_paths(
        remove_actions,
        add_actions,
        table_location,
        retention_hours
    )
    orphaned_paths = [r.path for r in orphaned]
    notes.append(f"Identified {len(orphaned_paths)} orphaned external files")

    if not orphaned_paths:
        return ExternalVacuumResult(
            table_name="",
            table_location=table_location,
            retention_hours=retention_hours,
            dry_run=dry_run,
            orphaned_files=[],
            deleted_files=[],
            failed_deletions=[],
            total_size_bytes=0,
            execution_time_seconds=time.time() - start_time,
            notes=notes,
        )

    # Get file sizes
    sizes = get_file_sizes(orphaned_paths, region)
    total_size = sum(sizes.values())
    notes.append(f"Total size: {total_size:,} bytes")

    # Delete files
    delete_result = delete_s3_files(orphaned_paths, region, dry_run)

    return ExternalVacuumResult(
        table_name="",
        table_location=table_location,
        retention_hours=retention_hours,
        dry_run=dry_run,
        orphaned_files=orphaned_paths,
        deleted_files=delete_result["deleted"],
        failed_deletions=delete_result["failed"],
        total_size_bytes=total_size,
        execution_time_seconds=time.time() - start_time,
        notes=notes,
    )


def get_table_location_from_spark(spark, table_name: str) -> str:
    """
    Get the S3 location of a Delta table using Spark.

    Args:
        spark: SparkSession
        table_name: Fully qualified table name (catalog.schema.table)

    Returns:
        S3 location string
    """
    df = spark.sql(f"DESCRIBE DETAIL {table_name}")
    row = df.collect()[0]
    return row["location"]


def get_orphaned_external_files_spark(
    spark,
    table_name: str,
    retention_hours: float = 168,
) -> List[Dict[str, Any]]:
    """
    Get orphaned external files using Spark to read the Delta log.

    This uses Spark's ability to read JSON files directly.

    Args:
        spark: SparkSession
        table_name: Fully qualified table name
        retention_hours: Minimum age in hours

    Returns:
        List of dicts with file info (path, deletionTimestamp, size)
    """
    # Get table location
    location = get_table_location_from_spark(spark, table_name)
    delta_log_path = f"{location}/_delta_log/*.json"

    # Read all Delta log JSON files
    log_df = spark.read.json(delta_log_path)

    # Extract remove actions with absolute paths
    # Note: In Spark SQL, we can use LATERAL VIEW to explode the JSON
    log_df.createOrReplaceTempView("delta_log")

    now_ms = int(datetime.now().timestamp() * 1000)
    retention_ms = retention_hours * 60 * 60 * 1000
    cutoff_ms = now_ms - retention_ms

    # Query for external remove actions past retention
    orphans_df = spark.sql(f"""
        SELECT
            remove.path as path,
            remove.deletionTimestamp as deletion_timestamp,
            remove.size as size,
            remove.partitionValues as partition_values
        FROM delta_log
        WHERE remove IS NOT NULL
          AND remove.path LIKE 's3://%'
          AND remove.path NOT LIKE '{location}%'
          AND remove.deletionTimestamp < {cutoff_ms}
          AND remove.path NOT IN (
              SELECT add.path FROM delta_log WHERE add IS NOT NULL
          )
    """)

    return [row.asDict() for row in orphans_df.collect()]


def vacuum_external_files(
    spark,
    table_name: str,
    retention_hours: float = 168,
    dry_run: bool = True,
    region: str = "us-east-1",
) -> ExternalVacuumResult:
    """
    Vacuum orphaned external files for a Delta table.

    This is the main entry point for cleaning up orphaned files in external
    buckets that VACUUM cannot reach.

    Args:
        spark: SparkSession
        table_name: Fully qualified table name (catalog.schema.table)
        retention_hours: Minimum age in hours for file to be deleted (default 7 days)
        dry_run: If True, only identify files without deleting
        region: AWS region for S3 operations

    Returns:
        ExternalVacuumResult with details of the operation
    """
    start_time = time.time()
    notes = []

    # Get table location
    try:
        table_location = get_table_location_from_spark(spark, table_name)
        notes.append(f"Table location: {table_location}")
    except Exception as e:
        return ExternalVacuumResult(
            table_name=table_name,
            table_location="",
            retention_hours=retention_hours,
            dry_run=dry_run,
            orphaned_files=[],
            deleted_files=[],
            failed_deletions=[{"path": "N/A", "error": f"Failed to get table location: {e}"}],
            total_size_bytes=0,
            execution_time_seconds=time.time() - start_time,
            notes=notes,
        )

    # Get orphaned files using Spark
    try:
        orphaned = get_orphaned_external_files_spark(spark, table_name, retention_hours)
        orphaned_paths = [o["path"] for o in orphaned]
        notes.append(f"Identified {len(orphaned_paths)} orphaned external files")
    except Exception as e:
        # Fall back to pure Python approach
        notes.append(f"Spark query failed ({e}), falling back to pure Python")
        return vacuum_external_files_pure_python(
            table_location,
            retention_hours,
            dry_run,
            region
        )

    if not orphaned_paths:
        return ExternalVacuumResult(
            table_name=table_name,
            table_location=table_location,
            retention_hours=retention_hours,
            dry_run=dry_run,
            orphaned_files=[],
            deleted_files=[],
            failed_deletions=[],
            total_size_bytes=0,
            execution_time_seconds=time.time() - start_time,
            notes=notes,
        )

    # Calculate total size
    total_size = sum(o.get("size", 0) or 0 for o in orphaned)
    notes.append(f"Total size: {total_size:,} bytes")

    # Delete files
    delete_result = delete_s3_files(orphaned_paths, region, dry_run)

    return ExternalVacuumResult(
        table_name=table_name,
        table_location=table_location,
        retention_hours=retention_hours,
        dry_run=dry_run,
        orphaned_files=orphaned_paths,
        deleted_files=delete_result["deleted"],
        failed_deletions=delete_result["failed"],
        total_size_bytes=total_size,
        execution_time_seconds=time.time() - start_time,
        notes=notes,
    )


# -----------------------------------------------------------------------------
# SQL-Based Approach (for Databricks SQL)
# -----------------------------------------------------------------------------

def generate_orphaned_files_query(
    table_name: str,
    retention_hours: float = 168,
) -> str:
    """
    Generate SQL to find orphaned external files.

    This can be run in Databricks SQL to identify files that need cleanup.
    The actual deletion would need to be done via Python/boto3.

    Args:
        table_name: Fully qualified table name
        retention_hours: Minimum age in hours

    Returns:
        SQL query string
    """
    now_ms = int(datetime.now().timestamp() * 1000)
    retention_ms = retention_hours * 60 * 60 * 1000
    cutoff_ms = now_ms - retention_ms

    return f"""
    -- Find orphaned external files for {table_name}
    -- These files are marked as removed but cannot be deleted by VACUUM
    -- because they are at absolute S3 paths outside the table root.
    --
    -- Run this query, then use the results to delete files via boto3.

    WITH table_info AS (
        SELECT location FROM (DESCRIBE DETAIL {table_name})
    ),
    delta_log AS (
        SELECT * FROM json.`${{(SELECT location FROM table_info)}}/_delta_log/*.json`
    ),
    removes AS (
        SELECT
            remove.path as path,
            remove.deletionTimestamp as deletion_timestamp,
            remove.size as size
        FROM delta_log
        WHERE remove IS NOT NULL
    ),
    adds AS (
        SELECT add.path as path
        FROM delta_log
        WHERE add IS NOT NULL
    )
    SELECT
        r.path,
        r.deletion_timestamp,
        r.size,
        from_unixtime(r.deletion_timestamp / 1000) as deletion_datetime
    FROM removes r
    LEFT JOIN adds a ON r.path = a.path
    WHERE a.path IS NULL  -- Not re-added
      AND r.path LIKE 's3://%'  -- Absolute path
      AND r.deletion_timestamp < {cutoff_ms}  -- Past retention
    ORDER BY r.deletion_timestamp
    """


# -----------------------------------------------------------------------------
# Convenience Functions
# -----------------------------------------------------------------------------

def list_external_remove_actions(
    table_location: str,
    region: str = "us-east-1"
) -> List[RemoveAction]:
    """
    List all remove actions for external files (for inspection).

    Args:
        table_location: S3 path to Delta table root
        region: AWS region

    Returns:
        List of RemoveAction objects for external paths
    """
    entries = get_delta_log_from_s3(table_location, region)
    removes = parse_remove_actions(entries)

    # Filter to external paths only
    return [r for r in removes if r.is_absolute_path]


def check_external_files_exist(
    table_location: str,
    region: str = "us-east-1"
) -> Dict[str, bool]:
    """
    Check which external removed files still exist in S3.

    Useful for auditing the state before/after vacuum.

    Args:
        table_location: S3 path to Delta table root
        region: AWS region

    Returns:
        Dict mapping path to exists (True/False)
    """
    s3_client = get_s3_client(region)
    external_removes = list_external_remove_actions(table_location, region)

    exists = {}
    for remove in external_removes:
        try:
            bucket, key = parse_s3_path(remove.path)
            s3_client.head_object(Bucket=bucket, Key=key)
            exists[remove.path] = True
        except s3_client.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                exists[remove.path] = False
            else:
                exists[remove.path] = False  # Assume doesn't exist on error

    return exists
