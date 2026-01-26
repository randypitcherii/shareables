"""External VACUUM: Cleanup orphaned files at absolute S3 paths.

When Delta tables use absolute S3 paths pointing to external buckets,
standard VACUUM cannot clean up orphaned files because it only scans
the table root directory.

This module provides utilities to:
1. Read Delta transaction log and identify "remove" actions with absolute paths
2. Filter for files past the retention period
3. Delete those files via boto3 (with dry_run mode)

CRITICAL: Only vacuums files with absolute S3 paths (s3://) that are OUTSIDE
the table root. Standard VACUUM handles files under the table root.
"""

import json
import logging
from datetime import datetime
from typing import Any
from urllib.parse import unquote

from hive_to_delta.models import VacuumResult
from hive_to_delta.s3 import get_s3_client, parse_s3_path

logger = logging.getLogger(__name__)


def get_delta_log_entries(
    table_location: str,
    region: str = "us-east-1",
) -> list[dict]:
    """Read all Delta transaction log JSON entries from S3.

    Reads JSON commit files (000...000.json) from the _delta_log directory.
    Each line in a JSON file is a separate log entry.

    Args:
        table_location: S3 path to the Delta table root.
        region: AWS region for S3 client.

    Returns:
        List of parsed JSON objects from the transaction log.
        Each entry includes a '_version' field with the log version number.
    """
    s3_client = get_s3_client(region)
    table_location = table_location.rstrip("/")
    bucket, prefix = parse_s3_path(table_location)

    delta_log_prefix = f"{prefix}/_delta_log/"

    entries = []
    json_files = []

    # List all JSON files in _delta_log
    paginator = s3_client.get_paginator("list_objects_v2")
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
            logger.warning(f"Failed to read {key}: {e}")

    return entries


def parse_remove_actions(entries: list[dict]) -> list[dict]:
    """Extract remove actions from Delta log entries.

    Args:
        entries: List of parsed Delta log JSON objects.

    Returns:
        List of remove action dicts with keys:
        - path: File path (URL-decoded)
        - deletion_timestamp: Unix timestamp in milliseconds
        - size: File size in bytes (optional)
        - version: Transaction log version where remove occurred
    """
    removes = []

    for entry in entries:
        if "remove" in entry:
            remove = entry["remove"]
            # URL-decode the path (Delta encodes special characters)
            path = unquote(remove.get("path", ""))

            removes.append({
                "path": path,
                "deletion_timestamp": remove.get("deletionTimestamp", 0),
                "size": remove.get("size"),
                "version": entry.get("_version", 0),
            })

    return removes


def parse_add_actions(entries: list[dict]) -> list[str]:
    """Extract currently active file paths from Delta log entries.

    Args:
        entries: List of parsed Delta log JSON objects.

    Returns:
        List of active file paths (URL-decoded).
    """
    # Track add/remove versions to determine which files are currently active
    path_to_latest_add_version: dict[str, int] = {}
    path_to_latest_remove_version: dict[str, int] = {}

    for entry in entries:
        if "add" in entry:
            add = entry["add"]
            path = unquote(add.get("path", ""))
            version = entry.get("_version", 0)
            current_version = path_to_latest_add_version.get(path, -1)
            if version > current_version:
                path_to_latest_add_version[path] = version

        if "remove" in entry:
            remove = entry["remove"]
            path = unquote(remove.get("path", ""))
            version = entry.get("_version", 0)
            current_version = path_to_latest_remove_version.get(path, -1)
            if version > current_version:
                path_to_latest_remove_version[path] = version

    # A path is active if its latest action was an add (not a remove)
    active_paths = []
    for path, add_version in path_to_latest_add_version.items():
        remove_version = path_to_latest_remove_version.get(path, -1)
        if add_version > remove_version:
            active_paths.append(path)

    return active_paths


def find_orphaned_external_paths(
    removes: list[dict],
    adds: list[str],
    table_location: str,
    retention_hours: float = 168,
) -> list[str]:
    """Find removed files at absolute S3 paths outside the table root.

    A file is considered orphaned if:
    1. It has a remove action in the log
    2. The path is absolute (starts with s3://)
    3. The path is OUTSIDE the table location
    4. The path is not currently active (not in adds)
    5. The deletion timestamp is older than the retention period

    Args:
        removes: List of remove action dicts from parse_remove_actions.
        adds: List of active file paths from parse_add_actions.
        table_location: S3 path to the table root.
        retention_hours: Minimum age in hours for a file to be vacuumed.

    Returns:
        List of absolute S3 paths for orphaned external files.
    """
    table_location = table_location.rstrip("/")
    active_paths = set(adds)

    now_ms = int(datetime.now().timestamp() * 1000)
    retention_ms = retention_hours * 60 * 60 * 1000
    cutoff_ms = now_ms - retention_ms

    # Track latest remove version per path to avoid duplicates
    path_to_latest_remove: dict[str, dict[str, Any]] = {}
    for remove in removes:
        path = remove["path"]
        current = path_to_latest_remove.get(path)
        if current is None or remove["version"] > current["version"]:
            path_to_latest_remove[path] = remove

    orphaned = []
    for path, remove in path_to_latest_remove.items():
        # Skip non-absolute paths
        if not path.startswith("s3://"):
            continue

        # Skip paths inside table location (standard VACUUM handles these)
        if path.startswith(table_location):
            continue

        # Skip if file is currently active
        if path in active_paths:
            continue

        # Skip if not past retention period
        if remove["deletion_timestamp"] > cutoff_ms:
            continue

        orphaned.append(path)

    return orphaned


def delete_s3_files(
    paths: list[str],
    region: str = "us-east-1",
    dry_run: bool = True,
) -> list[str]:
    """Delete files from S3 using batch delete.

    Args:
        paths: List of full S3 paths to delete.
        region: AWS region for S3 client.
        dry_run: If True, don't actually delete, just return paths.

    Returns:
        List of successfully deleted (or would-be-deleted) paths.
    """
    if not paths:
        return []

    if dry_run:
        return paths

    s3_client = get_s3_client(region)
    deleted = []

    # Group by bucket for batch deletion
    bucket_to_keys: dict[str, list[str]] = {}
    for path in paths:
        try:
            bucket, key = parse_s3_path(path)
            if bucket not in bucket_to_keys:
                bucket_to_keys[bucket] = []
            bucket_to_keys[bucket].append(key)
        except ValueError as e:
            logger.warning(f"Invalid S3 path {path}: {e}")

    # Delete from each bucket (S3 delete_objects handles up to 1000 keys)
    for bucket, keys in bucket_to_keys.items():
        for i in range(0, len(keys), 1000):
            batch = keys[i : i + 1000]
            delete_objects = [{"Key": k} for k in batch]

            try:
                response = s3_client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": delete_objects, "Quiet": False},
                )

                # Track successes
                for obj in response.get("Deleted", []):
                    deleted.append(f"s3://{bucket}/{obj['Key']}")

                # Log failures
                for error in response.get("Errors", []):
                    logger.error(
                        f"Failed to delete s3://{bucket}/{error['Key']}: "
                        f"{error.get('Message', 'Unknown error')}"
                    )

            except Exception as e:
                logger.error(f"Batch delete failed for bucket {bucket}: {e}")

    return deleted


def vacuum_external_files(
    spark,
    table_name: str,
    retention_hours: float = 168,
    dry_run: bool = True,
    region: str = "us-east-1",
) -> VacuumResult:
    """Vacuum orphaned external files for a Delta table.

    This is the main entry point for cleaning up orphaned files at absolute
    S3 paths that standard VACUUM cannot reach.

    CRITICAL: Only vacuums files with absolute S3 paths (s3://) that are
    OUTSIDE the table root. Standard VACUUM handles files under the table root.

    Args:
        spark: SparkSession (used to get table location).
        table_name: Fully qualified table name (catalog.schema.table).
        retention_hours: Minimum age in hours for file to be deleted (default 7 days).
        dry_run: If True, only identify files without deleting.
        region: AWS region for S3 operations.

    Returns:
        VacuumResult with details of the operation.
    """
    # Validate retention period (minimum 7 days for safety)
    MIN_RETENTION_HOURS = 168  # 7 days
    if retention_hours < MIN_RETENTION_HOURS:
        return VacuumResult(
            table_name=table_name,
            orphaned_files=[],
            deleted_files=[],
            dry_run=dry_run,
            error=f"Retention period must be at least {MIN_RETENTION_HOURS} hours (7 days), got {retention_hours}",
        )

    # Get table location from Spark
    try:
        df = spark.sql(f"DESCRIBE DETAIL {table_name}")
        row = df.collect()[0]
        table_location = row["location"]
    except Exception as e:
        return VacuumResult(
            table_name=table_name,
            orphaned_files=[],
            deleted_files=[],
            dry_run=dry_run,
            error=f"Failed to get table location: {e}",
        )

    # Read Delta log
    try:
        entries = get_delta_log_entries(table_location, region)
    except Exception as e:
        return VacuumResult(
            table_name=table_name,
            orphaned_files=[],
            deleted_files=[],
            dry_run=dry_run,
            error=f"Failed to read Delta log: {e}",
        )

    # Parse actions
    removes = parse_remove_actions(entries)
    adds = parse_add_actions(entries)

    # Find orphaned external files
    orphaned_paths = find_orphaned_external_paths(
        removes, adds, table_location, retention_hours
    )

    if not orphaned_paths:
        return VacuumResult(
            table_name=table_name,
            orphaned_files=[],
            deleted_files=[],
            dry_run=dry_run,
        )

    # Delete files
    deleted = delete_s3_files(orphaned_paths, region, dry_run)

    return VacuumResult(
        table_name=table_name,
        orphaned_files=orphaned_paths,
        deleted_files=deleted if not dry_run else [],
        dry_run=dry_run,
    )
