"""S3 utilities for scanning and managing parquet files.

Provides helpers for:
- S3 path parsing
- Parquet file discovery
- Writing content to S3
"""

import logging
from typing import Any

import boto3
from botocore.exceptions import ClientError, BotoCoreError

from hive_to_delta.models import ParquetFileInfo

logger = logging.getLogger(__name__)


def get_s3_client(region: str = "us-east-1") -> Any:
    """Create boto3 S3 client.

    Args:
        region: AWS region for the S3 client.

    Returns:
        Boto3 S3 client configured for the specified region.
    """
    return boto3.client("s3", region_name=region)


def parse_s3_path(s3_path: str) -> tuple[str, str]:
    """Parse S3 path into bucket and key.

    Args:
        s3_path: Full S3 path (s3://bucket/key).

    Returns:
        Tuple of (bucket, key).

    Raises:
        ValueError: If path is not a valid S3 path.
    """
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")

    parts = s3_path[5:].split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""

    return bucket, key


def list_parquet_files(
    s3_path: str,
    region: str = "us-east-1",
) -> list[ParquetFileInfo]:
    """List all parquet files at an S3 path.

    Scans the given S3 location recursively and returns metadata
    for all .parquet files found.

    Args:
        s3_path: S3 path to scan (e.g., s3://bucket/path/).
        region: AWS region for S3 client.

    Returns:
        List of ParquetFileInfo for each discovered .parquet file.
    """
    s3_client = get_s3_client(region)
    s3_path = s3_path.rstrip("/")
    bucket, prefix = parse_s3_path(s3_path)

    # Ensure prefix ends with / for directory listing
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    files = []

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(".parquet"):
                continue

            # Use absolute S3 path - critical for cross-bucket support
            absolute_path = f"s3://{bucket}/{key}"

            files.append(
                ParquetFileInfo(
                    path=absolute_path,
                    size=obj["Size"],
                    partition_values={},
                )
            )

    return files


def scan_partition_files(
    partition_locations: list[tuple[str, dict[str, str]]],
    region: str = "us-east-1",
) -> list[ParquetFileInfo]:
    """Scan multiple partition paths for parquet files.

    This handles scenarios where partitions are scattered across different
    S3 locations, buckets, or even regions. Each partition location is
    scanned and files are tagged with partition values.

    Args:
        partition_locations: List of tuples containing (s3_location, partition_values).
            - s3_location: S3 path to scan for parquet files.
            - partition_values: Dict mapping partition column names to values.
        region: AWS region for S3 client.

    Returns:
        List of ParquetFileInfo with absolute S3 paths and partition values.
    """
    s3_client = get_s3_client(region)
    files = []

    for partition_location, partition_values in partition_locations:
        partition_location = partition_location.rstrip("/")
        bucket, prefix = parse_s3_path(partition_location)

        # Ensure prefix ends with / for directory listing
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        try:
            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]

                    if not key.endswith(".parquet"):
                        continue

                    # Use absolute S3 path - critical for cross-bucket support
                    absolute_path = f"s3://{bucket}/{key}"

                    files.append(
                        ParquetFileInfo(
                            path=absolute_path,
                            size=obj["Size"],
                            partition_values=partition_values.copy(),
                        )
                    )
        except (ClientError, BotoCoreError) as e:
            logger.warning(f"AWS API error scanning {partition_location}: {e}")
            # Don't add files from this partition to the result
        except ValueError as e:
            logger.warning(f"Invalid S3 path {partition_location}: {e}")
        except Exception as e:
            # Re-raise unexpected errors (programming errors)
            logger.error(f"Unexpected error scanning {partition_location}: {e}")
            raise

    return files


def write_to_s3(
    content: str,
    s3_path: str,
    region: str = "us-east-1",
) -> str:
    """Write content to an S3 path.

    Args:
        content: String content to write.
        s3_path: Full S3 path to write to (s3://bucket/key).
        region: AWS region for S3 client.

    Returns:
        The S3 path where content was written.

    Raises:
        ValueError: If s3_path is not a valid S3 path.
    """
    s3_client = get_s3_client(region)
    bucket, key = parse_s3_path(s3_path)

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=content.encode("utf-8"),
        ContentType="application/json",
    )

    return s3_path
