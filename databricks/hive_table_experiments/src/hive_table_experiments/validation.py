"""Validation utilities for write operation testing.

Provides helpers for:
- Glue metadata validation
- S3 file verification
- Partition location inspection
"""

import boto3
from typing import Optional, Dict, Any, List


def get_glue_client(region: str = "us-east-1"):
    """Create boto3 Glue client."""
    return boto3.client("glue", region_name=region)


def get_s3_client(region: str = "us-east-1"):
    """Create boto3 S3 client."""
    return boto3.client("s3", region_name=region)


def get_partition_location(
    glue_client,
    database: str,
    table: str,
    partition_values: List[str]
) -> Optional[str]:
    """
    Get the S3 location for a specific partition from Glue.

    Args:
        glue_client: Boto3 Glue client
        database: Glue database name
        table: Table name
        partition_values: List of partition values in order

    Returns:
        S3 location string or None if not found
    """
    try:
        response = glue_client.get_partition(
            DatabaseName=database,
            TableName=table,
            PartitionValues=partition_values
        )
        return response["Partition"]["StorageDescriptor"]["Location"]
    except glue_client.exceptions.EntityNotFoundException:
        return None


def get_table_location(
    glue_client,
    database: str,
    table: str
) -> Optional[str]:
    """
    Get the S3 location for a table from Glue.

    Args:
        glue_client: Boto3 Glue client
        database: Glue database name
        table: Table name

    Returns:
        S3 location string or None if not found
    """
    try:
        response = glue_client.get_table(DatabaseName=database, Name=table)
        return response["Table"]["StorageDescriptor"]["Location"]
    except glue_client.exceptions.EntityNotFoundException:
        return None


def get_glue_partitions(
    glue_client,
    database: str,
    table: str
) -> List[Dict[str, Any]]:
    """
    Get all partitions for a Glue table.

    Args:
        glue_client: Boto3 Glue client
        database: Glue database name
        table: Table name

    Returns:
        List of partition dictionaries
    """
    try:
        response = glue_client.get_partitions(DatabaseName=database, TableName=table)
        return response.get("Partitions", [])
    except Exception:
        return []


def parse_s3_path(s3_path: str) -> tuple[str, str]:
    """
    Parse S3 path into bucket and key.

    Args:
        s3_path: Full S3 path (s3://bucket/key)

    Returns:
        Tuple of (bucket, key)

    Raises:
        ValueError: If path is not a valid S3 path
    """
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")

    parts = s3_path[5:].split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""

    return bucket, key


def list_s3_files(s3_client, s3_path: str) -> List[str]:
    """
    List all files at an S3 path.

    Args:
        s3_client: Boto3 S3 client
        s3_path: Full S3 path (s3://bucket/prefix)

    Returns:
        List of S3 keys
    """
    if not s3_path.startswith("s3://"):
        return []

    bucket, prefix = parse_s3_path(s3_path)

    # Ensure prefix ends with / for directory listing
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [obj["Key"] for obj in response.get("Contents", [])]
    except Exception:
        return []


def count_s3_parquet_files(s3_client, s3_path: str) -> int:
    """
    Count parquet files at an S3 path.

    Args:
        s3_client: Boto3 S3 client
        s3_path: Full S3 path (s3://bucket/prefix)

    Returns:
        Number of .parquet files
    """
    files = list_s3_files(s3_client, s3_path)
    return len([f for f in files if f.endswith(".parquet")])


def verify_partition_exists_in_glue(
    glue_client,
    database: str,
    table: str,
    partition_values: List[str]
) -> bool:
    """
    Check if a partition exists in Glue.

    Args:
        glue_client: Boto3 Glue client
        database: Glue database name
        table: Table name
        partition_values: List of partition values

    Returns:
        True if partition exists
    """
    return get_partition_location(glue_client, database, table, partition_values) is not None


def verify_s3_files_exist(s3_client, s3_path: str) -> bool:
    """
    Check if any files exist at an S3 path.

    Args:
        s3_client: Boto3 S3 client
        s3_path: Full S3 path

    Returns:
        True if at least one file exists
    """
    files = list_s3_files(s3_client, s3_path)
    return len(files) > 0


def get_partition_info(
    glue_client,
    database: str,
    table: str,
    partition_values: List[str]
) -> Optional[Dict[str, Any]]:
    """
    Get full partition information from Glue.

    Args:
        glue_client: Boto3 Glue client
        database: Glue database name
        table: Table name
        partition_values: List of partition values

    Returns:
        Partition dictionary or None if not found
    """
    try:
        response = glue_client.get_partition(
            DatabaseName=database,
            TableName=table,
            PartitionValues=partition_values
        )
        return response["Partition"]
    except glue_client.exceptions.EntityNotFoundException:
        return None


class WriteValidationResult:
    """Result of a write validation check."""

    def __init__(
        self,
        operation: str,
        scenario: str,
        success: bool,
        message: str,
        row_count_before: Optional[int] = None,
        row_count_after: Optional[int] = None,
        glue_partition_created: Optional[bool] = None,
        s3_files_written: Optional[bool] = None,
        error: Optional[str] = None
    ):
        self.operation = operation
        self.scenario = scenario
        self.success = success
        self.message = message
        self.row_count_before = row_count_before
        self.row_count_after = row_count_after
        self.glue_partition_created = glue_partition_created
        self.s3_files_written = s3_files_written
        self.error = error

    def __repr__(self):
        status = "SUCCESS" if self.success else "FAILED"
        return f"WriteValidationResult({self.operation}, {self.scenario}, {status}: {self.message})"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "operation": self.operation,
            "scenario": self.scenario,
            "success": self.success,
            "message": self.message,
            "row_count_before": self.row_count_before,
            "row_count_after": self.row_count_after,
            "glue_partition_created": self.glue_partition_created,
            "s3_files_written": self.s3_files_written,
            "error": self.error,
        }
