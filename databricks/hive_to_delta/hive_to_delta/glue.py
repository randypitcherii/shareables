"""AWS Glue metadata utilities.

Provides helpers for interacting with AWS Glue Data Catalog:
- Fetching table metadata
- Listing partitions and their locations
- Discovering tables in a database
"""

import fnmatch
from typing import Any, Optional

import boto3


def get_glue_client(region: str = "us-east-1") -> boto3.client:
    """
    Create a boto3 Glue client.

    Args:
        region: AWS region for the Glue client.

    Returns:
        Configured boto3 Glue client.
    """
    return boto3.client("glue", region_name=region)


def get_glue_table_metadata(
    glue_database: str,
    table_name: str,
    region: str = "us-east-1",
) -> dict[str, Any]:
    """
    Fetch table metadata from AWS Glue Data Catalog.

    Retrieves the full table definition including schema, storage descriptor,
    partition keys, and table properties.

    Args:
        glue_database: Name of the Glue database containing the table.
        table_name: Name of the table to fetch metadata for.
        region: AWS region where the Glue catalog resides.

    Returns:
        Dictionary containing the full table metadata from Glue.

    Raises:
        botocore.exceptions.ClientError: If the table doesn't exist or
            there's an AWS API error.

    Example:
        >>> metadata = get_glue_table_metadata("my_database", "my_table")
        >>> print(metadata["StorageDescriptor"]["Location"])
        s3://my-bucket/my-table/
    """
    glue_client = get_glue_client(region)
    response = glue_client.get_table(DatabaseName=glue_database, Name=table_name)
    return response["Table"]


def get_glue_partitions(
    glue_database: str,
    table_name: str,
    region: str = "us-east-1",
) -> list[dict[str, Any]]:
    """
    Fetch all partition metadata from AWS Glue Data Catalog.

    Handles pagination automatically to retrieve all partitions for tables
    with many partitions.

    Args:
        glue_database: Name of the Glue database containing the table.
        table_name: Name of the table to fetch partitions for.
        region: AWS region where the Glue catalog resides.

    Returns:
        List of partition dictionaries, each containing:
        - Values: List of partition column values
        - StorageDescriptor: Storage info including Location (S3 path)
        - CreationTime: When the partition was created
        - Other Glue partition metadata

    Raises:
        botocore.exceptions.ClientError: If the table doesn't exist or
            there's an AWS API error.

    Example:
        >>> partitions = get_glue_partitions("my_database", "my_table")
        >>> for p in partitions:
        ...     print(p["Values"], p["StorageDescriptor"]["Location"])
        ['2024', '01'] s3://bucket/table/year=2024/month=01/
    """
    glue_client = get_glue_client(region)
    partitions: list[dict[str, Any]] = []

    paginator = glue_client.get_paginator("get_partitions")
    for page in paginator.paginate(DatabaseName=glue_database, TableName=table_name):
        partitions.extend(page.get("Partitions", []))

    return partitions


def list_glue_tables(
    glue_database: str,
    pattern: Optional[str] = None,
    region: str = "us-east-1",
) -> list[str]:
    """
    List tables in a Glue database with optional pattern filtering.

    Retrieves table names from the specified Glue database, optionally
    filtering by a glob-style pattern.

    Args:
        glue_database: Name of the Glue database to list tables from.
        pattern: Optional glob pattern to filter table names (e.g., "sales_*",
            "*_fact", "dim_???"). Supports * (any chars), ? (single char),
            [seq] (char in seq), [!seq] (char not in seq). If None, returns
            all tables.
        region: AWS region where the Glue catalog resides.

    Returns:
        List of table names matching the pattern (or all tables if no
        pattern specified), sorted alphabetically.

    Raises:
        botocore.exceptions.ClientError: If the database doesn't exist or
            there's an AWS API error.

    Example:
        >>> # List all tables
        >>> tables = list_glue_tables("my_database")
        ['customers', 'orders', 'products']

        >>> # List tables matching a pattern
        >>> tables = list_glue_tables("my_database", pattern="dim_*")
        ['dim_customer', 'dim_date', 'dim_product']
    """
    glue_client = get_glue_client(region)
    table_names: list[str] = []

    paginator = glue_client.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=glue_database):
        for table in page.get("TableList", []):
            table_names.append(table["Name"])

    # Filter by pattern if provided
    if pattern is not None:
        table_names = [name for name in table_names if fnmatch.fnmatch(name, pattern)]

    return sorted(table_names)
