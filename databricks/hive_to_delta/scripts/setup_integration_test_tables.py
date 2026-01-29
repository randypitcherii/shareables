#!/usr/bin/env python3
"""Create test tables and data for hive_to_delta integration tests.

Creates 3 scenarios:
1. test_table_standard - Single bucket, single region
2. test_table_cross_bucket - Multiple buckets, same region
3. test_table_cross_region - Different regions

Each scenario creates partitioned tables with test data.
"""

import io
import os
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


# Configuration
AWS_PROFILE = "aws-sandbox-field-eng_databricks-sandbox-admin"
DEFAULT_REGION = "us-east-1"
DATABASE_NAME = "hive_to_delta_test"
BUCKET_EAST_1A = "htd-east-1a"
BUCKET_EAST_1B = "htd-east-1b"
BUCKET_WEST_2 = "htd-west-2"

# Schema for all tables
SCHEMA_COLUMNS = [
    {"Name": "id", "Type": "bigint"},
    {"Name": "name", "Type": "string"},
    {"Name": "value", "Type": "double"},
    {"Name": "created_at", "Type": "timestamp"},
]


def get_boto3_session():
    """Get boto3 session with explicit AWS profile."""
    return boto3.Session(profile_name=AWS_PROFILE, region_name=DEFAULT_REGION)


def generate_sample_data(
    partition_value: str,
    num_rows: int = 5,
    id_offset: int = 0,
) -> pa.Table:
    """Generate sample data with partition value embedded in name field.

    Args:
        partition_value: Value to embed in name field for identification
        num_rows: Number of rows to generate
        id_offset: Starting ID offset for uniqueness across partitions

    Returns:
        PyArrow Table with sample data
    """
    import datetime

    data = {
        "id": list(range(id_offset + 1, id_offset + num_rows + 1)),
        "name": [f"record_{partition_value}_{i}" for i in range(1, num_rows + 1)],
        "value": [i * 10.5 for i in range(1, num_rows + 1)],
        "created_at": [
            datetime.datetime(2024, 1, int(partition_value.split("-")[-1]), 12, 0, 0)
            for _ in range(num_rows)
        ],
    }
    return pa.table(data)


def upload_parquet_to_s3(
    table: pa.Table,
    bucket: str,
    key: str,
    aws_region: str = DEFAULT_REGION,
) -> int:
    """Upload PyArrow table as Parquet to S3.

    Args:
        table: PyArrow table to upload
        bucket: S3 bucket name
        key: S3 key (path within bucket)
        aws_region: AWS region for the S3 client

    Returns:
        Number of rows uploaded
    """
    # Ensure key ends with data.parquet
    if not key.endswith(".parquet"):
        key = key.rstrip("/") + "/data.parquet"

    # Write to buffer
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # Upload to S3 with explicit profile
    session = boto3.Session(profile_name=AWS_PROFILE, region_name=aws_region)
    s3_client = session.client("s3")
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())

    print(f"  Uploaded {len(table)} rows to s3://{bucket}/{key}")
    return len(table)


def create_glue_database(database_name: str) -> None:
    """Create Glue database if it doesn't exist."""
    session = get_boto3_session()
    glue_client = session.client("glue")

    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": database_name,
                "Description": "Hive to Delta integration test database",
            }
        )
        print(f"Created Glue database: {database_name}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"Glue database already exists: {database_name}")


def delete_glue_table_if_exists(table_name: str, database_name: str) -> None:
    """Delete Glue table if it exists (for fresh start)."""
    session = get_boto3_session()
    glue_client = session.client("glue")

    try:
        glue_client.delete_table(DatabaseName=database_name, Name=table_name)
        print(f"  Deleted existing table: {database_name}.{table_name}")
    except glue_client.exceptions.EntityNotFoundException:
        pass  # Table doesn't exist, nothing to delete


def create_glue_table(
    table_name: str,
    database_name: str,
    table_location: str,
    partition_keys: list[str],
    description: str = "",
) -> None:
    """Create Glue table with specified schema.

    Args:
        table_name: Name of the table
        database_name: Glue database name
        table_location: S3 location for table root
        partition_keys: List of partition column names
        description: Table description
    """
    session = get_boto3_session()
    glue_client = session.client("glue")

    partition_key_defs = [{"Name": key, "Type": "string"} for key in partition_keys]

    table_input = {
        "Name": table_name,
        "Description": description,
        "StorageDescriptor": {
            "Columns": SCHEMA_COLUMNS,
            "Location": table_location,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            },
        },
        "PartitionKeys": partition_key_defs,
        "TableType": "EXTERNAL_TABLE",
    }

    glue_client.create_table(DatabaseName=database_name, TableInput=table_input)
    print(f"  Created Glue table: {database_name}.{table_name}")


def add_glue_partition(
    table_name: str,
    database_name: str,
    partition_values: dict[str, str],
    location: str,
) -> None:
    """Add partition to Glue table.

    Args:
        table_name: Table name
        database_name: Database name
        partition_values: Partition key-value pairs (e.g., {"date": "2024-01-01"})
        location: S3 location for partition
    """
    session = get_boto3_session()
    glue_client = session.client("glue")

    # Get table to reuse storage descriptor
    table = glue_client.get_table(DatabaseName=database_name, Name=table_name)["Table"]

    partition_input = {
        "Values": list(partition_values.values()),
        "StorageDescriptor": {
            **table["StorageDescriptor"],
            "Location": location,
        },
    }

    glue_client.create_partition(
        DatabaseName=database_name,
        TableName=table_name,
        PartitionInput=partition_input,
    )
    print(f"  Added partition {partition_values} -> {location}")


def setup_standard_table() -> dict[str, Any]:
    """Scenario 1: Standard table (single bucket, single region).

    Table name: standard_table
    Location: s3://htd-east-1a/standard/
    Partitions: date=2024-01-01, date=2024-01-02
    """
    table_name = "standard_table"
    table_location = f"s3://{BUCKET_EAST_1A}/standard/"

    print(f"\n{'='*60}")
    print(f"Setting up: {table_name}")
    print(f"{'='*60}")

    # Delete existing table for fresh start
    delete_glue_table_if_exists(table_name, DATABASE_NAME)

    # Create table
    create_glue_table(
        table_name=table_name,
        database_name=DATABASE_NAME,
        table_location=table_location,
        partition_keys=["date"],
        description="Standard table - single bucket, single region",
    )

    # Create partition data and register
    partitions = {
        "2024-01-01": f"s3://{BUCKET_EAST_1A}/standard/date=2024-01-01/",
        "2024-01-02": f"s3://{BUCKET_EAST_1A}/standard/date=2024-01-02/",
    }

    row_counts = {}
    for date, location in partitions.items():
        # Generate data
        data = generate_sample_data(partition_value=date, num_rows=5)

        # Upload to S3
        bucket = location.split("/")[2]
        key = "/".join(location.split("/")[3:])
        row_count = upload_parquet_to_s3(data, bucket, key)
        row_counts[date] = row_count

        # Register partition
        add_glue_partition(table_name, DATABASE_NAME, {"date": date}, location)

    return {
        "table_name": table_name,
        "table_location": table_location,
        "partitions": partitions,
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
    }


def setup_cross_bucket_table() -> dict[str, Any]:
    """Scenario 2: Cross-bucket table (multiple buckets, same region).

    Table name: cross_bucket_table
    Location: s3://htd-east-1a/cross_bucket/
    Partition locations:
      - date=2024-01-01 -> s3://htd-east-1a/cross_bucket/date=2024-01-01/
      - date=2024-01-02 -> s3://htd-east-1b/cross_bucket/date=2024-01-02/
    """
    table_name = "cross_bucket_table"
    table_location = f"s3://{BUCKET_EAST_1A}/cross_bucket/"

    print(f"\n{'='*60}")
    print(f"Setting up: {table_name}")
    print(f"{'='*60}")

    # Delete existing table for fresh start
    delete_glue_table_if_exists(table_name, DATABASE_NAME)

    # Create table
    create_glue_table(
        table_name=table_name,
        database_name=DATABASE_NAME,
        table_location=table_location,
        partition_keys=["date"],
        description="Cross-bucket table - multiple buckets, same region",
    )

    # Create partition data and register
    partitions = {
        "2024-01-01": f"s3://{BUCKET_EAST_1A}/cross_bucket/date=2024-01-01/",
        "2024-01-02": f"s3://{BUCKET_EAST_1B}/cross_bucket/date=2024-01-02/",  # Different bucket!
    }

    row_counts = {}
    for date, location in partitions.items():
        # Generate data
        data = generate_sample_data(partition_value=date, num_rows=5)

        # Upload to S3
        bucket = location.split("/")[2]
        key = "/".join(location.split("/")[3:])
        row_count = upload_parquet_to_s3(data, bucket, key)
        row_counts[date] = row_count

        # Register partition
        add_glue_partition(table_name, DATABASE_NAME, {"date": date}, location)

    return {
        "table_name": table_name,
        "table_location": table_location,
        "partitions": partitions,
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
        "note": f"Partition 2024-01-02 in different bucket: {BUCKET_EAST_1B}",
    }


def setup_cross_region_table() -> dict[str, Any]:
    """Scenario 3: Cross-region table (different regions).

    Table name: cross_region_table
    Location: s3://htd-east-1a/cross_region/
    Partition locations:
      - date=2024-01-01 -> s3://htd-east-1a/cross_region/date=2024-01-01/ (us-east-1)
      - date=2024-01-02 -> s3://htd-west-2/cross_region/date=2024-01-02/ (us-west-2)
    """
    table_name = "cross_region_table"
    table_location = f"s3://{BUCKET_EAST_1A}/cross_region/"

    print(f"\n{'='*60}")
    print(f"Setting up: {table_name}")
    print(f"{'='*60}")

    # Delete existing table for fresh start
    delete_glue_table_if_exists(table_name, DATABASE_NAME)

    # Create table
    create_glue_table(
        table_name=table_name,
        database_name=DATABASE_NAME,
        table_location=table_location,
        partition_keys=["date"],
        description="Cross-region table - different regions",
    )

    # Create partition data and register
    partitions = {
        "2024-01-01": {
            "location": f"s3://{BUCKET_EAST_1A}/cross_region/date=2024-01-01/",
            "aws_region": "us-east-1",
        },
        "2024-01-02": {
            "location": f"s3://{BUCKET_WEST_2}/cross_region/date=2024-01-02/",
            "aws_region": "us-west-2",  # Different AWS region!
        },
    }

    row_counts = {}
    partition_locations = {}

    for date, config in partitions.items():
        location = config["location"]
        aws_region = config["aws_region"]

        # Generate data
        data = generate_sample_data(partition_value=date, num_rows=5)

        # Upload to S3 (using appropriate region for the bucket)
        bucket = location.split("/")[2]
        key = "/".join(location.split("/")[3:])
        row_count = upload_parquet_to_s3(data, bucket, key, aws_region=aws_region)
        row_counts[date] = row_count
        partition_locations[date] = location

        # Register partition (Glue is in us-east-1)
        add_glue_partition(table_name, DATABASE_NAME, {"date": date}, location)

    return {
        "table_name": table_name,
        "table_location": table_location,
        "partitions": partition_locations,
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
        "note": f"Partition 2024-01-02 in different region: {BUCKET_WEST_2} (us-west-2)",
    }


def print_summary(results: dict[str, dict[str, Any]]) -> None:
    """Print summary of all created tables."""
    print(f"\n{'='*60}")
    print("SUMMARY: Integration Test Tables Created")
    print(f"{'='*60}\n")

    for scenario_name, info in results.items():
        print(f"{scenario_name}:")
        print(f"  Table: {DATABASE_NAME}.{info['table_name']}")
        print(f"  Location: {info['table_location']}")
        print(f"  Total Rows: {info['total_rows']}")

        if "partitions" in info:
            print("  Partitions:")
            for partition, location in info["partitions"].items():
                row_count = info["row_counts"].get(partition, "N/A")
                print(f"    - {partition}: {row_count} rows @ {location}")

        if "note" in info:
            print(f"  NOTE: {info['note']}")
        print()


def main() -> dict[str, dict[str, Any]]:
    """Set up all 3 test scenarios."""
    print("Creating Integration Test Tables in AWS Glue")
    print("=" * 60)
    print(f"AWS Profile: {AWS_PROFILE}")
    print(f"AWS Region: {DEFAULT_REGION}")
    print(f"Glue Database: {DATABASE_NAME}")
    print(f"S3 Buckets: {BUCKET_EAST_1A}, {BUCKET_EAST_1B}, {BUCKET_WEST_2}")
    print("=" * 60)

    # Ensure database exists
    create_glue_database(DATABASE_NAME)

    results = {}

    # 1. Standard table
    results["scenario_1_standard"] = setup_standard_table()

    # 2. Cross-bucket table
    results["scenario_2_cross_bucket"] = setup_cross_bucket_table()

    # 3. Cross-region table
    results["scenario_3_cross_region"] = setup_cross_region_table()

    # Print summary
    print_summary(results)

    print("All tables created successfully!")
    print(f"Database: {DATABASE_NAME}")
    print(f"Total tables: {len(results)}")

    return results


if __name__ == "__main__":
    main()
