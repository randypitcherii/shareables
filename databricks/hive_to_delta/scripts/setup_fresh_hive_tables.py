#!/usr/bin/env python3
"""Create fresh Hive tables in AWS Glue for all partition scenarios.

Creates 5 scenarios:
1. standard_table - Partitions under table root
2. scattered_table - Partitions outside table root
3. cross_bucket_table - Partitions across S3 buckets (same region)
4. cross_region_table - Partitions across AWS regions
5. recursive_table - Non-partitioned with nested subdirectories

Each partition has unique, identifiable data (partition value in name field).
"""

import io
import os
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


# Configuration
DATABASE_NAME = os.getenv("HTD_GLUE_DATABASE", "hive_to_delta_test")
DEFAULT_REGION = "us-east-1"
BUCKET_EAST_1A = os.getenv("HTD_BUCKET_EAST_1A", "htd-east-1a")
BUCKET_EAST_1B = os.getenv("HTD_BUCKET_EAST_1B", "htd-east-1b")
BUCKET_WEST_2 = os.getenv("HTD_BUCKET_WEST_2", "htd-west-2")

# Schema for all tables
SCHEMA_COLUMNS = [
    {"Name": "id", "Type": "bigint"},
    {"Name": "name", "Type": "string"},
    {"Name": "value", "Type": "double"},
    {"Name": "active", "Type": "boolean"},
]


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
    data = {
        "id": list(range(id_offset + 1, id_offset + num_rows + 1)),
        "name": [f"record_{partition_value}_{i}" for i in range(1, num_rows + 1)],
        "value": [i * 10.5 for i in range(1, num_rows + 1)],
        "active": [i % 2 == 0 for i in range(1, num_rows + 1)],
    }
    return pa.table(data)


def generate_nested_data(subdir_name: str, num_rows: int = 3) -> pa.Table:
    """Generate sample data for recursive/nested directories.

    Args:
        subdir_name: Subdirectory name to embed in name field
        num_rows: Number of rows to generate

    Returns:
        PyArrow Table with sample data
    """
    data = {
        "id": list(range(1, num_rows + 1)),
        "name": [f"nested_{subdir_name}_{i}" for i in range(1, num_rows + 1)],
        "value": [i * 10.5 for i in range(1, num_rows + 1)],
        "active": [i % 2 == 0 for i in range(1, num_rows + 1)],
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

    # Upload to S3
    s3_client = boto3.client("s3", region_name=aws_region)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())

    print(f"  Uploaded {len(table)} rows to s3://{bucket}/{key}")
    return len(table)


def create_glue_database(database_name: str, aws_region: str = DEFAULT_REGION) -> None:
    """Create Glue database if it doesn't exist."""
    glue_client = boto3.client("glue", region_name=aws_region)

    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": database_name,
                "Description": "Hive to Delta migration test database",
            }
        )
        print(f"Created Glue database: {database_name}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"Glue database already exists: {database_name}")


def delete_glue_table_if_exists(
    table_name: str,
    database_name: str,
    aws_region: str = DEFAULT_REGION,
) -> None:
    """Delete Glue table if it exists (for fresh start)."""
    glue_client = boto3.client("glue", region_name=aws_region)

    try:
        glue_client.delete_table(DatabaseName=database_name, Name=table_name)
        print(f"  Deleted existing table: {database_name}.{table_name}")
    except glue_client.exceptions.EntityNotFoundException:
        pass  # Table doesn't exist, nothing to delete


def create_glue_table(
    table_name: str,
    database_name: str,
    table_location: str,
    partition_keys: list[str] | None = None,
    description: str = "",
    aws_region: str = DEFAULT_REGION,
) -> None:
    """Create Glue table with specified schema.

    Args:
        table_name: Name of the table
        database_name: Glue database name
        table_location: S3 location for table root
        partition_keys: List of partition column names (None for non-partitioned)
        description: Table description
        aws_region: AWS region
    """
    glue_client = boto3.client("glue", region_name=aws_region)

    partition_key_defs = []
    if partition_keys:
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
    aws_region: str = DEFAULT_REGION,
) -> None:
    """Add partition to Glue table.

    Args:
        table_name: Table name
        database_name: Database name
        partition_values: Partition key-value pairs (e.g., {"region": "us-east"})
        location: S3 location for partition
        aws_region: AWS region
    """
    glue_client = boto3.client("glue", region_name=aws_region)

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
    """Scenario 1: Partitions under table root.

    Location: s3://<bucket-east-1a>/tables/standard/
    Partitions: region=us-east, region=us-west (both under table root)
    """
    table_name = "standard_table"
    table_location = f"s3://{BUCKET_EAST_1A}/tables/standard/"

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
        partition_keys=["region"],
        description="Standard partitioned table - all partitions under table root",
    )

    # Create partition data and register
    partitions = {
        "us-east": f"s3://{BUCKET_EAST_1A}/tables/standard/region=us-east/",
        "us-west": f"s3://{BUCKET_EAST_1A}/tables/standard/region=us-west/",
    }

    row_counts = {}
    for region, location in partitions.items():
        # Generate data with partition value in name field
        data = generate_sample_data(partition_value=f"standard_{region}", num_rows=5)

        # Upload to S3
        bucket = location.split("/")[2]
        key = "/".join(location.split("/")[3:])
        row_count = upload_parquet_to_s3(data, bucket, key)
        row_counts[region] = row_count

        # Register partition
        add_glue_partition(table_name, DATABASE_NAME, {"region": region}, location)

    return {
        "table_name": table_name,
        "table_location": table_location,
        "partitions": partitions,
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
    }


def setup_scattered_table() -> dict[str, Any]:
    """Scenario 2: Partitions outside table root.

    Table Location: s3://<bucket-east-1a>/tables/scattered/
    Partition us-east: under table root
    Partition us-west: s3://<bucket-east-1a>/external/scattered-west/ (OUTSIDE table root)
    """
    table_name = "scattered_table"
    table_location = f"s3://{BUCKET_EAST_1A}/tables/scattered/"

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
        partition_keys=["region"],
        description="Scattered table - partitions outside table root",
    )

    # Create partition data and register
    # Key difference: us-west is OUTSIDE table root
    partitions = {
        "us-east": f"s3://{BUCKET_EAST_1A}/tables/scattered/region=us-east/",
        "us-west": f"s3://{BUCKET_EAST_1A}/external/scattered-west/",  # OUTSIDE table root!
    }

    row_counts = {}
    for region, location in partitions.items():
        # Generate data with partition value in name field
        data = generate_sample_data(partition_value=f"scattered_{region}", num_rows=5)

        # Upload to S3
        bucket = location.split("/")[2]
        key = "/".join(location.split("/")[3:])
        row_count = upload_parquet_to_s3(data, bucket, key)
        row_counts[region] = row_count

        # Register partition
        add_glue_partition(table_name, DATABASE_NAME, {"region": region}, location)

    return {
        "table_name": table_name,
        "table_location": table_location,
        "partitions": partitions,
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
        "external_partition": "us-west is OUTSIDE table root",
    }


def setup_cross_bucket_table() -> dict[str, Any]:
    """Scenario 3: Partitions across S3 buckets (same region).

    Table Location: s3://<bucket-east-1a>/tables/cross_bucket/
    Partition us-east: s3://<bucket-east-1a>/tables/cross_bucket/region=us-east/
    Partition us-west: s3://<bucket-east-1b>/tables/cross_bucket/region=us-west/ (DIFFERENT bucket)
    """
    table_name = "cross_bucket_table"
    table_location = f"s3://{BUCKET_EAST_1A}/tables/cross_bucket/"

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
        partition_keys=["region"],
        description="Cross-bucket table - partitions across different S3 buckets (same region)",
    )

    # Create partition data and register
    # Key difference: us-west is in DIFFERENT bucket
    partitions = {
        "us-east": f"s3://{BUCKET_EAST_1A}/tables/cross_bucket/region=us-east/",
        "us-west": f"s3://{BUCKET_EAST_1B}/tables/cross_bucket/region=us-west/",  # DIFFERENT bucket!
    }

    row_counts = {}
    for region, location in partitions.items():
        # Generate data with partition value in name field
        data = generate_sample_data(partition_value=f"cross_bucket_{region}", num_rows=5)

        # Upload to S3
        bucket = location.split("/")[2]
        key = "/".join(location.split("/")[3:])
        row_count = upload_parquet_to_s3(data, bucket, key)
        row_counts[region] = row_count

        # Register partition
        add_glue_partition(table_name, DATABASE_NAME, {"region": region}, location)

    return {
        "table_name": table_name,
        "table_location": table_location,
        "partitions": partitions,
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
        "cross_bucket": f"us-west in {BUCKET_EAST_1B}, us-east in {BUCKET_EAST_1A}",
    }


def setup_cross_region_table() -> dict[str, Any]:
    """Scenario 4: Partitions across AWS regions.

    Table Location: s3://<bucket-east-1a>/tables/cross_region/
    Partition us-east: in east-1a bucket (us-east-1)
    Partition us-west: s3://<bucket-west-2>/tables/cross_region/region=us-west/ (us-west-2 region)
    """
    table_name = "cross_region_table"
    table_location = f"s3://{BUCKET_EAST_1A}/tables/cross_region/"

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
        partition_keys=["region"],
        description="Cross-region table - partitions across different AWS regions",
    )

    # Create partition data and register
    # Key difference: us-west is in DIFFERENT AWS REGION
    partitions = {
        "us-east": {
            "location": f"s3://{BUCKET_EAST_1A}/tables/cross_region/region=us-east/",
            "aws_region": "us-east-1",
        },
        "us-west": {
            "location": f"s3://{BUCKET_WEST_2}/tables/cross_region/region=us-west/",
            "aws_region": "us-west-2",  # DIFFERENT AWS REGION!
        },
    }

    row_counts = {}
    partition_locations = {}

    for region, config in partitions.items():
        location = config["location"]
        aws_region = config["aws_region"]

        # Generate data with partition value in name field
        data = generate_sample_data(partition_value=f"cross_region_{region}", num_rows=5)

        # Upload to S3 (using appropriate region for the bucket)
        bucket = location.split("/")[2]
        key = "/".join(location.split("/")[3:])
        row_count = upload_parquet_to_s3(data, bucket, key, aws_region=aws_region)
        row_counts[region] = row_count
        partition_locations[region] = location

        # Register partition (Glue is in us-east-1)
        add_glue_partition(table_name, DATABASE_NAME, {"region": region}, location)

    return {
        "table_name": table_name,
        "table_location": table_location,
        "partitions": partition_locations,
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
        "cross_region": f"us-west in {BUCKET_WEST_2} (us-west-2), us-east in {BUCKET_EAST_1A} (us-east-1)",
    }


def setup_recursive_table() -> dict[str, Any]:
    """Scenario 5: Non-partitioned with nested subdirectories.

    Location: s3://<bucket-east-1a>/tables/recursive/
    Files in nested paths: subdir1/, subdir1/subdir2/, subdir3/
    Not partitioned - requires recursive file discovery
    """
    table_name = "recursive_table"
    table_location = f"s3://{BUCKET_EAST_1A}/tables/recursive/"

    print(f"\n{'='*60}")
    print(f"Setting up: {table_name}")
    print(f"{'='*60}")

    # Delete existing table for fresh start
    delete_glue_table_if_exists(table_name, DATABASE_NAME)

    # Create table (NOT partitioned)
    create_glue_table(
        table_name=table_name,
        database_name=DATABASE_NAME,
        table_location=table_location,
        partition_keys=None,  # NOT partitioned!
        description="Recursive table - non-partitioned with nested subdirectories",
    )

    # Create nested directory structure with data
    nested_paths = {
        "subdir1": f"tables/recursive/subdir1/",
        "subdir1_subdir2": f"tables/recursive/subdir1/subdir2/",
        "subdir3": f"tables/recursive/subdir3/",
    }

    row_counts = {}
    for subdir_name, key_prefix in nested_paths.items():
        # Generate data with subdirectory name in name field
        data = generate_nested_data(subdir_name=subdir_name, num_rows=3)

        # Upload to S3
        row_count = upload_parquet_to_s3(data, BUCKET_EAST_1A, key_prefix)
        row_counts[subdir_name] = row_count

    return {
        "table_name": table_name,
        "table_location": table_location,
        "nested_paths": nested_paths,
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
        "note": "Not partitioned - requires recursive scan to find all data files",
    }


def print_summary(results: dict[str, dict[str, Any]]) -> None:
    """Print summary of all created tables."""
    print(f"\n{'='*60}")
    print("SUMMARY: Fresh Hive Tables Created")
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
        elif "nested_paths" in info:
            print("  Nested Paths:")
            for subdir, path in info["nested_paths"].items():
                row_count = info["row_counts"].get(subdir, "N/A")
                print(f"    - {subdir}: {row_count} rows @ {path}")

        if "external_partition" in info:
            print(f"  NOTE: {info['external_partition']}")
        if "cross_bucket" in info:
            print(f"  NOTE: {info['cross_bucket']}")
        if "cross_region" in info:
            print(f"  NOTE: {info['cross_region']}")
        if "note" in info:
            print(f"  NOTE: {info['note']}")
        print()


def main() -> None:
    """Set up all 5 scenarios."""
    print("Creating Fresh Hive Tables in AWS Glue")
    print("=" * 60)

    # Ensure database exists
    create_glue_database(DATABASE_NAME)

    results = {}

    # 1. Standard table
    results["standard_table"] = setup_standard_table()

    # 2. Scattered table (external partitions)
    results["scattered_table"] = setup_scattered_table()

    # 3. Cross-bucket table
    results["cross_bucket_table"] = setup_cross_bucket_table()

    # 4. Cross-region table
    results["cross_region_table"] = setup_cross_region_table()

    # 5. Recursive table
    results["recursive_table"] = setup_recursive_table()

    # Print summary
    print_summary(results)

    print("All tables created successfully!")
    print(f"Database: {DATABASE_NAME}")
    print(f"Total tables: {len(results)}")

    return results


if __name__ == "__main__":
    main()
