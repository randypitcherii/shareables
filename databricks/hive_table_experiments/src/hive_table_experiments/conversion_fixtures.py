"""Conversion evaluation fixture creation for Glue-to-UC Delta migration.

Creates 21 independent Glue source tables (7 scenarios × 3 conversion methods)
for evaluating Delta conversion approaches:
- CONVERT TO DELTA
- SHALLOW CLONE
- Manual Delta Log creation

Each scenario × method combination gets:
- Unique Glue table: {scenario}_src_{method}
- Unique S3 path: s3://bucket/conversion_eval/{scenario}/{method}/
- Separate partitions to avoid cross-contamination during conversion testing

Hard constraint: Evaluate which methods require data copies (disqualified) and
which methods read data files (unsafe for cold storage).
"""

import io
import os
from dataclasses import dataclass, field
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from .scenarios import (
    ALL_SCENARIOS,
    BUCKET_EAST_1A,
    BUCKET_EAST_1B,
    BUCKET_WEST_2,
    HiveTableScenario,
    PartitionLocation,
    SCENARIO_BY_BASE_NAME,
)

# Conversion methods under evaluation
CONVERSION_METHODS = ["convert", "clone", "manual"]

# Default settings (uses environment variable via scenarios.py)
DEFAULT_BUCKET = BUCKET_EAST_1A
DEFAULT_DATABASE = os.getenv("HIVE_EVAL_GLUE_DATABASE", "your_glue_database")


@dataclass
class ConversionSourceTable:
    """Represents a Glue source table created for conversion testing.

    Attributes:
        scenario: Base scenario name (e.g., "standard", "scattered")
        method: Conversion method (e.g., "convert", "clone", "manual")
        glue_table_name: Full Glue table name
        s3_base_path: Base S3 path for table data
        partitions: List of partition info dicts
        database: Glue database name
        row_count: Number of rows in the table
    """
    scenario: str
    method: str
    glue_table_name: str
    s3_base_path: str
    partitions: list[dict[str, Any]] = field(default_factory=list)
    database: str = DEFAULT_DATABASE
    row_count: int = 0


@dataclass
class ConversionResult:
    """Tracks the outcome of a conversion attempt.

    Attributes:
        scenario: Base scenario name
        method: Conversion method used
        success: Whether conversion succeeded
        error_message: Error message if failed
        requires_data_copy: Whether method copied data (DISQUALIFIER)
        reads_data_files: Whether method read actual data (cold storage unsafe)
        source_table: Glue source table name
        target_table: UC Delta table name (if successful)
        row_count_before: Rows in source table
        row_count_after: Rows in target table (if successful)
    """
    scenario: str
    method: str
    success: bool = False
    error_message: str | None = None
    requires_data_copy: bool = False
    reads_data_files: bool = False
    source_table: str = ""
    target_table: str | None = None
    row_count_before: int = 0
    row_count_after: int | None = None


def get_conversion_table_name(scenario: str, method: str) -> str:
    """Generate Glue table name for conversion source.

    Args:
        scenario: Scenario base name (e.g., "standard")
        method: Conversion method (e.g., "convert")

    Returns:
        Table name like "standard_src_convert"
    """
    return f"{scenario}_src_{method}"


def get_conversion_s3_path(
    scenario: str,
    method: str,
    bucket: str = DEFAULT_BUCKET,
) -> str:
    """Generate S3 base path for conversion source data.

    Args:
        scenario: Scenario base name
        method: Conversion method
        bucket: S3 bucket name

    Returns:
        S3 path like "s3://bucket/conversion_eval/standard/convert/"
    """
    return f"s3://{bucket}/conversion_eval/{scenario}/{method}/"


def generate_sample_data(num_rows: int = 10) -> pa.Table:
    """Generate minimal synthetic data for testing.

    Args:
        num_rows: Number of rows to generate

    Returns:
        PyArrow Table with sample data
    """
    data = {
        "id": list(range(1, num_rows + 1)),
        "name": [f"record_{i}" for i in range(1, num_rows + 1)],
        "value": [i * 10.5 for i in range(1, num_rows + 1)],
        "is_active": [i % 2 == 0 for i in range(1, num_rows + 1)],
    }
    return pa.table(data)


def upload_parquet_to_s3(
    table: pa.Table,
    s3_path: str,
    aws_region: str = "us-east-1",
) -> None:
    """Upload PyArrow table as Parquet to S3.

    Args:
        table: PyArrow table to upload
        s3_path: Full S3 path (s3://bucket/key)
        aws_region: AWS region
    """
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")

    parts = s3_path[5:].split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""

    # Add .parquet extension if not present
    if not key.endswith(".parquet"):
        key = key.rstrip("/")
        key = f"{key}/data.parquet"

    # Write to buffer
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # Upload to S3
    s3_client = boto3.client("s3", region_name=aws_region)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())

    print(f"  ✓ Uploaded {len(table)} rows to s3://{bucket}/{key}")


def create_glue_database(
    database_name: str,
    aws_region: str = "us-east-1",
) -> None:
    """Create Glue database if it doesn't exist."""
    glue_client = boto3.client("glue", region_name=aws_region)

    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": database_name,
                "Description": "Hive migration compatibility validation - conversion evaluation",
            }
        )
        print(f"✓ Created Glue database: {database_name}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"⊙ Glue database already exists: {database_name}")


def create_conversion_source_table(
    scenario: HiveTableScenario,
    method: str,
    database_name: str = DEFAULT_DATABASE,
    bucket: str = DEFAULT_BUCKET,
    aws_region: str = "us-east-1",
) -> ConversionSourceTable:
    """Create a single Glue source table for conversion testing.

    For exotic partition scenarios (scattered, cross_bucket, cross_region, shared),
    we replicate the exotic layout in the conversion source to test whether each
    conversion method can handle it.

    Args:
        scenario: HiveTableScenario to create
        method: Conversion method ("convert", "clone", "manual")
        database_name: Glue database name
        bucket: Primary S3 bucket
        aws_region: AWS region

    Returns:
        ConversionSourceTable with table metadata
    """
    glue_client = boto3.client("glue", region_name=aws_region)
    table_name = get_conversion_table_name(scenario.base_name, method)
    s3_base_path = get_conversion_s3_path(scenario.base_name, method, bucket)

    # Generate sample data
    sample_data = generate_sample_data(10)

    result = ConversionSourceTable(
        scenario=scenario.base_name,
        method=method,
        glue_table_name=table_name,
        s3_base_path=s3_base_path,
        database=database_name,
        row_count=len(sample_data),
    )

    # Define schema
    columns = [
        {"Name": "id", "Type": "bigint"},
        {"Name": "name", "Type": "string"},
        {"Name": "value", "Type": "double"},
        {"Name": "is_active", "Type": "boolean"},
    ]

    # Handle different scenario types
    if scenario.recursive_scan_required:
        # Recursive scenario: nested directories, no partitions
        partition_keys = []
        table_input = {
            "Name": table_name,
            "Description": f"Conversion source for {scenario.base_name} ({method})",
            "StorageDescriptor": {
                "Columns": columns,
                "Location": s3_base_path,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                },
            },
            "PartitionKeys": partition_keys,
            "TableType": "EXTERNAL_TABLE",
        }

        # Create table
        try:
            glue_client.create_table(DatabaseName=database_name, TableInput=table_input)
            print(f"  ✓ Created Glue table: {database_name}.{table_name}")
        except glue_client.exceptions.AlreadyExistsException:
            print(f"  ⊙ Glue table already exists: {database_name}.{table_name}")

        # Upload nested data
        nested_paths = [
            f"{s3_base_path}subdir1/data.parquet",
            f"{s3_base_path}subdir1/subdir2/data.parquet",
            f"{s3_base_path}subdir3/data.parquet",
        ]
        for path in nested_paths:
            upload_parquet_to_s3(sample_data, path, aws_region)

    elif scenario.partitions:
        # Partitioned scenario - replicate the exotic layout
        partition_key_names = list(set(p.partition_key for p in scenario.partitions))
        partition_keys = [{"Name": key, "Type": "string"} for key in partition_key_names]

        table_input = {
            "Name": table_name,
            "Description": f"Conversion source for {scenario.base_name} ({method})",
            "StorageDescriptor": {
                "Columns": columns,
                "Location": s3_base_path,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                },
            },
            "PartitionKeys": partition_keys,
            "TableType": "EXTERNAL_TABLE",
        }

        # Create table
        try:
            glue_client.create_table(DatabaseName=database_name, TableInput=table_input)
            print(f"  ✓ Created Glue table: {database_name}.{table_name}")
        except glue_client.exceptions.AlreadyExistsException:
            print(f"  ⊙ Glue table already exists: {database_name}.{table_name}")

        # Create partitions - replicate exotic layouts
        for original_partition in scenario.partitions:
            # Determine partition location based on scenario type
            if scenario.use_external_partitions:
                # For exotic layouts, replicate the external partition pattern
                # but in our conversion_eval namespace
                partition_path = _get_exotic_partition_path(
                    scenario, original_partition, method, bucket
                )
            else:
                # Standard layout: partition under table root
                partition_path = (
                    f"{s3_base_path}{original_partition.partition_key}="
                    f"{original_partition.partition_value}/"
                )

            # Upload data
            upload_parquet_to_s3(sample_data, partition_path, aws_region)

            # Register partition
            partition_values = {original_partition.partition_key: original_partition.partition_value}
            _add_glue_partition(
                table_name, database_name, partition_values, partition_path, aws_region
            )

            result.partitions.append({
                "key": original_partition.partition_key,
                "value": original_partition.partition_value,
                "path": partition_path,
            })

    else:
        # Non-partitioned, non-recursive - simple table
        table_input = {
            "Name": table_name,
            "Description": f"Conversion source for {scenario.base_name} ({method})",
            "StorageDescriptor": {
                "Columns": columns,
                "Location": s3_base_path,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                },
            },
            "PartitionKeys": [],
            "TableType": "EXTERNAL_TABLE",
        }

        try:
            glue_client.create_table(DatabaseName=database_name, TableInput=table_input)
            print(f"  ✓ Created Glue table: {database_name}.{table_name}")
        except glue_client.exceptions.AlreadyExistsException:
            print(f"  ⊙ Glue table already exists: {database_name}.{table_name}")

        upload_parquet_to_s3(sample_data, s3_base_path, aws_region)

    return result


def _get_exotic_partition_path(
    scenario: HiveTableScenario,
    partition: PartitionLocation,
    method: str,
    bucket: str,
) -> str:
    """Generate partition path that replicates exotic layout for conversion testing.

    For exotic scenarios (scattered, cross_bucket, cross_region, shared), we need
    to replicate the exotic partition layout so we can test whether each conversion
    method handles it correctly.

    Args:
        scenario: The scenario being replicated
        partition: Original partition definition
        method: Conversion method
        bucket: Primary bucket

    Returns:
        S3 path for the partition data
    """
    base_path = get_conversion_s3_path(scenario.base_name, method, bucket)

    if scenario.base_name == "scattered":
        # Scattered: some partitions outside table root
        if partition.partition_value == "us-west":
            # This partition is "external" - put it outside table root
            return f"s3://{bucket}/conversion_eval/{scenario.base_name}_{method}_external/{partition.partition_key}={partition.partition_value}/"
        else:
            return f"{base_path}{partition.partition_key}={partition.partition_value}/"

    elif scenario.base_name == "cross_bucket":
        # Cross-bucket: partitions in different buckets
        if partition.partition_value == "us-west":
            # Put in second bucket
            return f"s3://{BUCKET_EAST_1B}/conversion_eval/{scenario.base_name}/{method}/{partition.partition_key}={partition.partition_value}/"
        else:
            return f"{base_path}{partition.partition_key}={partition.partition_value}/"

    elif scenario.base_name == "cross_region":
        # Cross-region: partitions in different AWS regions
        if partition.partition_value == "us-west":
            # Put in west-2 bucket
            return f"s3://{BUCKET_WEST_2}/conversion_eval/{scenario.base_name}/{method}/{partition.partition_key}={partition.partition_value}/"
        else:
            return f"{base_path}{partition.partition_key}={partition.partition_value}/"

    elif scenario.base_name in ("shared_a", "shared_b"):
        # Shared: both tables share one partition location
        if partition.partition_value == "shared":
            # Shared partition - same path for both tables
            return f"s3://{bucket}/conversion_eval/shared_data/{method}/{partition.partition_key}={partition.partition_value}/"
        else:
            return f"{base_path}{partition.partition_key}={partition.partition_value}/"

    else:
        # Standard layout
        return f"{base_path}{partition.partition_key}={partition.partition_value}/"


def _add_glue_partition(
    table_name: str,
    database_name: str,
    partition_values: dict[str, str],
    location: str,
    aws_region: str = "us-east-1",
) -> None:
    """Add partition to Glue table."""
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

    try:
        glue_client.create_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionInput=partition_input,
        )
        print(f"  ✓ Added partition {partition_values} at {location}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"  ⊙ Partition already exists: {partition_values}")


def setup_conversion_source_tables(
    scenarios: list[str] | None = None,
    methods: list[str] | None = None,
    database_name: str = DEFAULT_DATABASE,
    bucket: str = DEFAULT_BUCKET,
    aws_region: str = "us-east-1",
) -> dict[str, dict[str, ConversionSourceTable]]:
    """Create all Glue source tables for conversion testing.

    Creates 7 scenarios × 3 methods = 21 independent tables.

    Args:
        scenarios: Scenario base names to create (default: all 7)
        methods: Conversion methods to create (default: all 3)
        database_name: Glue database name
        bucket: Primary S3 bucket
        aws_region: AWS region

    Returns:
        Dict mapping scenario -> method -> ConversionSourceTable
    """
    if scenarios is None:
        scenarios = [s.base_name for s in ALL_SCENARIOS]
    if methods is None:
        methods = CONVERSION_METHODS

    print("=" * 70)
    print("Setting up Conversion Source Tables")
    print(f"Scenarios: {len(scenarios)} | Methods: {len(methods)}")
    print(f"Total tables: {len(scenarios) * len(methods)}")
    print("=" * 70)

    # Create database
    create_glue_database(database_name, aws_region)

    results: dict[str, dict[str, ConversionSourceTable]] = {}

    for scenario_name in scenarios:
        scenario = SCENARIO_BY_BASE_NAME.get(scenario_name)
        if not scenario:
            print(f"\n⚠ Unknown scenario: {scenario_name}, skipping")
            continue

        print(f"\n{'─' * 50}")
        print(f"Scenario: {scenario_name}")
        print(f"{'─' * 50}")

        results[scenario_name] = {}

        for method in methods:
            print(f"\n  Method: {method}")
            table_info = create_conversion_source_table(
                scenario, method, database_name, bucket, aws_region
            )
            results[scenario_name][method] = table_info

    print("\n" + "=" * 70)
    print("Conversion Source Tables Complete!")
    print(f"Created {sum(len(m) for m in results.values())} tables")
    print("=" * 70)

    return results


def get_conversion_source_info(
    scenario: str,
    method: str,
    database_name: str = DEFAULT_DATABASE,
    bucket: str = DEFAULT_BUCKET,
) -> dict[str, Any]:
    """Get info about a conversion source table.

    Args:
        scenario: Scenario base name
        method: Conversion method
        database_name: Glue database
        bucket: S3 bucket

    Returns:
        Dict with table_name, s3_path, fully_qualified name
    """
    table_name = get_conversion_table_name(scenario, method)
    s3_path = get_conversion_s3_path(scenario, method, bucket)

    return {
        "scenario": scenario,
        "method": method,
        "table_name": table_name,
        "s3_path": s3_path,
        "database": database_name,
        "fully_qualified": f"{database_name}.{table_name}",
        "glue_catalog_table": f"glue_catalog.{database_name}.{table_name}",
    }


def list_all_conversion_sources(
    scenarios: list[str] | None = None,
    methods: list[str] | None = None,
) -> list[dict[str, Any]]:
    """List all expected conversion source tables.

    Args:
        scenarios: Scenario names (default: all)
        methods: Method names (default: all)

    Returns:
        List of table info dicts
    """
    if scenarios is None:
        scenarios = [s.base_name for s in ALL_SCENARIOS]
    if methods is None:
        methods = CONVERSION_METHODS

    result = []
    for scenario in scenarios:
        for method in methods:
            result.append(get_conversion_source_info(scenario, method))
    return result


if __name__ == "__main__":
    # Run fixture setup when executed directly
    setup_conversion_source_tables()
