"""Test data generation and multi-access-method table creation utilities.

Supports creating fixtures for three access methods:
1. glue_as_hive_metastore (glue_hive) - Spark's built-in Glue integration
2. uc_glue_federation (glue_fed) - UC foreign catalog pointing to Glue
3. uc_external_tables (uc_ext, uc_delta) - Manual UC external tables

Each access method gets separate S3 paths to avoid conflicts:
- s3://bucket/tables/glue_hive/{scenario}/ - For Glue-based access
- s3://bucket/tables/uc_ext/{scenario}/ - For UC external (Parquet)
- s3://bucket/tables/uc_delta/{scenario}/ - For UC external (Delta)
"""

import io
import os
from typing import Any, Dict

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from .access_methods import (
    GLUE_AS_HIVE,
    UC_EXTERNAL_DELTA,
    UC_EXTERNAL_HIVE,
    UC_GLUE_FEDERATION,
    AccessMethodConfig,
    ALL_ACCESS_METHODS,
)
from .scenarios import (
    ALL_SCENARIOS,
    BASIC_LAYOUTS,
    BUCKET_EAST_1A,
    EXTERNAL_PARTITIONS,
    MULTI_LOCATION,
    HiveTableScenario,
)

# Default bucket for fixtures (uses environment variable via scenarios.py)
DEFAULT_BUCKET = BUCKET_EAST_1A
DEFAULT_DATABASE = os.getenv("HIVE_EVAL_GLUE_DATABASE", "your_glue_database")


def generate_sample_data(num_rows: int = 10) -> pa.Table:
    """
    Generate minimal synthetic data for testing.

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
    """
    Upload PyArrow table as Parquet to S3.

    Args:
        table: PyArrow table to upload
        s3_path: Full S3 path (s3://bucket/key)
        aws_region: AWS region
    """
    # Parse S3 path
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")

    parts = s3_path[5:].split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""

    # Add .parquet extension if not present
    if not key.endswith(".parquet"):
        # Strip trailing slash before adding data.parquet to avoid double-slash paths
        key = key.rstrip("/")
        key = f"{key}/data.parquet"

    # Write to buffer
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # Upload to S3
    s3_client = boto3.client("s3", region_name=aws_region)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())

    print(f"✓ Uploaded {len(table)} rows to s3://{bucket}/{key}")


def create_glue_database(
    database_name: str,
    aws_region: str = "us-east-1",
) -> None:
    """
    Create Glue database if it doesn't exist.

    Args:
        database_name: Name of the database
        aws_region: AWS region
    """
    glue_client = boto3.client("glue", region_name=aws_region)

    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": database_name,
                "Description": "Hive migration compatibility validation test database",
            }
        )
        print(f"✓ Created Glue database: {database_name}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"⊙ Glue database already exists: {database_name}")


def create_glue_table(
    scenario: HiveTableScenario,
    database_name: str,
    aws_region: str = "us-east-1",
) -> None:
    """
    Create Glue table for a scenario.

    Args:
        scenario: Table scenario definition
        database_name: Glue database name
        aws_region: AWS region
    """
    glue_client = boto3.client("glue", region_name=aws_region)

    # Define schema
    columns = [
        {"Name": "id", "Type": "bigint"},
        {"Name": "name", "Type": "string"},
        {"Name": "value", "Type": "double"},
        {"Name": "is_active", "Type": "boolean"},
    ]

    partition_keys = []
    if scenario.partitions and not scenario.recursive_scan_required:
        # Extract unique partition keys
        partition_key_names = list(set(p.partition_key for p in scenario.partitions))
        partition_keys = [{"Name": key, "Type": "string"} for key in partition_key_names]

    table_input = {
        "Name": scenario.name,
        "Description": scenario.description,
        "StorageDescriptor": {
            "Columns": columns,
            "Location": scenario.table_location,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            },
        },
        "PartitionKeys": partition_keys,
        "TableType": "EXTERNAL_TABLE",
    }

    try:
        glue_client.create_table(
            DatabaseName=database_name,
            TableInput=table_input,
        )
        print(f"✓ Created Glue table: {database_name}.{scenario.name}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"⊙ Glue table already exists: {database_name}.{scenario.name}")


def add_glue_partition(
    table_name: str,
    database_name: str,
    partition_values: Dict[str, str],
    location: str,
    aws_region: str = "us-east-1",
) -> None:
    """
    Add partition to Glue table.

    Args:
        table_name: Table name
        database_name: Database name
        partition_values: Partition key-value pairs
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

    try:
        glue_client.create_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionInput=partition_input,
        )
        print(f"✓ Added partition {partition_values} to {database_name}.{table_name}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"⊙ Partition already exists: {partition_values}")


def setup_basic_layout_fixtures(
    database_name: str = DEFAULT_DATABASE,
    aws_region: str = "us-east-1",
) -> None:
    """
    Set up Basic Layout test fixtures (database, tables, data).

    Args:
        database_name: Glue database name
        aws_region: AWS region
    """
    print("=" * 60)
    print("Setting up Basic Layout fixtures")
    print("=" * 60)

    # Create database
    create_glue_database(database_name, aws_region)

    # Generate sample data
    sample_data = generate_sample_data(10)

    for scenario in BASIC_LAYOUTS:
        print(f"\n--- Scenario: {scenario.name} ---")

        # Create Glue table
        create_glue_table(scenario, database_name, aws_region)

        if scenario.partitions:
            # Upload data for each partition
            for partition in scenario.partitions:
                upload_parquet_to_s3(sample_data, partition.s3_path, aws_region)
                add_glue_partition(
                    scenario.name,
                    database_name,
                    {partition.partition_key: partition.partition_value},
                    partition.s3_path,
                    aws_region,
                )
        elif scenario.recursive_scan_required:
            # For recursive scenario, create nested directory structure
            nested_paths = [
                f"{scenario.table_location}subdir1/data.parquet",
                f"{scenario.table_location}subdir1/subdir2/data.parquet",
                f"{scenario.table_location}subdir3/data.parquet",
            ]
            for path in nested_paths:
                upload_parquet_to_s3(sample_data, path, aws_region)
        else:
            # Non-partitioned table
            upload_parquet_to_s3(sample_data, scenario.table_location, aws_region)

    print("\n" + "=" * 60)
    print("Basic Layout fixtures setup complete!")
    print("=" * 60)


def setup_external_and_multi_location_fixtures(
    database_name: str = DEFAULT_DATABASE,
    aws_region: str = "us-east-1",
) -> None:
    """
    Set up External Partitions and Multi-Location test fixtures.

    Tests partitions outside table root and across buckets/regions.

    Args:
        database_name: Glue database name
        aws_region: AWS region
    """
    print("=" * 60)
    print("Setting up External Partitions and Multi-Location fixtures")
    print("=" * 60)

    # Create database
    create_glue_database(database_name, aws_region)

    # Generate sample data
    sample_data = generate_sample_data(10)

    for scenario in EXTERNAL_PARTITIONS + MULTI_LOCATION:
        print(f"\n--- Scenario: {scenario.name} ---")

        # Create Glue table
        create_glue_table(scenario, database_name, aws_region)

        # Upload data to each partition location
        for partition in scenario.partitions:
            upload_parquet_to_s3(sample_data, partition.s3_path, aws_region)
            add_glue_partition(
                scenario.name,
                database_name,
                {partition.partition_key: partition.partition_value},
                partition.s3_path,
                aws_region,
            )

    print("\n" + "=" * 60)
    print("External Partitions and Multi-Location fixtures setup complete!")
    print("=" * 60)


# =============================================================================
# Multi-Access-Method Fixture Setup
# =============================================================================


def setup_glue_table_for_access_method(
    scenario: HiveTableScenario,
    access_config: AccessMethodConfig,
    database_name: str = DEFAULT_DATABASE,
    bucket: str = DEFAULT_BUCKET,
    aws_region: str = "us-east-1",
) -> dict[str, Any]:
    """Create a Glue table for a specific access method and scenario.

    For glue_hive and glue_fed, creates Glue tables at method-specific S3 paths.
    For uc_ext and uc_delta, only uploads S3 data (UC table creation done separately).

    Args:
        scenario: The table scenario to create
        access_config: Access method configuration
        database_name: Glue database name
        bucket: S3 bucket for data
        aws_region: AWS region

    Returns:
        Dict with table_name, s3_path, and partitions info
    """
    table_name = scenario.get_table_name(access_config.table_suffix)
    s3_base_path = scenario.get_s3_path_for_method(bucket, access_config.s3_path_prefix)

    result = {
        "table_name": table_name,
        "s3_path": s3_base_path,
        "access_method": access_config.method.value,
        "partitions": [],
    }

    # Generate sample data
    sample_data = generate_sample_data(10)

    # Only create Glue tables for Glue-based access methods
    if access_config.method.value in ("glue_hive", "glue_fed"):
        # Create modified scenario with method-specific paths
        method_partitions = []
        for p in scenario.partitions:
            method_partition_path = f"{s3_base_path}{p.partition_key}={p.partition_value}/"
            method_partitions.append(type(p)(p.partition_key, p.partition_value, method_partition_path))

        # Create a temporary scenario with updated paths
        from dataclasses import replace
        method_scenario = replace(
            scenario,
            name=table_name,
            table_location=s3_base_path,
            partitions=method_partitions,
        )

        # Create Glue table
        create_glue_table(method_scenario, database_name, aws_region)

        # Upload data and add partitions
        if method_partitions:
            for partition in method_partitions:
                upload_parquet_to_s3(sample_data, partition.s3_path, aws_region)
                add_glue_partition(
                    table_name,
                    database_name,
                    {partition.partition_key: partition.partition_value},
                    partition.s3_path,
                    aws_region,
                )
                result["partitions"].append({
                    "key": partition.partition_key,
                    "value": partition.partition_value,
                    "path": partition.s3_path,
                })
        elif scenario.recursive_scan_required:
            # Nested directories
            nested_paths = [
                f"{s3_base_path}subdir1/data.parquet",
                f"{s3_base_path}subdir1/subdir2/data.parquet",
                f"{s3_base_path}subdir3/data.parquet",
            ]
            for path in nested_paths:
                upload_parquet_to_s3(sample_data, path, aws_region)
        else:
            # Non-partitioned
            upload_parquet_to_s3(sample_data, s3_base_path, aws_region)
    else:
        # For UC external methods, just upload S3 data (UC table creation done in Databricks)
        # UC doesn't support external partitions, so we consolidate into standard partition layout
        if scenario.partitions:
            for p in scenario.partitions:
                partition_path = f"{s3_base_path}{p.partition_key}={p.partition_value}/"
                upload_parquet_to_s3(sample_data, partition_path, aws_region)
                result["partitions"].append({
                    "key": p.partition_key,
                    "value": p.partition_value,
                    "path": partition_path,
                })
        elif scenario.recursive_scan_required:
            # Flatten nested structure for UC
            upload_parquet_to_s3(sample_data, f"{s3_base_path}data.parquet", aws_region)
        else:
            upload_parquet_to_s3(sample_data, s3_base_path, aws_region)

    return result


def setup_fixtures_for_access_method(
    access_config: AccessMethodConfig,
    scenarios: list[HiveTableScenario] | None = None,
    database_name: str = DEFAULT_DATABASE,
    bucket: str = DEFAULT_BUCKET,
    aws_region: str = "us-east-1",
) -> dict[str, Any]:
    """Set up all fixtures for a specific access method.

    Args:
        access_config: Access method configuration
        scenarios: List of scenarios to set up (default: all compatible scenarios)
        database_name: Glue database name
        bucket: S3 bucket for data
        aws_region: AWS region

    Returns:
        Dict mapping scenario base_name to table info
    """
    from .scenarios import get_scenarios_for_access_method

    if scenarios is None:
        scenarios = get_scenarios_for_access_method(access_config)

    print("=" * 60)
    print(f"Setting up fixtures for: {access_config.method.value}")
    print(f"Scenarios: {len(scenarios)}")
    print("=" * 60)

    # Create database for Glue-based methods
    if access_config.method.value in ("glue_hive", "glue_fed"):
        create_glue_database(database_name, aws_region)

    results = {}
    for scenario in scenarios:
        supported, reason = scenario.supports_access_method(access_config)
        if not supported:
            print(f"\n⊘ Skipping {scenario.base_name}: {reason}")
            continue

        print(f"\n--- Scenario: {scenario.base_name} ---")
        result = setup_glue_table_for_access_method(
            scenario, access_config, database_name, bucket, aws_region
        )
        results[scenario.base_name] = result

    print("\n" + "=" * 60)
    print(f"{access_config.method.value} fixtures complete: {len(results)} tables")
    print("=" * 60)

    return results


def setup_all_access_methods(
    scenarios: list[HiveTableScenario] | None = None,
    database_name: str = DEFAULT_DATABASE,
    bucket: str = DEFAULT_BUCKET,
    aws_region: str = "us-east-1",
) -> dict[str, dict[str, Any]]:
    """Set up fixtures for all access methods.

    Creates all tables across all access methods:
    - glue_hive: Creates Glue tables at s3://bucket/tables/glue_hive/
    - glue_fed: Creates Glue tables at s3://bucket/tables/glue_fed/
    - uc_ext: Uploads data to s3://bucket/tables/uc_ext/ (UC table creation separate)
    - uc_delta: Uploads data to s3://bucket/tables/uc_delta/ (Delta conversion separate)

    Args:
        scenarios: List of scenarios (default: all scenarios)
        database_name: Glue database name
        bucket: S3 bucket
        aws_region: AWS region

    Returns:
        Dict mapping access_method -> scenario -> table_info
    """
    if scenarios is None:
        scenarios = ALL_SCENARIOS

    all_results = {}

    for access_config in ALL_ACCESS_METHODS:
        results = setup_fixtures_for_access_method(
            access_config,
            scenarios=scenarios,
            database_name=database_name,
            bucket=bucket,
            aws_region=aws_region,
        )
        all_results[access_config.method.value] = results

    print("\n" + "=" * 60)
    print("ALL ACCESS METHODS COMPLETE")
    for method, results in all_results.items():
        print(f"  {method}: {len(results)} tables")
    print("=" * 60)

    return all_results


def get_table_info_for_scenario(
    scenario_base_name: str,
    access_suffix: str,
    bucket: str = DEFAULT_BUCKET,
) -> dict[str, str]:
    """Get table info for a specific scenario and access method.

    Useful for querying the correct table in tests and notebooks.

    Args:
        scenario_base_name: Scenario name like "standard", "scattered"
        access_suffix: Access method suffix like "_glue_hive", "_uc_delta"
        bucket: S3 bucket

    Returns:
        Dict with table_name, catalog, schema, fully_qualified, s3_path
    """
    from .access_methods import ACCESS_METHOD_BY_SUFFIX
    from .scenarios import get_scenario_by_base_name

    scenario = get_scenario_by_base_name(scenario_base_name)
    if not scenario:
        raise ValueError(f"Unknown scenario: {scenario_base_name}")

    access_config = ACCESS_METHOD_BY_SUFFIX.get(access_suffix)
    if not access_config:
        raise ValueError(f"Unknown access suffix: {access_suffix}")

    table_name = scenario.get_table_name(access_suffix)
    s3_path = scenario.get_s3_path_for_method(bucket, access_config.s3_path_prefix)

    return {
        "table_name": table_name,
        "catalog": access_config.catalog,
        "schema": access_config.schema,
        "fully_qualified": access_config.get_fully_qualified_table(scenario.base_name),
        "s3_path": s3_path,
        "access_method": access_config.method.value,
    }


# Backward-compatible aliases (deprecated)
def setup_phase_1_fixtures(*args, **kwargs):
    """Deprecated: Use setup_basic_layout_fixtures instead"""
    return setup_basic_layout_fixtures(*args, **kwargs)


def setup_phase_2_fixtures(*args, **kwargs):
    """Deprecated: Use setup_external_and_multi_location_fixtures instead"""
    return setup_external_and_multi_location_fixtures(*args, **kwargs)


if __name__ == "__main__":
    setup_basic_layout_fixtures()
