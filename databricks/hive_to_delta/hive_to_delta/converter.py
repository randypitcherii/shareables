"""Main converter module for hive_to_delta package.

Provides the primary interface for converting Hive tables (via AWS Glue)
to Delta tables registered in Unity Catalog.

Key functions:
- convert_single_table: Convert one Hive table to Delta
- convert_tables: Convert multiple tables in parallel
"""

import time
from typing import Union

from hive_to_delta.models import ConversionResult
from hive_to_delta.glue import (
    get_glue_table_metadata,
    get_glue_partitions,
    list_glue_tables,
)
from hive_to_delta.s3 import scan_partition_files
from hive_to_delta.delta_log import (
    build_delta_schema,
    generate_delta_log,
    write_delta_log,
)
from hive_to_delta.parallel import run_parallel


def convert_single_table(
    spark,
    glue_database: str,
    table_name: str,
    target_catalog: str,
    target_schema: str,
    aws_region: str = "us-east-1",
    target_table_name: str = None,
) -> ConversionResult:
    """Convert a single Hive table to Delta and register in Unity Catalog.

    This function performs the full conversion workflow:
    1. Fetch table metadata from AWS Glue
    2. Fetch partition information from Glue
    3. Scan S3 for parquet files in each partition
    4. Generate Delta transaction log
    5. Write Delta log to S3
    6. Register table in Unity Catalog

    Args:
        spark: Active Spark session with Unity Catalog access.
        glue_database: Name of the Glue database containing the source table.
        table_name: Name of the source table in Glue.
        target_catalog: Unity Catalog catalog for the target table.
        target_schema: Unity Catalog schema for the target table.
        aws_region: AWS region for Glue/S3 operations.
        target_table_name: Optional override for the target table name.
            If not provided, uses the source table_name.

    Returns:
        ConversionResult with success/failure details, file count,
        delta log location, and duration.

    Example:
        >>> result = convert_single_table(
        ...     spark=spark,
        ...     glue_database="my_glue_db",
        ...     table_name="events",
        ...     target_catalog="analytics",
        ...     target_schema="delta_tables",
        ... )
        >>> if result.success:
        ...     print(f"Converted {result.file_count} files in {result.duration_seconds:.2f}s")
    """
    start_time = time.perf_counter()
    final_table_name = target_table_name or table_name
    target_table = f"{target_catalog}.{target_schema}.{final_table_name}"

    try:
        # Step 1: Fetch Glue table metadata
        metadata = get_glue_table_metadata(glue_database, table_name, aws_region)
        table_location = metadata["StorageDescriptor"]["Location"].rstrip("/")

        # Extract column info
        data_columns = metadata["StorageDescriptor"].get("Columns", [])
        partition_keys = metadata.get("PartitionKeys", [])
        partition_col_names = [col["Name"] for col in partition_keys]

        # Combine data columns and partition columns for schema
        all_columns = data_columns + partition_keys

        # Step 2: Fetch partition information
        partitions = get_glue_partitions(glue_database, table_name, aws_region)

        # Build partition locations list for scanning
        if partitions:
            # Partitioned table - build list of (location, partition_values)
            partition_locations = []
            for partition in partitions:
                location = partition["StorageDescriptor"]["Location"]
                values = partition["Values"]
                partition_values = dict(zip(partition_col_names, values))
                partition_locations.append((location, partition_values))
        else:
            # Non-partitioned table - scan table root
            partition_locations = [(table_location, {})]

        # Step 3: Scan S3 for parquet files
        files = scan_partition_files(partition_locations, aws_region)

        if not files:
            duration = time.perf_counter() - start_time
            return ConversionResult(
                source_table=table_name,
                target_table=target_table,
                success=False,
                file_count=0,
                delta_log_location="",
                error="No parquet files found in table or partitions",
                duration_seconds=duration,
            )

        # Step 4: Build Delta schema
        schema = build_delta_schema(all_columns)

        # Step 5: Generate Delta transaction log
        delta_log_content = generate_delta_log(
            files=files,
            schema=schema,
            partition_columns=partition_col_names,
            table_location=table_location,
        )

        # Step 6: Write Delta log to S3
        delta_log_path = write_delta_log(
            delta_log_content,
            table_location,
            aws_region,
        )

        # Step 7: Ensure schema exists and register table in Unity Catalog
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")
        spark.sql(f"""
            CREATE TABLE {target_table}
            USING DELTA
            LOCATION '{table_location}'
        """)

        duration = time.perf_counter() - start_time
        return ConversionResult(
            source_table=table_name,
            target_table=target_table,
            success=True,
            file_count=len(files),
            delta_log_location=delta_log_path,
            error=None,
            duration_seconds=duration,
        )

    except Exception as e:
        duration = time.perf_counter() - start_time
        return ConversionResult(
            source_table=table_name,
            target_table=target_table,
            success=False,
            file_count=0,
            delta_log_location="",
            error=str(e),
            duration_seconds=duration,
        )


def convert_tables(
    spark,
    glue_database: str,
    tables: Union[list[str], str],
    target_catalog: str,
    target_schema: str,
    aws_region: str = "us-east-1",
    max_workers: int = 4,
) -> list[ConversionResult]:
    """Convert multiple Hive tables to Delta in parallel.

    Accepts either an explicit list of table names or a glob pattern
    to match tables in the Glue database.

    Args:
        spark: Active Spark session with Unity Catalog access.
        glue_database: Name of the Glue database containing the source tables.
        tables: Either a list of table names or a glob pattern string
            (e.g., "sales_*", "*_fact", "dim_*"). Patterns support
            * (any chars), ? (single char), [seq], [!seq].
        target_catalog: Unity Catalog catalog for the target tables.
        target_schema: Unity Catalog schema for the target tables.
        aws_region: AWS region for Glue/S3 operations.
        max_workers: Maximum number of concurrent conversion workers.

    Returns:
        List of ConversionResult for each table (in completion order).

    Example:
        >>> # Convert specific tables
        >>> results = convert_tables(
        ...     spark=spark,
        ...     glue_database="analytics",
        ...     tables=["orders", "customers", "products"],
        ...     target_catalog="main",
        ...     target_schema="bronze",
        ... )

        >>> # Convert tables matching a pattern
        >>> results = convert_tables(
        ...     spark=spark,
        ...     glue_database="analytics",
        ...     tables="dim_*",
        ...     target_catalog="main",
        ...     target_schema="bronze",
        ...     max_workers=8,
        ... )

        >>> # Check results
        >>> successful = [r for r in results if r.success]
        >>> failed = [r for r in results if not r.success]
        >>> print(f"Converted {len(successful)}/{len(results)} tables")
    """
    # Expand pattern to table list if string provided
    if isinstance(tables, str):
        table_list = list_glue_tables(glue_database, pattern=tables, region=aws_region)
        if not table_list:
            print(f"No tables found matching pattern '{tables}' in {glue_database}")
            return []
        print(f"Found {len(table_list)} tables matching pattern '{tables}'")
    else:
        table_list = tables

    if not table_list:
        return []

    # Create conversion function that captures spark and config
    def convert_table(table_name: str) -> ConversionResult:
        return convert_single_table(
            spark=spark,
            glue_database=glue_database,
            table_name=table_name,
            target_catalog=target_catalog,
            target_schema=target_schema,
            aws_region=aws_region,
        )

    # Run conversions in parallel
    results = run_parallel(convert_table, table_list, max_workers=max_workers)

    # Convert any exceptions to failed ConversionResult
    final_results: list[ConversionResult] = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            # This shouldn't happen often since convert_single_table catches exceptions
            # but handle it just in case
            table_name = table_list[i] if i < len(table_list) else "unknown"
            final_results.append(
                ConversionResult(
                    source_table=table_name,
                    target_table=f"{target_catalog}.{target_schema}.{table_name}",
                    success=False,
                    error=str(result),
                )
            )
        else:
            final_results.append(result)

    return final_results
