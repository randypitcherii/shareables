"""Main converter module for hive_to_delta package.

Provides a two-tier API for converting Hive tables to Delta:
- convert_table: Tier 1 simple API (single table from a DataFrame of files)
- convert: Tier 2 composable bulk API (discovery + listing strategies)

Plus legacy backward-compatible wrappers:
- convert_single_table: Convert one Hive table via Glue metadata
- convert_tables: Convert multiple tables in parallel via Glue
"""

import time
from typing import Any, Optional, Union

from hive_to_delta.delta_log import generate_delta_log, write_delta_log
from hive_to_delta.listing import Listing, _parse_partition_values, validate_files_df
from hive_to_delta.models import ConversionResult, ParquetFileInfo, TableInfo
from hive_to_delta.parallel import ConversionSummary, create_summary, run_parallel
from hive_to_delta.schema import build_delta_schema_from_glue, build_delta_schema_from_spark


# =============================================================================
# Shared internal pipeline
# =============================================================================


def _convert_one_table(
    spark,
    table_info: TableInfo,
    files: list[ParquetFileInfo],
    target_catalog: str,
    target_schema: str,
    aws_region: str = "us-east-1",
) -> ConversionResult:
    """Shared conversion pipeline for a single table.

    Handles schema inference, delta log generation/writing, and UC registration.

    Args:
        spark: Active Spark session with Unity Catalog access.
        table_info: Metadata about the table to convert.
        files: List of parquet files belonging to the table.
        target_catalog: Unity Catalog catalog for the target table.
        target_schema: Unity Catalog schema for the target table.
        aws_region: AWS region for S3 operations.

    Returns:
        ConversionResult with success/failure details.
    """
    start_time = time.perf_counter()
    final_name = table_info.target_table_name or table_info.name
    target_table = f"{target_catalog}.{target_schema}.{final_name}"

    if not files:
        duration = time.perf_counter() - start_time
        return ConversionResult(
            source_table=table_info.name,
            target_table=target_table,
            success=False,
            file_count=0,
            delta_log_location="",
            error="No parquet files found for table",
            duration_seconds=duration,
        )

    try:
        # Schema inference
        if table_info.columns is not None:
            schema = build_delta_schema_from_glue(table_info.columns)
        else:
            spark_schema = spark.read.parquet(files[0].path).schema
            schema = build_delta_schema_from_spark(spark_schema)

        # Generate and write delta log
        delta_log_content = generate_delta_log(
            files=files,
            schema=schema,
            partition_columns=table_info.partition_keys,
            table_location=table_info.location,
        )
        delta_log_path = write_delta_log(
            delta_log_content, table_info.location, aws_region
        )

        # Register in Unity Catalog
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")
        spark.sql(
            f"CREATE TABLE {target_table} USING DELTA LOCATION '{table_info.location}'"
        )

        duration = time.perf_counter() - start_time
        return ConversionResult(
            source_table=table_info.name,
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
            source_table=table_info.name,
            target_table=target_table,
            success=False,
            file_count=0,
            delta_log_location="",
            error=str(e),
            duration_seconds=duration,
        )


# =============================================================================
# Tier 1: Simple API
# =============================================================================


def convert_table(
    spark,
    files_df,
    table_location: str,
    target_catalog: str,
    target_schema: str,
    target_table: str,
    partition_columns: Optional[list[str]] = None,
    aws_region: str = "us-east-1",
) -> ConversionResult:
    """Convert a single table from a DataFrame of file paths.

    Tier 1 API: pass a Spark DataFrame with file_path and size columns,
    plus the target table coordinates, and get a ConversionResult back.

    Args:
        spark: Active Spark session with Unity Catalog access.
        files_df: Spark DataFrame with file_path (string) and size (int/bigint/long) columns.
        table_location: S3 root path for the table data.
        target_catalog: Unity Catalog catalog for the target table.
        target_schema: Unity Catalog schema for the target table.
        target_table: Name of the target table in Unity Catalog.
        partition_columns: Optional list of partition column names.
        aws_region: AWS region for S3 operations.

    Returns:
        ConversionResult with success/failure details.

    Raises:
        ValueError: If the DataFrame is missing required columns or has wrong types.
    """
    # Validate DataFrame schema
    validate_files_df(files_df)

    partition_keys = partition_columns or []
    table_info = TableInfo(
        name=target_table,
        location=table_location.rstrip("/"),
        partition_keys=partition_keys,
    )

    # Collect rows and build ParquetFileInfo list
    rows = files_df.collect()
    files: list[ParquetFileInfo] = []
    for row in rows:
        partition_values = (
            _parse_partition_values(row.file_path, partition_keys)
            if partition_keys
            else {}
        )
        files.append(
            ParquetFileInfo(
                path=row.file_path,
                size=row.size,
                partition_values=partition_values,
            )
        )

    return _convert_one_table(
        spark, table_info, files, target_catalog, target_schema, aws_region
    )


# =============================================================================
# Tier 2: Composable bulk API
# =============================================================================


def convert(
    spark,
    discovery,
    listing,
    target_catalog: str,
    target_schema: str,
    aws_region: str = "us-east-1",
    max_workers: int = 4,
    print_summary: bool = True,
) -> list[ConversionResult]:
    """Convert tables using pluggable discovery and listing strategies.

    Tier 2 API: pass Discovery and Listing strategy objects, and get
    back a list of ConversionResults for all discovered tables.

    Args:
        spark: Active Spark session with Unity Catalog access.
        discovery: A Discovery strategy that returns TableInfo objects.
        listing: A Listing strategy that returns ParquetFileInfo lists.
        target_catalog: Unity Catalog catalog for the target tables.
        target_schema: Unity Catalog schema for the target tables.
        aws_region: AWS region for S3 operations.
        max_workers: Maximum number of concurrent conversion workers.
        print_summary: If True, print a summary when converting multiple tables.

    Returns:
        List of ConversionResult for each discovered table.
    """
    tables = discovery.discover(spark)
    if not tables:
        return []

    # Collect files for each table
    table_files: list[tuple[TableInfo, list[ParquetFileInfo]]] = []
    for table in tables:
        files = listing.list_files(spark, table)
        table_files.append((table, files))

    # Single table: convert directly; multiple: use run_parallel
    if len(table_files) == 1:
        table_info, files = table_files[0]
        result = _convert_one_table(
            spark, table_info, files, target_catalog, target_schema, aws_region
        )
        return [result]

    # Multiple tables — run in parallel
    def _do_convert(pair: tuple[TableInfo, list[ParquetFileInfo]]) -> ConversionResult:
        tbl, fls = pair
        return _convert_one_table(
            spark, tbl, fls, target_catalog, target_schema, aws_region
        )

    raw_results = run_parallel(_do_convert, table_files, max_workers=max_workers)

    # Convert any exceptions to failed ConversionResult
    results: list[ConversionResult] = []
    for i, result in enumerate(raw_results):
        if isinstance(result, Exception):
            tbl_info = table_files[i][0] if i < len(table_files) else None
            name = tbl_info.name if tbl_info else "unknown"
            results.append(
                ConversionResult(
                    source_table=name,
                    target_table=f"{target_catalog}.{target_schema}.{name}",
                    success=False,
                    error=str(result),
                )
            )
        else:
            results.append(result)

    if print_summary and len(results) > 1:
        summary = create_summary(results)
        print(summary)

    return results


# =============================================================================
# Legacy wrappers (backward compatible)
# =============================================================================


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

    Legacy wrapper that uses GlueDiscovery + S3Listing internally.

    Args:
        spark: Active Spark session with Unity Catalog access.
        glue_database: Name of the Glue database containing the source table.
        table_name: Name of the source table in Glue.
        target_catalog: Unity Catalog catalog for the target table.
        target_schema: Unity Catalog schema for the target table.
        aws_region: AWS region for Glue/S3 operations.
        target_table_name: Optional override for the target table name.

    Returns:
        ConversionResult with success/failure details.
    """
    from hive_to_delta.discovery import GlueDiscovery
    from hive_to_delta.listing import S3Listing

    discovery = GlueDiscovery(
        database=glue_database, pattern=table_name, region=aws_region
    )
    tables = discovery.discover(spark)

    if not tables:
        return ConversionResult(
            source_table=table_name,
            target_table=f"{target_catalog}.{target_schema}.{target_table_name or table_name}",
            success=False,
            error=f"Table '{table_name}' not found in Glue database '{glue_database}'",
        )

    table_info = tables[0]
    if target_table_name:
        table_info.target_table_name = target_table_name

    listing = S3Listing(region=aws_region, glue_database=glue_database)
    files = listing.list_files(spark, table_info)

    return _convert_one_table(
        spark, table_info, files, target_catalog, target_schema, aws_region
    )


def convert_tables(
    spark,
    glue_database: str,
    tables: Union[list[str], str],
    target_catalog: str,
    target_schema: str,
    aws_region: str = "us-east-1",
    max_workers: int = 4,
    print_summary: bool = True,
) -> list[ConversionResult]:
    """Convert multiple Hive tables to Delta in parallel.

    Legacy wrapper that uses GlueDiscovery + S3Listing internally.

    Args:
        spark: Active Spark session with Unity Catalog access.
        glue_database: Name of the Glue database containing the source tables.
        tables: Either a list of table names or a glob pattern string.
        target_catalog: Unity Catalog catalog for the target tables.
        target_schema: Unity Catalog schema for the target tables.
        aws_region: AWS region for Glue/S3 operations.
        max_workers: Maximum number of concurrent conversion workers.
        print_summary: If True, print a summary of results after completion.

    Returns:
        List of ConversionResult for each table.
    """
    from hive_to_delta.discovery import GlueDiscovery
    from hive_to_delta.listing import S3Listing

    # Resolve table list
    if isinstance(tables, str):
        discovery = GlueDiscovery(
            database=glue_database, pattern=tables, region=aws_region
        )
        discovered = discovery.discover(spark)
        if not discovered:
            print(f"No tables found matching pattern '{tables}' in {glue_database}")
            return []
        print(f"Found {len(discovered)} tables matching pattern '{tables}'")
    else:
        # Explicit list — discover each table individually
        discovered = []
        for table_name in tables:
            discovery = GlueDiscovery(
                database=glue_database, pattern=table_name, region=aws_region
            )
            found = discovery.discover(spark)
            discovered.extend(found)

    if not discovered:
        return []

    listing = S3Listing(region=aws_region, glue_database=glue_database)

    # Collect files for each table
    table_files: list[tuple[TableInfo, list[ParquetFileInfo]]] = []
    for table_info in discovered:
        files = listing.list_files(spark, table_info)
        table_files.append((table_info, files))

    # Run conversions in parallel
    def _do_convert(pair: tuple[TableInfo, list[ParquetFileInfo]]) -> ConversionResult:
        tbl, fls = pair
        return _convert_one_table(
            spark, tbl, fls, target_catalog, target_schema, aws_region
        )

    raw_results = run_parallel(_do_convert, table_files, max_workers=max_workers)

    # Convert any exceptions to failed ConversionResult
    final_results: list[ConversionResult] = []
    for i, result in enumerate(raw_results):
        if isinstance(result, Exception):
            tbl_info = table_files[i][0] if i < len(table_files) else None
            name = tbl_info.name if tbl_info else "unknown"
            final_results.append(
                ConversionResult(
                    source_table=name,
                    target_table=f"{target_catalog}.{target_schema}.{name}",
                    success=False,
                    error=str(result),
                )
            )
        else:
            final_results.append(result)

    if print_summary:
        summary = create_summary(final_results)
        print(summary)

    return final_results
