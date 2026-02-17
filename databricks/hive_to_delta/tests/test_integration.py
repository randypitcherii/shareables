"""Integration tests for hive_to_delta composable pipeline.

These tests validate end-to-end flows with realistic mock compositions,
mocking only at external boundaries (boto3, spark) and letting the real
internal code (delta_log, schema, converter, listing, discovery) run.
"""

import json
from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

import pytest

from hive_to_delta.models import ConversionResult, ParquetFileInfo, TableInfo
from hive_to_delta.converter import convert_table, convert, convert_single_table, convert_tables
from hive_to_delta.listing import InventoryListing, _parse_partition_values


# =============================================================================
# Test helpers — realistic mock factories
# =============================================================================


def make_mock_spark(parquet_schema_fields=None):
    """Build a mock SparkSession that behaves realistically.

    Mocks spark.sql(), spark.read.parquet().schema, and spark.catalog.listColumns().
    The schema fields default to a simple (id bigint, name string) schema.
    """
    spark = MagicMock()

    # Build a realistic Spark schema for parquet inference
    if parquet_schema_fields is None:
        parquet_schema_fields = [
            SimpleNamespace(name="id", dataType=SimpleNamespace(simpleString=lambda: "bigint"), nullable=False),
            SimpleNamespace(name="name", dataType=SimpleNamespace(simpleString=lambda: "string"), nullable=True),
        ]

    mock_schema = SimpleNamespace(fields=parquet_schema_fields)
    spark.read.parquet.return_value.schema = mock_schema

    return spark


def make_mock_files_df(rows):
    """Build a mock Spark DataFrame with file_path (string) and size (bigint) columns.

    Args:
        rows: list of (file_path, size) tuples.
    """
    df = MagicMock()
    df.columns = ["file_path", "size"]
    df.dtypes = [("file_path", "string"), ("size", "bigint")]

    mock_rows = []
    for file_path, size in rows:
        row = SimpleNamespace(file_path=file_path, size=size)
        mock_rows.append(row)

    df.collect.return_value = mock_rows
    return df


def make_glue_table_response(name, location, columns, partitions=None):
    """Build a realistic Glue get_table response dict.

    Args:
        name: Table name.
        location: S3 location string.
        columns: list of dicts with Name/Type keys (data columns only).
        partitions: optional list of dicts with Name/Type keys (partition keys).
    """
    partition_keys = partitions or []
    return {
        "Table": {
            "Name": name,
            "StorageDescriptor": {
                "Location": location,
                "Columns": columns,
            },
            "PartitionKeys": partition_keys,
        }
    }


def make_glue_partition_response(values, location):
    """Build a single Glue partition dict."""
    return {
        "Values": values,
        "StorageDescriptor": {
            "Location": location,
        },
    }


def make_s3_list_response(files):
    """Build an S3 list_objects_v2 page response.

    Args:
        files: list of (key, size) tuples.
    """
    contents = [{"Key": key, "Size": size} for key, size in files]
    return {"Contents": contents}


# =============================================================================
# 1. convert_table() end-to-end (Tier 1)
# =============================================================================


class TestConvertTableIntegration:
    """End-to-end tests for the Tier 1 simple API.

    Mocks only boto3 (S3 write) and spark; lets delta_log, schema, and
    converter internals run for real.
    """

    @patch("hive_to_delta.delta_log.write_to_s3")
    def test_basic_successful_conversion(self, mock_write_s3):
        """A DataFrame with valid files produces a successful ConversionResult
        and a well-formed delta log is written to S3."""
        mock_write_s3.return_value = "s3://bucket/table/_delta_log/00000000000000000000.json"
        spark = make_mock_spark()

        files_df = make_mock_files_df([
            ("s3://bucket/table/part-0.parquet", 1000),
            ("s3://bucket/table/part-1.parquet", 2000),
        ])

        result = convert_table(
            spark, files_df, "s3://bucket/table",
            "test_cat", "test_sch", "my_table",
        )

        assert result.success is True
        assert result.source_table == "my_table"
        assert result.target_table == "test_cat.test_sch.my_table"
        assert result.file_count == 2
        assert result.duration_seconds > 0
        assert "delta_log" in result.delta_log_location

        # Verify delta log content written to S3
        mock_write_s3.assert_called_once()
        written_content = mock_write_s3.call_args[0][0]
        written_path = mock_write_s3.call_args[0][1]
        assert written_path.endswith("_delta_log/00000000000000000000.json")

        # Parse the delta log — should have protocol, metadata, and 2 add actions
        lines = written_content.strip().split("\n")
        assert len(lines) == 4  # protocol + metadata + 2 adds
        protocol = json.loads(lines[0])
        assert "protocol" in protocol
        metadata = json.loads(lines[1])
        assert "metaData" in metadata
        # Schema should contain the spark-inferred fields
        schema_str = metadata["metaData"]["schemaString"]
        schema = json.loads(schema_str)
        field_names = [f["name"] for f in schema["fields"]]
        assert "id" in field_names
        assert "name" in field_names

        add1 = json.loads(lines[2])
        add2 = json.loads(lines[3])
        assert "add" in add1
        assert "add" in add2

    @patch("hive_to_delta.delta_log.write_to_s3")
    def test_with_partition_columns(self, mock_write_s3):
        """Partition columns are parsed from file paths and appear in the delta log."""
        mock_write_s3.return_value = "s3://bucket/table/_delta_log/00000000000000000000.json"
        spark = make_mock_spark()

        files_df = make_mock_files_df([
            ("s3://bucket/table/year=2024/month=01/part-0.parquet", 500),
            ("s3://bucket/table/year=2024/month=02/part-0.parquet", 600),
        ])

        result = convert_table(
            spark, files_df, "s3://bucket/table",
            "test_cat", "test_sch", "partitioned_table",
            partition_columns=["year", "month"],
        )

        assert result.success is True
        assert result.file_count == 2

        # Verify delta log has partitionColumns in metadata
        written_content = mock_write_s3.call_args[0][0]
        lines = written_content.strip().split("\n")
        metadata = json.loads(lines[1])
        assert metadata["metaData"]["partitionColumns"] == ["year", "month"]

        # Verify add actions have partition values
        add1 = json.loads(lines[2])
        assert add1["add"]["partitionValues"] == {"year": "2024", "month": "01"}
        add2 = json.loads(lines[3])
        assert add2["add"]["partitionValues"] == {"year": "2024", "month": "02"}

    @patch("hive_to_delta.delta_log.write_to_s3")
    def test_files_from_multiple_directories(self, mock_write_s3):
        """Files from different subdirectories under the same table location
        all get included with relative paths in the delta log."""
        mock_write_s3.return_value = "s3://bucket/table/_delta_log/00000000000000000000.json"
        spark = make_mock_spark()

        files_df = make_mock_files_df([
            ("s3://bucket/table/dir_a/part-0.parquet", 100),
            ("s3://bucket/table/dir_b/part-0.parquet", 200),
            ("s3://bucket/table/dir_c/nested/part-0.parquet", 300),
        ])

        result = convert_table(
            spark, files_df, "s3://bucket/table",
            "test_cat", "test_sch", "multi_dir_table",
        )

        assert result.success is True
        assert result.file_count == 3

        written_content = mock_write_s3.call_args[0][0]
        lines = written_content.strip().split("\n")
        # 3 add actions
        add_actions = [json.loads(line) for line in lines[2:]]
        paths = [a["add"]["path"] for a in add_actions]
        # Paths should be relative (inside table root)
        assert "dir_a/part-0.parquet" in paths
        assert "dir_b/part-0.parquet" in paths
        assert "dir_c/nested/part-0.parquet" in paths

    def test_empty_dataframe_returns_failure(self):
        """A DataFrame with 0 rows produces a failed ConversionResult."""
        spark = make_mock_spark()
        files_df = make_mock_files_df([])  # empty

        result = convert_table(
            spark, files_df, "s3://bucket/table",
            "test_cat", "test_sch", "empty_table",
        )

        assert result.success is False
        assert "No parquet files found" in result.error
        assert result.file_count == 0


# =============================================================================
# 2. convert() with Discovery + Listing composition (Tier 2)
# =============================================================================


class TestConvertComposableIntegration:
    """End-to-end tests for Tier 2 composable API with real Discovery/Listing classes."""

    @patch("hive_to_delta.delta_log.write_to_s3")
    @patch("hive_to_delta.s3.get_s3_client")
    @patch("hive_to_delta.glue.get_glue_client")
    def test_glue_discovery_s3_listing(self, mock_glue_client_factory, mock_s3_client_factory, mock_write_s3):
        """GlueDiscovery + S3Listing: mock Glue API -> mock S3 scan -> verify ConversionResults."""
        from hive_to_delta.discovery import GlueDiscovery
        from hive_to_delta.listing import S3Listing

        # Mock Glue client
        glue_client = MagicMock()
        mock_glue_client_factory.return_value = glue_client

        # list_glue_tables paginator
        tables_paginator = MagicMock()
        tables_paginator.paginate.return_value = [
            {"TableList": [{"Name": "orders"}]}
        ]

        # get_table response
        glue_client.get_table.return_value = make_glue_table_response(
            "orders", "s3://data-bucket/orders",
            columns=[{"Name": "id", "Type": "bigint"}, {"Name": "amount", "Type": "double"}],
        )

        # get_paginator routes
        def paginator_router(operation):
            if operation == "get_tables":
                return tables_paginator
            elif operation == "get_partitions":
                # non-partitioned table — empty partitions
                p = MagicMock()
                p.paginate.return_value = [{"Partitions": []}]
                return p
            raise ValueError(f"Unexpected paginator: {operation}")

        glue_client.get_paginator.side_effect = paginator_router

        # Mock S3 client
        s3_client = MagicMock()
        mock_s3_client_factory.return_value = s3_client
        s3_paginator = MagicMock()
        s3_paginator.paginate.return_value = [
            make_s3_list_response([
                ("orders/part-00000.parquet", 5000),
                ("orders/part-00001.parquet", 6000),
            ])
        ]
        s3_client.get_paginator.return_value = s3_paginator

        mock_write_s3.return_value = "s3://data-bucket/orders/_delta_log/00000000000000000000.json"

        spark = make_mock_spark(parquet_schema_fields=[
            SimpleNamespace(name="id", dataType=SimpleNamespace(simpleString=lambda: "bigint"), nullable=False),
            SimpleNamespace(name="amount", dataType=SimpleNamespace(simpleString=lambda: "double"), nullable=True),
        ])

        discovery = GlueDiscovery(database="my_glue_db", pattern="orders", region="us-east-1")
        listing = S3Listing(region="us-east-1")

        results = convert(spark, discovery, listing, "cat", "sch", print_summary=False)

        assert len(results) == 1
        assert results[0].success is True
        assert results[0].source_table == "orders"
        assert results[0].file_count == 2

    @patch("hive_to_delta.delta_log.write_to_s3")
    def test_uc_discovery_inventory_listing(self, mock_write_s3):
        """UCDiscovery + InventoryListing: mock Spark SQL -> mock DataFrame -> verify."""
        from hive_to_delta.discovery import UCDiscovery

        mock_write_s3.return_value = "s3://bucket/t/_delta_log/00000000000000000000.json"

        spark = make_mock_spark()

        # UCDiscovery needs spark.sql() to return table lists and DESCRIBE output
        def sql_router(query):
            q = query.strip()
            result = MagicMock()

            if q.startswith("SHOW TABLES"):
                row = MagicMock()
                row.__getitem__ = lambda self, key: "users" if key == "tableName" else ""
                result.collect.return_value = [row]
            elif q.startswith("DESCRIBE TABLE"):
                # Return rows with col_name/data_type
                loc_row = MagicMock()
                loc_row.__getitem__ = lambda self, key: {
                    "col_name": "Location",
                    "data_type": "s3://bucket/users",
                }[key]
                result.collect.return_value = [loc_row]
            elif "CREATE SCHEMA" in q or "DROP TABLE" in q or "CREATE TABLE" in q:
                result.collect.return_value = []
            else:
                result.collect.return_value = []

            return result

        spark.sql.side_effect = sql_router

        # Mock catalog.listColumns for partition detection
        col_id = SimpleNamespace(name="id", isPartition=False)
        col_name = SimpleNamespace(name="name", isPartition=False)
        spark.catalog.listColumns.return_value = [col_id, col_name]

        # Build inventory DataFrame for InventoryListing
        inventory_df = make_mock_files_df([
            ("s3://bucket/users/part-0.parquet", 1000),
            ("s3://bucket/users/part-1.parquet", 2000),
            ("s3://bucket/other_table/part-0.parquet", 999),  # different table, should be filtered
        ])

        discovery = UCDiscovery(allow=["hive_metastore.mydb.*"])
        listing = InventoryListing(inventory_df)

        results = convert(spark, discovery, listing, "cat", "sch", print_summary=False)

        assert len(results) == 1
        assert results[0].success is True
        assert results[0].source_table == "users"
        assert results[0].file_count == 2  # only the 2 files under s3://bucket/users/

    @patch("hive_to_delta.delta_log.write_to_s3")
    @patch("hive_to_delta.s3.get_s3_client")
    @patch("hive_to_delta.glue.get_glue_client")
    def test_multiple_tables_partial_failure(self, mock_glue_client_factory, mock_s3_client_factory, mock_write_s3):
        """Multiple tables discovered, one fails (no files) -> partial results."""
        from hive_to_delta.discovery import GlueDiscovery
        from hive_to_delta.listing import S3Listing

        glue_client = MagicMock()
        mock_glue_client_factory.return_value = glue_client

        # Two tables discovered
        tables_paginator = MagicMock()
        tables_paginator.paginate.return_value = [
            {"TableList": [{"Name": "good_table"}, {"Name": "empty_table"}]}
        ]

        def get_table_router(**kwargs):
            name = kwargs["Name"]
            if name == "good_table":
                return make_glue_table_response(
                    "good_table", "s3://bucket/good_table",
                    columns=[{"Name": "id", "Type": "bigint"}],
                )
            else:
                return make_glue_table_response(
                    "empty_table", "s3://bucket/empty_table",
                    columns=[{"Name": "id", "Type": "bigint"}],
                )

        glue_client.get_table.side_effect = get_table_router

        def paginator_router(operation):
            if operation == "get_tables":
                return tables_paginator
            elif operation == "get_partitions":
                p = MagicMock()
                p.paginate.return_value = [{"Partitions": []}]
                return p
            raise ValueError(f"Unexpected paginator: {operation}")

        glue_client.get_paginator.side_effect = paginator_router

        # S3 client — good_table has files, empty_table has none
        s3_client = MagicMock()
        mock_s3_client_factory.return_value = s3_client

        s3_call_count = 0
        def s3_paginator_factory():
            nonlocal s3_call_count
            pag = MagicMock()
            s3_call_count += 1
            if s3_call_count == 1:
                # First call: good_table has files
                pag.paginate.return_value = [
                    make_s3_list_response([("good_table/part-0.parquet", 1000)])
                ]
            else:
                # Second call: empty_table has no parquet files
                pag.paginate.return_value = [{"Contents": []}]
            return pag

        s3_client.get_paginator.side_effect = lambda op: s3_paginator_factory()

        mock_write_s3.return_value = "s3://bucket/good_table/_delta_log/00000000000000000000.json"

        spark = make_mock_spark()

        discovery = GlueDiscovery(database="mydb", pattern="*_table", region="us-east-1")
        listing = S3Listing(region="us-east-1")

        results = convert(spark, discovery, listing, "cat", "sch", print_summary=False)

        assert len(results) == 2
        successes = [r for r in results if r.success]
        failures = [r for r in results if not r.success]
        assert len(successes) == 1
        assert len(failures) == 1
        assert failures[0].error == "No parquet files found for table"


# =============================================================================
# 3. convert() parallel execution
# =============================================================================


class TestParallelExecution:
    """Tests that parallel execution handles multiple tables and errors correctly."""

    @patch("hive_to_delta.delta_log.write_to_s3")
    def test_multiple_tables_all_processed(self, mock_write_s3):
        """Multiple tables via convert() with InventoryListing -> all get processed."""
        mock_write_s3.return_value = "s3://bucket/t/_delta_log/00000000000000000000.json"
        spark = make_mock_spark()

        inventory_df = make_mock_files_df([
            ("s3://bucket/t1/part-0.parquet", 100),
            ("s3://bucket/t2/part-0.parquet", 200),
            ("s3://bucket/t3/part-0.parquet", 300),
        ])

        # Use a simple discovery mock that returns 3 tables
        discovery = MagicMock()
        discovery.discover.return_value = [
            TableInfo(name="t1", location="s3://bucket/t1"),
            TableInfo(name="t2", location="s3://bucket/t2"),
            TableInfo(name="t3", location="s3://bucket/t3"),
        ]

        listing = InventoryListing(inventory_df)
        results = convert(spark, discovery, listing, "cat", "sch", max_workers=2, print_summary=False)

        assert len(results) == 3
        table_names = {r.source_table for r in results}
        assert table_names == {"t1", "t2", "t3"}

    @patch("hive_to_delta.delta_log.write_to_s3")
    def test_one_table_exception_others_succeed(self, mock_write_s3):
        """One table throws exception during conversion -> captured as failure, others succeed."""
        call_count = 0

        def write_s3_side_effect(content, path, region="us-east-1"):
            nonlocal call_count
            call_count += 1
            if "bad_table" in path:
                raise RuntimeError("S3 write exploded")
            return path

        mock_write_s3.side_effect = write_s3_side_effect
        spark = make_mock_spark()

        inventory_df = make_mock_files_df([
            ("s3://bucket/ok1/part-0.parquet", 100),
            ("s3://bucket/bad_table/part-0.parquet", 200),
            ("s3://bucket/ok2/part-0.parquet", 300),
        ])

        discovery = MagicMock()
        discovery.discover.return_value = [
            TableInfo(name="ok1", location="s3://bucket/ok1"),
            TableInfo(name="bad_table", location="s3://bucket/bad_table"),
            TableInfo(name="ok2", location="s3://bucket/ok2"),
        ]

        listing = InventoryListing(inventory_df)
        results = convert(spark, discovery, listing, "cat", "sch", max_workers=2, print_summary=False)

        assert len(results) == 3
        successes = [r for r in results if r.success]
        failures = [r for r in results if not r.success]
        assert len(successes) == 2
        assert len(failures) == 1
        failed = failures[0]
        assert "bad_table" in failed.source_table or "exploded" in (failed.error or "")


# =============================================================================
# 4. Legacy wrapper backward compatibility
# =============================================================================


class TestLegacyWrappers:
    """Tests for convert_single_table() and convert_tables()."""

    @patch("hive_to_delta.delta_log.write_to_s3")
    @patch("hive_to_delta.s3.get_s3_client")
    @patch("hive_to_delta.glue.get_glue_client")
    def test_convert_single_table(self, mock_glue_client_factory, mock_s3_client_factory, mock_write_s3):
        """convert_single_table() should produce the same result as manual GlueDiscovery + S3Listing."""
        glue_client = MagicMock()
        mock_glue_client_factory.return_value = glue_client

        # list_glue_tables
        tables_paginator = MagicMock()
        tables_paginator.paginate.return_value = [
            {"TableList": [{"Name": "events"}]}
        ]

        # get_partitions — no partitions
        partitions_paginator = MagicMock()
        partitions_paginator.paginate.return_value = [{"Partitions": []}]

        def paginator_router(operation):
            if operation == "get_tables":
                return tables_paginator
            elif operation == "get_partitions":
                return partitions_paginator
            raise ValueError(f"Unexpected: {operation}")

        glue_client.get_paginator.side_effect = paginator_router

        glue_client.get_table.return_value = make_glue_table_response(
            "events", "s3://bucket/events",
            columns=[{"Name": "event_id", "Type": "string"}],
        )

        s3_client = MagicMock()
        mock_s3_client_factory.return_value = s3_client
        s3_paginator = MagicMock()
        s3_paginator.paginate.return_value = [
            make_s3_list_response([("events/data.parquet", 4000)])
        ]
        s3_client.get_paginator.return_value = s3_paginator

        mock_write_s3.return_value = "s3://bucket/events/_delta_log/00000000000000000000.json"

        spark = MagicMock()  # Uses Glue columns, not spark schema

        result = convert_single_table(
            spark, "mydb", "events", "cat", "sch", aws_region="us-east-1",
        )

        assert result.success is True
        assert result.source_table == "events"
        assert result.target_table == "cat.sch.events"
        assert result.file_count == 1

    @patch("hive_to_delta.delta_log.write_to_s3")
    @patch("hive_to_delta.s3.get_s3_client")
    @patch("hive_to_delta.glue.get_glue_client")
    def test_convert_tables_explicit_list(self, mock_glue_client_factory, mock_s3_client_factory, mock_write_s3):
        """convert_tables() with explicit list discovers each table individually."""
        glue_client = MagicMock()
        mock_glue_client_factory.return_value = glue_client

        # Each table is discovered individually — separate paginate calls
        call_idx = {"tables": 0}

        def tables_paginate(**kwargs):
            call_idx["tables"] += 1
            # Each call returns the single table matching the pattern
            db = kwargs.get("DatabaseName", "mydb")
            tables = [{"Name": "t1"}, {"Name": "t2"}]
            # Return one table per call based on call order
            idx = min(call_idx["tables"] - 1, len(tables) - 1)
            return [{"TableList": [tables[idx]]}]

        tables_paginator = MagicMock()
        tables_paginator.paginate.side_effect = tables_paginate

        partitions_paginator = MagicMock()
        partitions_paginator.paginate.return_value = [{"Partitions": []}]

        def paginator_router(operation):
            if operation == "get_tables":
                return tables_paginator
            elif operation == "get_partitions":
                return partitions_paginator
            raise ValueError(f"Unexpected: {operation}")

        glue_client.get_paginator.side_effect = paginator_router

        def get_table_router(**kwargs):
            name = kwargs["Name"]
            return make_glue_table_response(
                name, f"s3://bucket/{name}",
                columns=[{"Name": "id", "Type": "bigint"}],
            )

        glue_client.get_table.side_effect = get_table_router

        s3_client = MagicMock()
        mock_s3_client_factory.return_value = s3_client
        s3_paginator = MagicMock()
        s3_paginator.paginate.return_value = [
            make_s3_list_response([("data/part-0.parquet", 100)])
        ]
        s3_client.get_paginator.return_value = s3_paginator

        mock_write_s3.return_value = "s3://bucket/t/_delta_log/00000000000000000000.json"

        spark = MagicMock()

        results = convert_tables(
            spark, "mydb", ["t1", "t2"], "cat", "sch",
            aws_region="us-east-1", print_summary=False,
        )

        assert len(results) == 2
        # Verify per-table discovery: get_paginator("get_tables") called twice
        get_tables_calls = [
            c for c in glue_client.get_paginator.call_args_list
            if c[0][0] == "get_tables"
        ]
        assert len(get_tables_calls) == 2

    @patch("hive_to_delta.delta_log.write_to_s3")
    @patch("hive_to_delta.s3.get_s3_client")
    @patch("hive_to_delta.glue.get_glue_client")
    def test_convert_tables_glob_pattern(self, mock_glue_client_factory, mock_s3_client_factory, mock_write_s3):
        """convert_tables() with glob pattern discovers all matching tables at once."""
        glue_client = MagicMock()
        mock_glue_client_factory.return_value = glue_client

        tables_paginator = MagicMock()
        tables_paginator.paginate.return_value = [
            {"TableList": [
                {"Name": "dim_customer"},
                {"Name": "dim_product"},
                {"Name": "fact_sales"},  # should not match dim_*
            ]}
        ]

        partitions_paginator = MagicMock()
        partitions_paginator.paginate.return_value = [{"Partitions": []}]

        def paginator_router(operation):
            if operation == "get_tables":
                return tables_paginator
            elif operation == "get_partitions":
                return partitions_paginator
            raise ValueError(f"Unexpected: {operation}")

        glue_client.get_paginator.side_effect = paginator_router

        def get_table_router(**kwargs):
            name = kwargs["Name"]
            return make_glue_table_response(
                name, f"s3://bucket/{name}",
                columns=[{"Name": "id", "Type": "bigint"}],
            )

        glue_client.get_table.side_effect = get_table_router

        s3_client = MagicMock()
        mock_s3_client_factory.return_value = s3_client
        s3_paginator = MagicMock()
        s3_paginator.paginate.return_value = [
            make_s3_list_response([("data/part-0.parquet", 100)])
        ]
        s3_client.get_paginator.return_value = s3_paginator

        mock_write_s3.return_value = "s3://bucket/t/_delta_log/00000000000000000000.json"

        spark = MagicMock()

        results = convert_tables(
            spark, "mydb", "dim_*", "cat", "sch",
            aws_region="us-east-1", print_summary=False,
        )

        # Only dim_customer and dim_product should match
        assert len(results) == 2
        source_tables = {r.source_table for r in results}
        assert source_tables == {"dim_customer", "dim_product"}


# =============================================================================
# 5. Error handling paths
# =============================================================================


class TestErrorHandling:
    """Tests that various failure modes produce ConversionResult with success=False."""

    def test_schema_inference_failure(self):
        """Spark parquet read raises -> ConversionResult.success=False with error."""
        spark = make_mock_spark()
        spark.read.parquet.side_effect = Exception("Cannot read parquet: corrupt file")

        files_df = make_mock_files_df([
            ("s3://bucket/table/corrupt.parquet", 100),
        ])

        # columns=None in TableInfo means spark inference path is used
        result = convert_table(
            spark, files_df, "s3://bucket/table",
            "cat", "sch", "bad_schema_table",
        )

        assert result.success is False
        assert "Cannot read parquet" in result.error

    @patch("hive_to_delta.delta_log.write_to_s3")
    def test_delta_log_write_failure(self, mock_write_s3):
        """S3 write fails -> ConversionResult.success=False."""
        mock_write_s3.side_effect = Exception("S3 access denied")
        spark = make_mock_spark()

        files_df = make_mock_files_df([
            ("s3://bucket/table/part-0.parquet", 100),
        ])

        result = convert_table(
            spark, files_df, "s3://bucket/table",
            "cat", "sch", "write_fail_table",
        )

        assert result.success is False
        assert "S3 access denied" in result.error

    @patch("hive_to_delta.delta_log.write_to_s3")
    def test_sql_execution_failure(self, mock_write_s3):
        """spark.sql() raises during CREATE TABLE -> ConversionResult.success=False."""
        mock_write_s3.return_value = "s3://bucket/table/_delta_log/00000000000000000000.json"
        spark = make_mock_spark()

        def sql_side_effect(query):
            if "CREATE TABLE" in query and "USING DELTA" in query:
                raise Exception("INSUFFICIENT_PERMISSIONS")
            return MagicMock()

        spark.sql.side_effect = sql_side_effect

        files_df = make_mock_files_df([
            ("s3://bucket/table/part-0.parquet", 100),
        ])

        result = convert_table(
            spark, files_df, "s3://bucket/table",
            "cat", "sch", "perm_fail_table",
        )

        assert result.success is False
        assert "INSUFFICIENT_PERMISSIONS" in result.error

    def test_discovery_returns_empty(self):
        """Discovery finds nothing -> empty results list."""
        spark = make_mock_spark()
        discovery = MagicMock()
        discovery.discover.return_value = []
        listing = MagicMock()

        results = convert(spark, discovery, listing, "cat", "sch", print_summary=False)

        assert results == []
        listing.list_files.assert_not_called()


# =============================================================================
# 6. InventoryListing location filtering
# =============================================================================


class TestInventoryListingFiltering:
    """Tests that InventoryListing correctly filters files by table location."""

    def test_filters_to_matching_table_location(self):
        """Only files under table.location are returned."""
        inventory_df = make_mock_files_df([
            ("s3://bucket/table_a/part-0.parquet", 100),
            ("s3://bucket/table_a/part-1.parquet", 200),
            ("s3://bucket/table_b/part-0.parquet", 300),
            ("s3://bucket/other/part-0.parquet", 400),
        ])

        listing = InventoryListing(inventory_df)
        table = TableInfo(name="table_a", location="s3://bucket/table_a")

        files = listing.list_files(None, table)

        assert len(files) == 2
        paths = {f.path for f in files}
        assert paths == {
            "s3://bucket/table_a/part-0.parquet",
            "s3://bucket/table_a/part-1.parquet",
        }

    def test_trailing_slash_handling(self):
        """table.location with trailing slash vs without should both work."""
        inventory_df = make_mock_files_df([
            ("s3://bucket/my_table/part-0.parquet", 100),
        ])

        listing = InventoryListing(inventory_df)

        # Location WITH trailing slash
        table_with_slash = TableInfo(name="my_table", location="s3://bucket/my_table/")
        files = listing.list_files(None, table_with_slash)
        assert len(files) == 1

        # Location WITHOUT trailing slash
        table_no_slash = TableInfo(name="my_table", location="s3://bucket/my_table")
        files = listing.list_files(None, table_no_slash)
        assert len(files) == 1

    def test_partition_values_parsed_from_inventory(self):
        """InventoryListing parses partition values from file paths when table has partition_keys."""
        inventory_df = make_mock_files_df([
            ("s3://bucket/events/year=2024/month=01/part-0.parquet", 100),
            ("s3://bucket/events/year=2024/month=02/part-0.parquet", 200),
        ])

        listing = InventoryListing(inventory_df)
        table = TableInfo(
            name="events",
            location="s3://bucket/events",
            partition_keys=["year", "month"],
        )

        files = listing.list_files(None, table)

        assert len(files) == 2
        pv0 = files[0].partition_values
        pv1 = files[1].partition_values
        assert pv0["year"] == "2024"
        assert pv0["month"] == "01"
        assert pv1["year"] == "2024"
        assert pv1["month"] == "02"

    def test_no_matching_files_returns_empty(self):
        """DataFrame with files for other tables -> empty result for this table."""
        inventory_df = make_mock_files_df([
            ("s3://bucket/other_table/part-0.parquet", 100),
        ])

        listing = InventoryListing(inventory_df)
        table = TableInfo(name="my_table", location="s3://bucket/my_table")

        files = listing.list_files(None, table)
        assert len(files) == 0
