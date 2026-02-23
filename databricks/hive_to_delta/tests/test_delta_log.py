"""Unit tests for delta_log.py functions.

Tests the four main functions:
1. build_delta_schema() - Converts Glue column definitions to Delta schema
2. build_add_action() - Creates Delta log add actions for files
3. generate_delta_log() - Generates complete Delta transaction log
4. write_delta_log() - Writes Delta log to S3
"""

import json
from datetime import datetime
from unittest.mock import MagicMock, patch, call

import pytest

from hive_to_delta.delta_log import (
    build_delta_schema,
    build_add_action,
    generate_delta_log,
    write_delta_log,
    _normalize_glue_type,
    _map_glue_to_delta_type,
)
from hive_to_delta.models import ParquetFileInfo


# =============================================================================
# Tests for _normalize_glue_type() helper
# =============================================================================


class TestNormalizeGlueType:
    """Tests for the _normalize_glue_type helper function."""

    def test_simple_type_lowercase(self):
        """Test simple type is lowercased."""
        assert _normalize_glue_type("STRING") == "string"
        assert _normalize_glue_type("String") == "string"
        assert _normalize_glue_type("BIGINT") == "bigint"

    def test_parameterized_varchar(self):
        """Test varchar(255) returns base type."""
        assert _normalize_glue_type("varchar(255)") == "varchar"
        assert _normalize_glue_type("VARCHAR(100)") == "varchar"

    def test_parameterized_char(self):
        """Test char(10) returns base type."""
        assert _normalize_glue_type("char(10)") == "char"
        assert _normalize_glue_type("CHAR(5)") == "char"

    def test_decimal_preserves_params(self):
        """Test decimal(10,2) preserves full specification."""
        assert _normalize_glue_type("decimal(10,2)") == "decimal(10,2)"
        assert _normalize_glue_type("DECIMAL(18,4)") == "decimal(18,4)"

    def test_whitespace_stripped(self):
        """Test whitespace is stripped."""
        assert _normalize_glue_type("  string  ") == "string"
        assert _normalize_glue_type("  varchar(100)  ") == "varchar"


# =============================================================================
# Tests for _map_glue_to_delta_type() helper
# =============================================================================


class TestMapGlueToDeltaType:
    """Tests for the _map_glue_to_delta_type helper function."""

    def test_string_types(self):
        """Test string type mappings."""
        assert _map_glue_to_delta_type("string") == "string"
        assert _map_glue_to_delta_type("varchar(255)") == "string"
        assert _map_glue_to_delta_type("char(10)") == "string"

    def test_numeric_types(self):
        """Test numeric type mappings."""
        assert _map_glue_to_delta_type("tinyint") == "byte"
        assert _map_glue_to_delta_type("smallint") == "short"
        assert _map_glue_to_delta_type("int") == "integer"
        assert _map_glue_to_delta_type("integer") == "integer"
        assert _map_glue_to_delta_type("bigint") == "long"
        assert _map_glue_to_delta_type("float") == "float"
        assert _map_glue_to_delta_type("double") == "double"

    def test_decimal_preserved(self):
        """Test decimal preserves precision and scale."""
        assert _map_glue_to_delta_type("decimal(10,2)") == "decimal(10,2)"
        assert _map_glue_to_delta_type("DECIMAL(18,4)") == "decimal(18,4)"

    def test_boolean_type(self):
        """Test boolean type mapping."""
        assert _map_glue_to_delta_type("boolean") == "boolean"

    def test_datetime_types(self):
        """Test date/time type mappings."""
        assert _map_glue_to_delta_type("date") == "date"
        assert _map_glue_to_delta_type("timestamp") == "timestamp"

    def test_binary_type(self):
        """Test binary type mapping."""
        assert _map_glue_to_delta_type("binary") == "binary"

    def test_bare_complex_types(self):
        """Test bare complex type names (without parameters) still map correctly."""
        assert _map_glue_to_delta_type("array") == "array"
        assert _map_glue_to_delta_type("map") == "map"
        assert _map_glue_to_delta_type("struct") == "struct"

    def test_parameterized_complex_types(self):
        """Test parameterized complex types produce Delta protocol JSON objects."""
        # array<string> → Delta protocol array object
        result = _map_glue_to_delta_type("array<string>")
        assert result == {
            "type": "array",
            "elementType": "string",
            "containsNull": True,
        }

        # map<string,int> → Delta protocol map object
        result = _map_glue_to_delta_type("map<string,int>")
        assert result == {
            "type": "map",
            "keyType": "string",
            "valueType": "integer",
            "valueContainsNull": True,
        }

        # struct<name:string,age:int> → Delta protocol struct object
        result = _map_glue_to_delta_type("struct<name:string,age:int>")
        assert result == {
            "type": "struct",
            "fields": [
                {"name": "name", "type": "string", "nullable": True, "metadata": {}},
                {"name": "age", "type": "integer", "nullable": True, "metadata": {}},
            ],
        }

    def test_unknown_type_defaults_to_string(self):
        """Test unknown types default to string."""
        assert _map_glue_to_delta_type("unknown_type") == "string"
        assert _map_glue_to_delta_type("custom_type") == "string"


# =============================================================================
# Tests for build_delta_schema()
# =============================================================================


class TestBuildDeltaSchema:
    """Tests for build_delta_schema function."""

    def test_empty_column_list_raises_error(self):
        """Test that empty column list raises ValueError."""
        with pytest.raises(ValueError, match="Invalid Glue schema: no columns provided"):
            build_delta_schema([])

    def test_column_with_empty_name_raises_error(self):
        """Test that column with empty name raises ValueError."""
        columns = [
            {"Name": "valid_column", "Type": "string"},
            {"Name": "", "Type": "string"},
        ]
        with pytest.raises(ValueError, match="Invalid Glue schema: column without name found"):
            build_delta_schema(columns)

    def test_column_with_whitespace_name_raises_error(self):
        """Test that column with only whitespace name raises ValueError."""
        columns = [
            {"Name": "valid_column", "Type": "string"},
            {"Name": "   ", "Type": "string"},
        ]
        with pytest.raises(ValueError, match="Invalid Glue schema: column without name found"):
            build_delta_schema(columns)

    def test_column_with_missing_name_key_raises_error(self):
        """Test that column without Name key (and no name fallback) raises ValueError."""
        columns = [
            {"Name": "valid_column", "Type": "string"},
            {"Type": "string"},  # Missing Name key
        ]
        with pytest.raises(ValueError, match="Invalid Glue schema: column without name found"):
            build_delta_schema(columns)

    def test_standard_column_types(self):
        """Test standard column types (string, int, bigint, double, boolean, date, timestamp)."""
        columns = [
            {"Name": "col_string", "Type": "string"},
            {"Name": "col_int", "Type": "int"},
            {"Name": "col_bigint", "Type": "bigint"},
            {"Name": "col_double", "Type": "double"},
            {"Name": "col_boolean", "Type": "boolean"},
            {"Name": "col_date", "Type": "date"},
            {"Name": "col_timestamp", "Type": "timestamp"},
        ]
        schema = build_delta_schema(columns)

        assert schema["type"] == "struct"
        assert len(schema["fields"]) == 7

        # Verify each field
        assert schema["fields"][0] == {
            "name": "col_string",
            "type": "string",
            "nullable": True,
            "metadata": {},
        }
        assert schema["fields"][1] == {
            "name": "col_int",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        }
        assert schema["fields"][2] == {
            "name": "col_bigint",
            "type": "long",
            "nullable": True,
            "metadata": {},
        }
        assert schema["fields"][3] == {
            "name": "col_double",
            "type": "double",
            "nullable": True,
            "metadata": {},
        }
        assert schema["fields"][4] == {
            "name": "col_boolean",
            "type": "boolean",
            "nullable": True,
            "metadata": {},
        }
        assert schema["fields"][5] == {
            "name": "col_date",
            "type": "date",
            "nullable": True,
            "metadata": {},
        }
        assert schema["fields"][6] == {
            "name": "col_timestamp",
            "type": "timestamp",
            "nullable": True,
            "metadata": {},
        }

    def test_bare_complex_types(self):
        """Test bare complex type names (backward compat, not realistic Glue output)."""
        columns = [
            {"Name": "col_array", "Type": "array"},
            {"Name": "col_map", "Type": "map"},
            {"Name": "col_struct", "Type": "struct"},
        ]
        schema = build_delta_schema(columns)

        assert schema["type"] == "struct"
        assert len(schema["fields"]) == 3

        assert schema["fields"][0]["name"] == "col_array"
        assert schema["fields"][0]["type"] == "array"
        assert schema["fields"][1]["name"] == "col_map"
        assert schema["fields"][1]["type"] == "map"
        assert schema["fields"][2]["name"] == "col_struct"
        assert schema["fields"][2]["type"] == "struct"

    def test_parameterized_complex_types_in_schema(self):
        """Test parameterized complex types produce Delta protocol JSON objects in schema."""
        columns = [
            {"Name": "id", "Type": "bigint"},
            {"Name": "tags", "Type": "array<string>"},
            {"Name": "properties", "Type": "map<string,int>"},
            {"Name": "address", "Type": "struct<street:string,city:string,zip:int>"},
        ]
        schema = build_delta_schema(columns)

        assert schema["type"] == "struct"
        assert len(schema["fields"]) == 4

        # Simple type
        assert schema["fields"][0]["type"] == "long"

        # array<string>
        assert schema["fields"][1]["type"] == {
            "type": "array",
            "elementType": "string",
            "containsNull": True,
        }

        # map<string,int>
        assert schema["fields"][2]["type"] == {
            "type": "map",
            "keyType": "string",
            "valueType": "integer",
            "valueContainsNull": True,
        }

        # struct<street:string,city:string,zip:int>
        assert schema["fields"][3]["type"] == {
            "type": "struct",
            "fields": [
                {"name": "street", "type": "string", "nullable": True, "metadata": {}},
                {"name": "city", "type": "string", "nullable": True, "metadata": {}},
                {"name": "zip", "type": "integer", "nullable": True, "metadata": {}},
            ],
        }

    def test_type_mapping_edge_cases(self):
        """Test type mapping edge cases (unknown types default to string)."""
        columns = [
            {"Name": "col_unknown", "Type": "unknown_type"},
            {"Name": "col_custom", "Type": "custom_type"},
        ]
        schema = build_delta_schema(columns)

        assert schema["type"] == "struct"
        assert len(schema["fields"]) == 2

        # Unknown types should default to string
        assert schema["fields"][0]["type"] == "string"
        assert schema["fields"][1]["type"] == "string"

    def test_case_sensitivity_uppercase(self):
        """Test case sensitivity (Name vs name, Type vs type) - uppercase."""
        columns = [
            {"Name": "id", "Type": "bigint"},
            {"Name": "name", "Type": "string"},
        ]
        schema = build_delta_schema(columns)

        assert schema["type"] == "struct"
        assert len(schema["fields"]) == 2
        assert schema["fields"][0]["name"] == "id"
        assert schema["fields"][0]["type"] == "long"
        assert schema["fields"][1]["name"] == "name"
        assert schema["fields"][1]["type"] == "string"

    def test_case_sensitivity_lowercase(self):
        """Test case sensitivity - lowercase keys."""
        columns = [
            {"name": "id", "type": "bigint"},
            {"name": "value", "type": "double"},
        ]
        schema = build_delta_schema(columns)

        assert schema["type"] == "struct"
        assert len(schema["fields"]) == 2
        assert schema["fields"][0]["name"] == "id"
        assert schema["fields"][0]["type"] == "long"
        assert schema["fields"][1]["name"] == "value"
        assert schema["fields"][1]["type"] == "double"

    def test_decimal_with_precision_scale(self):
        """Test decimal type with precision and scale is preserved."""
        columns = [
            {"Name": "price", "Type": "decimal(10,2)"},
            {"Name": "amount", "Type": "decimal(18,4)"},
        ]
        schema = build_delta_schema(columns)

        assert schema["type"] == "struct"
        assert len(schema["fields"]) == 2
        assert schema["fields"][0]["type"] == "decimal(10,2)"
        assert schema["fields"][1]["type"] == "decimal(18,4)"

    def test_all_fields_nullable_true(self):
        """Test that all fields have nullable=True."""
        columns = [
            {"Name": "col1", "Type": "string"},
            {"Name": "col2", "Type": "int"},
        ]
        schema = build_delta_schema(columns)

        for field in schema["fields"]:
            assert field["nullable"] is True

    def test_all_fields_have_empty_metadata(self):
        """Test that all fields have empty metadata dict."""
        columns = [
            {"Name": "col1", "Type": "string"},
            {"Name": "col2", "Type": "int"},
        ]
        schema = build_delta_schema(columns)

        for field in schema["fields"]:
            assert field["metadata"] == {}


# =============================================================================
# Tests for build_add_action()
# =============================================================================


class TestBuildAddAction:
    """Tests for build_add_action function."""

    @patch("hive_to_delta.delta_log.datetime")
    def test_with_partition_columns_and_values(self, mock_datetime):
        """Test with partition columns and values."""
        mock_datetime.now.return_value.timestamp.return_value = 1234567890.123
        expected_mod_time = 1234567890123

        file_info = ParquetFileInfo(
            path="s3://bucket/table/year=2024/month=01/file1.parquet",
            size=1024,
            partition_values={"year": "2024", "month": "01"},
        )
        table_location = "s3://bucket/table"

        result = build_add_action(file_info, table_location)

        assert result == {
            "add": {
                "path": "year=2024/month=01/file1.parquet",
                "partitionValues": {"year": "2024", "month": "01"},
                "size": 1024,
                "modificationTime": expected_mod_time,
                "dataChange": True,
            }
        }

    @patch("hive_to_delta.delta_log.datetime")
    def test_without_partition_columns_unpartitioned(self, mock_datetime):
        """Test without partition columns (unpartitioned table)."""
        mock_datetime.now.return_value.timestamp.return_value = 1234567890.123
        expected_mod_time = 1234567890123

        file_info = ParquetFileInfo(
            path="s3://bucket/table/file1.parquet",
            size=2048,
            partition_values={},
        )
        table_location = "s3://bucket/table"

        result = build_add_action(file_info, table_location)

        assert result == {
            "add": {
                "path": "file1.parquet",
                "partitionValues": {},
                "size": 2048,
                "modificationTime": expected_mod_time,
                "dataChange": True,
            }
        }

    @patch("hive_to_delta.delta_log.datetime")
    def test_relative_path_calculation(self, mock_datetime):
        """Test relative path calculation."""
        mock_datetime.now.return_value.timestamp.return_value = 1234567890.123

        file_info = ParquetFileInfo(
            path="s3://bucket/root/subdir/file.parquet",
            size=512,
            partition_values={},
        )
        table_location = "s3://bucket/root"

        result = build_add_action(file_info, table_location)

        assert result["add"]["path"] == "subdir/file.parquet"

    @patch("hive_to_delta.delta_log.datetime")
    def test_absolute_s3_path_cross_bucket(self, mock_datetime):
        """Test absolute S3 paths (cross-bucket scenarios)."""
        mock_datetime.now.return_value.timestamp.return_value = 1234567890.123

        file_info = ParquetFileInfo(
            path="s3://other-bucket/data/file.parquet",
            size=4096,
            partition_values={"region": "us-west-2"},
        )
        table_location = "s3://main-bucket/table"

        result = build_add_action(file_info, table_location)

        # File in different bucket should use absolute path
        assert result["add"]["path"] == "s3://other-bucket/data/file.parquet"
        assert result["add"]["partitionValues"] == {"region": "us-west-2"}

    @patch("hive_to_delta.delta_log.datetime")
    def test_absolute_s3_path_cross_region(self, mock_datetime):
        """Test absolute S3 paths (cross-region scenarios)."""
        mock_datetime.now.return_value.timestamp.return_value = 1234567890.123

        file_info = ParquetFileInfo(
            path="s3://bucket-us-west-2/data/file.parquet",
            size=8192,
            partition_values={"region": "us-west-2"},
        )
        table_location = "s3://bucket-us-east-1/table"

        result = build_add_action(file_info, table_location)

        # File in different bucket should use absolute path
        assert result["add"]["path"] == "s3://bucket-us-west-2/data/file.parquet"

    @patch("hive_to_delta.delta_log.datetime")
    def test_file_size_and_modification_time(self, mock_datetime):
        """Test file size and modification time."""
        mock_datetime.now.return_value.timestamp.return_value = 1609459200.5
        expected_mod_time = 1609459200500

        file_info = ParquetFileInfo(
            path="s3://bucket/table/file.parquet",
            size=12345,
            partition_values={},
        )
        table_location = "s3://bucket/table"

        result = build_add_action(file_info, table_location)

        assert result["add"]["size"] == 12345
        assert result["add"]["modificationTime"] == expected_mod_time

    @patch("hive_to_delta.delta_log.datetime")
    def test_table_location_with_trailing_slash(self, mock_datetime):
        """Test that table_location with trailing slash is handled correctly."""
        mock_datetime.now.return_value.timestamp.return_value = 1234567890.123

        file_info = ParquetFileInfo(
            path="s3://bucket/table/subdir/file.parquet",
            size=1024,
            partition_values={},
        )
        table_location = "s3://bucket/table/"  # Trailing slash

        result = build_add_action(file_info, table_location)

        # Should still calculate relative path correctly
        assert result["add"]["path"] == "subdir/file.parquet"

    @patch("hive_to_delta.delta_log.datetime")
    def test_already_relative_path(self, mock_datetime):
        """Test with already relative path (not starting with s3://)."""
        mock_datetime.now.return_value.timestamp.return_value = 1234567890.123

        file_info = ParquetFileInfo(
            path="subdir/file.parquet",
            size=1024,
            partition_values={},
        )
        table_location = "s3://bucket/table"

        result = build_add_action(file_info, table_location)

        # Should use path as-is
        assert result["add"]["path"] == "subdir/file.parquet"


# =============================================================================
# Tests for generate_delta_log()
# =============================================================================


class TestGenerateDeltaLog:
    """Tests for generate_delta_log function."""

    def test_partitioned_table_log_generation(self):
        """Test partitioned table log generation."""
        files = [
            ParquetFileInfo(
                path="s3://bucket/table/year=2024/file1.parquet",
                size=1024,
                partition_values={"year": "2024"},
            ),
            ParquetFileInfo(
                path="s3://bucket/table/year=2023/file2.parquet",
                size=2048,
                partition_values={"year": "2023"},
            ),
        ]
        schema = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": True, "metadata": {}},
                {"name": "value", "type": "string", "nullable": True, "metadata": {}},
            ],
        }
        partition_columns = ["year"]
        table_location = "s3://bucket/table"

        result = generate_delta_log(files, schema, partition_columns, table_location)

        # Parse each line as JSON
        lines = result.strip().split("\n")
        assert len(lines) == 4  # protocol + metadata + 2 add actions

        # Verify protocol
        protocol = json.loads(lines[0])
        assert protocol == {
            "protocol": {
                "minReaderVersion": 1,
                "minWriterVersion": 2,
            }
        }

        # Verify metadata
        metadata = json.loads(lines[1])
        assert "metaData" in metadata
        assert metadata["metaData"]["format"]["provider"] == "parquet"
        assert metadata["metaData"]["partitionColumns"] == ["year"]
        schema_from_metadata = json.loads(metadata["metaData"]["schemaString"])
        assert schema_from_metadata == schema

        # Verify add actions
        add1 = json.loads(lines[2])
        assert "add" in add1
        assert add1["add"]["partitionValues"] == {"year": "2024"}

        add2 = json.loads(lines[3])
        assert "add" in add2
        assert add2["add"]["partitionValues"] == {"year": "2023"}

    def test_unpartitioned_table_log_generation(self):
        """Test unpartitioned table log generation."""
        files = [
            ParquetFileInfo(
                path="s3://bucket/table/file1.parquet",
                size=1024,
                partition_values={},
            ),
        ]
        schema = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": True, "metadata": {}},
            ],
        }
        partition_columns = []
        table_location = "s3://bucket/table"

        result = generate_delta_log(files, schema, partition_columns, table_location)

        # Parse each line as JSON
        lines = result.strip().split("\n")
        assert len(lines) == 3  # protocol + metadata + 1 add action

        # Verify metadata has empty partition columns
        metadata = json.loads(lines[1])
        assert metadata["metaData"]["partitionColumns"] == []

        # Verify add action has empty partition values
        add1 = json.loads(lines[2])
        assert add1["add"]["partitionValues"] == {}

    def test_with_multiple_files(self):
        """Test with multiple files."""
        files = [
            ParquetFileInfo(path=f"s3://bucket/table/file{i}.parquet", size=1024, partition_values={})
            for i in range(5)
        ]
        schema = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": True, "metadata": {}},
            ],
        }
        partition_columns = []
        table_location = "s3://bucket/table"

        result = generate_delta_log(files, schema, partition_columns, table_location)

        # Parse each line as JSON
        lines = result.strip().split("\n")
        assert len(lines) == 7  # protocol + metadata + 5 add actions

        # Verify all lines are valid JSON
        for line in lines:
            parsed = json.loads(line)
            assert parsed is not None

    def test_with_empty_file_list(self):
        """Test with empty file list."""
        files = []
        schema = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": True, "metadata": {}},
            ],
        }
        partition_columns = []
        table_location = "s3://bucket/table"

        result = generate_delta_log(files, schema, partition_columns, table_location)

        # Parse each line as JSON
        lines = result.strip().split("\n")
        assert len(lines) == 2  # protocol + metadata only

        # Verify protocol and metadata
        protocol = json.loads(lines[0])
        assert "protocol" in protocol

        metadata = json.loads(lines[1])
        assert "metaData" in metadata

    def test_protocol_json_structure(self):
        """Test protocol JSON structure."""
        files = []
        schema = {"type": "struct", "fields": []}
        partition_columns = []
        table_location = "s3://bucket/table"

        result = generate_delta_log(files, schema, partition_columns, table_location)

        lines = result.strip().split("\n")
        protocol = json.loads(lines[0])

        assert protocol == {
            "protocol": {
                "minReaderVersion": 1,
                "minWriterVersion": 2,
            }
        }

    def test_metadata_json_structure(self):
        """Test metadata JSON structure."""
        files = []
        schema = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": True, "metadata": {}},
            ],
        }
        partition_columns = ["year"]
        table_location = "s3://bucket/table"

        result = generate_delta_log(files, schema, partition_columns, table_location)

        lines = result.strip().split("\n")
        metadata = json.loads(lines[1])

        assert "metaData" in metadata
        assert "id" in metadata["metaData"]  # UUID
        assert metadata["metaData"]["format"] == {"provider": "parquet", "options": {}}
        assert "schemaString" in metadata["metaData"]
        assert metadata["metaData"]["partitionColumns"] == ["year"]
        assert metadata["metaData"]["configuration"] == {}
        assert "createdTime" in metadata["metaData"]

        # Verify schemaString is valid JSON
        schema_from_metadata = json.loads(metadata["metaData"]["schemaString"])
        assert schema_from_metadata == schema

    def test_json_format_compact(self):
        """Test that JSON is compact (no whitespace)."""
        files = [
            ParquetFileInfo(
                path="s3://bucket/table/file.parquet",
                size=1024,
                partition_values={},
            ),
        ]
        schema = {"type": "struct", "fields": []}
        partition_columns = []
        table_location = "s3://bucket/table"

        result = generate_delta_log(files, schema, partition_columns, table_location)

        # Check that lines don't have extra whitespace
        lines = result.strip().split("\n")
        for line in lines:
            # Should not have spaces after commas/colons (compact JSON)
            assert ", " not in line or line.count(", ") == 0
            assert ": " not in line or line.count(": ") == 0


# =============================================================================
# Tests for write_delta_log()
# =============================================================================


class TestWriteDeltaLog:
    """Tests for write_delta_log function."""

    @patch("hive_to_delta.delta_log.write_to_s3")
    def test_write_to_s3(self, mock_write_to_s3):
        """Test writing to S3."""
        mock_write_to_s3.return_value = "s3://bucket/table/_delta_log/00000000000000000000.json"

        delta_log_content = '{"protocol":{"minReaderVersion":1}}'
        table_location = "s3://bucket/table"
        region = "us-east-1"

        result = write_delta_log(delta_log_content, table_location, region)

        # Verify write_to_s3 was called with correct arguments
        mock_write_to_s3.assert_called_once_with(
            delta_log_content,
            "s3://bucket/table/_delta_log/00000000000000000000.json",
            region,
        )

        # Verify return value
        assert result == "s3://bucket/table/_delta_log/00000000000000000000.json"

    @patch("hive_to_delta.delta_log.write_to_s3")
    def test_delta_log_subdirectory_creation(self, mock_write_to_s3):
        """Test _delta_log subdirectory creation."""
        mock_write_to_s3.return_value = "s3://bucket/table/_delta_log/00000000000000000000.json"

        delta_log_content = '{"protocol":{"minReaderVersion":1}}'
        table_location = "s3://bucket/table"
        region = "us-west-2"

        write_delta_log(delta_log_content, table_location, region)

        # Verify path includes _delta_log subdirectory
        call_args = mock_write_to_s3.call_args[0]
        assert "_delta_log/" in call_args[1]

    @patch("hive_to_delta.delta_log.write_to_s3")
    def test_filename_format(self, mock_write_to_s3):
        """Test filename format (00000000000000000000.json)."""
        mock_write_to_s3.return_value = "s3://bucket/table/_delta_log/00000000000000000000.json"

        delta_log_content = '{"protocol":{"minReaderVersion":1}}'
        table_location = "s3://bucket/table"
        region = "us-east-1"

        write_delta_log(delta_log_content, table_location, region)

        # Verify filename is correct
        call_args = mock_write_to_s3.call_args[0]
        assert call_args[1].endswith("/00000000000000000000.json")

    @patch("hive_to_delta.delta_log.write_to_s3")
    def test_table_location_trailing_slash_handling(self, mock_write_to_s3):
        """Test handling of table_location with trailing slash."""
        mock_write_to_s3.return_value = "s3://bucket/table/_delta_log/00000000000000000000.json"

        delta_log_content = '{"protocol":{"minReaderVersion":1}}'
        table_location = "s3://bucket/table/"  # Trailing slash
        region = "us-east-1"

        write_delta_log(delta_log_content, table_location, region)

        # Verify path is correct (no double slash)
        call_args = mock_write_to_s3.call_args[0]
        assert "//" not in call_args[1].replace("s3://", "")
        assert call_args[1] == "s3://bucket/table/_delta_log/00000000000000000000.json"

    @patch("hive_to_delta.delta_log.write_to_s3")
    def test_region_parameter_passed(self, mock_write_to_s3):
        """Test that region parameter is passed to write_to_s3."""
        mock_write_to_s3.return_value = "s3://bucket/table/_delta_log/00000000000000000000.json"

        delta_log_content = '{"protocol":{"minReaderVersion":1}}'
        table_location = "s3://bucket/table"
        region = "ap-northeast-1"

        write_delta_log(delta_log_content, table_location, region)

        # Verify region was passed
        call_args = mock_write_to_s3.call_args
        assert call_args[0][2] == "ap-northeast-1"

    @patch("hive_to_delta.delta_log.write_to_s3")
    def test_default_region(self, mock_write_to_s3):
        """Test default region parameter."""
        mock_write_to_s3.return_value = "s3://bucket/table/_delta_log/00000000000000000000.json"

        delta_log_content = '{"protocol":{"minReaderVersion":1}}'
        table_location = "s3://bucket/table"

        # Call without region parameter
        write_delta_log(delta_log_content, table_location)

        # Verify default region is used
        call_args = mock_write_to_s3.call_args
        assert call_args[0][2] == "us-east-1"
