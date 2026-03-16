"""Unit tests for schema.py functions.

Tests the schema inference functions for both Glue-based and Spark-based paths:
1. build_delta_schema_from_glue() - Converts Glue column definitions to Delta schema
2. build_delta_schema_from_spark() - Converts Spark StructType to Delta schema

Complex types are tested to produce Delta protocol JSON objects, not flat strings.
"""

from unittest.mock import MagicMock

import pytest

from hive_to_delta.schema import (
    build_delta_schema_from_glue,
    build_delta_schema_from_spark,
    _normalize_glue_type,
    _map_glue_to_delta_type,
    _parse_hive_type,
    GLUE_TO_DELTA_TYPE_MAP,
)


# =============================================================================
# Tests for _parse_hive_type()
# =============================================================================


class TestParseHiveComplexType:
    """Tests for the _parse_hive_type helper function."""

    def test_array_of_string(self):
        """Test array<string> produces correct Delta protocol JSON."""
        result = _parse_hive_type("array<string>")
        assert result == {
            "type": "array",
            "elementType": "string",
            "containsNull": True,
        }

    def test_array_of_int(self):
        """Test array<int> maps int to integer in elementType."""
        result = _parse_hive_type("array<int>")
        assert result == {
            "type": "array",
            "elementType": "integer",
            "containsNull": True,
        }

    def test_map_string_to_int(self):
        """Test map<string,int> produces correct Delta protocol JSON."""
        result = _parse_hive_type("map<string,int>")
        assert result == {
            "type": "map",
            "keyType": "string",
            "valueType": "integer",
            "valueContainsNull": True,
        }

    def test_struct(self):
        """Test struct<name:string,age:int> produces correct Delta protocol JSON."""
        result = _parse_hive_type("struct<name:string,age:int>")
        assert result == {
            "type": "struct",
            "fields": [
                {"name": "name", "type": "string", "nullable": True, "metadata": {}},
                {"name": "age", "type": "integer", "nullable": True, "metadata": {}},
            ],
        }

    def test_nested_array(self):
        """Test array<array<string>> produces nested JSON."""
        result = _parse_hive_type("array<array<string>>")
        assert result == {
            "type": "array",
            "elementType": {
                "type": "array",
                "elementType": "string",
                "containsNull": True,
            },
            "containsNull": True,
        }

    def test_map_with_complex_value(self):
        """Test map<string,array<int>> produces nested JSON."""
        result = _parse_hive_type("map<string,array<int>>")
        assert result == {
            "type": "map",
            "keyType": "string",
            "valueType": {
                "type": "array",
                "elementType": "integer",
                "containsNull": True,
            },
            "valueContainsNull": True,
        }

    def test_array_of_struct(self):
        """Test array<struct<id:int,name:string>> produces nested JSON."""
        result = _parse_hive_type("array<struct<id:int,name:string>>")
        assert result == {
            "type": "array",
            "elementType": {
                "type": "struct",
                "fields": [
                    {"name": "id", "type": "integer", "nullable": True, "metadata": {}},
                    {"name": "name", "type": "string", "nullable": True, "metadata": {}},
                ],
            },
            "containsNull": True,
        }

    def test_struct_with_complex_fields(self):
        """Test struct with nested complex fields."""
        result = _parse_hive_type(
            "struct<id:bigint,tags:array<string>,props:map<string,int>>"
        )
        assert result["type"] == "struct"
        assert len(result["fields"]) == 3
        assert result["fields"][0]["type"] == "long"
        assert result["fields"][1]["type"] == {
            "type": "array",
            "elementType": "string",
            "containsNull": True,
        }
        assert result["fields"][2]["type"] == {
            "type": "map",
            "keyType": "string",
            "valueType": "integer",
            "valueContainsNull": True,
        }


# =============================================================================
# Tests for build_delta_schema_from_glue()
# =============================================================================


class TestBuildDeltaSchemaFromGlue:
    """Tests for build_delta_schema_from_glue function."""

    def test_standard_columns(self):
        """Test standard column types produce correct Delta schema."""
        columns = [
            {"Name": "id", "Type": "bigint"},
            {"Name": "name", "Type": "string"},
            {"Name": "price", "Type": "decimal(10,2)"},
            {"Name": "active", "Type": "boolean"},
        ]
        schema = build_delta_schema_from_glue(columns)

        assert schema["type"] == "struct"
        assert len(schema["fields"]) == 4

        assert schema["fields"][0] == {
            "name": "id",
            "type": "long",
            "nullable": True,
            "metadata": {},
        }
        assert schema["fields"][1] == {
            "name": "name",
            "type": "string",
            "nullable": True,
            "metadata": {},
        }
        assert schema["fields"][2] == {
            "name": "price",
            "type": "decimal(10,2)",
            "nullable": True,
            "metadata": {},
        }
        assert schema["fields"][3] == {
            "name": "active",
            "type": "boolean",
            "nullable": True,
            "metadata": {},
        }

    def test_empty_columns_raises_value_error(self):
        """Test that empty column list raises ValueError."""
        with pytest.raises(ValueError, match="no columns provided"):
            build_delta_schema_from_glue([])

    def test_parameterized_array_type(self):
        """Test that array<string> from Glue produces Delta protocol JSON object."""
        columns = [
            {"Name": "id", "Type": "bigint"},
            {"Name": "tags", "Type": "array<string>"},
        ]
        schema = build_delta_schema_from_glue(columns)

        assert schema["fields"][1]["type"] == {
            "type": "array",
            "elementType": "string",
            "containsNull": True,
        }

    def test_parameterized_map_type(self):
        """Test that map<string,int> from Glue produces Delta protocol JSON object."""
        columns = [
            {"Name": "id", "Type": "bigint"},
            {"Name": "properties", "Type": "map<string,int>"},
        ]
        schema = build_delta_schema_from_glue(columns)

        assert schema["fields"][1]["type"] == {
            "type": "map",
            "keyType": "string",
            "valueType": "integer",
            "valueContainsNull": True,
        }

    def test_parameterized_struct_type(self):
        """Test that struct<...> from Glue produces Delta protocol JSON object."""
        columns = [
            {"Name": "id", "Type": "bigint"},
            {"Name": "address", "Type": "struct<street:string,city:string,zip:int>"},
        ]
        schema = build_delta_schema_from_glue(columns)

        assert schema["fields"][1]["type"] == {
            "type": "struct",
            "fields": [
                {"name": "street", "type": "string", "nullable": True, "metadata": {}},
                {"name": "city", "type": "string", "nullable": True, "metadata": {}},
                {"name": "zip", "type": "integer", "nullable": True, "metadata": {}},
            ],
        }

    def test_nested_complex_types(self):
        """Test deeply nested complex types from Glue produce nested JSON."""
        columns = [
            {"Name": "nested_tags", "Type": "array<array<string>>"},
            {"Name": "nested_map", "Type": "map<string,array<int>>"},
            {"Name": "nested_struct", "Type": "array<struct<id:int,tags:array<string>>>"},
        ]
        schema = build_delta_schema_from_glue(columns)

        # array<array<string>>
        assert schema["fields"][0]["type"] == {
            "type": "array",
            "elementType": {
                "type": "array",
                "elementType": "string",
                "containsNull": True,
            },
            "containsNull": True,
        }

        # map<string,array<int>>
        assert schema["fields"][1]["type"] == {
            "type": "map",
            "keyType": "string",
            "valueType": {
                "type": "array",
                "elementType": "integer",
                "containsNull": True,
            },
            "valueContainsNull": True,
        }

        # array<struct<id:int,tags:array<string>>>
        assert schema["fields"][2]["type"]["type"] == "array"
        inner_struct = schema["fields"][2]["type"]["elementType"]
        assert inner_struct["type"] == "struct"
        assert inner_struct["fields"][0]["type"] == "integer"
        assert inner_struct["fields"][1]["type"] == {
            "type": "array",
            "elementType": "string",
            "containsNull": True,
        }

    def test_mixed_simple_and_complex_columns(self):
        """Test a realistic table with both simple and complex columns."""
        columns = [
            {"Name": "id", "Type": "bigint"},
            {"Name": "name", "Type": "string"},
            {"Name": "tags", "Type": "array<string>"},
            {"Name": "properties", "Type": "map<string,int>"},
            {"Name": "address", "Type": "struct<street:string,city:string>"},
            {"Name": "active", "Type": "boolean"},
            {"Name": "created_at", "Type": "timestamp"},
        ]
        schema = build_delta_schema_from_glue(columns)

        assert schema["type"] == "struct"
        assert len(schema["fields"]) == 7

        # Simple types are strings
        assert schema["fields"][0]["type"] == "long"
        assert schema["fields"][1]["type"] == "string"
        assert schema["fields"][5]["type"] == "boolean"
        assert schema["fields"][6]["type"] == "timestamp"

        # Complex types are dicts
        assert isinstance(schema["fields"][2]["type"], dict)
        assert isinstance(schema["fields"][3]["type"], dict)
        assert isinstance(schema["fields"][4]["type"], dict)


# =============================================================================
# Tests for build_delta_schema_from_spark()
# =============================================================================


class TestBuildDeltaSchemaFromSpark:
    """Tests for build_delta_schema_from_spark function.

    The Spark path now uses jsonValue() on each field's dataType, which
    returns strings for simple types and dicts for complex types.
    """

    def _make_field(self, name, json_value, nullable=True):
        """Create a mock Spark StructField with jsonValue() return."""
        field = MagicMock()
        field.name = name
        field.dataType.jsonValue.return_value = json_value
        field.nullable = nullable
        return field

    def test_simple_types(self):
        """Test simple Spark types (string, long, double)."""
        spark_schema = MagicMock()
        spark_schema.fields = [
            self._make_field("id", "long"),
            self._make_field("name", "string"),
            self._make_field("value", "double"),
        ]

        schema = build_delta_schema_from_spark(spark_schema)

        assert schema["type"] == "struct"
        assert len(schema["fields"]) == 3

        assert schema["fields"][0] == {
            "name": "id",
            "type": "long",
            "nullable": True,
            "metadata": {},
        }
        assert schema["fields"][1] == {
            "name": "name",
            "type": "string",
            "nullable": True,
            "metadata": {},
        }
        assert schema["fields"][2] == {
            "name": "value",
            "type": "double",
            "nullable": True,
            "metadata": {},
        }

    def test_decimal_type(self):
        """Test decimal type preserves precision and scale."""
        spark_schema = MagicMock()
        spark_schema.fields = [
            self._make_field("amount", "decimal(18,4)"),
        ]

        schema = build_delta_schema_from_spark(spark_schema)

        assert schema["fields"][0]["type"] == "decimal(18,4)"

    def test_complex_types(self):
        """Test complex Spark types produce Delta protocol JSON objects."""
        spark_schema = MagicMock()
        spark_schema.fields = [
            self._make_field("tags", {
                "type": "array",
                "elementType": "string",
                "containsNull": True,
            }),
            self._make_field("props", {
                "type": "map",
                "keyType": "string",
                "valueType": "integer",
                "valueContainsNull": True,
            }),
        ]

        schema = build_delta_schema_from_spark(spark_schema)

        assert schema["fields"][0] == {
            "name": "tags",
            "type": {
                "type": "array",
                "elementType": "string",
                "containsNull": True,
            },
            "nullable": True,
            "metadata": {},
        }
        assert schema["fields"][1] == {
            "name": "props",
            "type": {
                "type": "map",
                "keyType": "string",
                "valueType": "integer",
                "valueContainsNull": True,
            },
            "nullable": True,
            "metadata": {},
        }

    def test_struct_type(self):
        """Test struct type produces nested Delta protocol JSON."""
        spark_schema = MagicMock()
        spark_schema.fields = [
            self._make_field("address", {
                "type": "struct",
                "fields": [
                    {"name": "street", "type": "string", "nullable": True, "metadata": {}},
                    {"name": "city", "type": "string", "nullable": True, "metadata": {}},
                ],
            }),
        ]

        schema = build_delta_schema_from_spark(spark_schema)

        assert schema["fields"][0]["type"] == {
            "type": "struct",
            "fields": [
                {"name": "street", "type": "string", "nullable": True, "metadata": {}},
                {"name": "city", "type": "string", "nullable": True, "metadata": {}},
            ],
        }

    def test_empty_schema_raises_value_error(self):
        """Test that empty schema raises ValueError."""
        spark_schema = MagicMock()
        spark_schema.fields = []

        with pytest.raises(ValueError, match="no fields"):
            build_delta_schema_from_spark(spark_schema)

    def test_nullable_from_field(self):
        """Test that nullable is taken from StructField, not hardcoded."""
        spark_schema = MagicMock()
        spark_schema.fields = [
            self._make_field("required_col", "string", nullable=False),
            self._make_field("optional_col", "string", nullable=True),
        ]

        schema = build_delta_schema_from_spark(spark_schema)

        assert schema["fields"][0]["nullable"] is False
        assert schema["fields"][1]["nullable"] is True
