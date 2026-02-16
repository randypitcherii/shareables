"""Unit tests for schema.py functions.

Tests the schema inference functions for both Glue-based and Spark-based paths:
1. build_delta_schema_from_glue() - Converts Glue column definitions to Delta schema
2. build_delta_schema_from_spark() - Converts Spark StructType to Delta schema
"""

from unittest.mock import MagicMock

import pytest

from hive_to_delta.schema import (
    build_delta_schema_from_glue,
    build_delta_schema_from_spark,
    _normalize_glue_type,
    _map_glue_to_delta_type,
    GLUE_TO_DELTA_TYPE_MAP,
)


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


# =============================================================================
# Tests for build_delta_schema_from_spark()
# =============================================================================


class TestBuildDeltaSchemaFromSpark:
    """Tests for build_delta_schema_from_spark function."""

    def _make_field(self, name, type_string, nullable=True):
        field = MagicMock()
        field.name = name
        field.dataType.simpleString.return_value = type_string
        field.nullable = nullable
        return field

    def test_simple_types(self):
        """Test simple Spark types (string, long, double)."""
        spark_schema = MagicMock()
        spark_schema.fields = [
            self._make_field("id", "bigint"),
            self._make_field("name", "string"),
            self._make_field("value", "double"),
        ]

        schema = build_delta_schema_from_spark(spark_schema)

        assert schema["type"] == "struct"
        assert len(schema["fields"]) == 3

        assert schema["fields"][0] == {
            "name": "id",
            "type": "bigint",
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
        """Test complex Spark types (array<string>, map<string,int>)."""
        spark_schema = MagicMock()
        spark_schema.fields = [
            self._make_field("tags", "array<string>"),
            self._make_field("props", "map<string,int>"),
        ]

        schema = build_delta_schema_from_spark(spark_schema)

        assert schema["fields"][0] == {
            "name": "tags",
            "type": "array<string>",
            "nullable": True,
            "metadata": {},
        }
        assert schema["fields"][1] == {
            "name": "props",
            "type": "map<string,int>",
            "nullable": True,
            "metadata": {},
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
