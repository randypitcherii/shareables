"""Tests for schema inference."""

import pytest
from databricks_gsheets_reader.schema import (
    infer_column_type,
    infer_schema,
    sanitize_column_name,
    sanitize_table_name,
)


class TestInferColumnType:
    """Test type inference for individual values."""

    def test_infers_string_for_text(self):
        """Text values should be STRING."""
        assert infer_column_type(["hello", "world"]) == "STRING"
        assert infer_column_type(["Alice", "Bob", "Charlie"]) == "STRING"

    def test_infers_double_for_decimals(self):
        """Decimal numbers should be DOUBLE."""
        assert infer_column_type(["1.5", "2.7", "3.14"]) == "DOUBLE"
        assert infer_column_type(["0.0", "-1.5", "100.001"]) == "DOUBLE"

    def test_infers_bigint_for_integers(self):
        """Integer values should be BIGINT."""
        assert infer_column_type(["1", "2", "3"]) == "BIGINT"
        assert infer_column_type(["0", "-100", "999999"]) == "BIGINT"

    def test_infers_boolean_for_true_false(self):
        """Boolean-like values should be BOOLEAN."""
        assert infer_column_type(["true", "false", "TRUE"]) == "BOOLEAN"
        assert infer_column_type(["True", "False"]) == "BOOLEAN"

    def test_infers_date_for_iso_dates(self):
        """ISO date strings should be DATE."""
        assert infer_column_type(["2024-01-15", "2024-12-31"]) == "DATE"

    def test_infers_timestamp_for_iso_timestamps(self):
        """ISO timestamp strings should be TIMESTAMP."""
        assert infer_column_type(["2024-01-15T10:30:00", "2024-12-31T23:59:59"]) == "TIMESTAMP"
        assert infer_column_type(["2024-01-15 10:30:00", "2024-12-31 23:59:59"]) == "TIMESTAMP"

    def test_defaults_to_string_for_mixed_types(self):
        """Mixed types should default to STRING."""
        assert infer_column_type(["1", "hello", "3"]) == "STRING"
        assert infer_column_type(["1.5", "2", "text"]) == "STRING"

    def test_handles_empty_and_none_values(self):
        """Should skip empty/None values when inferring."""
        assert infer_column_type(["1", "", "2", None, "3"]) == "BIGINT"
        assert infer_column_type(["", None, ""]) == "STRING"

    def test_conservative_string_fallback(self):
        """Ambiguous cases should fall back to STRING."""
        # Could be phone number, not integer
        assert infer_column_type(["123-456-7890"]) == "STRING"
        # Could be ID with leading zeros
        assert infer_column_type(["001", "002"]) == "STRING"


class TestSanitizeColumnName:
    """Test column name sanitization."""

    def test_replaces_spaces_with_underscores(self):
        assert sanitize_column_name("First Name") == "first_name"
        assert sanitize_column_name("User Email Address") == "user_email_address"

    def test_replaces_dashes_with_underscores(self):
        assert sanitize_column_name("first-name") == "first_name"

    def test_removes_special_characters(self):
        assert sanitize_column_name("price ($)") == "price"
        assert sanitize_column_name("email@domain") == "emaildomain"

    def test_lowercases(self):
        assert sanitize_column_name("FirstName") == "firstname"
        assert sanitize_column_name("TOTAL") == "total"

    def test_handles_leading_numbers(self):
        assert sanitize_column_name("1st_place") == "_1st_place"

    def test_collapses_multiple_underscores(self):
        assert sanitize_column_name("first  name") == "first_name"
        assert sanitize_column_name("a--b__c") == "a_b_c"


class TestSanitizeTableName:
    """Test table name sanitization."""

    def test_sanitizes_worksheet_names(self):
        assert sanitize_table_name("Sales Data 2024") == "sales_data_2024"
        assert sanitize_table_name("Q1-Results") == "q1_results"
        assert sanitize_table_name("Sheet 1/2") == "sheet_1_2"


class TestInferSchema:
    """Test full schema inference from data."""

    def test_infers_schema_from_rows(self):
        headers = ["id", "name", "score", "active"]
        rows = [
            {"id": "1", "name": "Alice", "score": "95.5", "active": "true"},
            {"id": "2", "name": "Bob", "score": "87.0", "active": "false"},
        ]

        schema = infer_schema(headers, rows)

        assert schema == [
            {"name": "id", "sql_name": "id", "type": "BIGINT"},
            {"name": "name", "sql_name": "name", "type": "STRING"},
            {"name": "score", "sql_name": "score", "type": "DOUBLE"},
            {"name": "active", "sql_name": "active", "type": "BOOLEAN"},
        ]

    def test_sanitizes_column_names(self):
        headers = ["User ID", "First Name", "Is Active?"]
        rows = [{"User ID": "1", "First Name": "Alice", "Is Active?": "yes"}]

        schema = infer_schema(headers, rows)

        assert schema[0]["sql_name"] == "user_id"
        assert schema[1]["sql_name"] == "first_name"
        assert schema[2]["sql_name"] == "is_active"

    def test_handles_empty_rows(self):
        headers = ["id", "name"]
        rows = []

        schema = infer_schema(headers, rows)

        # With no data, all columns default to STRING
        assert schema[0]["type"] == "STRING"
        assert schema[1]["type"] == "STRING"
