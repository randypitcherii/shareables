"""Tests for main GSheetReader class."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from databricks_gsheets_reader.reader import GSheetReader


class TestGSheetReaderInit:
    """Test GSheetReader initialization."""

    def test_init_with_credentials_json(self):
        """Should initialize with credentials_json."""
        reader = GSheetReader(credentials_json='{"type": "service_account"}')
        assert reader._auth is not None

    def test_init_with_secret_scope_and_key(self):
        """Should initialize with secret_scope and secret_key."""
        reader = GSheetReader(secret_scope="scope", secret_key="key")
        assert reader._auth is not None

    def test_init_requires_auth(self):
        """Should raise ValueError without auth."""
        with pytest.raises(ValueError):
            GSheetReader()


class TestGSheetReaderCreateView:
    """Test create_view method."""

    @patch("databricks_gsheets_reader.reader.DatabricksSession")
    @patch("databricks_gsheets_reader.reader.GoogleSheetsClient")
    def test_create_view_with_explicit_table_name(self, mock_client_cls, mock_session):
        """Should use provided table_name."""
        # Setup mocks
        mock_client = Mock()
        mock_client.get_sheet_metadata.return_value = {"title": "My Sheet", "sheets": []}
        mock_client.get_sheet_data.return_value = (
            ["id", "name"],
            [{"id": "1", "name": "Alice"}],
        )
        mock_client_cls.return_value = mock_client

        mock_spark = Mock()
        mock_session.builder.getOrCreate.return_value = mock_spark

        reader = GSheetReader(secret_scope="scope", secret_key="key")

        reader.create_view(
            sheet_id="sheet123",
            catalog="cat",
            schema="sch",
            table_name="custom_table",
        )

        # Verify SQL was executed with custom table name
        calls = mock_spark.sql.call_args_list
        sql_calls = [str(c) for c in calls]
        assert any("custom_table" in s for s in sql_calls)

    @patch("databricks_gsheets_reader.reader.DatabricksSession")
    @patch("databricks_gsheets_reader.reader.GoogleSheetsClient")
    def test_create_view_derives_table_name_from_sheet(self, mock_client_cls, mock_session):
        """Should derive table_name from sheet title when not provided."""
        mock_client = Mock()
        mock_client.get_sheet_metadata.return_value = {"title": "Sales Data 2024", "sheets": [{"title": "Sheet1"}]}
        mock_client.get_sheet_data.return_value = (
            ["id"],
            [{"id": "1"}],
        )
        mock_client_cls.return_value = mock_client

        mock_spark = Mock()
        mock_session.builder.getOrCreate.return_value = mock_spark

        reader = GSheetReader(secret_scope="scope", secret_key="key")

        reader.create_view(
            sheet_id="sheet123",
            catalog="cat",
            schema="sch",
        )

        # Should sanitize "Sales Data 2024" to "sales_data_2024"
        calls = mock_spark.sql.call_args_list
        sql_calls = [str(c) for c in calls]
        assert any("sales_data_2024" in s for s in sql_calls)

    @patch("databricks_gsheets_reader.reader.DatabricksSession")
    @patch("databricks_gsheets_reader.reader.GoogleSheetsClient")
    def test_create_view_creates_schema_and_udtf(self, mock_client_cls, mock_session):
        """Should execute schema, UDTF, and view creation SQL."""
        mock_client = Mock()
        mock_client.get_sheet_metadata.return_value = {"title": "Test", "sheets": []}
        mock_client.get_sheet_data.return_value = (
            ["id"],
            [{"id": "1"}],
        )
        mock_client_cls.return_value = mock_client

        mock_spark = Mock()
        mock_session.builder.getOrCreate.return_value = mock_spark

        reader = GSheetReader(secret_scope="scope", secret_key="key")
        reader.create_view(
            sheet_id="sheet123",
            catalog="cat",
            schema="sch",
            table_name="tbl",
        )

        # Should have called sql() multiple times
        assert mock_spark.sql.call_count >= 3  # schema, udtf, view


class TestGSheetReaderGetSchema:
    """Test get_schema method."""

    @patch("databricks_gsheets_reader.reader.GoogleSheetsClient")
    def test_get_schema_returns_inferred_types(self, mock_client_cls):
        """Should return inferred schema without creating view."""
        mock_client = Mock()
        mock_client.get_sheet_data.return_value = (
            ["id", "name", "score"],
            [
                {"id": "1", "name": "Alice", "score": "95.5"},
                {"id": "2", "name": "Bob", "score": "87.0"},
            ],
        )
        mock_client_cls.return_value = mock_client

        reader = GSheetReader(credentials_json='{"type": "service_account"}')
        schema = reader.get_schema(sheet_id="sheet123")

        assert len(schema) == 3
        assert schema[0]["type"] == "BIGINT"
        assert schema[1]["type"] == "STRING"
        assert schema[2]["type"] == "DOUBLE"
