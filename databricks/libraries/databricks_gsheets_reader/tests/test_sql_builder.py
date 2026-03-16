"""Tests for SQL view builder."""

import pytest
from databricks_gsheets_reader.sql_builder import ViewBuilder


class TestViewBuilder:
    """Test SQL view generation."""

    def test_generates_basic_view(self):
        """Should generate CREATE VIEW with column parsing."""
        schema = [
            {"name": "id", "sql_name": "id", "type": "BIGINT"},
            {"name": "name", "sql_name": "name", "type": "STRING"},
        ]

        builder = ViewBuilder(
            catalog="my_catalog",
            schema_name="my_schema",
            table_name="my_table",
            sheet_id="sheet123",
            sheet_name="My Test Sheet",
            secret_scope="gcp-creds",
            secret_key="service-account",
            columns=schema,
        )

        sql = builder.build_create_view_sql()

        assert "CREATE OR REPLACE VIEW my_catalog.my_schema.my_table" in sql
        assert "json_data:id::BIGINT AS id" in sql
        assert "json_data:name::STRING AS name" in sql
        assert "read_google_sheet" in sql
        assert "sheet123" in sql

    def test_generates_udtf_sql(self):
        """Should generate the UDTF creation SQL."""
        builder = ViewBuilder(
            catalog="cat",
            schema_name="sch",
            table_name="tbl",
            sheet_id="sheet123",
            sheet_name="Test Sheet",
            secret_scope="scope",
            secret_key="key",
            columns=[],
        )

        sql = builder.build_udtf_sql()

        assert "CREATE OR REPLACE FUNCTION" in sql
        assert "cat.sch.read_google_sheet" in sql
        assert "RETURNS TABLE" in sql
        assert "json_data STRING" in sql

    def test_handles_special_column_names(self):
        """Should properly escape column names in JSON path."""
        schema = [
            {"name": "User Name", "sql_name": "user_name", "type": "STRING"},
            {"name": "Price ($)", "sql_name": "price", "type": "DOUBLE"},
        ]

        builder = ViewBuilder(
            catalog="cat",
            schema_name="sch",
            table_name="tbl",
            sheet_id="sheet123",
            sheet_name="Test Sheet",
            secret_scope="scope",
            secret_key="key",
            columns=schema,
        )

        sql = builder.build_create_view_sql()

        # Original names in JSON path, sanitized as aliases
        assert 'json_data:`User Name`::STRING AS user_name' in sql
        assert 'json_data:`Price ($)`::DOUBLE AS price' in sql

    def test_full_qualified_names(self):
        """Should use fully qualified catalog.schema.table names."""
        builder = ViewBuilder(
            catalog="prod_catalog",
            schema_name="analytics",
            table_name="users",
            sheet_id="abc",
            sheet_name="Test Sheet",
            secret_scope="s",
            secret_key="k",
            columns=[{"name": "id", "sql_name": "id", "type": "STRING"}],
        )

        sql = builder.build_create_view_sql()

        assert "prod_catalog.analytics.users" in sql
        assert "prod_catalog.analytics.read_google_sheet" in sql

    def test_includes_comment_with_markdown_link(self):
        """Should include COMMENT clause with markdown link to Google Sheet."""
        builder = ViewBuilder(
            catalog="cat",
            schema_name="sch",
            table_name="tbl",
            sheet_id="1a2b3c4d5e",
            sheet_name="Sales Data Q4",
            secret_scope="scope",
            secret_key="key",
            columns=[{"name": "revenue", "sql_name": "revenue", "type": "DOUBLE"}],
        )

        sql = builder.build_create_view_sql()

        assert "COMMENT 'Synced from Google Sheet: [Sales Data Q4](https://docs.google.com/spreadsheets/d/1a2b3c4d5e)'" in sql
        assert "CREATE OR REPLACE VIEW cat.sch.tbl" in sql
        assert sql.index("COMMENT") < sql.index("AS")
