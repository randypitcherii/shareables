"""Integration tests against real Google Sheets and Databricks.

These tests require:
1. A .env file with valid credentials
2. Google Sheets accessible by the service account
3. Databricks workspace access

Tests run against 3 sample spreadsheets from the service account's accessible sheets.

Run with: uv run pytest tests/test_integration.py -v
"""

import pytest
from conftest import get_databricks_session


class TestIntegrationGetSchema:
    """Integration tests for schema inference."""

    @pytest.mark.parametrize("sheet", [0, 1, 2])
    def test_get_schema_from_real_sheet(self, gsheet_reader, sample_sheets, sheet):
        """Should infer schema from real Google Sheets."""
        if sheet >= len(sample_sheets):
            pytest.skip(f"Only {len(sample_sheets)} sheets available, skipping sheet {sheet}")
        
        sheet_info = sample_sheets[sheet]
        schema = gsheet_reader.get_schema(sheet_id=sheet_info["id"])

        # Should return non-empty schema
        assert len(schema) > 0, f"Schema should not be empty for sheet '{sheet_info['name']}'"
        # Each column should have required keys
        for col in schema:
            assert "name" in col
            assert "sql_name" in col
            assert "type" in col
            assert col["type"] in ("STRING", "BIGINT", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP")


class TestIntegrationCreateView:
    """Integration tests for view creation."""

    @pytest.mark.parametrize("sheet", [0, 1, 2])
    def test_create_view_in_databricks(self, gsheet_reader, sample_sheets, integration_config, sheet):
        """Should create a real view in Databricks for each sample sheet."""
        if sheet >= len(sample_sheets):
            pytest.skip(f"Only {len(sample_sheets)} sheets available, skipping sheet {sheet}")
        
        # Check reader's actual auth type (not env vars) since create_view requires secrets
        if gsheet_reader._secret_scope is None:
            pytest.skip("View creation requires secret_scope/secret_key")

        sheet_info = sample_sheets[sheet]
        
        # Use a unique table name to avoid conflicts
        import time
        table_name = f"integration_test_{sheet_info['name'].lower().replace(' ', '_')}_{int(time.time())}"

        try:
            view_name = gsheet_reader.create_view(
                sheet_id=sheet_info["id"],
                catalog=integration_config["catalog"],
                schema=integration_config["schema"],
                table_name=table_name,
            )

            # Verify view was created
            assert view_name == f"{integration_config['catalog']}.{integration_config['schema']}.{table_name}"

            # Query the view
            spark = get_databricks_session()
            df = spark.sql(f"SELECT * FROM {view_name} LIMIT 5")

            # Should have columns (not just json_data)
            assert len(df.columns) > 0, f"View should have columns for sheet '{sheet_info['name']}'"
            assert "json_data" not in df.columns

        finally:
            # Cleanup: drop the test view
            try:
                spark = get_databricks_session()
                spark.sql(f"DROP VIEW IF EXISTS {integration_config['catalog']}.{integration_config['schema']}.{table_name}")
            except Exception:
                pass  # Best effort cleanup


class TestIntegrationPreviewSql:
    """Integration tests for SQL preview."""

    @pytest.mark.parametrize("sheet", [0, 1, 2])
    def test_preview_sql_shows_correct_structure(self, gsheet_reader, sample_sheets, integration_config, sheet):
        """Should generate valid SQL for each sample sheet."""
        if sheet >= len(sample_sheets):
            pytest.skip(f"Only {len(sample_sheets)} sheets available, skipping sheet {sheet}")
        
        # Check reader's actual auth type (not env vars) since preview_sql requires secrets
        if gsheet_reader._secret_scope is None:
            pytest.skip("preview_sql requires secret_scope/secret_key")

        sheet_info = sample_sheets[sheet]
        sql = gsheet_reader.preview_sql(
            sheet_id=sheet_info["id"],
            catalog=integration_config["catalog"],
            schema=integration_config["schema"],
            table_name="preview_test",
        )

        assert "schema_sql" in sql
        assert "udtf_sql" in sql
        assert "view_sql" in sql

        assert "CREATE SCHEMA IF NOT EXISTS" in sql["schema_sql"]
        assert "CREATE OR REPLACE FUNCTION" in sql["udtf_sql"]
        assert "CREATE OR REPLACE VIEW" in sql["view_sql"]
