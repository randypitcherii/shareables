"""Tests for sync_all_views method."""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock
from databricks_gsheets_reader.reader import GSheetReader, SyncResult


# Fake credentials for testing
FAKE_CREDS = json.dumps({
    "type": "service_account",
    "project_id": "test-project",
    "private_key_id": "key123",
    "private_key": "-----BEGIN RSA PRIVATE KEY-----\nFAKE_KEY_FOR_TESTING\n-----END RSA PRIVATE KEY-----\n",  # gitleaks:allow
    "client_email": "test@test-project.iam.gserviceaccount.com",
    "client_id": "123456789",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
})


class TestSyncAllViews:
    """Test sync_all_views method with mocked dependencies."""

    @patch("databricks_gsheets_reader.reader.DatabricksSession")
    @patch.object(GSheetReader, '_create_view_only')
    @patch.object(GSheetReader, 'list_sheets')
    def test_sync_all_views_returns_sync_results(self, mock_list_sheets, mock_create_view, mock_session):
        """Should sync all sheets and return SyncResult objects."""
        mock_session.builder.getOrCreate.return_value = Mock()

        mock_list_sheets.return_value = [
            {'id': 'sheet1', 'name': 'Sales Data'},
            {'id': 'sheet2', 'name': 'Inventory'},
        ]

        def create_view_side_effect(sheet_id, catalog, schema, table_name):
            return f"{catalog}.{schema}.{table_name}"

        mock_create_view.side_effect = create_view_side_effect

        reader = GSheetReader(secret_scope="scope", secret_key="key")
        results = reader.sync_all_views(catalog="cat", schema="sch")

        assert len(results) == 2
        assert all(isinstance(r, SyncResult) for r in results)

        result1 = next(r for r in results if r.sheet_id == 'sheet1')
        assert result1.sheet_name == 'Sales Data'
        assert result1.view_fqn == 'cat.sch.sales_data'
        assert result1.error is None

        result2 = next(r for r in results if r.sheet_id == 'sheet2')
        assert result2.sheet_name == 'Inventory'
        assert result2.view_fqn == 'cat.sch.inventory'
        assert result2.error is None

        assert mock_create_view.call_count == 2

    @patch("databricks_gsheets_reader.reader.DatabricksSession")
    @patch.object(GSheetReader, '_create_view_only')
    @patch.object(GSheetReader, 'list_sheets')
    def test_sync_all_views_continues_on_errors(self, mock_list_sheets, mock_create_view, mock_session):
        """Should continue syncing other sheets when one fails."""
        mock_session.builder.getOrCreate.return_value = Mock()

        mock_list_sheets.return_value = [
            {'id': 'sheet1', 'name': 'Good Sheet'},
            {'id': 'sheet2', 'name': 'Bad Sheet'},
            {'id': 'sheet3', 'name': 'Another Good Sheet'},
        ]

        def create_view_side_effect(sheet_id, catalog, schema, table_name):
            if sheet_id == 'sheet2':
                raise ValueError("Sheet2 has invalid data")
            return f"{catalog}.{schema}.{table_name}"

        mock_create_view.side_effect = create_view_side_effect

        reader = GSheetReader(secret_scope="scope", secret_key="key")
        results = reader.sync_all_views(catalog="cat", schema="sch")

        assert len(results) == 3
        assert mock_create_view.call_count == 3

        result1 = next(r for r in results if r.sheet_id == 'sheet1')
        assert result1.error is None
        assert result1.view_fqn is not None

        result3 = next(r for r in results if r.sheet_id == 'sheet3')
        assert result3.error is None
        assert result3.view_fqn is not None

        result2 = next(r for r in results if r.sheet_id == 'sheet2')
        assert result2.error == "Sheet2 has invalid data"
        assert result2.view_fqn is None

    @patch("databricks_gsheets_reader.reader.DatabricksSession")
    @patch.object(GSheetReader, '_create_view_only')
    @patch.object(GSheetReader, 'list_sheets')
    def test_sync_all_views_handles_empty_sheet_list(self, mock_list_sheets, mock_create_view, mock_session):
        """Should return empty list when no sheets found."""
        mock_session.builder.getOrCreate.return_value = Mock()
        mock_list_sheets.return_value = []

        reader = GSheetReader(secret_scope="scope", secret_key="key")
        results = reader.sync_all_views(catalog="cat", schema="sch")

        assert results == []
        mock_create_view.assert_not_called()

    @patch("databricks_gsheets_reader.reader.DatabricksSession")
    @patch.object(GSheetReader, '_create_view_only')
    @patch.object(GSheetReader, 'list_sheets')
    def test_sync_all_views_uses_custom_max_workers(self, mock_list_sheets, mock_create_view, mock_session):
        """Should respect custom max_workers parameter."""
        mock_session.builder.getOrCreate.return_value = Mock()

        mock_list_sheets.return_value = [
            {'id': f'sheet{i}', 'name': f'Sheet {i}'}
            for i in range(10)
        ]

        mock_create_view.return_value = "cat.sch.view"

        reader = GSheetReader(secret_scope="scope", secret_key="key")
        results = reader.sync_all_views(catalog="cat", schema="sch", max_workers=2)

        assert len(results) == 10
        assert all(r.error is None for r in results)

    @patch("databricks_gsheets_reader.reader.DatabricksSession")
    @patch.object(GSheetReader, '_create_view_only')
    @patch.object(GSheetReader, 'list_sheets')
    def test_sync_all_views_passes_catalog_and_schema(self, mock_list_sheets, mock_create_view, mock_session):
        """Should pass catalog and schema to _create_view_only."""
        mock_session.builder.getOrCreate.return_value = Mock()

        mock_list_sheets.return_value = [
            {'id': 'sheet1', 'name': 'Test Sheet'},
        ]

        mock_create_view.return_value = "my_cat.my_sch.test_sheet"

        reader = GSheetReader(secret_scope="scope", secret_key="key")
        reader.sync_all_views(catalog="my_cat", schema="my_sch")

        mock_create_view.assert_called_once_with(
            sheet_id='sheet1',
            catalog='my_cat',
            schema='my_sch',
            table_name='test_sheet',
        )

    @patch("databricks_gsheets_reader.reader.DatabricksSession")
    @patch.object(GSheetReader, '_create_view_only')
    @patch.object(GSheetReader, 'list_sheets')
    def test_sync_all_views_collects_error_messages(self, mock_list_sheets, mock_create_view, mock_session):
        """Should capture full error messages in SyncResult."""
        mock_session.builder.getOrCreate.return_value = Mock()

        mock_list_sheets.return_value = [
            {'id': 'sheet1', 'name': 'Bad Sheet'},
        ]

        error_msg = "Detailed error: API rate limit exceeded (429)"
        mock_create_view.side_effect = RuntimeError(error_msg)

        reader = GSheetReader(secret_scope="scope", secret_key="key")
        results = reader.sync_all_views(catalog="cat", schema="sch")

        assert len(results) == 1
        assert results[0].error == error_msg
        assert results[0].view_fqn is None

    @patch("databricks_gsheets_reader.reader.DatabricksSession")
    @patch.object(GSheetReader, '_create_view_only')
    @patch.object(GSheetReader, 'list_sheets')
    def test_sync_all_views_handles_duplicate_names(self, mock_list_sheets, mock_create_view, mock_session):
        """Should enumerate duplicate sheet names with _2, _3 suffixes."""
        mock_session.builder.getOrCreate.return_value = Mock()

        # Three sheets with the same name after sanitization
        mock_list_sheets.return_value = [
            {'id': 'sheet1', 'name': 'Test Data'},
            {'id': 'sheet2', 'name': 'Test Data'},  # Duplicate
            {'id': 'sheet3', 'name': 'Test Data'},  # Another duplicate
            {'id': 'sheet4', 'name': 'Other Sheet'},
        ]

        def create_view_side_effect(sheet_id, catalog, schema, table_name):
            return f"{catalog}.{schema}.{table_name}"

        mock_create_view.side_effect = create_view_side_effect

        reader = GSheetReader(secret_scope="scope", secret_key="key")
        results = reader.sync_all_views(catalog="cat", schema="sch")

        assert len(results) == 4

        # Check that unique names were assigned
        result1 = next(r for r in results if r.sheet_id == 'sheet1')
        assert result1.view_fqn == 'cat.sch.test_data'

        result2 = next(r for r in results if r.sheet_id == 'sheet2')
        assert result2.view_fqn == 'cat.sch.test_data_2'

        result3 = next(r for r in results if r.sheet_id == 'sheet3')
        assert result3.view_fqn == 'cat.sch.test_data_3'

        result4 = next(r for r in results if r.sheet_id == 'sheet4')
        assert result4.view_fqn == 'cat.sch.other_sheet'


class TestSyncAllViewsIntegration:
    """Integration tests for sync_all_views (requires .env)."""

    @pytest.mark.skip(reason="Integration test - requires real Databricks connection and cleanup")
    def test_sync_all_views_from_real_api(self, integration_config):
        """Should sync all sheets from Google Drive and create views.

        WARNING: This creates real views in Databricks. Only run manually with cleanup.
        """
        if integration_config["credentials_json"]:
            reader = GSheetReader(
                credentials_json=integration_config["credentials_json"]
            )
        else:
            reader = GSheetReader(
                secret_scope=integration_config["secret_scope"],
                secret_key=integration_config["secret_key"],
            )

        # Use test catalog/schema from config
        results = reader.sync_all_views(
            catalog=integration_config["catalog"],
            schema=integration_config["schema"],
            max_workers=3,
        )

        # Verify results structure
        assert isinstance(results, list)
        assert all(isinstance(r, SyncResult) for r in results)

        # Check that all results have required fields
        for result in results:
            assert result.sheet_id
            assert result.sheet_name
            # Either view_fqn or error should be set
            assert (result.view_fqn is not None) or (result.error is not None)

        # Report results
        successes = [r for r in results if r.error is None]
        failures = [r for r in results if r.error is not None]

        print(f"\nSync Results:")
        print(f"  Total: {len(results)}")
        print(f"  Success: {len(successes)}")
        print(f"  Failures: {len(failures)}")

        if failures:
            print("\nFailed sheets:")
            for f in failures:
                print(f"  - {f.sheet_name}: {f.error}")

        # NOTE: Manual cleanup required!
        # Run: DROP VIEW IF EXISTS <catalog>.<schema>.<view_name>;
        # for each view created
