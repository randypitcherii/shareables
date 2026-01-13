"""Tests for list_sheets method."""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock
from databricks_gsheets_reader.reader import GSheetReader


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


class TestListSheets:
    """Test list_sheets method with mocked Drive API."""

    @patch("googleapiclient.discovery.build")
    @patch("databricks_gsheets_reader.auth.service_account.Credentials.from_service_account_info")
    def test_list_sheets_returns_all_sheets(self, mock_creds, mock_build):
        """Should return all sheets from Drive API."""
        # Mock credentials
        mock_creds.return_value = Mock()

        # Mock Drive API response
        mock_drive = MagicMock()
        mock_files = mock_drive.files.return_value
        mock_list = mock_files.list.return_value
        mock_list.execute.return_value = {
            'files': [
                {'id': 'sheet1', 'name': 'Sales Data'},
                {'id': 'sheet2', 'name': 'Inventory'},
            ],
            'nextPageToken': None
        }
        mock_build.return_value = mock_drive

        reader = GSheetReader(credentials_json=FAKE_CREDS)
        sheets = reader.list_sheets()

        # Verify results
        assert len(sheets) == 2
        assert sheets[0] == {'id': 'sheet1', 'name': 'Sales Data'}
        assert sheets[1] == {'id': 'sheet2', 'name': 'Inventory'}

        # Verify Drive API was called correctly
        mock_build.assert_called_once()
        assert mock_build.call_args[0][0] == 'drive'
        assert mock_build.call_args[0][1] == 'v3'

        mock_files.list.assert_called_once()
        call_kwargs = mock_files.list.call_args[1]
        assert call_kwargs['q'] == "mimeType='application/vnd.google-apps.spreadsheet'"
        assert call_kwargs['spaces'] == 'drive'
        assert 'files(id, name)' in call_kwargs['fields']

    @patch("googleapiclient.discovery.build")
    @patch("databricks_gsheets_reader.auth.service_account.Credentials.from_service_account_info")
    def test_list_sheets_handles_pagination(self, mock_creds, mock_build):
        """Should handle paginated responses from Drive API."""
        # Mock credentials
        mock_creds.return_value = Mock()

        # Mock Drive API with paginated responses
        mock_drive = MagicMock()
        mock_files = mock_drive.files.return_value
        mock_list = mock_files.list.return_value

        # First page
        first_response = {
            'files': [
                {'id': 'sheet1', 'name': 'Sheet 1'},
                {'id': 'sheet2', 'name': 'Sheet 2'},
            ],
            'nextPageToken': 'token123'
        }

        # Second page (final)
        second_response = {
            'files': [
                {'id': 'sheet3', 'name': 'Sheet 3'},
            ],
            'nextPageToken': None
        }

        mock_list.execute.side_effect = [first_response, second_response]
        mock_build.return_value = mock_drive

        reader = GSheetReader(credentials_json=FAKE_CREDS)
        sheets = reader.list_sheets()

        # Should return all sheets from both pages
        assert len(sheets) == 3
        assert sheets[0]['name'] == 'Sheet 1'
        assert sheets[1]['name'] == 'Sheet 2'
        assert sheets[2]['name'] == 'Sheet 3'

        # Verify pagination was handled correctly
        assert mock_files.list.call_count == 2

        # First call should not have pageToken
        first_call = mock_files.list.call_args_list[0]
        assert first_call[1]['pageToken'] is None

        # Second call should have pageToken
        second_call = mock_files.list.call_args_list[1]
        assert second_call[1]['pageToken'] == 'token123'

    @patch("googleapiclient.discovery.build")
    @patch("databricks_gsheets_reader.auth.service_account.Credentials.from_service_account_info")
    def test_list_sheets_returns_empty_list_when_no_sheets(self, mock_creds, mock_build):
        """Should return empty list when no sheets found."""
        # Mock credentials
        mock_creds.return_value = Mock()

        mock_drive = MagicMock()
        mock_files = mock_drive.files.return_value
        mock_list = mock_files.list.return_value
        mock_list.execute.return_value = {
            'files': [],
            'nextPageToken': None
        }
        mock_build.return_value = mock_drive

        reader = GSheetReader(credentials_json=FAKE_CREDS)
        sheets = reader.list_sheets()

        assert sheets == []

    @patch("googleapiclient.discovery.build")
    @patch("databricks_gsheets_reader.auth.service_account.Credentials.from_service_account_info")
    def test_list_sheets_handles_missing_files_key(self, mock_creds, mock_build):
        """Should handle response without 'files' key gracefully."""
        # Mock credentials
        mock_creds.return_value = Mock()

        mock_drive = MagicMock()
        mock_files = mock_drive.files.return_value
        mock_list = mock_files.list.return_value
        mock_list.execute.return_value = {
            'nextPageToken': None
        }
        mock_build.return_value = mock_drive

        reader = GSheetReader(credentials_json=FAKE_CREDS)
        sheets = reader.list_sheets()

        assert sheets == []

    @patch("googleapiclient.discovery.build")
    @patch("databricks_gsheets_reader.auth.service_account.Credentials.from_service_account_info")
    def test_list_sheets_uses_credentials_from_auth(self, mock_creds, mock_build):
        """Should use credentials from GoogleAuth."""
        # Mock credentials
        mock_creds.return_value = Mock()

        mock_drive = MagicMock()
        mock_files = mock_drive.files.return_value
        mock_list = mock_files.list.return_value
        mock_list.execute.return_value = {'files': [], 'nextPageToken': None}
        mock_build.return_value = mock_drive

        reader = GSheetReader(credentials_json=FAKE_CREDS)
        reader.list_sheets()

        # Verify build was called with credentials
        assert mock_build.call_count == 1
        assert 'credentials' in mock_build.call_args[1]


class TestListSheetsIntegration:
    """Integration tests for list_sheets (requires .env)."""

    def test_list_sheets_from_real_drive_api(self, integration_config):
        """Should list real sheets from Google Drive."""
        if integration_config["credentials_json"]:
            reader = GSheetReader(
                credentials_json=integration_config["credentials_json"]
            )
        else:
            reader = GSheetReader(
                secret_scope=integration_config["secret_scope"],
                secret_key=integration_config["secret_key"],
            )

        sheets = reader.list_sheets()

        # Should return a list
        assert isinstance(sheets, list)

        # If there are sheets, verify structure
        if len(sheets) > 0:
            for sheet in sheets:
                assert 'id' in sheet
                assert 'name' in sheet
                assert isinstance(sheet['id'], str)
                assert isinstance(sheet['name'], str)
                assert len(sheet['id']) > 0
                assert len(sheet['name']) > 0
