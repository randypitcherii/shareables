"""Tests for Google Sheets client."""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock
from databricks_gsheets_reader.client import GoogleSheetsClient
from databricks_gsheets_reader.auth import GoogleAuth


@pytest.fixture
def mock_auth():
    """Create a mock GoogleAuth that returns a fake token."""
    auth = Mock(spec=GoogleAuth)
    auth.get_access_token.return_value = "fake-token-123"
    return auth


class TestGoogleSheetsClient:
    """Test GoogleSheetsClient."""

    def test_init_stores_auth(self, mock_auth):
        """Should store auth instance."""
        client = GoogleSheetsClient(mock_auth)
        assert client._auth is mock_auth

    def test_get_sheet_metadata_returns_title(self, mock_auth):
        """Should return sheet title from metadata."""
        client = GoogleSheetsClient(mock_auth)

        mock_response = {
            "properties": {"title": "My Test Sheet"},
            "sheets": [{"properties": {"title": "Sheet1"}}]
        }

        with patch("databricks_gsheets_reader.client.urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__ = Mock(
                return_value=Mock(read=Mock(return_value=json.dumps(mock_response).encode()))
            )
            mock_urlopen.return_value.__exit__ = Mock(return_value=False)

            metadata = client.get_sheet_metadata("sheet-id-123")

        assert metadata["title"] == "My Test Sheet"
        assert metadata["sheets"][0]["title"] == "Sheet1"

    def test_get_sheet_data_returns_headers_and_rows(self, mock_auth):
        """Should return parsed headers and rows."""
        client = GoogleSheetsClient(mock_auth)

        mock_response = {
            "values": [
                ["id", "name", "email"],
                ["1", "Alice", "alice@example.com"],
                ["2", "Bob", "bob@example.com"],
            ]
        }

        with patch("databricks_gsheets_reader.client.urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__ = Mock(
                return_value=Mock(read=Mock(return_value=json.dumps(mock_response).encode()))
            )
            mock_urlopen.return_value.__exit__ = Mock(return_value=False)

            headers, rows = client.get_sheet_data("sheet-id-123")

        assert headers == ["id", "name", "email"]
        assert len(rows) == 2
        assert rows[0] == {"id": "1", "name": "Alice", "email": "alice@example.com"}

    def test_get_sheet_data_handles_empty_sheet(self, mock_auth):
        """Should return empty lists for empty sheet."""
        client = GoogleSheetsClient(mock_auth)

        mock_response = {"values": []}

        with patch("databricks_gsheets_reader.client.urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__ = Mock(
                return_value=Mock(read=Mock(return_value=json.dumps(mock_response).encode()))
            )
            mock_urlopen.return_value.__exit__ = Mock(return_value=False)

            headers, rows = client.get_sheet_data("sheet-id-123")

        assert headers == []
        assert rows == []

    def test_get_sheet_data_handles_ragged_rows(self, mock_auth):
        """Should handle rows with fewer columns than headers."""
        client = GoogleSheetsClient(mock_auth)

        mock_response = {
            "values": [
                ["id", "name", "email"],
                ["1", "Alice"],  # Missing email
                ["2"],  # Missing name and email
            ]
        }

        with patch("databricks_gsheets_reader.client.urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__ = Mock(
                return_value=Mock(read=Mock(return_value=json.dumps(mock_response).encode()))
            )
            mock_urlopen.return_value.__exit__ = Mock(return_value=False)

            headers, rows = client.get_sheet_data("sheet-id-123")

        assert rows[0] == {"id": "1", "name": "Alice", "email": None}
        assert rows[1] == {"id": "2", "name": None, "email": None}
