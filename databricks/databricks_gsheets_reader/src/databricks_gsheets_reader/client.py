"""Google Sheets API client."""

from __future__ import annotations

import json
import urllib.request
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks_gsheets_reader.auth import GoogleAuth


class GoogleSheetsClient:
    """Client for reading Google Sheets data."""

    BASE_URL = "https://sheets.googleapis.com/v4/spreadsheets"

    def __init__(self, auth: GoogleAuth) -> None:
        """Initialize client with authentication.

        Args:
            auth: GoogleAuth instance for API authentication
        """
        self._auth = auth

    def get_sheet_metadata(self, sheet_id: str) -> dict:
        """Get spreadsheet metadata including title and sheet names.

        Args:
            sheet_id: Google Sheets ID from URL

        Returns:
            Dict with 'title' and 'sheets' keys
        """
        url = f"{self.BASE_URL}/{sheet_id}?fields=properties.title,sheets.properties.title"
        data = self._make_request(url)

        return {
            "title": data.get("properties", {}).get("title", ""),
            "sheets": [
                {"title": sheet.get("properties", {}).get("title", "")}
                for sheet in data.get("sheets", [])
            ],
        }

    def get_sheet_data(
        self,
        sheet_id: str,
        range_name: str = "A:ZZ",
    ) -> tuple[list[str], list[dict]]:
        """Get sheet data as headers and row dictionaries.

        Args:
            sheet_id: Google Sheets ID from URL
            range_name: A1 notation range (default: all columns)

        Returns:
            Tuple of (headers list, rows as list of dicts)
        """
        url = f"{self.BASE_URL}/{sheet_id}/values/{range_name}"
        data = self._make_request(url)

        values = data.get("values", [])
        if not values:
            return [], []

        headers = values[0]
        rows = []

        for row_values in values[1:]:
            row_dict = {
                header: (row_values[i] if i < len(row_values) else None)
                for i, header in enumerate(headers)
            }
            rows.append(row_dict)

        return headers, rows

    def get_sheet_metadata_and_data(
        self,
        sheet_id: str,
        range_name: str = "A:ZZ",
    ) -> tuple[dict, list[str], list[dict]]:
        """Get both metadata and data in a single API call.

        Uses spreadsheets.get with includeGridData to fetch everything at once.

        Args:
            sheet_id: Google Sheets ID from URL
            range_name: A1 notation range (default: all columns)

        Returns:
            Tuple of (metadata dict, headers list, rows as list of dicts)
        """
        # Single API call with includeGridData=true and ranges parameter
        url = f"{self.BASE_URL}/{sheet_id}?includeGridData=true&ranges={range_name}"
        data = self._make_request(url)

        metadata = {
            "title": data.get("properties", {}).get("title", ""),
            "sheets": [
                {"title": sheet.get("properties", {}).get("title", "")}
                for sheet in data.get("sheets", [])
            ],
        }

        # Extract values from grid data
        sheets = data.get("sheets", [])
        if not sheets:
            return metadata, [], []

        grid_data = sheets[0].get("data", [])
        if not grid_data:
            return metadata, [], []

        row_data = grid_data[0].get("rowData", [])
        if not row_data:
            return metadata, [], []

        # Extract headers from first row
        headers = []
        first_row = row_data[0].get("values", [])
        for cell in first_row:
            cell_value = cell.get("effectiveValue", {})
            value = cell_value.get("stringValue") or cell_value.get("numberValue") or cell_value.get("boolValue") or ""
            headers.append(str(value) if value != "" else "")

        # Extract data rows
        rows = []
        for row in row_data[1:]:
            row_values = row.get("values", [])
            row_dict = {}
            for i, header in enumerate(headers):
                if i < len(row_values):
                    cell = row_values[i]
                    cell_value = cell.get("effectiveValue", {})
                    # Get the formatted value for consistency with values API
                    value = cell.get("formattedValue", "")
                    row_dict[header] = value if value else None
                else:
                    row_dict[header] = None
            rows.append(row_dict)

        return metadata, headers, rows

    def _make_request(self, url: str, timeout: int = 300) -> dict:
        """Make authenticated request to Google Sheets API.

        Args:
            url: Full URL to request
            timeout: Request timeout in seconds (default: 300)

        Returns:
            Parsed JSON response
        """
        token = self._auth.get_access_token()
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Bearer {token}")

        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
