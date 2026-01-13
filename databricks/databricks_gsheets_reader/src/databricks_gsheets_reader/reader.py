"""Main GSheetReader class."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from databricks.connect import DatabricksSession

from databricks_gsheets_reader.auth import GoogleAuth
from databricks_gsheets_reader.client import GoogleSheetsClient
from databricks_gsheets_reader.schema import infer_schema, sanitize_table_name
from databricks_gsheets_reader.sql_builder import ViewBuilder

if TYPE_CHECKING:
    pass


@dataclass
class SyncResult:
    """Result from syncing a single sheet.

    Attributes:
        sheet_id: Google Sheets ID
        sheet_name: Name of the sheet
        view_fqn: Fully qualified view name if successful, None if error
        error: Error message if sync failed, None if successful
    """

    sheet_id: str
    sheet_name: str
    view_fqn: str | None
    error: str | None


class GSheetReader:
    """Create typed Databricks views from Google Sheets.

    Example:
        >>> reader = GSheetReader(secret_scope="gcp-creds", secret_key="service-account")
        >>> reader.create_view(
        ...     sheet_id="1abc...",
        ...     catalog="my_catalog",
        ...     schema="my_schema",
        ... )
    """

    def __init__(
        self,
        *,
        credentials_json: str | None = None,
        secret_scope: str | None = None,
        secret_key: str | None = None,
    ) -> None:
        """Initialize GSheetReader with authentication.

        Args:
            credentials_json: GCP service account JSON string
            secret_scope: Databricks secret scope name
            secret_key: Databricks secret key name

        Raises:
            ValueError: If auth configuration is invalid
        """
        self._auth = GoogleAuth(
            credentials_json=credentials_json,
            secret_scope=secret_scope,
            secret_key=secret_key,
        )
        self._client = GoogleSheetsClient(self._auth)
        self._secret_scope = secret_scope
        self._secret_key = secret_key

    def create_view(
        self,
        *,
        sheet_id: str,
        catalog: str,
        schema: str,
        table_name: str | None = None,
        sample_rows: int = 1000,
    ) -> str:
        """Create a typed Databricks view from a Google Sheet.

        Args:
            sheet_id: Google Sheets ID from URL
            catalog: Unity Catalog name
            schema: Schema name within catalog
            table_name: View name (derived from sheet title if not provided)
            sample_rows: Number of rows to sample for schema inference (default: 1000)

        Returns:
            Fully qualified view name that was created

        Raises:
            ValueError: If using credentials_json (must use secrets for views)
        """
        if self._secret_scope is None or self._secret_key is None:
            raise ValueError(
                "create_view requires secret_scope and secret_key for persistent views. "
                "Direct credentials_json cannot be used in view definitions."
            )

        # Get metadata first (fast)
        metadata = self._client.get_sheet_metadata(sheet_id)
        
        # Get limited sample of data for schema inference (much faster than fetching all)
        # The view will query all data at runtime through the UDTF
        range_name = f"A1:ZZ{sample_rows + 1}"  # +1 for header row
        headers, rows = self._client.get_sheet_data(sheet_id, range_name=range_name)

        # Determine table name
        if table_name is None:
            table_name = sanitize_table_name(metadata["title"])

        # Infer schema
        columns = infer_schema(headers, rows)

        # Build SQL
        builder = ViewBuilder(
            catalog=catalog,
            schema_name=schema,
            table_name=table_name,
            sheet_id=sheet_id,
            sheet_name=metadata["title"],
            secret_scope=self._secret_scope,
            secret_key=self._secret_key,
            columns=columns,
        )

        # Execute SQL
        spark = DatabricksSession.builder.getOrCreate()

        # Create schema if needed
        spark.sql(builder.build_create_schema_sql())

        # Create/update UDTF
        spark.sql(builder.build_udtf_sql())

        # Create/update view
        spark.sql(builder.build_create_view_sql())

        return builder.full_table_name

    def get_schema(self, *, sheet_id: str) -> list[dict[str, str]]:
        """Get inferred schema for a Google Sheet without creating a view.

        Useful for previewing what columns and types would be created.

        Args:
            sheet_id: Google Sheets ID from URL

        Returns:
            List of column definitions with 'name', 'sql_name', and 'type'
        """
        headers, rows = self._client.get_sheet_data(sheet_id)
        return infer_schema(headers, rows)

    def preview_sql(
        self,
        *,
        sheet_id: str,
        catalog: str,
        schema: str,
        table_name: str | None = None,
        sample_rows: int = 1000,
    ) -> dict[str, str]:
        """Preview the SQL that would be executed without running it.

        Args:
            sheet_id: Google Sheets ID from URL
            catalog: Unity Catalog name
            schema: Schema name within catalog
            table_name: View name (derived from sheet title if not provided)
            sample_rows: Number of rows to sample for schema inference (default: 1000)

        Returns:
            Dict with 'schema_sql', 'udtf_sql', and 'view_sql' keys
        """
        if self._secret_scope is None or self._secret_key is None:
            raise ValueError(
                "preview_sql requires secret_scope and secret_key"
            )

        # Get metadata first (fast)
        metadata = self._client.get_sheet_metadata(sheet_id)
        
        # Get limited sample of data for schema inference (much faster than fetching all)
        # Use values API which is faster than includeGridData
        range_name = f"A1:ZZ{sample_rows + 1}"  # +1 for header row
        headers, rows = self._client.get_sheet_data(sheet_id, range_name=range_name)

        if table_name is None:
            table_name = sanitize_table_name(metadata["title"])

        columns = infer_schema(headers, rows)

        builder = ViewBuilder(
            catalog=catalog,
            schema_name=schema,
            table_name=table_name,
            sheet_id=sheet_id,
            sheet_name=metadata["title"],
            secret_scope=self._secret_scope,
            secret_key=self._secret_key,
            columns=columns,
        )

        return {
            "schema_sql": builder.build_create_schema_sql(),
            "udtf_sql": builder.build_udtf_sql(),
            "view_sql": builder.build_create_view_sql(),
        }

    def list_sheets(self) -> list[dict[str, str]]:
        """List all Google Sheets accessible by the service account.

        Returns:
            List of dicts with 'id' and 'name' keys

        Example:
            >>> reader = GSheetReader(secret_scope="scope", secret_key="key")
            >>> sheets = reader.list_sheets()
            >>> for sheet in sheets:
            ...     print(f"{sheet['name']}: {sheet['id']}")
        """
        from googleapiclient.discovery import build

        creds = self._auth.get_credentials()
        drive = build('drive', 'v3', credentials=creds)

        sheets = []
        page_token = None

        while True:
            response = drive.files().list(
                q="mimeType='application/vnd.google-apps.spreadsheet'",
                spaces='drive',
                fields='nextPageToken, files(id, name)',
                pageToken=page_token
            ).execute()

            sheets.extend(response.get('files', []))
            page_token = response.get('nextPageToken')
            if not page_token:
                break

        return sheets

    def sync_all_views(
        self,
        *,
        catalog: str,
        schema: str,
        max_workers: int = 5,
    ) -> list[SyncResult]:
        """Sync views for all accessible Google Sheets in parallel.

        Continues on errors and collects results for all sheets.
        Handles duplicate sheet names by appending _2, _3, etc.

        Args:
            catalog: Unity Catalog name
            schema: Schema name within catalog
            max_workers: Maximum number of parallel workers (default: 5)

        Returns:
            List of SyncResult objects, one per sheet

        Example:
            >>> reader = GSheetReader(secret_scope="scope", secret_key="key")
            >>> results = reader.sync_all_views(catalog="cat", schema="sch")
            >>> successes = [r for r in results if r.error is None]
            >>> failures = [r for r in results if r.error is not None]
            >>> print(f"Synced {len(successes)}/{len(results)} sheets")
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        sheets = self.list_sheets()
        results = []

        # Pre-compute unique table names to handle duplicates
        name_counts: dict[str, int] = {}
        sheet_table_names: dict[str, str] = {}  # sheet_id -> unique_table_name

        for sheet in sheets:
            base_name = sanitize_table_name(sheet["name"])
            count = name_counts.get(base_name, 0) + 1
            name_counts[base_name] = count

            if count == 1:
                sheet_table_names[sheet["id"]] = base_name
            else:
                sheet_table_names[sheet["id"]] = f"{base_name}_{count}"

        # Create schema and shared UDTF once before parallel view creation
        spark = DatabricksSession.builder.getOrCreate()
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

        # Create the shared UDTF once (same for all sheets)
        udtf_sql = f'''
CREATE OR REPLACE FUNCTION {catalog}.{schema}.read_google_sheet(sheet_id STRING, secret_value STRING)
RETURNS TABLE (json_data STRING)
LANGUAGE PYTHON
HANDLER 'GoogleSheetUDTF'
AS $$
class GoogleSheetUDTF:
    def eval(self, sheet_id: str, secret_value: str):
        import json
        import urllib.request
        from google.oauth2 import service_account
        import google.auth.transport.requests

        service_account_info = json.loads(secret_value)
        scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        creds = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=scopes
        )

        auth_req = google.auth.transport.requests.Request()
        creds.refresh(auth_req)
        token = creds.token

        url = f"https://sheets.googleapis.com/v4/spreadsheets/{{sheet_id}}/values/A:ZZ"
        req = urllib.request.Request(url)
        req.add_header('Authorization', f'Bearer {{token}}')

        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode('utf-8'))

        if 'values' not in data or len(data['values']) == 0:
            return

        headers = data.get('values', [])[0]
        raw_rows = data.get('values', [])[1:]

        for row in raw_rows:
            row_dict = {{h: (row[i] if i < len(row) else None) for i, h in enumerate(headers)}}
            yield (json.dumps(row_dict),)
$$
'''
        spark.sql(udtf_sql)

        def sync_one(sheet: dict) -> SyncResult:
            try:
                table_name = sheet_table_names[sheet["id"]]
                view_fqn = self._create_view_only(
                    sheet_id=sheet["id"],
                    catalog=catalog,
                    schema=schema,
                    table_name=table_name,
                )
                return SyncResult(
                    sheet_id=sheet["id"],
                    sheet_name=sheet["name"],
                    view_fqn=view_fqn,
                    error=None,
                )
            except Exception as e:
                return SyncResult(
                    sheet_id=sheet["id"],
                    sheet_name=sheet["name"],
                    view_fqn=None,
                    error=str(e),
                )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(sync_one, sheet): sheet for sheet in sheets}
            for future in as_completed(futures):
                results.append(future.result())

        return results

    def _create_view_only(
        self,
        *,
        sheet_id: str,
        catalog: str,
        schema: str,
        table_name: str | None = None,
    ) -> str:
        """Create just the view (assumes schema and UDTF already exist).

        Internal method used by sync_all_views for parallel execution.
        """
        # Get sheet metadata and data in single API call
        metadata, headers, rows = self._client.get_sheet_metadata_and_data(sheet_id)

        # Determine table name
        if table_name is None:
            table_name = sanitize_table_name(metadata["title"])

        # Infer schema
        columns = infer_schema(headers, rows)

        # Build SQL
        builder = ViewBuilder(
            catalog=catalog,
            schema_name=schema,
            table_name=table_name,
            sheet_id=sheet_id,
            sheet_name=metadata["title"],
            secret_scope=self._secret_scope,
            secret_key=self._secret_key,
            columns=columns,
        )

        # Execute only view creation SQL
        spark = DatabricksSession.builder.getOrCreate()
        spark.sql(builder.build_create_view_sql())

        return builder.full_table_name
