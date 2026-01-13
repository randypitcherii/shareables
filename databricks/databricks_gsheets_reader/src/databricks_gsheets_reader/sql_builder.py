"""SQL generation for Databricks views."""

from __future__ import annotations

import re


class ViewBuilder:
    """Builds SQL statements for Google Sheets views."""

    def __init__(
        self,
        *,
        catalog: str,
        schema_name: str,
        table_name: str,
        sheet_id: str,
        sheet_name: str,
        secret_scope: str,
        secret_key: str,
        columns: list[dict[str, str]],
    ) -> None:
        """Initialize view builder.

        Args:
            catalog: Unity Catalog name
            schema_name: Schema name within catalog
            table_name: Target view/table name
            sheet_id: Google Sheets ID
            sheet_name: Human-readable sheet name for comments
            secret_scope: Databricks secret scope for GCP credentials
            secret_key: Databricks secret key for GCP credentials
            columns: List of column definitions from schema inference
        """
        self.catalog = catalog
        self.schema_name = schema_name
        self.table_name = table_name
        self.sheet_id = sheet_id
        self.sheet_name = sheet_name
        self.secret_scope = secret_scope
        self.secret_key = secret_key
        self.columns = columns

    @property
    def full_table_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.catalog}.{self.schema_name}.{self.table_name}"

    @property
    def full_function_name(self) -> str:
        """Get fully qualified UDTF name."""
        return f"{self.catalog}.{self.schema_name}.read_google_sheet"

    def build_udtf_sql(self) -> str:
        """Generate CREATE FUNCTION SQL for the UDTF.

        Returns:
            SQL string to create the UDTF
        """
        return f'''
CREATE OR REPLACE FUNCTION {self.full_function_name}(sheet_id STRING, secret_value STRING)
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
'''.strip()

    def build_create_view_sql(self) -> str:
        """Generate CREATE VIEW SQL with typed columns.

        Returns:
            SQL string to create the view
        """
        column_expressions = []
        for col in self.columns:
            # Escape column name for JSON path if it contains special chars
            json_col = self._escape_json_key(col["name"])
            sql_name = col["sql_name"]
            sql_type = col["type"]
            column_expressions.append(f"json_data:{json_col}::{sql_type} AS {sql_name}")

        columns_sql = ",\n    ".join(column_expressions)

        sheet_url = f"https://docs.google.com/spreadsheets/d/{self.sheet_id}"
        comment = f"Synced from Google Sheet: [{self.sheet_name}]({sheet_url})"

        return f'''
CREATE OR REPLACE VIEW {self.full_table_name}
COMMENT '{comment}'
AS
SELECT
    {columns_sql}
FROM {self.full_function_name}(
    '{self.sheet_id}',
    secret('{self.secret_scope}', '{self.secret_key}')
)
'''.strip()

    def build_create_schema_sql(self) -> str:
        """Generate CREATE SCHEMA IF NOT EXISTS SQL.

        Returns:
            SQL string to create the schema
        """
        return f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema_name}"

    def _escape_json_key(self, key: str) -> str:
        """Escape JSON key for Databricks JSON path notation.

        Args:
            key: Original column name

        Returns:
            Escaped key for use in json_data:key notation
        """
        # If key contains special characters, wrap in backticks
        if re.search(r"[^a-zA-Z0-9_]", key):
            return f"`{key}`"
        return key
