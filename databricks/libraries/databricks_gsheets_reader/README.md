# Databricks Google Sheets Reader

Create typed Databricks views directly from Google Sheets. Views are refreshed automatically at query time—no ETL jobs required.

## How It Works

This library creates a SQL view in Unity Catalog that:
1. Calls a Python UDTF to fetch data from Google Sheets API
2. Parses the JSON response and infers column types
3. Returns typed columns (STRING, BIGINT, DOUBLE, BOOLEAN, DATE, TIMESTAMP)

The view reads live data from Google Sheets on every query. No data is stored in Databricks.

## Prerequisites

### 1. Google Cloud Service Account

Create a GCP service account with access to Google Sheets API:
1. Go to [GCP Console](https://console.cloud.google.com/) → IAM & Admin → Service Accounts
2. Create a new service account
3. Enable the **Google Sheets API** and **Google Drive API** for your project
4. Create a JSON key for the service account
5. Share your Google Sheets with the service account email (e.g., `my-sa@project.iam.gserviceaccount.com`)

### 2. Databricks Secret

Store the service account JSON in a Databricks secret:

```sql
-- Create a secret scope (if needed)
-- Note: This must be done via CLI or REST API

-- Store the JSON key
databricks secrets put-secret gcp-credentials service-account-json --string-value '{"type":"service_account",...}'
```

Or via Databricks CLI:
```bash
databricks secrets create-scope gcp-credentials
databricks secrets put-secret gcp-credentials service-account-json --string-value "$(cat service-account.json)"
```

## Installation

Install the wheel file in your Databricks notebook or job:

```python
%pip install /path/to/databricks_gsheets_reader-0.1.0-py3-none-any.whl
```

Or build from source:
```bash
cd databricks/databricks_gsheets_reader
uv build
```

## Quick Start

### Create a View from a Single Sheet

```python
from databricks_gsheets_reader import GSheetReader

reader = GSheetReader(
    secret_scope="gcp-credentials",
    secret_key="service-account-json"
)

# Create a view from a Google Sheet
view_name = reader.create_view(
    sheet_id="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",  # From sheet URL
    catalog="my_catalog",
    schema="my_schema",
    table_name="my_view"  # Optional - derived from sheet title if omitted
)

print(f"Created view: {view_name}")
```

### Sync All Accessible Sheets

```python
from databricks_gsheets_reader import GSheetReader

reader = GSheetReader(
    secret_scope="gcp-credentials",
    secret_key="service-account-json"
)

# Sync all sheets the service account can access
results = reader.sync_all_views(
    catalog="my_catalog",
    schema="google_sheets"
)

# Report results
successes = [r for r in results if r.error is None]
failures = [r for r in results if r.error is not None]

print(f"Synced {len(successes)}/{len(results)} sheets")

for r in failures:
    print(f"  Failed: {r.sheet_name} - {r.error}")
```

### Preview Schema Before Creating

```python
# See what columns and types would be inferred
schema = reader.get_schema(sheet_id="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms")
for col in schema:
    print(f"{col['sql_name']}: {col['type']}")
```

### Preview SQL Without Executing

```python
sql = reader.preview_sql(
    sheet_id="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
    catalog="my_catalog",
    schema="my_schema"
)

print(sql["view_sql"])
```

## API Reference

### GSheetReader

```python
GSheetReader(
    secret_scope: str,      # Databricks secret scope name
    secret_key: str         # Databricks secret key name
)
```

#### Methods

| Method | Description |
|--------|-------------|
| `create_view(sheet_id, catalog, schema, table_name=None)` | Create a typed view from a Google Sheet |
| `sync_all_views(catalog, schema, max_workers=5)` | Sync views for all accessible sheets in parallel |
| `list_sheets()` | List all Google Sheets accessible by the service account |
| `get_schema(sheet_id)` | Preview inferred schema without creating a view |
| `preview_sql(sheet_id, catalog, schema)` | Preview SQL that would be executed |

### SyncResult

Returned by `sync_all_views()`:

```python
@dataclass
class SyncResult:
    sheet_id: str           # Google Sheets ID
    sheet_name: str         # Human-readable sheet name
    view_fqn: str | None    # Fully qualified view name if successful
    error: str | None       # Error message if failed
```

## Type Inference

The library automatically infers SQL types from sheet data:

| Inferred Type | Pattern |
|--------------|---------|
| `BOOLEAN` | `true`, `false` (case-insensitive) |
| `TIMESTAMP` | `YYYY-MM-DDTHH:MM:SS` or `YYYY-MM-DD HH:MM:SS` |
| `DATE` | `YYYY-MM-DD` |
| `BIGINT` | Integers without leading zeros |
| `DOUBLE` | Decimal numbers |
| `STRING` | Everything else (default) |

Column names are sanitized to be SQL-safe (lowercase, underscores for spaces, etc.).

## Scheduled Sync with Databricks Jobs

Use the included notebook `Sync_GSheets_to_Views.ipynb` as a scheduled job:

1. Upload the notebook to your Databricks workspace
2. Create a job with parameters:
   - `catalog`: Target Unity Catalog name
   - `schema`: Target schema name
3. Schedule to run hourly, daily, etc.

The notebook syncs all accessible Google Sheets and reports success/failure counts.

## Running Tests

### Environment Variables

Create a `.env` file for local testing:

```bash
# Required for integration tests
TEST_CATALOG=your_catalog
TEST_SCHEMA_BASE=google_sheets_test

# Authentication (choose one)
GSHEETS_SECRET_SCOPE=gcp-credentials
GSHEETS_SECRET_KEY=service-account-json

# OR for local testing with JSON directly
# GSHEETS_CREDENTIALS_JSON='{"type":"service_account",...}'

# Optional
DATABRICKS_PROFILE=your_profile
TEST_CLEANUP_SCHEMA=true  # Set to 'false' to keep test schema for debugging
```

### Run Tests

```bash
cd databricks/databricks_gsheets_reader
uv run pytest
```

Tests create a unique schema per run (e.g., `google_sheets_test_a1b2c3d4`) and clean it up after completion.

## Limitations

- Only reads the first sheet/tab of each spreadsheet
- Maximum 26² columns (A:ZZ range)
- Views require Databricks secrets (cannot use raw credentials in view definitions)
- Schema inference samples up to 1000 rows by default


