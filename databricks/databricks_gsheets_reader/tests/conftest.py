"""Pytest configuration and fixtures."""

import os
import uuid
import pytest
from dotenv import load_dotenv


# Module-level storage for test session schema
_test_schema = None
_test_catalog = None


def get_databricks_session():
    """Get DatabricksSession, using DATABRICKS_PROFILE if set."""
    from databricks.connect import DatabricksSession
    
    builder = DatabricksSession.builder
    profile = os.getenv("DATABRICKS_PROFILE")
    
    if profile:
        builder = builder.profile(profile)
    
    return builder.getOrCreate()


def pytest_configure(config):
    """Load .env file for tests and generate unique test schema."""
    global _test_schema, _test_catalog
    
    load_dotenv()
    
    # Generate unique schema name from TEST_SCHEMA_BASE
    schema_base = os.getenv("TEST_SCHEMA_BASE")
    if schema_base:
        # Generate short UUID (8 characters) and append to base
        short_uuid = str(uuid.uuid4()).replace("-", "")[:8]
        _test_schema = f"{schema_base}_{short_uuid}"
        _test_catalog = os.getenv("TEST_CATALOG")


@pytest.fixture(scope="session")
def cleanup_test_schema():
    """Cleanup fixture that drops the test schema after all tests complete.
    
    Controlled by TEST_CLEANUP_SCHEMA env var (default: true).
    Set to 'false' to skip cleanup (useful for debugging).
    """
    yield  # Run tests first
    
    # Check if cleanup is enabled (default: True)
    cleanup_enabled = os.getenv("TEST_CLEANUP_SCHEMA", "true").lower() == "true"
    
    if not cleanup_enabled:
        return
    
    if _test_schema and _test_catalog:
        try:
            spark = get_databricks_session()
            spark.sql(f"DROP SCHEMA IF EXISTS {_test_catalog}.{_test_schema} CASCADE")
        except Exception:
            pass  # Best effort cleanup - don't fail if cleanup fails


@pytest.fixture(scope="session")
def gsheet_reader():
    """Create a GSheetReader instance for integration tests.
    
    Prioritizes secrets over JSON since create_view and preview_sql require secrets.
    """
    from databricks_gsheets_reader import GSheetReader
    
    # Check for auth - either secrets or JSON
    has_secrets = os.getenv("GSHEETS_SECRET_SCOPE") and os.getenv("GSHEETS_SECRET_KEY")
    has_json = os.getenv("GSHEETS_CREDENTIALS_JSON")

    if not (has_secrets or has_json):
        pytest.skip("No authentication configured (need GSHEETS_SECRET_SCOPE/KEY or GSHEETS_CREDENTIALS_JSON)")

    # Prioritize secrets since create_view and preview_sql require them
    if has_secrets:
        return GSheetReader(
            secret_scope=os.getenv("GSHEETS_SECRET_SCOPE"),
            secret_key=os.getenv("GSHEETS_SECRET_KEY"),
        )
    else:
        return GSheetReader(credentials_json=os.getenv("GSHEETS_CREDENTIALS_JSON"))


@pytest.fixture(scope="session")
def sample_sheets(gsheet_reader):
    """Get 3 sample spreadsheets accessible by the service account.
    
    Session-scoped to avoid multiple API calls to list sheets.
    """
    sheets = gsheet_reader.list_sheets()
    
    if len(sheets) == 0:
        pytest.skip("No Google Sheets found - service account has no access to any sheets")
    
    # Take first 3 sheets (or all if less than 3)
    return sheets[:3]


@pytest.fixture
def integration_config(cleanup_test_schema):
    """Get integration test configuration from environment.

    Skips test if required env vars not set.
    Uses unique schema generated from TEST_SCHEMA_BASE.
    """
    required_vars = [
        "TEST_CATALOG",
        "TEST_SCHEMA_BASE",
    ]

    # Check for auth - either secrets or JSON
    has_secrets = os.getenv("GSHEETS_SECRET_SCOPE") and os.getenv("GSHEETS_SECRET_KEY")
    has_json = os.getenv("GSHEETS_CREDENTIALS_JSON")

    if not (has_secrets or has_json):
        pytest.skip("No authentication configured (need GSHEETS_SECRET_SCOPE/KEY or GSHEETS_CREDENTIALS_JSON)")

    for var in required_vars:
        if not os.getenv(var):
            pytest.skip(f"Missing required env var: {var}")

    # Use generated unique schema
    if not _test_schema:
        pytest.skip("Test schema not initialized - check TEST_SCHEMA_BASE is set")

    return {
        "catalog": _test_catalog,
        "schema": _test_schema,
        "secret_scope": os.getenv("GSHEETS_SECRET_SCOPE"),
        "secret_key": os.getenv("GSHEETS_SECRET_KEY"),
        "credentials_json": os.getenv("GSHEETS_CREDENTIALS_JSON"),
    }
