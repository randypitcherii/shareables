"""Tests for the db module and /api/v1/db/health endpoint."""

from pathlib import Path
from unittest.mock import MagicMock, patch
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(autouse=True)
def _reset_token_cache():
    """Clear the db module's cached token between tests."""
    import db

    db._cached_token = None
    db._cached_token_expiry = 0.0
    yield
    db._cached_token = None
    db._cached_token_expiry = 0.0


@pytest.fixture
def _pg_env(monkeypatch):
    """Set up the required environment variables for Lakebase connectivity."""
    monkeypatch.setenv("DATABRICKS_HOST", "workspace.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "test-client-secret")
    monkeypatch.setenv("PGHOST", "instance-abc.database.cloud.databricks.com")
    monkeypatch.setenv("PGPORT", "5432")
    monkeypatch.setenv("PGDATABASE", "postgres")
    monkeypatch.setenv("PGSSLMODE", "require")


def _mock_urlopen(token: str = "mock-access-token", expires_in: int = 3600):
    """Return a mock for urllib.request.urlopen that returns a token response."""
    import json

    mock_response = MagicMock()
    mock_response.read.return_value = json.dumps({
        "access_token": token,
        "token_type": "Bearer",
        "expires_in": expires_in,
    }).encode()
    mock_response.__enter__ = MagicMock(return_value=mock_response)
    mock_response.__exit__ = MagicMock(return_value=False)
    return MagicMock(return_value=mock_response)


class TestGetM2MToken:
    def test_fetches_token_from_oidc_endpoint(self, _pg_env):
        import db

        mock_open = _mock_urlopen("test-token-123")
        with patch.object(db, "urlopen", mock_open):
            token = db._get_m2m_token()

        assert token == "test-token-123"
        call_args = mock_open.call_args
        req = call_args[0][0]
        assert "oidc/v1/token" in req.full_url
        assert req.has_header("Authorization")

    def test_caches_token_on_subsequent_calls(self, _pg_env):
        import db

        mock_open = _mock_urlopen("cached-token")
        with patch.object(db, "urlopen", mock_open):
            first = db._get_m2m_token()
            second = db._get_m2m_token()

        assert first == second == "cached-token"
        assert mock_open.call_count == 1

    def test_refreshes_expired_token(self, _pg_env):
        import db

        mock_open = _mock_urlopen("fresh-token", expires_in=3600)
        with patch.object(db, "urlopen", mock_open):
            db._cached_token = "stale-token"
            db._cached_token_expiry = time.time() - 10
            token = db._get_m2m_token()

        assert token == "fresh-token"
        assert mock_open.call_count == 1


class TestGetConnection:
    def test_returns_connection_with_correct_params(self, _pg_env):
        import db

        mock_connect = MagicMock()
        mock_open = _mock_urlopen("conn-token")
        with (
            patch("psycopg2.connect", mock_connect),
            patch.object(db, "urlopen", mock_open),
        ):
            db.get_connection()

        mock_connect.assert_called_once()
        kwargs = mock_connect.call_args[1]
        assert kwargs["host"] == "instance-abc.database.cloud.databricks.com"
        assert kwargs["port"] == 5432
        assert kwargs["dbname"] == "postgres"
        assert kwargs["sslmode"] == "require"
        assert kwargs["user"] == "test-client-id"
        assert kwargs["password"] == "conn-token"


class TestCheckConnectivity:
    def test_returns_ok_on_success(self, _pg_env):
        import db

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("test-user", "postgres", "PostgreSQL 16.11")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(db, "get_connection", return_value=mock_conn):
            result = db.check_connectivity()

        assert result["ok"] is True
        assert result["current_user"] == "test-user"
        assert result["database"] == "postgres"
        assert result["pg_version"] == "PostgreSQL 16.11"
        assert result["error"] is None
        assert isinstance(result["latency_ms"], int)
        mock_conn.close.assert_called_once()

    def test_returns_error_on_failure(self, _pg_env):
        import db

        with patch.object(db, "get_connection", side_effect=Exception("Connection refused")):
            result = db.check_connectivity()

        assert result["ok"] is False
        assert "Connection refused" in result["error"]
        assert result["current_user"] is None


class TestDbHealthEndpoint:
    def test_db_health_endpoint_returns_connectivity_result(self, _pg_env):
        from fastapi.testclient import TestClient
        from app import app
        import db

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("sp-user", "postgres", "PostgreSQL 16.11")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        client = TestClient(app)
        with patch.object(db, "get_connection", return_value=mock_conn):
            response = client.get("/api/v1/db/health")

        assert response.status_code == 200
        payload = response.json()
        assert payload["ok"] is True
        assert payload["database"] == "postgres"

    def test_db_health_endpoint_surfaces_errors(self, _pg_env):
        from fastapi.testclient import TestClient
        from app import app
        import db

        client = TestClient(app)
        with patch.object(db, "get_connection", side_effect=Exception("no pg")):
            response = client.get("/api/v1/db/health")

        assert response.status_code == 200
        payload = response.json()
        assert payload["ok"] is False
        assert "no pg" in payload["error"]
