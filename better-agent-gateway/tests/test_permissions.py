"""Tests for the permissions comparison endpoint."""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.routes.permissions import (
    _get_sp_token,
    _truncate_names,
    router,
)
import server.routes.permissions as perm_mod


@pytest.fixture(autouse=True)
def _reset_sp_cache():
    """Reset SP token cache before each test."""
    perm_mod._sp_token = None
    perm_mod._sp_token_expires_at = 0.0


@pytest.fixture
def app():
    app = FastAPI()
    app.include_router(router, prefix="/api")
    return app


@pytest.fixture
def client(app):
    return TestClient(app)


# --- _truncate_names ---


def test_truncate_names_short_list():
    names = ["a", "b", "c"]
    assert _truncate_names(names) == ["a", "b", "c"]


def test_truncate_names_at_limit():
    names = [f"name_{i}" for i in range(20)]
    assert _truncate_names(names) == names


def test_truncate_names_over_limit():
    names = [f"name_{i}" for i in range(25)]
    result = _truncate_names(names)
    assert len(result) == 21
    assert result[-1] == "...and 5 more"


# --- _get_sp_token ---


@pytest.mark.asyncio
async def test_get_sp_token_missing_env(monkeypatch):
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
    monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
    with pytest.raises(RuntimeError, match="Missing"):
        await _get_sp_token()


@pytest.mark.asyncio
async def test_get_sp_token_caches(monkeypatch):
    perm_mod._sp_token = "cached-token"
    perm_mod._sp_token_expires_at = time.time() + 600
    token = await _get_sp_token()
    assert token == "cached-token"


@pytest.mark.asyncio
async def test_get_sp_token_fetches_new(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csecret")

    mock_response = MagicMock()
    mock_response.json.return_value = {"access_token": "new-sp-token"}
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("server.routes.permissions.httpx.AsyncClient", return_value=mock_client):
        token = await _get_sp_token()

    assert token == "new-sp-token"
    assert perm_mod._sp_token == "new-sp-token"
    assert perm_mod._sp_token_expires_at > time.time()


# --- comparison endpoint ---


def test_comparison_no_obo_token(client, monkeypatch):
    """Without OBO token, OBO results should show error; SP should still attempt."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
    monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)

    resp = client.get("/api/v1/permissions/comparison")
    assert resp.status_code == 200
    data = resp.json()
    assert data["obo_user"]["catalogs"]["error"] == "No OBO token available"
    # SP should also error because no client credentials
    assert data["app_sp"]["catalogs"]["error"] is not None


def test_comparison_with_obo_token(client, monkeypatch):
    """With OBO token, both sides should attempt API calls."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
    monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)

    resp = client.get(
        "/api/v1/permissions/comparison",
        headers={"X-Forwarded-Access-Token": "fake-obo-token"},
    )
    assert resp.status_code == 200
    data = resp.json()
    # OBO calls will fail (no real endpoint) but should return error, not crash
    assert "catalogs" in data["obo_user"]
    assert "warehouses" in data["obo_user"]
    assert "serving_endpoints" in data["obo_user"]
    assert "catalogs" in data["app_sp"]


def test_comparison_response_shape(client, monkeypatch):
    """Verify the full response shape matches the contract."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csecret")

    resp = client.get("/api/v1/permissions/comparison")
    assert resp.status_code == 200
    data = resp.json()

    for actor in ("obo_user", "app_sp"):
        assert actor in data
        for resource in ("catalogs", "warehouses", "serving_endpoints"):
            assert resource in data[actor]
            cell = data[actor][resource]
            assert "count" in cell
            assert "names" in cell
            assert "error" in cell
