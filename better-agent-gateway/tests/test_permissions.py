"""Tests for the permissions comparison endpoint."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.routes.permissions import (
    CHAT_CHECK_MODEL,
    _truncate_names,
    router,
)
from server.utils import sanitize_error


@pytest.fixture
def app():
    app = FastAPI()
    app.include_router(router, prefix="/api")
    return app


@pytest.fixture
def client(app):
    return TestClient(app)


def _make_mock_user(user_name="test@example.com", display_name="Test User"):
    """Create a mock user object with user_name and display_name."""
    user = MagicMock()
    user.user_name = user_name
    user.display_name = display_name
    return user


def _make_mock_client(catalogs=None, warehouses=None, endpoints=None,
                      user_name="test@example.com", display_name="Test User"):
    """Create a mock WorkspaceClient with configurable list responses."""
    mock = MagicMock()
    mock.catalogs.list.return_value = catalogs or []
    mock.warehouses.list.return_value = warehouses or []
    mock.serving_endpoints.list.return_value = endpoints or []
    mock.current_user.me.return_value = _make_mock_user(user_name, display_name)
    mock.config.authenticate.return_value = {"Authorization": "Bearer fake"}
    return mock


def _make_error_client(error_msg="Connection refused"):
    """Create a mock WorkspaceClient that raises on every call."""
    mock = MagicMock()
    mock.catalogs.list.side_effect = Exception(error_msg)
    mock.warehouses.list.side_effect = Exception(error_msg)
    mock.serving_endpoints.list.side_effect = Exception(error_msg)
    mock.current_user.me.side_effect = Exception(error_msg)
    mock.config.authenticate.side_effect = Exception(error_msg)
    return mock


def _mock_httpx_success():
    """Return a mock httpx response for a successful chat completion."""
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {
        "choices": [{"message": {"content": "Hello to you!"}}],
    }
    return resp


def _mock_httpx_error(status_code=403):
    """Return a mock httpx response for an HTTP error."""
    resp = MagicMock()
    resp.status_code = status_code
    return resp


# --- sanitize_error ---


def testsanitize_error_clean_message():
    """Clean message passes through unchanged."""
    assert sanitize_error("Something went wrong") == "Something went wrong"


def testsanitize_error_strips_config_suffix():
    """Message with '. Config: ...' gets truncated at '. Config:'."""
    msg = "Auth failed. Config: host=https://x.com, client_id=abc, client_secret=***"
    assert sanitize_error(msg) == "Auth failed"


def testsanitize_error_strips_reqid():
    """Message with '[ReqId: ...]' gets the ReqId stripped."""
    msg = "Permission denied [ReqId: abc-123-def]"
    assert sanitize_error(msg) == "Permission denied"


def testsanitize_error_strips_both_config_and_reqid():
    """Message with both Config and ReqId gets both stripped."""
    msg = "Bad request [ReqId: xyz-789]. Config: host=https://x.com, auth_type=oauth"
    assert sanitize_error(msg) == "Bad request"


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


# --- _chat_completion_check ---


def test_chat_completion_check_success():
    """httpx returns 200 with valid chat response."""
    mock_client = _make_mock_client()

    with patch("httpx.post", return_value=_mock_httpx_success()):
        from server.routes.permissions import _chat_completion_check
        result = _chat_completion_check(mock_client)

    assert result["success"] is True
    assert result["model"] == CHAT_CHECK_MODEL
    assert result["response"] == "Hello to you!"
    assert result["error"] is None


def test_chat_completion_check_http_error():
    """httpx returns 403 → error with HTTP status."""
    mock_client = _make_mock_client()

    with patch("httpx.post", return_value=_mock_httpx_error(403)):
        from server.routes.permissions import _chat_completion_check
        result = _chat_completion_check(mock_client)

    assert result["success"] is False
    assert result["model"] == CHAT_CHECK_MODEL
    assert result["response"] is None
    assert result["error"] == "HTTP 403"


def test_chat_completion_check_exception():
    """httpx raises an exception → error is captured."""
    mock_client = _make_mock_client()

    with patch("httpx.post", side_effect=Exception("Connection timeout")):
        from server.routes.permissions import _chat_completion_check
        result = _chat_completion_check(mock_client)

    assert result["success"] is False
    assert result["model"] == CHAT_CHECK_MODEL
    assert result["response"] is None
    assert result["error"] == "Connection timeout"


# --- comparison endpoint ---


def test_comparison_no_obo_token(client, monkeypatch):
    """Without OBO token, OBO results should show error; SP errors without credentials."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
    monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)

    with patch("server.routes.permissions._sp_client", side_effect=RuntimeError("No SP creds")):
        resp = client.get("/api/v1/permissions/comparison")

    assert resp.status_code == 200
    data = resp.json()
    assert data["obo_user"]["current_user"]["error"] == "No OBO token available"
    assert data["obo_user"]["catalogs"]["error"] == "No OBO token available"
    # chat_completion should also report no OBO token
    chat = data["obo_user"]["chat_completion"]
    assert chat["success"] is False
    assert chat["error"] == "No OBO token available"
    assert chat["model"] == CHAT_CHECK_MODEL
    assert data["app_sp"]["catalogs"]["error"] is not None


def test_comparison_response_shape(client, monkeypatch):
    """Verify the full response shape matches the contract."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")

    mock_obo = _make_mock_client()
    mock_sp = _make_mock_client()

    with patch("server.routes.permissions._obo_client", return_value=mock_obo), \
         patch("server.routes.permissions._sp_client", return_value=mock_sp), \
         patch("httpx.post", return_value=_mock_httpx_success()):
        resp = client.get(
            "/api/v1/permissions/comparison",
            headers={"X-Forwarded-Access-Token": "fake-token"},
        )

    assert resp.status_code == 200
    data = resp.json()

    for actor in ("obo_user", "app_sp"):
        assert actor in data
        # Check current_user shape
        assert "current_user" in data[actor]
        user_cell = data[actor]["current_user"]
        assert "username" in user_cell
        assert "display_name" in user_cell
        assert "error" in user_cell
        # Check resource shapes
        for resource in ("catalogs", "warehouses", "serving_endpoints"):
            assert resource in data[actor]
            cell = data[actor][resource]
            assert "count" in cell
            assert "names" in cell
            assert "error" in cell
        # Check chat_completion shape
        assert "chat_completion" in data[actor]
        chat = data[actor]["chat_completion"]
        assert "success" in chat
        assert "model" in chat
        assert "response" in chat
        assert "error" in chat


def test_comparison_obo_sees_more_than_sp(client, monkeypatch):
    """OBO user sees full access, SP sees limited — the core use case."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")

    # OBO user has broad access
    obo_catalog = MagicMock(); obo_catalog.name = "main"
    obo_catalog2 = MagicMock(); obo_catalog2.name = "dev"
    obo_wh = MagicMock(); obo_wh.name = "serverless-wh"
    obo_ep = MagicMock(); obo_ep.name = "databricks-claude-sonnet-4"
    obo_ep2 = MagicMock(); obo_ep2.name = "databricks-gpt-4o"

    mock_obo = _make_mock_client(
        catalogs=[obo_catalog, obo_catalog2],
        warehouses=[obo_wh],
        endpoints=[obo_ep, obo_ep2],
        user_name="randy@databricks.com",
        display_name="Randy Pitcher",
    )

    # SP has limited access
    sp_ep = MagicMock(); sp_ep.name = "databricks-claude-sonnet-4"
    mock_sp = _make_mock_client(
        endpoints=[sp_ep],
        user_name="app-sp-id-12345",
        display_name="better-agent-gateway SP",
    )

    with patch("server.routes.permissions._obo_client", return_value=mock_obo), \
         patch("server.routes.permissions._sp_client", return_value=mock_sp), \
         patch("httpx.post", return_value=_mock_httpx_success()):
        resp = client.get(
            "/api/v1/permissions/comparison",
            headers={"X-Forwarded-Access-Token": "fake-token"},
        )

    assert resp.status_code == 200
    data = resp.json()

    # Verify different user identities
    assert data["obo_user"]["current_user"]["username"] == "randy@databricks.com"
    assert data["obo_user"]["current_user"]["display_name"] == "Randy Pitcher"
    assert data["obo_user"]["current_user"]["error"] is None
    assert data["app_sp"]["current_user"]["username"] == "app-sp-id-12345"
    assert data["app_sp"]["current_user"]["display_name"] == "better-agent-gateway SP"
    assert data["app_sp"]["current_user"]["error"] is None

    # OBO user sees everything
    assert data["obo_user"]["catalogs"]["count"] == 2
    assert "dev" in data["obo_user"]["catalogs"]["names"]
    assert "main" in data["obo_user"]["catalogs"]["names"]
    assert data["obo_user"]["warehouses"]["count"] == 1
    assert data["obo_user"]["serving_endpoints"]["count"] == 2

    # SP sees only serving endpoint
    assert data["app_sp"]["catalogs"]["count"] == 0
    assert data["app_sp"]["warehouses"]["count"] == 0
    assert data["app_sp"]["serving_endpoints"]["count"] == 1
    assert data["app_sp"]["serving_endpoints"]["names"] == ["databricks-claude-sonnet-4"]

    # Both chat completions succeeded (mocked)
    assert data["obo_user"]["chat_completion"]["success"] is True
    assert data["obo_user"]["chat_completion"]["model"] == CHAT_CHECK_MODEL
    assert data["app_sp"]["chat_completion"]["success"] is True
    assert data["app_sp"]["chat_completion"]["model"] == CHAT_CHECK_MODEL


def test_comparison_graceful_error_handling(client, monkeypatch):
    """If SDK calls fail, errors are returned without crashing."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")

    mock_obo = _make_error_client("OBO auth failed")
    mock_sp = _make_error_client("SP auth failed")

    with patch("server.routes.permissions._obo_client", return_value=mock_obo), \
         patch("server.routes.permissions._sp_client", return_value=mock_sp):
        resp = client.get(
            "/api/v1/permissions/comparison",
            headers={"X-Forwarded-Access-Token": "fake-token"},
        )

    assert resp.status_code == 200
    data = resp.json()

    for actor in ("obo_user", "app_sp"):
        assert data[actor]["current_user"]["username"] is None
        assert data[actor]["current_user"]["error"] is not None
        for resource in ("catalogs", "warehouses", "serving_endpoints"):
            assert data[actor][resource]["count"] == 0
            assert data[actor][resource]["error"] is not None
        # chat_completion should also have an error
        chat = data[actor]["chat_completion"]
        assert chat["success"] is False
        assert chat["error"] is not None
        assert chat["model"] == CHAT_CHECK_MODEL
        assert chat["response"] is None
