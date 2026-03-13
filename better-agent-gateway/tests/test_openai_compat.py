"""Tests for OpenAI-compatible /v1/models response format.

Cursor and the OpenAI SDK expect the standard OpenAI list format:
  {"object": "list", "data": [{"id": "...", "object": "model", ...}]}

These tests verify our /v1/models endpoint returns this format while
also preserving backward-compatible aliases/endpoints metadata.
"""
from fastapi.testclient import TestClient

from app import app

client = TestClient(app)


def test_models_response_has_openai_list_format():
    """GET /api/v1/models returns top-level 'object' and 'data' keys."""
    response = client.get("/api/v1/models")
    assert response.status_code == 200
    body = response.json()
    assert body["object"] == "list", "top-level 'object' must be 'list'"
    assert isinstance(body["data"], list), "'data' must be a list"


def test_models_data_items_have_openai_model_schema():
    """Each item in 'data' must have id, object, created, owned_by."""
    # Seed the registry so there's at least one model
    from server.alias_registry import get_registry
    registry = get_registry()
    registry.refresh_from_endpoints([
        {"name": "databricks-claude-sonnet-4-6", "state": {"ready": "READY"}},
    ])

    response = client.get("/api/v1/models")
    body = response.json()
    assert len(body["data"]) > 0, "data should contain at least one model"

    for model in body["data"]:
        assert "id" in model, "each model must have 'id'"
        assert model["object"] == "model", "each model object must be 'model'"
        assert "created" in model, "each model must have 'created'"
        assert "owned_by" in model, "each model must have 'owned_by'"


def test_models_includes_aliases_and_endpoints():
    """Both aliases (as model entries) and raw endpoints appear in data."""
    from server.alias_registry import get_registry
    registry = get_registry()
    registry.refresh_from_endpoints([
        {"name": "databricks-claude-sonnet-4-6", "state": {"ready": "READY"}},
    ])

    response = client.get("/api/v1/models")
    body = response.json()
    model_ids = {m["id"] for m in body["data"]}
    # The endpoint itself should appear
    assert "databricks-claude-sonnet-4-6" in model_ids
    # The alias (claude-sonnet-latest) should also appear
    assert "claude-sonnet-latest" in model_ids


def test_models_preserves_backward_compat_keys():
    """Response still includes 'aliases' and 'endpoints' for the frontend."""
    from server.alias_registry import get_registry
    registry = get_registry()
    registry.refresh_from_endpoints([
        {"name": "databricks-claude-sonnet-4-6", "state": {"ready": "READY"}},
    ])

    response = client.get("/api/v1/models")
    body = response.json()
    assert "aliases" in body, "backward-compat 'aliases' key must be present"
    assert "endpoints" in body, "backward-compat 'endpoints' key must be present"
    assert body["aliases"]["claude-sonnet-latest"] == "databricks-claude-sonnet-4-6"
    assert "databricks-claude-sonnet-4-6" in body["endpoints"]


def test_openai_sdk_models_response_format():
    """Validate the response can be parsed as an OpenAI models list response.

    The OpenAI SDK expects: {"object": "list", "data": [...]} where each
    data item has at minimum: id, object, created, owned_by.
    """
    from server.alias_registry import get_registry
    registry = get_registry()
    registry.refresh_from_endpoints([
        {"name": "databricks-claude-sonnet-4-6", "state": {"ready": "READY"}},
        {"name": "databricks-gpt-4o", "state": {"ready": "READY"}},
    ])

    response = client.get("/api/v1/models")
    body = response.json()

    # Validate full OpenAI schema
    assert body["object"] == "list"
    assert isinstance(body["data"], list)
    assert len(body["data"]) >= 2  # at least the 2 endpoints

    for item in body["data"]:
        assert isinstance(item["id"], str)
        assert item["object"] == "model"
        assert isinstance(item["created"], int)
        assert isinstance(item["owned_by"], str)


def test_cursor_config_models_match_openai_models():
    """Fetch cursor config, build an OpenAI-style client config, and verify
    the models listed in cursor config appear in /v1/models."""
    from server.alias_registry import get_registry
    registry = get_registry()
    registry.refresh_from_endpoints([
        {"name": "databricks-claude-sonnet-4-6", "state": {"ready": "READY"}},
    ])

    # Get cursor config
    config_resp = client.get("/api/v1/config/cursor")
    assert config_resp.status_code == 200, "cursor config endpoint must exist"
    config = config_resp.json()

    # Cursor config should list models (nested under config.models)
    assert "config" in config, "cursor config must include 'config'"
    assert "models" in config["config"], "cursor config.config must include 'models'"
    cursor_models = config["config"]["models"]
    assert len(cursor_models) > 0, "cursor config must list at least one model"

    # Now verify those models appear in /v1/models
    models_resp = client.get("/api/v1/models")
    models_body = models_resp.json()
    available_ids = {m["id"] for m in models_body["data"]}

    for model_name in cursor_models:
        assert model_name in available_ids, (
            f"cursor config model '{model_name}' must appear in /v1/models data"
        )
