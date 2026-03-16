"""Tests for gateway → OpenAI model format translation."""

from databricks_agent_proxy.models import gateway_to_openai


def test_aliases_become_model_ids():
    gateway = {
        "aliases": {"gpt-4": "ep-gpt4", "claude": "ep-claude"},
        "endpoints": ["ep-gpt4", "ep-claude"],
    }
    result = gateway_to_openai(gateway)

    assert result["object"] == "list"
    ids = [m["id"] for m in result["data"]]
    # Aliases should appear first
    assert "gpt-4" in ids
    assert "claude" in ids


def test_endpoints_without_aliases_included():
    gateway = {
        "aliases": {"gpt-4": "ep-gpt4"},
        "endpoints": ["ep-gpt4", "ep-extra"],
    }
    result = gateway_to_openai(gateway)
    ids = [m["id"] for m in result["data"]]
    assert "gpt-4" in ids
    assert "ep-extra" in ids


def test_empty_gateway_response():
    result = gateway_to_openai({})
    assert result == {"object": "list", "data": []}


def test_model_object_shape():
    gateway = {"aliases": {"my-model": "ep"}, "endpoints": []}
    result = gateway_to_openai(gateway)
    model = result["data"][0]
    assert model["id"] == "my-model"
    assert model["object"] == "model"
    assert model["created"] == 0
    assert model["owned_by"] == "databricks"


def test_no_duplicates():
    """If an alias name matches an endpoint name, it should appear only once."""
    gateway = {
        "aliases": {"shared-name": "ep"},
        "endpoints": ["shared-name"],
    }
    result = gateway_to_openai(gateway)
    ids = [m["id"] for m in result["data"]]
    assert ids.count("shared-name") == 1
