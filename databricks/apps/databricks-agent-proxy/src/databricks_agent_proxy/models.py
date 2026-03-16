"""Translate gateway model format to OpenAI-compatible format."""

from __future__ import annotations

from typing import Any


def gateway_to_openai(gateway_response: dict[str, Any]) -> dict[str, Any]:
    """Convert gateway model listing to OpenAI /v1/models format.

    Gateway format:
        {"aliases": {"name": "endpoint_name"}, "endpoints": ["endpoint_name"]}

    OpenAI format:
        {"object": "list", "data": [{"id": "name", "object": "model", ...}]}
    """
    models: list[dict[str, Any]] = []
    seen: set[str] = set()

    # Aliases are the user-friendly names (what users add in Cursor)
    aliases = gateway_response.get("aliases", {})
    for alias_name in sorted(aliases.keys()):
        if alias_name not in seen:
            models.append(_make_model(alias_name))
            seen.add(alias_name)

    # Also include raw endpoint names not already covered by aliases
    endpoints = gateway_response.get("endpoints", [])
    for endpoint in sorted(endpoints):
        if endpoint not in seen:
            models.append(_make_model(endpoint))
            seen.add(endpoint)

    return {"object": "list", "data": models}


def _make_model(model_id: str) -> dict[str, Any]:
    """Create an OpenAI-format model object."""
    return {
        "id": model_id,
        "object": "model",
        "created": 0,
        "owned_by": "databricks",
    }
