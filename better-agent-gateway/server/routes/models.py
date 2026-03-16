from fastapi import APIRouter, Request

router = APIRouter()


def _get_alias_registry():
    from ..alias_registry import get_registry
    return get_registry()


@router.get("/v1/models")
def list_models():
    registry = _get_alias_registry()
    aliases = registry.list_aliases()
    endpoints = registry.list_endpoints()

    # Build OpenAI-standard model objects for all endpoints and aliases
    seen: set[str] = set()
    data: list[dict] = []

    for alias_name in sorted(aliases.keys()):
        if alias_name not in seen:
            seen.add(alias_name)
            data.append({
                "id": alias_name,
                "object": "model",
                "created": 0,
                "owned_by": "databricks",
            })

    for ep_name in endpoints:
        if ep_name not in seen:
            seen.add(ep_name)
            data.append({
                "id": ep_name,
                "object": "model",
                "created": 0,
                "owned_by": "databricks",
            })

    return {
        "object": "list",
        "data": data,
        "aliases": aliases,
        "endpoints": endpoints,
    }


@router.get("/v1/config/cursor")
def cursor_config(request: Request):
    """Return a Cursor-compatible configuration snippet.

    Lists all available model IDs (aliases + endpoints) so Cursor
    users can configure the gateway as an OpenAI-compatible provider.
    """
    registry = _get_alias_registry()
    aliases = registry.list_aliases()
    endpoints = registry.list_endpoints()

    # Collect all usable model names: aliases first, then endpoints
    models: list[str] = sorted(aliases.keys()) + [
        ep for ep in endpoints if ep not in aliases.values()
    ]

    base_url = str(request.base_url).rstrip("/") + "/api/v1"

    return {
        "models": models,
        "base_url": base_url,
        "provider": "databricks",
    }
