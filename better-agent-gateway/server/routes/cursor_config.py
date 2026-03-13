from fastapi import APIRouter, Request

router = APIRouter()


def _get_alias_registry():
    from ..alias_registry import get_registry
    return get_registry()


def _get_gateway_base_url(request: Request) -> str:
    """Derive the public-facing gateway URL, respecting reverse proxy headers."""
    forwarded_host = request.headers.get("x-forwarded-host")
    forwarded_proto = request.headers.get("x-forwarded-proto", "https")
    if forwarded_host:
        return f"{forwarded_proto}://{forwarded_host}"
    base = request.base_url
    return f"{base.scheme}://{base.netloc}"


@router.get("/v1/config/cursor")
def get_cursor_config(request: Request):
    registry = _get_alias_registry()
    aliases = registry.list_aliases()
    model_names = sorted(aliases.keys())

    gateway_url = _get_gateway_base_url(request)

    return {
        "instructions": "Copy this configuration into Cursor Settings > Models > OpenAI API Compatible",
        "config": {
            "apiKey": "databricks-oauth",
            "baseUrl": f"{gateway_url}/api/v1",
            "models": model_names,
        },
        "notes": [
            "Authentication: Visit the gateway URL in your browser first to establish a session, OR use a Databricks OAuth token as the API key",
            "The apiKey value shown is a placeholder — Cursor requires a non-empty value but the gateway authenticates via your browser session or Bearer token",
            "Models listed are auto-discovered aliases that resolve to the latest version of each model family",
        ],
    }


@router.post("/v1/config/cursor/validate")
def validate_cursor_config(request: Request):
    checks = []

    # Check 1: gateway health (inline — same process)
    checks.append({
        "name": "gateway_health",
        "status": "pass",
        "detail": "Gateway is healthy",
    })

    # Check 2: models available
    registry = _get_alias_registry()
    aliases = registry.list_aliases()
    alias_count = len(aliases)
    if alias_count > 0:
        checks.append({
            "name": "models_available",
            "status": "pass",
            "detail": f"Found {alias_count} model aliases",
        })
    else:
        checks.append({
            "name": "models_available",
            "status": "fail",
            "detail": "No model aliases found — registry may not be populated yet",
        })

    # Check 3: auth configured
    from ..auth import extract_request_context
    x_forwarded_access_token = request.headers.get("x-forwarded-access-token")
    x_forwarded_user_id = request.headers.get("x-forwarded-user-id")
    x_forwarded_email = request.headers.get("x-forwarded-email")
    authorization = request.headers.get("authorization")

    try:
        ctx = extract_request_context(
            x_forwarded_access_token=x_forwarded_access_token,
            x_forwarded_user_id=x_forwarded_user_id,
            x_forwarded_email=x_forwarded_email,
            authorization=authorization,
        )
        checks.append({
            "name": "auth_configured",
            "status": "pass",
            "detail": f"Authenticated as {ctx.user_id} via {ctx.auth_mode}",
        })
    except ValueError:
        checks.append({
            "name": "auth_configured",
            "status": "info",
            "detail": "No authentication detected — visit the gateway URL in your browser or provide a Bearer token",
        })

    overall_valid = all(c["status"] in ("pass", "info") for c in checks)

    return {
        "valid": overall_valid,
        "checks": checks,
    }
