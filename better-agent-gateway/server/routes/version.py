"""Version and proxy setup info endpoints."""

from fastapi import APIRouter, Request

from server.alias_registry import get_registry
from server.version import get_git_info, version_dict, version_string

GITHUB_REPO = "randypitcherii/shareables"
PROXY_SUBDIRECTORY = "databricks-agent-proxy"

router = APIRouter()


@router.get("/v1/version")
def get_version():
    return version_dict()


def _uvx_from_spec(git_hash: str | None) -> str:
    """Build the uvx --from spec, pinned to commit if available."""
    ref = f"@{git_hash}" if git_hash else ""
    return (
        f'"databricks-agent-proxy @ '
        f"git+https://github.com/{GITHUB_REPO}{ref}"
        f'#subdirectory={PROXY_SUBDIRECTORY}"'
    )


@router.get("/v1/proxy-setup")
def get_proxy_setup(request: Request):
    """Return proxy setup instructions with the current gateway URL."""
    # Prefer X-Forwarded headers (set by Databricks Apps proxy), fall back to request
    proto = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("x-forwarded-host") or request.headers.get("host", "")
    gateway_url = f"{proto}://{host}".rstrip("/")

    git_hash, _ = get_git_info()
    from_spec = _uvx_from_spec(git_hash)

    return {
        "gateway_url": gateway_url,
        "uvx_from_spec": from_spec,
        "setup_command": f"uvx --from {from_spec} databricks-agent-proxy setup --gateway-url {gateway_url}",
        "start_command": f"uvx --from {from_spec} databricks-agent-proxy start --gateway-url {gateway_url}",
        "status_command": f"uvx --from {from_spec} databricks-agent-proxy status",
        "health_url": "http://127.0.0.1:8787/health",
        "cursor_base_url": "http://127.0.0.1:8787/v1",
        "version": version_string(),
        "git_hash": git_hash or "main",
        "model_aliases": sorted(get_registry().list_aliases().keys()),
    }
