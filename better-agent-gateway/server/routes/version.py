"""Version and proxy setup info endpoints."""

from fastapi import APIRouter, Request

from server.alias_registry import get_registry
from server.version import get_git_info, version_dict, version_string

GITHUB_REPO = "randypitcherii/shareables"
PROXY_SUBDIRECTORY = "databricks-agent-proxy"
PROXY_BASE_URL = "http://127.0.0.1:8787"

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
        "health_url": f"{PROXY_BASE_URL}/health",
        "cursor_base_url": f"{PROXY_BASE_URL}/v1",
        "version": version_string(),
        "git_hash": git_hash or "main",
        "model_aliases": sorted(get_registry().list_aliases().keys()),
        "proxy_base_url": PROXY_BASE_URL,
        "tool_configs": {
            "claude_code": {
                "name": "Claude Code",
                "description": "Anthropic's AI coding CLI",
                "config_file": ".claude/settings.local.json",
                "config_content": {
                    "env": {
                        "ANTHROPIC_BASE_URL": PROXY_BASE_URL,
                        "ANTHROPIC_API_KEY": "unused",
                    }
                },
                "config_hint": "Scoped to Claude Code only \u2014 does not affect other tools.",
            },
            "codex": {
                "name": "Codex",
                "description": "OpenAI's AI coding CLI",
                "config_file": "~/.codex/config.json",
                "config_content": {
                    "model": "claude-sonnet-latest",
                    "provider": "databricks-proxy",
                    "providers": {
                        "databricks-proxy": {
                            "name": "Databricks Proxy",
                            "baseURL": f"{PROXY_BASE_URL}/v1",
                            "envKey": "CODEX_PROXY_KEY",
                        }
                    },
                },
                "config_hint": "Uses a named provider \u2014 does not override OPENAI_BASE_URL.",
            },
            "crush": {
                "name": "Crush",
                "description": "Charmbracelet's AI coding CLI (formerly OpenCode)",
                "config_file": ".crush.json",
                "config_content": {
                    "providers": {
                        "databricks-proxy": {
                            "type": "openai-compat",
                            "base_url": f"{PROXY_BASE_URL}/v1",
                            "api_key": "unused",
                        }
                    },
                },
                "config_hint": "Project-level config \u2014 does not affect global settings.",
            },
        },
    }
