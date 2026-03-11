"""Permission comparison endpoint -- shows OBO user vs app SP access side-by-side.

Uses Databricks SDK WorkspaceClient instances for both the OBO user and the
app service principal, avoiding raw HTTP and protocol prefix issues.
"""

import logging
import os
import re
from typing import Any

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Header

router = APIRouter()
logger = logging.getLogger(__name__)

MAX_NAMES = 20


def _sanitize_error(msg: str) -> str:
    """Strip SDK config noise and request IDs from error messages.

    The Databricks SDK appends config details (client_id, client_secret,
    auth_type, env vars) and request IDs that should not be shown to users.
    """
    # Remove '. Config: ...' suffix (and everything after it)
    msg = re.sub(r"\. Config:.*", "", msg)
    # Remove '[ReqId: ...]' fragments
    msg = re.sub(r"\s*\[ReqId:\s*[^\]]*\]", "", msg)
    return msg.strip()


def _truncate_names(names: list[str]) -> list[str]:
    """Return at most MAX_NAMES entries, with a summary suffix if truncated."""
    if len(names) <= MAX_NAMES:
        return names
    remaining = len(names) - MAX_NAMES
    return names[:MAX_NAMES] + [f"...and {remaining} more"]


def _get_host() -> str:
    """Get workspace host with https:// prefix."""
    host = os.getenv("DATABRICKS_HOST", "")
    if host and not host.startswith("https://"):
        host = f"https://{host}"
    return host


def _obo_client(obo_token: str) -> WorkspaceClient:
    """Create a WorkspaceClient authenticated as the OBO user.

    Forces auth_type="pat" so the SDK ignores DATABRICKS_CLIENT_ID /
    DATABRICKS_CLIENT_SECRET env vars that the platform injects for the
    app service principal.
    """
    return WorkspaceClient(
        host=_get_host(),
        token=obo_token,
        auth_type="pat",
    )


def _sp_client() -> WorkspaceClient:
    """Create a WorkspaceClient authenticated as the app service principal.

    Uses DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET which are
    auto-injected by the Databricks Apps platform.
    """
    return WorkspaceClient(
        host=_get_host(),
        client_id=os.getenv("DATABRICKS_CLIENT_ID", ""),
        client_secret=os.getenv("DATABRICKS_CLIENT_SECRET", ""),
    )


def _get_current_user(client: WorkspaceClient) -> dict[str, Any]:
    try:
        me = client.current_user.me()
        return {
            "username": me.user_name,
            "display_name": me.display_name,
            "error": None,
        }
    except Exception as exc:
        logger.warning("Failed to get current user", exc_info=True)
        return {"username": None, "display_name": None, "error": _sanitize_error(str(exc))}


def _list_catalogs(client: WorkspaceClient) -> dict[str, Any]:
    try:
        catalogs = list(client.catalogs.list())
        names = sorted([c.name for c in catalogs if c.name])
        return {"count": len(names), "names": _truncate_names(names), "error": None}
    except Exception as exc:
        logger.warning("Failed to list catalogs", exc_info=True)
        return {"count": 0, "names": [], "error": _sanitize_error(str(exc))}


def _list_warehouses(client: WorkspaceClient) -> dict[str, Any]:
    try:
        warehouses = list(client.warehouses.list())
        names = sorted([w.name for w in warehouses if w.name])
        return {"count": len(names), "names": _truncate_names(names), "error": None}
    except Exception as exc:
        logger.warning("Failed to list warehouses", exc_info=True)
        return {"count": 0, "names": [], "error": _sanitize_error(str(exc))}


def _list_serving_endpoints(client: WorkspaceClient) -> dict[str, Any]:
    try:
        endpoints = list(client.serving_endpoints.list())
        names = sorted([e.name for e in endpoints if e.name])
        return {"count": len(names), "names": _truncate_names(names), "error": None}
    except Exception as exc:
        logger.warning("Failed to list serving endpoints", exc_info=True)
        return {"count": 0, "names": [], "error": _sanitize_error(str(exc))}


CHAT_CHECK_MODEL = "databricks-claude-haiku-4-5"


def _chat_completion_check(client: WorkspaceClient) -> dict[str, Any]:
    """Send a minimal chat completion to prove the identity can invoke a model."""
    import httpx

    try:
        host = _get_host()
        headers = client.config.authenticate()

        resp = httpx.post(
            f"{host}/serving-endpoints/{CHAT_CHECK_MODEL}/invocations",
            headers=headers,
            json={
                "messages": [{"role": "user", "content": "Say hello in exactly 3 words."}],
                "max_tokens": 20,
            },
            timeout=15.0,
        )
        if resp.status_code == 200:
            body = resp.json()
            content = body.get("choices", [{}])[0].get("message", {}).get("content", "")
            return {"success": True, "model": CHAT_CHECK_MODEL, "response": content.strip(), "error": None}
        else:
            return {"success": False, "model": CHAT_CHECK_MODEL, "response": None, "error": f"HTTP {resp.status_code}"}
    except Exception as exc:
        logger.warning("Chat completion check failed", exc_info=True)
        return {"success": False, "model": CHAT_CHECK_MODEL, "response": None, "error": _sanitize_error(str(exc))}


_NO_TOKEN_ERROR = "No OBO token available"
_NO_TOKEN_RESOURCE = {"count": 0, "names": [], "error": _NO_TOKEN_ERROR}
_NO_TOKEN_USER = {"username": None, "display_name": None, "error": _NO_TOKEN_ERROR}
_NO_TOKEN_CHAT = {"success": False, "model": CHAT_CHECK_MODEL, "response": None, "error": _NO_TOKEN_ERROR}


@router.get("/v1/permissions/comparison")
async def permissions_comparison(
    x_forwarded_access_token: str | None = Header(default=None),
) -> dict[str, Any]:
    # OBO user results
    obo_results: dict[str, Any] = {}
    if x_forwarded_access_token:
        obo = _obo_client(x_forwarded_access_token)
        obo_results["current_user"] = _get_current_user(obo)
        obo_results["catalogs"] = _list_catalogs(obo)
        obo_results["warehouses"] = _list_warehouses(obo)
        obo_results["serving_endpoints"] = _list_serving_endpoints(obo)
        obo_results["chat_completion"] = _chat_completion_check(obo)
    else:
        obo_results = {
            "current_user": _NO_TOKEN_USER,
            "catalogs": _NO_TOKEN_RESOURCE,
            "warehouses": _NO_TOKEN_RESOURCE,
            "serving_endpoints": _NO_TOKEN_RESOURCE,
            "chat_completion": _NO_TOKEN_CHAT,
        }

    # App SP results
    sp_results: dict[str, Any] = {}
    try:
        sp = _sp_client()
        sp_results["current_user"] = _get_current_user(sp)
        sp_results["catalogs"] = _list_catalogs(sp)
        sp_results["warehouses"] = _list_warehouses(sp)
        sp_results["serving_endpoints"] = _list_serving_endpoints(sp)
        sp_results["chat_completion"] = _chat_completion_check(sp)
    except Exception as exc:
        err = _sanitize_error(str(exc))
        sp_error = {"count": 0, "names": [], "error": err}
        sp_results = {
            "current_user": {"username": None, "display_name": None, "error": err},
            "catalogs": sp_error,
            "warehouses": sp_error,
            "serving_endpoints": sp_error,
            "chat_completion": {"success": False, "model": CHAT_CHECK_MODEL, "response": None, "error": err},
        }

    return {"obo_user": obo_results, "app_sp": sp_results}
