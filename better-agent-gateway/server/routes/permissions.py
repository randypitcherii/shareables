"""Permission comparison endpoint -- shows OBO user vs app SP access side-by-side."""

import logging
import os
import time
from typing import Any

import httpx
from fastapi import APIRouter, Header

router = APIRouter()
logger = logging.getLogger(__name__)

# Cached SP token
_sp_token: str | None = None
_sp_token_expires_at: float = 0.0

MAX_NAMES = 20


async def _get_sp_token() -> str:
    """Exchange client credentials for an SP OAuth token, cached for 50 min."""
    global _sp_token, _sp_token_expires_at

    if _sp_token and time.time() < _sp_token_expires_at:
        return _sp_token

    host = os.getenv("DATABRICKS_HOST", "")
    client_id = os.getenv("DATABRICKS_CLIENT_ID", "")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET", "")

    if not all([host, client_id, client_secret]):
        raise RuntimeError(
            "Missing DATABRICKS_HOST, DATABRICKS_CLIENT_ID, or DATABRICKS_CLIENT_SECRET"
        )

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            f"{host}/oidc/v1/token",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "all-apis",
            },
        )
        resp.raise_for_status()
        data = resp.json()

    _sp_token = data["access_token"]
    _sp_token_expires_at = time.time() + 50 * 60  # cache for 50 minutes
    return _sp_token


def _truncate_names(names: list[str]) -> list[str]:
    """Return at most MAX_NAMES entries, with a summary suffix if truncated."""
    if len(names) <= MAX_NAMES:
        return names
    remaining = len(names) - MAX_NAMES
    return names[:MAX_NAMES] + [f"...and {remaining} more"]


async def _list_catalogs(token: str, host: str) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{host}/api/2.1/unity-catalog/catalogs",
                headers={"Authorization": f"Bearer {token}"},
            )
            resp.raise_for_status()
            data = resp.json()
        catalogs = data.get("catalogs", [])
        names = [c.get("name", "unknown") for c in catalogs]
        return {"count": len(names), "names": _truncate_names(sorted(names)), "error": None}
    except Exception as exc:
        logger.warning("Failed to list catalogs", exc_info=True)
        return {"count": 0, "names": [], "error": str(exc)}


async def _list_warehouses(token: str, host: str) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{host}/api/2.0/sql/warehouses",
                headers={"Authorization": f"Bearer {token}"},
            )
            resp.raise_for_status()
            data = resp.json()
        warehouses = data.get("warehouses", [])
        names = [w.get("name", "unknown") for w in warehouses]
        return {"count": len(names), "names": _truncate_names(sorted(names)), "error": None}
    except Exception as exc:
        logger.warning("Failed to list warehouses", exc_info=True)
        return {"count": 0, "names": [], "error": str(exc)}


async def _list_serving_endpoints(token: str, host: str) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{host}/api/2.0/serving-endpoints",
                headers={"Authorization": f"Bearer {token}"},
            )
            resp.raise_for_status()
            data = resp.json()
        endpoints = data.get("endpoints", [])
        names = [e.get("name", "unknown") for e in endpoints]
        return {"count": len(names), "names": _truncate_names(sorted(names)), "error": None}
    except Exception as exc:
        logger.warning("Failed to list serving endpoints", exc_info=True)
        return {"count": 0, "names": [], "error": str(exc)}


@router.get("/v1/permissions/comparison")
async def permissions_comparison(
    x_forwarded_access_token: str | None = Header(default=None),
) -> dict[str, Any]:
    host = os.getenv("DATABRICKS_HOST", "")

    # OBO user results
    obo_results: dict[str, Any] = {}
    if x_forwarded_access_token:
        obo_results["catalogs"] = await _list_catalogs(x_forwarded_access_token, host)
        obo_results["warehouses"] = await _list_warehouses(x_forwarded_access_token, host)
        obo_results["serving_endpoints"] = await _list_serving_endpoints(
            x_forwarded_access_token, host
        )
    else:
        no_token = {"count": 0, "names": [], "error": "No OBO token available"}
        obo_results = {
            "catalogs": no_token,
            "warehouses": no_token,
            "serving_endpoints": no_token,
        }

    # App SP results
    sp_results: dict[str, Any] = {}
    try:
        sp_token = await _get_sp_token()
        sp_results["catalogs"] = await _list_catalogs(sp_token, host)
        sp_results["warehouses"] = await _list_warehouses(sp_token, host)
        sp_results["serving_endpoints"] = await _list_serving_endpoints(sp_token, host)
    except Exception as exc:
        sp_error = {"count": 0, "names": [], "error": str(exc)}
        sp_results = {
            "catalogs": sp_error,
            "warehouses": sp_error,
            "serving_endpoints": sp_error,
        }

    return {"obo_user": obo_results, "app_sp": sp_results}
