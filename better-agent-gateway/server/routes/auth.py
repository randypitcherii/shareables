"""Auth context endpoint -- shows forwarded user and OBO token status."""

import logging
import os
from typing import Any

import httpx
from fastapi import APIRouter, Header

router = APIRouter()
logger = logging.getLogger(__name__)


async def _resolve_user_identity(
    obo_token: str,
) -> dict[str, str | None]:
    """Use the SCIM /Me endpoint to resolve the current user's identity.

    The OBO token carries the user's identity, so /Me returns info about
    the user who consented -- no admin privileges required.
    """
    databricks_host = os.getenv("DATABRICKS_HOST", "")
    if not databricks_host:
        return {"display_name": None, "email": None, "user_name": None}

    url = f"{databricks_host}/api/2.0/preview/scim/v2/Me"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                url,
                headers={"Authorization": f"Bearer {obo_token}"},
            )
            resp.raise_for_status()
            data = resp.json()
            emails = data.get("emails", [])
            primary_email = next(
                (e["value"] for e in emails if e.get("primary")), None
            )
            return {
                "display_name": data.get("displayName"),
                "email": primary_email or (emails[0]["value"] if emails else None),
                "user_name": data.get("userName"),
            }
    except Exception:
        logger.warning("Failed to resolve user identity via SCIM /Me", exc_info=True)
        return {"display_name": None, "email": None, "user_name": None}


@router.get("/v1/auth/context")
async def auth_context(
    x_forwarded_user: str | None = Header(default=None),
    x_forwarded_access_token: str | None = Header(default=None),
    x_forwarded_email: str | None = Header(default=None),
) -> dict[str, Any]:
    identity = {"display_name": None, "email": None, "user_name": None}

    # Try the X-Forwarded-Email header first (cheapest path)
    if x_forwarded_email:
        identity["email"] = x_forwarded_email

    # If we have an OBO token, resolve full identity via SCIM /Me
    if x_forwarded_access_token:
        resolved = await _resolve_user_identity(x_forwarded_access_token)
        # Merge: SCIM results override, but keep header-sourced values as fallback
        for key in ("display_name", "email", "user_name"):
            if resolved.get(key):
                identity[key] = resolved[key]

    return {
        "forwarded_user": x_forwarded_user,
        "display_name": identity["display_name"],
        "email": identity["email"],
        "user_name": identity["user_name"],
        "obo_token_present": bool(x_forwarded_access_token),
        "mode": "databricks-app" if os.getenv("DATABRICKS_APP_NAME") else "local",
    }
