"""Auth context endpoint -- shows forwarded user and OBO token status."""

import os
from typing import Any

from fastapi import APIRouter, Header

router = APIRouter()


@router.get("/v1/auth/context")
def auth_context(
    x_forwarded_user: str | None = Header(default=None),
    x_forwarded_access_token: str | None = Header(default=None),
) -> dict[str, Any]:
    return {
        "forwarded_user": x_forwarded_user,
        "obo_token_present": bool(x_forwarded_access_token),
        "mode": "databricks-app" if os.getenv("DATABRICKS_APP_NAME") else "local",
    }
