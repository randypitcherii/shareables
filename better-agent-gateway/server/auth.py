from __future__ import annotations

from dataclasses import dataclass

from fastapi import Header, HTTPException, status


@dataclass(frozen=True)
class RequestContext:
    access_token: str
    user_id: str
    auth_mode: str  # "on-behalf-of-user" | "bearer"


def extract_request_context(
    *,
    x_forwarded_access_token: str | None,
    x_forwarded_user_id: str | None,
    x_forwarded_email: str | None,
    authorization: str | None,
) -> RequestContext:
    """Extract auth context from Databricks App proxy headers."""
    if x_forwarded_access_token:
        user_id = x_forwarded_user_id or x_forwarded_email or "unknown-user"
        return RequestContext(
            access_token=x_forwarded_access_token,
            user_id=user_id,
            auth_mode="on-behalf-of-user",
        )

    if authorization and authorization.lower().startswith("bearer "):
        token = authorization[7:]
        if token:
            return RequestContext(
                access_token=token,
                user_id=x_forwarded_user_id or x_forwarded_email or "bearer-user",
                auth_mode="bearer",
            )

    raise ValueError("No authentication provided")


def get_request_context(
    x_forwarded_access_token: str | None = Header(default=None),
    x_forwarded_user_id: str | None = Header(default=None),
    x_forwarded_email: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
) -> RequestContext:
    """FastAPI dependency that extracts auth context from request headers."""
    try:
        return extract_request_context(
            x_forwarded_access_token=x_forwarded_access_token,
            x_forwarded_user_id=x_forwarded_user_id,
            x_forwarded_email=x_forwarded_email,
            authorization=authorization,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
        ) from exc
