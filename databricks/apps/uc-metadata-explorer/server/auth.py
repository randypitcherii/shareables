from dataclasses import dataclass

from fastapi import Header

from .config import get_app_access_token


@dataclass
class RequestContext:
    access_token: str
    user_name: str
    auth_mode: str


def _bearer_token(authorization: str | None) -> str:
    if not authorization:
        return ""
    if not authorization.lower().startswith("bearer "):
        return ""
    return authorization[7:]


def get_request_context(
    x_forwarded_access_token: str | None = Header(default=None),
    x_forwarded_preferred_username: str | None = Header(default=None),
    x_forwarded_email: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
) -> RequestContext:
    obo_token = x_forwarded_access_token or _bearer_token(authorization)
    if obo_token:
        user_name = x_forwarded_preferred_username or x_forwarded_email or "current-user"
        return RequestContext(access_token=obo_token, user_name=user_name, auth_mode="on-behalf-of-user")

    app_token = get_app_access_token()
    return RequestContext(
        access_token=app_token,
        user_name="app-service-principal",
        auth_mode="app-fallback",
    )
