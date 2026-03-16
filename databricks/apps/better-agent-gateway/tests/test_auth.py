import pytest
from server.auth import extract_request_context, RequestContext


def test_obo_token_from_forwarded_header():
    ctx = extract_request_context(
        x_forwarded_access_token="obo-token-123",
        x_forwarded_user_id="user@company.com",
        x_forwarded_email="user@company.com",
        authorization=None,
    )
    assert ctx.access_token == "obo-token-123"
    assert ctx.user_id == "user@company.com"
    assert ctx.auth_mode == "on-behalf-of-user"


def test_bearer_token_fallback():
    ctx = extract_request_context(
        x_forwarded_access_token=None,
        x_forwarded_user_id=None,
        x_forwarded_email=None,
        authorization="Bearer my-bearer-token",
    )
    assert ctx.access_token == "my-bearer-token"
    assert ctx.auth_mode == "bearer"


def test_missing_auth_raises():
    with pytest.raises(ValueError, match="No authentication"):
        extract_request_context(
            x_forwarded_access_token=None,
            x_forwarded_user_id=None,
            x_forwarded_email=None,
            authorization=None,
        )


def test_user_id_from_email_fallback():
    ctx = extract_request_context(
        x_forwarded_access_token="token",
        x_forwarded_user_id=None,
        x_forwarded_email="fallback@company.com",
        authorization=None,
    )
    assert ctx.user_id == "fallback@company.com"
