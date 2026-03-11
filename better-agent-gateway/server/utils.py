"""Shared utilities for the server package."""

import re


def sanitize_error(msg: str) -> str:
    """Strip SDK config noise and request IDs from error messages.

    The Databricks SDK appends config details (client_id, client_secret,
    auth_type, env vars) and request IDs that should not be shown to users.
    """
    # Remove '. Config: ...' suffix (and everything after it)
    msg = re.sub(r"\. Config:.*", "", msg)
    # Remove '[ReqId: ...]' fragments
    msg = re.sub(r"\s*\[ReqId:\s*[^\]]*\]", "", msg)
    return msg.strip()
