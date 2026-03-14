"""Databricks OAuth token provider using the databricks-sdk U2M flow."""

from __future__ import annotations

import logging

from typing import Callable

from databricks.sdk.core import Config

logger = logging.getLogger(__name__)


class TokenProvider:
    """Wraps databricks-sdk Config to provide fresh OAuth tokens."""

    def __init__(self, host: str | None = None) -> None:
        kwargs: dict[str, str] = {}
        if host:
            kwargs["host"] = host
        self._config = Config(**kwargs)
        self._header_factory: Callable[[], dict[str, str]] | None = None

    def login(self) -> None:
        """Trigger the U2M browser-based OAuth flow and cache credentials."""
        self._header_factory = self._config.authenticate
        # Force an initial token fetch to trigger the browser flow
        headers = self._header_factory()
        if not headers:
            raise RuntimeError("OAuth login failed — no credentials returned")
        logger.info("Successfully authenticated with Databricks")

    def get_auth_headers(self) -> dict[str, str]:
        """Return fresh Authorization headers. Tokens are auto-refreshed by the SDK."""
        if self._header_factory is None:
            self._header_factory = self._config.authenticate
        headers = self._header_factory()
        if not headers:
            raise RuntimeError("No valid credentials — run `databricks-agent-proxy setup` first")
        return headers

    def is_authenticated(self) -> bool:
        """Check whether valid credentials are available."""
        try:
            headers = self.get_auth_headers()
            return bool(headers.get("Authorization"))
        except Exception:
            return False
