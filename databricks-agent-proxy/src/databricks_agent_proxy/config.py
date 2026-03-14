"""Configuration settings for the proxy."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Settings:
    """Proxy configuration loaded from CLI args or environment."""

    gateway_url: str = ""
    port: int = 8787
    host: str = "127.0.0.1"
    databricks_host: str | None = None
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, **overrides: object) -> Settings:
        """Create settings from environment variables, with CLI overrides taking precedence."""
        settings = cls(
            gateway_url=os.environ.get("GATEWAY_URL", ""),
            port=int(os.environ.get("PORT", "8787")),
            host=os.environ.get("HOST", "127.0.0.1"),
            databricks_host=os.environ.get("DATABRICKS_HOST") or None,
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
        )
        for key, value in overrides.items():
            if value is not None and hasattr(settings, key):
                setattr(settings, key, value)
        return settings

    def validate(self) -> None:
        """Raise ValueError if required settings are missing."""
        if not self.gateway_url:
            raise ValueError("gateway_url is required (set GATEWAY_URL or pass --gateway-url)")
        # Strip trailing slash for consistency
        self.gateway_url = self.gateway_url.rstrip("/")
