from __future__ import annotations

import os
from dataclasses import dataclass, field

from databricks.sdk import WorkspaceClient


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _list_env(name: str, default: list[str] | None = None) -> list[str]:
    value = os.getenv(name)
    if not value:
        return default or []
    return [v.strip() for v in value.split(",") if v.strip()]


@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv("DATABRICKS_APP_NAME", "better-agent-gateway")
    is_databricks_app: bool = bool(os.getenv("DATABRICKS_APP_NAME"))
    default_model_alias: str = os.getenv("DEFAULT_MODEL_ALIAS", "claude-sonnet-latest")
    alias_refresh_interval_seconds: int = int(
        os.getenv("ALIAS_REFRESH_INTERVAL_SECONDS", "300")
    )
    model_family_prefixes: list[str] = field(
        default_factory=lambda: _list_env(
            "MODEL_FAMILY_PREFIXES",
            ["databricks-claude", "databricks-gpt", "databricks-gemini", "databricks-codex"],
        )
    )


settings = Settings()


def get_workspace_client() -> WorkspaceClient:
    profile = os.getenv("DATABRICKS_PROFILE")
    if profile and not settings.is_databricks_app:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient()
