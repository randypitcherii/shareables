import os
from dataclasses import dataclass
from urllib.parse import urlparse

from databricks.sdk import WorkspaceClient


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv("DATABRICKS_APP_NAME", "uc-metadata-command-space")
    is_databricks_app: bool = bool(os.getenv("DATABRICKS_APP_NAME"))
    sql_warehouse_id: str = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", os.getenv("WAREHOUSE_ID", ""))
    serving_endpoint: str = os.getenv("DATABRICKS_SERVING_ENDPOINT", "")
    limit_default: int = int(os.getenv("UC_OBJECT_LIMIT", "200"))
    limit_max: int = int(os.getenv("UC_OBJECT_LIMIT_MAX", "1000"))
    metadata_query_timeout_seconds: float = float(os.getenv("UC_METADATA_QUERY_TIMEOUT_SECONDS", "20"))


settings = Settings()


def get_workspace_client() -> WorkspaceClient:
    profile = os.getenv("DATABRICKS_PROFILE")
    if profile and not settings.is_databricks_app:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient()


def get_app_access_token() -> str:
    client = get_workspace_client()
    auth_headers = client.config.authenticate()
    auth_header = auth_headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return ""
    return auth_header.replace("Bearer ", "")


def get_workspace_host() -> str:
    host = os.getenv("DATABRICKS_HOST", "")
    if not host:
        client = get_workspace_client()
        host = client.config.host or ""
    if host and not host.startswith("http"):
        return f"https://{host}"
    return host


def get_workspace_hostname() -> str:
    host = get_workspace_host()
    parsed = urlparse(host)
    if parsed.hostname:
        return parsed.hostname
    return host.replace("https://", "").replace("http://", "")
