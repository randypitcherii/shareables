import os
from collections.abc import Mapping

PG_REQUIRED_ENV_VARS: tuple[str, ...] = ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER")


def missing_pg_connection_env(env: Mapping[str, str] | None = None) -> list[str]:
    source = env or os.environ
    return [name for name in PG_REQUIRED_ENV_VARS if not source.get(name)]


def resolve_pg_secret(
    *,
    user_token: str | None = None,
    env: Mapping[str, str] | None = None,
) -> str | None:
    source = env or os.environ
    candidates = (
        user_token,
        source.get("PGPASSWORD"),
        source.get("DATABRICKS_TOKEN"),
    )
    for candidate in candidates:
        if candidate:
            return candidate
    return None
