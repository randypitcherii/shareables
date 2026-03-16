import asyncio
import os

import asyncpg
import pytest

from server.postgres_env import missing_pg_connection_env, resolve_pg_secret


def test_postgres_connectivity_from_env() -> None:
    missing = missing_pg_connection_env()
    if missing:
        pytest.skip(f"Skipping Postgres connectivity test; missing env vars: {', '.join(missing)}")

    secret = resolve_pg_secret()
    if not secret:
        pytest.skip(
            "Skipping Postgres connectivity test; set PGPASSWORD or DATABRICKS_TOKEN to authenticate."
        )

    async def _check_connection() -> None:
        conn = await asyncpg.connect(
            host=os.environ["PGHOST"],
            port=int(os.environ["PGPORT"]),
            database=os.environ["PGDATABASE"],
            user=os.environ["PGUSER"],
            password=secret,
            ssl="require",
            timeout=10,
        )
        try:
            result = await conn.fetchval("SELECT 1")
            assert result == 1
        finally:
            await conn.close()

    asyncio.run(_check_connection())
