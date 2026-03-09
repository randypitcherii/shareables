import json
import os
from typing import Any

import asyncpg

from .config import get_app_access_token

_pool: asyncpg.Pool | None = None


def lakebase_configured() -> bool:
    return all(os.getenv(name) for name in ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER"))


async def get_pool(user_token: str | None = None) -> asyncpg.Pool | None:
    global _pool
    if _pool is not None:
        return _pool
    if not lakebase_configured():
        return None

    # OBO first. Fallback to app identity token if user token cannot auth to Lakebase.
    candidate_tokens = [token for token in [user_token, get_app_access_token()] if token]
    for token in candidate_tokens:
        try:
            _pool = await asyncpg.create_pool(
                host=os.environ["PGHOST"],
                port=int(os.environ["PGPORT"]),
                database=os.environ["PGDATABASE"],
                user=os.environ["PGUSER"],
                password=token,
                ssl="require",
                min_size=1,
                max_size=5,
            )
            return _pool
        except Exception:
            _pool = None
    return None


async def ensure_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE SCHEMA IF NOT EXISTS metadata_review;

            CREATE TABLE IF NOT EXISTS metadata_review.proposals (
              id BIGSERIAL PRIMARY KEY,
              catalog_name TEXT NOT NULL,
              schema_name TEXT NOT NULL,
              object_name TEXT NOT NULL,
              object_type TEXT NOT NULL,
              created_by TEXT NOT NULL,
              reviewer_notes TEXT NOT NULL DEFAULT '',
              payload JSONB NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )


async def save_proposal(
    pool: asyncpg.Pool,
    *,
    created_by: str,
    catalog_name: str,
    schema_name: str,
    object_name: str,
    object_type: str,
    reviewer_notes: str,
    changes: list[dict[str, Any]],
) -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO metadata_review.proposals (
              catalog_name,
              schema_name,
              object_name,
              object_type,
              created_by,
              reviewer_notes,
              payload
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
            RETURNING id
            """,
            catalog_name,
            schema_name,
            object_name,
            object_type,
            created_by,
            reviewer_notes,
            json.dumps({"changes": changes}),
        )
    return int(row["id"])
