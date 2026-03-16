"""Lakebase (Postgres) connection helper for Databricks Apps.

Authentication uses the app's service principal M2M OAuth token.
Environment variables are auto-injected by the Databricks Apps runtime
when a Lakebase database resource is attached:

    PGHOST          - Lakebase instance hostname
    PGPORT          - 5432 (default)
    PGDATABASE      - database name (usually "postgres")
    PGSSLMODE       - "require"
    DATABRICKS_HOST - workspace hostname (for OAuth token endpoint)
    DATABRICKS_CLIENT_ID     - service principal client ID
    DATABRICKS_CLIENT_SECRET - service principal client secret
"""

from __future__ import annotations

import base64
import json
import logging
import os
import threading
import time
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

_TOKEN_LOCK = threading.Lock()
_cached_token: str | None = None
_cached_token_expiry: float = 0.0
_TOKEN_REFRESH_MARGIN_SECONDS = 120


def _get_m2m_token() -> str:
    """Fetch an M2M OAuth access token from the workspace OIDC endpoint.

    Tokens are cached in-process and refreshed when within
    ``_TOKEN_REFRESH_MARGIN_SECONDS`` of expiry.
    """
    global _cached_token, _cached_token_expiry

    with _TOKEN_LOCK:
        now = time.time()
        if _cached_token and now < _cached_token_expiry:
            return _cached_token

        host = os.environ["DATABRICKS_HOST"]
        client_id = os.environ["DATABRICKS_CLIENT_ID"]
        client_secret = os.environ["DATABRICKS_CLIENT_SECRET"]

        data = urlencode({
            "grant_type": "client_credentials",
            "scope": "all-apis",
        }).encode()
        req = Request(f"https://{host}/oidc/v1/token", data=data)
        creds = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
        req.add_header("Authorization", f"Basic {creds}")

        with urlopen(req, timeout=10) as response:
            resp = json.loads(response.read())
        access_token: str = resp["access_token"]
        expires_in: int = resp.get("expires_in", 3600)

        _cached_token = access_token
        _cached_token_expiry = now + expires_in - _TOKEN_REFRESH_MARGIN_SECONDS
        logger.info("Refreshed M2M OAuth token (expires_in=%ds)", expires_in)
        return access_token


def get_connection(*, autocommit: bool = True) -> Any:
    """Return a new psycopg2 connection to the Lakebase database.

    Uses the app service principal identity with an M2M OAuth token.
    Caller is responsible for closing the connection.
    """
    import psycopg2

    client_id = os.environ["DATABRICKS_CLIENT_ID"]
    return psycopg2.connect(
        host=os.environ.get("PGHOST", ""),
        port=int(os.environ.get("PGPORT", "5432")),
        dbname=os.environ.get("PGDATABASE", "postgres"),
        sslmode=os.environ.get("PGSSLMODE", "require"),
        user=client_id,
        password=_get_m2m_token(),
        options="-c statement_timeout=30000",
        connect_timeout=10,
    )


def check_connectivity() -> dict[str, Any]:
    """Run a lightweight connectivity check and return a status dict.

    Returns keys: ``ok`` (bool), ``latency_ms`` (int), ``pg_version`` (str),
    ``current_user`` (str), ``database`` (str), and ``error`` (str | None).
    """
    started = time.time()
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT current_user, current_database(), version()")
        row = cur.fetchone()
        conn.close()
        latency_ms = int((time.time() - started) * 1000)
        return {
            "ok": True,
            "latency_ms": latency_ms,
            "current_user": row[0],
            "database": row[1],
            "pg_version": row[2],
            "error": None,
        }
    except Exception as exc:
        latency_ms = int((time.time() - started) * 1000)
        logger.warning("Lakebase connectivity check failed: %s", exc)
        return {
            "ok": False,
            "latency_ms": latency_ms,
            "current_user": None,
            "database": None,
            "pg_version": None,
            "error": str(exc),
        }
