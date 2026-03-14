"""FastAPI proxy application that injects Databricks OAuth tokens."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from databricks_agent_proxy.auth import TokenProvider
from databricks_agent_proxy.config import Settings
from databricks_agent_proxy.models import gateway_to_openai
from databricks_agent_proxy.version import version_dict

logger = logging.getLogger(__name__)


def _forward_error(resp: httpx.Response) -> JSONResponse:
    """Forward an upstream error response to the client with the original status code."""
    try:
        body = resp.json()
    except Exception:
        body = {"error": resp.text or f"Gateway returned {resp.status_code}"}
    return JSONResponse(content=body, status_code=resp.status_code)


def create_app(settings: Settings, token_provider: TokenProvider | None = None) -> FastAPI:
    """Create the FastAPI proxy application."""
    if token_provider is None:
        token_provider = TokenProvider(host=settings.databricks_host)

    http_client = httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=10.0))

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
        yield
        await http_client.aclose()

    app = FastAPI(title="databricks-agent-proxy", docs_url=None, redoc_url=None, lifespan=lifespan)

    # Store on app state for access in routes
    app.state.settings = settings
    app.state.token_provider = token_provider
    app.state.http_client = http_client

    @app.get("/health")
    async def health() -> dict[str, Any]:
        tp: TokenProvider = app.state.token_provider
        return {
            "status": "ok",
            "gateway_url": settings.gateway_url,
            "port": settings.port,
            "authenticated": tp.is_authenticated(),
            **version_dict(),
        }

    @app.get("/v1/models")
    async def list_models() -> Response:
        client: httpx.AsyncClient = app.state.http_client
        tp: TokenProvider = app.state.token_provider
        headers = tp.get_auth_headers()

        resp = await client.get(f"{settings.gateway_url}/api/v1/models", headers=headers)
        if not resp.is_success:
            return _forward_error(resp)

        openai_format = gateway_to_openai(resp.json())
        return JSONResponse(content=openai_format)

    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request) -> Response:
        client: httpx.AsyncClient = app.state.http_client
        tp: TokenProvider = app.state.token_provider
        headers = tp.get_auth_headers()
        headers["Content-Type"] = "application/json"

        body = await request.json()
        is_streaming = body.get("stream", False)

        if is_streaming:
            return await _stream_response(client, settings.gateway_url, headers, body)
        else:
            return await _non_stream_response(client, settings.gateway_url, headers, body)

    return app


async def _non_stream_response(
    client: httpx.AsyncClient,
    gateway_url: str,
    headers: dict[str, str],
    body: dict[str, Any],
) -> JSONResponse:
    """Forward a non-streaming request and return the JSON response."""
    resp = await client.post(
        f"{gateway_url}/api/v1/chat/completions",
        headers=headers,
        json=body,
    )
    if not resp.is_success:
        return _forward_error(resp)
    return JSONResponse(content=resp.json())


async def _stream_response(
    client: httpx.AsyncClient,
    gateway_url: str,
    headers: dict[str, str],
    body: dict[str, Any],
) -> Response:
    """Forward a streaming request and relay SSE chunks.

    Opens the upstream connection first, checks the status code, and only
    then returns a StreamingResponse so errors are caught before we commit
    to a 200.
    """
    req = client.build_request(
        "POST",
        f"{gateway_url}/api/v1/chat/completions",
        headers=headers,
        json=body,
    )
    resp = await client.send(req, stream=True)

    if not resp.is_success:
        body_bytes = await resp.aread()
        await resp.aclose()
        try:
            import json

            error_body = json.loads(body_bytes)
        except Exception:
            error_body = {"error": body_bytes.decode(errors="replace")}
        return JSONResponse(content=error_body, status_code=resp.status_code)

    async def event_generator():  # type: ignore[no-untyped-def]
        try:
            async for chunk in resp.aiter_bytes():
                yield chunk
        finally:
            await resp.aclose()

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
