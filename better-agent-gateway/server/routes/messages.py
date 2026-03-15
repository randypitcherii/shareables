from __future__ import annotations

import logging
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import StreamingResponse

from ..audit import get_audit_store
from ..auth import RequestContext, get_request_context
from ..utils import sanitize_error

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = httpx.Timeout(300.0, connect=10.0)

# Module-level httpx client singleton (initialized lazily)
_client: httpx.AsyncClient | None = None

router = APIRouter()


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT)
    return _client


def _get_workspace_host() -> str:
    from ..config import get_workspace_client
    host = get_workspace_client().config.host or ""
    if not host.startswith("http"):
        host = f"https://{host}"
    return host.rstrip("/")


def _get_alias_registry():
    from ..alias_registry import get_registry
    return get_registry()


@router.post("/v1/messages")
async def messages(
    request: Request,
    context: RequestContext = Depends(get_request_context),
):
    body: dict[str, Any] = await request.json()

    model = body.get("model", "")
    if not model:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing required field: model",
        )

    registry = _get_alias_registry()
    resolved_model = registry.resolve(model)

    # Replace model with resolved Databricks model name
    forwarded_body = {**body, "model": resolved_model}

    get_audit_store().add(
        user_id=context.user_id,
        action="messages",
        decision="allowed",
        reason="allowed",
        metadata={
            "requested_model": model,
            "resolved_model": resolved_model,
            "auth_mode": context.auth_mode,
        },
    )

    workspace_host = _get_workspace_host()
    url = f"{workspace_host}/serving-endpoints/anthropic/v1/messages"
    headers = {
        "Authorization": f"Bearer {context.access_token}",
        "Content-Type": "application/json",
    }

    stream = body.get("stream", False)

    try:
        client = _get_client()
        if stream:
            return await _handle_streaming(client, url, headers, forwarded_body, context)
        else:
            response = await client.post(url, json=forwarded_body, headers=headers)
            if response.status_code >= 400:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=response.text,
                )
            return response.json()
    except HTTPException:
        raise
    except Exception as exc:
        get_audit_store().add(
            user_id=context.user_id,
            action="messages_error",
            decision="error",
            reason=str(exc)[:200],
            metadata={"resolved_model": resolved_model},
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Anthropic messages endpoint error: {sanitize_error(str(exc))}",
        ) from exc


async def _handle_streaming(
    client: httpx.AsyncClient,
    url: str,
    headers: dict[str, str],
    body: dict[str, Any],
    context: RequestContext,
) -> StreamingResponse:
    async def stream_generator():
        try:
            async with client.stream("POST", url, json=body, headers=headers) as response:
                if response.status_code >= 400:
                    error_body = await response.aread()
                    yield f"data: {error_body.decode()}\n\n"
                    return
                async for chunk in response.aiter_bytes():
                    if chunk:
                        yield chunk
        except Exception as exc:
            logger.error("Streaming error for messages: %s", exc)
            yield f"data: {{\"error\": \"{sanitize_error(str(exc))}\"}}\n\n"

    return StreamingResponse(stream_generator(), media_type="text/event-stream")
