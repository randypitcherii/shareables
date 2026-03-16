from __future__ import annotations

import re
import time
import logging
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse, StreamingResponse

from ..audit import get_audit_store
from ..auth import RequestContext, get_request_context
from ..request_log import create_log_entry, log_request_async
from ..utils import sanitize_error

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = httpx.Timeout(300.0, connect=10.0)
_VALID_MODEL_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")

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

    if not _VALID_MODEL_RE.match(model):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid model name. Only alphanumeric characters, hyphens, underscores, and dots are allowed.",
        )

    max_tokens = body.get("max_tokens")
    if max_tokens is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing required field: max_tokens",
        )

    if not isinstance(body.get("messages"), list) or len(body["messages"]) == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing or empty required field: messages",
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

    is_streaming = body.get("stream", False)

    if is_streaming:
        return await _handle_streaming(
            url, headers, forwarded_body, context, model, resolved_model,
        )
    else:
        return await _handle_non_streaming(
            url, headers, forwarded_body, context, model, resolved_model,
        )


async def _handle_non_streaming(
    url: str,
    headers: dict[str, str],
    body: dict[str, Any],
    context: RequestContext,
    model_requested: str,
    model_resolved: str,
) -> dict[str, Any]:
    start_time = time.monotonic()
    try:
        client = _get_client()
        response = await client.post(url, json=body, headers=headers)
        latency_ms = int((time.monotonic() - start_time) * 1000)

        if response.status_code >= 400:
            entry = create_log_entry(
                user_id=context.user_id,
                model_requested=model_requested,
                model_resolved=model_resolved,
                latency_ms=latency_ms,
                status_code=response.status_code,
            )
            await log_request_async(entry)
            raise HTTPException(
                status_code=response.status_code,
                detail=sanitize_error(response.text),
            )

        resp_json = response.json()
        usage = resp_json.get("usage", {})
        entry = create_log_entry(
            user_id=context.user_id,
            model_requested=model_requested,
            model_resolved=model_resolved,
            latency_ms=latency_ms,
            status_code=200,
            response_body=resp_json,
        )
        await log_request_async(entry)

        return resp_json
    except HTTPException:
        raise
    except Exception as exc:
        latency_ms = int((time.monotonic() - start_time) * 1000)
        entry = create_log_entry(
            user_id=context.user_id,
            model_requested=model_requested,
            model_resolved=model_resolved,
            latency_ms=latency_ms,
            status_code=502,
        )
        await log_request_async(entry)

        get_audit_store().add(
            user_id=context.user_id,
            action="messages_error",
            decision="error",
            reason=str(exc)[:200],
            metadata={"resolved_model": model_resolved},
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Anthropic messages endpoint error: {sanitize_error(str(exc))}",
        ) from exc


async def _handle_streaming(
    url: str,
    headers: dict[str, str],
    body: dict[str, Any],
    context: RequestContext,
    model_requested: str,
    model_resolved: str,
) -> StreamingResponse:
    """Forward a streaming messages request.

    Opens the upstream connection first, checks the status code, and only
    then returns a StreamingResponse so errors are caught before committing
    to a 200.
    """
    start_time = time.monotonic()
    client = _get_client()

    try:
        req = client.build_request("POST", url, json=body, headers=headers)
        resp = await client.send(req, stream=True)
    except Exception as exc:
        latency_ms = int((time.monotonic() - start_time) * 1000)
        entry = create_log_entry(
            user_id=context.user_id,
            model_requested=model_requested,
            model_resolved=model_resolved,
            latency_ms=latency_ms,
            status_code=502,
        )
        await log_request_async(entry)

        get_audit_store().add(
            user_id=context.user_id,
            action="messages_error",
            decision="error",
            reason=str(exc)[:200],
            metadata={"resolved_model": model_resolved},
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Anthropic messages endpoint error: {sanitize_error(str(exc))}",
        ) from exc

    if not resp.is_success:
        latency_ms = int((time.monotonic() - start_time) * 1000)
        body_bytes = await resp.aread()
        await resp.aclose()

        entry = create_log_entry(
            user_id=context.user_id,
            model_requested=model_requested,
            model_resolved=model_resolved,
            latency_ms=latency_ms,
            status_code=resp.status_code,
        )
        await log_request_async(entry)

        try:
            import json
            error_body = json.loads(body_bytes)
        except Exception:
            error_body = {"error": body_bytes.decode(errors="replace")}
        return JSONResponse(content=error_body, status_code=resp.status_code)

    async def event_generator():
        try:
            async for chunk in resp.aiter_bytes():
                yield chunk
        finally:
            await resp.aclose()
            latency_ms = int((time.monotonic() - start_time) * 1000)
            entry = create_log_entry(
                user_id=context.user_id,
                model_requested=model_requested,
                model_resolved=model_resolved,
                latency_ms=latency_ms,
                status_code=200,
            )
            await log_request_async(entry)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
