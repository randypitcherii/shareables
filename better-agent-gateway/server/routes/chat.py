from __future__ import annotations

import re
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ..audit import get_audit_store
from ..auth import RequestContext, get_request_context
from ..proxy import ServingEndpointProxy
from ..request_log import create_log_entry, log_request_async
from ..utils import sanitize_error

_VALID_MODEL_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")

# Module-level proxy singleton (initialized lazily)
_proxy: ServingEndpointProxy | None = None

router = APIRouter()


def _get_proxy() -> ServingEndpointProxy:
    global _proxy
    if _proxy is None:
        from ..config import get_workspace_client
        host = get_workspace_client().config.host or ""
        if not host.startswith("http"):
            host = f"https://{host}"
        _proxy = ServingEndpointProxy(workspace_host=host)
    return _proxy


def _get_alias_registry():
    from ..alias_registry import get_registry
    return get_registry()


class ChatCompletionRequest(BaseModel):
    model: str = Field(description="Model alias or endpoint name")
    messages: list[dict[str, Any]]
    max_tokens: int | None = None
    temperature: float | None = None
    stream: bool = False


@router.post("/v1/chat/completions")
async def chat_completions(
    payload: ChatCompletionRequest,
    context: RequestContext = Depends(get_request_context),
):
    registry = _get_alias_registry()

    if not _VALID_MODEL_RE.match(payload.model):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid model name. Only alphanumeric characters, hyphens, underscores, and dots are allowed.",
        )

    resolved_endpoint = registry.resolve(payload.model)

    get_audit_store().add(
        user_id=context.user_id,
        action="chat_completion",
        decision="allowed",
        reason="allowed",
        metadata={
            "requested_model": payload.model,
            "resolved_endpoint": resolved_endpoint,
            "auth_mode": context.auth_mode,
        },
    )

    if payload.stream:
        return await _handle_streaming(payload, context, resolved_endpoint)
    else:
        return await _handle_non_streaming(payload, context, resolved_endpoint)


async def _handle_non_streaming(
    payload: ChatCompletionRequest,
    context: RequestContext,
    resolved_endpoint: str,
) -> dict[str, Any]:
    start_time = time.monotonic()
    try:
        proxy = _get_proxy()
        result = await proxy.chat_completion(
            endpoint_name=resolved_endpoint,
            access_token=context.access_token,
            messages=payload.messages,
            max_tokens=payload.max_tokens,
            temperature=payload.temperature,
        )
        latency_ms = int((time.monotonic() - start_time) * 1000)

        entry = create_log_entry(
            user_id=context.user_id,
            model_requested=payload.model,
            model_resolved=resolved_endpoint,
            latency_ms=latency_ms,
            status_code=200,
            response_body=result,
        )
        await log_request_async(entry)

        return result
    except HTTPException:
        raise
    except Exception as exc:
        latency_ms = int((time.monotonic() - start_time) * 1000)

        entry = create_log_entry(
            user_id=context.user_id,
            model_requested=payload.model,
            model_resolved=resolved_endpoint,
            latency_ms=latency_ms,
            status_code=502,
        )
        await log_request_async(entry)

        get_audit_store().add(
            user_id=context.user_id,
            action="chat_completion_error",
            decision="error",
            reason=str(exc)[:200],
            metadata={"endpoint": resolved_endpoint},
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Serving endpoint error: {sanitize_error(str(exc))}",
        ) from exc


async def _handle_streaming(
    payload: ChatCompletionRequest,
    context: RequestContext,
    resolved_endpoint: str,
) -> StreamingResponse:
    start_time = time.monotonic()
    try:
        proxy = _get_proxy()
        upstream_resp = await proxy.chat_completion_stream(
            endpoint_name=resolved_endpoint,
            access_token=context.access_token,
            messages=payload.messages,
            max_tokens=payload.max_tokens,
            temperature=payload.temperature,
        )
    except HTTPException:
        raise
    except Exception as exc:
        latency_ms = int((time.monotonic() - start_time) * 1000)

        entry = create_log_entry(
            user_id=context.user_id,
            model_requested=payload.model,
            model_resolved=resolved_endpoint,
            latency_ms=latency_ms,
            status_code=502,
        )
        await log_request_async(entry)

        get_audit_store().add(
            user_id=context.user_id,
            action="chat_completion_error",
            decision="error",
            reason=str(exc)[:200],
            metadata={"endpoint": resolved_endpoint},
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Serving endpoint error: {sanitize_error(str(exc))}",
        ) from exc

    async def event_generator():
        try:
            async for chunk in upstream_resp.aiter_bytes():
                yield chunk
        finally:
            await upstream_resp.aclose()
            latency_ms = int((time.monotonic() - start_time) * 1000)
            entry = create_log_entry(
                user_id=context.user_id,
                model_requested=payload.model,
                model_resolved=resolved_endpoint,
                latency_ms=latency_ms,
                status_code=200,
            )
            await log_request_async(entry)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
