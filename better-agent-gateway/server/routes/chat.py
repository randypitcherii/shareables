from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from ..auth import RequestContext, get_request_context
from ..audit import get_audit_store
from ..proxy import ServingEndpointProxy

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

    try:
        proxy = _get_proxy()
        result = await proxy.chat_completion(
            endpoint_name=resolved_endpoint,
            access_token=context.access_token,
            messages=payload.messages,
            max_tokens=payload.max_tokens,
            temperature=payload.temperature,
            stream=payload.stream,
        )
        return result
    except Exception as exc:
        get_audit_store().add(
            user_id=context.user_id,
            action="chat_completion_error",
            decision="error",
            reason=str(exc)[:200],
            metadata={"endpoint": resolved_endpoint},
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Serving endpoint error: {exc}",
        ) from exc
