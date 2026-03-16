"""API routes for request logs."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from ..auth import RequestContext, get_request_context
from ..request_log import RequestLogEntry, get_log_store

router = APIRouter()


class LogEntryPayload(BaseModel):
    """Inbound log entry from the agent proxy."""

    timestamp: str | None = None
    user_id: str | None = None
    model_requested: str
    model_resolved: str
    latency_ms: int
    status_code: int
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None


def _payload_to_entry(
    payload: LogEntryPayload, fallback_user_id: str
) -> RequestLogEntry:
    return RequestLogEntry(
        id=str(uuid.uuid4()),
        timestamp=payload.timestamp or datetime.now(tz=timezone.utc).isoformat(),
        user_id=payload.user_id or fallback_user_id,
        model_requested=payload.model_requested,
        model_resolved=payload.model_resolved,
        latency_ms=payload.latency_ms,
        status_code=payload.status_code,
        prompt_tokens=payload.prompt_tokens,
        completion_tokens=payload.completion_tokens,
        total_tokens=payload.total_tokens,
    )


@router.post("/v1/logs", status_code=201)
async def post_request_logs(
    body: LogEntryPayload | list[LogEntryPayload],
    ctx: RequestContext = Depends(get_request_context),
):
    """Accept one or more log entries from the agent proxy."""
    store = get_log_store()
    payloads = body if isinstance(body, list) else [body]
    entries: list[dict[str, Any]] = []
    for payload in payloads:
        entry = _payload_to_entry(payload, fallback_user_id=ctx.user_id)
        store.add(entry)
        entries.append(entry.to_dict())
    return {"entries": entries, "count": len(entries)}


@router.get("/v1/logs")
async def get_request_logs(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
):
    """Return recent request logs with optional pagination."""
    store = get_log_store()
    return {
        "logs": store.recent(limit=limit, offset=offset),
        "total": store.count(),
    }


@router.get("/v1/logs/summary")
async def get_request_log_summary():
    """Return aggregated request stats by model."""
    store = get_log_store()
    return {
        "summary": store.summary_by_model(),
        "total_requests": store.count(),
    }
