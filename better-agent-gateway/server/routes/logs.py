"""API routes for request logs."""

from __future__ import annotations

from fastapi import APIRouter, Query

from ..request_log import get_log_store

router = APIRouter()


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
