"""
Request logging for the Better Agent Gateway.

Stores chat completion request metadata for observability.
Currently uses an in-memory store with a configurable max size.
Designed to be swapped for Lakebase (Databricks Postgres) when deployed.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_MAX_LOG_SIZE = 10_000


@dataclass(frozen=True)
class RequestLogEntry:
    id: str
    timestamp: str
    user_id: str
    model_requested: str
    model_resolved: str
    latency_ms: int
    status_code: int
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class RequestLogStore:
    """In-memory request log store. Thread-safe via asyncio (single-threaded event loop)."""

    def __init__(self, max_size: int = _MAX_LOG_SIZE) -> None:
        self._entries: deque[RequestLogEntry] = deque(maxlen=max_size)

    def add(self, entry: RequestLogEntry) -> None:
        self._entries.appendleft(entry)

    def recent(self, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        entries = list(self._entries)
        return [e.to_dict() for e in entries[offset : offset + limit]]

    def count(self) -> int:
        return len(self._entries)

    def summary_by_model(self) -> list[dict[str, Any]]:
        """Aggregate request counts and avg latency by resolved model."""
        from collections import defaultdict

        buckets: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"model": "", "count": 0, "total_latency_ms": 0, "total_tokens": 0}
        )
        for entry in self._entries:
            b = buckets[entry.model_resolved]
            b["model"] = entry.model_resolved
            b["count"] += 1
            b["total_latency_ms"] += entry.latency_ms
            b["total_tokens"] += entry.total_tokens or 0

        result = []
        for b in buckets.values():
            result.append({
                "model": b["model"],
                "count": b["count"],
                "avg_latency_ms": round(b["total_latency_ms"] / b["count"]) if b["count"] else 0,
                "total_tokens": b["total_tokens"],
            })
        return sorted(result, key=lambda x: x["count"], reverse=True)


# Module-level singleton
_log_store: RequestLogStore | None = None


def get_log_store() -> RequestLogStore:
    global _log_store
    if _log_store is None:
        _log_store = RequestLogStore()
    return _log_store


def create_log_entry(
    *,
    user_id: str,
    model_requested: str,
    model_resolved: str,
    latency_ms: int,
    status_code: int,
    response_body: dict[str, Any] | None = None,
) -> RequestLogEntry:
    """Build a log entry, extracting token counts from the response if available."""
    prompt_tokens = None
    completion_tokens = None
    total_tokens = None

    if response_body and "usage" in response_body:
        usage = response_body["usage"]
        prompt_tokens = usage.get("prompt_tokens")
        completion_tokens = usage.get("completion_tokens")
        total_tokens = usage.get("total_tokens")

    return RequestLogEntry(
        id=str(uuid.uuid4()),
        timestamp=datetime.now(tz=timezone.utc).isoformat(),
        user_id=user_id,
        model_requested=model_requested,
        model_resolved=model_resolved,
        latency_ms=latency_ms,
        status_code=status_code,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=total_tokens,
    )


async def log_request_async(entry: RequestLogEntry) -> None:
    """Non-blocking log write. Failures are logged but never propagated."""
    try:
        get_log_store().add(entry)
    except Exception:
        logger.exception("Failed to write request log entry")
