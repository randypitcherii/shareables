"""Tests for request logging feature."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from app import app
from server.request_log import (
    RequestLogEntry,
    RequestLogStore,
    create_log_entry,
    get_log_store,
)

client = TestClient(app)


# --- Unit tests for RequestLogStore ---


def test_log_store_add_and_recent():
    store = RequestLogStore()
    entry = create_log_entry(
        user_id="u1",
        model_requested="claude-sonnet-latest",
        model_resolved="databricks-claude-sonnet-4-6",
        latency_ms=150,
        status_code=200,
    )
    store.add(entry)
    logs = store.recent()
    assert len(logs) == 1
    assert logs[0]["user_id"] == "u1"
    assert logs[0]["status_code"] == 200


def test_log_store_recent_ordering():
    """Most recent entries come first."""
    store = RequestLogStore()
    for i in range(5):
        entry = create_log_entry(
            user_id=f"u{i}",
            model_requested="m",
            model_resolved="m",
            latency_ms=i * 10,
            status_code=200,
        )
        store.add(entry)
    logs = store.recent(limit=3)
    assert len(logs) == 3
    # Last added should be first
    assert logs[0]["user_id"] == "u4"


def test_log_store_pagination():
    store = RequestLogStore()
    for i in range(10):
        entry = create_log_entry(
            user_id=f"u{i}",
            model_requested="m",
            model_resolved="m",
            latency_ms=100,
            status_code=200,
        )
        store.add(entry)
    page = store.recent(limit=3, offset=2)
    assert len(page) == 3
    assert page[0]["user_id"] == "u7"


def test_log_store_max_size():
    store = RequestLogStore(max_size=5)
    for i in range(10):
        entry = create_log_entry(
            user_id=f"u{i}",
            model_requested="m",
            model_resolved="m",
            latency_ms=100,
            status_code=200,
        )
        store.add(entry)
    assert store.count() == 5


def test_create_log_entry_extracts_tokens():
    response = {
        "id": "chatcmpl-123",
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 20,
            "total_tokens": 30,
        },
    }
    entry = create_log_entry(
        user_id="u1",
        model_requested="m",
        model_resolved="m",
        latency_ms=100,
        status_code=200,
        response_body=response,
    )
    assert entry.prompt_tokens == 10
    assert entry.completion_tokens == 20
    assert entry.total_tokens == 30


def test_create_log_entry_no_usage():
    entry = create_log_entry(
        user_id="u1",
        model_requested="m",
        model_resolved="m",
        latency_ms=100,
        status_code=502,
    )
    assert entry.prompt_tokens is None
    assert entry.total_tokens is None


def test_summary_by_model():
    store = RequestLogStore()
    for i in range(3):
        store.add(create_log_entry(
            user_id="u1",
            model_requested="a",
            model_resolved="model-a",
            latency_ms=100,
            status_code=200,
        ))
    store.add(create_log_entry(
        user_id="u1",
        model_requested="b",
        model_resolved="model-b",
        latency_ms=200,
        status_code=200,
    ))
    summary = store.summary_by_model()
    assert len(summary) == 2
    assert summary[0]["model"] == "model-a"
    assert summary[0]["count"] == 3


# --- API endpoint tests ---


def test_logs_endpoint_empty():
    response = client.get("/api/v1/logs")
    assert response.status_code == 200
    body = response.json()
    assert body["logs"] == []
    assert body["total"] == 0


def test_logs_summary_endpoint_empty():
    response = client.get("/api/v1/logs/summary")
    assert response.status_code == 200
    body = response.json()
    assert body["summary"] == []
    assert body["total_requests"] == 0


def test_post_single_log_entry():
    """POST a single log entry to /api/v1/logs."""
    response = client.post(
        "/api/v1/logs",
        headers={"authorization": "Bearer test-token"},
        json={
            "model_requested": "claude-sonnet-latest",
            "model_resolved": "databricks-claude-sonnet-4-6",
            "latency_ms": 120,
            "status_code": 200,
            "prompt_tokens": 10,
            "completion_tokens": 20,
            "total_tokens": 30,
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["count"] == 1
    entry = body["entries"][0]
    assert entry["model_requested"] == "claude-sonnet-latest"
    assert entry["total_tokens"] == 30
    # user_id falls back to auth context
    assert entry["user_id"] == "bearer-user"


def test_post_log_entry_with_explicit_user_and_timestamp():
    """POST with explicit user_id and timestamp uses provided values."""
    response = client.post(
        "/api/v1/logs",
        headers={"authorization": "Bearer test-token"},
        json={
            "timestamp": "2026-01-01T00:00:00+00:00",
            "user_id": "proxy-user@co.com",
            "model_requested": "gpt-4o",
            "model_resolved": "databricks-gpt-4o",
            "latency_ms": 200,
            "status_code": 200,
        },
    )
    assert response.status_code == 201
    entry = response.json()["entries"][0]
    assert entry["user_id"] == "proxy-user@co.com"
    assert entry["timestamp"] == "2026-01-01T00:00:00+00:00"


def test_post_batch_log_entries():
    """POST an array of log entries."""
    entries = [
        {
            "model_requested": f"model-{i}",
            "model_resolved": f"resolved-{i}",
            "latency_ms": 100 + i,
            "status_code": 200,
        }
        for i in range(3)
    ]
    response = client.post(
        "/api/v1/logs",
        headers={"authorization": "Bearer test-token"},
        json=entries,
    )
    assert response.status_code == 201
    body = response.json()
    assert body["count"] == 3


def test_post_log_entry_appears_in_get():
    """Posted entries appear in GET /api/v1/logs."""
    client.post(
        "/api/v1/logs",
        headers={"authorization": "Bearer test-token"},
        json={
            "model_requested": "test-model",
            "model_resolved": "test-resolved",
            "latency_ms": 50,
            "status_code": 200,
        },
    )
    logs_response = client.get("/api/v1/logs")
    body = logs_response.json()
    assert body["total"] >= 1
    assert any(l["model_requested"] == "test-model" for l in body["logs"])


def test_post_log_entry_requires_auth():
    """POST without auth returns 401."""
    response = client.post(
        "/api/v1/logs",
        json={
            "model_requested": "m",
            "model_resolved": "m",
            "latency_ms": 100,
            "status_code": 200,
        },
    )
    assert response.status_code == 401


def test_chat_completion_creates_log_entry():
    """Verify that a successful chat completion creates a request log."""
    mock_result = {
        "id": "chatcmpl-123",
        "choices": [{"message": {"role": "assistant", "content": "Hi!"}}],
        "model": "databricks-claude-sonnet-4-6",
        "usage": {"prompt_tokens": 5, "completion_tokens": 10, "total_tokens": 15},
    }
    with patch("server.routes.chat._get_proxy") as mock_proxy_fn:
        mock_proxy = AsyncMock()
        mock_proxy.chat_completion = AsyncMock(return_value=mock_result)
        mock_proxy_fn.return_value = mock_proxy

        response = client.post(
            "/api/v1/chat/completions",
            headers={
                "x-forwarded-access-token": "user-token",
                "x-forwarded-user-id": "testuser@co.com",
            },
            json={
                "model": "claude-sonnet-latest",
                "messages": [{"role": "user", "content": "Hello"}],
            },
        )

    assert response.status_code == 200

    logs_response = client.get("/api/v1/logs")
    body = logs_response.json()
    assert body["total"] >= 1
    log = body["logs"][0]
    assert log["user_id"] == "testuser@co.com"
    assert log["model_requested"] == "claude-sonnet-latest"
    assert log["status_code"] == 200
    assert log["total_tokens"] == 15


def test_chat_completion_error_creates_log_entry():
    """Verify that a failed chat completion also creates a request log."""
    with patch("server.routes.chat._get_proxy") as mock_proxy_fn:
        mock_proxy = AsyncMock()
        mock_proxy.chat_completion = AsyncMock(side_effect=RuntimeError("boom"))
        mock_proxy_fn.return_value = mock_proxy

        response = client.post(
            "/api/v1/chat/completions",
            headers={
                "x-forwarded-access-token": "user-token",
                "x-forwarded-user-id": "testuser@co.com",
            },
            json={
                "model": "claude-sonnet-latest",
                "messages": [{"role": "user", "content": "Hello"}],
            },
        )

    assert response.status_code == 502

    logs_response = client.get("/api/v1/logs")
    body = logs_response.json()
    assert body["total"] >= 1
    log = body["logs"][0]
    assert log["status_code"] == 502
