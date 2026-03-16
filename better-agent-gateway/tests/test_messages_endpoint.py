from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from fastapi.testclient import TestClient

from app import app

client = TestClient(app)


def test_messages_requires_auth():
    response = client.post(
        "/api/v1/messages",
        json={
            "model": "claude-sonnet-latest",
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": "Hello"}],
        },
    )
    assert response.status_code == 401


def test_messages_rejects_invalid_model_name():
    response = client.post(
        "/api/v1/messages",
        headers={
            "x-forwarded-access-token": "user-token",
            "x-forwarded-user-id": "user@co.com",
        },
        json={
            "model": "../etc/passwd",
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": "Hello"}],
        },
    )
    assert response.status_code == 400
    assert "Invalid model name" in response.json()["detail"]


def test_messages_rejects_missing_max_tokens():
    response = client.post(
        "/api/v1/messages",
        headers={
            "x-forwarded-access-token": "user-token",
            "x-forwarded-user-id": "user@co.com",
        },
        json={
            "model": "claude-sonnet-latest",
            "messages": [{"role": "user", "content": "Hello"}],
        },
    )
    assert response.status_code == 400
    assert "max_tokens" in response.json()["detail"]


def test_messages_rejects_empty_messages():
    response = client.post(
        "/api/v1/messages",
        headers={
            "x-forwarded-access-token": "user-token",
            "x-forwarded-user-id": "user@co.com",
        },
        json={
            "model": "claude-sonnet-latest",
            "max_tokens": 1024,
            "messages": [],
        },
    )
    assert response.status_code == 400
    assert "messages" in response.json()["detail"]


def test_messages_resolves_alias_and_proxies():
    mock_response_body = {
        "id": "msg_123",
        "type": "message",
        "role": "assistant",
        "content": [{"type": "text", "text": "Hi there!"}],
        "model": "claude-sonnet-4-6-20251101",
        "stop_reason": "end_turn",
    }

    mock_http_response = MagicMock(spec=httpx.Response)
    mock_http_response.status_code = 200
    mock_http_response.json.return_value = mock_response_body

    with patch("server.routes.messages._get_client") as mock_get_client, \
         patch("server.routes.messages._get_workspace_host", return_value="https://my-workspace.databricks.com"):
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_http_response)
        mock_get_client.return_value = mock_client

        response = client.post(
            "/api/v1/messages",
            headers={
                "x-forwarded-access-token": "user-token",
                "x-forwarded-user-id": "user@co.com",
            },
            json={
                "model": "claude-sonnet-latest",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": "Hello"}],
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["content"][0]["text"] == "Hi there!"

    # Verify the URL was built correctly
    call_args = mock_client.post.call_args
    assert "anthropic/v1/messages" in call_args.args[0]


def test_messages_passes_through_body():
    mock_response_body = {
        "id": "msg_456",
        "type": "message",
        "role": "assistant",
        "content": [{"type": "text", "text": "Done"}],
        "model": "claude-sonnet-4-6-20251101",
        "stop_reason": "end_turn",
    }

    mock_http_response = MagicMock(spec=httpx.Response)
    mock_http_response.status_code = 200
    mock_http_response.json.return_value = mock_response_body

    with patch("server.routes.messages._get_client") as mock_get_client, \
         patch("server.routes.messages._get_workspace_host", return_value="https://my-workspace.databricks.com"):
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_http_response)
        mock_get_client.return_value = mock_client

        response = client.post(
            "/api/v1/messages",
            headers={
                "x-forwarded-access-token": "user-token",
                "x-forwarded-user-id": "user@co.com",
            },
            json={
                "model": "claude-sonnet-latest",
                "max_tokens": 2048,
                "messages": [{"role": "user", "content": "Hello"}],
                "system": "You are a helpful assistant.",
                "temperature": 0.5,
                "top_p": 0.9,
            },
        )

    assert response.status_code == 200

    # Verify extra Anthropic fields were forwarded in the body
    call_args = mock_client.post.call_args
    forwarded_json = call_args.kwargs.get("json") or call_args.args[1] if len(call_args.args) > 1 else call_args.kwargs["json"]
    assert forwarded_json["system"] == "You are a helpful assistant."
    assert forwarded_json["temperature"] == 0.5
    assert forwarded_json["top_p"] == 0.9
    assert forwarded_json["max_tokens"] == 2048
    # model field must be present in the forwarded body
    assert "model" in forwarded_json
