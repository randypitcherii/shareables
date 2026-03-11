from __future__ import annotations

from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from app import app

client = TestClient(app)


def test_chat_completions_requires_auth():
    response = client.post("/api/v1/chat/completions", json={
        "model": "claude-sonnet-latest",
        "messages": [{"role": "user", "content": "Hello"}],
    })
    assert response.status_code == 401


def test_chat_completions_resolves_alias_and_proxies():
    mock_result = {
        "id": "chatcmpl-123",
        "choices": [{"message": {"role": "assistant", "content": "Hi!"}}],
        "model": "databricks-claude-sonnet-4-6",
    }
    with patch("server.routes.chat._get_proxy") as mock_proxy_fn:
        mock_proxy = AsyncMock()
        mock_proxy.chat_completion = AsyncMock(return_value=mock_result)
        mock_proxy_fn.return_value = mock_proxy

        response = client.post(
            "/api/v1/chat/completions",
            headers={
                "x-forwarded-access-token": "user-token",
                "x-forwarded-user-id": "user@co.com",
            },
            json={
                "model": "claude-sonnet-latest",
                "messages": [{"role": "user", "content": "Hello"}],
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["choices"][0]["message"]["content"] == "Hi!"


def test_chat_completions_passes_optional_params():
    mock_result = {"id": "chatcmpl-123", "choices": []}
    with patch("server.routes.chat._get_proxy") as mock_proxy_fn:
        mock_proxy = AsyncMock()
        mock_proxy.chat_completion = AsyncMock(return_value=mock_result)
        mock_proxy_fn.return_value = mock_proxy

        response = client.post(
            "/api/v1/chat/completions",
            headers={
                "x-forwarded-access-token": "user-token",
                "x-forwarded-user-id": "user@co.com",
            },
            json={
                "model": "claude-sonnet-latest",
                "messages": [{"role": "user", "content": "Hello"}],
                "max_tokens": 500,
                "temperature": 0.7,
            },
        )

    assert response.status_code == 200
    call_kwargs = mock_proxy.chat_completion.call_args.kwargs
    assert call_kwargs["max_tokens"] == 500
    assert call_kwargs["temperature"] == 0.7
