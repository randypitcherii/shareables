"""Tests for the proxy FastAPI application."""

from __future__ import annotations

import json

import httpx
import pytest
from fastapi.testclient import TestClient

from databricks_agent_proxy.proxy_app import create_app


@pytest.fixture
def client(app):
    return TestClient(app)


def test_health_endpoint(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["gateway_url"] == "https://gateway.example.com"
    assert data["authenticated"] is True


def test_models_endpoint(settings, mock_token_provider):
    """Test /v1/models translates gateway format to OpenAI format."""
    gateway_response = {
        "aliases": {"gpt-4": "ep-gpt4"},
        "endpoints": ["ep-gpt4"],
    }

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        assert "Authorization" in request.headers
        return httpx.Response(200, json=gateway_response)

    mock_transport = httpx.MockTransport(mock_handler)
    mock_client = httpx.AsyncClient(transport=mock_transport)

    app = create_app(settings, token_provider=mock_token_provider)
    app.state.http_client = mock_client

    with TestClient(app) as tc:
        resp = tc.get("/v1/models")
        assert resp.status_code == 200
        data = resp.json()
        assert data["object"] == "list"
        assert any(m["id"] == "gpt-4" for m in data["data"])


def test_chat_completions_non_streaming(settings, mock_token_provider):
    """Test non-streaming chat completions passthrough."""
    gateway_resp = {"choices": [{"message": {"role": "assistant", "content": "Hello!"}}]}

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        assert "Authorization" in request.headers
        body = json.loads(request.content)
        assert body["model"] == "gpt-4"
        return httpx.Response(200, json=gateway_resp)

    mock_transport = httpx.MockTransport(mock_handler)
    mock_client = httpx.AsyncClient(transport=mock_transport)

    app = create_app(settings, token_provider=mock_token_provider)
    app.state.http_client = mock_client

    with TestClient(app) as tc:
        resp = tc.post(
            "/v1/chat/completions",
            json={"model": "gpt-4", "messages": [{"role": "user", "content": "Hi"}]},
        )
        assert resp.status_code == 200
        assert resp.json()["choices"][0]["message"]["content"] == "Hello!"


def test_chat_completions_injects_auth_header(settings, mock_token_provider):
    """Verify the proxy injects Databricks OAuth headers, not the client's."""
    captured_headers: dict[str, str] = {}

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        captured_headers.update(dict(request.headers))
        return httpx.Response(200, json={"choices": []})

    mock_transport = httpx.MockTransport(mock_handler)
    mock_client = httpx.AsyncClient(transport=mock_transport)

    app = create_app(settings, token_provider=mock_token_provider)
    app.state.http_client = mock_client

    with TestClient(app) as tc:
        tc.post(
            "/v1/chat/completions",
            json={"model": "test", "messages": []},
            headers={"Authorization": "Bearer cursor-key-should-be-stripped"},
        )

    # The request to the gateway should have the OAuth token, not Cursor's key
    assert captured_headers["authorization"] == "Bearer fake-token"


def test_chat_completions_streaming(settings, mock_token_provider):
    """Test that streaming requests relay SSE data."""
    sse_data = b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n'

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=sse_data,
            headers={"content-type": "text/event-stream"},
        )

    mock_transport = httpx.MockTransport(mock_handler)
    mock_client = httpx.AsyncClient(transport=mock_transport)

    app = create_app(settings, token_provider=mock_token_provider)
    app.state.http_client = mock_client

    with TestClient(app) as tc:
        resp = tc.post(
            "/v1/chat/completions",
            json={
                "model": "gpt-4",
                "messages": [{"role": "user", "content": "Hi"}],
                "stream": True,
            },
        )
        assert resp.status_code == 200
        assert "text/event-stream" in resp.headers.get("content-type", "")
        assert b"Hi" in resp.content
