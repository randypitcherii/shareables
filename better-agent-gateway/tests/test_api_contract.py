"""Smoke tests for basic API contract."""
from fastapi.testclient import TestClient
from app import app

client = TestClient(app)


def test_health_endpoint():
    response = client.get("/api/v1/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_models_endpoint():
    response = client.get("/api/v1/models")
    assert response.status_code == 200
    body = response.json()
    assert "aliases" in body
    assert "endpoints" in body


def test_chat_completions_requires_auth():
    response = client.post("/api/v1/chat/completions", json={
        "model": "claude-sonnet-latest",
        "messages": [{"role": "user", "content": "Hi"}],
    })
    assert response.status_code == 401


def test_proxy_setup_includes_tool_configs():
    response = client.get("/api/v1/proxy-setup")
    assert response.status_code == 200
    body = response.json()
    assert "tool_configs" in body
    configs = body["tool_configs"]
    assert "claude_code" in configs
    assert "codex" in configs
    assert "opencode" in configs
    # Claude Code uses Anthropic API format
    assert configs["claude_code"]["env_vars"]["ANTHROPIC_BASE_URL"] == "http://127.0.0.1:8787"
    # Codex uses OpenAI format
    assert configs["codex"]["env_vars"]["OPENAI_BASE_URL"] == "http://127.0.0.1:8787/v1"
