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
    assert "proxy_base_url" in body
    configs = body["tool_configs"]
    assert "claude_code" in configs
    assert "codex" in configs
    assert "crush" in configs
    # Each tool has settings-file config, not global env vars
    assert "config_file" in configs["claude_code"]
    assert "config_content" in configs["claude_code"]
    assert "config_hint" in configs["claude_code"]
    # Claude Code uses scoped settings.local.json
    assert configs["claude_code"]["config_file"] == ".claude/settings.local.json"
    # Codex uses named provider config
    assert "providers" in configs["codex"]["config_content"]
    # Crush uses project-level config
    assert configs["crush"]["config_file"] == ".crush.json"
