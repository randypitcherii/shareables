import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi.testclient import TestClient

from app import app

client = TestClient(app)


def test_get_cursor_config_returns_expected_structure():
    response = client.get("/api/v1/config/cursor")
    assert response.status_code == 200
    data = response.json()
    assert "instructions" in data
    assert "config" in data
    assert "notes" in data

    config = data["config"]
    assert "apiKey" in config
    assert "baseUrl" in config
    assert "models" in config
    assert isinstance(config["models"], list)


def test_get_cursor_config_base_url_uses_request_host():
    response = client.get("/api/v1/config/cursor")
    assert response.status_code == 200
    base_url = response.json()["config"]["baseUrl"]
    assert base_url.endswith("/api/v1")
    assert base_url.startswith("http")


def test_get_cursor_config_base_url_respects_forwarded_headers():
    response = client.get(
        "/api/v1/config/cursor",
        headers={
            "x-forwarded-host": "my-gateway.aws.databricksapps.com",
            "x-forwarded-proto": "https",
        },
    )
    assert response.status_code == 200
    base_url = response.json()["config"]["baseUrl"]
    assert base_url == "https://my-gateway.aws.databricksapps.com/api/v1"


def test_get_cursor_config_notes_is_list():
    response = client.get("/api/v1/config/cursor")
    assert response.status_code == 200
    notes = response.json()["notes"]
    assert isinstance(notes, list)
    assert len(notes) > 0


def test_validate_cursor_config_returns_valid_structure():
    response = client.post("/api/v1/config/cursor/validate")
    assert response.status_code == 200
    data = response.json()
    assert "valid" in data
    assert "checks" in data
    assert isinstance(data["valid"], bool)
    assert isinstance(data["checks"], list)


def test_validate_cursor_config_has_required_checks():
    response = client.post("/api/v1/config/cursor/validate")
    assert response.status_code == 200
    checks = response.json()["checks"]
    check_names = {c["name"] for c in checks}
    assert "gateway_health" in check_names
    assert "models_available" in check_names
    assert "auth_configured" in check_names


def test_validate_cursor_config_check_structure():
    response = client.post("/api/v1/config/cursor/validate")
    assert response.status_code == 200
    for check in response.json()["checks"]:
        assert "name" in check
        assert "status" in check
        assert "detail" in check
        assert check["status"] in ("pass", "fail", "info")


def test_validate_cursor_config_gateway_health_passes():
    response = client.post("/api/v1/config/cursor/validate")
    assert response.status_code == 200
    checks = {c["name"]: c for c in response.json()["checks"]}
    assert checks["gateway_health"]["status"] == "pass"


def test_validate_cursor_config_auth_info_without_token():
    response = client.post("/api/v1/config/cursor/validate")
    assert response.status_code == 200
    checks = {c["name"]: c for c in response.json()["checks"]}
    # Without auth headers, auth_configured should be "info" (not a hard failure)
    assert checks["auth_configured"]["status"] in ("pass", "info")


def test_validate_cursor_config_auth_pass_with_bearer_token():
    response = client.post(
        "/api/v1/config/cursor/validate",
        headers={"Authorization": "Bearer fake-test-token"},
    )
    assert response.status_code == 200
    checks = {c["name"]: c for c in response.json()["checks"]}
    assert checks["auth_configured"]["status"] == "pass"
