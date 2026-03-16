from fastapi.testclient import TestClient

from app import app

client = TestClient(app)


def test_models_list_returns_aliases_and_endpoints():
    response = client.get("/api/v1/models")
    assert response.status_code == 200
    body = response.json()
    assert "aliases" in body
    assert "endpoints" in body


def test_health_check():
    response = client.get("/api/v1/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
