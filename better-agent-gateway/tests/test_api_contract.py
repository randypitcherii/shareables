from fastapi.testclient import TestClient

from app import app


client = TestClient(app)


def test_session_creation_requires_forwarded_identity():
    response = client.post("/v1/chat/sessions", json={"model_alias": "claude-sonnet-latest"})
    assert response.status_code == 401
    assert response.json()["detail"] == "Missing forwarded user identity"


def test_create_session_and_resolve_latest_alias():
    response = client.post(
        "/v1/chat/sessions",
        headers={"x-forwarded-user-id": "user-1"},
        json={"model_alias": "claude-sonnet-latest"},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["resolved_model"] == "claude-sonnet-4-5-20260215"
    assert body["session_id"]


def test_cross_user_session_replay_is_denied():
    create = client.post(
        "/v1/chat/sessions",
        headers={"x-forwarded-user-id": "user-1"},
        json={"model_alias": "claude-sonnet-latest"},
    )
    session_id = create.json()["session_id"]
    response = client.post(
        f"/v1/chat/sessions/{session_id}/messages",
        headers={"x-forwarded-user-id": "user-2"},
        json={"content": "hello"},
    )
    assert response.status_code == 403
    assert response.json()["detail"] == "Session is not bound to current user"


def test_deny_by_default_tool_policy_and_audit_lookup():
    denied = client.post(
        "/v1/tools/files/delete/invoke",
        headers={"x-forwarded-user-id": "analyst-1"},
        json={"args": {"path": "/tmp/data.csv"}},
    )
    assert denied.status_code == 403
    denied_request_id = denied.json()["request_id"]

    audit = client.get(f"/v1/audit/requests/{denied_request_id}")
    assert audit.status_code == 200
    record = audit.json()
    assert record["decision"] == "denied"
    assert record["reason"] == "tool_not_allowlisted"
