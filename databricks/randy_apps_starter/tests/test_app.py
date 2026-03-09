from pathlib import Path
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import SESSION_CWD_STORE, SESSION_STATE_DB_PATH, SessionCwdStore, app


client = TestClient(app)


@pytest.fixture(autouse=True)
def reset_session_state_store() -> None:
    SESSION_CWD_STORE.clear()


def test_healthcheck() -> None:
    response = client.get("/api/v1/healthcheck")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["app"] == "randy-apps-starter"
    assert isinstance(payload["runtime_marker"], str)
    assert payload["runtime_marker"]


def test_shell_run() -> None:
    response = client.post(
        "/api/v1/shell/run",
        json={"argv": ["bash", "-lc", "echo hello-starter"]},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["exit_code"] == 0
    assert "hello-starter" in payload["stdout"]


def test_manual_terminal_ui_page() -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert "Randy Apps Starter Sandbox" in response.text
    assert "Runtime marker:" in response.text
    assert "/api/v1/shell/stream" in response.text
    assert "/api/v1/shell/complete" in response.text
    assert "handleTabCompletion" in response.text
    assert "xterm.min.js" not in response.text
    assert 'id="command"' not in response.text
    assert 'id="runBtn"' not in response.text
    assert 'id="cwd"' not in response.text
    assert 'id="timeout"' not in response.text
    assert 'role="textbox"' in response.text
    assert "timeout_seconds: 20" in response.text
    assert "payload.cwd" not in response.text
    assert "addEventListener(\"paste\"" in response.text
    assert "response body is unavailable for streaming" in response.text
    assert "#terminal:focus .caret" in response.text
    assert "@keyframes caret-blink" in response.text


def test_auth_context_reads_forwarded_headers() -> None:
    response = client.get(
        "/api/v1/auth/context",
        headers={
            "x-forwarded-user": "randy.pitcher@databricks.com",
            "x-forwarded-access-token": "token-value",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["forwarded_user"] == "randy.pitcher@databricks.com"
    assert payload["obo_token_present"] is True


def test_shell_stream() -> None:
    with client.stream(
        "POST",
        "/api/v1/shell/stream",
        json={"argv": ["bash", "-lc", "echo stream-ok"]},
    ) as response:
        assert response.status_code == 200
        body = "".join(response.iter_text())
    assert "$ bash -lc" in body
    assert "stream-ok" in body
    assert "[exit=0" in body


def test_shell_run_persists_cwd_per_session(tmp_path: Path) -> None:
    session_id = "test-cwd-session"
    target = tmp_path / "child"
    target.mkdir()
    parent = target.parent

    first = client.post(
        "/api/v1/shell/run",
        json={"argv": ["bash", "-lc", f"cd {target}"], "session_id": session_id},
    )
    assert first.status_code == 200
    assert first.json()["exit_code"] == 0

    second = client.post(
        "/api/v1/shell/run",
        json={"argv": ["bash", "-lc", "pwd"], "session_id": session_id},
    )
    assert second.status_code == 200
    assert second.json()["stdout"].strip() == str(target)

    third = client.post(
        "/api/v1/shell/run",
        json={"argv": ["bash", "-lc", "cd .. && pwd"], "session_id": session_id},
    )
    assert third.status_code == 200
    assert third.json()["stdout"].strip() == str(parent)

    fourth = client.post(
        "/api/v1/shell/run",
        json={"argv": ["bash", "-lc", "pwd"], "session_id": session_id},
    )
    assert fourth.status_code == 200
    assert fourth.json()["stdout"].strip() == str(parent)


def test_shell_stream_persists_cwd_per_session(tmp_path: Path) -> None:
    session_id = "test-stream-cwd-session"
    target = tmp_path / "stream-child"
    target.mkdir()

    with client.stream(
        "POST",
        "/api/v1/shell/stream",
        json={"argv": ["bash", "-lc", f"cd {target}"], "session_id": session_id},
    ) as response:
        assert response.status_code == 200
        init_body = "".join(response.iter_text())
    assert "[exit=0" in init_body

    with client.stream(
        "POST",
        "/api/v1/shell/stream",
        json={"argv": ["bash", "-lc", "pwd"], "session_id": session_id},
    ) as response:
        assert response.status_code == 200
        body = "".join(response.iter_text())
    assert str(target) in body


def test_shell_run_persists_cwd_across_worker_memory_reset(tmp_path: Path) -> None:
    session_id = "test-cross-worker-cwd-session"
    target = tmp_path / "cross-worker-child"
    target.mkdir()

    first = client.post(
        "/api/v1/shell/run",
        json={"argv": ["bash", "-lc", f"cd {target}"], "session_id": session_id},
    )
    assert first.status_code == 200
    assert first.json()["exit_code"] == 0

    # Simulate a subsequent request being served by a different worker process.
    separate_worker_store = SessionCwdStore(SESSION_STATE_DB_PATH)
    assert separate_worker_store.get_cwd(session_id) == str(target)

    second = client.post(
        "/api/v1/shell/run",
        json={"argv": ["bash", "-lc", "pwd"], "session_id": session_id},
    )
    assert second.status_code == 200
    assert second.json()["stdout"].strip() == str(target)


def test_shell_complete_applies_common_prefix(tmp_path: Path) -> None:
    cwd = tmp_path / "complete-prefix"
    cwd.mkdir()
    (cwd / "alpha-one.txt").write_text("a")
    (cwd / "alpha-two.txt").write_text("b")

    response = client.post(
        "/api/v1/shell/complete",
        json={"line": "alp", "cwd": str(cwd)},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["input"] == "alp"
    assert payload["fragment"] == "alp"
    assert payload["common_prefix"] == "alpha-"
    assert payload["completed_input"] == "alpha-"
    assert payload["candidates"] == ["alpha-one.txt", "alpha-two.txt"]


def test_shell_complete_uses_session_cwd(tmp_path: Path) -> None:
    session_id = "test-complete-session"
    target = tmp_path / "complete-child"
    target.mkdir()
    (target / "only-match.txt").write_text("x")

    first = client.post(
        "/api/v1/shell/run",
        json={"argv": ["bash", "-lc", f"cd {target}"], "session_id": session_id},
    )
    assert first.status_code == 200
    assert first.json()["exit_code"] == 0

    response = client.post(
        "/api/v1/shell/complete",
        json={"line": "only", "session_id": session_id},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["completed_input"] == "only-match.txt"
    assert payload["candidates"] == ["only-match.txt"]
