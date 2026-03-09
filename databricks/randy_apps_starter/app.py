import asyncio
import logging
import os
from pathlib import Path
import re
import shlex
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("randy_apps_starter")

app = FastAPI(title="Randy Databricks App Starter", version="0.1.0")
RUNTIME_MARKER = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
SESSION_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
SESSION_STATE_DB_PATH = os.getenv(
    "SESSION_STATE_DB_PATH",
    "/tmp/randy_apps_starter_session_state.sqlite3",
)


class SessionCwdStore:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        db_parent = os.path.dirname(db_path)
        if db_parent:
            os.makedirs(db_parent, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._db_path, timeout=5.0)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    def _init_db(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS shell_session_state (
                    session_id TEXT PRIMARY KEY,
                    cwd TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            connection.commit()

    def get_cwd(self, session_id: str) -> str | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT cwd FROM shell_session_state WHERE session_id = ?",
                (session_id,),
            ).fetchone()
        return row[0] if row else None

    def set_cwd(self, session_id: str, cwd: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO shell_session_state(session_id, cwd, updated_at)
                VALUES(?, ?, strftime('%s', 'now'))
                ON CONFLICT(session_id) DO UPDATE
                SET cwd = excluded.cwd, updated_at = excluded.updated_at
                """,
                (session_id, cwd),
            )
            connection.commit()

    def clear(self) -> None:
        with self._connect() as connection:
            connection.execute("DELETE FROM shell_session_state")
            connection.commit()


SESSION_CWD_STORE = SessionCwdStore(SESSION_STATE_DB_PATH)
SHELL_SESSION_CWD: dict[str, str] = {}

_STATIC_DIR = Path(__file__).resolve().parent / "static"
MANUAL_UI_HTML = (_STATIC_DIR / "terminal.html").read_text()


class ShellRunRequest(BaseModel):
    argv: list[str] = Field(..., min_length=1)
    cwd: str | None = None
    session_id: str | None = None
    timeout_seconds: int = Field(default=20, ge=1, le=120)


class ShellCompleteRequest(BaseModel):
    line: str = ""
    cwd: str | None = None
    session_id: str | None = None
    max_candidates: int = Field(default=100, ge=1, le=500)


def _validated_session_id(raw: str | None) -> str | None:
    if raw is None:
        return None
    session_id = raw.strip()
    if not session_id:
        return None
    if not SESSION_ID_RE.fullmatch(session_id):
        raise HTTPException(status_code=400, detail="Invalid session_id")
    return session_id


def _resolve_cwd(payload: Any) -> tuple[str, str | None]:
    session_id = _validated_session_id(payload.session_id)
    if payload.cwd:
        cwd = payload.cwd
    elif session_id:
        cwd = SHELL_SESSION_CWD.get(session_id)
        if not cwd:
            cwd = SESSION_CWD_STORE.get_cwd(session_id) or os.getcwd()
            SHELL_SESSION_CWD[session_id] = cwd
    else:
        cwd = os.getcwd()
    if session_id and payload.cwd:
        SHELL_SESSION_CWD[session_id] = cwd
        SESSION_CWD_STORE.set_cwd(session_id, cwd)
    return cwd, session_id


def _split_completion_fragment(line: str) -> tuple[str, str]:
    if not line:
        return "", ""
    if line[-1].isspace():
        return line, ""
    boundary = max(line.rfind(" "), line.rfind("\t"))
    if boundary == -1:
        return "", line
    return line[: boundary + 1], line[boundary + 1 :]


def _path_completion_candidates(fragment: str, cwd: str) -> list[str]:
    if "/" in fragment:
        dir_part, name_prefix = fragment.rsplit("/", 1)
    else:
        dir_part, name_prefix = "", fragment

    if fragment.startswith("/") and dir_part == "":
        typed_dir_prefix = "/"
        absolute_dir = "/"
    else:
        typed_dir_prefix = f"{dir_part}/" if dir_part else ""
        if dir_part.startswith("~"):
            absolute_dir = os.path.expanduser(dir_part)
        elif os.path.isabs(dir_part):
            absolute_dir = dir_part
        else:
            absolute_dir = os.path.join(cwd, dir_part or ".")

    try:
        entries = sorted(os.listdir(absolute_dir))
    except OSError:
        return []

    include_hidden = name_prefix.startswith(".")
    candidates: list[str] = []
    for entry in entries:
        if not include_hidden and entry.startswith("."):
            continue
        if not entry.startswith(name_prefix):
            continue
        candidate = f"{typed_dir_prefix}{entry}"
        if os.path.isdir(os.path.join(absolute_dir, entry)):
            candidate += "/"
        candidates.append(candidate)
    return candidates


def _command_completion_candidates(fragment: str) -> list[str]:
    include_hidden = fragment.startswith(".")
    candidates: set[str] = set()
    for dir_path in os.getenv("PATH", "").split(os.pathsep):
        if not dir_path:
            continue
        try:
            entries = os.listdir(dir_path)
        except OSError:
            continue
        for entry in entries:
            if not include_hidden and entry.startswith("."):
                continue
            if not entry.startswith(fragment):
                continue
            full_path = os.path.join(dir_path, entry)
            if os.path.isfile(full_path) and os.access(full_path, os.X_OK):
                candidates.add(entry)
    candidates.update(
        builtin
        for builtin in {"cd", "echo", "exit", "pwd", "export", "unset", "type", "alias", "unalias"}
        if builtin.startswith(fragment)
    )
    return sorted(candidates)


def _shell_complete(line: str, cwd: str, max_candidates: int = 100) -> dict[str, Any]:
    prefix, fragment = _split_completion_fragment(line)
    first_token = not prefix.strip()
    path_candidates = _path_completion_candidates(fragment, cwd)
    candidates = path_candidates
    if first_token and "/" not in fragment:
        candidates = sorted(set(path_candidates).union(_command_completion_candidates(fragment)))
    candidates = candidates[:max_candidates]

    if not candidates:
        common_prefix = fragment
    elif len(candidates) == 1:
        common_prefix = candidates[0]
    else:
        common_prefix = os.path.commonprefix(candidates)
    completed_input = line if not candidates else f"{prefix}{common_prefix}"

    return {
        "input": line,
        "fragment": fragment,
        "common_prefix": common_prefix,
        "completed_input": completed_input,
        "candidates": candidates,
    }


def _wrap_bash_command_for_cwd_capture(argv: list[str], cwd_marker: str) -> list[str]:
    if len(argv) >= 3 and argv[0] in {"bash", "/bin/bash"} and argv[1] == "-lc":
        command = argv[2]
        wrapped = (
            f"{command}; __rcp_ec=$?; "
            f"printf '\\n{cwd_marker}%s\\n' \"$PWD\"; "
            "exit $__rcp_ec"
        )
        return [argv[0], argv[1], wrapped, *argv[3:]]
    return argv


def _extract_cwd_marker(text: str, cwd_marker: str) -> tuple[str, str | None]:
    marker_with_path = re.compile(rf"(?:^|\n){re.escape(cwd_marker)}([^\r\n]*)\r?\n?")
    new_cwd: str | None = None

    def _replace(match: re.Match[str]) -> str:
        nonlocal new_cwd
        new_cwd = match.group(1)
        prefix = match.group(0)
        return "\n" if prefix.startswith("\n") else ""

    cleaned = marker_with_path.sub(_replace, text)
    return cleaned, new_cwd


@app.get("/", response_class=HTMLResponse)
def manual_ui() -> str:
    return MANUAL_UI_HTML.replace("__RUNTIME_MARKER__", RUNTIME_MARKER)


@app.get("/api/v1/healthcheck")
def healthcheck() -> dict[str, Any]:
    return {
        "status": "ok",
        "app": "randy-apps-starter",
        "mode": "databricks-app" if os.getenv("DATABRICKS_APP_NAME") else "local",
        "runtime_marker": RUNTIME_MARKER,
    }


@app.get("/api/v1/db/health")
def db_health() -> dict[str, Any]:
    from db import check_connectivity

    return check_connectivity()


@app.get("/api/v1/auth/context")
def auth_context(
    x_forwarded_user: str | None = Header(default=None),
    x_forwarded_access_token: str | None = Header(default=None),
) -> dict[str, Any]:
    return {
        "forwarded_user": x_forwarded_user,
        "obo_token_present": bool(x_forwarded_access_token),
        "mode": "databricks-app" if os.getenv("DATABRICKS_APP_NAME") else "local",
    }


@app.post("/api/v1/shell/run")
async def shell_run(payload: ShellRunRequest) -> dict[str, Any]:
    request_id = str(uuid.uuid4())
    started = time.time()
    cwd, session_id = _resolve_cwd(payload)
    cwd_marker = f"__RCP_CWD_{request_id}__="
    argv = _wrap_bash_command_for_cwd_capture(payload.argv, cwd_marker)
    logger.info(
        "shell.run.start request_id=%s cwd=%s timeout_seconds=%s argv=%s",
        request_id,
        cwd,
        payload.timeout_seconds,
        payload.argv[:20],
    )

    process = await asyncio.create_subprocess_exec(
        *argv,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(),
            timeout=payload.timeout_seconds,
        )
    except TimeoutError as exc:
        process.kill()
        await process.wait()
        logger.warning("shell.run.timeout request_id=%s", request_id)
        raise HTTPException(
            status_code=408,
            detail=f"Command timed out after {payload.timeout_seconds} seconds",
        ) from exc

    duration_ms = int((time.time() - started) * 1000)
    decoded_stdout = stdout.decode("utf-8", errors="replace")
    decoded_stdout, marker_cwd = _extract_cwd_marker(decoded_stdout, cwd_marker)
    if session_id and marker_cwd:
        SHELL_SESSION_CWD[session_id] = marker_cwd
        SESSION_CWD_STORE.set_cwd(session_id, marker_cwd)
    result = {
        "request_id": request_id,
        "exit_code": process.returncode,
        "duration_ms": duration_ms,
        "stdout": decoded_stdout,
        "stderr": stderr.decode("utf-8", errors="replace"),
    }
    logger.info(
        "shell.run.done request_id=%s exit_code=%s duration_ms=%s",
        request_id,
        process.returncode,
        duration_ms,
    )
    return result


@app.post("/api/v1/shell/complete")
def shell_complete(payload: ShellCompleteRequest) -> dict[str, Any]:
    cwd, _ = _resolve_cwd(payload)
    return _shell_complete(payload.line, cwd, payload.max_candidates)


@app.post("/api/v1/shell/stream")
async def shell_stream(payload: ShellRunRequest) -> StreamingResponse:
    async def stream_output() -> Any:
        request_id = str(uuid.uuid4())
        started = time.time()
        cwd, session_id = _resolve_cwd(payload)
        cwd_marker = f"__RCP_CWD_{request_id}__="
        argv = _wrap_bash_command_for_cwd_capture(payload.argv, cwd_marker)
        next_cwd: str | None = None
        command_display = " ".join(shlex.quote(arg) for arg in payload.argv)
        logger.info(
            "shell.stream.start request_id=%s cwd=%s timeout_seconds=%s argv=%s",
            request_id,
            cwd,
            payload.timeout_seconds,
            payload.argv[:20],
        )
        yield f"$ {command_display}\n"
        process = await asyncio.create_subprocess_exec(
            *argv,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        timed_out = False
        while True:
            try:
                line = await asyncio.wait_for(
                    process.stdout.readline(),
                    timeout=payload.timeout_seconds,
                )
            except TimeoutError:
                process.kill()
                await process.wait()
                timed_out = True
                yield f"[error] Command timed out after {payload.timeout_seconds} seconds\n"
                break
            if not line:
                break
            decoded = line.decode("utf-8", errors="replace")
            cleaned, marker_cwd = _extract_cwd_marker(decoded, cwd_marker)
            if marker_cwd:
                next_cwd = marker_cwd
            if cleaned:
                yield cleaned

        if not timed_out:
            await process.wait()
        if session_id and next_cwd:
            SHELL_SESSION_CWD[session_id] = next_cwd
            SESSION_CWD_STORE.set_cwd(session_id, next_cwd)
        duration_ms = int((time.time() - started) * 1000)
        exit_code = process.returncode if process.returncode is not None else 124
        yield f"[exit={exit_code}, duration_ms={duration_ms}]\n"
        logger.info(
            "shell.stream.done request_id=%s exit_code=%s duration_ms=%s timed_out=%s",
            request_id,
            exit_code,
            duration_ms,
            timed_out,
        )

    return StreamingResponse(stream_output(), media_type="text/plain; charset=utf-8")
