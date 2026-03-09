import asyncio
import logging
import os
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
# Backward-compatible in-memory cache; persisted store remains source of truth.
SHELL_SESSION_CWD: dict[str, str] = {}


MANUAL_UI_HTML = r"""
<!doctype html>
<html>
  <head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>Randy Apps Starter Sandbox</title>
    <style>
      :root {
        color-scheme: dark;
        --bg: #0b1020;
        --panel: #121a2b;
        --border: #2a3a5e;
        --text: #dce7ff;
        --muted: #9fb2da;
        --ok: #5dd39e;
        --warn: #ffd166;
        --err: #ff6b6b;
        --terminal-bg: #060b16;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        padding: 20px;
        background: radial-gradient(80% 120% at 0% 0%, #182748 0%, var(--bg) 55%);
        color: var(--text);
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      }
      .shell-wrap {
        max-width: 1100px;
        margin: 0 auto;
        border: 1px solid var(--border);
        border-radius: 14px;
        background: linear-gradient(180deg, rgba(255, 255, 255, 0.03), rgba(255, 255, 255, 0));
        box-shadow: 0 20px 40px rgba(0, 0, 0, 0.35);
        overflow: hidden;
      }
      .topbar {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        padding: 10px 14px;
        border-bottom: 1px solid var(--border);
        background: rgba(7, 11, 20, 0.8);
      }
      .title {
        font-size: 14px;
        font-weight: 700;
        letter-spacing: 0.2px;
      }
      .hint {
        margin: 0;
        color: var(--muted);
        font-size: 12px;
      }
      .runtime-marker {
        margin-top: 6px;
        display: inline-block;
        padding: 4px 8px;
        border: 1px solid #5dd39e;
        border-radius: 8px;
        background: rgba(93, 211, 158, 0.1);
        color: #b8ffd8;
        font-size: 12px;
        font-weight: 700;
      }
      .dot-row { display: flex; gap: 6px; align-items: center; }
      .dot {
        width: 10px;
        height: 10px;
        border-radius: 999px;
        background: #ff5f56;
      }
      .dot:nth-child(2) { background: #ffbd2e; }
      .dot:nth-child(3) { background: #27c93f; }
      .actions { display: inline-flex; align-items: center; gap: 10px; }
      .controls {
        display: grid;
        grid-template-columns: 1fr;
        gap: 10px;
        padding: 12px;
        border-bottom: 1px solid var(--border);
        background: var(--panel);
      }
      input, button {
        min-height: 36px;
        border-radius: 9px;
        border: 1px solid var(--border);
        padding: 8px 10px;
        font: inherit;
      }
      input {
        background: #0c1426;
        color: var(--text);
      }
      button {
        cursor: pointer;
        background: #0f1c38;
        color: var(--text);
      }
      button:disabled { opacity: 0.55; cursor: not-allowed; }
      #status {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        font-size: 12px;
        color: var(--muted);
      }
      #status .pill {
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 99px;
        background: var(--warn);
      }
      #status[data-state="idle"] .pill { background: var(--ok); }
      #status[data-state="running"] .pill { background: var(--warn); }
      #status[data-state="error"] .pill { background: var(--err); }
      #terminal {
        height: 460px;
        background: var(--terminal-bg);
        padding: 10px;
        overflow: auto;
        font-size: 13px;
        line-height: 1.42;
        white-space: pre-wrap;
        word-break: break-word;
        outline: none;
        cursor: text;
      }
      #terminal:focus {
        box-shadow: inset 0 0 0 1px rgba(93, 211, 158, 0.2);
      }
      .line { min-height: 1.42em; }
      .line.prompt .prompt-symbol { color: #7aa2ff; }
      .line.prompt .prompt-input { color: #d8e6ff; }
      .line.stdout { color: #dce7ff; }
      .line.stderr { color: #ff9f9f; }
      .line.status-ok { color: #8cffbf; }
      .line.status-error { color: #ff8f8f; font-weight: 700; }
      .line.info { color: var(--muted); }
      .caret {
        display: inline-block;
        width: 0.55ch;
        height: 1.15em;
        margin-left: 1px;
        border-radius: 2px;
        background: linear-gradient(180deg, #f4f8ff 0%, #dce7ff 100%);
        box-shadow: 0 0 0 1px rgba(220, 231, 255, 0.35), 0 0 6px rgba(122, 162, 255, 0.35);
        vertical-align: text-bottom;
        opacity: 0.55;
      }
      #terminal:focus .caret {
        opacity: 1;
        animation: caret-blink 1.05s ease-in-out infinite;
      }
      @keyframes caret-blink {
        0%, 45% { opacity: 1; }
        55%, 100% { opacity: 0.12; }
      }
      .ansi-bold { font-weight: 700; }
      .ansi-fg-red { color: #ff6b6b; }
      .ansi-fg-green { color: #5dd39e; }
      .ansi-fg-yellow { color: #ffd166; }
      .ansi-fg-blue { color: #7aa2ff; }
      .ansi-fg-magenta { color: #d6a3ff; }
      .ansi-fg-cyan { color: #73d8ff; }
      .ansi-fg-white { color: #f7f9ff; }
      .ansi-fg-bright-black { color: #97a4bf; }
      .ansi-fg-bright-red { color: #ff8f8f; }
      .ansi-fg-bright-green { color: #8cffbf; }
      .ansi-fg-bright-yellow { color: #ffe296; }
      .ansi-fg-bright-blue { color: #9abbff; }
      .ansi-fg-bright-magenta { color: #e6b9ff; }
      .ansi-fg-bright-cyan { color: #9de7ff; }
      .ansi-fg-bright-white { color: #ffffff; }
    </style>
  </head>
  <body>
    <div class="shell-wrap" id="shellWrap">
      <div class="topbar">
        <div>
          <div class="title">Randy Apps Starter Sandbox</div>
          <p class="hint">Type directly inside the terminal. Enter runs, Up/Down browse history, Backspace edits.</p>
          <div class="runtime-marker">Runtime marker: __RUNTIME_MARKER__</div>
        </div>
        <div class="dot-row" aria-hidden="true">
          <span class="dot"></span><span class="dot"></span><span class="dot"></span>
        </div>
      </div>
      <div class="controls">
        <div class="actions">
          <button id="clearBtn" type="button">Clear Output</button>
          <div id="status" data-state="idle"><span class="pill"></span><span id="statusText">idle</span></div>
        </div>
      </div>
      <div id="terminal" tabindex="0" role="textbox" aria-multiline="true" aria-label="Shell transcript and input"></div>
    </div>
    <script>
      const shellWrap = document.getElementById("shellWrap");
      const clearBtn = document.getElementById("clearBtn");
      const status = document.getElementById("status");
      const statusText = document.getElementById("statusText");
      const terminalEl = document.getElementById("terminal");

      const HISTORY_LIMIT = 40;
      const PROMPT = "$ ";
      const SESSION_KEY = "shell_session_id_v1";
      const COMPLETE_ENDPOINT = "/api/v1/shell/complete";
      let history = [];
      let historyCursor = -1;
      let historyDraft = "";
      let running = false;
      let currentInput = "";
      let editorLine = null;
      let editorInputSpan = null;
      let editorCaret = null;
      let shellSessionId = "";
      let lastCompletionInput = "";
      let lastCompletionCandidatesKey = "";

      function createSessionId() {
        if (window.crypto && typeof window.crypto.randomUUID === "function") {
          return window.crypto.randomUUID();
        }
        return "session-" + Date.now().toString(36) + "-" + Math.random().toString(36).slice(2, 10);
      }

      function ensureSessionId() {
        try {
          let value = sessionStorage.getItem(SESSION_KEY);
          if (!value) {
            value = createSessionId();
            sessionStorage.setItem(SESSION_KEY, value);
          }
          return value;
        } catch (_) {
          return createSessionId();
        }
      }

      function setStatus(kind, text) {
        status.setAttribute("data-state", kind);
        statusText.textContent = text;
      }

      function resetCompletionState() {
        lastCompletionInput = "";
        lastCompletionCandidatesKey = "";
      }

      function getShellArgv(command) {
        return ["bash", "-lc", command];
      }

      function scrollToBottom() {
        terminalEl.scrollTop = terminalEl.scrollHeight;
      }

      function focusTerminal() {
        window.setTimeout(function () { terminalEl.focus(); }, 0);
      }

      function createLine(kind) {
        const line = document.createElement("div");
        line.className = "line " + kind;
        terminalEl.appendChild(line);
        return line;
      }

      function styleClassFromAnsi(state) {
        const classes = [];
        if (state.bold) classes.push("ansi-bold");
        if (state.fg) classes.push(state.fg);
        return classes.join(" ");
      }

      function applyAnsiCode(code, state) {
        if (code === 0) {
          state.bold = false;
          state.fg = "";
          return;
        }
        if (code === 1) {
          state.bold = true;
          return;
        }
        const map = {
          30: "ansi-fg-bright-black",
          31: "ansi-fg-red",
          32: "ansi-fg-green",
          33: "ansi-fg-yellow",
          34: "ansi-fg-blue",
          35: "ansi-fg-magenta",
          36: "ansi-fg-cyan",
          37: "ansi-fg-white",
          90: "ansi-fg-bright-black",
          91: "ansi-fg-bright-red",
          92: "ansi-fg-bright-green",
          93: "ansi-fg-bright-yellow",
          94: "ansi-fg-bright-blue",
          95: "ansi-fg-bright-magenta",
          96: "ansi-fg-bright-cyan",
          97: "ansi-fg-bright-white"
        };
        if (map[code]) {
          state.fg = map[code];
        }
      }

      function appendAnsiText(target, text) {
        const pattern = /\u001b\[([0-9;]*)m/g;
        let lastIdx = 0;
        const styleState = { bold: false, fg: "" };
        while (true) {
          const match = pattern.exec(text);
          if (!match) break;
          if (match.index > lastIdx) {
            const chunk = text.slice(lastIdx, match.index);
            const span = document.createElement("span");
            const className = styleClassFromAnsi(styleState);
            if (className) span.className = className;
            span.textContent = chunk;
            target.appendChild(span);
          }
          const codes = (match[1] || "0").split(";");
          codes.forEach(function (token) {
            const code = Number.parseInt(token || "0", 10);
            if (Number.isFinite(code)) applyAnsiCode(code, styleState);
          });
          lastIdx = pattern.lastIndex;
        }
        if (lastIdx < text.length) {
          const span = document.createElement("span");
          const className = styleClassFromAnsi(styleState);
          if (className) span.className = className;
          span.textContent = text.slice(lastIdx);
          target.appendChild(span);
        }
      }

      function appendClassifiedLine(text, kind) {
        const line = createLine(kind);
        appendAnsiText(line, text);
        scrollToBottom();
      }

      function createPromptEditor() {
        editorLine = createLine("prompt");
        const symbol = document.createElement("span");
        symbol.className = "prompt-symbol";
        symbol.textContent = PROMPT;
        editorInputSpan = document.createElement("span");
        editorInputSpan.className = "prompt-input";
        editorCaret = document.createElement("span");
        editorCaret.className = "caret";
        editorLine.appendChild(symbol);
        editorLine.appendChild(editorInputSpan);
        editorLine.appendChild(editorCaret);
        currentInput = "";
        historyCursor = -1;
        historyDraft = "";
        renderInput();
      }

      function renderInput() {
        if (!editorInputSpan || !editorCaret) return;
        editorInputSpan.textContent = currentInput;
        editorCaret.style.display = running ? "none" : "inline-block";
        scrollToBottom();
      }

      function finalizePromptLine() {
        if (!editorLine || !editorCaret) return;
        editorCaret.remove();
        editorCaret = null;
        editorInputSpan = null;
        editorLine = null;
      }

      function loadHistory() {
        try {
          const parsed = JSON.parse(localStorage.getItem("shell_command_history") || "[]");
          history = Array.isArray(parsed) ? parsed.filter(function (v) { return typeof v === "string"; }) : [];
        } catch (_) {
          history = [];
        }
      }

      function saveHistory(cmd) {
        const existing = history.indexOf(cmd);
        if (existing >= 0) history.splice(existing, 1);
        history.unshift(cmd);
        history = history.slice(0, HISTORY_LIMIT);
        localStorage.setItem("shell_command_history", JSON.stringify(history));
      }

      function historyUp() {
        if (!history.length) return;
        resetCompletionState();
        if (historyCursor === -1) {
          historyDraft = currentInput;
        }
        const nextCursor = Math.min(historyCursor + 1, history.length - 1);
        if (nextCursor !== historyCursor) {
          historyCursor = nextCursor;
          currentInput = history[historyCursor];
          renderInput();
        }
      }

      function historyDown() {
        if (historyCursor === -1) return;
        resetCompletionState();
        historyCursor -= 1;
        currentInput = historyCursor >= 0 ? history[historyCursor] : historyDraft;
        renderInput();
      }

      function showCompletionCandidates(candidates) {
        if (!Array.isArray(candidates) || !candidates.length) return;
        const preservedInput = currentInput;
        finalizePromptLine();
        appendClassifiedLine(candidates.join("  "), "info");
        createPromptEditor();
        currentInput = preservedInput;
        renderInput();
      }

      function classifyLine(lineText) {
        if (lineText.startsWith("[error]")) return "stderr";
        if (lineText.startsWith("[stderr]")) return "stderr";
        if (lineText.startsWith("[exit=")) {
          const match = lineText.match(/\[exit=(-?\d+)/);
          const exitCode = match ? Number.parseInt(match[1], 10) : 1;
          return exitCode === 0 ? "status-ok" : "status-error";
        }
        const stderrHint = /(\berror\b|\bfailed\b|\bexception\b|\btraceback\b|\bpermission denied\b|\bnot found\b|\bno such file\b)/i;
        if (stderrHint.test(lineText)) return "stderr";
        return "stdout";
      }

      async function handleTabCompletion() {
        if (running) return;
        const inputBefore = currentInput;
        try {
          const res = await fetch(COMPLETE_ENDPOINT, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ line: inputBefore, session_id: shellSessionId })
          });
          if (!res.ok) {
            resetCompletionState();
            return;
          }
          const payload = await res.json();
          const candidates = Array.isArray(payload.candidates) ? payload.candidates : [];
          if (!candidates.length) {
            resetCompletionState();
            return;
          }
          const nextInput = typeof payload.completed_input === "string" ? payload.completed_input : inputBefore;
          const hasFurtherPrefix = nextInput !== inputBefore;
          const candidatesKey = candidates.join("\n");
          const repeatedTab =
            lastCompletionInput === inputBefore &&
            lastCompletionCandidatesKey === candidatesKey;

          currentInput = nextInput;
          renderInput();

          if (candidates.length === 1) {
            resetCompletionState();
            return;
          }
          if (repeatedTab || !hasFurtherPrefix) {
            showCompletionCandidates(candidates);
          }
          lastCompletionInput = inputBefore;
          lastCompletionCandidatesKey = candidatesKey;
        } catch (_) {
          // Silent fallback keeps terminal interaction uninterrupted.
        }
      }

      async function runCommand(commandRaw) {
        if (running) return;
        resetCompletionState();
        const command = commandRaw.trim();
        finalizePromptLine();
        if (!command) {
          appendClassifiedLine("[error] command is empty", "stderr");
          createPromptEditor();
          return;
        }

        const payload = { argv: getShellArgv(command), timeout_seconds: 20, session_id: shellSessionId };

        running = true;
        setStatus("running", "running...");
        saveHistory(command);
        appendClassifiedLine(PROMPT + command, "prompt");

        let skippedBackendPrompt = false;
        try {
          const res = await fetch("/api/v1/shell/stream", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(payload)
          });
          if (!res.ok) {
            const detailText = await res.text();
            appendClassifiedLine("[error] " + (detailText || ("HTTP " + res.status)), "stderr");
            setStatus("error", "request failed");
            return;
          }
          if (!res.body) {
            appendClassifiedLine("[error] response body is unavailable for streaming", "stderr");
            setStatus("error", "stream unavailable");
            return;
          }

          const reader = res.body.getReader();
          const decoder = new TextDecoder();
          let buffer = "";
          while (true) {
            const part = await reader.read();
            if (part.done) break;
            buffer += decoder.decode(part.value, { stream: true });
            const lines = buffer.split(/\r?\n/);
            buffer = lines.pop() || "";
            lines.forEach(function (line) {
              if (!skippedBackendPrompt && line.startsWith("$ ")) {
                skippedBackendPrompt = true;
                return;
              }
              const kind = classifyLine(line);
              appendClassifiedLine(line, kind);
              if (line.startsWith("[exit=")) {
                const match = line.match(/\[exit=(-?\d+)/);
                const exitCode = match ? Number.parseInt(match[1], 10) : 1;
                setStatus(exitCode === 0 ? "idle" : "error", exitCode === 0 ? "done" : "exit " + exitCode);
              }
            });
          }
          if (buffer) {
            const kind = classifyLine(buffer);
            appendClassifiedLine(buffer, kind);
          }
        } catch (err) {
          appendClassifiedLine("[error] " + (err && err.message ? err.message : String(err)), "stderr");
          setStatus("error", "network error");
        } finally {
          running = false;
          if (statusText.textContent === "running...") {
            setStatus("idle", "done");
          }
          createPromptEditor();
          terminalEl.focus();
        }
      }

      terminalEl.addEventListener("keydown", function (event) {
        if (event.key === "Tab") {
          event.preventDefault();
          handleTabCompletion();
          return;
        }
        if (running) {
          event.preventDefault();
          return;
        }
        if (event.key === "Enter") {
          event.preventDefault();
          runCommand(currentInput);
          return;
        }
        if (event.key === "Backspace") {
          event.preventDefault();
          resetCompletionState();
          if (currentInput.length > 0) {
            currentInput = currentInput.slice(0, -1);
            renderInput();
          }
          return;
        }
        if (event.key === "ArrowUp") {
          event.preventDefault();
          historyUp();
          return;
        }
        if (event.key === "ArrowDown") {
          event.preventDefault();
          historyDown();
          return;
        }
        if (event.key.length === 1 && !event.ctrlKey && !event.metaKey && !event.altKey) {
          event.preventDefault();
          resetCompletionState();
          currentInput += event.key;
          renderInput();
        }
      });

      terminalEl.addEventListener("mousedown", function () {
        focusTerminal();
      });

      shellWrap.addEventListener("mousedown", function (event) {
        if (
          event.target instanceof HTMLElement &&
          (event.target.closest("button") || event.target.closest("input"))
        ) {
          return;
        }
        focusTerminal();
      });

      terminalEl.addEventListener("paste", function (event) {
        if (running) {
          event.preventDefault();
          return;
        }
        event.preventDefault();
        const clipboard = event.clipboardData || window.clipboardData;
        const rawText = clipboard ? clipboard.getData("text") : "";
        if (!rawText) return;
        const normalized = rawText.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
        const firstLine = normalized.split("\n", 1)[0];
        if (firstLine) {
          resetCompletionState();
          currentInput += firstLine;
          renderInput();
        }
      });

      clearBtn.addEventListener("click", function () {
        resetCompletionState();
        terminalEl.textContent = "";
        appendClassifiedLine("[info] output cleared", "info");
        setStatus("idle", "idle");
        createPromptEditor();
        terminalEl.focus();
      });

      loadHistory();
      shellSessionId = ensureSessionId();
      appendClassifiedLine("Ready. Type inside this terminal and press Enter.", "info");
      createPromptEditor();
      terminalEl.focus();
    </script>
  </body>
</html>
"""


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
        completed_input = line
    elif len(candidates) == 1:
        common_prefix = candidates[0]
        completed_input = f"{prefix}{common_prefix}"
    else:
        common_prefix = os.path.commonprefix(candidates)
        completed_input = f"{prefix}{common_prefix}"

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

    # MVP mode: accepts argv directly, including bash -lc usage in sandbox.
    run_fn = getattr(asyncio, "create_subprocess_exec")
    process = await run_fn(
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
