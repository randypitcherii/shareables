# Research: Agent-Assisted Local OAuth Proxy for Cursor IDE

**Date:** 2026-03-13
**Status:** Feasibility Research Complete

---

## 1. Feasibility Verdict

**YES — this is feasible, with one significant caveat around Cursor configuration.**

The core architecture works:
- A localhost Python proxy can inject Databricks OAuth tokens into forwarded requests
- `databricks-sdk` handles OAuth U2M flow including browser login and automatic token refresh
- Token cache persists at `~/.databricks/token-cache.json`, so re-authentication is rare
- The proxy can be packaged as a PyPI tool and run via `uvx` or installed as a system service
- Cursor supports "Override OpenAI Base URL" pointing to localhost

**Primary risk:** Cursor's "Override OpenAI Base URL" is a **global setting** — it affects ALL models including Cursor Pro models. Users must toggle it or give up built-in Cursor models. This is a known Cursor limitation confirmed across multiple forum reports (as of March 2026).

**Secondary risk:** Cursor stores API keys in secure OS storage (Keychain on macOS, etc.), not in a plain config file. The OpenAI API Key and Base URL override can only be set through the GUI — there is no supported way to set them programmatically. The agent prompt must instruct the user to manually configure these two fields.

---

## 2. Cursor Settings — Exact File Paths and Schema

### Settings Storage Architecture

Cursor does **NOT** use a plain `settings.json` for model provider settings. It uses a hybrid approach:

| Setting Type | Storage Location |
|---|---|
| VS Code-inherited settings | `~/Library/Application Support/Cursor/User/settings.json` (macOS) |
| Cursor-specific state (model config, etc.) | SQLite DB: `~/Library/Application Support/Cursor/User/globalStorage/state.vscdb` |
| API keys (OpenAI, Anthropic) | OS secure storage (macOS Keychain, Windows Credential Manager, Linux secret-service) |
| CLI config | `~/.cursor/cli-config.json` |

**Cross-platform settings DB paths:**
- **macOS:** `~/Library/Application Support/Cursor/User/globalStorage/state.vscdb`
- **Linux:** `~/.config/Cursor/User/globalStorage/state.vscdb` (or `$XDG_CONFIG_HOME/Cursor/...`)
- **Windows:** `%APPDATA%\Cursor\User\globalStorage\state.vscdb`

### OpenAI-Compatible Model Configuration (GUI only)

In `Cursor Settings > Models`:
1. **OpenAI API Key** — stored in OS secure storage, not readable from files
2. **Override OpenAI Base URL** — toggle + URL field (e.g., `http://127.0.0.1:8787/v1`)
3. **Custom Models** — add model names that map to the overridden endpoint

**Critical limitation:** The Override OpenAI Base URL applies to ALL OpenAI-routed models globally. If you set it to your localhost proxy, Cursor Pro models that route through OpenAI will also hit your proxy (and fail). Users must either:
- Only use custom models when the override is active
- Toggle the override on/off when switching (painful UX)

### Hot-Reload Behavior

- Settings changes in the GUI take effect immediately (no restart needed)
- The SQLite DB has a WAL journal; external writes risk corruption if Cursor is running
- **Recommendation:** Do NOT attempt to modify settings programmatically. Instruct users to use the GUI.

### Cursor CLI (Agent Mode)

The Cursor CLI (`~/.cursor/cli-config.json`) is separate from the IDE model settings. It has its own model selection but does **not** support custom OpenAI base URL override. CLI model config:
```json
{
  "model": "claude-sonnet-4-20250514",
  "hasChangedDefaultModel": true
}
```

---

## 3. Proxy Architecture

### Recommended Tech Stack

```
databricks-cursor-proxy (PyPI package)
├── proxy server (aiohttp or FastAPI/Uvicorn)
│   ├── /v1/chat/completions  → forward to gateway with Bearer token
│   ├── /v1/models            → return available models from gateway
│   └── /health               → health check
├── auth module (databricks-sdk)
│   ├── OAuth U2M browser flow (initial login)
│   ├── Token cache (~/.databricks/token-cache.json)
│   └── Auto-refresh (SDK handles this)
└── CLI entry point
    ├── setup  → auth + configure service
    ├── start  → run proxy foreground
    └── status → check proxy health
```

### Key Dependencies

| Package | Purpose |
|---|---|
| `databricks-sdk` | OAuth U2M flow, token refresh, credential caching |
| `aiohttp` or `uvicorn[standard]` + `fastapi` | Async HTTP proxy with SSE streaming support |
| `httpx` | Async HTTP client for forwarding requests to gateway |
| `click` or `typer` | CLI entry point |

### Proxy Implementation (Estimated ~200-300 LOC)

Core logic is straightforward:

```python
# Pseudocode for the proxy handler
async def proxy_chat_completions(request):
    # 1. Get fresh token from databricks-sdk (auto-refreshes)
    headers = workspace_client.config.authenticate()

    # 2. Forward request to gateway
    gateway_url = f"{GATEWAY_BASE_URL}/v1/chat/completions"

    # 3. Handle streaming vs non-streaming
    if request_body.get("stream"):
        # Stream SSE chunks back to Cursor
        async with httpx_client.stream("POST", gateway_url,
                                        headers=headers,
                                        json=request_body) as response:
            # Relay each SSE chunk
            async for chunk in response.aiter_bytes():
                yield chunk
    else:
        response = await httpx_client.post(gateway_url,
                                           headers=headers,
                                           json=request_body)
        return response.json()
```

### Token Refresh Flow

The `databricks-sdk` handles this transparently:

1. **Initial auth:** `databricks auth login --host <workspace-url>` opens browser for SSO
2. **Token cache:** Saved to `~/.databricks/token-cache.json` with refresh token
3. **Auto-refresh:** `config.authenticate()` checks expiry and uses refresh token — NO browser re-open
4. **Refresh token lifetime:** Configurable per workspace (default varies, typically days-weeks)
5. **Re-auth needed:** Only when refresh token itself expires (rare with `offline_access` scope)

Key SDK code for programmatic token access:
```python
from databricks.sdk.core import Config

config = Config(host="https://<workspace>.cloud.databricks.com")
token = config.oauth_token().access_token
# OR
headers = config.authenticate()  # Returns {"Authorization": "Bearer <token>"}
```

---

## 4. Service Persistence

### macOS (launchd)

File: `~/Library/LaunchAgents/com.databricks.cursor-proxy.plist`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.databricks.cursor-proxy</string>
    <key>ProgramArguments</key>
    <array>
        <string>/Users/USERNAME/.local/bin/databricks-cursor-proxy</string>
        <string>start</string>
        <string>--gateway-url</string>
        <string>https://GATEWAY_URL</string>
        <string>--port</string>
        <string>8787</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/databricks-cursor-proxy.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/databricks-cursor-proxy.err</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/Users/USERNAME/.local/bin</string>
        <key>HOME</key>
        <string>/Users/USERNAME</string>
    </dict>
</dict>
</plist>
```

Commands:
```bash
launchctl load ~/Library/LaunchAgents/com.databricks.cursor-proxy.plist
launchctl unload ~/Library/LaunchAgents/com.databricks.cursor-proxy.plist
launchctl list | grep databricks
```

### Linux (systemd user unit)

File: `~/.config/systemd/user/databricks-cursor-proxy.service`

```ini
[Unit]
Description=Databricks Cursor OAuth Proxy
After=network-online.target

[Service]
Type=simple
ExecStart=%h/.local/bin/databricks-cursor-proxy start --gateway-url https://GATEWAY_URL --port 8787
Restart=on-failure
RestartSec=5
Environment=HOME=%h
Environment=PATH=%h/.local/bin:/usr/local/bin:/usr/bin:/bin

[Install]
WantedBy=default.target
```

Commands:
```bash
systemctl --user daemon-reload
systemctl --user enable databricks-cursor-proxy
systemctl --user start databricks-cursor-proxy
systemctl --user status databricks-cursor-proxy
```

### Windows

Simplest approach: Startup folder shortcut.

File: `%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\databricks-cursor-proxy.bat`

```bat
@echo off
start /B "" "%USERPROFILE%\.local\bin\databricks-cursor-proxy.exe" start --gateway-url https://GATEWAY_URL --port 8787
```

Alternative: Use `pythonw` (no console window) or register as a Windows Service via `pywin32` / `nssm`.

---

## 5. Agent Prompt Design

### The Copyable Prompt

The gateway dashboard generates a personalized prompt like:

```
Set up the Databricks AI Gateway proxy for Cursor. Run this command:

uvx databricks-cursor-proxy setup --gateway-url https://my-gateway.databricks.app --port 8787

This will:
1. Open your browser for Databricks SSO login (one-time)
2. Start a local proxy on http://127.0.0.1:8787
3. Install it as a system service (auto-starts on login)

Then in Cursor, go to Settings > Models and:
- Set "OpenAI API Key" to: gateway-local-proxy
- Enable "Override OpenAI Base URL" and set it to: http://127.0.0.1:8787/v1
- Click "+ Add Custom Model" and add the model names shown at http://127.0.0.1:8787/v1/models
```

### How `uvx` Works for This

- `uvx databricks-cursor-proxy setup` runs the `setup` CLI command in a temporary venv
- For persistent service, `setup` internally runs `uv tool install databricks-cursor-proxy` to get a permanent installation
- The installed binary at `~/.local/bin/databricks-cursor-proxy` is what the service runs

**Key insight:** `uvx` CAN run long-lived processes (it just keeps the temp venv alive). But for a system service, `uv tool install` is the right approach — it creates a persistent venv and symlinks the entry point to `~/.local/bin/`.

### Cross-Agent Compatibility

The prompt works with:
- **Cursor (AI chat):** Agent runs the shell command, then instructs user on GUI steps
- **Claude Code CLI:** Runs the command directly via Bash tool
- **Gemini CLI / Codex CLI:** Same — shell command execution
- **Manual:** User can just copy-paste the `uvx` command into terminal

### OS Detection

The `setup` command detects OS at runtime:
```python
import platform
system = platform.system()  # 'Darwin', 'Linux', 'Windows'
```

No need for OS detection in the prompt itself.

---

## 6. Security Analysis

### Risks and Mitigations

| Risk | Severity | Mitigation |
|---|---|---|
| Other local processes can hit the proxy | Low | Bind to `127.0.0.1` only (not `0.0.0.0`). Local-only exposure. |
| Token visible in proxy memory | Low | Standard for any OAuth client. Token is short-lived (1hr). |
| Proxy logs could contain tokens | Medium | Never log Authorization headers. Log only request metadata. |
| Cursor sends API key field to proxy | Low | Proxy ignores the API key Cursor sends; uses its own OAuth token. |
| Refresh token in file cache | Medium | `~/.databricks/token-cache.json` has user-only permissions (0600). Same as `databricks auth login`. |
| Proxy could be used to exfiltrate | Low | Proxy only forwards to the configured gateway URL. Hardcoded destination. |

### Token Storage

The `databricks-sdk` caches OAuth tokens at:
- **All platforms:** `~/.databricks/token-cache.json`
- Contains: access token, refresh token, expiry timestamp
- File permissions: set by the SDK (user-readable only)
- Same location used by `databricks auth login` — no new security surface

### Recommended Security Measures

1. **Bind to 127.0.0.1 only** — never expose to network
2. **Validate gateway URL** — only forward to the configured Databricks Apps URL
3. **Strip sensitive headers from logs** — never log Bearer tokens
4. **Optional: local shared secret** — proxy could require a header secret that Cursor sends via the API key field (e.g., a random UUID generated at setup time). This prevents other local processes from using the proxy without the secret.

---

## 7. Estimated Effort

| Component | Effort | Notes |
|---|---|---|
| Proxy server (async, streaming) | 2-3 days | ~200-300 LOC. FastAPI+httpx or aiohttp. |
| Databricks OAuth integration | 1 day | SDK does the heavy lifting. Mainly plumbing `config.authenticate()`. |
| CLI (setup, start, status) | 1-2 days | `click` or `typer`. OS detection, service installation. |
| Service templates (launchd/systemd/Windows) | 1-2 days | Template generation + installation logic per OS. |
| PyPI packaging | 0.5 days | `pyproject.toml`, entry point, build + publish. |
| Testing + edge cases | 2-3 days | Token expiry, SSE streaming, error handling, reconnection. |
| Documentation + prompt engineering | 1 day | Gateway dashboard integration, user-facing docs. |
| **Total** | **~8-12 days** | For a production-quality, cross-platform tool. |

### MVP (macOS only, no service)

| Component | Effort |
|---|---|
| Proxy + OAuth + CLI | 3-4 days |
| PyPI package | 0.5 days |
| Basic docs | 0.5 days |
| **MVP Total** | **~4-5 days** |

---

## 8. Prior Art

### Directly Relevant

| Project | Description | Relevance |
|---|---|---|
| **[ProxyPilot](https://github.com/Finesssee/ProxyPilot)** (CLIProxyAPI fork) | Windows-native local proxy with TUI, system tray, multi-provider OAuth for AI coding tools. Go-based. 118 stars. | Very similar architecture. Has Cursor integration docs. Validates the localhost proxy pattern works with Cursor. |
| **[LiteLLM Proxy](https://docs.litellm.ai/docs/tutorials/cursor_integration)** | Full-featured OpenAI-compatible proxy with budget controls, logging. Has official Cursor integration guide. | Proven Cursor integration. But heavyweight (Postgres, Redis, dashboard). Overkill for our use case. Does NOT handle Databricks OAuth natively. |
| **[cursor-api-proxy](https://reddit.com/r/LocalLLM/...)** | Node.js proxy that exposes Cursor CLI models as OpenAI API. | Reverse direction (exposes Cursor, not wraps external). Validates SSE streaming proxy pattern. |

### Databricks-Adjacent

| Project | Description | Relevance |
|---|---|---|
| **databricks-sdk** Python | Built-in OAuth U2M with browser flow and token caching | Our core auth dependency. Production-grade. |
| **databricks auth login** CLI | CLI command for OAuth browser login, caches at `~/.databricks/token-cache.json` | We can either invoke this or replicate its flow via SDK. |
| **LiteLLM Databricks provider** | `litellm.completion(model="databricks/...")` with PAT auth | Uses PAT tokens, not OAuth. Could be adapted but adds unnecessary complexity. |

### Patterns to Learn From

- **ProxyPilot:** Service management (tray icon on Windows, launchd on macOS), multi-provider routing
- **LiteLLM Cursor guide:** Exact Cursor settings walkthrough (Base URL must end with `/cursor` in their case, we'd use `/v1`)
- **Ollama + Cursor guides:** Prove that `http://localhost:PORT/v1` as OpenAI Base URL works

---

## 9. Open Questions (Require Hands-On Testing)

### Must Verify

1. **Does Cursor's Override OpenAI Base URL work with `http://127.0.0.1:8787/v1` for streaming (SSE)?**
   - Forum reports suggest HTTP/1.1 mode may be needed (`Cursor Settings > Network > HTTP Compatibility Mode`)
   - HTTP/2 can cause issues with custom endpoints

2. **Does Cursor send proper `stream: true` in request body?**
   - Need to verify the exact request format Cursor sends

3. **What model names does Cursor send in the request?**
   - When a custom model "gateway-claude-sonnet" is added, does Cursor send that exact string?
   - The proxy/gateway needs to map these to actual serving endpoint model names

4. **Can the proxy's "API key" field be used as a local shared secret?**
   - Cursor sends whatever you put in "OpenAI API Key" as `Authorization: Bearer <key>`
   - Proxy could validate this before forwarding (replacing it with the OAuth token)

5. **Token refresh under load:**
   - If the OAuth token expires mid-stream, does the SDK's refresh happen synchronously?
   - Should we proactively refresh 5 minutes before expiry?

### Nice to Verify

6. **Can `uv tool install` from a private PyPI (Databricks-hosted)?**
   - If we don't want to publish to public PyPI, can we host on a private index?
   - `uvx --index-url https://... databricks-cursor-proxy` should work

7. **Cursor Agent mode with custom models:**
   - Forum reports that Agent mode "doesn't support custom API keys yet" — need to test if this has changed
   - If Agent mode doesn't work, only Ask/Plan modes would be available

8. **Windows service reliability:**
   - Is a startup .bat sufficient, or do we need NSSM / Windows Service registration?

9. **Multiple gateway support:**
   - Can one proxy serve multiple gateways with different auth?
   - Path-based routing: `/gateway-a/v1/...` vs `/gateway-b/v1/...`

---

## Appendix: Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│ User's Machine                                                   │
│                                                                   │
│  ┌──────────┐     http://127.0.0.1:8787/v1     ┌──────────────┐ │
│  │          │ ──────────────────────────────────>│              │ │
│  │  Cursor  │     POST /v1/chat/completions     │  Local OAuth │ │
│  │   IDE    │     Authorization: Bearer xxx     │    Proxy     │ │
│  │          │ <──────────────────────────────────│              │ │
│  └──────────┘     SSE stream / JSON response    │  (Python)    │ │
│                                                  │              │ │
│  ┌──────────┐                                    │  - Replaces  │ │
│  │ Databricks│   ~/.databricks/token-cache.json  │    Bearer    │ │
│  │   SDK     │ <─────────────────────────────────│    token     │ │
│  │  (Auth)   │   OAuth refresh (automatic)       │  - Forwards  │ │
│  └──────────┘                                    │    to gateway│ │
│       │                                          └──────┬───────┘ │
│       │ (first-time only: opens browser)                │         │
│       v                                                 │         │
│  ┌──────────┐                                           │         │
│  │ Browser  │                                           │         │
│  │  SSO     │                                           │         │
│  └──────────┘                                           │         │
└─────────────────────────────────────────────────────────┼─────────┘
                                                          │
                                    HTTPS + Bearer token  │
                                                          v
                                              ┌───────────────────┐
                                              │  Databricks App   │
                                              │  (Agent Gateway)  │
                                              │                   │
                                              │  /v1/chat/...     │
                                              │  /v1/models       │
                                              └───────────────────┘
```

---

## Appendix: Comparison with Alternative Approaches

### Alternative 1: Direct PAT Token (No Proxy)

- User generates a Databricks PAT, pastes it into Cursor's "OpenAI API Key" field
- Gateway validates the PAT directly
- **Pros:** No proxy needed, simpler setup
- **Cons:** PAT tokens are long-lived secrets, harder to rotate, against Databricks OAuth-first direction

### Alternative 2: LiteLLM as the Proxy

- Deploy LiteLLM with Databricks provider configured
- Point Cursor at LiteLLM
- **Pros:** Battle-tested, feature-rich
- **Cons:** Heavyweight (needs Postgres for budget tracking), doesn't handle Databricks OAuth U2M natively (only PAT/M2M), overkill for single-user localhost use case

### Alternative 3: MCP Server Instead of OpenAI Proxy

- Register the gateway as an MCP server in Cursor's `mcp.json`
- Cursor supports OAuth for MCP servers natively
- **Pros:** Cursor handles OAuth redirect flow natively, no local proxy needed
- **Cons:** MCP is for tools/resources, not for model providers. Cursor's model routing doesn't go through MCP. This would provide tools but not replace the model itself.

### Recommendation

The **local OAuth proxy** (this research) is the best fit because:
1. It works with Cursor's existing BYOK model configuration
2. It handles OAuth lifecycle transparently
3. It's lightweight (single Python process, ~200 LOC)
4. It works with any OpenAI-compatible client, not just Cursor
