# Configuring Custom OpenAI-Compatible Endpoints for AI Coding Tools

Research completed 2026-03-15. Each tool below can be pointed at any OpenAI-compatible `/v1/chat/completions` endpoint.

---

## 1. Claude Code (Anthropic CLI)

**Config mechanism:** Environment variables

| Variable | Purpose |
|---|---|
| `ANTHROPIC_BASE_URL` | Custom API base URL (replaces `https://api.anthropic.com`) |
| `ANTHROPIC_API_KEY` | API key sent as `X-Api-Key` header |
| `ANTHROPIC_AUTH_TOKEN` | Alternative — sent as `Authorization: Bearer <value>` |
| `ANTHROPIC_MODEL` | Model name override |
| `ANTHROPIC_DEFAULT_SONNET_MODEL` | Override which model is used for the "Sonnet" tier |
| `ANTHROPIC_DEFAULT_OPUS_MODEL` | Override which model is used for the "Opus" tier |
| `ANTHROPIC_DEFAULT_HAIKU_MODEL` | Override which model is used for the "Haiku" tier |
| `ANTHROPIC_CUSTOM_HEADERS` | Extra headers (newline-separated `Name: Value`) |
| `CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC` | Set to `1` to prevent telemetry/update checks |

**API key required:** Yes — at least a non-empty dummy string for `ANTHROPIC_API_KEY`.

**Model name:** Must be a valid model name recognized by the proxy. Use dated names like `claude-sonnet-4-20250514` or whatever your proxy maps.

**Important:** Claude Code speaks Anthropic's native API format (`/v1/messages`), NOT OpenAI's `/v1/chat/completions`. To use an OpenAI-compatible endpoint, you need a translation proxy (like the `databricks-agent-proxy` in this repo, or CCR/CLIProxyAPI). If your endpoint already speaks Anthropic format, you can point directly at it.

### Example

```bash
ANTHROPIC_BASE_URL="http://localhost:8080" \
ANTHROPIC_API_KEY="dummy-key" \
ANTHROPIC_MODEL="claude-sonnet-4-20250514" \
CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1 \
claude
```

**Docs:** https://code.claude.com/docs/en/env-vars

---

## 2. Gemini CLI (Google)

**Config mechanism:** Environment variables

| Variable | Purpose |
|---|---|
| `GOOGLE_GEMINI_BASE_URL` | Custom API endpoint URL |
| `GEMINI_API_KEY` | API key for authentication (bypasses Google OAuth login) |
| `GEMINI_MODEL` | Default model selection |

**API key required:** Yes — provide your proxy's API key (or a dummy if auth is disabled).

**Model name:** Use the model name your endpoint expects (e.g., `gemini-2.5-pro`).

**Important:** Gemini CLI speaks Google's Gemini API format natively, NOT OpenAI's format. Similar to Claude Code, pointing it at a raw OpenAI-compatible endpoint requires a translation layer. If your endpoint speaks Gemini's native format, you can point directly.

### Example

```bash
export GOOGLE_GEMINI_BASE_URL="http://localhost:8080"
export GEMINI_API_KEY="dummy-key"
gemini
```

**Docs / Source:** https://github.com/google-gemini/gemini-cli/issues/1679 (confirmed by collaborator: `GOOGLE_GEMINI_BASE_URL` is the env var)

---

## 3. OpenCode

**Config mechanism:** JSON config file + environment variables

**Config file location:** `~/.config/opencode/opencode.jsonc` (or `opencode.json` in project root)

| Field | Purpose |
|---|---|
| `provider.<name>.npm` | AI SDK package — use `@ai-sdk/openai-compatible` for `/v1/chat/completions` |
| `provider.<name>.options.baseURL` | API endpoint URL |
| `provider.<name>.options.apiKey` | API key (can also use env var) |
| `provider.<name>.options.headers` | Custom headers object |
| `provider.<name>.models.<id>.name` | Display name for each model |
| `provider.<name>.name` | Display name for the provider in UI |

**Environment variable shortcut:** `LOCAL_ENDPOINT` — sets a self-hosted OpenAI-compatible provider endpoint.

**API key required:** Depends on your endpoint. Can be set in config or via env var.

**Model name:** Any string your endpoint accepts.

### Example

```jsonc
// ~/.config/opencode/opencode.jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "provider": {
    "my-proxy": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "My Local Proxy",
      "options": {
        "baseURL": "http://localhost:8080/v1"
      },
      "models": {
        "my-model": {
          "name": "My Model"
        }
      }
    }
  }
}
```

Or simply:
```bash
LOCAL_ENDPOINT="http://localhost:8080" opencode
```

**Docs:** https://opencode.ai/docs/providers/

---

## 4. Codex (OpenAI CLI)

**Config mechanism:** TOML config file + environment variables

**Config file location:** `~/.codex/config.toml` (user-level) or `.codex/config.toml` (project-level)

| Field / Variable | Purpose |
|---|---|
| `model` | Default model name |
| `model_provider` | Which provider block to use |
| `[model_providers.<name>]` | Provider definition section |
| `.name` | Display name |
| `.base_url` | API endpoint (must include `/v1`) |
| `.env_key` | Name of env var holding the API key |
| `.wire_api` | `"chat"` for `/v1/chat/completions`, `"responses"` for `/v1/responses` |
| `.http_headers` | Static headers |
| `.env_http_headers` | Headers from env vars |
| `OPENAI_BASE_URL` | Env var shortcut — overrides the default OpenAI endpoint without config changes |
| `OPENAI_API_KEY` | Default API key env var |

**API key required:** Yes — set via the env var named in `env_key`.

**Model name:** Whatever your endpoint serves.

**Shortcut:** For simple proxy use, just `export OPENAI_BASE_URL` and skip config.toml entirely.

### Example (config.toml)

```toml
# ~/.codex/config.toml
model = "my-model"
model_provider = "myproxy"

[model_providers.myproxy]
name = "My Local Proxy"
base_url = "http://localhost:8080/v1"
env_key = "MY_PROXY_API_KEY"
wire_api = "chat"
```

```bash
export MY_PROXY_API_KEY="dummy-key"
codex "hello"
```

### Example (env var shortcut)

```bash
export OPENAI_BASE_URL="http://localhost:8080/v1"
export OPENAI_API_KEY="dummy-key"
codex "hello"
```

**Docs:** https://developers.openai.com/codex/config-advanced/

---

## Summary Comparison

| Tool | Native API Format | Config Type | Base URL Setting | API Key Setting |
|---|---|---|---|---|
| Claude Code | Anthropic `/v1/messages` | Env vars | `ANTHROPIC_BASE_URL` | `ANTHROPIC_API_KEY` |
| Gemini CLI | Google Gemini | Env vars | `GOOGLE_GEMINI_BASE_URL` | `GEMINI_API_KEY` |
| OpenCode | OpenAI-compatible | JSON config | `provider.<name>.options.baseURL` | `provider.<name>.options.apiKey` or env |
| Codex | OpenAI | TOML config or env | `base_url` in config or `OPENAI_BASE_URL` env | `env_key` in config or `OPENAI_API_KEY` env |

**Key distinction:** Claude Code and Gemini CLI speak their respective native formats, not OpenAI's chat completions format. To use them with a pure OpenAI-compatible endpoint, you need a format translation proxy. OpenCode and Codex natively speak OpenAI-compatible format and can connect directly to any `/v1/chat/completions` endpoint.
