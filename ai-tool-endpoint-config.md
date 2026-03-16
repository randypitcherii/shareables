# Configuring Custom API Endpoints Per-Tool (Without Global Env Vars)

## 1. Claude Code (Anthropic CLI)

### Recommended: `settings.json` with `env` key

Claude Code supports an `env` key in its settings files that sets environment variables **only for Claude Code sessions**, not globally.

**Project-level** (shared with team): `.claude/settings.json`
```json
{
  "env": {
    "ANTHROPIC_BASE_URL": "https://your-gateway.example.com"
  }
}
```

**Project-level** (personal, gitignored): `.claude/settings.local.json`
```json
{
  "env": {
    "ANTHROPIC_BASE_URL": "https://your-gateway.example.com",
    "ANTHROPIC_AUTH_TOKEN": "your-gateway-token"
  }
}
```

**User-level** (all projects): `~/.claude/settings.json`
```json
{
  "env": {
    "ANTHROPIC_BASE_URL": "https://your-gateway.example.com"
  }
}
```

### Settings precedence (highest to lowest)
1. Managed settings (server-managed / MDM / `managed-settings.json`)
2. CLI arguments
3. `.claude/settings.local.json` (project, personal)
4. `.claude/settings.json` (project, shared)
5. `~/.claude/settings.json` (user)

### Other relevant env vars (also settable via `env` key)
| Variable | Purpose |
|---|---|
| `ANTHROPIC_BASE_URL` | Override base URL for Anthropic API (gateway/proxy) |
| `ANTHROPIC_AUTH_TOKEN` | Custom `Authorization: Bearer` header value |
| `ANTHROPIC_CUSTOM_HEADERS` | Extra headers (newline-separated `Name: Value`) |
| `ANTHROPIC_BEDROCK_BASE_URL` | Override for Bedrock endpoint |
| `ANTHROPIC_VERTEX_BASE_URL` | Override for Vertex AI endpoint |
| `ANTHROPIC_FOUNDRY_BASE_URL` | Override for Microsoft Foundry endpoint |

### Authentication helpers
For dynamic/rotating keys, use `apiKeyHelper` in settings:
```json
{
  "apiKeyHelper": "~/bin/get-gateway-key.sh"
}
```

### CLI flags
No `--api-base-url` flag exists. The `env` key in settings is the official approach.

### .env file support
Claude Code does **not** load `.env` files from the project directory. Use `settings.json` with the `env` key instead.

### Docs
- https://docs.anthropic.com/en/docs/claude-code/settings
- https://docs.anthropic.com/en/docs/claude-code/llm-gateway
- https://docs.anthropic.com/en/docs/claude-code/env-vars

---

## 2. Codex (OpenAI CLI)

### Recommended: `~/.codex/config.json` with custom provider

Codex uses a `config.json` (or `config.yaml`) file that supports custom provider definitions with a `baseURL` field. This fully replaces the need for `OPENAI_BASE_URL`.

**User-level config**: `~/.codex/config.json`
```json
{
  "model": "o4-mini",
  "provider": "my-gateway",
  "providers": {
    "my-gateway": {
      "name": "My Gateway",
      "baseURL": "https://your-gateway.example.com/v1",
      "envKey": "MY_GATEWAY_API_KEY"
    }
  }
}
```

The `envKey` field tells Codex which environment variable holds the API key for that provider. This means you can use a custom env var name (e.g., `MY_GATEWAY_API_KEY`) instead of `OPENAI_API_KEY`.

### Project-level .env
Codex loads `.env` from the project root via `dotenv/config`. You can put provider-specific env vars there:
```env
MY_GATEWAY_API_KEY=your-key-here
```

### CLI flags
- `--provider <name>` selects which provider from config to use
- `--model/-m <model>` selects the model
- No direct `--base-url` flag; use the config file

### Per-project config
No documented support for a project-level `.codex/config.json`. The config is user-level only at `~/.codex/config.json`. Use the project `.env` for per-project API keys.

### Format options
`~/.codex/config.json` (JSON) or `~/.codex/config.yaml` (YAML)

### Note
The TypeScript CLI docs describe `config.json`. The newer Rust CLI may have different config conventions -- check `codex --help` for the latest.

### Docs
- https://github.com/openai/codex/blob/main/codex-cli/README.md

---

## 3. OpenCode / Crush (charmbracelet/opencode)

> **Note**: OpenCode has been renamed to **Crush** and moved to `charmbracelet/opencode` (binary name: `crush`). The old `opencode-ai/opencode` repo redirects there.

### Recommended: `crush.json` with custom providers

Crush supports custom provider definitions directly in its config file with `base_url` per provider. This fully replaces the need for global `OPENAI_BASE_URL` or `ANTHROPIC_BASE_URL`.

**Project-level** (highest priority): `.crush.json` or `crush.json` in project root
**User-level**: `~/.config/crush/crush.json`

### Config precedence
1. `.crush.json` (project root)
2. `crush.json` (project root)
3. `~/.config/crush/crush.json` (user global)

### Custom OpenAI-compatible provider
```json
{
  "$schema": "https://charm.land/crush.json",
  "providers": {
    "my-gateway": {
      "type": "openai",
      "base_url": "https://your-gateway.example.com/v1",
      "api_key": "$MY_GATEWAY_API_KEY",
      "models": [
        {
          "id": "gpt-4o",
          "name": "GPT-4o via Gateway",
          "context_window": 128000,
          "default_max_tokens": 16384
        }
      ]
    }
  }
}
```

Provider `type` values:
- `"openai"` -- for proxying/routing through actual OpenAI
- `"openai-compat"` -- for non-OpenAI providers with OpenAI-compatible APIs
- `"anthropic"` -- for Anthropic-compatible APIs

### Custom Anthropic-compatible provider
```json
{
  "$schema": "https://charm.land/crush.json",
  "providers": {
    "my-anthropic-gateway": {
      "type": "anthropic",
      "base_url": "https://your-gateway.example.com/v1",
      "api_key": "$MY_ANTHROPIC_KEY",
      "extra_headers": {
        "anthropic-version": "2023-06-01"
      },
      "models": [
        {
          "id": "claude-sonnet-4-20250514",
          "name": "Claude Sonnet 4",
          "cost_per_1m_in": 3,
          "cost_per_1m_out": 15,
          "context_window": 200000,
          "default_max_tokens": 50000,
          "can_reason": true,
          "supports_attachments": true
        }
      ]
    }
  }
}
```

### Environment variables (legacy OpenCode)
The old OpenCode supported `LOCAL_ENDPOINT` for self-hosted models. In Crush, use the custom providers config instead.

### CLI flags
No `--base-url` flag. Configuration is file-based.

### Docs
- https://github.com/charmbracelet/opencode (README)

---

## Summary Comparison

| Feature | Claude Code | Codex | Crush |
|---|---|---|---|
| **Config file** | `.claude/settings.json` | `~/.codex/config.json` | `.crush.json` / `crush.json` |
| **Base URL setting** | `env.ANTHROPIC_BASE_URL` in settings | `providers.<name>.baseURL` | `providers.<name>.base_url` |
| **Project-level** | Yes (`.claude/settings.json` or `.claude/settings.local.json`) | No config, but `.env` for keys | Yes (`.crush.json` in project root) |
| **User-level** | `~/.claude/settings.json` | `~/.codex/config.json` | `~/.config/crush/crush.json` |
| **CLI flag** | None | `--provider <name>` | None |
| **Loads .env** | No | Yes (project root) | No (uses `$VAR` expansion in config) |
| **Custom API key name** | Use `ANTHROPIC_AUTH_TOKEN` | `envKey` per provider | `api_key` per provider (supports `$VAR`) |
