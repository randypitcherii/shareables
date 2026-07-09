# Claude Desktop — Databricks Unity AI Gateway (escape hatch)

**ucode coverage: NOT covered.** `ucode` configures CLI coding agents (Claude
Code, Codex, OpenCode, Gemini CLI, Copilot CLI); it does not configure Claude
Desktop. This document is the primary path for Desktop.

## What Claude Desktop does and does not support

Be aware of the hard limitation up front:

- **Claude Desktop's chat model canNOT be repointed at a custom
  Anthropic-compatible backend.** There is no `ANTHROPIC_BASE_URL`, no
  `apiKeyHelper`, and no custom-model setting in the Desktop app. The chat
  panel always talks to Anthropic's hosted API using your Claude account
  login. This is unlike Claude Code, whose `settings.json` supports both.
  (`# VERIFY:` accurate as of 2026-07; re-check Desktop release notes —
  if Anthropic ships custom-endpoint support, promote a settings-based
  template over this MCP workaround.)
- **What Desktop DOES support is MCP servers** (`claude_desktop_config.json`).
  That is the escape hatch: Databricks model inference is exposed to Desktop
  as a *tool* the chat model can call, not as the chat model itself.

If you need the actual conversation loop to run on Databricks-served models
with SSO, use Claude Code (`configs/claude-code.settings.json`) — Desktop is
the wrong tool for that job.

## Escape hatch: an MCP server that queries Databricks Model Serving

Config file locations:

- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`

Merge the snippet in [`claude-desktop.mcp.json`](./claude-desktop.mcp.json)
into the `mcpServers` block. The pattern:

1. `command` is a shell wrapper, so the short-lived OAuth token is minted by
   `bin/get-databricks-token.sh` **at server launch** — nothing static is
   ever written into the config (zero PATs).
2. The MCP server itself (`<your-mcp-server-command>` placeholder) is whatever
   server you use to reach Databricks — e.g. a server that calls
   `https://<your-workspace-host>/ai-gateway/mlflow/v1` with the token and a
   three-part model id like `system.ai.claude-sonnet-4-6`.
   This experiment does not ship a server; bring your own or use a
   Databricks-managed MCP endpoint.

### Honest caveats

- **Token lifetime:** Desktop launches MCP servers once at app startup. A
  token minted in the wrapper expires after ~1 hour, after which the server's
  Databricks calls 401 until you restart the app. The robust fix is an MCP
  server that invokes the token helper (or the Databricks SDK's OAuth flow)
  *per request* instead of reading a launch-time env var — prefer that if
  your server supports it.
- **No env substitution:** values in the config's `env` block are static
  strings; Desktop does not expand `$(...)` or `{env:...}` there. That is why
  the snippet mints the token inside a `/bin/sh -c` wrapper instead.
- **Remote MCP connectors** (Settings → Connectors in Desktop) support OAuth
  natively; if you host an MCP server behind a Databricks App with OAuth,
  that avoids the launch-time token problem entirely.
  (`# VERIFY:` confirm your Desktop plan exposes custom remote connectors.)

## Placeholders

See [`README.md`](./README.md) for the shared placeholder legend
(`<your-workspace-host>`, `system.ai.<model>`, `<path-to-this-experiment>`).
