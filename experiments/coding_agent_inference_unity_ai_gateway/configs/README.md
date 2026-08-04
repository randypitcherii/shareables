# Per-agent config templates — Coding Agent Inference over Unity AI Gateway

Templates for pointing coding agents at Databricks Model Serving with
**SSO/OAuth only — zero PATs** (never a static `dapi...` token in any config)
and **three-part Unity Catalog model identifiers** (`system.ai.<model>`,
never legacy single-string names).

The documented happy path for most agents is Databricks' setup tool:
`ucode <agent>`. These templates are the **escape hatch** — the reference
config for agents ucode covers, and the primary path for the one it doesn't
(Claude Desktop).

## Index

| Agent | File | ucode coverage | Auth mechanism |
|---|---|---|---|
| Claude Code | [`claude-code.settings.json`](./claude-code.settings.json) | Covered (`ucode claude`) — template is reference/fallback | `apiKeyHelper` runs `bin/get-databricks-token.sh` per refresh; token is never stored |
| Claude Desktop | [`claude-desktop.md`](./claude-desktop.md) + [`claude-desktop.mcp.json`](./claude-desktop.mcp.json) | **NOT covered** — this template is the primary path | MCP-server launch wrapper mints token via helper (chat model itself cannot be repointed; see the doc) |
| Codex CLI | [`codex.config.toml`](./codex.config.toml) | Covered (`ucode codex`) — template is reference/fallback | Shell-level `export OPENAI_API_KEY="$(bin/get-databricks-token.sh)"`; provider `env_key` reads it |
| OpenCode | [`opencode.json`](./opencode.json) | Covered (`ucode opencode`) — template is reference/fallback | Shell-level `export DATABRICKS_TOKEN="$(bin/get-databricks-token.sh)"`; config reads `{env:DATABRICKS_TOKEN}` |

## Placeholder legend (shared across all templates)

| Placeholder | Replace with |
|---|---|
| `<your-workspace-host>` | Your Databricks workspace hostname, e.g. `myshard.cloud.databricks.com` (no trailing slash) |
| `system.ai.<model>` | A three-part Unity Catalog model id, e.g. `system.ai.claude-sonnet-4-6`. Never a legacy single-string name. |
| `<path-to-this-experiment>` | Absolute path to this experiment directory (so `bin/get-databricks-token.sh` resolves) |
| `<your-databricks-cli-profile>` | The `databricks auth login` profile to mint tokens with (helper defaults to `DEFAULT`) |
| `<your-mcp-server-command>` | (Claude Desktop only) the MCP server executable that calls Databricks serving endpoints |
| `<your-team>` / `<your-project>` | Values for the attribution tags described below; keys and values are yours to choose |

## Per-agent usage attribution

Every template sets the `Databricks-Ai-Gateway-Request-Tags` header — a JSON
object of string→string that lands in
`system.ai_gateway.usage.request_tags`, so you can tell one coding agent's
spend from another's:

| Agent | How the header is set |
|---|---|
| Claude Code | `ANTHROPIC_CUSTOM_HEADERS` in the `env` block |
| Codex CLI | `[model_providers.databricks.http_headers]` |
| OpenCode | `provider.databricks.options.headers` |

Live-tested 2026-08-03 on both `/ai-gateway/mlflow/v1/chat/completions` and
`/ai-gateway/anthropic/v1/messages`. Two gotchas:

- This is the **gateway** mechanism. The classic `/serving-endpoints/*`
  routes use a different one — a `usage_context` map in the request *body*,
  landing in `system.serving.endpoint_usage`. Crossing them fails silently:
  neither the header nor the body field works on the other route family.
- Ingestion into these tables ran **15–25 minutes behind** during testing, so
  a tag you just sent will not be queryable immediately.

`system.ai_gateway.usage` also records `user_agent`, `url` and `api_type`
without any client cooperation, so agents you have not templated still show
up — just less legibly.

## Endpoint-level guardrails do not cover these templates

Guardrails (`input.pii`, `input.safety`) configured on a foundation-model
serving endpoint are enforced on the classic `/serving-endpoints/*` routes.
They are not enforced on the `/ai-gateway/*` routes that every template here
points at — live-tested 2026-08-03, synthetic PII returns `400` on the former
and `200` on the latter under one config. Content filtering for the gateway
path comes from service policies on model services (Beta). See the experiment
README's Key Findings for the full probe trail.

## Base URLs

Live-tested 2026-07-09 on a real workspace: **three-part `system.ai.<model>`
ids resolve only on the `/ai-gateway/*` routes.** The classic
`/serving-endpoints/*` routes resolve legacy single-string endpoint names
only (a `system.ai.*` id there returns `404 ENDPOINT_NOT_FOUND`), while the
gateway routes accept both forms. Hence every template points at
`/ai-gateway/*`:

- **Anthropic-compatible route** (Claude Code):
  `https://<your-workspace-host>/ai-gateway/anthropic` (the client appends
  `/v1/messages`). Tested: HTTP 200 with `system.ai.claude-sonnet-4-6`;
  the `/serving-endpoints/anthropic` variant from the "Query with the
  Anthropic Messages API" docs works with legacy names only.
- **OpenAI-compatible route** (Codex, OpenCode):
  `https://<your-workspace-host>/ai-gateway/mlflow/v1` (the client appends
  `/chat/completions`). Tested: HTTP 200 with `system.ai.claude-sonnet-4-6`.
  `# VERIFY:` Databricks' coding-agent integration docs additionally document a
  dedicated Codex route, `https://<your-workspace-host>/ai-gateway/codex/v1`
  with `wire_api = "responses"`. Tested 2026-07-09: the route exists, but
  Responses-API passthrough returned `400 BAD_REQUEST — not supported` for a
  Claude model; it may work for other model families. See `codex.config.toml`.

Run `make verify` at the experiment root to test your own workspace — it
tries the gateway route first and prints every route's response as data.

## Token lifetime: why helper-based auth beats shell exports

`bin/get-databricks-token.sh` prints a **short-lived OAuth access token
(~1 hour)** to stdout (profile from `DATABRICKS_CONFIG_PROFILE`, host from
`DATABRICKS_HOST` or the profile config). Consequences:

- **Shell-level `export VAR="$(bin/get-databricks-token.sh)"`** (Codex,
  OpenCode) captures one token at export time. When it expires mid-session
  you get 401s until you re-run the export and relaunch the agent.
- **Helper-based mechanisms** (Claude Code's `apiKeyHelper`) re-invoke the
  script themselves, so a fresh token is minted automatically. This is why
  `apiKeyHelper` is the preferred pattern wherever an agent supports it —
  and why the Codex/OpenCode templates carry the re-mint caveat inline.
  (Claude Code refreshes the helper value periodically and on auth failure;
  tune with the `CLAUDE_CODE_API_KEY_HELPER_TTL_MS` env var if needed.)

Never work around expiry by pasting a static PAT into a config file.

## Notes on `claude-code.settings.json`

JSON has no comments, so the placeholder explanations live here:

- `env.ANTHROPIC_BASE_URL` — the workspace's Anthropic-compatible route
  (see Base URLs above).
- `env.ANTHROPIC_MODEL` — three-part `system.ai.<model>` id.
- `env.DATABRICKS_CONFIG_PROFILE` — forwarded to the token helper so it mints
  against the right profile; drop it to use the helper's `DEFAULT`.
- `apiKeyHelper` — absolute path to `bin/get-databricks-token.sh`. Claude Code
  sends the helper's output as both `Authorization: Bearer` and `X-Api-Key`
  headers, which the Databricks route accepts as a Bearer token.
- Merge into `~/.claude/settings.json` (global) or `.claude/settings.json`
  (per-project). Note `ucode claude` writes an equivalent config for you.

## Notes on `opencode.json`

Also comment-free JSON:

- Uses the `@ai-sdk/openai-compatible` provider against the OpenAI-compatible
  serving base.
- `{env:DATABRICKS_TOKEN}` is OpenCode's env-substitution syntax — the token
  is resolved from your shell at startup, never stored in the file. Launch:

  ```sh
  export DATABRICKS_TOKEN="$(<path-to-this-experiment>/bin/get-databricks-token.sh)"
  opencode
  ```

- Top-level `model` is `<provider-id>/<model-id>`, hence
  `databricks/system.ai.claude-sonnet-4-6`.
- Place as `opencode.json` in the project root or merge into
  `~/.config/opencode/opencode.json`.
