---
name: unity-ai-gateway-coding-agent-setup
description: >-
  Point a coding agent (Claude Code, Claude Desktop, Codex CLI, or OpenCode)
  at Databricks Model Serving / Unity AI Gateway using SSO/OAuth only. Trigger
  when a user wants their coding agent to run inference through their
  Databricks workspace — zero PATs, three-part system.ai.<model> Unity Catalog
  identifiers, proven with a real HTTP 200.
---

# Unity AI Gateway coding-agent setup

You are driving a customer's setup on their own laptop against their own
Databricks workspace. Hard rules, no exceptions:

- **SSO/OAuth only. Zero PATs.** Never create, request, store, or accept a
  static `dapi...` token. If the user offers one, decline: explain that this
  setup mints short-lived (~1h) OAuth tokens on demand via
  [`../bin/get-databricks-token.sh`](../bin/get-databricks-token.sh), which is
  strictly safer and is the only auth path these configs support (the helper
  itself refuses PATs).
- **Three-part Unity Catalog model ids** (`system.ai.<model>`, e.g.
  `system.ai.claude-sonnet-4-6`) — never legacy single-string
  endpoint names. Whether three-part ids actually resolve is
  **workspace-dependent** — that is exactly what Step 3 verifies. Do not
  assume; do not silently downgrade.

Companion script: [`scripts/setup_helper.sh`](scripts/setup_helper.sh)
(subcommands `preflight`, `materialize`, `verify`).
Templates + placeholder legend: [`../configs/`](../configs/README.md).

## Step 0 — Preflight

Run `scripts/setup_helper.sh preflight`. It prints a status table:
Databricks CLI, OAuth state, `ucode` availability, package-registry
reachability.

- **CLI missing** → have the user install the Databricks CLI, then re-run.
- **Not OAuth-authenticated** → run
  `databricks auth login --host https://<workspace-host>` yourself; the human
  completes browser SSO. Confirm with `databricks auth describe`, then re-run
  preflight.
- **Auth type is `pat`** → refuse to proceed on it. Re-authenticate with
  `databricks auth login` (OAuth) instead.

## Step 1 — Prefer ucode (happy path)

If preflight shows `ucode` available and the target agent is **Claude Code,
Codex, or OpenCode**: run `ucode <agent>` and go straight to Step 3. Do not
hand-edit configs when ucode covers the agent.

Fall back to Step 2 only for:

- **Claude Desktop** — not covered by ucode (see
  [`../configs/claude-desktop.md`](../configs/claude-desktop.md), including
  its hard limitation: Desktop's chat model cannot be repointed; the escape
  hatch is an MCP server).
- **ucode unavailable** on this machine.

## Step 2 — Escape hatch: materialize a template

```sh
scripts/setup_helper.sh materialize <agent> <workspace-host> <model> [--profile <cli-profile>] [--out <path>]
```

`<agent>` ∈ `claude-code | claude-desktop | codex | opencode`. This
substitutes the user's workspace host, CLI profile, a three-part
`system.ai.<model>` id, and the **absolute path** to
`../bin/get-databricks-token.sh` (for `apiKeyHelper`-style mechanisms) into
the matching `../configs/` template. Where to install each result — and which
mechanisms re-mint tokens vs. capture one at export time — is in
[`../configs/README.md`](../configs/README.md).

**Legacy names:** `materialize` refuses non-three-part model names. If (and
only if) Step 3 proves three-part ids do not resolve on this workspace,
re-run with `--allow-legacy --reason "<why>"`; the output is annotated
"three-part ids do not resolve on this workspace yet (see experiment README
row 2)". Say exactly that to the user — record it as a finding, never a
silent downgrade.

## Step 3 — Verify with a real request

Run `scripts/setup_helper.sh verify` (delegates to `make verify` at the
experiment root: one real chat completion over SSO OAuth). Interpret:

- **HTTP 200** → done. Auth and model id proven end-to-end (verify tries the
  `/ai-gateway/mlflow/v1/chat/completions` gateway route first — the only
  route family that resolves three-part ids; live-tested 2026-07-09).
- **404 on the three-part id from every route** (the gateway route reports a
  model-level `NOT_FOUND '<id>' does not exist`, distinct from the classic
  routes' `ENDPOINT_NOT_FOUND`) → first check the registered name: `system.ai`
  names drop the `databricks-` prefix of legacy endpoint names (e.g. legacy
  `databricks-claude-sonnet-4-6` ↔ `system.ai.claude-sonnet-4-6`). If the
  corrected name still 404s, Unity AI Gateway model services are likely not
  provisioned on that workspace. Point the user at the experiment
  [`../README.md`](../README.md) results matrix and the governance checks in
  [`../scripts/governance/`](../scripts/governance/), then decide together
  whether to use `--allow-legacy` (Step 2) with the finding recorded.
- **401/403** → auth triage, in order: token expired (shell-export mechanisms
  capture one token — re-run the export / restart the agent); wrong profile
  or host (`databricks auth describe`, check `DATABRICKS_CONFIG_PROFILE`);
  re-login (`databricks auth login --host <host>`); finally, workspace
  permissions on the serving endpoint.

## Step 4 — Locked-down package registries

Before any `uv sync` / `npm install`, check preflight's registry probes. If
the default registries are blocked (common on corporate networks):

1. Ask the user for their org's internal index URLs — do not guess.
2. Apply generically: `UV_INDEX_URL` / `PIP_INDEX_URL` for Python;
   `npm config set registry <url>` for Node.
3. **Warn: never commit proxy-pinned lockfiles to shared repos** — they break
   for anyone outside that network.
