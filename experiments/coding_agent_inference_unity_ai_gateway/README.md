# Coding Agent Inference over Unity AI Gateway (SSO)

## Overview

This experiment establishes — with hard testing against a real workspace — which
[Unity AI Gateway](https://docs.databricks.com/aws/en/ai-gateway) governance
capabilities actually work today for coding-agent inference. The scenario: an
organization standardizes on Databricks Model Serving as the inference backend
for all coding assistants (Claude Code, Claude Desktop, Codex, OpenCode), with
engineers onboarding through [ucode](https://github.com/databricks/ucode) and
the gateway. The field keeps hitting contradictions between what was announced
and what is enforceable in a real workspace, so the first deliverable is the
✅/❌ capabilities matrix below — not a demo.

The ground rules ("the One True Way"):

- **SSO/OAuth only, zero PATs** — every token is short-lived and minted by
  `bin/get-databricks-token.sh`; anything starting with `dapi` is refused.
- **Three-part Unity Catalog model identifiers** (`system.ai.<model>`), never
  legacy single-string endpoint names.
- **Prove it with a real HTTP 200** — `make verify` makes one live request.

This is a **client-side config layer** — a companion to a server-side gateway
app, not a replacement. Phase 2 (a polished copy/paste demo) is intentionally
out of scope until the matrix says what deserves demoing: anything ❌ or
Preview-gated below is documented as such, never demoed as if it works.

## Results Matrix

Tested 2026-07-09 / 2026-07-10 against one real AWS workspace **without** the
gated "Foundation Model Permissions" Preview enrolled. The mutation rows
(3–6, 10) were run end-to-end using a purpose-created workspace service
principal (OAuth M2M, no PAT) as the second test identity, then torn down.
Machine-readable results live in [`results/matrix_results.json`](./results/matrix_results.json),
written by the scripts in [`scripts/governance/`](./scripts/governance/). A
remaining ❓ means the capability could not be isolated on a GA workspace, not
that the script didn't run — see the notes.

| # | Capability | Claim / Source | OOTB (GA) | Gated Preview | Notes |
|---|---|---|---|---|---|
| 1 | Query model via gateway with SSO/OAuth (no PAT) | Core goal | ✅ | — | HTTP 200 with a short-lived OAuth token on `/ai-gateway/mlflow/v1/chat/completions` |
| 2 | Three-part UC identifier (`system.ai.<model>`) resolves | [Unity AI Gateway](https://docs.databricks.com/aws/en/ai-gateway) | ✅ | — | **Gateway routes only** — see Key Findings; `/serving-endpoints/*` 404s on three-part ids |
| 3 | GRANT/REVOKE EXECUTE on model service enforced | [Govern model services](https://docs.databricks.com/aws/en/ai-gateway/govern-model-services) | ❌ | ❓ | GRANT/REVOKE EXECUTE **accepted** (securable type `FUNCTION`) but **not enforced**: after REVOKE the principal still inferred (HTTP 200). A default schema-wide `EXECUTE` grant to `account users` on `system.ai` makes direct grants moot OOTB. Preview may change this |
| 4 | DENY EXECUTE on a model actually blocks inference | [Govern model services](https://docs.databricks.com/aws/en/ai-gateway/govern-model-services) | ❌ | — | **Not achievable as documented**: Unity Catalog has no `DENY` command (`UC_COMMAND_NOT_SUPPORTED`; grants are additive-only). You restrict by not-granting / revoking broader grants, not by DENY |
| 5 | DENY on model + GRANT on gateway → user can still infer | Definer's-rights invocation per docs | ❓ | — | Setup impossible on a built-in endpoint: no UC `DENY`, and pay-per-token FM endpoints expose no per-principal endpoint ACL (no endpoint `id`; permissions API rejects the name). Needs a custom / provisioned-throughput endpoint |
| 6 | Revoke gateway access → no fallback to old endpoints | Field sandbox observation | ❓ | — | Not reproducible on a built-in endpoint: pay-per-token FM endpoints have no per-principal permission to revoke. Repro needs a custom / provisioned endpoint (or the original field environment) |
| 7 | Per-user alerting on spend threshold | [Budgets](https://docs.databricks.com/aws/en/admin/account-settings/budgets) | ✅ | — | Usage tracking ON; note there is no "alert at $X per user" field on the endpoint — alerting is composed from usage system tables |
| 8 | Per-user hard cap (block at budget) | Conference-talk claim | ❌ | ❓ | Only alert-style surfaces and per-user *rate* limits exist; no spend-denominated block-at-budget knob found on the endpoint |
| 9 | Usage tracking per user / per agent (`user_agent`) | [Usage system tables](https://docs.databricks.com/aws/en/ai-gateway/configure-ai-gateway-endpoints#usage-tracking) | ◑ | — | Per-**user** attribution ✅ (`system.serving.endpoint_usage.requester` populated). Per-**agent**: the `usage_context` column IS populated on 5,825 historical rows (mechanism works), but **0 in the last 2 days** across 2.6M requests and our gateway chat-completions marker never landed — the OpenAI-compatible route does not propagate `usage_context`/`User-Agent` |
| 10 | Guardrails (PII / injection / unsafe) on service | [Configure AI Gateway](https://docs.databricks.com/aws/en/ai-gateway/configure-ai-gateway-endpoints) | ❌ | — | Guardrail config (`input.pii` behavior=BLOCK, `input.safety`) is **settable** (PUT accepted) but **not enforced**: synthetic PII passed through with HTTP 200 on every probe up to ~90s. No dedicated prompt-injection knob — knobs are `pii`, `safety`, `valid_topics`, `invalid_keywords` |

## Key Findings

### The route split is the headline: three-part ids resolve ONLY on `/ai-gateway/*`

Live-tested with the same OAuth token, same workspace, same minute:

| Route | `system.ai.claude-sonnet-4-6` | legacy `databricks-claude-sonnet-4-6` |
|---|---|---|
| `POST /ai-gateway/mlflow/v1/chat/completions` | **200** | 200 |
| `POST /ai-gateway/anthropic/v1/messages` | **200** | — |
| `POST /serving-endpoints/chat/completions` | 404 `ENDPOINT_NOT_FOUND` | 200 |
| `POST /serving-endpoints/anthropic/v1/messages` | 404 | 200 |
| `POST /serving-endpoints/<model>/invocations` | 404 `ENDPOINT_NOT_FOUND` | 200 |

The classic serving routes resolve the `model` value as an **endpoint name**;
the Unity AI Gateway routes resolve it in the model catalog. The error shapes
differ tellingly: the gateway returns a model-level `NOT_FOUND: '<id>' does
not exist`, the classic routes an endpoint-level `ENDPOINT_NOT_FOUND`. Every
config template in [`configs/`](./configs/) therefore points at `/ai-gateway/*`.

### `system.ai` names drop the `databricks-` prefix

`system.ai.databricks-claude-sonnet-4-6` does **not** exist —
`system.ai.claude-sonnet-4-6` does. The legacy endpoint
`databricks-claude-sonnet-4-6` maps to `system.ai.claude-sonnet-4-6`, so a
mechanical "prepend `system.ai.`" migration produces names that 404. The
`/ai-gateway/codex/v1` route confirmed the mapping from the other side: given
the three-part id, its error message named the legacy endpoint.

### SSO/OAuth works end-to-end with zero PATs

The whole flow — `databricks auth login` → short-lived OAuth token from the
CLI → Bearer header → 200 — works on both route families. No PAT was created
at any point, and everything in this experiment refuses `dapi` tokens on sight.

### Hard per-user budget caps do not exist on the endpoint today

The endpoint's `ai_gateway` block exposes usage tracking and per-user *rate*
limits (requests per time window), but no spend-denominated block-at-budget
field. Per-user spend **alerting** is real but composed: usage tracking feeds
`system.serving.endpoint_usage`, and alerts/budgets are built on top of the
system tables, not set as a single endpoint field.

### Governance enforcement is the real gap: it's advertised but not on OOTB

This is what the experiment was built to settle, and the answer on a GA
workspace (no Foundation Model Permissions Preview) is consistent across three
independent rows: **the governance surface accepts configuration but does not
enforce it.**

- **GRANT/REVOKE EXECUTE (row 3):** grants apply to `system.ai` models as
  securable type `FUNCTION` (not the documented `MODEL` keyword — that's a
  parse error). But `system.ai` ships a schema-wide `EXECUTE` grant to
  `account users`, so a specific GRANT is redundant and a REVOKE of it changes
  nothing — the principal still inferred (HTTP 200) after REVOKE.
- **DENY (row 4):** cannot exist — Unity Catalog has no `DENY` command at all
  (`UC_COMMAND_NOT_SUPPORTED`; grants are additive-only). "DENY EXECUTE blocks
  inference" is not achievable as documented; you restrict access by *not*
  granting and by removing broad grants.
- **Guardrails (row 10):** `input.pii` (BLOCK) and `input.safety` are settable
  via `PUT .../ai-gateway` and the API returns 200, but a request carrying
  synthetic PII still returned HTTP 200 with the values echoed on every probe
  up to ~90 seconds after enablement. Settable ≠ enforcing.

The likely reconciliation: real per-model enforcement is what the account-
console **Foundation Model Permissions** Preview turns on. This profile could
not enable that Preview (it needs account-admin console access), so those
cells stay ❓ in the Gated-Preview column rather than being asserted either way.

### Built-in FM endpoints have no per-principal ACL surface

Rows 5 and 6 need per-principal *endpoint* permissions, but the built-in
pay-per-token foundation-model endpoints (`databricks-claude-*`) expose no
endpoint `id` and the permissions API rejects their name
(`not a valid Inference Endpoint ID`). So endpoint-level CAN_QUERY grants and
the "revoke → silent fallback" repro cannot be exercised against them at all —
they need a custom or provisioned-throughput serving endpoint.

### Per-agent usage attribution: the mechanism exists, the gateway path doesn't feed it

`system.serving.endpoint_usage` proves per-user attribution out of the box
(`requester` is populated). The per-agent `usage_context` column is real and
**has been populated on 5,825 historical rows** — so the mechanism works for
some clients. But across 2.6M requests in the last 2 days it was populated
**zero** times, and a marker sent through the OpenAI-compatible gateway
chat-completions route (in both `User-Agent` and a `usage_context` body field)
never landed. Conclusion: per-agent attribution is supported by the table but
is **not fed by the gateway chat-completions path** used by these coding
agents today.

### The Codex Responses route exists but rejected a Claude model

`POST /ai-gateway/codex/v1/responses` is live, but returned
`400 — Responses API passthrough is not supported` for the Claude model
tested. Codex CLI works today via the OpenAI-compatible
`/ai-gateway/mlflow/v1` base with `wire_api = "chat"`.

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) v0.218+ (OAuth-capable), authenticated with `databricks auth login --host https://<your-workspace>` — **not** a PAT
- [uv](https://docs.astral.sh/uv/) (Python package manager)
- A workspace where Unity AI Gateway model services are provisioned (run `make verify` to find out — that's the point)
- For the permission-enforcement rows (3–6): a second test principal (`TEST_PRINCIPAL`) and a CLI profile authenticated as it (`TEST_PRINCIPAL_PROFILE`), plus admin rights to grant. A workspace **service principal with OAuth M2M** credentials is the self-service way to get this without a second human — see "Running the Tests" below. (`bin/get-databricks-token.sh` mints M2M tokens for `client_id`/`client_secret` profiles via the workspace OIDC endpoint, so it is still zero-PAT.)
- Optional: [ucode](https://github.com/databricks/ucode) — the happy-path setup tool this experiment prefers wherever it covers an agent

## Setup

```bash
cd experiments/coding_agent_inference_unity_ai_gateway
cp .env.example .env   # set DATABRICKS_HOST / DATABRICKS_CONFIG_PROFILE / RPW_MODEL
uv sync
```

If your machine routes package installs through a private registry proxy, set
it generically (`UV_INDEX_URL` / `PIP_INDEX_URL`, `npm config set registry`)
— and do not commit lockfiles that pin a private proxy URL into shared repos.
The bundled skill's `preflight` probes registry reachability for you.

## Running the Tests

One real request proving SSO auth + identifier resolution end-to-end:

```bash
make verify          # tries /ai-gateway first, prints every route's response
```

Fill the matrix (each script records its row into `results/matrix_results.json`):

```bash
uv run python scripts/governance/01_query_via_gateway_sso.py
uv run python scripts/governance/02_three_part_identifier_resolves.py
uv run python scripts/governance/07_per_user_spend_alerting.py
uv run python scripts/governance/08_per_user_hard_cap.py
uv run python scripts/governance/09_usage_tracking_per_user_agent.py
```

The permission-enforcement rows mutate grants/permissions and are therefore
double-gated — they dry-run with instructions until you opt in. The
self-service way to get a second identity (no second human) is a workspace
service principal with OAuth M2M credentials:

```bash
# 1. Create a test service principal and mint OAuth M2M creds (never a PAT):
databricks service-principals create --display-name uag-test-principal
databricks service-principal-secrets-proxy create <sp-numeric-id>   # returns client_id/secret

# 2. Add an M2M profile to a TEMP config file (keep it out of ~/.databrickscfg):
#    [uag-test-sp]
#    host          = https://<your-workspace-host>
#    client_id     = <application-id>
#    client_secret = <secret>
#    auth_type     = oauth-m2m
export DATABRICKS_CONFIG_FILE=/tmp/uag-test.databrickscfg

# 3. Run the rows as admin (DEFAULT) with the SP as the target principal:
export TEST_PRINCIPAL="<sp-application-id>"
export TEST_PRINCIPAL_PROFILE="uag-test-sp"
export ALLOW_ENDPOINT_MUTATION=1
uv run python scripts/governance/03_grant_revoke_execute_enforced.py
uv run python scripts/governance/04_deny_execute_blocks_inference.py
uv run python scripts/governance/05_deny_model_grant_gateway.py
uv run python scripts/governance/06_revoke_gateway_no_fallback.py
uv run python scripts/governance/10_guardrails_pii_injection_unsafe.py

# 4. Tear down: delete the SP secret + SP, remove the temp config file.
databricks service-principals delete <sp-numeric-id>
```

Every script is idempotent and restores grants / endpoint config / guardrails
in a `finally` block. Rows 5 and 6 additionally need a **custom or
provisioned-throughput** serving endpoint (built-in pay-per-token FM endpoints
have no per-principal ACL surface — see Key Findings). Re-run the rows once
more with the gated **Foundation Model Permissions** Preview enrolled
(account console → Previews) to fill the Gated-Preview column, which requires
account-admin access this experiment's workspace profile did not have.

## Configuring your coding agents

The documented happy path is `ucode <agent>` (covers Claude Code, Codex,
OpenCode). The escape-hatch templates — required for Claude Desktop, useful
everywhere ucode is unavailable — live in [`configs/`](./configs/README.md),
all pointing at the tested `/ai-gateway/*` routes with three-part ids and the
SSO token helper. A self-service agent skill that drives the whole setup
(preflight → ucode-or-template → verify → locked-down-registry handling) is
bundled in [`skill/`](./skill/SKILL.md).

### MDM / fleet rollout notes

- Everything here is file-based (settings JSON/TOML + a POSIX/PowerShell/cmd
  token helper), so it distributes cleanly via MDM or dotfile tooling; the
  helper needs no per-user secrets, only a completed `databricks auth login`.
- Prefer helper-based auth (Claude Code's `apiKeyHelper`) over shell-export
  tokens in fleet setups — exported tokens expire after ~1 hour and strand
  long-lived agent sessions (details in [`configs/README.md`](./configs/README.md)).
- The gateway is evolving quickly; pin your rollout to `make verify` passing
  per workspace rather than to any assumption in this README.

## Project Structure

```
coding_agent_inference_unity_ai_gateway/
├── Makefile                      # `make verify` — one real SSO request
├── pyproject.toml                # uv-managed; databricks-sdk + requests
├── .env.example                  # DATABRICKS_HOST / PROFILE / RPW_MODEL
├── bin/
│   ├── get-databricks-token.sh   # SSO OAuth token helper (refuses PATs)
│   ├── get-databricks-token.ps1
│   └── get-databricks-token.cmd
├── configs/                      # per-agent escape-hatch templates + README
│   ├── claude-code.settings.json
│   ├── claude-desktop.md / claude-desktop.mcp.json
│   ├── codex.config.toml
│   └── opencode.json
├── scripts/
│   ├── _common.py                # token/host/model resolution, routed chat_completion, record_result
│   ├── verify.py                 # the `make verify` implementation
│   └── governance/               # one script per matrix row (01–10) + _gov_common.py
├── skill/                        # bundled agent skill (SKILL.md + setup_helper.sh)
└── results/
    └── matrix_results.json       # machine-readable matrix, written by the scripts
```
