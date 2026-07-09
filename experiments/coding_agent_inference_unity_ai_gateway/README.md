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

Tested 2026-07-09 against one real AWS workspace **without** the gated
"Foundation Model Permissions" Preview enrolled. ❓ = not yet resolved
empirically — each row's script prints exactly what it needs to run fully
(most ❓ rows need a second test principal and an explicit mutation opt-in).
Machine-readable results live in [`results/matrix_results.json`](./results/matrix_results.json),
written by the scripts in [`scripts/governance/`](./scripts/governance/).

| # | Capability | Claim / Source | OOTB (GA) | Gated Preview | Notes |
|---|---|---|---|---|---|
| 1 | Query model via gateway with SSO/OAuth (no PAT) | Core goal | ✅ | — | HTTP 200 with a short-lived OAuth token on `/ai-gateway/mlflow/v1/chat/completions` |
| 2 | Three-part UC identifier (`system.ai.<model>`) resolves | [Unity AI Gateway](https://docs.databricks.com/aws/en/ai-gateway) | ✅ | — | **Gateway routes only** — see Key Findings; `/serving-endpoints/*` 404s on three-part ids |
| 3 | GRANT/REVOKE EXECUTE on model service enforced | [Govern model services](https://docs.databricks.com/aws/en/ai-gateway/govern-model-services) | ❓ | ❓ | Suspected gated behind the "Foundation Model Permissions" account-console Preview; needs `TEST_PRINCIPAL` + mutation opt-in |
| 4 | DENY EXECUTE on a model actually blocks inference | [Govern model services](https://docs.databricks.com/aws/en/ai-gateway/govern-model-services) | ❓ | ❓ | Needs a second principal profile to query as the denied user |
| 5 | DENY on model + GRANT on gateway → user can still infer | Definer's-rights invocation per docs | ❓ | ❓ | Expected "yes" (owner needs EXECUTE, caller may not) — confirm empirically |
| 6 | Revoke gateway access → no fallback to old endpoints | Field sandbox observation | ❓ | ❓ | Script reproduces the reported silent-fallback leak once a second principal is configured |
| 7 | Per-user alerting on spend threshold | [Budgets](https://docs.databricks.com/aws/en/admin/account-settings/budgets) | ✅ | — | Usage tracking ON; note there is no "alert at $X per user" field on the endpoint — alerting is composed from usage system tables |
| 8 | Per-user hard cap (block at budget) | Conference-talk claim | ❌ | ❓ | Only alert-style surfaces and per-user *rate* limits exist; no spend-denominated block-at-budget knob found on the endpoint |
| 9 | Usage tracking per user / per agent (`user_agent`) | [Usage system tables](https://docs.databricks.com/aws/en/ai-gateway/configure-ai-gateway-endpoints#usage-tracking) | ❓ | — | Per-**user** attribution proven (`system.serving.endpoint_usage.requester` populated); per-**agent** columns (`usage_context`, `client_request_id`) exist but were empty in all sampled rows; marker probe pending ingestion lag |
| 10 | Guardrails (PII / injection / unsafe) on service | [Configure AI Gateway](https://docs.databricks.com/aws/en/ai-gateway/configure-ai-gateway-endpoints) | ❓ | ❓ | No guardrails configured on the probed endpoint; documented knobs are `pii`, `safety`, `valid_topics`, `invalid_keywords` — no dedicated prompt-injection filter in the schema |

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

### Per-agent usage attribution requires caller cooperation (and patience)

`system.serving.endpoint_usage` proves per-user attribution out of the box
(`requester` is populated). The per-agent columns (`usage_context`,
`client_request_id`) were null/empty in every sampled row — they populate only
when callers send the metadata, and system-table ingestion lag exceeds a
single script run. Script 09 sends a distinctive marker in both `User-Agent`
and `usage_context`; re-run it later to check whether the marker landed.

### The Codex Responses route exists but rejected a Claude model

`POST /ai-gateway/codex/v1/responses` is live, but returned
`400 — Responses API passthrough is not supported` for the Claude model
tested. Codex CLI works today via the OpenAI-compatible
`/ai-gateway/mlflow/v1` base with `wire_api = "chat"`.

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) v0.218+ (OAuth-capable), authenticated with `databricks auth login --host https://<your-workspace>` — **not** a PAT
- [uv](https://docs.astral.sh/uv/) (Python package manager)
- A workspace where Unity AI Gateway model services are provisioned (run `make verify` to find out — that's the point)
- Optional, for the permission-enforcement rows (3–6): a second test principal (`TEST_PRINCIPAL`), a CLI profile authenticated as that principal (`TEST_PRINCIPAL_PROFILE`), and admin rights to grant/deny
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
double-gated — they dry-run with instructions until you opt in:

```bash
export TEST_PRINCIPAL="test-user@your-org.com"
export TEST_PRINCIPAL_PROFILE="test-user-profile"   # profile authed AS that user
export ALLOW_ENDPOINT_MUTATION=1                    # explicit mutation opt-in
uv run python scripts/governance/03_grant_revoke_execute_enforced.py
uv run python scripts/governance/04_deny_execute_blocks_inference.py
uv run python scripts/governance/05_deny_model_grant_gateway.py
uv run python scripts/governance/06_revoke_gateway_no_fallback.py
uv run python scripts/governance/10_guardrails_pii_injection_unsafe.py
```

Run rows twice — once without and once with the gated **Foundation Model
Permissions** Preview enrolled (account console → Previews) — to fill both
matrix columns. Every script is idempotent and restores permissions in a
`finally` block.

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
