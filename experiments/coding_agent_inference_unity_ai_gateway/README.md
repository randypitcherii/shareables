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
gated below is documented as such, never demoed as if it works.

The gateway is moving fast enough that a matrix has a shelf life. This one has
been re-validated once (July → August 2026) and four rows moved — two of them
(9 and 10) because the *original probe was wrong*, not because the product
changed. Re-run it against your own workspace before quoting it.

## Results Matrix

**Re-validated 2026-08-03** against the same AWS workspace, after a wave of
Databricks releases (foundation-model UC permissions relabelled
"GA but requires enablement", budgets GA, model/MCP services and service
policies in Beta, the new `system.ai_gateway.usage` table). Rows 3, 8, 9 and
10 moved. The original run was 2026-07-09 / 2026-07-10; both verdicts are
shown so you can see which way the product is travelling.

Machine-readable results live in [`results/matrix_results.json`](./results/matrix_results.json),
written by the scripts in [`scripts/governance/`](./scripts/governance/). A
remaining ❓ means the capability could not be isolated on this workspace, not
that the script didn't run — run
[`00_enablement_probe.py`](./scripts/governance/00_enablement_probe.py) first
to see which gates are open on yours.

Rows 1, 2, 7, 8, 9 and 10 were re-executed end-to-end in August and carry
August timestamps in the results file. Rows 3–6 need a second test principal
and were **not** re-executed; instead their blockers were re-verified
directly — `DENY` still returns `UC_COMMAND_NOT_SUPPORTED`, and the
enablement probe confirms the `MODEL` securable is still off — so their July
records stand.

| # | Capability | Claim / Source | Jul 2026 | **Aug 2026** | Notes |
|---|---|---|---|---|---|
| 1 | Query model via gateway with SSO/OAuth (no PAT) | Core goal | ✅ | **✅** | Unchanged. HTTP 200 with a short-lived OAuth token on `/ai-gateway/mlflow/v1/chat/completions` |
| 2 | Three-part UC identifier (`system.ai.<model>`) resolves | [Unity AI Gateway](https://docs.databricks.com/aws/en/ai-gateway) | ✅ | **✅** | Unchanged. **Gateway routes only** — `/serving-endpoints/*` still 404s on three-part ids |
| 3 | GRANT/REVOKE EXECUTE on model service enforced | [Govern model services](https://docs.databricks.com/aws/en/ai-gateway/govern-model-services) | ❌ | **❓** | Still not enforceable here, but the *reason* is now a named gate rather than a missing feature: foundation-model UC permissions answer `INVALID_STATE: MODEL is not enabled`, and `system.ai` still carries a schema-wide `EXECUTE` to `account users`. Enabling it is an account-team request |
| 4 | DENY EXECUTE on a model actually blocks inference | [Govern model services](https://docs.databricks.com/aws/en/ai-gateway/govern-model-services) | ❌ | **❌** | Unity Catalog still has no `DENY` (`UC_COMMAND_NOT_SUPPORTED`; grants are additive-only). ABAC arrived but adds **GRANT** policies only. The one ALLOW/DENY/ASK surface — service policies on model services — is Beta and not enabled here (`MODEL SERVICE` is not a recognised securable) |
| 5 | DENY on model + GRANT on gateway → user can still infer | Definer's-rights invocation per docs | ❓ | **❓** | Same blocker as row 4 plus no per-principal ACL on pay-per-token FM endpoints. Needs model services (Beta) or a custom / provisioned-throughput endpoint |
| 6 | Revoke gateway access → no fallback to old endpoints | Field sandbox observation | ❓ | **❓** | Unchanged. Pay-per-token FM endpoints still expose no per-principal permission to revoke |
| 7 | Per-user alerting on spend threshold | [Budgets](https://docs.databricks.com/aws/en/admin/account-settings/budgets) | ✅ | **✅** | Unchanged. Usage tracking ON; alerting is composed from the usage system tables, not set as a field on the endpoint |
| 8 | Per-user hard cap (block at budget) | [AI Gateway budgets](https://docs.databricks.com/aws/en/ai-gateway/budgets) | ❌ | **❓ (was ❌)** | **The blocking half is now real.** Account budgets carry a genuine `BLOCK_USAGE` action alongside `EMAIL_NOTIFICATION`, spend-denominated (`LIST_PRICE_DOLLARS_USD`, `CUMULATIVE_SPENDING_EXCEEDED`, monthly) — 47 of 440 budgets on a probed account use it. The **per-user** half stays unproven: every budget this API version returns is scoped by `workspace_id`/tags, with no per-user threshold or override field |
| 9 | Usage tracking per user / per agent | [Usage tracking](https://docs.databricks.com/aws/en/ai-gateway/usage-tracking-beta) | ◑ | **✅ (was ◑)** | **Solved by a new table.** `system.ai_gateway.usage` populates `requester` (27.0M rows), `user_agent` (2.67M) and `request_tags` (284K), fed by the `Databricks-Ai-Gateway-Request-Tags` header. Our own marker landed on both `/ai-gateway/mlflow/v1/chat/completions` and `/ai-gateway/anthropic/v1/messages`. 170,924 tagged rows exist on the gateway chat-completions route — the exact path July found empty |
| 10 | Guardrails (PII / injection / unsafe) on service | [Guardrails](https://docs.databricks.com/aws/en/ai-gateway/guardrails) | ❌ | **◑ (was ❌)** | **They enforce now — but not on the routes this experiment recommends.** With `input.pii=BLOCK` + `input.safety` set, synthetic PII is rejected `HTTP 400` on `/serving-endpoints/chat/completions` (`input_guardrail` flagged, categories `['privacy']`) and sails through `HTTP 200` on `/ai-gateway/mlflow/v1/chat/completions`. Benign controls return 200 on both, so the block is guardrail-specific. See Key Findings — this is the headline change |

The July ❌ on row 10 was **partly our own bug**: the probe called only the
gateway route, so it never saw the enforcement that was happening one route
family over. [`10_guardrails_pii_injection_unsafe.py`](./scripts/governance/10_guardrails_pii_injection_unsafe.py)
now probes both families with a benign control, which is how the split
surfaced. Row 9's July ❌ came from sampling ten rows instead of counting the
whole table; that script now counts table-wide.

## Key Findings

### The route split now cuts both ways: `/ai-gateway/*` resolves three-part ids AND skips endpoint guardrails

This is the single most important thing in the experiment, and the August
re-validation is what exposed the second half of it.

Guardrails are configured on a *serving endpoint*
(`PUT /api/2.0/serving-endpoints/<name>/ai-gateway`). They are enforced when
you call that endpoint. The Unity AI Gateway routes resolve the model in the
Unity Catalog model catalog instead, so they never consult that endpoint's
config. Same workspace, same model, same minute, one guardrail config:

| Route | benign prompt | synthetic PII |
|---|---|---|
| `POST /serving-endpoints/chat/completions` | 200 | **400** — `input_guardrail` flagged, categories `['privacy']` |
| `POST /ai-gateway/mlflow/v1/chat/completions` | 200 | **200** — values echoed back |

Reproduced across four guardrail configurations (`pii` alone, `safety`
alone, both, both plus output) and confirmed by a no-guardrail control run
where every cell returned 200.

**The practical consequence:** every config template in this repo points at
`/ai-gateway/*`, because that is the only route family that resolves
three-part ids. So a fleet configured this way gets three-part identifiers
and gateway usage tracking, and gets **no PII or safety guardrail at all**
from the endpoint config an admin is most likely to have set. If you need
guardrails on the gateway path today, they come from **service policies** on
model services — Beta, and not enabled on this workspace (see the enablement
probe). Do not tell a customer that setting `input.pii = BLOCK` on the
foundation-model endpoint protects their coding agents. It does not.

Two smaller notes from the same probe:

- `pii.behavior = BLOCK` fires on a strong signal (a card number), not on an
  SSN alone. `safety = true` flags both under a `privacy` category, so the
  two knobs overlap more than the names suggest.
- The endpoint API **silently drops guardrail keys it does not understand.**
  `PUT`ting `jailbreak`, `hallucination`, `custom`, or `pii.behavior =
  SANITIZE` all return `HTTP 200` and come back with those keys gone and the
  remaining ones reset to `false`/`NONE`. The richer guardrail types in the
  current docs are not on this API surface, and an operator who sets them
  will believe they are protected. Always read back the response body.

### Per-agent attribution is solved — by a different table and a different header

July's finding ("the mechanism exists but the gateway route doesn't feed
it") is obsolete. There are now two attribution surfaces, and they line up
exactly with the two route families:

| Route family | Mechanism | Lands in | Verified |
|---|---|---|---|
| `/serving-endpoints/*` | `usage_context` map in the **request body** | `system.serving.endpoint_usage.usage_context` | ✅ on `/chat/completions` and `/<name>/invocations` |
| `/ai-gateway/*` | `Databricks-Ai-Gateway-Request-Tags` **header** (JSON string→string) | `system.ai_gateway.usage.request_tags` | ✅ on `/mlflow/v1/chat/completions` and `/anthropic/v1/messages` |

Crossing them fails silently: a `usage_context` body field sent to a gateway
route never lands, and there is no `X-Databricks-Usage-Context` header —
both were probed and both produced zero rows.

`system.ai_gateway.usage` also records `user_agent`, `url` and `api_type`
with no caller cooperation at all, so per-agent attribution works even for
clients that send no tags: real Claude Code traffic in this account shows up
as `user_agent = claude-code/1.0.44` on `/ai-gateway/mlflow/v1/chat/completions`.
The [`configs/`](./configs/) templates now set the request-tags header for
Claude Code, Codex and OpenCode.

Budget your validation time accordingly: **ingestion lag on both tables ran
15–25 minutes**, so a script that sends a marker and immediately queries for
it will always come up empty. That is lag, not absence — the row-09 script
now says so explicitly and prints the observed lag.

### Hard spend caps exist; the per-user dimension is the unproven half

`BLOCK_USAGE` is a real `action_type` in the account budgets API, sitting
next to `EMAIL_NOTIFICATION` on an alert configuration with a
`LIST_PRICE_DOLLARS_USD` threshold and a `CUMULATIVE_SPENDING_EXCEEDED`
trigger. On a probed account, 47 of 440 budgets use it. So "block at budget"
is no longer a conference-talk claim — it ships.

What is still unproven is **per-user**. Every budget this API version returns
is filtered by `workspace_id` and/or tags; there is no per-user threshold or
per-user override field in the response, so a triggered cap blocks the whole
filtered scope rather than the one engineer who overspent. Budgets that
their owners *named* for per-user AI Gateway demos come back with plain
workspace filters. The docs describe per-user thresholds and overrides;
confirm them in the account console before promising them to anyone.

Note also that this surface is **account-scoped**, not a field on the serving
endpoint — a workspace token cannot see or set it. Point
`DATABRICKS_ACCOUNT_PROFILE` at an account-console profile to probe it.

### The three-part id / route story is unchanged

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

### Per-model access control: the blocker moved from "absent" to "not enabled"

In July the honest summary was "the governance surface accepts configuration
but does not enforce it". In August it is more precise, and more actionable:
**the enforcement mechanisms now exist, and each one is behind its own gate
that this workspace does not have open.** Run
[`00_enablement_probe.py`](./scripts/governance/00_enablement_probe.py) to see
which apply to yours; here is what it reports on this one:

| Mechanism | State here | What it would settle |
|---|---|---|
| Foundation-model UC permissions (`MODEL` securable) | ❌ `INVALID_STATE: MODEL is not enabled` | Rows 3–6. Docs call it "GA but requires enablement" — an account-team request, not a self-serve toggle |
| `system.ai` schema-wide `EXECUTE` to `account users` | ✅ still granted | Why row 3 cannot be isolated: while a broad group holds `EXECUTE`, a per-model GRANT is redundant and REVOKE of it changes nothing |
| ABAC policy engine | ✅ available, 0 policies on `system.ai` | Adds **GRANT** policies (`GRANT EXECUTE FOR MODELS`). There is no DENY form, so row 4 is untouched |
| Model services + service policies (Beta) | ❌ `MODEL SERVICE` not a recognised securable | Rows 4–6. Service policies are the only ALLOW/**DENY**/ASK surface for model access, and the only guardrail path for `/ai-gateway/*` |
| Model provider services header | ❌ 404 on `system.ai.anthropic` | The documented `Databricks-Model-Provider-Service` on-ramp for Claude Code |

Two things did **not** change and are worth stating plainly, because they are
still commonly asserted otherwise:

- **Unity Catalog still has no `DENY`.** `UC_COMMAND_NOT_SUPPORTED`; grants
  remain additive-only. ABAC did not add one — its `EXCEPT principal` clause
  excludes a principal from a grant, which is not a deny assignment. "DENY
  EXECUTE blocks inference" is not achievable as documented.
- **Built-in pay-per-token FM endpoints have no per-principal ACL.** They
  expose no endpoint `id` and the permissions API rejects their name
  (`not a valid Inference Endpoint ID`), so endpoint-level `CAN_QUERY` grants
  and the "revoke → silent fallback" repro still need a custom or
  provisioned-throughput endpoint.

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

Start by finding out which governance features are switched on where you are
— most ❓ cells are an enablement gate, not a missing capability:

```bash
uv run python scripts/governance/00_enablement_probe.py   # read-only, no gate
```

One real request proving SSO auth + identifier resolution end-to-end:

```bash
make verify          # tries /ai-gateway first, prints every route's response
```

Fill the matrix (each script records its row into `results/matrix_results.json`):

```bash
uv run python scripts/governance/01_query_via_gateway_sso.py
uv run python scripts/governance/02_three_part_identifier_resolves.py
uv run python scripts/governance/07_per_user_spend_alerting.py
uv run python scripts/governance/09_usage_tracking_per_user_agent.py

# Row 8's block-at-budget surface is ACCOUNT-scoped, so it needs an
# account-console profile in addition to your workspace profile:
#   databricks auth login --host https://accounts.cloud.databricks.com \
#     --account-id <account-id> --profile <name>
DATABRICKS_ACCOUNT_PROFILE=<name> \
  uv run python scripts/governance/08_per_user_hard_cap.py
```

Two rows depend on ingestion into system tables, which ran **15–25 minutes
behind** during this validation. Row 9 reports the observed lag and treats a
missing marker as lag rather than absence; if you want to see your own marker
land, re-run it after twenty minutes.

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
in a `finally` block. Row 10 mutates the shared foundation-model endpoint's
guardrail config for roughly a minute; it restores the original config even
on failure, but do not run it against a workspace where a brief PII block
would disrupt someone.

Rows 3–6 remain blocked on enablement rather than on setup, and the specific
blockers are listed under Key Findings. To make them meaningful you need
foundation-model UC permissions enabled (account-team request), and then to
revoke the schema-wide `EXECUTE` on `system.ai` from `account users` — which
affects **every user of that workspace**, so do it somewhere you own. Rows 5
and 6 additionally need a custom or provisioned-throughput endpoint, since
built-in pay-per-token endpoints have no per-principal ACL surface.

## Configuring your coding agents

The documented happy path is `ucode <agent>` (covers Claude Code, Codex,
OpenCode). The escape-hatch templates — required for Claude Desktop, useful
everywhere ucode is unavailable — live in [`configs/`](./configs/README.md),
all pointing at the tested `/ai-gateway/*` routes with three-part ids and the
SSO token helper. A self-service agent skill that drives the whole setup
(preflight → ucode-or-template → verify → locked-down-registry handling) is
bundled in [`skill/`](./skill/SKILL.md).

Each template now also sets `Databricks-Ai-Gateway-Request-Tags`, so an
agent's traffic is attributable in `system.ai_gateway.usage.request_tags`.
Edit the team/project values; the keys are yours to choose.

**Know what you are trading.** Pointing an agent at `/ai-gateway/*` is what
makes three-part identifiers and gateway usage tracking work, and it is also
what puts the agent outside any PII/safety guardrail configured on the
serving endpoint (see Key Findings). Until service policies are available on
your workspace, treat "governed inference" here as *identity, attribution and
spend* — not content filtering.

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
│   └── governance/               # 00_enablement_probe.py (which gates are open here)
│                                 # + one script per matrix row (01–10) + _gov_common.py
├── skill/                        # bundled agent skill (SKILL.md + setup_helper.sh)
└── results/
    └── matrix_results.json       # machine-readable matrix, written by the scripts
```
