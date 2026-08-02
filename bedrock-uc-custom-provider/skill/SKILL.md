---
name: bedrock-uc-ai-gateway
description: >-
  Set up and call AWS Bedrock (Claude Sonnet 5) behind the Databricks Unity
  Catalog AI Gateway using a Bedrock long-term API key (ABSK bearer key) — no
  AWS access-key pair, no IAM role. Trigger when a user wants Bedrock models
  governed and logged through Unity Catalog, asks why the native AMAZON_BEDROCK
  provider rejects their Bedrock API key, or hits UnknownOperationException /
  "Nodes do not exist" on a CUSTOM gateway provider pointed at bedrock-runtime.
---

# Bedrock behind the Unity Catalog AI Gateway

You are setting up a **CUSTOM** UC AI Gateway provider pointed at Bedrock's
runtime endpoint, authenticated with a bearer key that lives server-side in
Unity Catalog. Callers authenticate with their normal Databricks identity and
never see the AWS key. All traffic logs to inference tables.

```
caller ──(Databricks OAuth)──► <workspace>/ai-gateway/... ──(ABSK bearer key)──► bedrock-runtime
```

Everything is created by [`../deploy.py`](../deploy.py). **Orchestrate that
script — never reimplement its logic here.** If a parameter needs to change,
edit the parameter block at the top of `deploy.py`; this skill's helper reads
its settings from that same block, so the two cannot drift.

Companion script: [`scripts/gateway.py`](scripts/gateway.py) — subcommands
`preflight`, `status`, `invoke`. Background and the full findings write-up:
[`../README.md`](../README.md).

## Hard rules

- **Never handle the raw `ABSK` key.** Do not accept it in chat, do not write
  it to a file, do not put it in an env var or a command you run. The human
  stores it as a Databricks secret; `deploy.py` reads it server-side.
- **Passthrough is the working path.** Managed serving does not work for Claude
  on a CUSTOM+Bedrock provider (see Constraints). Do not "fix" a failing
  managed call by hunting for another `base_url` — that ground is covered.
- **One provider, one model, one region.** Sonnet 5 (`us.anthropic.claude-sonnet-5`),
  `us-east-1` by default. Resist adding models until the single path works.
- **Beta, undocumented APIs.** `model-provider-services` / `model-services` are
  not GA and not DABs resources. Tell the human this is a demo pattern, not a
  production one, if they ask about durability.

## Step 0 — Preflight

Run everything below from `bedrock-uc-custom-provider/`.

```sh
skill/scripts/gateway.py preflight
```

Checks the Databricks CLI, auth state, `uv`, and whether the Bedrock key is
present as a secret (it lists keys in the scope — it never reads the value).

- **CLI missing** → have the user install the Databricks CLI, then re-run.
- **Not authenticated** → run `databricks auth login --host https://<workspace-host>`
  yourself; the human completes browser SSO. Confirm with `databricks auth describe`.
- **Secret missing** → Step 1.
- **`uv` missing** → either install `uv`, or run
  `pip install 'databricks-sdk>=0.38' && python3 deploy.py`.

## Step 1 — The human stores the Bedrock key (once)

Give them these two commands to run themselves. Do not run the second one for
them, and do not ask them to paste the key to you:

```sh
databricks secrets create-scope bedrock
databricks secrets put-secret bedrock aws_bearer_token_bedrock --string-value 'ABSK...'
```

Scope and key names come from `SECRET_SCOPE` / `SECRET_KEY` in `deploy.py`.
Re-run `preflight` to confirm.

## Step 2 — Deploy

```sh
cd bedrock-uc-custom-provider
uv run deploy.py                      # BEDROCK_REGION=us-west-2 uv run deploy.py to change region
```

Idempotent, create-if-missing: catalog → schema → CUSTOM provider service
(`<catalog>.<schema>.aws_bedrock`, holding the key + `base_url`, with an
inference table) → model service (`<catalog>.<schema>.sonnet5_bedrock`, with its
own inference table) → a live verification call.

Expected output on a first run:

```
catalog ai_gateway_demo: created
schema ai_gateway_demo.bedrock: created
model-provider-service ai_gateway_demo.bedrock.aws_bedrock: created (inference table aws_bedrock_payload in ai_gateway_demo.bedrock)
model-service ai_gateway_demo.bedrock.sonnet5_bedrock: created (inference table sonnet5_bedrock_payload in ai_gateway_demo.bedrock)

new provider service — the gateway routing cache may need ~1–3 min
verification: Sonnet 5 replied through the gateway: 'gateway OK'
```

A re-run prints `exists — leaving as-is` for objects already there. That is
deliberate, not a bug: `config.custom` and `config.routing` are not patchable,
so the script never tries (see Constraints).

**If catalog creation fails** with a managed-location / storage-root error,
this metastore has no default storage, and `deploy.py` deliberately creates
catalogs without a `storage_root`. Do **not** go scavenging the workspace for
an external location to borrow — that silently plants the demo's data in
someone else's bucket. Point `TARGET_CATALOG` at a catalog that already exists
and that the human owns, or have them create one with an explicit storage root.
Then re-run, and pass `--catalog <name>` to `gateway.py` so it follows you.

To change the region, catalog, schema, or model, edit the parameter block at
the top of `deploy.py`. Changing the key, `base_url`, or routing of an
*existing* object requires delete + recreate — see the recreate recipe below.

## Step 3 — Verify

`deploy.py` already made a live call. To make another one at any time:

```sh
skill/scripts/gateway.py invoke "Reply with exactly: gateway OK"
skill/scripts/gateway.py status
```

`invoke` prints the reply on stdout and token usage on stderr; it retries the
routing-cache lag for up to 5 minutes. Verified live 2026-07-24 in `us-east-1`:
Sonnet 5 responds in ~1.6s via `us.anthropic.claude-sonnet-5` (a `global.`
inference profile is also available).

The request it sends — the only invocation shape you should use here:

```
POST <workspace>/ai-gateway/model/us.anthropic.claude-sonnet-5/converse
Authorization: Bearer <databricks OAuth token>
Databricks-Model-Provider-Service: <catalog>.<schema>.aws_bedrock
Content-Type: application/json

{"messages":[{"role":"user","content":[{"text":"..."}]}],"inferenceConfig":{"maxTokens":200}}
```

Two mechanics worth stating to the human: the
`Databricks-Model-Provider-Service` header picks which registered provider
(stored key + base URL) handles the call, and everything after `/ai-gateway/`
path-joins onto that provider's `base_url` with the stored key injected as the
upstream `Authorization` header — which is why the call lands on Bedrock's
native Converse API. That path-join requires `forward_unmanaged_paths: true`.

## Step 4 — Tell the human where the logs land

Say this explicitly; it is the point of putting Bedrock behind the gateway.

- **`<catalog>.<schema>.aws_bedrock_payload`** — the provider service's
  inference table. **This is the one that fills**: passthrough traffic logs
  here, request and response.
- **`<catalog>.<schema>.sonnet5_bedrock_payload`** — the model service's
  inference table. Provisions correctly but **stays empty** until managed
  serving works for Claude on a CUSTOM provider. Do not present it as broken;
  it is a placeholder for a gap that is expected to close.
- **Gateway usage system tables** — passthrough traffic is also counted there.

With defaults, that is `ai_gateway_demo.bedrock.aws_bedrock_payload`. Rows are
not instant; give the tables a few minutes after a call.

## Constraints

- **Managed serving through the CUSTOM provider does not work against Bedrock
  today.** The managed routes (`/ai-gateway/anthropic/v1/messages`,
  `/ai-gateway/openai/v1/chat/completions` with the 3-part model-service name)
  resolve the model service and its destination correctly, but the upstream
  request does not land on a valid Bedrock operation — AWS returns
  `UnknownOperationException` for every `base_url` shape tested (host root,
  `.../openai`, `.../openai/v1`). Anthropic models are also excluded from AWS's
  OpenAI-compat layer (`model_not_found`, retested 2026-07-24). There is no
  Bedrock URL that speaks a gateway-translatable API for Claude.
- **So passthrough is the working, logged path**, and it satisfies "inference
  tables enabled in the target namespace" for the traffic that actually flows.
- **The fully-managed alternative needs different credentials.** If the human
  needs managed Sonnet 5 serving today, the supported path is the native
  `AMAZON_BEDROCK` provider type with an IAM access-key pair. Offer it as a
  trade-off — it abandons this setup's bearer-key-only stance — and let them
  decide. Do not switch silently.

## Gotchas → what to do

| Symptom | Cause | Action |
| --- | --- | --- |
| `Nodes do not exist` | Routing-cache lag on a new/changed service (~1–3 min) | Wait. `deploy.py` and `gateway.py invoke` already retry for 5 min. Do not recreate anything. |
| `UnknownOperationException` / `model_not_found` | You used a managed route | Switch to passthrough (Step 3). Do not try more `base_url` shapes. |
| Create succeeded but invocation fails on AWS auth | **Create does not validate credentials** — a typo'd key creates fine | Fix the secret, then delete + recreate the provider (recipe below). The verification call is the real check. |
| Table `<name>_payload` already exists on create | Deleting a provider/model service **orphans its inference table** | Drop the orphan (or change `table_name_prefix`) before recreating. `gateway.py status` flags orphans. |
| Need to change key, `base_url`, or routing | `config.custom` and `config.routing` are **not updatable** | Delete + recreate (recipe below). Do not attempt a PATCH. |
| Provider create rejects `targets[]` | Entries require `native_api_types` | Keep `"anthropic/v1/messages"` as `deploy.py` sets it. |
| Catalog create fails on managed location / storage root | The metastore has no default storage | Point `TARGET_CATALOG` at an existing catalog the human owns; don't borrow a storage location you found lying around. Match it with `gateway.py --catalog <name>`. |
| An in-workspace coding agent refuses to run `deploy.py` | Agent safety layers read "secret value into an API request body" as credential egress | Expected — see below. Run `deploy.py` from a shell or a notebook the human drives. |

**Recreate recipe** (the only way to change an existing provider's key, base
URL, or routing):

1. `DELETE /api/2.1/unity-catalog/model-provider-services/<catalog>.<schema>.aws_bedrock`
   (or `.../model-services/<...>.sonnet5_bedrock`).
2. Drop the orphaned inference table: `DROP TABLE <catalog>.<schema>.aws_bedrock_payload`.
   Skipping this makes the recreate collide.
3. Re-run `uv run deploy.py`.
4. Expect the routing-cache lag again before the first successful call.

Confirm the delete with the human first — it is destructive, and dropping the
`_payload` table discards logged traffic.

## Where this runs

Run `deploy.py` from a shell (or a notebook the human is driving). **An
in-workspace coding agent will not get through it.** Tested with Genie Code on
2026-07-29: given only the repo URL it cloned the repo, found and read this
skill, planned correctly, and created the catalog and schema — then its safety
layer denied the provider-creation step, on the grounds that reading a secret
and posting it in an API body is credential egress, and suggested a secret
reference instead. There is no secret-reference form in this beta API;
`api_key.plaintext` is the only shape it accepts, so the objection cannot be
satisfied — it can only be overridden by a human who understands the trade.

If you are that agent: say this plainly to your human and hand them the shell
command. Do not try to reshape the request until the guardrail stops noticing.
