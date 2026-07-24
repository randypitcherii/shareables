# AWS Bedrock behind the Unity Catalog AI Gateway — one script, one model

One opinionated setup: **Claude Sonnet 5 on AWS Bedrock**, governed through the
**Unity Catalog AI Gateway**, authenticated with a **Bedrock long-term API key**
(the `ABSK...` bearer keys) — no AWS access-key pair required. The AWS
credential lives server-side in Unity Catalog; callers authenticate with their
normal Databricks identity and never see the AWS key. All traffic logs to
inference tables in your target schema.

> ⚠️ This uses the **beta, undocumented** UC `model-provider-services` /
> `model-services` APIs. Shapes and behavior may change without notice. Treat
> this as a demo, not a production pattern. (These objects are also not DABs
> resources yet — official CLI/SDK/TF support is targeted for end of Q2 — hence
> a plain Python script: declarative parameters at the top, idempotent apply.)

## The story

UC AI Gateway has a native Bedrock provider type
(`EXTERNAL_MODEL_PROVIDER_TYPE_AMAZON_BEDROCK`), but it only accepts AWS
access-key pairs (SigV4) and rejects Bedrock API keys. Bedrock's runtime
endpoints, meanwhile, happily authenticate with plain bearer keys. The generic
`EXTERNAL_MODEL_PROVIDER_TYPE_CUSTOM` provider type accepts any bearer key +
base URL — chaining the two gives you Bedrock through the UC gateway with
nothing but an `ABSK` key:

```
caller ──(Databricks OAuth)──► <workspace>/ai-gateway/... ──(ABSK bearer key)──► bedrock-runtime
```

## What `deploy.py` creates

Parameters are at the top of the script; the key comes from the
`AWS_BEARER_TOKEN_BEDROCK` env var (region from `BEDROCK_REGION`, default
`us-east-1`). Everything is create-if-missing:

1. **Catalog** — no `storage_root`, so it uses the metastore's default storage.
2. **Schema** — inside that catalog.
3. **Model provider service** `<catalog>.<schema>.aws_bedrock` — the CUSTOM
   provider holding the bearer key and
   `https://bedrock-runtime.<region>.amazonaws.com` as `base_url`, with
   `allow_all_targets` + `forward_unmanaged_paths`, a Sonnet 5 target entry,
   and an **inference table** (auto-provisioned as `aws_bedrock_payload` in the
   same schema).
4. **Model service** `<catalog>.<schema>.sonnet5_bedrock` — routes 100% of
   traffic to Sonnet 5 on that provider, with its own **inference table**
   (`sonnet5_bedrock_payload`).

It ends with a live verification call — Sonnet 5 through the gateway via
provider passthrough — and prints the reply.

```bash
export AWS_BEARER_TOKEN_BEDROCK='ABSK...'   # never commit this
uv run deploy.py
```

## How to call Sonnet 5 (the working path: provider passthrough)

```
POST <workspace>/ai-gateway/model/us.anthropic.claude-sonnet-5/converse
Authorization: Bearer <databricks OAuth token>
Databricks-Model-Provider-Service: <catalog>.<schema>.aws_bedrock
Content-Type: application/json

{"messages":[{"role":"user","content":[{"text":"..."}]}],"inferenceConfig":{"maxTokens":200}}
```

Two mechanics to know:

- The `Databricks-Model-Provider-Service` header picks which registered
  provider (stored key + base URL) handles the call.
- Everything after `/ai-gateway/` path-joins onto the provider's `base_url`,
  with the stored key injected as the upstream `Authorization` header — so the
  request above lands on Bedrock's native Converse API. This requires
  `forward_unmanaged_paths: true` on the provider.

Verified live through the gateway (2026-07-24, `us-east-1`): Sonnet 5 responds
in ~1.6s via `us.anthropic.claude-sonnet-5` (a `global.` inference profile is
also available). Passthrough traffic is fully logged — it lands in the provider
service's `aws_bedrock_payload` inference table and the gateway usage system
tables.

## Constraints discovered

Stated plainly, because they shape the opinionated stance:

- **Managed serving through the CUSTOM provider doesn't work against Bedrock
  today.** The managed route (`/ai-gateway/anthropic/v1/messages` or
  `/ai-gateway/openai/v1/chat/completions` with the 3-part model-service name)
  resolves the model service and its destination correctly, but the request the
  gateway then sends upstream doesn't land on a valid Bedrock operation — AWS
  returns `UnknownOperationException` for every `base_url` shape tested (host
  root, `.../openai`, `.../openai/v1`). And Anthropic models are excluded from
  AWS's OpenAI-compat layer (`model_not_found`, retested 2026-07-24), so there
  is no Bedrock URL that speaks a gateway-translatable API for Claude.
- **So: passthrough is the working, logged path.** The script still creates the
  `sonnet5_bedrock` model service — the object and its inference table
  provision correctly and will light up when the managed-translation gap
  closes — but day-to-day Sonnet 5 traffic goes through provider passthrough,
  which satisfies "inference tables enabled in the target namespace" for the
  traffic that actually flows.
- **The fully-managed alternative exists and needs different credentials.** If
  you need managed Sonnet 5 serving today, use the native `AMAZON_BEDROCK`
  provider type with an IAM access-key pair. This repo's stance is
  bearer-key-only, so it accepts passthrough.

## Gotchas

- **Deleting a provider/model service orphans its `_payload` inference
  table.** Recreating then collides — drop the table (or change
  `table_name_prefix`) first. This is also why `deploy.py` leaves existing
  objects untouched instead of patching them.
- **`config.custom` and `config.routing` are not updatable.** Changing the key,
  base URL, or routing means delete + recreate (see previous gotcha).
- **New/changed services take ~1–3 min** to reach the gateway's routing cache
  (`Nodes do not exist` in the interim). `deploy.py` retries the verification
  call for up to 5 minutes.
- **Provider `targets[]` entries require `native_api_types`** (here
  `"anthropic/v1/messages"`).
- **Create does not validate credentials.** A typo'd key creates fine and only
  fails at invocation — the verification call at the end of `deploy.py` is the
  real check.
