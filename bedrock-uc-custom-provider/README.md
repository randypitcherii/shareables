# AWS Bedrock as a custom LLM provider in Unity Catalog AI Gateway

Register **AWS Bedrock** behind the **Unity Catalog AI Gateway** using a
**Bedrock long-term API key** (the `ABSK...` bearer keys) — no AWS access-key
pair required. The AWS credential lives server-side in Unity Catalog; callers
authenticate with their normal Databricks identity and never see the AWS key.

> ⚠️ This uses the **beta, undocumented** UC `model-provider-services` API.
> Shapes and behavior may change without notice. Treat this as a demo, not a
> production pattern.

## Why the CUSTOM provider type

UC AI Gateway has a native Bedrock provider type
(`EXTERNAL_MODEL_PROVIDER_TYPE_AMAZON_BEDROCK`), but it **only accepts AWS
access-key pairs** (SigV4) and rejects Bedrock API keys.

Meanwhile, Bedrock's runtime endpoints happily authenticate with plain bearer
keys:

```bash
# Verify your key directly against Bedrock before involving Databricks:
curl "https://bedrock.${AWS_REGION}.amazonaws.com/foundation-models" \
  -H "Authorization: Bearer $AWS_BEARER_TOKEN_BEDROCK"

curl -X POST "https://bedrock-runtime.${AWS_REGION}.amazonaws.com/model/us.anthropic.claude-sonnet-5/converse" \
  -H "Authorization: Bearer $AWS_BEARER_TOKEN_BEDROCK" \
  -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":[{"text":"hi"}]}],"inferenceConfig":{"maxTokens":50}}'
```

The generic `EXTERNAL_MODEL_PROVIDER_TYPE_CUSTOM` provider type accepts **any
bearer key + base URL**. Chaining the two gives you every Bedrock model through
the UC gateway:

```
caller ──(Databricks OAuth)──► <workspace>/ai-gateway/... ──(ABSK bearer key)──► bedrock-runtime
```

## Routing mechanics

Two things select and shape the upstream call:

1. **Provider selection** — the request header
   `Databricks-Model-Provider-Service: <catalog>.<schema>.<provider_id>`
   picks which registered provider (and therefore which stored key + base URL)
   handles the call.
2. **Path joining** — everything after `/ai-gateway/` is appended to the
   provider's `base_url`, with the stored key injected as the upstream
   `Authorization` header:

   ```
   POST <workspace>/ai-gateway/model/<MODEL_ID>/converse
        └────────────────────────┬────────────────────┘
   POST <base_url>/model/<MODEL_ID>/converse
   ```

   **Exception:** `/ai-gateway/v1/...` is the gateway's *managed* API surface
   and does **not** path-join onto `base_url`. To reach Bedrock's Converse API
   you must use the raw `/ai-gateway/<suffix>` form, and the provider must be
   created with `forward_unmanaged_paths: true` — otherwise non-managed paths
   like `/model/.../converse` return 400.

Requests through this provider are **passthrough, not managed**: the gateway
forwards bytes to Bedrock's native Converse API. You get UC-side credential
custody and Databricks-identity access control, but not the managed surface's
schema translation.

## Usage

All scripts are placeholder-driven — override any of these via env vars
(defaults in parentheses):

| Variable | Meaning |
|---|---|
| `AWS_REGION` (`us-east-1`) | Bedrock region for `base_url` |
| `UC_CATALOG` (`main`) / `UC_SCHEMA` (`default`) | Parent schema for the provider object |
| `PROVIDER_ID` (`bedrock_bearer`) | Provider service id — fully qualified name is `<catalog>.<schema>.<id>` |
| `DATABRICKS_PROFILE` (`DEFAULT`) | CLI profile used to resolve host/token when `DATABRICKS_HOST`/`DATABRICKS_TOKEN` aren't exported |
| `AWS_BEARER_TOKEN_BEDROCK` (required for setup) | Your Bedrock long-term API key (`ABSK...`), from IAM → service-specific credentials |

```bash
export AWS_BEARER_TOKEN_BEDROCK='ABSK...'   # never commit this

./setup.sh            # create the provider
./validate-models.sh  # invoke every model in the matrix, print pass/fail
./teardown.sh         # delete the provider
```

The create call `setup.sh` makes (the undocumented bit):

```
POST /api/2.1/unity-catalog/model-provider-services?parent=schemas/<catalog>.<schema>&model_provider_service_id=<id>
{
  "config": {
    "provider_type": "EXTERNAL_MODEL_PROVIDER_TYPE_CUSTOM",
    "allow_all_targets": true,
    "forward_unmanaged_paths": true,
    "custom": {
      "direct": {
        "api_key": {"plaintext": "<ABSK key>"},
        "base_url": "https://bedrock-runtime.<region>.amazonaws.com"
      }
    }
  }
}
```

Teardown is `DELETE /api/2.1/unity-catalog/model-provider-services/<catalog>.<schema>.<id>`.

## Per-model instructions

Invocation template for every model (this is what `validate-models.sh` loops):

```
POST <workspace>/ai-gateway/model/<MODEL_ID>/converse
Authorization: Bearer <databricks OAuth token>
Databricks-Model-Provider-Service: <catalog>.<schema>.<provider_id>
Content-Type: application/json

{"messages":[{"role":"user","content":[{"text":"..."}]}],"inferenceConfig":{"maxTokens":200}}
```

### Verified working (live through the gateway, 2026-07-23, `us-east-1`)

| Model | `MODEL_ID` | Observed latency | Notes |
|---|---|---|---|
| Claude Sonnet 5 | `us.anthropic.claude-sonnet-5` | ~1.6s | `global.` inference profile also available |
| Claude Opus 4.8 | `us.anthropic.claude-opus-4-8` | ~1.2s | `global.` inference profile also available |
| GPT-OSS 120B | `openai.gpt-oss-120b-1:0` | ~0.7s | also reachable via Bedrock's OpenAI-compat layer (see caveats) |
| GLM 5 | `zai.glm-5` | ~0.5s | direct model ID — no inference profile needed |
| Nova 2 Lite | `global.amazon.nova-2-lite-v1:0` | ~0.6s | latest Nova text model; `us.` profile also available |

These latencies and results are from a live verification run on 2026-07-23;
your numbers will vary. `validate-models.sh` re-checks the whole matrix so
drift in Bedrock's catalog is caught on re-run.

### Claude Fable 5 — one-time account opt-in required

`us.anthropic.claude-fable-5` / `global.anthropic.claude-fable-5`
(inference-profile only — there is no direct model ID).

Out of the box, invocation fails with:

```
data retention mode 'default' is not available for this model
```

Mythos-class models (Fable 5) require the AWS **account** to opt into provider
data sharing — prompts/completions are shared with Anthropic and retained up to
30 days for trust & safety
([AWS docs](https://docs.aws.amazon.com/bedrock/latest/userguide/data-retention.html)).
One-time fix by an AWS account admin:

```bash
aws bedrock put-account-data-retention --mode provider_data_share
```

This only affects models that *require* `provider_data_share`; other Bedrock
models are unaffected. It is **not** a request-level setting —
`additionalModelRequestFields` and custom headers were probed and do not work.
`validate-models.sh` includes Fable 5 and reports this error as a known caveat
(`SKIP`) rather than a failure.

Once enabled, Fable 5 supports extended thinking via additional request
fields (from its converse schema):

```json
{"additionalModelRequestFields": {"reasoningConfig": {"enabled": true, "budgetTokens": "low"}}}
```

`budgetTokens` accepts `low` / `medium` / `high` (1,024 / 40,000 / 63,999
tokens). The schema flags `hideSamplingParameter` — **do not send
`temperature` or `topP`** to Fable 5.

### ❌ Not on Bedrock (verified absent in us-east-1, us-west-2, eu-west-1)

- **GPT 5.6 (sol / terra / luna)** — OpenAI's proprietary models are not
  distributed through Bedrock; only the open-weight `gpt-oss` family is.
- **GLM 5.2** — the newest Z.ai model on Bedrock is `zai.glm-5`.

To govern these through the same gateway pattern, register a **second custom
provider** pointed at the provider's own OpenAI-compatible API — identical
recipe, different endpoint and key:

```json
"custom": {
  "direct": {
    "api_key": {"plaintext": "<OpenAI or Z.ai API key>"},
    "base_url": "https://api.openai.com/v1"
  }
}
```

Then call `<workspace>/ai-gateway/chat/completions` (path-joins to
`<base_url>/chat/completions`) with the second provider's
`Databricks-Model-Provider-Service` header. Same pattern for GLM 5.2 with
Z.ai's OpenAI-compatible endpoint.

## Caveats

- **Beta, undocumented API.** `model-provider-services` isn't in the public
  Databricks docs; everything here was discovered empirically and may change.
- **Passthrough, not managed.** No request/response translation, no managed
  `/ai-gateway/v1` surface for these calls — you speak Bedrock's Converse API
  directly.
- **Create does not validate credentials.** A typo'd key creates fine and only
  fails at invocation — always run `validate-models.sh` after setup.
- **`custom.direct` shape is strict.** Exactly `api_key` + `base_url`, and the
  secret must be the wrapped object `{"plaintext": "..."}`, not a bare string.
- **PATCH needs `update_mask`.** Updating a field requires
  `PATCH ...?update_mask=config.<field>` — a bare PATCH is rejected.
- **Bedrock's OpenAI-compat layer is partial.** `https://bedrock-runtime.<region>.amazonaws.com/openai/v1`
  works as a `base_url`, but AWS only exposes some models there (the `gpt-oss`
  family; Anthropic models return `model_not_found`). Point at the runtime root
  and use Converse for full coverage.
