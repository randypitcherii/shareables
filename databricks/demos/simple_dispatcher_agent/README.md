# Simple Model Dispatcher

## Per-request model routing on Unity AI Gateway

Clients send every request to **one** endpoint. A small judge model (**JEV**)
reads each request and picks the model that fits it, and the dispatcher
forwards the request through Unity AI Gateway:

| Route | Model (`system.ai.*`) | Picked for |
| --- | --- | --- |
| **luna** (default) | `gpt-6-luna` | Everyday and low-difficulty work, including work over a lot of context: chat, summaries, extraction, classification, long docs and logs. Cheap and fast. |
| **opus** | `claude-opus-5-5` | Work that needs deep intelligence: hard reasoning, proofs, complex code, architecture trade-offs, subtle debugging. |
| **argon** | `gemini-4-argon` | Visual and creative craft: UI/visual design, layouts, real-world 3D/spatial reasoning, polished written copy. |

The router is `system.ai.openjev-qwen35-4b`. It answers a single `choice`
question with a probability for each route and generates no text, so a routing
call is cheap, fast, and auditable.

```mermaid
flowchart LR
    C[client] --> D[dispatcher endpoint]
    D -- "1. which model?" --> J["JEV<br/>/ai-gateway/typesafe/v1/systemone"]
    J -- "argon (p=0.99)" --> D
    D -- "2. forward request" --> G["Unity AI Gateway<br/>/ai-gateway/mlflow/v1/chat/completions"]
    G --> L[gpt-6-luna]
    G --> O[claude-opus-5-5]
    G --> A[gemini-4-argon]
    D -. "3. trace: route, model, why" .-> M[(MLflow traces<br/>in UC)]
```

The whole dispatcher is one function in `agent.py`:

```python
@mlflow.trace(name="dispatch", span_type=SpanType.AGENT)
def dispatch(messages, cfg, client=None, forced_route=None, params=None):
    decision = route_request(client, messages, cfg, forced_route)   # ask JEV
    response = call_model(client, decision.model, decision.route, messages, params)
    _tag_trace(decision)                                            # log why
    return {"text": ..., "routing": decision.as_dict(), "response": response}
```

To change routing behavior, edit the plain-English criteria in `ROUTES`. JEV
judges each request against those sentences.

## How a routing decision is logged

Every request leaves the same evidence in four places, so "which model ran,
and why" is never a guess:

| Where | What you see |
| --- | --- |
| **MLflow trace tags** (searchable) | `dispatcher.route`, `dispatcher.model`, `dispatcher.decided_by`, `dispatcher.jev_confidence`, `dispatcher.reason`, plus `dispatcher.fallback_from` / `dispatcher.fallback_reason` when a fallback ran |
| **MLflow spans** | `dispatch` (AGENT) → `jev_router` (CHAIN; attributes `routing.*` hold JEV's full probability table) → `call_model` (CHAT_MODEL; `model`, `route`, token `usage`). A failed model call is its own red span. |
| **Response** | `custom_outputs.routing` carries the same decision object back to the caller |
| **`system.ai_gateway.usage`** | every gateway call carries `Databricks-Ai-Gateway-Request-Tags: {"app": "simple_dispatcher_agent", "route": "opus"}`, so billing rows show the route too |

Example `custom_outputs.routing` from a live call:

```json
{
  "route": "opus",
  "model": "system.ai.claude-opus-5-5",
  "decided_by": "jev",
  "jev_choice": "opus",
  "confidence": 0.84,
  "probabilities": {"luna": 0.094, "opus": 0.894, "argon": 0.011},
  "reason": "JEV chose 'opus' with confidence 0.84: Work that needs deep intelligence: ...",
  "fallback_from": null,
  "fallback_reason": null
}
```

`decided_by` is one of these values:

| Value | Meaning |
| --- | --- |
| `jev` | JEV's pick was confident (≥ `ROUTER_MIN_CONFIDENCE`, default 0.5), or JEV picked luna anyway |
| `low_confidence_default` | JEV was unsure, so luna answered. JEV's lean is still recorded in `jev_choice`. |
| `jev_error_default` | The router call failed. Luna answered rather than failing the request. |
| `caller` | The client pinned a route with `custom_inputs: {"route": "opus"}` |

If the chosen model call fails (for example, a model not yet rolled out to the
workspace's region), the dispatcher retries once on luna and records the
fallback. It never silently swaps the model.

Find every request that went to opus, in the MLflow UI search box or with code:

```python
mlflow.search_traces(filter_string="tags.`dispatcher.route` = 'opus'")
```

Deployed traces are UC-backed, so SQL works too (this query ran against the dev
deploy):

```sql
SELECT from_unixtime(start_time_unix_nano / 1e9)        AS at,
       attributes:['routing.jev_choice']::string        AS jev_choice,
       attributes:['routing.route']::string             AS route,
       attributes:['routing.decided_by']::string        AS decided_by
FROM   <catalog>.<schema>.`<experiment_id>_otel_spans`
WHERE  name = 'jev_router'
ORDER  BY 1 DESC;
```

## Files

| File | Purpose |
| --- | --- |
| `agent.py` | The whole dispatcher: route menu, JEV router, Unity AI Gateway client, MLflow spans/tags, CLI |
| `serving_agent.py` | MLflow 3 `ResponsesAgent` wrapper for Model Serving (Playground, Review App), used by code-based logging |
| `eval_routing.py` | Golden-set check: 12 prompts with an expected route each; run it after editing a criterion (JEV calls only) |
| `driver.py` | Bundle job entry point: log, register to UC, and optionally deploy the endpoint |
| `databricks.yml` | Databricks Asset Bundle (dev/test/prod targets, one deploy-on-demand job) |
| `tests/` | Pytest suite. Runs fully offline (the gateway is faked). |

## Requirements

- A workspace with Unity AI Gateway and the four `system.ai` model services
  above. Check with `databricks api get /api/2.1/unity-catalog/model-services`.
  `gemini-4-argon` is still rolling out by region: where it's missing, argon
  requests fall back to luna (logged), or you can set `ARGON_MODEL` to another
  Gemini.
- `EXECUTE` on those model services. By default, all account users have it.
- Python 3.10–3.12 and `uv`.

## Usage

### Local

```bash
uv sync
uv run pytest                                  # offline, no workspace needed
databricks auth login --profile DEFAULT        # one-time SSO; PATs are not supported

uv run python agent.py "Summarize: the standup moved from 9 to 10am"           # → luna
uv run python agent.py "Prove sqrt(2) is irrational, then say why sqrt(4) fails" # → opus
uv run python agent.py "Write a three-line hero tagline for a coffee roaster"    # → argon
```

Changed a criterion? Re-check routing against the golden set (calls JEV only):

```bash
uv run python eval_routing.py     # 12/12 routed as expected
```

Output from `agent.py` shows the route, the reason, and JEV's probabilities:

```
Route:  argon  →  system.ai.gemini-4-argon
Why:    JEV chose 'argon' with confidence 0.99: Visual and creative craft: ...
JEV:    luna=0.005, opus=0.001, argon=0.994
```

Configuration is optional. Copy `.env.template` to `.env` to override any of
these settings:

| Variable | Default | Purpose |
| --- | --- | --- |
| `LUNA_MODEL` | `system.ai.gpt-6-luna` | default / cheap-fast route |
| `OPUS_MODEL` | `system.ai.claude-opus-5-5` | high-intelligence route |
| `ARGON_MODEL` | `system.ai.gemini-4-argon` | visual / spatial / copy route |
| `JEV_MODEL` | `system.ai.openjev-qwen35-4b` | the router |
| `ROUTER_MIN_CONFIDENCE` | `0.5` | below this, luna answers |
| `MLFLOW_EXPERIMENT` | unset | experiment for traces (local runs go to `./mlflow.db` otherwise) |
| `BASE_CATALOG` / `BASE_SCHEMA` | unset | with an experiment, stores traces in UC Delta tables |

Model ids must be three-part UC identifiers. Legacy `databricks-*` endpoint
names are refused, because the `/ai-gateway/*` routes resolve models in the UC
model catalog (and `system.ai` names drop the `databricks-` prefix).

### Deploy (bundle)

```bash
databricks bundle validate -t dev      # also: -t test --var pr_number=1, -t prod
databricks bundle deploy -t dev
databricks bundle run deploy_dispatcher -t dev          # log + register only

databricks bundle deploy -t prod
databricks bundle run deploy_dispatcher -t prod         # + scale-to-zero endpoint
```

| Target | Schema | Deploys endpoint |
| --- | --- | --- |
| `dev` (default) | `${base_schema}_<short_name>` | no (`--var deploy_endpoint=true` to try it) |
| `test` | `${base_schema}_<pr_number>` | no |
| `prod` | `${base_schema}` | yes |

Query the endpoint (the CLI's `serving-endpoints query` drops the `output`
field, so use `api post`):

```bash
databricks api post /serving-endpoints/<endpoint>/invocations --json '{
  "input": [{"role": "user", "content": "Design a hero layout for a coffee site"}],
  "custom_inputs": {"max_tokens": 800}
}'
```

### The serving endpoint needs a service principal (one-time setup)

Agents on Model Serving **can't use automatic authentication passthrough to
query Unity AI Gateway model services** ([docs](https://docs.databricks.com/aws/en/ai-gateway/migrate-to-unity-gateway)).
Declaring the model services as resources isn't enough: the gateway answers the
endpoint's built-in credential with `NOT_FOUND: 'system.ai.gpt-6-luna' does not exist`.

So the endpoint runs as a service principal whose OAuth secret lives in a
secret scope. The SDK picks OAuth M2M ahead of the model-serving credential,
so the agent code doesn't change:

```bash
SP=$(databricks service-principals create --display-name simple-dispatcher-agent -o json)
databricks service-principal-secrets-proxy create "$(echo "$SP" | jq -r .id)" -o json > /tmp/sp.json
databricks secrets create-scope simple_dispatcher_agent
databricks secrets put-secret simple_dispatcher_agent client_id     --string-value "$(echo "$SP" | jq -r .applicationId)"
databricks secrets put-secret simple_dispatcher_agent client_secret --string-value "$(jq -r .secret /tmp/sp.json)"
rm /tmp/sp.json
```

The SP gets `EXECUTE` on `system.ai` model services as an account user. The
deploy job grants it the rest: `CAN_EDIT` on the experiment, plus
`USE CATALOG` / `USE SCHEMA` / `SELECT` / `MODIFY` on the trace schema, so the
endpoint's traces land in UC. Use another scope name with
`--var secret_scope=<scope>`.

> Databricks now recommends Databricks Apps for new agent deployments, where
> the app's service principal gets this for free. This demo stays on Model
> Serving for the AI Gateway endpoint features (usage tracking, inference
> tables, Review App).

## Engineering notes

**Why JEV and not an LLM prompt or a case statement?** A case statement can't
read intent, and a chat-model router spends output tokens and returns prose
you have to parse. JEV returns a calibrated probability per option and no
text, so the routing decision is structured data you can log, threshold, and
audit.

**Router input is bounded.** JEV sees the latest user turn (truncated to
4,000 chars) plus size signals: turn count, total conversation chars, and
whether images are attached. "Low difficulty but huge context" routes to luna
without shipping the whole context to the router.

**Pass-through, not an agent loop.** The dispatcher forwards the conversation
as-is (text and image parts) and passes `temperature`, `max_tokens`, `top_p`,
and `stop` from `custom_inputs`. It adds one cheap routing call per request and
nothing else.

**Packaging.** `driver.py` logs `serving_agent.py` with `code_paths=["agent.py"]`
(`TestModelPackaging` loads the logged model from a clean cwd to guard this).
Logging runs one real routed request as mlflow's input-example validation,
so a broken model id fails the deploy job, not the first user.

**`uv.lock` is gitignored.** On a corporate network `uv sync` resolves against
an internal mirror, and a committed lockfile would pin unreachable URLs.

**Python 3.10–3.12.** `databricks-agents` pulls `whenever`, which only ships
wheels for the Databricks runtime interpreters.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `NOT_FOUND: 'system.ai.databricks-…' does not exist` | `system.ai` model-service names drop the `databricks-` prefix |
| `ENDPOINT_NOT_FOUND` for `gemini-4-argon` | Not rolled out to this workspace's region yet. Argon requests fall back to luna (see `dispatcher.fallback_reason`), or set `ARGON_MODEL`. |
| Deployed endpoint: `NOT_FOUND` for every model, `JEV routing failed` in logs | The endpoint is using its built-in credential. Set up the service principal secret scope above and redeploy. |
| Deployed endpoint: `Failed to send trace … USE CATALOG` in logs | The endpoint identity can't write the UC trace tables. Redeploy with `--secret-scope` so the job grants the SP access. |
| `401` with two profiles matching one host | `export DATABRICKS_CONFIG_PROFILE=<profile>` |
