# N Claude subscriptions behind one Unity Gateway, shared in Omnigent

## Status: first live run 2026-11-05

Rows 01–08 ran against one AWS Databricks workspace, `dev` target. 7 rows answered, 1 waiting
on usage-table lag. The four rows that need real, separate Claude subscriptions are in the
[runbook](docs/runbook.md).

## The question

Some organizations buy several Anthropic Enterprise or Team subscriptions instead of one large
one, and each engineer belongs to exactly one. They want every engineer to go through one
Databricks workspace's Unity Gateway, billed to their own subscription, and to work together in
shared Omnigent sessions.

**Can one workspace front N subscriptions, keep each group on its own subscription's service,
and run Omnigent's Claude sessions through them?**

Not in scope: routing one person across several subscriptions, or pooling quota. Neither is
supported.

## Answer

Yes, with one caveat that matters.

- **N relayed services sit side by side in one schema** (row 01). Each stores no credential and
  relays the caller's own Anthropic token, so each call is billed to whichever subscription the
  caller signed in to (rows 02–03).
- **Unity Catalog `EXECUTE` gates each service before the relay** (row 04). A caller granted
  `sub_a` only gets 403 on `sub_b` and `sub_c` and never reaches Anthropic.
- **The caveat: a service can't be pinned to an Anthropic organization** (row 05). The relayed
  config has no fields. The API accepts organization fields with HTTP 200 and drops them, so a
  `sub_a` service relays a subscription-B token too. Group grants keep people on the right
  service. Per-service usage rows let you audit it. Nothing enforces it.
- **Per-service rate limits fire before the relay** (row 06), so you can keep each subscription's
  traffic under Anthropic's limit without spending any of it on throttled calls.
- **Omnigent needs no change** (row 08). A small launcher plugin rewrites Omnigent's native
  `claude <args>` launch to `ug claude --provider <service> <args>`, keeping every argument
  Omnigent adds. Each Omnigent host runs on its owner's subscription.

So in a shared Omnigent session, every turn runs on the host owner's subscription, whoever typed
it ([runbook R4](docs/runbook.md#r4--cross-subscription-collaboration-in-one-shared-omnigent-session)).

```
 engineer (group A) ──► Omnigent host A ──► ug claude --provider cat.sch.claude_sub_a ─┐
 engineer (group B) ──► Omnigent host B ──► ug claude --provider cat.sch.claude_sub_b ─┼─► Unity Gateway
 engineer (group C) ──► Omnigent host C ──► ug claude --provider cat.sch.claude_sub_c ─┘   │ UC EXECUTE per group
                                                                                          │ rate limit per service
                                                                                          │ usage + payload per service
                                                   relays caller's own Anthropic token ◄──┘
                                                   ──► Anthropic (subscription A | B | C)
```

## Findings matrix

Source of truth: [`results/matrix_results.json`](results/matrix_results.json). ✅ works ·
❌ does not · ◑ partly · ❓ not isolated (reason in notes).

| # | Question | Result | Evidence |
|---|---|---|---|
| 01 | N relayed Anthropic provider services coexist in one schema | ✅ | 3/3 read back as `anthropic.relayed`, none with `direct` (stored key), each with its own inference table |
| 02 | A relayed service refuses a call with no subscription token | ✅ | every service: 400 "uses relayed authentication and can only be invoked by a relayed (credential-swapped) request" |
| 03 | The gateway forwards the caller's Anthropic token to Anthropic | ✅ | fake OAuth token, every service: upstream 401 "OAuth access token is invalid." from `provider=anthropic` |
| 04 | `EXECUTE` gates each service before the relay | ✅ | caller with EXECUTE on `sub_a` only: `sub_a` reached Anthropic; `sub_b`, `sub_c` 403 at Unity Catalog |
| 05 | A relayed service can be pinned to one Anthropic organization | ❌ | control create 200; `organization_id`, `anthropic_organization_id`, `allowed_organizations` each 200 and **dropped**. Config stays `relayed: {}` |
| 06 | A per-service rate limit throttles before the relay | ✅ | limit 2/min: reached → reached → 429 → 429 |
| 07 | Usage attributes each relayed call to caller **and** service | ❓ | `system.ai_gateway.usage` has `requester`, `service_name`, `invocation_metadata.relayed`; ingestion lags 15–40 min, re-run `make row-07` |
| 08 | Omnigent's native Claude launch runs through `ug claude --provider` | ◑ | Omnigent's own `resolve_claude_launch()` returns `ug claude --provider <svc> --workspace <ws> <Omnigent args…>`. Live session: runbook R3 |
| R1 | A real call through each service bills that subscription only | ❓ | needs subscriptions: [runbook](docs/runbook.md#r1--a-real-call-through-each-service-bills-that-subscription) |
| R2 | A subscription-B token through `sub_a`'s service is refused | ❓ | expected ❌ per row 05: [runbook](docs/runbook.md#r2--a-subscription-b-token-through-subscription-as-service) |
| R3 | A live Omnigent session on each subscription | ❓ | [runbook](docs/runbook.md#r3--an-omnigent-session-on-each-subscription) |
| R4 | Cross-subscription collaboration in one shared Omnigent session | ❓ | [runbook](docs/runbook.md#r4--cross-subscription-collaboration-in-one-shared-omnigent-session) |

### How the relay authenticates

A relayed request carries two credentials:

| Header | Carries | Used for |
|---|---|---|
| `X-Databricks-AI-Gateway-Token: Bearer <databricks token>` | the caller's Databricks identity | `EXECUTE` check, rate limits, usage `requester` |
| `Authorization: Bearer <anthropic oauth token>` | the caller's Claude subscription | relayed to Anthropic, which bills that subscription |
| `Databricks-Model-Provider-Service: <cat>.<sch>.<svc>` | which service | which grants, limits and inference table apply |

Two shapes that fail, for the record:

- The `X-Databricks-AI-Gateway-Token` value needs the `Bearer ` prefix. Without it the gateway
  answers 401 "Credential was not sent or was of an unsupported type" before any service lookup.
- Databricks token in `Authorization` with no swap header gets row 02's 400: the relay has nothing
  to forward.

`ug claude --provider` sets all three headers and refreshes the Databricks token per request
through a local proxy, so the one-hour OAuth expiry never reaches Claude Code.

### Operational gotchas

- **A create can succeed after the client gives up.** The first `bundle deploy` returned
  `499 CANCELLED` for two services that the server had in fact created. The retry then failed
  `409 ALREADY_EXISTS`, with the services outside bundle state. Delete them (or `bundle
  deployment bind`) and redeploy.
- **Inference tables outlive their service.** Delete a service and its `<prefix>_payload` table
  stays. Re-creating under the same prefix fails "inference table … already exists and is not
  associated with this service" until you drop the table.

## The Omnigent launcher plugin

[`omnigent_ug_launcher/`](omnigent_ug_launcher/) registers `ug` in Omnigent's
`omnigent.claude_launcher` entry-point group. On each host:

```bash
uv pip install -e omnigent_ug_launcher            # into the env the Omnigent host runs in
export OMNIGENT_CLAUDE_LAUNCHER=ug
export UG_CLAUDE_PROVIDER=<catalog>.<schema>.claude_sub_<x>   # this host owner's subscription
export UG_WORKSPACE=https://<workspace-url>
export OMNIGENT_RUNNER_ENV_PASSTHROUGH=UG_CLAUDE_PROVIDER,UG_WORKSPACE   # survives the daemon→runner env strip
```

With no `UG_CLAUDE_PROVIDER` the launch is left as plain `claude`. A malformed one raises, and
Omnigent logs it and falls back to plain `claude`, so a misconfigured host never blocks a session.

## Run it

```bash
cp .env.example .env    # profile + catalog
make                    # list targets
make up                 # dev: throwaway test-caller SP, then bundle deploy
make matrix             # rows 01–08 (row 05 creates and deletes a probe service)
make row-07             # again ~30 min later, once usage has landed
make test               # no-infra unit tests: harness + launcher plugin
make teardown           # bundle destroy, delete the test-caller SP
```

`dev` grants `sub_a` to the test caller and leaves `sub_b` / `sub_c` owner-only, so row 04 has
something to deny. A real rollout uses the `rollout` target and three account groups (see the
[runbook](docs/runbook.md#before-you-start)).

## Identities

| Principal | Used for |
|---|---|
| owner (you, via profile) | deploys and owns the services; rows 01–03, 05 |
| test caller SP (`make setup`) | the non-owner rows 04 and 06 act as. Not an admin; EXECUTE on `sub_a` only |

Results are written with identities, the workspace host, the schema and request ids replaced by
`<owner>`, `<test_caller>`, `<workspace>`, `<schema>` and `<id>`.

## Layout

```
databricks.yml  resources/      bundle: schema + three relayed provider services (dev / rollout)
scripts/_common.py              principals, relayed-call helper, result classifier, redacting recorder
scripts/setup/                  test-caller SP create / delete
scripts/matrix/NN_*.py          one row each
omnigent_ug_launcher/           Omnigent Claude launcher plugin (+ tests)
docs/runbook.md                 rows R1–R4, which need real subscriptions
results/matrix_results.json     committed results
tests/                          no-infra unit tests
```

## Related

- [Claude Max and Anthropic Enterprise support](https://docs.databricks.com/aws/en/ai-gateway/claude-max-anthropic-enterprise-support), the relay this builds on
- [`coding_agent_inference_unity_ai_gateway`](../coding_agent_inference_unity_ai_gateway/): governance findings for coding agents on the gateway
- [Unity Gateway CLI (`ug`)](https://github.com/databricks/unity-gateway)

> ⚠️ Uses Unity AI Gateway APIs that are still moving. Re-run the matrix before relying on it.
