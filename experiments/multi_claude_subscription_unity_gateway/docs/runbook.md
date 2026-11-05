# Runbook: the rows that need real Claude subscriptions

Rows 01–08 run with no Anthropic credential at all. The four rows here can't: each needs people
signed in to real, separate Claude Enterprise or Team subscriptions. Run them once per rollout,
with one volunteer per subscription, and paste the evidence into `results/matrix_results.json`
by hand (`"result"`, `"notes"`, `"evidence"`, `"recorded_at"`).

## Before you start

1. Deploy for real: three account groups, one per subscription.
   ```bash
   databricks bundle deploy -t rollout --var catalog=<catalog> \
     --var sub_a_principal=<group-a> --var sub_b_principal=<group-b> --var sub_c_principal=<group-c>
   ```
2. Each volunteer installs the Unity Gateway CLI and signs in to Claude Code with **their own**
   subscription:
   ```bash
   uv tool install git+https://github.com/databricks/unity-gateway
   ug claude --provider <catalog>.<schema>.claude_sub_<x> --workspace https://<workspace-url>
   # in Claude Code: /login → "Claude account with subscription"
   ```

## R1 — a real call through each service bills that subscription

| Step | Evidence |
|---|---|
| Volunteer A runs one prompt through `claude_sub_a`, B through `claude_sub_b`, C through `claude_sub_c` | HTTP 200 in Claude Code |
| Each subscription admin opens the Anthropic console usage page | the call shows on A's org only, B's only, C's only |
| `make row-07` after ~30 min | one row per (service, volunteer), `relayed = true`, `status_code = 200` |

✅ when every call lands on its own subscription and nowhere else.

## R2 — a subscription-B token through subscription A's service

Row 05 shows a relayed service stores no organization, so the expected answer is **it works**.
That is the finding to confirm, because it means the gateway cannot stop it.

| Step | Evidence |
|---|---|
| Grant volunteer B `EXECUTE` on `claude_sub_a` for the test, then run one prompt through it while signed in to subscription B | HTTP 200 (expected) |
| Anthropic console | the call bills **subscription B**, not A |
| `system.ai_gateway.usage` | the row says `service_name = …claude_sub_a`, `requester = volunteer B` |
| Revoke the test grant | — |

Record ❌ ("no org binding") with this evidence. The control that remains is the group grant,
plus an audit query: join `system.ai_gateway.usage` `requester` to group membership and flag
rows whose service doesn't match the requester's group.

## R3 — an Omnigent session on each subscription

On each volunteer's Omnigent host:

```bash
uv pip install -e experiments/multi_claude_subscription_unity_gateway/omnigent_ug_launcher  # same env as the host
export OMNIGENT_CLAUDE_LAUNCHER=ug
export UG_CLAUDE_PROVIDER=<catalog>.<schema>.claude_sub_<x>
export UG_WORKSPACE=https://<workspace-url>
export OMNIGENT_RUNNER_ENV_PASSTHROUGH=UG_CLAUDE_PROVIDER,UG_WORKSPACE
# restart the Omnigent host so the runner picks these up, then start a native Claude session
```

| Evidence | ✅ when |
|---|---|
| `ps` on the host shows `ug claude --provider …claude_sub_<x>` as the session's process | the plugin is in force |
| a prompt in the session returns | the relay works under Omnigent |
| `system.ai_gateway.usage` row for that service and volunteer | attribution survives Omnigent |

## R4 — cross-subscription collaboration in one shared Omnigent session

The point of the rollout: people on different subscriptions working in one session.

| Step | Evidence |
|---|---|
| Volunteer A (subscription A) starts a session on A's host and shares it with B | B sees the live session |
| B sends a prompt into A's session | the prompt runs |
| Usage + Anthropic console | the turn bills **subscription A**, the host owner's, under `service_name = …claude_sub_a` |

Write down who the gateway records as `requester` for B's turn. It will be A, since A's host
holds the credentials. Before rolling this out, check your Anthropic agreement on whether one
subscriber's seat may run turns that someone else typed.

## Teardown

```bash
make teardown   # dev: bundle destroy + delete the test-caller SP
```
Inference tables (`claude_sub_*_payload`) outlive their services. Drop them if you redeploy under
the same prefixes, or the create fails with "inference table … already exists and is not
associated with this service".
