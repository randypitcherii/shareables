# AI Gateway improvement loop

Unity AI Gateway logs every model, MCP, and coding-agent request. The built-in
usage dashboard tells you **how much, by whom, on what**. This loop tells you
**what to change**. Every hour it rebuilds sessions from the gateway logs and
scores them with `ai_decide`. It then publishes a ranked list of fixes, each
with estimated dollars and the number of people affected.

```
system.ai_gateway.usage ──► stg_ai_gateway__usage        streaming table (each row read once)
                              │
                              ▼
                            int_ai_gateway__usage_events   materialized view (normalized, priced)
                              │
                              ▼
                            int_ai_gateway__session_events materialized view (session ids)
                              │
                              ▼
                            int_ai_gateway__sessions       materialized view (per-session metrics)
                              │
inference tables ──► int_ai_gateway__payloads ──► int_ai_gateway__session_transcripts   (distilled, redacted)
 (per model service)          │                              │
                              ▼                              ▼
                            ai_gateway__session_scores     ai_decide, append-once per rubric version
                              │
                              ▼
                            ai_gateway__sessions ──► ai_gateway__recommendations ──► dashboard
                                                     ai_gateway__measurement_health
```

## Quickstart

```bash
cp template.env .env            # fill DBT_HOST + DBT_HTTP_PATH
DBT_AI_GATEWAY_HISTORY_DAYS=3 DBT_AI_GATEWAY_MAX_SESSIONS_PER_RUN=25 make ai-gateway
make ai-gateway-eval            # rubric accuracy on the golden set
make deploy                     # dev bundle, including the dashboard
```

In production, `dbt Hourly` builds `tag:hourly`, which is this folder. The
dashboard is `resources/ai_gateway_improvement.dashboard.yml`.

**Prerequisites**
- `SELECT` on `system.ai_gateway` and `system.billing` for whoever runs dbt.
- `ai_decide` enabled: it is a Beta, so check the workspace Previews page.
- For message content: inference tables on your model services, plus `SELECT`
  on them.

## Where message content comes from

The system tables hold **no message content**. `system.ai_gateway.usage` is
metadata only: tokens, latency, routing, requester, client, and session ids.
Request and response bodies exist only in **inference tables**. Each model
service has its own table, and logging is opt-in per service.

The loop finds those tables on its own. `endpoint_metadata.inference_table`
names each one, and every build unions the tables the run's principal can
`SELECT`. Tables it cannot read are skipped and never fail the build.

Coding agents resend the whole conversation on every turn, so the largest
request in a session already holds the full history. The
`ai_gateway_distill_transcript` UDF turns that one payload into at most 12 KB
of JSON with emails and secrets redacted. `ai_decide` only ever sees this
summary.

Content scores only exist where payloads are logged. So the
**`payload_coverage_gap`** recommendation lists the unlogged services by
spend.

## Sessions

Fewer than 1% of gateway rows carry a client session id. Recent Claude Code
and Codex builds send one; most other clients do not. A session is therefore:

| `session_id_source` | Rule |
|---|---|
| `native` | `session_metadata.client_session_id`. Subagent calls roll up into the parent. |
| `synthetic` | Same requester + workspace + client family + invocation source. A new session starts after 30 idle minutes (`ai_gateway_session_gap_minutes`). |

`client_family` merges the gateway's `coding_agent` field with user-agent
rules. For example, older Claude Code builds only send `claude-cli/*`. Edit
`macros/ai_gateway_improvement/client_family.sql` to teach the loop your own
clients.

## Scoring with `ai_decide`

There are two rubrics, both in `macros/ai_gateway_improvement/rubrics.sql`
as literal JSON:

| Rubric | Runs on | Questions |
|---|---|---|
| metadata | every finished session | token efficiency (0-4), model fit, context pressure (0-3), thrash |
| content | sessions with a transcript | goal category, complexity, outcome, quality, frustration, primary friction, prompt clarity, repeated instruction, automation candidate, novel friction, sensitive data |

The content categories reuse Claude Code's `/insights` facet taxonomy.

**Why scores are an append-only table, not a materialized view.** `ai_decide`
is non-deterministic and costs money per row. A materialized view refuses
non-deterministic functions for incremental refresh, so it would recompute,
and re-pay for, every score on every refresh. Instead, each session is scored
**once per rubric version**. The version is an md5 of the rubric JSON.

**Each run picks** up to `ai_gateway_max_sessions_per_run` (default 100)
finished sessions:
1. Sessions with transcripts.
2. Then the costliest sessions.
3. Plus a 10% random sample, so cheap sessions are represented.
4. Plus a 2% **stability probe** that re-scores sessions it already scored.

**How the scores stay reliable.** Imperfect measurement is fine;
unstructured measurement is not.
- Every answer is stored as value + confidence + probabilities, next to the
  raw VARIANT.
- Answers below confidence 0.6 count as `unclear` (choices) or NULL (scores)
  in every aggregate.
- Every choice list has an escape label (`unclear` / `other`). A noul
  question flags friction the taxonomy cannot label.
- `ai_decide` occasionally rejects its own answer. Each failed call is retried
  once, and the `assert_ai_gateway_scoring_errors_are_rare` test fails the
  build above 5% errors.
- **Deterministic anchors:** each judged score has a rule-based twin. For
  example, token efficiency is compared to the cache-hit ratio. The health
  page tracks their correlation.
- **Golden set:** `make ai-gateway-eval` scores 16 hand-labelled sessions
  with the live rubric and fails below per-question accuracy floors. Last
  run: outcome 81%, friction present 94%, exact friction 75%, goal 94%,
  frustration 88%, repeated instruction 100%.

**Throughput.** Measured on an X-Small serverless warehouse:
- `ai_decide` runs about 2–3 question evaluations per second.
- A session takes about 2 s for the metadata rubric, and about 6 s with the
  content rubric as well.
- At 100 sessions per hourly run, that is 2,400 sessions a day, costliest
  first. Raise the cap after measuring your own warehouse.

## Recommendations

`ai_gateway__recommendations` has one row per detector × scope. Nothing is
suppressed for low reach: a finding with one affected person still shows.
Ranking does the prioritizing:

```
impact_score = log10(1 + est_monthly_savings_usd) + log2(1 + people_impacted) + severity
```

At the same dollar value, a pain many people share outranks one person's
expensive habit. Every row carries a `people_impacted_band` (👤 1 · 👥 2-9 ·
🏢 10+), evidence JSON, up to 5 example session ids, and an action.

| Detector | Signal |
|---|---|
| `payload_coverage_gap` | Model services with no inference table. One account-level row; the biggest services by spend are in its evidence |
| `model_overkill` | Judged overpowered, or low complexity, on a premium or standard model. Savings = one tier down |
| `missed_prompt_caching` | Long multi-turn sessions with a cache-hit ratio below 0.3 |
| `context_bloat` | High context pressure, or a max input of 150k+ tokens |
| `retry_storms` | Thrash, 429s, error loops |
| `error_hotspot` | Endpoint × client version error rate ≥ 5% |
| `stale_agent_version` | People behind the newest version of their coding agent |
| `pat_usage` | Requests authenticated with personal access tokens |
| `untagged_spend` | Spend with no team / cost-center tag. One account-level row; the worst workspaces are in its evidence |
| `reasoning_effort_mismatch` | High reasoning effort on simple work |
| `guardrail_overhead` | Guardrail judge calls ≥ 10% of a workspace's cost. One account-level row listing those workspaces |
| `friction_hotspot` | Recurring primary friction per client, with a friction-specific fix |
| `codify_repeated_instructions` | Users restating standing rules: move them to AGENTS.md / CLAUDE.md |
| `low_outcome_task_area` | Task categories that often end not or partially achieved |
| `automation_candidate` | Repetitive workflows worth a skill or script |
| `novel_friction` | Friction the rubric cannot label: time to extend the taxonomy |

Account-wide detectors roll up to one row on purpose. On an account with hundreds of
workspaces, a row per workspace (436 untagged-spend rows in testing) buries every other finding.

Thresholds are dbt vars with inline defaults, such as
`ai_gateway_low_cache_hit_ratio`. Override them in `dbt_project.yml`.

## Cost estimates

Costs are **list-price estimates** from `seeds/ai_gateway_improvement/ai_gateway_model_prices.csv`.
Each model maps to a family, a tier, and prices per million tokens by token
class. The first regex match wins. The shipped prices are illustrative
defaults: replace them with your rates. Unknown models fall to a catch-all
row, and the health page reports that unpriced share.

Allocating `system.billing` to requests was tried and rejected. Billing also
covers non-gateway traffic to the same endpoints, so the per-token rates it
implied were off by 10x for some models. Billing is used for reconciliation
only.

## Incrementality: what was verified

| Layer | Refresh |
|---|---|
| `stg_ai_gateway__usage` | Streaming: each new usage row read once |
| `int_ai_gateway__usage_events` | Incremental when the model catalog only changes by merge. Row tracking on, and `price_hash` stops rewrites |
| `int_ai_gateway__sessions` | Incremental (`GENERIC_AGGREGATE`) |
| `int_ai_gateway__session_events` | Incremental (`WINDOW_FUNCTION`). The cost model may still pick a full recompute after a very large upstream change |

A materialized view directly over `system.ai_gateway.usage` **cannot** refresh
incrementally: system tables are Delta-Shared (`INPUT_NOT_IN_DELTA`). That is
why the streaming mirror exists. Check any view's choice with:

```sql
select timestamp, details:planning_information:incrementalization_status
from event_log(table(<catalog>.<schema>.int_ai_gateway__usage_events))
where event_type = 'planning_information' order by timestamp desc limit 5
```

## Privacy

The marts hold per-user, account-wide data:
- **Grants:** production grants `SELECT` only to the group in
  `DBT_AI_GATEWAY_READERS`. Nothing is granted to `account users`.
- **Dashboard:** it uses embedded credentials, and `CAN_READ` goes to
  `ai_gateway_dashboard_viewers` (default `admins`).
- **Transcripts:** they are redacted, bounded, and stored only as distilled
  summaries, never as raw payloads.

## Known limits

- **Synthetic sessions are approximate.** Several short conversations from
  one client within 30 minutes merge into one session.
- **Coverage tracks your configuration.** A service can name an inference
  table that holds no row for a given request, because of sampling, the 10 MiB
  limit, or unlogged 4xx/5xx responses. Those sessions get metadata scores
  only.
- **`ai_decide` is in Beta.** Never use `none` as a choice label: the
  function can return an extra probability for it and fail the whole call.
  The pytest enforces this.
