# AI Gateway Improvement Loop — Plan

> Status: **plan, not yet built.** Tracking issue: #90.

## Problem

Unity AI Gateway logs every model, MCP, and coding-agent request to
`system.ai_gateway.usage`. The built-in usage dashboard answers *"how much, by
whom, on what?"* It does not answer *"what should we change?"*:

- Which sessions were wasteful, and why (wrong model, no prompt caching, context bloat, retry storms)?
- Where do users hit friction (misunderstood requests, excessive changes, tool failures)?
- What is each fix worth in dollars, and who does it affect?

Customers have the logs on day one. They have no opinionated path from logs to
a ranked list of improvements.

## What we ship

One Databricks Asset Bundle. `databricks bundle deploy -t prod` gives you:

1. **An hourly Lakeflow pipeline** that materializes gateway usage and rebuilds sessions.
2. **An `ai_decide` scoring step** that scores each closed session once against a versioned rubric.
3. **A `recommendations` table** of aggregated pains and opportunities, ranked by estimated impact.
4. **An AI/BI dashboard** to investigate the recommendations, down to example sessions.

```
system.ai_gateway.usage ─┐
system.billing.*        ─┼─► usage_events (MV) ─► sessions (MV) ──────────┐
                         │                                                 ├─► session_scores (ai_decide, append-once)
inference tables ────────┴─► all_payloads (view) ─► session_transcripts (MV)┘            │
 (one per model service, discovered)                                                      ▼
                                                    recommendations (MV) ─► dashboard + measurement health
```

## Where message content lives

The system tables do **not** store message content. `system.ai_gateway.usage`
is request metadata only: tokens, latency, routing, requester, client, and
session ids. There is no request or response column. No other `system.*` table
holds payloads either.

Request and response bodies land only in **inference tables**. Each model
service gets its own Delta table, in a catalog and schema the service owner
picks. Logging is opt-in per service, `system.ai.*` services included. The
usage table names each service's inference table in
`endpoint_metadata.inference_table`, so the bundle can discover them all.

Consequences:

- **Content scoring is on by default**, but its coverage is only as good as
  inference-table enablement. In the workspace we probed, about 8% of
  coding-agent requests had a payload table.
- **Payload coverage is itself recommendation #0.** It lists every model
  service without an inference table, ranked by spend and users affected.
- The pipeline ships a **discovery step** that regenerates an `all_payloads`
  view each run: a `UNION ALL` of every discovered inference table the job's
  principal can read, normalized to one schema.
- Payload tables share Zerobus at-least-once delivery, so dedupe on
  `(request_id, invocation_id)`. They don't log bodies over 10 MiB, and they
  may skip 401/403/429/500 responses.

## Evidence the design rests on (verified live, Oct 2026)

| Finding | Design consequence |
|---|---|
| `client_session_id` is present on ~94–100% of rows tagged `claude-code` / `codex`, but on **<1% of all traffic**. Older Claude Code builds show up only as `user_agent = claude-cli/*` with no `coding_agent` | Sessions **must** be reconstructed: native id when present, else a gap-based synthetic id. User-agent normalization is a first-class seed |
| A large share of rows is `invocation_metadata.source = AI_QUERY` / `GUARDRAIL` / `DATABRICKS_APPS` | Classify `source` up front; guardrail overhead and batch `ai_query` are separate stories from interactive use |
| `ai_decide` works on serverless SQL. One call returns choice + probabilities + `confidence` per question. ~8 rows/s observed on an X-Small warehouse with short inputs | Score **sessions, not requests**. Cap rows per run. Score each session once |
| `ai_decide` is non-deterministic. MV incremental refresh rejects non-deterministic functions (time functions in `WHERE` excepted) | Scoring cannot live in an MV. An MV would fully recompute, re-scoring everything hourly. Scoring is a job-managed append-once table |
| Inference tables (payloads) are opt-in per model service, discoverable via `endpoint_metadata.inference_table`. The system tables have no message content | Content scoring runs wherever payloads exist and the bundle drives enablement. Metadata scoring covers every session |
| `SUM`/`AVG` over DOUBLE forces a full MV refresh | Store costs as `DECIMAL(38,10)` |

## Prior art: reuse vs. skip

| Prior art | Take | Leave |
|---|---|---|
| Built-in Unity AI Gateway usage dashboard (Coding Agents tab) | Link to it. Don't rebuild volume, latency, or cost charts | Its scope: descriptive only |
| Claude Code `/insights` facet taxonomy | Reuse its goal / outcome / friction / session-type / helpfulness categories **verbatim** as `ai_decide` `choice` criteria. Field-tested, and familiar to users | Its free-form LLM narratives |
| Anthropic Clio | Facets → aggregate. Every recommendation carries `affected_users` as a first-class impact signal | The min-k suppression floor: single-user findings still surface. Also embedding clustering (v2 candidate) |
| Error analysis (open → axial coding; Husain/Shankar) | A fixed v1 taxonomy plus an `other` choice and an `is_novel_friction` noul, so taxonomy drift is measurable | Manual labeling as a gate |
| `HobbsAnalytics/databricks-metric-views-system-tables` (already ported in `../dbt`) | Ship a metric view over `usage_events` for Genie | — |
| `lukaleet/genie-code-usage-tracking` (DAB) | Dashboard-in-bundle patterns: unqualified dataset refs + `dataset_catalog`/`dataset_schema` injection; permissions pinned in config | — |
| `databricks-solutions/agent-control-plane` | Mention as an adjacent governance app | Lakebase/app architecture (too heavy for a starter) |
| OpenTelemetry GenAI semconv | Align column names (`session_id`, `agent_name`, `request_model`, `usage_*_tokens`) | Span model |

## Data model

### 1. `usage_events` — materialized view, incremental

Flattened `system.ai_gateway.usage`, rolling `lookback_days` window (default 90).

- Identity: `account_id, workspace_id, request_id, invocation_id, event_time, requester, requester_type, auth_mode`
- Service: `service_type, source (invocation_metadata.source), endpoint_name, destination_model, model_family, model_tier`
- Client: `client_family` (normalized from `session_metadata.coding_agent` **or** a `user_agent` regex seed: `claude-cli→claude-code`, `codex-tui→codex`, `ucode`, `OpenAI/Python→sdk-python`, …), `agent_version, surface, reasoning_effort, smart_router_name`
- Session keys: `native_session_id, subagent_id`
- Tokens: `input, output, cache_read, cache_write_5m/1h, reasoning` plus derived `uncached_input`
- Outcome: `status_code, is_error, is_rate_limited, latency_ms, ttfb_ms, fallback_attempts`
- Attribution: `request_tags, service_tags, team` (`coalesce` over a configurable tag key list)
- Cost: `est_cost_usd DECIMAL`, from a `model_prices` seed (list price per 1M tokens by token class), cross-checked against `system.billing.usage` at the day × endpoint grain on the health page

### 2. `sessions` — materialized view

`session_id = coalesce(native_session_id, synthetic_id)`. `synthetic_id` = hash of
(`requester, workspace_id, client_family`, gap-bucket), with a new session after
`session_gap_minutes` idle (default 30). Subagent rows roll up into the parent.

Per session: `start/end, duration, n_requests, n_subagents, models_used, primary_model, model_tier_max, tokens by class, cache_hit_ratio, max_input_tokens, input_growth_per_turn (slope), output_input_ratio, error_rate, n_429, n_fallbacks, reasoning_effort_mode, mcp_tool_calls, est_cost_usd, is_closed (end < now - gap), session_id_source (native|synthetic)`.

### 3. `session_transcripts` — materialized view (`enable_content_scoring`, default **true**)

Reads `all_payloads`, joined to `usage_events` on `(request_id, invocation_id)`. For each session with payloads, take the request with the largest
`input_tokens`. Coding agents resend the full history, so that request *is*
the transcript. Distill it into a bounded `state` JSON (≤ `max_state_chars`, default 12k):
first user prompt, last N user turns, user corrections, tool-error snippets, final
assistant turn, plus the session metrics. Regex redaction runs for emails, keys, and tokens.

### 4. `session_scores` — Delta table, append-once, job-written

`MERGE` closed, unscored sessions (`rubric_version` not yet present). The job
takes the top `max_sessions_per_run` (default 1000) by cost, plus a 10%
stratified random sample so cheap sessions stay represented. Two rubrics
live in `src/rubrics/*.json`. The JSON is the constant `questions` argument,
and its hash is stored as `rubric_version`.

**Metadata rubric (all sessions)** — state = session metrics JSON:

| Field | Type | Criteria |
|---|---|---|
| `token_efficiency` | score 0–4 | wasteful → lean, given tokens, cache ratio, growth, and retries |
| `model_fit` | choice | `overpowered / right_sized / underpowered / unclear` |
| `context_pressure` | score 0–3 | none → near the context limit |
| `thrash` | noul | retry / error-loop behavior |
| `workload_type` | choice | `interactive_coding / batch_ai_query / app_backend / agent_pipeline / exploration / guardrail / other` |

**Content rubric (default on, wherever payloads exist)** — state = distilled transcript:

| Field | Type | Criteria (from `/insights`) |
|---|---|---|
| `goal_category` | choice | `debug_investigate, implement_feature, fix_bug, write_script_tool, refactor_code, configure_system, create_pr_commit, analyze_data, understand_codebase, write_tests, write_docs, deploy_infra, warmup_minimal, other` |
| `complexity` | score 0–4 | trivial → multi-system |
| `outcome` | choice | `fully / mostly / partially / not_achieved / unclear` |
| `quality` | score 0–4 | assistant output quality |
| `user_frustration` | score 0–3 | explicit user signals only |
| `primary_friction` | choice | 12 `/insights` friction types + `none` + `other` |
| `prompt_clarity` | score 0–3 | user request specificity |
| `repeated_instruction` | noul | user re-states a rule → candidate AGENTS.md / skill |
| `is_novel_friction` | noul | taxonomy-drift signal |
| `sensitive_data_present` | noul | governance flag |

Each answer is stored as `value, confidence, probabilities` (exploded columns, not just the raw VARIANT).

**Reliability rules** — imperfect is fine, *unstructured* is not:

1. Every choice has an `unclear`/`other` escape. Answers with `confidence < min_confidence` (default 0.6) count as `unclear` in aggregates.
2. Rubrics are versioned constants. A rubric change re-scores only via an explicit backfill.
3. **Stability probe:** each run re-scores 1% of previously scored sessions. The health page shows the agreement rate per field.
4. **Deterministic anchors:** each metadata score has a rule-based twin (e.g. `cache_hit_ratio` vs `token_efficiency`). The health page tracks rank correlation, so a judge that drifts from the numbers is visible.
5. **Golden set:** ~30 synthetic sessions with expected labels in `tests/golden/`. `make eval` fails CI on regression.

### 5. `recommendations` — materialized view

One row per (detector × scope). Scope = account | workspace | team | user | client_family | model.
Columns: `detector, category, scope_type, scope_value, severity, affected_sessions, affected_users, est_monthly_savings_usd, evidence (struct), example_session_ids (top 5), action (templated text), docs_url`.
**Nothing is suppressed for low reach.** A finding backed by one user still
shows. Ranking does the prioritizing:

- `people_impacted` = distinct `affected_users`, shown as a badge on every row
  (👤 1 · 👥 2–9 · 🏢 10+) next to the dollar figure.
- `impact_score` = `log10(1 + est_monthly_savings_usd)` + `log2(1 + affected_users)`
  + `severity_weight`, with weights in `variables`. A broad pain outranks one
  person's expensive habit at the same dollar value.
- The dashboard's default sort is `impact_score`. It also offers a
  "most people affected" sort.

v1 detector catalog (opinionated; each one is a SQL CTE with documented thresholds in `variables`):

| # | Detector | Signal | Action template |
|---|---|---|---|
| 0 | Payload coverage gap | Model services with no inference table, by spend and users | Enable inference tables so content scoring covers them |
| 1 | Model overkill | `model_fit=overpowered` or low `complexity` on `model_tier=premium` | Route to tier-X / smart router; est. $ via price delta |
| 2 | Missed prompt caching | High uncached input on repeated-prefix sessions, low `cache_hit_ratio` | Enable caching / upgrade client |
| 3 | Context bloat | High `input_growth_per_turn` / `context_pressure` | Compaction, subagents, smaller sessions |
| 4 | Retry storms | `thrash`, 429s, error loops | Raise rate limits, add fallbacks |
| 5 | Error hotspots | `error_rate` by endpoint × client_version | Pin / upgrade version, fix config |
| 6 | Stale agent versions | `agent_version` behind the latest seen | Upgrade |
| 7 | PAT usage | `auth_mode=PAT` | Move to OAuth |
| 8 | Unattributed spend | No team tag | Add request / service tags |
| 9 | Reasoning effort mismatch | `high` effort on low complexity | Lower default effort |
| 10 | Guardrail overhead | GUARDRAIL token share | Tune guardrail scope |
| 11 | Friction hotspots* | `primary_friction` × client × team | Specific rule / skill / training |
| 12 | Codify repeated instructions* | `repeated_instruction` clusters | Add to AGENTS.md / CLAUDE.md |
| 13 | Low-outcome task areas* | `outcome` / `frustration` by `goal_category` | Enablement, better tooling |

\* needs payloads (sessions on services with inference tables).

## Dashboard (AI/BI, in bundle)

1. **Start here** — top 10 recommendations by `impact_score`, with $ at stake, the people-impacted badge, and trend vs last week.
2. **Recommendations** — filterable ranked table; drill into evidence and example sessions.
3. **Sessions explorer** — per-session metrics and scores; filter by user / team / client / model.
4. **Efficiency** — model fit, caching, context, reasoning effort.
5. **Quality & friction** — outcome, friction, frustration by task category (sessions with payloads).
6. **Reliability** — errors, 429s, fallbacks, versions.
7. **Measurement health** — freshness, payload coverage (% of sessions and spend with content), scoring coverage, confidence distribution, stability agreement, judge-vs-anchor correlation, cost reconciliation vs billing, ai_decide spend.

The dashboard is published with embedded credentials and `CAN_READ` for
`var.dashboard_viewers` only. The data is per-user and account-wide, so do
not share it broadly by default.

## Bundle layout (house standards: `dabs-environments`, `databricks-jobs`)

```
databricks/demos/ai_gateway_improvement_loop/
├── databricks.yml            # dev/test/prod, base_catalog/base_schema, name_prefix, profile-only auth
├── resources/
│   ├── schema.yml            # schema + grants
│   ├── pipeline.yml          # serverless Lakeflow SQL pipeline (usage_events, sessions, transcripts, recommendations)
│   ├── job.yml               # hourly: pipeline core → score_sessions (SQL task) → pipeline recommendations
│   └── dashboard.yml
├── src/
│   ├── pipeline/*.sql
│   ├── scoring/score_sessions.sql
│   ├── rubrics/{metadata_v1,content_v1}.json
│   ├── seeds/{model_prices,client_normalization,model_tiers}.csv
│   └── dashboards/improvement_loop.lvdash.json
├── tests/                    # rubric schema + golden-set eval + bundle validate
├── Makefile                  # validate / deploy / run / eval / test
├── .env.template             # DATABRICKS_PROFILE=DEFAULT only
└── README.md                 # 10-minute quickstart, prerequisites (system table grants), cost notes
```

Schedules fire only in `prod` (`mode: production`). The job runs as a service
principal that needs `SELECT` on `system.ai_gateway`, `system.billing`, and
optionally on the payload tables.

## Delivery phases

| Phase | Scope | Exit |
|---|---|---|
| **0. Spikes** (½ day) | (a) `EXPLAIN CREATE MATERIALIZED VIEW` over `system.ai_gateway.usage`: does it plan incremental? Fallback: streaming table with `skipChangeCommits`. (b) `ai_decide` throughput on serverless S vs XS with realistic 4–12k-char states. (c) Payload shapes for `chat/completions`, `anthropic/messages`, and `responses` across inference tables. (d) user_agent normalization coverage ≥ 95% of rows. (e) Can inference tables be enabled via API/bundle, or only in the UI? | Decisions recorded in the README |
| **1. MVP** | Bundle, `usage_events`, `sessions`, payload discovery + `all_payloads`, `session_transcripts`, both rubrics, detectors 0–13, all dashboard pages | Deploys to a clean workspace via `make deploy` and shows real recommendations, content-based included, after one run |
| **2. Coverage** | Payload-enablement helper (script or job task, per spike e); per-service payload sampling knobs | Coverage gap closes on a demo workspace |
| **3. Hardening** | Stability probe, judge-vs-anchor correlation, CI test target (`_pr<N>` schema), demo GIF | `make eval` + `bundle validate` green in CI |

## Open decisions

1. **Home:** `databricks/demos/ai_gateway_improvement_loop` here, or a standalone repo? A standalone repo is easier for customers to fork.
2. **Engine:** a Lakeflow SQL pipeline (recommended; MVs native, no dbt prerequisite) vs the house dbt stack.
3. ~~Content tier default~~ — **decided: on** wherever payloads exist.
4. **Scope:** account-wide (the table is account-wide) with an optional `workspace_ids` filter.
5. **Costing:** a seed price table (portable) vs joining `system.billing.list_prices` (exact but coarser grain). Recommended: both, with billing as reconciliation.
