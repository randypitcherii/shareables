# Simple Dispatcher Agent

## When you outgrow your Genie space

You built a Genie space. People love it. Now they're asking questions Genie
alone can't answer — fresh public facts that live on the internet, not in your
tables.

Graduating from a Genie space to a sophisticated multi-tool agent takes
surprisingly little code. This demo is a **dispatcher** agent that routes each
question to either:

- **the Genie space you already have** (your org's structured data), or
- **a `web_search` Unity Catalog UDF** (fresh public/internet information).

The dispatcher itself is the whole pitch — it's about ten lines:

```python
# agent.py — "THIS IS THE WHOLE AGENT"
from langchain.agents import create_agent

def build_dispatcher(llm, tools):
    return create_agent(llm, tools, system_prompt=SYSTEM_PROMPT)

def build_dispatcher_from_config(cfg):
    llm = ChatDatabricks(endpoint=cfg["llm_endpoint"])
    tools = [
        make_genie_tool(cfg["genie_space_id"]),                  # your Genie space
        make_web_search_tool(cfg["base_catalog"], cfg["base_schema"]),  # web_search UDF
    ]
    return build_dispatcher(llm, tools)
```

Each tool's docstring tells the router when to pick it. The model does the
routing; you just hand it the tools you already have.

## Files

| File | Purpose |
| --- | --- |
| `agent.py` | The showpiece — config loader, the two tool seams, `build_dispatcher` (`create_agent`), MLflow tracing, CLI entry point |
| `serving_agent.py` | MLflow 3 `ResponsesAgent` wrapper around the same graph; used for Playground/Review App serving and code-based model logging (`mlflow.models.set_model`) |
| `register_web_search.py` | Registers the `web_search(query)` UC UDF; runs as a notebook cell or from `driver.py` |
| `driver.py` | Bundle job entry point — registers the UDF, logs + registers the agent to UC, optionally deploys the serving endpoint |
| `databricks.yml` | Databricks Asset Bundle (dev/test/prod targets, one deploy-on-demand job) |
| `tests/test_agent.py` | Pytest suite; runs with no live workspace (every seam is mocked) |
| `pyproject.toml` | uv-managed project; all commands via `uv run` |
| `.env.template` | Copy to `.env` for local dev (the real `.env` is gitignored) |

## Requirements

- Unity Catalog enabled workspace.
- A Genie space (note the space ID from the URL: `.../genie/spaces/<SPACE_ID>`).
- A chat model serving endpoint (default `databricks-claude-sonnet-4-6`).
- `CREATE FUNCTION` on the target `{BASE_CATALOG}.{BASE_SCHEMA}` (for the UDF and the registered model).
- Compute with internet egress to `mcp.exa.ai` (serverless works out of the box).
- Python 3.10–3.12 and `uv` (`brew install uv` or `curl -Lsf https://astral.sh/uv/install.sh | sh`).

## Auth (opinionated, PAT-free)

Three environments, one rule: **the code never branches on auth type.** It
relies on the Databricks SDK's default credential resolution. Personal access
tokens are **not** supported — `DATABRICKS_TOKEN` appears nowhere in the code,
env template, or docs.

| Environment | Identity | How it's supplied |
| --- | --- | --- |
| Local dev | Your user (SSO) | `databricks auth login` + `DATABRICKS_PROFILE=DEFAULT` |
| CI / prod jobs | Service principal | `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` injected by the environment |
| Deployed endpoint | Ambient runtime identity | Nothing to set; the endpoint's identity + declared resources |

The deployed agent's access to the Genie space, the `web_search` function, and
the LLM endpoint comes from **declared resources** (`DatabricksGenieSpace`,
`DatabricksFunction`, `DatabricksServingEndpoint`) attached at log time in
`driver.py`. That's what lets the Review App and Playground call the agent
on-behalf-of the signed-in reviewer.

## Usage

### Local dev loop

```bash
# 1. Install dependencies (uses the Databricks PyPI proxy on this network — see Troubleshooting)
uv sync

# 2. Run tests (no workspace required — everything is mocked)
uv run pytest

# 3. Configure
cp .env.template .env
# edit .env: set GENIE_SPACE_ID and BASE_CATALOG
# (BASE_SCHEMA is prefilled; LLM_ENDPOINT has a default)

# 4. Make sure you're logged in (one-time SSO)
databricks auth login --profile DEFAULT

# 5. Ask a question — the dispatcher routes it for you
uv run python agent.py "What were total sales last quarter?"      # → Genie
uv run python agent.py "What's the latest news about Databricks?" # → web_search
```

> Before you can route to `web_search` locally, the UDF must exist in
> `{BASE_CATALOG}.{BASE_SCHEMA}`. Register it once with the bundle job
> (below), or paste `register_web_search.py` into a notebook cell with
> `catalog`/`schema` widgets.

### Bundle workflow

The bundle defines one job, `deploy_dispatcher`, that creates the target
schema if needed (dev/test schemas are per-user/per-PR and won't pre-exist),
registers the UDF, logs and registers the agent model to UC, and (in `prod`)
deploys the serving endpoint. There is no schedule — it's deploy-on-demand
via `databricks bundle run`.

```bash
# Validate (read-only, safe) — all three targets must pass
databricks bundle validate -t dev
databricks bundle validate -t test --var pr_number=1
databricks bundle validate -t prod

# Deploy + run against your dev sandbox
databricks bundle deploy -t dev \
  --var base_catalog=<catalog> --var genie_space_id=<space-id>
databricks bundle run deploy_dispatcher -t dev \
  --var base_catalog=<catalog> --var genie_space_id=<space-id>

# Production (also creates/refreshes the scale-to-zero serving endpoint).
# genie_tables: comma-separated FQNs of the tables your Genie space queries —
# the deployed endpoint's credential is scoped to declared resources only,
# so without this the Genie route fails with table-level PermissionDenied.
databricks bundle deploy -t prod --var base_catalog=<catalog> --var genie_space_id=<space-id> \
  --var genie_tables=<catalog>.<schema>.<table>[,...]
databricks bundle run deploy_dispatcher -t prod --var base_catalog=<catalog> --var genie_space_id=<space-id> \
  --var genie_tables=<catalog>.<schema>.<table>[,...]
```

**Targets** (same env-isolation pattern as `databricks_gsheets_reader`):

| Target | Mode | Schema | `deploy_endpoint` | Notes |
| --- | --- | --- | --- | --- |
| `dev` (default) | development | `${base_schema}_<short_name>` | false | Your personal sandbox |
| `test` | development | `${base_schema}_<pr_number>` | false | Ephemeral per-PR; dev mode keeps schedules/guardrails off |
| `prod` | production | `${base_schema}` | true | Clean schema, deploys the endpoint |

**How the deployed endpoint authenticates** (learned the hard way, live): the
serving container's credential is scoped to *exactly* the resources declared
at `log_model` time — the Genie space, the SQL warehouse Genie executes on
(auto-derived from the space, no config needed), the `web_search` function,
the LLM endpoint, and the Genie space's tables (`genie_tables`). UC grants to
the serving principal are **not** a substitute; they are silently ineffective
for this scoped credential. If Genie answers with a permissions apology,
declare the missing table and redeploy.

The MLflow experiment path follows the same suffix convention:
`/Users/<you>/simple_dispatcher_agent_dev_<short_name>` in dev,
`..._test_<pr_number>` in test, and
`/Workspace/deployments/simple_dispatcher_agent/simple_dispatcher_agent_prod` in prod.

## Configuration

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| `DATABRICKS_PROFILE` | local dev | `DEFAULT` | CLI profile for user SSO |
| `GENIE_SPACE_ID` | yes | — | Genie space ID (from the space URL) |
| `BASE_CATALOG` | yes | — | UC catalog for the UDF + registered model |
| `BASE_SCHEMA` | yes | — (`.env.template` prefills `simple_dispatcher`) | UC schema for the UDF + registered model |
| `LLM_ENDPOINT` | no | `databricks-claude-sonnet-4-6` | Chat model serving endpoint |
| `MLFLOW_EXPERIMENT` | no | unset (traces go to the active MLflow experiment; the bundle job sets a per-target path) | MLflow experiment path for traces |

A missing required variable raises a `ValueError` that names every one that's
missing.

## Viewing traces

`agent.py` calls `mlflow.langchain.autolog()`, so each run produces one trace
covering the dispatcher decision and the tool span.

```bash
uv run mlflow ui   # local tracking at http://localhost:5000
```

If your workspace is the MLflow tracking server (`MLFLOW_TRACKING_URI=databricks`),
traces appear in the Databricks Experiments UI under your experiment path. In
that case set `MLFLOW_EXPERIMENT` to a path you can write to (e.g.
`/Users/<you@example.com>/simple_dispatcher_agent`) — a root-level path only
works with a local tracking server.

Traces are **UC-backed**: when an experiment is set with a UC namespace
(`setup_tracing` locally, the deploy job in the bundle), its traces are stored
in Delta tables in `{BASE_CATALOG}.{BASE_SCHEMA}` (table prefix = experiment
ID) rather than workspace-managed MLflow storage — no 100k-trace cap,
UC governance, and queryable from SQL/notebooks/dashboards.

One platform constraint: a UC trace destination can only be linked to an
experiment that has **no traces yet**. If the experiment already accumulated
workspace-managed traces, the agent prints a note and keeps using it
unlinked — switch to a fresh experiment name to get UC-backed storage.

## Adding a third tool

The whole point of a dispatcher is that the next tool is just one more entry in
the `tools` list. Say you stand up a **Knowledge Assistant** endpoint for your
unstructured docs. Adding it looks like:

```python
def make_knowledge_assistant_tool(endpoint_name):
    llm = ChatDatabricks(endpoint=endpoint_name)

    @tool
    def ask_docs(question: str) -> str:
        """Answer questions from the company's documents and knowledge base.

        Use for policies, runbooks, and written guidance — not metrics
        (use ask_genie) and not fresh public facts (use web_search).
        """
        return llm.invoke(question).content

    return ask_docs

# then, in build_dispatcher_from_config:
tools.append(make_knowledge_assistant_tool(cfg["ka_endpoint"]))
```

That's the entire change — a new seam function and one `append`. The router
picks it up from the docstring.

> Why isn't a Knowledge Assistant wired in by default? **DABs can't provision a
> Knowledge Assistant** (there's no bundle resource for it — you create it in
> the UI/API and point at its serving endpoint). So it stays an extension point
> rather than a requirement. When you have a KA endpoint, dropping it in is the
> snippet above.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `ValueError: Missing required environment variable(s): ...` | Set the named vars (or add them to `.env`). |
| `401: Credential was not sent or was of an unsupported type` + a warning that two profiles match the same host | Multiple `~/.databrickscfg` profiles point at one workspace; disambiguate with `export DATABRICKS_PROFILE=<profile>`. |
| Experiment creation fails on a Databricks-hosted tracking server | Set `MLFLOW_EXPERIMENT` to a writable workspace path like `/Users/<you@example.com>/simple_dispatcher_agent`; a root-level path only works locally. |
| `uv sync` can't reach `pypi.org` | On Databricks-networked machines, resolve through the proxy: `export UV_INDEX_URL=https://pypi-proxy.dev.databricks.com/simple/`. |
| `web_search(...)` errors after registration | The compute needs internet egress to `mcp.exa.ai`. Use serverless or allow outbound HTTPS to that host. |
| `RESOURCE_DOES_NOT_EXIST` for the function | The UDF isn't registered in `{BASE_CATALOG}.{BASE_SCHEMA}` yet. Run the bundle job (or `register_web_search.py` in a notebook). |
| `ImportError: cannot import name 'ExecutionInfo' from 'langgraph.runtime'` | A stale resolve pulled `langchain` 1.2.x + `langgraph` 1.0.x. The pin is `langchain>=1.3`; rerun `uv sync` to pick a matching `langgraph<1.3`. |
| Deployed endpoint: `not authorized to use or monitor this SQL Endpoint` | Old model version logged before the warehouse was a declared resource. Rerun the deploy job — the driver derives the warehouse from the Genie space automatically. |
| Deployed endpoint: Genie apologizes about missing `SELECT`/`USE` on a table | The serving credential only reaches declared resources. Add the table(s) to `--var genie_tables=...` and rerun the deploy job. UC grants to the serving principal won't help. |
| `databricks serving-endpoints query` returns only `{"id": ..., "object": "response"}` | CLI display quirk: it drops the ResponsesAgent `output` field. Query via `databricks api post /serving-endpoints/<name>/invocations --json '...'` to see the full response. |

## Engineering Notes

**`uv.lock` is gitignored.** `uv sync` resolves against the Databricks internal
PyPI proxy (`pypi-proxy.dev.databricks.com`), which isn't reachable outside the
Databricks corporate network. Committing the lockfile would pin those URLs and
break installs for external users. Regenerate it locally with `uv sync`.

**Python 3.10–3.12.** `databricks-agents` pulls transitive deps (`whenever`)
that only ship binary wheels for the Databricks runtime interpreters; 3.13+
falls back to building from sdist and needs a Rust toolchain. The `pyproject`
caps `requires-python` accordingly.

**How the deployed model gets its code and config.** `driver.py` logs
`serving_agent.py` with `code_paths=["agent.py"]` — serving imports `agent`,
and without it the container fails with `ModuleNotFoundError`
(`tests/test_agent.py::TestModelPackaging` guards this by loading the logged
model from a clean cwd). The agent reads `GENIE_SPACE_ID` / `BASE_CATALOG` /
`BASE_SCHEMA` / `LLM_ENDPOINT` from the environment, so the job sets them
before `log_model` (mlflow's log-time validation runs one real `predict`,
failing a broken config at deploy time instead of at the first user request)
and passes the same dict as `environment_vars` to `agents.deploy` so the
serving container can rebuild the dispatcher.

**Why `databricks.agents.deploy(...)` instead of a DABs
`model_serving_endpoints` resource.** Two reasons:

1. *First-deploy chicken-and-egg.* A DABs serving-endpoint resource needs a
   model version to point at, but the model version is produced by this same
   job's `log_model` + `register_model` step. The endpoint can't be declared
   before the version it serves exists.
2. *Auth passthrough + Review App.* `agents.deploy` wires up the on-behalf-of
   credential passthrough from the declared resources and provisions the Review
   App scaffolding. A plain serving-endpoint resource doesn't do that setup.

So the job logs the model with its resources and calls `agents.deploy(...,
scale_to_zero_enabled=True)` itself.

**The `web_search` UDF authoring constraints** are inherited from
`databricks/demos/genie_web_search`: the `CREATE FUNCTION` SQL is built with
plain string concatenation (the UDF body contains `{}` that would clash with
f-string/`.format` placeholders), the body is a raw triple-quoted string so
`\n` escapes survive, and `NUM_RESULTS` is injected by concatenation. See that
demo's README for the full rationale.
