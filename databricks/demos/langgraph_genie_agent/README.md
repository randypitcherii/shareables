# LangGraph Genie Agent

A LangGraph ReAct-style agent that answers user questions by calling a
Databricks Genie space as a tool. The agent is backed by a Databricks model
serving endpoint (any chat model supported by the endpoint) and produces
MLflow 3 traces for every run.

The demo is intentionally minimal: one file (`agent.py`), standard Databricks
SDK auth, no secrets on disk.

## Files

| File | Purpose |
| --- | --- |
| `agent.py` | Agent core — config, graph builder, Genie tool wrapper, MLflow setup, CLI entry point |
| `tests/test_agent.py` | Pytest test suite; runs without a live workspace (LLM and Genie are mocked) |
| `pyproject.toml` | uv-managed project; all commands via `uv run` |
| `.gitignore` | Excludes `uv.lock`, `.venv/`, and MLflow local artifacts |
| `README.md` | This file |

## Requirements

- Databricks workspace with Unity Catalog enabled.
- A Genie space (note the space ID from the URL: `.../genie/spaces/<SPACE_ID>`).
- A Databricks model serving endpoint that accepts chat messages (e.g. `databricks-claude-sonnet-4-5`, `databricks-meta-llama-3-3-70b-instruct`).
- Databricks authentication — any of:
  - `DATABRICKS_HOST` + `DATABRICKS_TOKEN` environment variables, or
  - a configured Databricks CLI profile (`databricks auth login`).
- Python ≥ 3.10.
- `uv` — install via `brew install uv` or `curl -Lsf https://astral.sh/uv/install.sh | sh`.

## Usage

```bash
# 1. Install dependencies
uv sync

# 2. Run tests (no workspace required — everything is mocked)
uv run pytest

# 3. Set required config
export GENIE_SPACE_ID="<your-genie-space-id>"
export LLM_ENDPOINT="databricks-claude-sonnet-4-5"

# 4. (Optional) Databricks auth if not already configured
export DATABRICKS_HOST="https://<your-workspace>.azuredatabricks.net"
export DATABRICKS_TOKEN="<your-pat>"

# 5. Ask a question
uv run python agent.py "What were total sales last quarter?"
```

The agent prints the final answer and notes where to find the MLflow trace.

## Configuration

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| `GENIE_SPACE_ID` | yes | — | Databricks Genie space ID (from the space URL) |
| `LLM_ENDPOINT` | yes | — | Databricks model serving endpoint name |
| `MLFLOW_EXPERIMENT` | no | `/langgraph-genie-agent` | MLflow experiment path for traces |
| `DATABRICKS_HOST` | no* | SDK default | Workspace URL (`https://...`) |
| `DATABRICKS_TOKEN` | no* | SDK default | Personal access token |

\* Required unless a Databricks CLI profile is already configured.

A missing `GENIE_SPACE_ID` or `LLM_ENDPOINT` raises a `ValueError` that names
each missing variable explicitly.

## Viewing Traces in the MLflow UI

After a run, open the MLflow UI:

```bash
uv run mlflow ui          # local tracking at http://localhost:5000
```

Navigate to the `/langgraph-genie-agent` experiment (or the name you set in
`MLFLOW_EXPERIMENT`) and open the latest run. Each invocation produces one
trace that shows the assistant and Genie tool spans.

If your workspace is configured as the MLflow tracking server
(`MLFLOW_TRACKING_URI=databricks`), traces appear directly in the Databricks
Experiments UI under the experiment path.

## How It Works

```
User question
      │
      ▼
[assistant node] ← LLM (ChatDatabricks) bound to the ask_genie tool
      │
      ├─ tool_calls present ──► [genie_tool node] ── calls Genie space ──┐
      │                                                                   │
      └─ no tool_calls ──────────────────────────────────────────────────┘
                                                                          │
                                                                         ▼
                                                                   [assistant node]
                                                                          │
                                                                   plain answer
                                                                          │
                                                                         END
```

1. The user question is wrapped in a `HumanMessage` and fed into the graph.
2. The `assistant` node invokes the LLM. If the LLM decides it needs data, it
   emits a tool call to `ask_genie`.
3. The `genie_tool` node extracts the question from the tool call args and
   calls `genie_fn(question)`, which calls `Genie.ask_question` from
   `databricks_ai_bridge`.
4. The Genie response is added to the message list as a `ToolMessage`, and
   control returns to the `assistant` node.
5. The LLM synthesises a final answer from the Genie data.
6. MLflow `langchain.autolog()` records the entire run as a trace.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `ValueError: Missing required environment variable(s): GENIE_SPACE_ID` | Set `export GENIE_SPACE_ID="<id>"` |
| `ValueError: Missing required environment variable(s): LLM_ENDPOINT` | Set `export LLM_ENDPOINT="<endpoint>"` |
| `databricks.sdk.errors.PermissionDenied` | Check that your PAT or CLI profile has access to the Genie space |
| `RESOURCE_DOES_NOT_EXIST` from the serving endpoint | Verify `LLM_ENDPOINT` matches the exact name in your workspace |
| `uv sync` fails to reach pypi.org | On Databricks-networked machines, set `UV_INDEX_URL=https://pypi-proxy.dev.databricks.com/simple/` |
| Empty or truncated Genie answer | The Genie space may need more context; try a more specific question |
| MLflow traces not appearing | Check `MLFLOW_TRACKING_URI` points to your server; confirm experiment exists |

## Natural Next Steps

- **Model Serving deployment** — wrap `build_agent` in an MLflow `pyfunc` model
  and log it with `mlflow.langchain.log_model()`, then deploy via Databricks
  Model Serving for a REST API endpoint.
- **MLflow evaluation** — use `mlflow.evaluate()` with a question/answer dataset
  to score the agent's accuracy against ground-truth Genie responses.

## Engineering Notes

`uv.lock` is gitignored in this directory because `uv sync` resolves against
the Databricks internal PyPI proxy (`pypi-proxy.dev.databricks.com`), which is
unreachable outside the Databricks corporate network. Committing a lockfile
with those URLs would break installs for all external users. Run `uv sync` to
regenerate the lockfile in your own environment.

The Genie integration uses `Genie` from `databricks_ai_bridge.genie` (re-used
internally by `databricks_langchain.GenieAgent`). The agent wraps it in a
plain callable (`genie_fn`) rather than using `GenieAgent` directly, which
makes the seam easy to mock in tests without patching deep library internals.
`GenieAgent` returns a `RunnableLambda` intended for LangChain LCEL pipelines;
for a ReAct tool loop, a `@tool`-decorated wrapper over a plain callable is
the right approach.
