"""
A simple dispatcher agent.

You built a Genie space. People love it. Now they're asking questions Genie
alone can't answer — fresh public facts that live on the internet, not in your
tables. This agent routes each question to the right tool:

  - your existing Genie space (structured org data), or
  - a `web_search` Unity Catalog UDF (fresh public/internet information).

The dispatcher itself is ~10 lines of LangChain `create_agent` (see the marked
section below). That small footprint is the whole pitch: graduating from a
Genie space to a multi-tool agent costs almost no code.

Usage:
    uv run python agent.py "What were total sales last quarter?"
    uv run python agent.py "What's the latest news about Databricks?"

Configuration (environment variables, or a local .env):
    GENIE_SPACE_ID    (required) Databricks Genie space ID
    BASE_CATALOG      (required) UC catalog hosting the web_search UDF
    BASE_SCHEMA       (required) UC schema hosting the web_search UDF
    LLM_ENDPOINT      (optional) chat model serving endpoint
                      Default: databricks-claude-sonnet-4-6
    MLFLOW_EXPERIMENT (optional) MLflow experiment path for traces

Auth is PAT-free and never branched on in code: local dev uses your SSO CLI
profile (DATABRICKS_PROFILE), CI/prod use injected service-principal creds,
and deployed code uses the ambient runtime identity. All of it resolves through
the Databricks SDK's default credential chain.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import mlflow
from databricks_langchain import ChatDatabricks, DatabricksFunctionClient
from databricks_ai_bridge.genie import Genie
from langchain.agents import create_agent
from langchain_core.tools import tool

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_LLM_ENDPOINT = "databricks-claude-sonnet-4-6"
WEB_SEARCH_FUNCTION = "web_search"

SYSTEM_PROMPT = (
    "You are a dispatcher. Answer the user's question by routing it to exactly "
    "one of your tools, then summarize the result.\n"
    "- Use `ask_genie` for questions about YOUR organization's structured data "
    "(metrics, KPIs, tables, anything that lives in the Genie space).\n"
    "- Use `web_search` for fresh public/internet information that would not be "
    "in an internal database or in your training data.\n"
    "If a question needs both, call the tools in turn. If neither applies, "
    "answer directly. Always ground your final answer in the tool output."
)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def _load_dotenv_if_present() -> None:
    """Minimal stdlib .env loader: KEY=VALUE lines, no overriding of real env.

    Kept tiny on purpose — avoids a python-dotenv dependency for a demo.
    """
    env_path = Path.cwd() / ".env"
    if not env_path.is_file():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def load_config() -> dict[str, str]:
    """Load and validate configuration from the environment (and a local .env).

    Returns a dict with: genie_space_id, base_catalog, base_schema,
    llm_endpoint, mlflow_experiment.

    Raises ValueError naming every missing required variable.
    """
    _load_dotenv_if_present()

    required = {
        "GENIE_SPACE_ID": os.environ.get("GENIE_SPACE_ID", "").strip(),
        "BASE_CATALOG": os.environ.get("BASE_CATALOG", "").strip(),
        "BASE_SCHEMA": os.environ.get("BASE_SCHEMA", "").strip(),
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ValueError(
            "Missing required environment variable(s): "
            + ", ".join(missing)
            + ". Set them (or add them to a local .env) before running the agent."
        )

    return {
        "genie_space_id": required["GENIE_SPACE_ID"],
        "base_catalog": required["BASE_CATALOG"],
        "base_schema": required["BASE_SCHEMA"],
        "llm_endpoint": os.environ.get("LLM_ENDPOINT", "").strip() or DEFAULT_LLM_ENDPOINT,
        "mlflow_experiment": os.environ.get("MLFLOW_EXPERIMENT", "").strip(),
    }


# ---------------------------------------------------------------------------
# MLflow tracing
# ---------------------------------------------------------------------------


def setup_tracing(
    experiment_name: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
) -> None:
    """Enable MLflow LangChain autologging and (optionally) set the experiment.

    When a UC namespace is provided alongside the experiment, the experiment's
    traces are stored UC-backed (Delta tables in {catalog}.{schema}) instead of
    workspace-managed MLflow storage — unlimited retention, governable,
    queryable from SQL.
    """
    mlflow.langchain.autolog()
    if experiment_name:
        kwargs: dict[str, Any] = {}
        if catalog and schema:
            from mlflow.entities.trace_location import UnityCatalog

            kwargs["trace_location"] = UnityCatalog(
                catalog_name=catalog, schema_name=schema
            )
        mlflow.set_experiment(experiment_name, **kwargs)


# ---------------------------------------------------------------------------
# Tool seams (each one wraps a single external dependency, easy to mock)
# ---------------------------------------------------------------------------


def make_genie_tool(space_id: str):
    """A @tool that asks a Databricks Genie space a natural-language question.

    This is the seam tests mock: all Genie I/O lives behind `Genie`.
    """
    genie = Genie(space_id)

    @tool
    def ask_genie(question: str) -> str:
        """Answer a question about YOUR organization's structured data.

        Use this for metrics, KPIs, and anything that lives in the company's
        Genie space (tables, dashboards, internal numbers). Not for fresh
        public/internet facts.

        Args:
            question: The natural-language question to ask the Genie space.
        """
        response = genie.ask_question(question)
        result = response.result if response.result is not None else ""
        if hasattr(result, "to_markdown"):  # pandas DataFrame
            return result.to_markdown(index=False)
        return str(result)

    return ask_genie


def make_web_search_tool(catalog: str, schema: str):
    """A @tool that runs the {catalog}.{schema}.web_search(query) UC function.

    Execution uses DatabricksFunctionClient in its default serverless mode, so
    no SQL warehouse needs to be configured. This is the seam tests mock.
    """
    client = DatabricksFunctionClient()
    fq_name = f"{catalog}.{schema}.{WEB_SEARCH_FUNCTION}"

    @tool
    def web_search(query: str) -> str:
        """Search the public internet for fresh information.

        Use this for current events, recent public facts, or anything that
        would not be in an internal database or in your training data. Not for
        the organization's private/structured data.

        Args:
            query: The natural-language web search query.
        """
        result = client.execute_function(fq_name, {"query": query})
        if getattr(result, "error", None):
            return f"web_search error: {result.error}"
        return str(result.value)

    return web_search


# ===========================================================================
# THIS IS THE WHOLE AGENT.
# A dispatcher is just create_agent over the tools you already have.
# ===========================================================================
def build_dispatcher(llm: Any, tools: list) -> Any:
    """Build the dispatcher: a LangChain create_agent over the given tools."""
    return create_agent(llm, tools, system_prompt=SYSTEM_PROMPT)


def build_dispatcher_from_config(cfg: dict[str, str]) -> Any:
    """Assemble the LLM + both tools + dispatcher from a loaded config dict."""
    llm = ChatDatabricks(endpoint=cfg["llm_endpoint"])
    tools = [
        make_genie_tool(cfg["genie_space_id"]),
        make_web_search_tool(cfg["base_catalog"], cfg["base_schema"]),
    ]
    return build_dispatcher(llm, tools)


# ===========================================================================


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main(question: str) -> None:
    cfg = load_config()
    setup_tracing(
        cfg["mlflow_experiment"] or None, cfg["base_catalog"], cfg["base_schema"]
    )

    dispatcher = build_dispatcher_from_config(cfg)
    result = dispatcher.invoke({"messages": [{"role": "user", "content": question}]})
    answer = result["messages"][-1].content

    print(f"\nAnswer: {answer}\n")
    if cfg["mlflow_experiment"]:
        print(
            f"MLflow trace logged to experiment '{cfg['mlflow_experiment']}' "
            f"(tracking URI: {mlflow.get_tracking_uri()})."
        )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: uv run python agent.py "<your question>"')
        sys.exit(1)
    main(sys.argv[1])
