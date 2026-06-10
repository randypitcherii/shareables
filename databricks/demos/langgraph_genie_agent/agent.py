"""
LangGraph ReAct agent that answers user questions by calling a Databricks
Genie space as a tool, backed by a Databricks-hosted LLM serving endpoint,
with MLflow 3 tracing enabled.

Usage:
    uv run python agent.py "What were total sales last quarter?"

Configuration (environment variables):
    GENIE_SPACE_ID       (required) Databricks Genie space ID
    LLM_ENDPOINT         (required) Databricks model serving endpoint name
                         e.g. "databricks-claude-sonnet-4-5"
    MLFLOW_EXPERIMENT    (optional) MLflow experiment name/path
                         Default: "/langgraph-genie-agent"

Databricks auth is handled implicitly via the SDK (env vars or CLI profile).
"""

from __future__ import annotations

import os
import sys
from typing import Any, Callable, Optional

import mlflow
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import MessagesState

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GENIE_TOOL_NAME = "ask_genie"

_DEFAULT_EXPERIMENT = "/langgraph-genie-agent"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def load_config() -> dict[str, str]:
    """
    Load and validate required configuration from environment variables.

    Returns a dict with keys:
        genie_space_id   - Databricks Genie space ID
        llm_endpoint     - Databricks model serving endpoint name
        mlflow_experiment - MLflow experiment path (with default)

    Raises:
        ValueError listing all missing required env vars.
    """
    missing = []
    genie_space_id = os.environ.get("GENIE_SPACE_ID", "")
    llm_endpoint = os.environ.get("LLM_ENDPOINT", "")

    if not genie_space_id:
        missing.append("GENIE_SPACE_ID")
    if not llm_endpoint:
        missing.append("LLM_ENDPOINT")

    if missing:
        raise ValueError(
            f"Missing required environment variable(s): {', '.join(missing)}. "
            "Set them before running the agent."
        )

    return {
        "genie_space_id": genie_space_id,
        "llm_endpoint": llm_endpoint,
        "mlflow_experiment": os.environ.get("MLFLOW_EXPERIMENT", _DEFAULT_EXPERIMENT),
    }


# ---------------------------------------------------------------------------
# MLflow tracing setup
# ---------------------------------------------------------------------------


def setup_tracing(experiment_name: Optional[str] = None) -> None:
    """
    Enable MLflow LangChain autologging and set the active experiment.

    Args:
        experiment_name: MLflow experiment name/path. Defaults to the
            MLFLOW_EXPERIMENT env var or "/langgraph-genie-agent".
    """
    if experiment_name is None:
        experiment_name = os.environ.get("MLFLOW_EXPERIMENT", _DEFAULT_EXPERIMENT)

    mlflow.langchain.autolog()
    mlflow.set_experiment(experiment_name)


# ---------------------------------------------------------------------------
# Genie tool factory
# ---------------------------------------------------------------------------


def make_genie_fn(genie_space_id: str) -> Callable[[str], str]:
    """
    Return a callable that asks a Databricks Genie space a question and
    returns the answer as a string.

    This is the seam that tests mock — all Genie I/O is isolated here.
    The Genie space ID is captured in a closure so callers only pass the
    question string.

    Args:
        genie_space_id: Databricks Genie space ID.

    Returns:
        A callable: (question: str) -> str
    """
    from databricks_ai_bridge.genie import Genie

    genie = Genie(genie_space_id)

    def _ask(question: str) -> str:
        response = genie.ask_question(question)
        # response.result may be a string or None; coerce to str
        result = response.result if response.result is not None else ""
        if hasattr(result, "to_markdown"):  # pandas DataFrame
            return result.to_markdown(index=False)
        return str(result)

    return _ask


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------


def build_agent(
    llm: Any,
    genie_fn: Callable[[str], str],
) -> Any:
    """
    Build and compile a LangGraph ReAct graph.

    Nodes:
      - assistant: calls the LLM (bound to the Genie tool)
      - genie_tool: executes the Genie tool call

    Edges:
      START → assistant
      assistant → genie_tool  (when the last message has tool calls)
      assistant → END          (when the last message has no tool calls)
      genie_tool → assistant

    Args:
        llm: A LangChain chat model (or mock). Will have bind_tools called on it.
        genie_fn: Callable (question: str) -> str. Wraps the Genie API.

    Returns:
        A compiled LangGraph StateGraph.
    """

    # Build a LangChain @tool around the genie_fn callable so it can be bound
    # to the LLM and discovered by the tool node.
    @tool(GENIE_TOOL_NAME)
    def ask_genie(question: str) -> str:
        """
        Ask a Databricks Genie space a natural-language question about your data.
        Use this tool whenever the user needs data-driven answers.

        Args:
            question: The natural-language question to ask.

        Returns:
            The answer from Genie (plain text or markdown table).
        """
        return genie_fn(question)

    tools = [ask_genie]
    llm_with_tools = llm.bind_tools(tools)

    # --- node functions -----------------------------------------------------

    def assistant_node(state: MessagesState) -> dict:
        response = llm_with_tools.invoke(state["messages"])
        return {"messages": [response]}

    def genie_tool_node(state: MessagesState) -> dict:
        """Execute any pending tool calls in the last message."""
        last_message = state["messages"][-1]
        tool_messages = []

        for tc in last_message.tool_calls:
            if tc["name"] == GENIE_TOOL_NAME:
                question = tc["args"].get("question", "")
                result = genie_fn(question)
                tool_messages.append(
                    ToolMessage(content=result, tool_call_id=tc["id"])
                )

        return {"messages": tool_messages}

    def should_continue(state: MessagesState) -> str:
        """Route to 'genie_tool' if the last message has tool calls; else END."""
        last_message = state["messages"][-1]
        if isinstance(last_message, AIMessage) and last_message.tool_calls:
            return "genie_tool"
        return END

    # --- graph assembly -----------------------------------------------------

    graph = StateGraph(MessagesState)
    graph.add_node("assistant", assistant_node)
    graph.add_node("genie_tool", genie_tool_node)

    graph.add_edge(START, "assistant")
    graph.add_conditional_edges("assistant", should_continue)
    graph.add_edge("genie_tool", "assistant")

    return graph.compile()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(question: str) -> None:
    """Ask one question and print the answer."""
    cfg = load_config()
    setup_tracing(cfg["mlflow_experiment"])

    from databricks_langchain import ChatDatabricks

    llm = ChatDatabricks(endpoint=cfg["llm_endpoint"])
    genie_fn = make_genie_fn(cfg["genie_space_id"])

    graph = build_agent(llm=llm, genie_fn=genie_fn)

    from langchain_core.messages import HumanMessage

    result = graph.invoke({"messages": [HumanMessage(content=question)]})
    last_message = result["messages"][-1]
    answer = last_message.content

    print(f"\nAnswer: {answer}\n")

    # Surface the MLflow run/trace URL if available
    active_run = mlflow.active_run()
    if active_run:
        tracking_uri = mlflow.get_tracking_uri()
        run_id = active_run.info.run_id
        print(f"MLflow run: {tracking_uri}/#/experiments/.../runs/{run_id}")
    else:
        print(
            "MLflow tracing is enabled. View traces in the MLflow UI at your "
            "tracking server or Databricks workspace under Experiments."
        )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: uv run python agent.py \"<your question>\"")
        sys.exit(1)
    main(sys.argv[1])
