"""
Tests for langgraph_genie_agent.

All tests run without a live Databricks workspace.
LLM and Genie are mocked at the seams defined in agent.py.
"""

import pytest
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# Config validation tests
# ---------------------------------------------------------------------------

class TestConfigValidation:
    def test_missing_genie_space_id_raises_with_var_name(self, monkeypatch):
        """load_config raises ValueError naming GENIE_SPACE_ID when it is absent."""
        monkeypatch.delenv("GENIE_SPACE_ID", raising=False)
        monkeypatch.setenv("LLM_ENDPOINT", "databricks-claude-sonnet-4-5")

        from agent import load_config
        with pytest.raises(ValueError, match="GENIE_SPACE_ID"):
            load_config()

    def test_missing_llm_endpoint_raises_with_var_name(self, monkeypatch):
        """load_config raises ValueError naming LLM_ENDPOINT when it is absent."""
        monkeypatch.setenv("GENIE_SPACE_ID", "space-abc")
        monkeypatch.delenv("LLM_ENDPOINT", raising=False)

        from agent import load_config
        with pytest.raises(ValueError, match="LLM_ENDPOINT"):
            load_config()

    def test_both_missing_raises_with_both_var_names(self, monkeypatch):
        """load_config raises ValueError listing both missing vars."""
        monkeypatch.delenv("GENIE_SPACE_ID", raising=False)
        monkeypatch.delenv("LLM_ENDPOINT", raising=False)

        from agent import load_config
        with pytest.raises(ValueError) as exc:
            load_config()
        msg = str(exc.value)
        assert "GENIE_SPACE_ID" in msg
        assert "LLM_ENDPOINT" in msg

    def test_valid_config_returns_dict(self, monkeypatch):
        """load_config returns a dict with genie_space_id and llm_endpoint when both vars are set."""
        monkeypatch.setenv("GENIE_SPACE_ID", "space-xyz")
        monkeypatch.setenv("LLM_ENDPOINT", "databricks-claude-sonnet-4-5")

        from agent import load_config
        cfg = load_config()
        assert cfg["genie_space_id"] == "space-xyz"
        assert cfg["llm_endpoint"] == "databricks-claude-sonnet-4-5"


# ---------------------------------------------------------------------------
# Graph construction tests
# ---------------------------------------------------------------------------

class TestGraphConstruction:
    """build_agent returns a compiled LangGraph with expected nodes and edges."""

    def _make_fake_llm(self):
        """Return a minimal fake ChatModel (not bound to any real endpoint)."""
        from langchain_core.messages import AIMessage
        fake_llm = MagicMock()
        fake_llm.bind_tools = MagicMock(return_value=fake_llm)
        # Default: return a plain text answer with no tool calls
        fake_llm.invoke = MagicMock(
            return_value=AIMessage(content="42 is the answer.")
        )
        return fake_llm

    def test_graph_has_expected_nodes(self):
        """build_agent graph contains 'assistant' and 'genie_tool' nodes."""
        fake_llm = self._make_fake_llm()
        fake_genie_fn = MagicMock(return_value="some data")

        from agent import build_agent
        graph = build_agent(llm=fake_llm, genie_fn=fake_genie_fn)

        node_names = set(graph.nodes.keys())
        assert "assistant" in node_names, f"Expected 'assistant' node, got {node_names}"
        assert "genie_tool" in node_names, f"Expected 'genie_tool' node, got {node_names}"

    def test_graph_compiles_without_error(self):
        """build_agent returns a compiled graph (StateGraph.compile succeeds)."""
        fake_llm = self._make_fake_llm()
        fake_genie_fn = MagicMock(return_value="some data")

        from agent import build_agent
        graph = build_agent(llm=fake_llm, genie_fn=fake_genie_fn)
        # A compiled graph has an 'invoke' method
        assert callable(getattr(graph, "invoke", None))


# ---------------------------------------------------------------------------
# Full-path routing tests
# ---------------------------------------------------------------------------

class TestGraphRouting:
    """End-to-end routing through the compiled graph using fake LLM and Genie."""

    def test_tool_call_path_calls_genie_and_returns_final_answer(self):
        """
        When the LLM first responds with a tool_call to the Genie tool and then
        responds with a final plain answer, the compiled graph:
          - calls the Genie function with a string that includes the user question, and
          - returns a final state whose last message content contains the final answer.
        """
        from langchain_core.messages import AIMessage, HumanMessage
        from langchain_core.messages.tool import ToolCall

        from agent import GENIE_TOOL_NAME

        user_question = "What were total sales last quarter?"
        genie_result = "Sales were $1.2M last quarter."
        final_answer = "Based on Genie data, sales were $1.2M last quarter."

        # First LLM call: returns a tool call
        tool_call = ToolCall(
            name=GENIE_TOOL_NAME,
            args={"question": user_question},
            id="call-001",
        )
        first_response = AIMessage(content="", tool_calls=[tool_call])
        # Second LLM call: returns a final plain answer
        second_response = AIMessage(content=final_answer)

        fake_llm = MagicMock()
        fake_llm.bind_tools = MagicMock(return_value=fake_llm)
        fake_llm.invoke = MagicMock(side_effect=[first_response, second_response])

        fake_genie_fn = MagicMock(return_value=genie_result)

        from agent import build_agent
        graph = build_agent(llm=fake_llm, genie_fn=fake_genie_fn)

        result = graph.invoke({"messages": [HumanMessage(content=user_question)]})

        # Genie was called
        assert fake_genie_fn.called, "Genie function was not called"
        # The argument passed to Genie contains the user question
        genie_call_arg = fake_genie_fn.call_args[0][0]
        assert user_question in genie_call_arg, (
            f"Expected user question in Genie call arg, got: {genie_call_arg!r}"
        )
        # Final state contains the final answer
        last_msg = result["messages"][-1]
        assert final_answer in last_msg.content, (
            f"Expected final answer in last message, got: {last_msg.content!r}"
        )

    def test_no_tool_call_path_goes_straight_to_end(self):
        """
        When the LLM responds with a plain answer (no tool calls), the graph
        goes straight to END without calling Genie.
        """
        from langchain_core.messages import AIMessage, HumanMessage

        final_answer = "The capital of France is Paris."

        fake_llm = MagicMock()
        fake_llm.bind_tools = MagicMock(return_value=fake_llm)
        fake_llm.invoke = MagicMock(return_value=AIMessage(content=final_answer))

        fake_genie_fn = MagicMock(return_value="should not be called")

        from agent import build_agent
        graph = build_agent(llm=fake_llm, genie_fn=fake_genie_fn)

        result = graph.invoke({"messages": [HumanMessage(content="What is the capital of France?")]})

        assert not fake_genie_fn.called, "Genie function should not be called on no-tool path"
        last_msg = result["messages"][-1]
        assert final_answer in last_msg.content


# ---------------------------------------------------------------------------
# MLflow setup tests
# ---------------------------------------------------------------------------

class TestMlflowSetup:
    def test_setup_tracing_calls_autolog(self, monkeypatch):
        """setup_tracing calls mlflow.langchain.autolog()."""
        mock_mlflow = MagicMock()
        monkeypatch.setattr("agent.mlflow", mock_mlflow)

        from agent import setup_tracing
        setup_tracing()

        mock_mlflow.langchain.autolog.assert_called_once()

    def test_setup_tracing_sets_experiment(self, monkeypatch):
        """setup_tracing calls mlflow.set_experiment with the provided or default name."""
        mock_mlflow = MagicMock()
        monkeypatch.setattr("agent.mlflow", mock_mlflow)

        from agent import setup_tracing
        setup_tracing(experiment_name="/my-experiment")

        mock_mlflow.set_experiment.assert_called_once_with("/my-experiment")
