"""
Tests for simple_dispatcher_agent.

All tests run without a live Databricks workspace. The Genie space, the
web_search Unity Catalog function, and the chat model are mocked at the seams
defined in agent.py / serving_agent.py.
"""

from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# A fake chat model that create_agent can drive.
#
# GenericFakeChatModel.bind_tools() raises NotImplementedError, but
# langchain.agents.create_agent() calls bind_tools() on the model. So we
# subclass it and make bind_tools return self. The model emits the AIMessages
# in `messages` in order, one per LLM turn.
# ---------------------------------------------------------------------------

from langchain_core.language_models.fake_chat_models import GenericFakeChatModel


class ToolCallingFakeChatModel(GenericFakeChatModel):
    """GenericFakeChatModel that tolerates bind_tools (returns self)."""

    def bind_tools(self, tools, **kwargs):  # noqa: ANN001, ANN003
        return self


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestConfigValidation:
    def test_missing_genie_space_id_named(self, monkeypatch):
        monkeypatch.delenv("GENIE_SPACE_ID", raising=False)
        monkeypatch.setenv("LLM_ENDPOINT", "databricks-claude-sonnet-4-6")
        monkeypatch.setenv("BASE_CATALOG", "main")
        monkeypatch.setenv("BASE_SCHEMA", "simple_dispatcher")

        from agent import load_config

        with pytest.raises(ValueError, match="GENIE_SPACE_ID"):
            load_config()

    def test_missing_base_catalog_named(self, monkeypatch):
        monkeypatch.setenv("GENIE_SPACE_ID", "space-abc")
        monkeypatch.setenv("LLM_ENDPOINT", "databricks-claude-sonnet-4-6")
        monkeypatch.delenv("BASE_CATALOG", raising=False)
        monkeypatch.setenv("BASE_SCHEMA", "simple_dispatcher")

        from agent import load_config

        with pytest.raises(ValueError, match="BASE_CATALOG"):
            load_config()

    def test_all_missing_required_are_named(self, monkeypatch):
        for var in ("GENIE_SPACE_ID", "BASE_CATALOG", "BASE_SCHEMA"):
            monkeypatch.delenv(var, raising=False)
        # LLM_ENDPOINT has a default, so it is never "missing".
        monkeypatch.delenv("LLM_ENDPOINT", raising=False)

        from agent import load_config

        with pytest.raises(ValueError) as exc:
            load_config()
        msg = str(exc.value)
        assert "GENIE_SPACE_ID" in msg
        assert "BASE_CATALOG" in msg
        assert "BASE_SCHEMA" in msg

    def test_valid_config_returns_dict_with_defaults(self, monkeypatch):
        monkeypatch.setenv("GENIE_SPACE_ID", "space-xyz")
        monkeypatch.setenv("BASE_CATALOG", "main")
        monkeypatch.setenv("BASE_SCHEMA", "simple_dispatcher")
        monkeypatch.delenv("LLM_ENDPOINT", raising=False)

        from agent import DEFAULT_LLM_ENDPOINT, load_config

        cfg = load_config()
        assert cfg["genie_space_id"] == "space-xyz"
        assert cfg["base_catalog"] == "main"
        assert cfg["base_schema"] == "simple_dispatcher"
        # LLM_ENDPOINT falls back to the documented default.
        assert cfg["llm_endpoint"] == DEFAULT_LLM_ENDPOINT

    def test_dotenv_file_is_loaded(self, tmp_path, monkeypatch):
        """load_config picks up values from a .env file in the working dir."""
        for var in ("GENIE_SPACE_ID", "BASE_CATALOG", "BASE_SCHEMA", "LLM_ENDPOINT"):
            monkeypatch.delenv(var, raising=False)
        env_file = tmp_path / ".env"
        env_file.write_text(
            "GENIE_SPACE_ID=from-dotenv\n"
            "BASE_CATALOG=cat_from_dotenv\n"
            "BASE_SCHEMA=schema_from_dotenv\n"
        )
        monkeypatch.chdir(tmp_path)

        from agent import load_config

        cfg = load_config()
        assert cfg["genie_space_id"] == "from-dotenv"
        assert cfg["base_catalog"] == "cat_from_dotenv"
        assert cfg["base_schema"] == "schema_from_dotenv"


# ---------------------------------------------------------------------------
# Genie tool seam
# ---------------------------------------------------------------------------


class TestGenieTool:
    def test_genie_tool_wraps_ask_question_and_routes_to_structured_data(self, monkeypatch):
        """make_genie_tool returns a @tool wrapping Genie.ask_question, with a
        description that mentions structured data so the router can pick it."""
        fake_genie = MagicMock()
        fake_response = MagicMock()
        fake_response.result = "Sales were $1.2M last quarter."
        fake_genie.ask_question.return_value = fake_response

        # Patch the Genie class the seam constructs.
        genie_class = MagicMock(return_value=fake_genie)
        monkeypatch.setattr("agent.Genie", genie_class)

        from agent import make_genie_tool

        tool = make_genie_tool("space-123")
        genie_class.assert_called_once_with("space-123")

        # Tool is invokable and routes the question through ask_question.
        out = tool.invoke({"question": "What were total sales last quarter?"})
        fake_genie.ask_question.assert_called_once()
        assert "1.2M" in out

        # Description is non-empty and routing-sharp.
        assert tool.description.strip()
        assert "structured data" in tool.description.lower()


# ---------------------------------------------------------------------------
# Web search tool seam
# ---------------------------------------------------------------------------


class TestWebSearchTool:
    def test_web_search_tool_executes_uc_function(self, monkeypatch):
        """make_web_search_tool returns a @tool that executes the UC function
        {catalog}.{schema}.web_search via the function client, with a
        description that mentions fresh public/internet info."""
        fake_client = MagicMock()
        fake_result = MagicMock()
        fake_result.value = "Top web results for the query..."
        fake_result.error = None
        fake_client.execute_function.return_value = fake_result

        client_class = MagicMock(return_value=fake_client)
        monkeypatch.setattr("agent.DatabricksFunctionClient", client_class)

        from agent import make_web_search_tool

        tool = make_web_search_tool("main", "simple_dispatcher")

        out = tool.invoke({"query": "latest news on Databricks"})

        fake_client.execute_function.assert_called_once()
        call_args, call_kwargs = fake_client.execute_function.call_args
        # First positional arg is the fully-qualified function name.
        fq_name = call_args[0] if call_args else call_kwargs.get("function_name")
        assert fq_name == "main.simple_dispatcher.web_search"
        # The query is forwarded as the `query` parameter.
        params = call_args[1] if len(call_args) > 1 else call_kwargs.get("parameters")
        assert params == {"query": "latest news on Databricks"}
        assert "Top web results" in out

        assert tool.description.strip()
        desc = tool.description.lower()
        assert "internet" in desc or "public" in desc


# ---------------------------------------------------------------------------
# Dispatcher wiring + full-path routing
# ---------------------------------------------------------------------------


class TestDispatcher:
    def test_build_dispatcher_returns_invokable(self):
        from langchain_core.messages import AIMessage

        from agent import build_dispatcher

        fake = ToolCallingFakeChatModel(messages=iter([AIMessage(content="hi")]))
        dispatcher = build_dispatcher(llm=fake, tools=[])
        assert callable(getattr(dispatcher, "invoke", None))

    def test_full_path_routes_to_genie_tool_then_final_answer(self, monkeypatch):
        """Fake model emits a tool_call to the genie tool, then a final answer.
        Assert the genie seam executed and the final answer is in the state."""
        from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

        # Mock the Genie seam.
        fake_genie = MagicMock()
        fake_response = MagicMock()
        fake_response.result = "Sales were $1.2M last quarter."
        fake_genie.ask_question.return_value = fake_response
        monkeypatch.setattr("agent.Genie", MagicMock(return_value=fake_genie))

        from agent import make_genie_tool

        genie_tool = make_genie_tool("space-123")

        final_answer = "Based on Genie data, sales were $1.2M last quarter."
        first = AIMessage(
            content="",
            tool_calls=[
                {
                    "name": genie_tool.name,
                    "args": {"question": "What were total sales last quarter?"},
                    "id": "call-001",
                    "type": "tool_call",
                }
            ],
        )
        second = AIMessage(content=final_answer)
        fake = ToolCallingFakeChatModel(messages=iter([first, second]))

        from agent import build_dispatcher

        dispatcher = build_dispatcher(llm=fake, tools=[genie_tool])
        result = dispatcher.invoke(
            {"messages": [HumanMessage(content="What were total sales last quarter?")]}
        )

        # The Genie seam executed.
        fake_genie.ask_question.assert_called_once()
        # A ToolMessage carried the Genie result back.
        tool_msgs = [m for m in result["messages"] if isinstance(m, ToolMessage)]
        assert tool_msgs
        assert "1.2M" in tool_msgs[0].content
        # Final answer present in the last message.
        assert final_answer in result["messages"][-1].content


# ---------------------------------------------------------------------------
# Serving wrapper
# ---------------------------------------------------------------------------


class TestServingWrapper:
    def test_predict_returns_responses_shape(self, monkeypatch):
        """DispatcherResponsesAgent.predict returns a ResponsesAgentResponse whose
        output carries the dispatcher's final answer text."""
        from langchain_core.messages import AIMessage
        from mlflow.types.responses import ResponsesAgentRequest

        import serving_agent

        # Mock the dispatcher graph so no LLM / workspace is touched.
        fake_dispatcher = MagicMock()
        fake_dispatcher.invoke.return_value = {
            "messages": [AIMessage(content="the dispatched answer")]
        }
        monkeypatch.setattr(
            serving_agent, "_build_dispatcher_from_env", lambda: fake_dispatcher
        )

        agent = serving_agent.DispatcherResponsesAgent()
        request = ResponsesAgentRequest(input=[{"role": "user", "content": "hello"}])
        response = agent.predict(request)

        # Response carries the answer somewhere in its output items.
        text_blob = serving_agent._response_text(response)
        assert "the dispatched answer" in text_blob
