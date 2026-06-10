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


# ---------------------------------------------------------------------------
# Model packaging (the deployed-serving path)
# ---------------------------------------------------------------------------


class TestModelPackaging:
    def test_logged_model_loads_outside_repo(self, tmp_path, monkeypatch):
        """Log the model exactly as driver.py does, then load it from a clean
        subprocess cwd'd outside the repo.

        serving_agent.py imports `agent`, so the logged artifact must bundle
        agent.py (code_paths) — otherwise the model only loads when the repo
        happens to be on sys.path, and the deployed serving container crashes
        with ModuleNotFoundError.

        mlflow validates ResponsesAgent models at log time by running predict
        on an input example; the env vars + patched dispatcher below let that
        validation run offline (the code file executed by log_model resolves
        `agent` from sys.modules, so the patch reaches it).
        """
        import os
        import subprocess
        import sys

        from langchain_core.messages import AIMessage
        import mlflow

        import agent as agent_module
        import driver

        monkeypatch.setenv("GENIE_SPACE_ID", "space-test")
        monkeypatch.setenv("BASE_CATALOG", "cat")
        monkeypatch.setenv("BASE_SCHEMA", "sch")
        fake_dispatcher = MagicMock()
        fake_dispatcher.invoke.return_value = {
            "messages": [AIMessage(content="validation answer")]
        }
        monkeypatch.setattr(
            agent_module, "build_dispatcher_from_config", lambda cfg: fake_dispatcher
        )

        mlflow.set_tracking_uri(f"sqlite:///{tmp_path}/mlflow.db")
        with mlflow.start_run():
            logged = mlflow.pyfunc.log_model(**driver.log_model_kwargs())

        probe = (
            "import mlflow; "
            f"mlflow.pyfunc.load_model({logged.model_uri!r}); "
            "print('LOADED')"
        )
        result = subprocess.run(
            [sys.executable, "-c", probe],
            cwd=tmp_path,  # outside the repo: no accidental `import agent`
            env={k: v for k, v in os.environ.items() if k != "PYTHONPATH"},
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert "LOADED" in result.stdout, (
            f"model failed to load in a clean cwd:\n{result.stderr[-2000:]}"
        )


# ---------------------------------------------------------------------------
# Tracing setup
# ---------------------------------------------------------------------------


class TestTracing:
    def test_setup_tracing_links_uc_trace_location(self, monkeypatch):
        """When an experiment and a UC namespace are configured, traces are
        stored UC-backed: set_experiment gets a UnityCatalog trace_location."""
        import agent as agent_module

        fake_mlflow = MagicMock()
        monkeypatch.setattr(agent_module, "mlflow", fake_mlflow)

        agent_module.setup_tracing("/Users/x/exp", catalog="cat", schema="sch")

        fake_mlflow.langchain.autolog.assert_called_once()
        args, kwargs = fake_mlflow.set_experiment.call_args
        assert args[0] == "/Users/x/exp"
        loc = kwargs["trace_location"]
        assert loc.catalog_name == "cat"
        assert loc.schema_name == "sch"

    def test_setup_tracing_without_experiment_only_autologs(self, monkeypatch):
        import agent as agent_module

        fake_mlflow = MagicMock()
        monkeypatch.setattr(agent_module, "mlflow", fake_mlflow)

        agent_module.setup_tracing(None, catalog="cat", schema="sch")

        fake_mlflow.langchain.autolog.assert_called_once()
        fake_mlflow.set_experiment.assert_not_called()

    def test_setup_tracing_falls_back_when_experiment_has_traces(self, monkeypatch, capsys):
        """A UC trace destination can only link to a trace-free experiment.
        If the link is rejected, fall back to the plain experiment instead of
        crashing the run."""
        import agent as agent_module

        fake_mlflow = MagicMock()
        fake_mlflow.set_experiment.side_effect = [
            Exception(
                "BAD_REQUEST: Experiment 123 already contains traces. A UC Trace "
                "Destination can only be linked to an Experiment that does not "
                "already contain any traces."
            ),
            None,
        ]
        monkeypatch.setattr(agent_module, "mlflow", fake_mlflow)

        agent_module.setup_tracing("/Users/x/exp", catalog="cat", schema="sch")

        # Second call retried without the UC trace_location.
        assert fake_mlflow.set_experiment.call_count == 2
        retry_args, retry_kwargs = fake_mlflow.set_experiment.call_args
        assert retry_args[0] == "/Users/x/exp"
        assert "trace_location" not in retry_kwargs
        # And the user was told why.
        assert "already contains traces" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Declared model resources (auth passthrough for the serving endpoint)
# ---------------------------------------------------------------------------


class TestModelResources:
    def test_resources_include_genie_warehouse(self):
        """The Genie space resource alone does not grant the serving identity
        access to the SQL warehouse Genie executes on — the warehouse must be
        declared too (live prod query failed without it)."""
        from mlflow.models.resources import DatabricksSQLWarehouse

        import driver

        resources = driver.model_resources(
            genie_space_id="space-1",
            web_search_fqn="cat.sch.web_search",
            llm_endpoint="databricks-claude-sonnet-4-6",
            warehouse_id="wh-123",
        )
        warehouses = [r for r in resources if isinstance(r, DatabricksSQLWarehouse)]
        assert warehouses, "DatabricksSQLWarehouse missing from declared resources"

    def test_resources_include_genie_tables(self):
        """The serving credential is scoped to declared resources only, so the
        tables the Genie space queries must be declared too (live prod query
        failed with table-level PermissionDenied without them)."""
        from mlflow.models.resources import DatabricksTable

        import driver

        resources = driver.model_resources(
            genie_space_id="space-1",
            web_search_fqn="cat.sch.web_search",
            llm_endpoint="databricks-claude-sonnet-4-6",
            warehouse_id="wh-123",
            genie_tables=["cat.data.spend", "cat.data.usage"],
        )
        tables = [r for r in resources if isinstance(r, DatabricksTable)]
        assert len(tables) == 2
