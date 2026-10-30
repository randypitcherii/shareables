"""
Tests for the simple model dispatcher.

Everything runs offline: the only external seam is agent.GatewayClient, which
these tests replace with FakeGateway (a scripted JEV answer + canned chat
responses, recording every call).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import mlflow
import pytest

import agent


def jev_answer(choice: str, confidence: float, probs: dict | None = None) -> dict:
    probs = probs or {k: (0.9 if k == choice else 0.05) for k in agent.ROUTES}
    return {
        "model": "databricks-openjev-qwen35-4b",
        "answers": {
            "model": {
                "type": "choice",
                "choice": choice,
                "confidence": confidence,
                "probabilities": probs,
            }
        },
        "usage": {"input_tokens": 80, "output_tokens": 0},
    }


def chat_response(text, model="m") -> dict:
    return {
        "model": model,
        "choices": [{"index": 0, "message": {"role": "assistant", "content": text}}],
        "usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
    }


class FakeGateway:
    """Stands in for agent.GatewayClient."""

    def __init__(self, jev=None, jev_error=None, failing_models=()):
        self.jev = jev or jev_answer("luna", 0.95)
        self.jev_error = jev_error
        self.failing_models = set(failing_models)
        self.systemone_calls: list[dict] = []
        self.chat_calls: list[dict] = []

    def systemone(self, model, state, questions, tags):
        self.systemone_calls.append(
            {"model": model, "state": state, "questions": questions, "tags": tags}
        )
        if self.jev_error:
            raise self.jev_error
        return self.jev

    def chat(self, model, messages, tags, **params):
        self.chat_calls.append({"model": model, "messages": messages, "tags": tags, **params})
        if model in self.failing_models:
            raise agent.GatewayError(404, '{"error_code":"ENDPOINT_NOT_FOUND"}')
        return chat_response(f"answer from {model}", model)


@pytest.fixture
def cfg(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)  # no stray .env
    for r in agent.ROUTES.values():
        monkeypatch.delenv(r.env_var, raising=False)
    for v in ("JEV_MODEL", "ROUTER_MIN_CONFIDENCE"):
        monkeypatch.delenv(v, raising=False)
    return agent.load_config()


USER = [{"role": "user", "content": "hello"}]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class TestConfig:
    def test_defaults_are_latest_uc_models(self, cfg):
        assert cfg["models"] == {
            "luna": "system.ai.gpt-6-luna",
            "opus": "system.ai.claude-opus-5-5",
            "argon": "system.ai.gemini-4-argon",
        }
        assert cfg["jev_model"] == "system.ai.openjev-qwen35-4b"
        assert cfg["min_confidence"] == 0.5

    def test_env_overrides(self, cfg, monkeypatch):
        monkeypatch.setenv("OPUS_MODEL", "main.models.my-opus")
        monkeypatch.setenv("ROUTER_MIN_CONFIDENCE", "0.8")
        c = agent.load_config()
        assert c["models"]["opus"] == "main.models.my-opus"
        assert c["min_confidence"] == 0.8

    def test_legacy_endpoint_names_are_refused(self, cfg, monkeypatch):
        monkeypatch.setenv("LUNA_MODEL", "databricks-gpt-6-luna")
        with pytest.raises(ValueError, match="three-part"):
            agent.load_config()

    def test_dotenv_file_is_loaded(self, cfg, monkeypatch, tmp_path):
        (tmp_path / ".env").write_text("ARGON_MODEL=cat.sch.argon\n")
        assert agent.load_config()["models"]["argon"] == "cat.sch.argon"
        monkeypatch.delenv("ARGON_MODEL")


# ---------------------------------------------------------------------------
# JEV routing
# ---------------------------------------------------------------------------


class TestRouting:
    @pytest.mark.parametrize("route", ["luna", "opus", "argon"])
    def test_confident_jev_choice_wins(self, cfg, route):
        gw = FakeGateway(jev=jev_answer(route, 0.9))
        d = agent.route_request(gw, USER, cfg)
        assert (d.route, d.model, d.decided_by) == (route, cfg["models"][route], "jev")
        assert d.confidence == 0.9
        assert set(d.probabilities) == set(agent.ROUTES)
        # the reason names the choice and the criterion that matched
        assert route in d.reason and agent.ROUTES[route].criterion in d.reason

    def test_low_confidence_goes_to_luna(self, cfg):
        gw = FakeGateway(jev=jev_answer("opus", 0.3))
        d = agent.route_request(gw, USER, cfg)
        assert d.route == "luna"
        assert d.decided_by == "low_confidence_default"
        assert d.jev_choice == "opus"
        assert "below" in d.reason

    def test_unsure_luna_pick_is_still_jevs_decision(self, cfg):
        d = agent.route_request(FakeGateway(jev=jev_answer("luna", 0.3)), USER, cfg)
        assert (d.route, d.decided_by) == ("luna", "jev")

    def test_jev_failure_goes_to_luna(self, cfg):
        gw = FakeGateway(jev_error=agent.GatewayError(503, "busy"))
        d = agent.route_request(gw, USER, cfg)
        assert (d.route, d.decided_by) == ("luna", "jev_error_default")

    def test_caller_can_force_a_route_without_asking_jev(self, cfg):
        gw = FakeGateway()
        d = agent.route_request(gw, USER, cfg, forced_route="opus")
        assert (d.route, d.decided_by) == ("opus", "caller")
        assert gw.systemone_calls == []

    def test_unknown_forced_route_is_rejected(self, cfg):
        with pytest.raises(ValueError, match="Unknown route"):
            agent.route_request(FakeGateway(), USER, cfg, forced_route="gpt-9")

    def test_jev_gets_choice_question_over_all_three_routes(self, cfg):
        gw = FakeGateway()
        agent.route_request(gw, USER, cfg)
        call = gw.systemone_calls[0]
        assert call["model"] == "system.ai.openjev-qwen35-4b"
        q = call["questions"]["model"]
        assert q["type"] == "choice"
        assert list(q["criteria"]) == ["luna", "opus", "argon"]

    def test_router_state_carries_size_signals_but_truncates(self):
        big = "x" * (agent.ROUTER_MAX_REQUEST_CHARS + 10)
        msgs = [
            {"role": "system", "content": "be brief"},
            {"role": "user", "content": big},
        ]
        s = agent.router_state(msgs)
        assert len(s["request"]) == agent.ROUTER_MAX_REQUEST_CHARS
        assert s["request_truncated"] is True
        assert s["conversation_turns"] == 2
        assert s["conversation_chars"] == len(big) + len("be brief")
        assert s["includes_images"] is False

    def test_router_state_flags_images(self):
        msgs = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "critique this layout"},
                    {"type": "image_url", "image_url": {"url": "https://x/y.png"}},
                ],
            }
        ]
        s = agent.router_state(msgs)
        assert s["request"] == "critique this layout"
        assert s["includes_images"] is True


# ---------------------------------------------------------------------------
# Dispatch end to end (fake gateway)
# ---------------------------------------------------------------------------


class TestDispatch:
    def test_dispatch_calls_chosen_model_and_tags_request(self, cfg):
        gw = FakeGateway(jev=jev_answer("opus", 0.9))
        out = agent.dispatch(USER, cfg, client=gw, params={"max_tokens": 50})
        assert out["text"] == "answer from system.ai.claude-opus-5-5"
        assert out["routing"]["route"] == "opus"
        call = gw.chat_calls[0]
        assert call["model"] == "system.ai.claude-opus-5-5"
        assert call["messages"] == USER
        assert call["max_tokens"] == 50
        # gateway request tags record the route in system.ai_gateway.usage
        assert call["tags"] == {"app": "simple_dispatcher_agent", "route": "opus"}

    def test_unavailable_model_falls_back_to_luna_and_says_why(self, cfg):
        gw = FakeGateway(jev=jev_answer("argon", 0.99), failing_models={"system.ai.gemini-4-argon"})
        out = agent.dispatch(USER, cfg, client=gw)
        r = out["routing"]
        assert (r["route"], r["fallback_from"]) == ("luna", "argon")
        assert "404" in r["fallback_reason"]
        assert [c["model"] for c in gw.chat_calls] == [
            "system.ai.gemini-4-argon",
            "system.ai.gpt-6-luna",
        ]

    def test_default_model_failure_is_raised(self, cfg):
        gw = FakeGateway(failing_models={"system.ai.gpt-6-luna"})
        with pytest.raises(agent.GatewayError):
            agent.dispatch(USER, cfg, client=gw)

    def test_list_content_is_flattened(self, cfg):
        class ListGateway(FakeGateway):
            def chat(self, model, messages, tags, **params):
                return {"choices": [{"message": {"content": [{"type": "text", "text": "hi"}]}}]}

        assert agent.dispatch(USER, cfg, client=ListGateway())["text"] == "hi"


# ---------------------------------------------------------------------------
# MLflow: the trace shows which model ran and why
# ---------------------------------------------------------------------------


class TestTrace:
    def test_trace_has_routing_tags_and_spans(self, cfg, tmp_path):
        mlflow.set_tracking_uri(f"sqlite:///{tmp_path}/mlflow.db")
        mlflow.set_experiment("dispatcher-test")
        gw = FakeGateway(jev=jev_answer("argon", 0.97), failing_models={"system.ai.gemini-4-argon"})

        agent.dispatch(USER, cfg, client=gw)
        mlflow.flush_trace_async_logging()
        trace = mlflow.get_trace(mlflow.get_last_active_trace_id())

        tags = trace.info.tags
        assert tags["dispatcher.route"] == "luna"
        assert tags["dispatcher.fallback_from"] == "argon"
        assert tags["dispatcher.decided_by"] == "jev"
        assert tags["dispatcher.jev_confidence"] == "0.970"

        spans = {s.name: s for s in trace.data.spans}
        assert {"dispatch", "jev_router", "call_model"} <= set(spans)
        router = spans["jev_router"]
        assert router.attributes["routing.jev_choice"] == "argon"
        assert router.attributes["routing.probabilities"]["argon"] == 0.9


class TestTracingSetup:
    def test_setup_tracing_links_uc_trace_location(self, monkeypatch):
        fake = MagicMock()
        monkeypatch.setattr(agent, "mlflow", fake)
        agent.setup_tracing("/Users/x/exp", catalog="cat", schema="sch")
        args, kwargs = fake.set_experiment.call_args
        assert args[0] == "/Users/x/exp"
        assert kwargs["trace_location"].catalog_name == "cat"

    def test_setup_tracing_without_experiment_is_a_noop(self, monkeypatch):
        fake = MagicMock()
        monkeypatch.setattr(agent, "mlflow", fake)
        agent.setup_tracing(None, catalog="cat", schema="sch")
        fake.set_experiment.assert_not_called()

    def test_falls_back_when_experiment_has_traces(self, monkeypatch, capsys):
        fake = MagicMock()
        fake.set_experiment.side_effect = [Exception("Experiment 1 already contains traces"), None]
        monkeypatch.setattr(agent, "mlflow", fake)
        agent.setup_tracing("/Users/x/exp", catalog="cat", schema="sch")
        assert fake.set_experiment.call_count == 2
        assert "trace_location" not in fake.set_experiment.call_args.kwargs
        assert "already contains traces" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Serving wrapper (ResponsesAgent)
# ---------------------------------------------------------------------------


class TestServingWrapper:
    def test_predict_returns_text_and_routing(self, cfg):
        from mlflow.types.responses import ResponsesAgentRequest

        import serving_agent

        gw = FakeGateway(jev=jev_answer("opus", 0.9))
        model = serving_agent.DispatcherResponsesAgent(client=gw)
        req = ResponsesAgentRequest(
            input=[{"role": "user", "content": "design a db schema"}],
            custom_inputs={"temperature": 0.2},
        )
        resp = model.predict(req)
        assert serving_agent.response_text(resp) == "answer from system.ai.claude-opus-5-5"
        assert resp.custom_outputs["routing"]["route"] == "opus"
        assert gw.chat_calls[0]["temperature"] == 0.2

    def test_custom_inputs_route_pins_the_model(self, cfg):
        from mlflow.types.responses import ResponsesAgentRequest

        import serving_agent

        gw = FakeGateway()
        model = serving_agent.DispatcherResponsesAgent(client=gw)
        req = ResponsesAgentRequest(
            input=[{"role": "user", "content": "hi"}], custom_inputs={"route": "argon"}
        )
        assert model.predict(req).custom_outputs["routing"]["decided_by"] == "caller"
        assert gw.chat_calls[0]["model"] == "system.ai.gemini-4-argon"

    def test_responses_input_converts_to_chat_messages(self):
        from mlflow.types.responses import ResponsesAgentRequest

        import serving_agent

        req = ResponsesAgentRequest(
            input=[
                {"role": "user", "content": [{"type": "input_text", "text": "a"}]},
                {
                    "role": "assistant",
                    "type": "message",
                    "id": "1",
                    "content": [{"type": "output_text", "text": "b", "annotations": []}],
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "look"},
                        {"type": "input_image", "image_url": "https://x/y.png"},
                    ],
                },
            ]
        )
        msgs = serving_agent.to_chat_messages(req)
        assert msgs[0] == {"role": "user", "content": "a"}
        assert msgs[1] == {"role": "assistant", "content": "b"}
        assert msgs[2]["content"][1] == {"type": "image_url", "image_url": {"url": "https://x/y.png"}}


# ---------------------------------------------------------------------------
# Packaging + declared resources (the deployed-serving path)
# ---------------------------------------------------------------------------


class TestModelPackaging:
    def test_logged_model_loads_outside_repo(self, cfg, tmp_path, monkeypatch):
        """Log exactly as driver.py does, then load from a clean cwd.

        serving_agent.py imports `agent`, so the artifact must bundle agent.py
        (code_paths); otherwise the serving container dies with
        ModuleNotFoundError. mlflow validates by running predict on the input
        example, so GatewayClient is swapped for the fake during logging.
        """
        import os
        import subprocess
        import sys

        from pathlib import Path

        import driver

        monkeypatch.chdir(Path(driver.__file__).parent)  # log_model paths are cwd-relative
        monkeypatch.setattr(agent, "GatewayClient", lambda: FakeGateway())
        mlflow.set_tracking_uri(f"sqlite:///{tmp_path}/mlflow.db")
        mlflow.set_experiment("packaging-test")
        with mlflow.start_run():
            logged = mlflow.pyfunc.log_model(**driver.log_model_kwargs())

        probe = f"import mlflow; mlflow.pyfunc.load_model({logged.model_uri!r}); print('LOADED')"
        result = subprocess.run(
            [sys.executable, "-c", probe],
            cwd=tmp_path,
            env={k: v for k, v in os.environ.items() if k != "PYTHONPATH"},
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert "LOADED" in result.stdout, result.stderr[-2000:]


class TestDriver:
    def test_resources_are_the_four_model_services(self):
        import driver

        args = driver._parse_args(
            [
                "--catalog", "c", "--schema", "s",
                "--luna-model", "system.ai.gpt-6-luna",
                "--opus-model", "system.ai.claude-opus-5-5",
                "--argon-model", "system.ai.gemini-4-argon",
                "--jev-model", "system.ai.openjev-qwen35-4b",
                "--experiment", "/Users/x/e",
            ]
        )  # fmt: skip
        env = driver.agent_environment(args)
        res = driver.model_resources(env.values())
        assert [r.name for r in res] == [
            "system.ai.gpt-6-luna",
            "system.ai.claude-opus-5-5",
            "system.ai.gemini-4-argon",
            "system.ai.openjev-qwen35-4b",
        ]
        assert all(r.type.value == "uc_model_service" for r in res)

    def test_serving_credentials_come_from_the_secret_scope(self):
        import driver

        env = driver.serving_credentials_env("https://h", "scope")
        assert env == {
            "DATABRICKS_HOST": "https://h",
            "DATABRICKS_CLIENT_ID": "{{secrets/scope/client_id}}",
            "DATABRICKS_CLIENT_SECRET": "{{secrets/scope/client_secret}}",
        }
