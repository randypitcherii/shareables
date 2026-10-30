"""
A simple model dispatcher on Unity AI Gateway.

Clients send one request to this agent. JEV picks the best model for that
request, and the agent forwards it through Unity AI Gateway:

  - luna  (system.ai.gpt-6-luna)       default; cheap and fast. Low-intelligence
                                       or high-context work (summaries,
                                       extraction, long documents).
  - opus  (system.ai.claude-opus-5-5)  high-intelligence work (hard reasoning,
                                       complex code, multi-step analysis).
  - argon (system.ai.gemini-4-argon)   visual design, real-world 3D/spatial
                                       thinking, high-quality written copy.

JEV (system.ai.openjev-qwen35-4b) is a judge model. It answers a `choice`
question with a probability for every option and generates no text, so a
routing call is fast and cheap, and its reasoning is auditable. Every decision
(the probabilities, the confidence, the criterion that won, and any fallback)
is written to the MLflow trace, so the trace shows which model answered and why.

Usage:
    uv run python agent.py "Summarize this changelog in three bullets: ..."
    uv run python agent.py "Prove there are infinitely many primes of the form 4k+3"
    uv run python agent.py "Write a hero tagline and layout for a coffee brand site"

Configuration (environment variables, or a local .env). Everything has a default:
    LUNA_MODEL / OPUS_MODEL / ARGON_MODEL  UC model ids of the three models
    JEV_MODEL                    UC model id of the router (JEV)
    ROUTER_MIN_CONFIDENCE        below this JEV confidence, use the default model (0.5)
    MLFLOW_EXPERIMENT            MLflow experiment path for traces
    BASE_CATALOG / BASE_SCHEMA   UC home for UC-backed traces (optional locally)

Auth is PAT-free and never branched on in code: local dev uses your SSO CLI
profile (DATABRICKS_PROFILE), CI/prod use injected service-principal creds,
and deployed code uses the ambient runtime identity. All of it resolves
through the Databricks SDK's default credential chain.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import mlflow
import requests
from mlflow.entities import SpanType

logger = logging.getLogger("simple_dispatcher_agent")

# ---------------------------------------------------------------------------
# The menu: three models, one router. Each criterion is the text JEV judges
# the request against, so editing a criterion changes routing behavior.
# ---------------------------------------------------------------------------

DEFAULT_ROUTE = "luna"
DEFAULT_JEV_MODEL = "system.ai.openjev-qwen35-4b"
DEFAULT_MIN_CONFIDENCE = 0.5


@dataclass(frozen=True)
class Route:
    name: str
    env_var: str
    default_model: str
    criterion: str


ROUTES: dict[str, Route] = {
    "luna": Route(
        name="luna",
        env_var="LUNA_MODEL",
        default_model="system.ai.gpt-6-luna",
        criterion=(
            "Everyday or low-difficulty work, including work over a lot of "
            "context: chat, quick answers, summarizing, extracting, classifying, "
            "reformatting, translating, or searching long documents and logs. "
            "The cheap, fast default."
        ),
    ),
    "opus": Route(
        name="opus",
        env_var="OPUS_MODEL",
        default_model="system.ai.claude-opus-5-5",
        criterion=(
            "Work that needs deep intelligence: hard multi-step reasoning, math "
            "proofs, complex or high-stakes code, architecture and design "
            "trade-offs, debugging subtle problems, or careful analysis where a "
            "mistake is costly."
        ),
    ),
    "argon": Route(
        name="argon",
        env_var="ARGON_MODEL",
        default_model="system.ai.gemini-4-argon",
        criterion=(
            "Visual and creative craft: visual or UI design, layouts, images and "
            "diagrams, real-world 3D or spatial and physical reasoning, or "
            "polished, high-quality written copy such as marketing, taglines, "
            "speeches, and brand voice."
        ),
    ),
}

ROUTER_INSTRUCTIONS = (
    "Route this user request to the one AI model whose description best fits "
    "what the request needs. luna is the cheap default; choose opus or argon "
    "when the request matches their description."
)

# How much of the conversation JEV sees. Routing needs the gist, not the whole
# context, so the last user turn is truncated and the size is passed as a number.
ROUTER_MAX_REQUEST_CHARS = 4000

CHAT_PATH = "/ai-gateway/mlflow/v1/chat/completions"
SYSTEMONE_PATH = "/ai-gateway/typesafe/v1/systemone"
REQUEST_TAGS_HEADER = "Databricks-Ai-Gateway-Request-Tags"
APP_TAG = "simple_dispatcher_agent"
HTTP_TIMEOUT_SECONDS = 300


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def _load_dotenv_if_present() -> None:
    """Minimal stdlib .env loader: KEY=VALUE lines, no overriding of real env."""
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


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, "").strip() or default


def load_config() -> dict[str, Any]:
    """Load configuration from the environment (and a local .env).

    Every key has a default, so a bare `uv run python agent.py "..."` works.
    Model ids must be three-part UC identifiers (catalog.schema.model): Unity
    AI Gateway resolves them in the model catalog, and legacy `databricks-*`
    endpoint names are refused so the demo can't silently drift back.
    """
    _load_dotenv_if_present()

    models = {name: _env(r.env_var, r.default_model) for name, r in ROUTES.items()}
    jev_model = _env("JEV_MODEL", DEFAULT_JEV_MODEL)
    bad = [m for m in [*models.values(), jev_model] if len(m.split(".")) != 3]
    if bad:
        raise ValueError(
            "Model ids must be three-part Unity Catalog identifiers "
            f"(catalog.schema.model); got: {', '.join(bad)}"
        )

    min_conf = float(_env("ROUTER_MIN_CONFIDENCE", str(DEFAULT_MIN_CONFIDENCE)))
    return {
        "models": models,
        "jev_model": jev_model,
        "min_confidence": min_conf,
        "mlflow_experiment": _env("MLFLOW_EXPERIMENT"),
        "base_catalog": _env("BASE_CATALOG"),
        "base_schema": _env("BASE_SCHEMA"),
    }


# ---------------------------------------------------------------------------
# MLflow tracing (UC-backed when a catalog/schema is given)
# ---------------------------------------------------------------------------


def setup_tracing(
    experiment_name: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
) -> None:
    """Set the experiment, UC-backed when a namespace is given.

    The dispatcher's spans are explicit (@mlflow.trace), so there is no
    framework autolog to enable.
    """
    if not experiment_name:
        return
    if catalog and schema:
        set_experiment_uc_backed(experiment_name, catalog, schema)
    else:
        mlflow.set_experiment(experiment_name)


def set_experiment_uc_backed(experiment_name: str, catalog: str, schema: str) -> None:
    """Set the experiment with UC-backed trace storage, falling back gracefully.

    A UC trace destination can only be linked to an experiment that has no
    traces yet. If this experiment already has workspace-managed traces, keep
    using it unlinked rather than failing the run.
    """
    from mlflow.entities.trace_location import UnityCatalog

    try:
        mlflow.set_experiment(
            experiment_name,
            trace_location=UnityCatalog(catalog_name=catalog, schema_name=schema),
        )
    except Exception as e:
        if "already contains traces" not in str(e):
            raise
        print(
            f"Note: experiment '{experiment_name}' already contains traces, and a "
            "UC trace destination can only be linked to a trace-free experiment. "
            "Continuing with workspace-managed trace storage; use a fresh "
            "experiment to get UC-backed traces."
        )
        mlflow.set_experiment(experiment_name)


# ---------------------------------------------------------------------------
# Unity AI Gateway client: the one seam to the outside world (tests mock it)
# ---------------------------------------------------------------------------


class GatewayError(RuntimeError):
    def __init__(self, status: int, body: str):
        super().__init__(f"HTTP {status}: {body[:500]}")
        self.status = status
        self.body = body


class GatewayClient:
    """Calls the Unity AI Gateway routes with SDK-resolved OAuth credentials."""

    def __init__(self, workspace_client: Any = None) -> None:
        if workspace_client is None:
            from databricks.sdk import WorkspaceClient

            workspace_client = WorkspaceClient()
        self._config = workspace_client.config
        self.host = self._config.host.rstrip("/")

    def _post(self, path: str, payload: dict, tags: dict[str, str]) -> dict:
        headers = {
            **self._config.authenticate(),
            "Content-Type": "application/json",
            # Lands in system.ai_gateway.usage.request_tags, so the billing
            # table records *why* a model was used, not just *that* it was.
            REQUEST_TAGS_HEADER: json.dumps(tags),
        }
        resp = requests.post(
            self.host + path, headers=headers, json=payload, timeout=HTTP_TIMEOUT_SECONDS
        )
        if resp.status_code != 200:
            raise GatewayError(resp.status_code, resp.text)
        return resp.json()

    def systemone(self, model: str, state: Any, questions: dict, tags: dict) -> dict:
        return self._post(
            SYSTEMONE_PATH, {"model": model, "state": state, "questions": questions}, tags
        )

    def chat(self, model: str, messages: list[dict], tags: dict, **params: Any) -> dict:
        return self._post(CHAT_PATH, {"model": model, "messages": messages, **params}, tags)


# ---------------------------------------------------------------------------
# Routing: ask JEV which model fits
# ---------------------------------------------------------------------------


@dataclass
class RoutingDecision:
    route: str
    model: str
    reason: str
    decided_by: str  # "jev" | "low_confidence_default" | "caller" | "jev_error_default"
    jev_choice: str | None = None
    confidence: float | None = None
    probabilities: dict[str, float] = field(default_factory=dict)
    fallback_from: str | None = None
    fallback_reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _text_of(content: Any) -> str:
    """Flatten chat content (a string or a list of parts) to plain text."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            p.get("text", "") for p in content if isinstance(p, dict) and p.get("text")
        )
    return "" if content is None else str(content)


def _has_images(messages: list[dict]) -> bool:
    return any(
        isinstance(m.get("content"), list)
        and any(isinstance(p, dict) and p.get("type") == "image_url" for p in m["content"])
        for m in messages
    )


def router_state(messages: list[dict]) -> dict[str, Any]:
    """What JEV judges: the latest user request plus size signals.

    The size signals let JEV see "low difficulty, huge context" (luna) without
    sending it the whole context.
    """
    user_turns = [m for m in messages if m.get("role") == "user"]
    latest = _text_of(user_turns[-1]["content"]) if user_turns else ""
    total_chars = sum(len(_text_of(m.get("content"))) for m in messages)
    return {
        "request": latest[:ROUTER_MAX_REQUEST_CHARS],
        "request_truncated": len(latest) > ROUTER_MAX_REQUEST_CHARS,
        "conversation_turns": len(messages),
        "conversation_chars": total_chars,
        "includes_images": _has_images(messages),
    }


def router_questions() -> dict[str, Any]:
    return {
        "model": {
            "type": "choice",
            "instructions": ROUTER_INSTRUCTIONS,
            "criteria": {name: r.criterion for name, r in ROUTES.items()},
        }
    }


@mlflow.trace(name="jev_router", span_type=SpanType.CHAIN)
def route_request(
    client: GatewayClient,
    messages: list[dict],
    cfg: dict[str, Any],
    forced_route: str | None = None,
) -> RoutingDecision:
    """Pick a route for this request. Never raises: a router failure means luna."""
    models = cfg["models"]
    span = mlflow.get_current_active_span()

    if forced_route:
        if forced_route not in ROUTES:
            raise ValueError(f"Unknown route {forced_route!r}; expected one of {list(ROUTES)}")
        decision = RoutingDecision(
            route=forced_route,
            model=models[forced_route],
            reason=f"Caller forced route '{forced_route}' via custom_inputs.route.",
            decided_by="caller",
        )
    else:
        try:
            answer = client.systemone(
                cfg["jev_model"],
                router_state(messages),
                router_questions(),
                tags={"app": APP_TAG, "step": "route"},
            )["answers"]["model"]
            choice = answer["choice"]
            confidence = float(answer.get("confidence", 0.0))
            probs = {k: round(float(v), 4) for k, v in answer.get("probabilities", {}).items()}
            confident = confidence >= cfg["min_confidence"]
            if choice in ROUTES and (confident or choice == DEFAULT_ROUTE):
                decision = RoutingDecision(
                    route=choice,
                    model=models[choice],
                    reason=(
                        f"JEV chose '{choice}' with confidence {confidence:.2f}: "
                        f"{ROUTES[choice].criterion}"
                    ),
                    decided_by="jev",
                    jev_choice=choice,
                    confidence=confidence,
                    probabilities=probs,
                )
            else:
                decision = RoutingDecision(
                    route=DEFAULT_ROUTE,
                    model=models[DEFAULT_ROUTE],
                    reason=(
                        f"JEV leaned '{choice}' but confidence {confidence:.2f} is below "
                        f"{cfg['min_confidence']:.2f}, so the default '{DEFAULT_ROUTE}' "
                        "handles it."
                    ),
                    decided_by="low_confidence_default",
                    jev_choice=choice,
                    confidence=confidence,
                    probabilities=probs,
                )
        except Exception as e:  # router down must not mean dispatcher down
            logger.warning("JEV routing failed, using default: %s", e)
            decision = RoutingDecision(
                route=DEFAULT_ROUTE,
                model=models[DEFAULT_ROUTE],
                reason=f"JEV call failed ({e}); the default '{DEFAULT_ROUTE}' handles it.",
                decided_by="jev_error_default",
            )

    if span is not None:
        span.set_attributes(
            {f"routing.{k}": v for k, v in decision.as_dict().items() if v is not None}
        )
    return decision


# ---------------------------------------------------------------------------
# Dispatch: call the chosen model, with one fallback to the default
# ---------------------------------------------------------------------------


@mlflow.trace(name="call_model", span_type=SpanType.CHAT_MODEL)
def call_model(
    client: GatewayClient, model: str, route: str, messages: list[dict], params: dict
) -> dict:
    span = mlflow.get_current_active_span()
    if span is not None:
        span.set_attributes({"model": model, "route": route})
    resp = client.chat(model, messages, tags={"app": APP_TAG, "route": route}, **params)
    if span is not None and resp.get("usage"):
        span.set_attribute("usage", resp["usage"])
    return resp


def _tag_trace(decision: RoutingDecision) -> None:
    """Trace tags are searchable in the MLflow UI and in the UC trace tables,
    e.g. `tags.dispatcher.route = 'opus'`."""
    tags = {
        "dispatcher.route": decision.route,
        "dispatcher.model": decision.model,
        "dispatcher.decided_by": decision.decided_by,
        "dispatcher.reason": decision.reason[:250],
    }
    if decision.confidence is not None:
        tags["dispatcher.jev_confidence"] = f"{decision.confidence:.3f}"
    if decision.fallback_from:
        tags["dispatcher.fallback_from"] = decision.fallback_from
        tags["dispatcher.fallback_reason"] = (decision.fallback_reason or "")[:250]
    if mlflow.get_current_active_span() is None:
        return  # tracing disabled (e.g. mlflow's log-time validation predict)
    try:
        mlflow.update_current_trace(tags=tags)
    except Exception as e:  # tagging is best-effort; never fail the request
        logger.debug("could not tag trace: %s", e)


# ===========================================================================
# THIS IS THE WHOLE DISPATCHER: ask JEV, call the model it picks, log why.
# ===========================================================================
@mlflow.trace(name="dispatch", span_type=SpanType.AGENT)
def dispatch(
    messages: list[dict],
    cfg: dict[str, Any],
    client: GatewayClient | None = None,
    forced_route: str | None = None,
    params: dict | None = None,
) -> dict[str, Any]:
    """Route `messages` to the best model. Returns {"text", "routing", "response"}."""
    client = client or GatewayClient()
    params = params or {}
    decision = route_request(client, messages, cfg, forced_route)

    try:
        response = call_model(client, decision.model, decision.route, messages, params)
    except GatewayError as e:
        if decision.route == DEFAULT_ROUTE:
            raise
        # e.g. a model not yet rolled out to this region: answer from the default
        # model rather than fail, and say so in the trace.
        decision.fallback_from = decision.route
        decision.fallback_reason = (
            f"'{decision.route}' ({decision.model}) failed with HTTP {e.status}, "
            f"so '{DEFAULT_ROUTE}' answered instead."
        )
        decision.route, decision.model = DEFAULT_ROUTE, cfg["models"][DEFAULT_ROUTE]
        response = call_model(client, decision.model, decision.route, messages, params)

    _tag_trace(decision)
    logger.info(
        "routed to %s (%s): %s%s",
        decision.route,
        decision.model,
        decision.reason,
        f" [{decision.fallback_reason}]" if decision.fallback_reason else "",
    )
    text = _text_of(response["choices"][0]["message"].get("content"))
    return {"text": text, "routing": decision.as_dict(), "response": response}


# ===========================================================================


def main(question: str) -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cfg = load_config()
    setup_tracing(cfg["mlflow_experiment"] or None, cfg["base_catalog"], cfg["base_schema"])

    result = dispatch([{"role": "user", "content": question}], cfg)
    r = result["routing"]
    print(f"\nRoute:  {r['route']}  →  {r['model']}")
    print(f"Why:    {r['reason']}")
    if r["fallback_reason"]:
        print(f"Fallback: {r['fallback_reason']}")
    if r["probabilities"]:
        print("JEV:    " + ", ".join(f"{k}={v:.3f}" for k, v in r["probabilities"].items()))
    print(f"\nAnswer:\n{result['text']}\n")
    trace_id = mlflow.get_last_active_trace_id()
    if trace_id:
        print(f"MLflow trace: {trace_id} (tracking URI: {mlflow.get_tracking_uri()})")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: uv run python agent.py "<your request>"')
        sys.exit(1)
    main(sys.argv[1])
