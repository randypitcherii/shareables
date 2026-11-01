"""
MLflow 3 serving interface for the model dispatcher.

Wraps `agent.dispatch` in an `mlflow.pyfunc.ResponsesAgent`, the recommended
interface for Databricks Model Serving (Playground, Review App, agent
endpoints). Deployed with `agents.deploy`, the endpoint gets AI Gateway usage
tracking and an inference table, so clients call one governed endpoint instead
of picking a model themselves.

Every response carries the routing decision in `custom_outputs.routing`, so
callers see which model answered and why. Callers can also pin a model with
`custom_inputs: {"route": "luna" | "opus" | "gemini"}`.

Model calls run as the caller (on-behalf-of-user): the model is logged with a
UserAuthPolicy for the `ai-gateway` scope, and a fresh GatewayClient is built
inside each predict() so it picks up that request's user token.

The bottom of the file calls `mlflow.models.set_model(...)` for code-based
logging: `driver.py` logs it with `mlflow.pyfunc.log_model(python_model="serving_agent.py", ...)`.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import mlflow
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

import agent

# Chat-completions params a caller may pass through to the chosen model.
PASSTHROUGH_PARAMS = ("temperature", "max_tokens", "top_p", "stop")


def _part_to_chat(part: dict) -> dict | None:
    kind = part.get("type")
    if kind in ("input_text", "output_text", "text"):
        return {"type": "text", "text": part.get("text", "")}
    if kind == "input_image" and part.get("image_url"):
        return {"type": "image_url", "image_url": {"url": part["image_url"]}}
    return None


def to_chat_messages(request: ResponsesAgentRequest) -> list[dict[str, Any]]:
    """Convert Responses-API input items into chat-completions messages."""
    messages: list[dict[str, Any]] = []
    for item in request.input:
        data = item.model_dump() if hasattr(item, "model_dump") else dict(item)
        role, content = data.get("role"), data.get("content")
        if not role or content is None:
            continue  # tool calls etc. are out of scope for a pass-through dispatcher
        if isinstance(content, list):
            parts = [p for p in (_part_to_chat(c) for c in content if isinstance(c, dict)) if p]
            if all(p["type"] == "text" for p in parts):
                content = "\n".join(p["text"] for p in parts)
            else:
                content = parts
        messages.append({"role": role, "content": content})
    return messages


def response_text(response: ResponsesAgentResponse) -> str:
    """Flatten a ResponsesAgentResponse's output items to one string."""
    parts: list[str] = []
    for item in response.output:
        data = item if isinstance(item, dict) else item.model_dump()
        for block in data.get("content") or []:
            if isinstance(block, dict) and block.get("text"):
                parts.append(block["text"])
    return "\n".join(parts)


class DispatcherResponsesAgent(ResponsesAgent):
    """ResponsesAgent that routes each request to luna, opus, or gemini via JEV."""

    def __init__(self, client: agent.GatewayClient | None = None) -> None:
        # Tests inject a fake; otherwise a client is built per request (OBO).
        self._client = client
        self._cfg: dict[str, Any] | None = None

    def client(self) -> agent.GatewayClient:
        return self._client or agent.GatewayClient()

    @property
    def cfg(self) -> dict[str, Any]:
        if self._cfg is None:
            self._cfg = agent.load_config()
        return self._cfg

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        custom = request.custom_inputs or {}
        params = {k: custom[k] for k in PASSTHROUGH_PARAMS if k in custom}
        result = agent.dispatch(
            to_chat_messages(request),
            self.cfg,
            client=self.client(),
            forced_route=custom.get("route"),
            params=params,
        )
        item = self.create_text_output_item(text=result["text"], id=str(uuid4()))
        return ResponsesAgentResponse(
            output=[item],
            custom_outputs={"routing": result["routing"], "usage": result["response"].get("usage")},
        )

    def predict_stream(self, request: ResponsesAgentRequest):
        # Minimal streaming: emit the final answer as a single completed item.
        response = self.predict(request)
        for item in response.output:
            yield ResponsesAgentStreamEvent(type="response.output_item.done", item=item)


mlflow.models.set_model(DispatcherResponsesAgent())
