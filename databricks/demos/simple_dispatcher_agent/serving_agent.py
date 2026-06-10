"""
MLflow 3 serving interface for the dispatcher agent.

Wraps the same `build_dispatcher` graph from agent.py in an
`mlflow.pyfunc.ResponsesAgent`, the currently-recommended interface for
Databricks Model Serving (Playground / Review App / agent endpoints).

This module is also the entry point for code-based model logging: the bottom
of the file calls `mlflow.models.set_model(...)`, so `driver.py` can log it
with `mlflow.pyfunc.log_model(python_model="serving_agent.py", ...)`.
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

from agent import build_dispatcher_from_config, load_config


def _build_dispatcher_from_env() -> Any:
    """Build the dispatcher from environment config.

    Indirection kept as a module-level function so tests can monkeypatch it.
    """
    return build_dispatcher_from_config(load_config())


def _to_langchain_messages(request: ResponsesAgentRequest) -> list[dict[str, str]]:
    """Convert a ResponsesAgentRequest into LangChain-style message dicts."""
    messages: list[dict[str, str]] = []
    for item in request.input:
        data = item.model_dump() if hasattr(item, "model_dump") else dict(item)
        role = data.get("role")
        content = data.get("content")
        if role and content is not None:
            messages.append({"role": role, "content": content})
    return messages


def _response_text(response: ResponsesAgentResponse) -> str:
    """Flatten a ResponsesAgentResponse's output items into a single string.

    Used by tests and as a convenience; the real output structure is the list
    of output items, but a flat string makes assertions and logging simple.
    """
    parts: list[str] = []
    for item in response.output:
        data = item if isinstance(item, dict) else item.model_dump()
        content = data.get("content")
        if isinstance(content, str):
            parts.append(content)
        elif isinstance(content, list):
            for block in content:
                text = block.get("text") if isinstance(block, dict) else None
                if text:
                    parts.append(text)
    return "\n".join(parts)


class DispatcherResponsesAgent(ResponsesAgent):
    """ResponsesAgent that delegates to the dispatcher graph."""

    def __init__(self, dispatcher: Any = None) -> None:
        # Lazy by default so model loading at serving time builds it once.
        self._dispatcher = dispatcher

    @property
    def dispatcher(self) -> Any:
        if self._dispatcher is None:
            self._dispatcher = _build_dispatcher_from_env()
        return self._dispatcher

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        messages = _to_langchain_messages(request)
        result = self.dispatcher.invoke({"messages": messages})
        final = result["messages"][-1]
        text = final.content if hasattr(final, "content") else str(final)
        output_item = self.create_text_output_item(text=str(text), id=str(uuid4()))
        return ResponsesAgentResponse(output=[output_item])

    def predict_stream(self, request: ResponsesAgentRequest):
        # Minimal streaming: emit the final answer as a single completed item.
        response = self.predict(request)
        for item in response.output:
            yield ResponsesAgentStreamEvent(type="response.output_item.done", item=item)


mlflow.models.set_model(DispatcherResponsesAgent())
