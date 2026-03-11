from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = httpx.Timeout(60.0, connect=10.0)


class ServingEndpointProxy:
    """Async proxy that forwards chat completion requests to Databricks serving endpoints."""

    def __init__(self, workspace_host: str, timeout: httpx.Timeout | None = None) -> None:
        self._workspace_host = workspace_host.rstrip("/")
        self._client = httpx.AsyncClient(timeout=timeout or _DEFAULT_TIMEOUT)

    def _build_url(self, endpoint_name: str) -> str:
        return f"{self._workspace_host}/serving-endpoints/{endpoint_name}/invocations"

    async def chat_completion(
        self,
        *,
        endpoint_name: str,
        access_token: str,
        messages: list[dict[str, Any]],
        max_tokens: int | None = None,
        temperature: float | None = None,
        stream: bool = False,
        extra_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = self._build_url(endpoint_name)
        payload: dict[str, Any] = {"messages": messages}
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        if temperature is not None:
            payload["temperature"] = temperature
        if stream:
            payload["stream"] = True
        if extra_params:
            payload.update(extra_params)

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        logger.info("Proxying to %s for endpoint %s", url, endpoint_name)
        response = await self._client.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()

    async def close(self) -> None:
        await self._client.aclose()
