from __future__ import annotations

import logging
from typing import Any, AsyncIterator

import httpx
from fastapi.responses import StreamingResponse

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = httpx.Timeout(300.0, connect=10.0)


class ServingEndpointProxy:
    """Async proxy that forwards chat completion requests to Databricks serving endpoints."""

    def __init__(self, workspace_host: str, timeout: httpx.Timeout | None = None) -> None:
        self._workspace_host = workspace_host.rstrip("/")
        self._client = httpx.AsyncClient(timeout=timeout or _DEFAULT_TIMEOUT)

    def _build_url(self, endpoint_name: str) -> str:
        return f"{self._workspace_host}/serving-endpoints/{endpoint_name}/invocations"

    def _build_payload(
        self,
        *,
        messages: list[dict[str, Any]],
        max_tokens: int | None = None,
        temperature: float | None = None,
        stream: bool = False,
        extra_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"messages": messages}
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        if temperature is not None:
            payload["temperature"] = temperature
        if stream:
            payload["stream"] = True
        if extra_params:
            payload.update(extra_params)
        return payload

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
        payload = self._build_payload(
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            stream=False,  # Force non-streaming for this method
            extra_params=extra_params,
        )
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        logger.info("Proxying to %s for endpoint %s", url, endpoint_name)
        response = await self._client.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()

    async def chat_completion_stream(
        self,
        *,
        endpoint_name: str,
        access_token: str,
        messages: list[dict[str, Any]],
        max_tokens: int | None = None,
        temperature: float | None = None,
        extra_params: dict[str, Any] | None = None,
    ) -> httpx.Response:
        """Open a streaming connection and return the raw httpx Response.

        Caller is responsible for iterating and closing the response.
        """
        url = self._build_url(endpoint_name)
        payload = self._build_payload(
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            stream=True,
            extra_params=extra_params,
        )
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        logger.info("Streaming proxy to %s for endpoint %s", url, endpoint_name)
        req = self._client.build_request("POST", url, json=payload, headers=headers)
        response = await self._client.send(req, stream=True)
        response.raise_for_status()
        return response

    async def close(self) -> None:
        await self._client.aclose()
