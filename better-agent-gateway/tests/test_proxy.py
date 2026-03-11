from __future__ import annotations

import pytest
import httpx
from unittest.mock import AsyncMock, patch

from server.proxy import ServingEndpointProxy


@pytest.fixture
def proxy():
    return ServingEndpointProxy(workspace_host="https://my-workspace.cloud.databricks.com")


class TestServingEndpointProxy:
    @pytest.mark.asyncio
    async def test_build_url(self, proxy):
        url = proxy._build_url("databricks-claude-sonnet-4-5")
        assert url == "https://my-workspace.cloud.databricks.com/serving-endpoints/databricks-claude-sonnet-4-5/invocations"

    @pytest.mark.asyncio
    async def test_proxy_chat_completion(self, proxy):
        mock_response = httpx.Response(
            200,
            json={
                "id": "chatcmpl-123",
                "object": "chat.completion",
                "choices": [{"message": {"role": "assistant", "content": "Hello!"}}],
            },
            request=httpx.Request("POST", "https://example.com"),
        )
        with patch.object(proxy._client, "post", new_callable=AsyncMock, return_value=mock_response):
            result = await proxy.chat_completion(
                endpoint_name="databricks-claude-sonnet-4-5",
                access_token="user-token",
                messages=[{"role": "user", "content": "Hi"}],
                max_tokens=100,
            )
        assert result["choices"][0]["message"]["content"] == "Hello!"

    @pytest.mark.asyncio
    async def test_proxy_passes_user_token(self, proxy):
        mock_response = httpx.Response(
            200,
            json={"choices": []},
            request=httpx.Request("POST", "https://example.com"),
        )
        mock_post = AsyncMock(return_value=mock_response)
        with patch.object(proxy._client, "post", mock_post):
            await proxy.chat_completion(
                endpoint_name="test-ep",
                access_token="user-obo-token",
                messages=[{"role": "user", "content": "test"}],
            )
        call_kwargs = mock_post.call_args
        assert call_kwargs.kwargs["headers"]["Authorization"] == "Bearer user-obo-token"

    @pytest.mark.asyncio
    async def test_proxy_error_propagation(self, proxy):
        mock_response = httpx.Response(
            429,
            json={"error": {"message": "Rate limited"}},
            request=httpx.Request("POST", "https://example.com"),
        )
        with patch.object(proxy._client, "post", new_callable=AsyncMock, return_value=mock_response):
            with pytest.raises(httpx.HTTPStatusError):
                await proxy.chat_completion(
                    endpoint_name="test-ep",
                    access_token="token",
                    messages=[{"role": "user", "content": "test"}],
                )
