"""Shared test fixtures."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from databricks_agent_proxy.auth import TokenProvider
from databricks_agent_proxy.config import Settings
from databricks_agent_proxy.proxy_app import create_app


@pytest.fixture
def settings() -> Settings:
    return Settings(gateway_url="https://gateway.example.com", port=8787)


@pytest.fixture
def mock_token_provider() -> TokenProvider:
    tp = MagicMock(spec=TokenProvider)
    tp.get_auth_headers.return_value = {"Authorization": "Bearer fake-token"}
    tp.is_authenticated.return_value = True
    return tp


@pytest.fixture
def app(settings: Settings, mock_token_provider: TokenProvider):
    return create_app(settings, token_provider=mock_token_provider)
