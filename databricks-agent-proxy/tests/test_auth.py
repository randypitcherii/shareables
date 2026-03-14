"""Tests for the auth module."""

from unittest.mock import MagicMock, patch

import pytest

from databricks_agent_proxy.auth import TokenProvider


@patch("databricks_agent_proxy.auth.Config")
def test_get_auth_headers_returns_bearer(mock_config_cls):
    mock_config = MagicMock()
    mock_config.authenticate.return_value = {"Authorization": "Bearer test-token"}
    mock_config_cls.return_value = mock_config

    tp = TokenProvider()
    headers = tp.get_auth_headers()

    assert headers["Authorization"] == "Bearer test-token"


@patch("databricks_agent_proxy.auth.Config")
def test_login_triggers_authenticate(mock_config_cls):
    mock_config = MagicMock()
    mock_config.authenticate.return_value = {"Authorization": "Bearer test-token"}
    mock_config_cls.return_value = mock_config

    tp = TokenProvider()
    tp.login()
    # Should have called authenticate at least once
    mock_config.authenticate.assert_called()


@patch("databricks_agent_proxy.auth.Config")
def test_login_fails_with_no_creds(mock_config_cls):
    mock_config = MagicMock()
    mock_config.authenticate.return_value = {}
    mock_config_cls.return_value = mock_config

    tp = TokenProvider()
    with pytest.raises(RuntimeError, match="OAuth login failed"):
        tp.login()


@patch("databricks_agent_proxy.auth.Config")
def test_is_authenticated_true(mock_config_cls):
    mock_config = MagicMock()
    mock_config.authenticate.return_value = {"Authorization": "Bearer ok"}
    mock_config_cls.return_value = mock_config

    tp = TokenProvider()
    assert tp.is_authenticated() is True


@patch("databricks_agent_proxy.auth.Config")
def test_is_authenticated_false_on_error(mock_config_cls):
    mock_config = MagicMock()
    mock_config.authenticate.side_effect = Exception("no creds")
    mock_config_cls.return_value = mock_config

    tp = TokenProvider()
    assert tp.is_authenticated() is False


@patch("databricks_agent_proxy.auth.Config")
def test_host_passed_to_config(mock_config_cls):
    TokenProvider(host="https://my-workspace.cloud.databricks.com")
    mock_config_cls.assert_called_once_with(host="https://my-workspace.cloud.databricks.com")
