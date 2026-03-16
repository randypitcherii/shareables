"""Tests for the CLI commands."""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from databricks_agent_proxy.cli import main


@pytest.fixture
def runner():
    return CliRunner()


def test_cli_group(runner):
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "setup" in result.output
    assert "start" in result.output
    assert "status" in result.output
    assert "uninstall" in result.output


@patch("databricks_agent_proxy.cli.TokenProvider")
@patch("databricks_agent_proxy.cli.httpx")
def test_setup_prints_cursor_config(mock_httpx, mock_tp_cls, runner):
    mock_tp = MagicMock()
    mock_tp_cls.return_value = mock_tp
    mock_tp.get_auth_headers.return_value = {"Authorization": "Bearer tok"}

    mock_resp = MagicMock()
    mock_resp.json.return_value = {"aliases": {"my-model": "ep"}, "endpoints": ["ep"]}
    mock_resp.raise_for_status = MagicMock()
    mock_httpx.get.return_value = mock_resp

    result = runner.invoke(
        main, ["setup", "--gateway-url", "https://gw.example.com", "--no-service"]
    )

    assert result.exit_code == 0
    assert "CURSOR CONFIGURATION" in result.output
    assert "my-model" in result.output


@patch("databricks_agent_proxy.cli.TokenProvider")
@patch("databricks_agent_proxy.cli.httpx")
def test_setup_auth_failure(mock_httpx, mock_tp_cls, runner):
    mock_tp = MagicMock()
    mock_tp_cls.return_value = mock_tp
    mock_tp.login.side_effect = RuntimeError("no creds")

    result = runner.invoke(main, ["setup", "--gateway-url", "https://gw.example.com"])
    assert result.exit_code == 1
    assert "Authentication failed" in result.output


def test_status_no_proxy(runner):
    """Status should fail gracefully when proxy isn't running."""
    result = runner.invoke(main, ["status", "--port", "19999"])
    assert result.exit_code == 1


@patch("databricks_agent_proxy.service.uninstall", return_value=False)
def test_uninstall_no_service(mock_uninstall, runner):
    result = runner.invoke(main, ["uninstall"])
    assert result.exit_code == 0
    assert "No service was installed" in result.output
