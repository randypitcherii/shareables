"""Tests for service management."""

from unittest.mock import patch

import pytest

from databricks_agent_proxy.service import _render_template, detect_os


def test_detect_os_returns_string():
    result = detect_os()
    assert result in ("macos", "linux")


def test_launchd_template_renders():
    content = _render_template(
        "launchd.plist",
        uvx_path="/usr/local/bin/uvx",
        gateway_url="https://gw.example.com",
        port="8787",
        log_dir="/tmp/logs",
    )
    assert "/usr/local/bin/uvx" in content
    assert "https://gw.example.com" in content
    assert "8787" in content
    assert "com.databricks.agent-proxy" in content


def test_systemd_template_renders():
    content = _render_template(
        "systemd.service",
        uvx_path="/home/user/.local/bin/uvx",
        gateway_url="https://gw.example.com",
        port="8787",
        home="/home/user",
    )
    assert "/home/user/.local/bin/uvx" in content
    assert "https://gw.example.com" in content
    assert "databricks-agent-proxy" in content


@patch("databricks_agent_proxy.service.platform")
def test_detect_os_unsupported(mock_platform):
    mock_platform.system.return_value = "Windows"
    with pytest.raises(RuntimeError, match="Unsupported OS"):
        detect_os()
