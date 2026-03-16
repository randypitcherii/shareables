"""OS service installation and management (launchd on macOS, systemd on Linux)."""

from __future__ import annotations

import platform
import shutil
import subprocess
from pathlib import Path
from string import Template

TEMPLATES_DIR = Path(__file__).parent / "templates"

LAUNCHD_LABEL = "com.databricks.agent-proxy"
SYSTEMD_UNIT = "databricks-agent-proxy.service"


def detect_os() -> str:
    """Return 'macos', 'linux', or raise on unsupported."""
    system = platform.system().lower()
    if system == "darwin":
        return "macos"
    elif system == "linux":
        return "linux"
    raise RuntimeError(f"Unsupported OS: {system}. Only macOS and Linux are supported.")


def _find_uvx() -> str:
    """Find the uvx binary path."""
    uvx = shutil.which("uvx")
    if uvx:
        return uvx
    # Fallback to common locations
    for path in [Path.home() / ".local" / "bin" / "uvx", Path("/usr/local/bin/uvx")]:
        if path.exists():
            return str(path)
    return "uvx"  # Hope it's on PATH at runtime


def _render_template(template_name: str, **kwargs: str) -> str:
    """Render a service template with the given variables."""
    template_path = TEMPLATES_DIR / template_name
    content = template_path.read_text()
    return Template(content).safe_substitute(**kwargs)


def install(gateway_url: str, port: int) -> Path:
    """Install the proxy as an OS service. Returns the service file path."""
    os_type = detect_os()
    uvx_path = _find_uvx()

    if os_type == "macos":
        return _install_launchd(gateway_url, port, uvx_path)
    else:
        return _install_systemd(gateway_url, port, uvx_path)


def _install_launchd(gateway_url: str, port: int, uvx_path: str) -> Path:
    """Install a launchd plist on macOS."""
    log_dir = Path.home() / "Library" / "Logs" / "databricks-agent-proxy"
    log_dir.mkdir(parents=True, exist_ok=True)

    content = _render_template(
        "launchd.plist",
        uvx_path=uvx_path,
        gateway_url=gateway_url,
        port=str(port),
        log_dir=str(log_dir),
    )

    plist_path = Path.home() / "Library" / "LaunchAgents" / f"{LAUNCHD_LABEL}.plist"
    plist_path.parent.mkdir(parents=True, exist_ok=True)

    # Unload existing if present
    if plist_path.exists():
        subprocess.run(["launchctl", "unload", str(plist_path)], capture_output=True)

    plist_path.write_text(content)
    subprocess.run(["launchctl", "load", str(plist_path)], check=True)
    return plist_path


def _install_systemd(gateway_url: str, port: int, uvx_path: str) -> Path:
    """Install a systemd user unit on Linux."""
    content = _render_template(
        "systemd.service",
        uvx_path=uvx_path,
        gateway_url=gateway_url,
        port=str(port),
        home=str(Path.home()),
    )

    unit_dir = Path.home() / ".config" / "systemd" / "user"
    unit_dir.mkdir(parents=True, exist_ok=True)
    unit_path = unit_dir / SYSTEMD_UNIT

    unit_path.write_text(content)
    subprocess.run(["systemctl", "--user", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "--user", "enable", "--now", SYSTEMD_UNIT], check=True)
    return unit_path


def uninstall() -> bool:
    """Remove the OS service. Returns True if something was removed."""
    os_type = detect_os()
    if os_type == "macos":
        return _uninstall_launchd()
    else:
        return _uninstall_systemd()


def _uninstall_launchd() -> bool:
    """Remove launchd plist on macOS."""
    plist_path = Path.home() / "Library" / "LaunchAgents" / f"{LAUNCHD_LABEL}.plist"
    if not plist_path.exists():
        return False
    subprocess.run(["launchctl", "unload", str(plist_path)], capture_output=True)
    plist_path.unlink()
    return True


def _uninstall_systemd() -> bool:
    """Remove systemd user unit on Linux."""
    unit_path = Path.home() / ".config" / "systemd" / "user" / SYSTEMD_UNIT
    if not unit_path.exists():
        return False
    subprocess.run(["systemctl", "--user", "disable", "--now", SYSTEMD_UNIT], capture_output=True)
    unit_path.unlink()
    subprocess.run(["systemctl", "--user", "daemon-reload"], capture_output=True)
    return True


def status() -> dict[str, str]:
    """Check the service status. Returns {installed, running} info."""
    os_type = detect_os()
    if os_type == "macos":
        return _status_launchd()
    else:
        return _status_systemd()


def _status_launchd() -> dict[str, str]:
    """Check launchd service status."""
    plist_path = Path.home() / "Library" / "LaunchAgents" / f"{LAUNCHD_LABEL}.plist"
    if not plist_path.exists():
        return {"installed": "false", "running": "unknown"}

    result = subprocess.run(
        ["launchctl", "list", LAUNCHD_LABEL],
        capture_output=True,
        text=True,
    )
    running = "true" if result.returncode == 0 else "false"
    return {"installed": "true", "running": running}


def _status_systemd() -> dict[str, str]:
    """Check systemd service status."""
    unit_path = Path.home() / ".config" / "systemd" / "user" / SYSTEMD_UNIT
    if not unit_path.exists():
        return {"installed": "false", "running": "unknown"}

    result = subprocess.run(
        ["systemctl", "--user", "is-active", SYSTEMD_UNIT],
        capture_output=True,
        text=True,
    )
    running = "true" if result.stdout.strip() == "active" else "false"
    return {"installed": "true", "running": running}
