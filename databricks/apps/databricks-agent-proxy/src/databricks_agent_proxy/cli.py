"""CLI for databricks-agent-proxy: setup, start, status, uninstall."""

from __future__ import annotations

import sys

import click
import httpx
import uvicorn

from databricks_agent_proxy.auth import TokenProvider
from databricks_agent_proxy.config import Settings
from databricks_agent_proxy.proxy_app import create_app


@click.group()
def main() -> None:
    """Localhost OAuth proxy for connecting Cursor to Databricks gateways."""


@main.command()
@click.option("--gateway-url", required=True, help="URL of the Databricks Apps gateway")
@click.option("--port", default=8787, help="Local proxy port (default: 8787)")
@click.option("--no-service", is_flag=True, help="Skip OS service installation")
def setup(gateway_url: str, port: int, no_service: bool) -> None:
    """Set up the proxy: authenticate, test gateway, install service."""
    from databricks_agent_proxy import service

    settings = Settings(gateway_url=gateway_url.rstrip("/"), port=port)

    # Step 1: OAuth login
    click.echo("Authenticating with Databricks...")
    tp = TokenProvider(host=settings.databricks_host)
    try:
        tp.login()
        click.echo("  Authenticated successfully.")
    except Exception as e:
        click.echo(f"  Authentication failed: {e}", err=True)
        sys.exit(1)

    # Step 2: Test gateway connectivity
    click.echo(f"Testing gateway connectivity: {settings.gateway_url}")
    try:
        headers = tp.get_auth_headers()
        resp = httpx.get(f"{settings.gateway_url}/api/v1/models", headers=headers, timeout=10)
        resp.raise_for_status()
        gateway_data = resp.json()
        click.echo("  Gateway is reachable.")
    except Exception as e:
        click.echo(f"  Gateway test failed: {e}", err=True)
        click.echo("  Continuing anyway — the gateway may not be deployed yet.")
        gateway_data = {}

    # Step 3: Install service
    if not no_service:
        click.echo("Installing OS service...")
        try:
            path = service.install(settings.gateway_url, settings.port)
            click.echo(f"  Service installed: {path}")
        except Exception as e:
            click.echo(f"  Service install failed: {e}", err=True)
            click.echo("  You can start manually with: databricks-agent-proxy start")
    else:
        click.echo("Skipping service installation (--no-service).")

    # Step 4: Print Cursor config instructions
    click.echo()
    click.echo("=" * 60)
    click.echo("CURSOR CONFIGURATION")
    click.echo("=" * 60)
    click.echo()
    click.echo("1. Open Cursor Settings → Models")
    click.echo(f"2. Set 'Override OpenAI Base URL' to: http://127.0.0.1:{port}/v1")
    click.echo("3. Add these as custom models:")

    aliases = gateway_data.get("aliases", {})
    if aliases:
        for name in sorted(aliases.keys()):
            click.echo(f"   - {name}")
    else:
        click.echo("   (could not fetch models — add them after gateway is deployed)")

    click.echo()
    click.echo(f"Proxy will listen on http://127.0.0.1:{port}")
    click.echo()


@main.command()
@click.option("--gateway-url", required=True, help="URL of the Databricks Apps gateway")
@click.option("--port", default=8787, help="Local proxy port (default: 8787)")
@click.option("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
@click.option("--log-level", default="info", help="Log level (default: info)")
def start(gateway_url: str, port: int, host: str, log_level: str) -> None:
    """Start the proxy server in the foreground."""
    settings = Settings(
        gateway_url=gateway_url.rstrip("/"),
        port=port,
        host=host,
        log_level=log_level.upper(),
    )
    settings.validate()

    app = create_app(settings)
    click.echo(f"Starting proxy on http://{host}:{port} → {settings.gateway_url}")
    uvicorn.run(app, host=host, port=port, log_level=log_level.lower())


@main.command()
@click.option("--port", default=8787, help="Proxy port to check (default: 8787)")
def status(port: int) -> None:
    """Check proxy health status."""
    try:
        resp = httpx.get(f"http://127.0.0.1:{port}/health", timeout=5)
        resp.raise_for_status()
        data = resp.json()
        click.echo(f"Status:        {data.get('status', 'unknown')}")
        click.echo(f"Gateway URL:   {data.get('gateway_url', 'N/A')}")
        click.echo(f"Port:          {data.get('port', 'N/A')}")
        click.echo(f"Authenticated: {data.get('authenticated', 'N/A')}")
    except httpx.ConnectError:
        click.echo(f"Proxy is not running on port {port}.", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error checking status: {e}", err=True)
        sys.exit(1)


@main.command()
@click.option("--remove-tool", is_flag=True, help="Also run 'uv tool uninstall'")
def uninstall(remove_tool: bool) -> None:
    """Remove the proxy OS service."""
    import subprocess

    from databricks_agent_proxy import service

    removed = service.uninstall()
    if removed:
        click.echo("Service removed.")
    else:
        click.echo("No service was installed.")

    if remove_tool:
        click.echo("Removing uvx tool installation...")
        subprocess.run(["uv", "tool", "uninstall", "databricks-agent-proxy"], check=False)
        click.echo("Done.")
