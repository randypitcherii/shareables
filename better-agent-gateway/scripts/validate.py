"""End-to-end validation for the Better Agent Gateway.

Verifies the three-layer security model and all core functionality:
  1. Health check
  2. Auth rejection (no credentials)
  3. PAT rejection
  4. Bearer token auth
  5. Model discovery
  6. Chat completion
  7. Scope enforcement (OBO downscoping)
  8. Identity attribution

Usage:
    uv run python scripts/validate.py
    uv run python scripts/validate.py --app-url https://your-gateway.aws.databricksapps.com
    uv run python scripts/validate.py --verbose
"""

from __future__ import annotations

import json
import os
import sys
import time

import click
import httpx
from databricks.sdk import WorkspaceClient

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_APP_URL = (
    "https://better-agent-gateway-dev-3638734694769498.aws.databricksapps.com"
)
DEFAULT_WORKSPACE_URL = "https://fe-vm-fe-randy-pitcher-workspace.cloud.databricks.com"

# ANSI colours
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _pass(label: str, detail: str = "") -> None:
    suffix = f"  {detail}" if detail else ""
    click.echo(f"  {GREEN}PASS{RESET}  {label}{suffix}")


def _fail(label: str, detail: str = "") -> None:
    suffix = f"  {detail}" if detail else ""
    click.echo(f"  {RED}FAIL{RESET}  {label}{suffix}")


def _info(msg: str) -> None:
    click.echo(f"  {YELLOW}INFO{RESET}  {msg}")


def _section(title: str) -> None:
    click.echo(f"\n{BOLD}{title}{RESET}")


def _get_token(workspace_url: str) -> str:
    """Get an OAuth bearer token via the Databricks SDK."""
    w = WorkspaceClient(host=workspace_url)
    headers = w.config.authenticate()
    auth_header = headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise click.ClickException(
            "Databricks SDK did not return a Bearer token. "
            "Run: databricks auth login --host " + workspace_url
        )
    return auth_header[7:]


def _pretty_json(data: dict, indent: int = 4) -> str:
    return json.dumps(data, indent=indent, default=str)


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------
def check_health(base: str, verbose: bool) -> bool:
    """1. GET /api/v1/healthz returns 200."""
    url = f"{base}/api/v1/healthz"
    try:
        r = httpx.get(url, timeout=10, follow_redirects=True)
        if verbose:
            _info(f"GET {url} -> {r.status_code} {r.text[:200]}")
        if r.status_code == 200:
            _pass("Health check", f"({r.status_code})")
            return True
        _fail("Health check", f"expected 200, got {r.status_code}")
        return False
    except Exception as exc:
        _fail("Health check", str(exc))
        return False


def check_no_auth_rejected(base: str, verbose: bool) -> bool:
    """2. Request without auth gets redirected (302) or rejected."""
    url = f"{base}/api/v1/healthz"
    try:
        r = httpx.get(url, timeout=10, follow_redirects=False)
        if verbose:
            _info(f"GET {url} (no auth) -> {r.status_code}")
        if r.status_code in (302, 401, 403):
            _pass("Auth rejection (no credentials)", f"({r.status_code})")
            return True
        # On Databricks Apps, unauthenticated requests get 302 to SSO.
        # Locally the health endpoint may return 200 (no auth middleware).
        _fail(
            "Auth rejection (no credentials)",
            f"expected 302/401/403, got {r.status_code} -- "
            "this is expected when running against local dev server",
        )
        return False
    except Exception as exc:
        _fail("Auth rejection (no credentials)", str(exc))
        return False


def check_pat_rejected(base: str, verbose: bool) -> bool:
    """3. PAT-style token gets 401."""
    url = f"{base}/api/v1/healthz"
    fake_pat = "not-a-real-token-just-testing-pat-rejection"
    headers = {"Authorization": f"Bearer {fake_pat}"}
    try:
        r = httpx.get(url, headers=headers, timeout=10, follow_redirects=False)
        if verbose:
            _info(f"GET {url} (PAT) -> {r.status_code}")
        # The Databricks App proxy rejects PATs with 401.
        # Locally, a bearer token hits the FastAPI auth layer directly --
        # it won't know it is a PAT, so it may pass through.  We accept
        # 401 or 302 as correct behaviour on the deployed app.
        if r.status_code in (401, 302):
            _pass("PAT rejection", f"({r.status_code})")
            return True
        _fail(
            "PAT rejection",
            f"expected 401/302, got {r.status_code} -- "
            "PAT rejection is enforced by the Databricks App proxy, "
            "not the gateway itself",
        )
        return False
    except Exception as exc:
        _fail("PAT rejection", str(exc))
        return False


def check_bearer_auth(base: str, token: str, verbose: bool) -> bool:
    """4. SDK OAuth token gets 200 on /api/v1/healthz."""
    url = f"{base}/api/v1/healthz"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = httpx.get(url, headers=headers, timeout=10, follow_redirects=True)
        if verbose:
            _info(f"GET {url} (bearer) -> {r.status_code} {r.text[:200]}")
        if r.status_code == 200:
            _pass("Bearer token auth", f"({r.status_code})")
            return True
        _fail("Bearer token auth", f"expected 200, got {r.status_code}")
        return False
    except Exception as exc:
        _fail("Bearer token auth", str(exc))
        return False


def check_model_discovery(base: str, token: str, verbose: bool) -> bool:
    """5. GET /api/v1/models returns aliases and endpoints."""
    url = f"{base}/api/v1/models"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = httpx.get(url, headers=headers, timeout=10, follow_redirects=True)
        if verbose:
            _info(f"GET {url} -> {r.status_code}")
        if r.status_code != 200:
            _fail("Model discovery", f"expected 200, got {r.status_code}")
            return False

        body = r.json()
        aliases = body.get("aliases", {})
        endpoints = body.get("endpoints", [])

        if verbose:
            _info(f"aliases: {json.dumps(aliases, indent=2)}")
            _info(f"endpoints: {endpoints}")

        if not endpoints:
            _fail("Model discovery", "no endpoints returned")
            return False

        _pass(
            "Model discovery",
            f"({len(aliases)} aliases, {len(endpoints)} endpoints)",
        )
        return True
    except Exception as exc:
        _fail("Model discovery", str(exc))
        return False


def check_chat_completion(base: str, token: str, verbose: bool) -> bool:
    """6. Send a simple chat to claude-haiku-latest and get a response."""
    url = f"{base}/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "claude-haiku-latest",
        "messages": [{"role": "user", "content": "Say hello in exactly 3 words."}],
        "max_tokens": 30,
    }
    try:
        _info("Sending chat completion (this may take a few seconds)...")
        start = time.time()
        r = httpx.post(url, headers=headers, json=payload, timeout=30, follow_redirects=True)
        elapsed = time.time() - start

        if verbose:
            _info(f"POST {url} -> {r.status_code} ({elapsed:.1f}s)")
            _info(f"response: {r.text[:500]}")

        if r.status_code != 200:
            _fail("Chat completion", f"expected 200, got {r.status_code}: {r.text[:200]}")
            return False

        body = r.json()
        content = (
            body.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
        )
        if not content:
            _fail("Chat completion", "empty response content")
            return False

        _pass("Chat completion", f'"{content.strip()}" ({elapsed:.1f}s)')
        return True
    except Exception as exc:
        _fail("Chat completion", str(exc))
        return False


def check_scope_enforcement(
    base: str, token: str, verbose: bool
) -> tuple[bool, dict | None]:
    """7. GET /api/v1/permissions/comparison shows catalogs/warehouses blocked."""
    url = f"{base}/api/v1/permissions/comparison"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        _info("Fetching permissions comparison (checks multiple APIs)...")
        r = httpx.get(url, headers=headers, timeout=30, follow_redirects=True)
        if verbose:
            _info(f"GET {url} -> {r.status_code}")

        if r.status_code != 200:
            _fail("Scope enforcement", f"expected 200, got {r.status_code}")
            return False, None

        body = r.json()
        if verbose:
            _info(f"response:\n{_pretty_json(body)}")

        obo = body.get("obo_user", {})
        obo_catalogs = obo.get("catalogs", {})
        obo_warehouses = obo.get("warehouses", {})
        obo_serving = obo.get("serving_endpoints", {})

        # Scoped OBO tokens should be blocked from catalogs and warehouses
        catalogs_blocked = obo_catalogs.get("error") is not None or obo_catalogs.get("count", 0) == 0
        warehouses_blocked = obo_warehouses.get("error") is not None or obo_warehouses.get("count", 0) == 0
        serving_works = obo_serving.get("count", 0) > 0 and obo_serving.get("error") is None

        all_ok = catalogs_blocked and warehouses_blocked and serving_works

        if catalogs_blocked:
            _pass("  Catalogs blocked for OBO user", f'(error: {obo_catalogs.get("error", "none")})')
        else:
            _fail("  Catalogs blocked for OBO user", f'(count: {obo_catalogs.get("count")})')

        if warehouses_blocked:
            _pass("  Warehouses blocked for OBO user", f'(error: {obo_warehouses.get("error", "none")})')
        else:
            _fail("  Warehouses blocked for OBO user", f'(count: {obo_warehouses.get("count")})')

        if serving_works:
            _pass("  Serving endpoints accessible", f'(count: {obo_serving.get("count")})')
        else:
            _fail("  Serving endpoints accessible", f'(error: {obo_serving.get("error", "none")})')

        if all_ok:
            _pass("Scope enforcement", "OBO token correctly downscoped")
        else:
            _fail("Scope enforcement", "unexpected permission pattern")

        return all_ok, body
    except Exception as exc:
        _fail("Scope enforcement", str(exc))
        return False, None


def check_identity_attribution(
    body: dict | None, workspace_url: str, verbose: bool
) -> bool:
    """8. The permissions comparison shows the correct user identity."""
    if body is None:
        _fail("Identity attribution", "no permissions data available")
        return False

    obo_user = body.get("obo_user", {}).get("current_user", {})
    sp_user = body.get("app_sp", {}).get("current_user", {})

    obo_username = obo_user.get("username")
    sp_username = sp_user.get("username")

    if verbose:
        _info(f"OBO user: {obo_username} ({obo_user.get('display_name')})")
        _info(f"App SP:   {sp_username} ({sp_user.get('display_name')})")

    if not obo_username:
        _fail("Identity attribution", "could not determine OBO user identity")
        return False

    # OBO user should not be the same as the SP
    if sp_username and obo_username == sp_username:
        _fail(
            "Identity attribution",
            f"OBO user ({obo_username}) is the same as the SP -- "
            "requests are not running as the end user",
        )
        return False

    _pass(
        "Identity attribution",
        f"OBO user={obo_username}, SP={sp_username or 'N/A'}",
    )
    return True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
@click.command()
@click.option(
    "--app-url",
    default=lambda: os.environ.get("GATEWAY_URL", DEFAULT_APP_URL),
    show_default="$GATEWAY_URL or dev deployment",
    help="Base URL of the deployed gateway (no trailing slash).",
)
@click.option(
    "--workspace-url",
    default=lambda: os.environ.get("DATABRICKS_HOST", DEFAULT_WORKSPACE_URL),
    show_default="$DATABRICKS_HOST or known workspace",
    help="Databricks workspace URL for token generation.",
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    default=False,
    help="Print detailed request/response info.",
)
def main(app_url: str, workspace_url: str, verbose: bool) -> None:
    """Validate the Better Agent Gateway end-to-end."""
    app_url = app_url.rstrip("/")
    workspace_url = workspace_url.rstrip("/")
    if not workspace_url.startswith("https://"):
        workspace_url = f"https://{workspace_url}"

    click.echo(f"{BOLD}Better Agent Gateway Validation{RESET}")
    click.echo(f"  App URL:       {app_url}")
    click.echo(f"  Workspace URL: {workspace_url}")

    results: list[bool] = []

    # --- Phase 1: Unauthenticated checks ---
    _section("Phase 1: Unauthenticated checks")
    results.append(check_health(app_url, verbose))
    results.append(check_no_auth_rejected(app_url, verbose))
    results.append(check_pat_rejected(app_url, verbose))

    # --- Get a token for authenticated checks ---
    _section("Phase 2: Authenticated checks")
    try:
        _info("Obtaining OAuth token via Databricks SDK...")
        token = _get_token(workspace_url)
        _pass("Token obtained", f"(length={len(token)})")
    except Exception as exc:
        _fail("Token obtained", str(exc))
        click.echo(
            f"\n{RED}Cannot continue without a valid token. "
            f"Run: databricks auth login --host {workspace_url}{RESET}"
        )
        sys.exit(1)

    results.append(check_bearer_auth(app_url, token, verbose))
    results.append(check_model_discovery(app_url, token, verbose))
    results.append(check_chat_completion(app_url, token, verbose))

    # --- Phase 3: Security model checks ---
    _section("Phase 3: Security model (scope enforcement + identity)")
    scope_ok, perm_body = check_scope_enforcement(app_url, token, verbose)
    results.append(scope_ok)
    results.append(check_identity_attribution(perm_body, workspace_url, verbose))

    # --- Summary ---
    passed = sum(results)
    total = len(results)
    _section("Summary")
    if passed == total:
        click.echo(f"  {GREEN}{BOLD}{passed}/{total} checks passed{RESET}")
    else:
        click.echo(
            f"  {RED}{BOLD}{passed}/{total} checks passed "
            f"({total - passed} failed){RESET}"
        )

    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
