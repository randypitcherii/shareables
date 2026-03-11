"""
Prototype: Headless authentication to a Databricks App.

Tries three approaches to programmatically authenticate:
  1. Follow the OAuth redirect chain with httpx
  2. Use the Databricks SDK token as a bearer header
  3. Use the app's own OAuth client_id to get a scoped token

Usage:
    uv run python scripts/headless_auth_test.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import httpx
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

APP_URL = os.getenv("GATEWAY_URL", "https://YOUR-APP.aws.databricksapps.com")
WORKSPACE_URL = os.getenv("DATABRICKS_HOST", "https://YOUR-WORKSPACE.cloud.databricks.com")
HEALTH_ENDPOINT = f"{APP_URL}/api/v1/healthz"

# The OAuth client_id that the app proxy uses (visible in the redirect URL).
# Set via env var — find yours by inspecting the OAuth redirect when visiting the app.
APP_OAUTH_CLIENT_ID = os.getenv("APP_OAUTH_CLIENT_ID", "")

SEPARATOR = "\n" + "=" * 72


def print_header(title: str) -> None:
    print(f"{SEPARATOR}")
    print(f"  {title}")
    print("=" * 72)


def print_redirect_chain(response: httpx.Response) -> None:
    """Print the full redirect chain from an httpx response."""
    for i, r in enumerate(response.history):
        print(f"  [{i}] {r.status_code} -> {r.headers.get('location', '(no location)')}")
    print(f"  [final] {response.status_code} {response.url}")


def print_response_summary(response: httpx.Response, body_preview_len: int = 500) -> None:
    """Print status, key headers, and a body preview."""
    print(f"  Status: {response.status_code}")
    print(f"  URL:    {response.url}")
    ct = response.headers.get("content-type", "")
    print(f"  Content-Type: {ct}")
    for h in ("www-authenticate", "set-cookie", "x-databricks-reason"):
        if h in response.headers:
            print(f"  {h}: {response.headers[h]}")
    body = response.text[:body_preview_len]
    print(f"  Body preview ({len(response.text)} chars total):\n    {body}")


# ---------------------------------------------------------------------------
# Approach 1: Follow the redirect chain
# ---------------------------------------------------------------------------
def approach_1_follow_redirects() -> httpx.Response | None:
    print_header("APPROACH 1: Follow OAuth redirect chain with httpx")

    print("\n> GET {APP_URL} with follow_redirects=True ...")
    with httpx.Client(follow_redirects=True, timeout=30) as client:
        resp = client.get(APP_URL)

    print("\nRedirect chain:")
    print_redirect_chain(resp)

    print("\nFinal response:")
    print_response_summary(resp)

    # Inspect the final URL to understand what the proxy wants
    final_url = str(resp.url)
    parsed = urlparse(final_url)
    qs = parse_qs(parsed.query)

    if "client_id" in qs:
        print(f"\n  -> OAuth client_id from redirect: {qs['client_id']}")
    if "scope" in qs:
        print(f"  -> Requested scopes: {qs['scope']}")
    if "redirect_uri" in qs:
        print(f"  -> Redirect URI: {qs['redirect_uri']}")
    if "code_challenge" in qs:
        print(f"  -> PKCE code_challenge present: yes")

    if resp.status_code == 200:
        print("\n  ** Got 200 — checking if it's the actual app or a login page...")
        if "login" in final_url.lower() or "<form" in resp.text.lower():
            print("  ** Looks like a login page, not the app.")
        else:
            print("  ** Might be the actual app content!")
            return resp
    return None


# ---------------------------------------------------------------------------
# Approach 2: SDK token as bearer
# ---------------------------------------------------------------------------
def approach_2_sdk_bearer() -> httpx.Response | None:
    print_header("APPROACH 2: Databricks SDK token as bearer header")

    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("  ERROR: databricks-sdk not installed. Skipping.")
        return None

    print("\n> Initializing WorkspaceClient ...")
    w = WorkspaceClient(host=WORKSPACE_URL)
    print(f"  Config auth type: {w.config.auth_type}")
    print(f"  Host: {w.config.host}")

    print("\n> Getting auth headers from SDK ...")
    headers = w.config.authenticate()
    # Redact the token for display
    for k, v in headers.items():
        display_val = v[:20] + "..." + v[-10:] if len(v) > 40 else v
        print(f"  {k}: {display_val}")

    # 2a: Try the app URL directly with the SDK token
    print(f"\n> GET {APP_URL} with SDK auth headers (no redirects) ...")
    with httpx.Client(follow_redirects=False, timeout=30) as client:
        resp = client.get(APP_URL, headers=headers)
    print_response_summary(resp)

    # 2b: Try the app URL with redirects + auth header
    print(f"\n> GET {APP_URL} with SDK auth headers (with redirects) ...")
    with httpx.Client(follow_redirects=True, timeout=30) as client:
        resp_redir = client.get(APP_URL, headers=headers)
    print("\nRedirect chain:")
    print_redirect_chain(resp_redir)
    print("\nFinal response:")
    print_response_summary(resp_redir)

    # 2c: Try the health endpoint directly
    print(f"\n> GET {HEALTH_ENDPOINT} with SDK auth headers ...")
    with httpx.Client(follow_redirects=False, timeout=30) as client:
        resp_health = client.get(HEALTH_ENDPOINT, headers=headers)
    print_response_summary(resp_health)

    # 2d: Try with Authorization header AND follow redirects
    print(f"\n> GET {HEALTH_ENDPOINT} with SDK auth headers (with redirects) ...")
    with httpx.Client(follow_redirects=True, timeout=30) as client:
        resp_health_redir = client.get(HEALTH_ENDPOINT, headers=headers)
    print("\nRedirect chain:")
    print_redirect_chain(resp_health_redir)
    print("\nFinal response:")
    print_response_summary(resp_health_redir)

    for r in [resp, resp_health, resp_health_redir]:
        if r.status_code == 200:
            print("\n  ** Got 200 on one of the attempts!")
            return r
    return None


# ---------------------------------------------------------------------------
# Approach 3: App-specific OAuth client_id with SDK
# ---------------------------------------------------------------------------
def approach_3_app_oauth() -> httpx.Response | None:
    print_header("APPROACH 3: App-specific OAuth flow")

    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.config import Config
        from databricks.sdk.oauth import (
            OAuthClient,
            TokenCache,
            SessionCredentials,
        )
    except ImportError as e:
        print(f"  ERROR: Missing import: {e}. Skipping.")
        return None

    # Try to discover OAuth endpoints from workspace
    print(f"\n> Fetching OAuth metadata from workspace ...")
    oidc_url = f"{WORKSPACE_URL}/oidc/.well-known/oauth-authorization-server"
    print(f"  GET {oidc_url}")
    with httpx.Client(timeout=30) as client:
        oidc_resp = client.get(oidc_url)
    print(f"  Status: {oidc_resp.status_code}")
    if oidc_resp.status_code == 200:
        oidc_data = oidc_resp.json()
        print(f"  authorization_endpoint: {oidc_data.get('authorization_endpoint')}")
        print(f"  token_endpoint:         {oidc_data.get('token_endpoint')}")
        scopes_supported = oidc_data.get("scopes_supported", [])
        print(f"  scopes_supported:       {scopes_supported}")
    else:
        print(f"  Could not fetch OIDC metadata: {oidc_resp.text[:300]}")
        oidc_data = {}

    # Try to use the SDK's OAuthClient with the app's client_id
    print(f"\n> Attempting OAuthClient with app client_id: {APP_OAUTH_CLIENT_ID}")
    print("  NOTE: This may fail if the client requires a secret or specific redirect_uri")

    try:
        oauth_client = OAuthClient(
            host=WORKSPACE_URL,
            client_id=APP_OAUTH_CLIENT_ID,
            redirect_url=f"{APP_URL}/oidc/v1/callback",
            scopes=["serving.serving-endpoints"],
        )
        print(f"  OAuthClient created successfully")
        print(f"  authorize_url: {oauth_client.authorize_url if hasattr(oauth_client, 'authorize_url') else '(not exposed)'}")

        # Check what methods are available
        print(f"  OAuthClient methods: {[m for m in dir(oauth_client) if not m.startswith('_')]}")

        # Try to initiate consent — this would normally open a browser
        print("\n  > Attempting to get a consent URL (not opening browser)...")
        if hasattr(oauth_client, "initiate_consent"):
            consent = oauth_client.initiate_consent()
            print(f"  Consent URL: {consent.auth_url if hasattr(consent, 'auth_url') else consent}")
        else:
            print("  initiate_consent not available on OAuthClient")

    except Exception as e:
        print(f"  OAuthClient creation failed: {type(e).__name__}: {e}")

    # Alternative: Try to exchange existing SDK token for an app-scoped token
    print(f"\n> Trying token exchange (RFC 8693) with SDK token ...")
    try:
        w = WorkspaceClient(host=WORKSPACE_URL)
        sdk_headers = w.config.authenticate()
        sdk_token = sdk_headers.get("Authorization", "").replace("Bearer ", "")

        if not sdk_token:
            print("  No bearer token from SDK, cannot attempt exchange.")
            return None

        token_endpoint = oidc_data.get("token_endpoint", f"{WORKSPACE_URL}/oidc/v1/token")
        print(f"  Token endpoint: {token_endpoint}")

        exchange_payload = {
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token": sdk_token,
            "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "scope": "serving.serving-endpoints",
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
        }
        print(f"  Exchange payload (token redacted): grant_type={exchange_payload['grant_type']}, scope={exchange_payload['scope']}")

        with httpx.Client(timeout=30) as client:
            exchange_resp = client.post(token_endpoint, data=exchange_payload)
        print(f"  Status: {exchange_resp.status_code}")
        print(f"  Response: {exchange_resp.text[:500]}")

        if exchange_resp.status_code == 200:
            new_token = exchange_resp.json().get("access_token")
            if new_token:
                print("\n  ** Got exchanged token! Trying health endpoint ...")
                with httpx.Client(timeout=30) as client:
                    health_resp = client.get(
                        HEALTH_ENDPOINT,
                        headers={"Authorization": f"Bearer {new_token}"},
                    )
                print_response_summary(health_resp)
                if health_resp.status_code == 200:
                    return health_resp

    except Exception as e:
        print(f"  Token exchange failed: {type(e).__name__}: {e}")

    return None


# ---------------------------------------------------------------------------
# Bonus: Try the Databricks Apps API to see if there's a token endpoint
# ---------------------------------------------------------------------------
def bonus_apps_api_inspection() -> None:
    print_header("BONUS: Inspect Databricks Apps API for auth hints")

    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("  ERROR: databricks-sdk not installed. Skipping.")
        return

    w = WorkspaceClient(host=WORKSPACE_URL)

    print("\n> Listing apps to find our app's details ...")
    try:
        apps = list(w.apps.list())
        for app in apps:
            print(f"  App: {app.name}")
            if hasattr(app, "url"):
                print(f"    URL: {app.url}")
            if hasattr(app, "service_principal_id"):
                print(f"    Service Principal ID: {app.service_principal_id}")
            if hasattr(app, "service_principal_name"):
                print(f"    Service Principal Name: {app.service_principal_name}")
            # Print all attributes for inspection
            for attr in sorted(vars(app)):
                if not attr.startswith("_"):
                    val = getattr(app, attr)
                    if val is not None:
                        print(f"    {attr}: {val}")
    except Exception as e:
        print(f"  Failed to list apps: {type(e).__name__}: {e}")

    # Check if there's an OAuth token endpoint specific to the app
    print(f"\n> Checking app-specific OIDC metadata ...")
    app_oidc_url = f"{APP_URL}/.well-known/oauth-authorization-server"
    with httpx.Client(follow_redirects=True, timeout=30) as client:
        resp = client.get(app_oidc_url)
    print(f"  GET {app_oidc_url}")
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 200:
        print(f"  Response: {resp.text[:500]}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    print("Databricks App Headless Auth Prototype")
    print(f"  App URL:       {APP_URL}")
    print(f"  Workspace URL: {WORKSPACE_URL}")
    print(f"  Health check:  {HEALTH_ENDPOINT}")

    results: dict[str, httpx.Response | None] = {}

    results["approach_1"] = approach_1_follow_redirects()
    results["approach_2"] = approach_2_sdk_bearer()
    results["approach_3"] = approach_3_app_oauth()
    bonus_apps_api_inspection()

    # Summary
    print_header("SUMMARY")
    for name, resp in results.items():
        if resp and resp.status_code == 200:
            print(f"  {name}: SUCCESS (200)")
        elif resp:
            print(f"  {name}: PARTIAL (status {resp.status_code})")
        else:
            print(f"  {name}: FAILED / NO 200 RESPONSE")

    # If any approach worked, try the health endpoint
    working = [r for r in results.values() if r and r.status_code == 200]
    if working:
        print("\n  At least one approach succeeded!")
    else:
        print("\n  No approach achieved a 200. Review output above for clues.")


if __name__ == "__main__":
    main()
