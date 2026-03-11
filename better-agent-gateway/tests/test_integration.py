"""Integration tests for the deployed Better Agent Gateway.

These tests verify the live app works with Service Principal OAuth
authentication. PAT auth is intentionally rejected by the Databricks Apps
proxy (it only accepts OAuth tokens), so PAT tests verify the 401 behavior.

Run with: uv run pytest tests/test_integration.py -m integration
"""

import os

import httpx
import pytest
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Environment configuration
# ---------------------------------------------------------------------------
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
APP_URL = os.getenv("APP_URL", "").rstrip("/")
PAT = os.getenv("INTEGRATION_TEST_PAT", "")
SP_CLIENT_ID = os.getenv("INTEGRATION_TEST_SP_CLIENT_ID", "")
SP_CLIENT_SECRET = os.getenv("INTEGRATION_TEST_SP_CLIENT_SECRET", "")

_missing_base = not (DATABRICKS_HOST and APP_URL)
_missing_pat = _missing_base or not PAT
_missing_sp = _missing_base or not (SP_CLIENT_ID and SP_CLIENT_SECRET)

skip_pat = pytest.mark.skipif(_missing_pat, reason="PAT credentials not configured")
skip_sp = pytest.mark.skipif(_missing_sp, reason="SP credentials not configured")

# Generous timeout for Databricks Apps cold-start
TIMEOUT = httpx.Timeout(60.0, connect=15.0)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def app_url() -> str:
    return APP_URL


@pytest.fixture(scope="module")
def pat_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {PAT}"}


@pytest.fixture(scope="module")
def sp_token() -> str:
    """Exchange SP client credentials for an OAuth token via the workspace OIDC endpoint."""
    token_url = f"{DATABRICKS_HOST.rstrip('/')}/oidc/v1/token"
    response = httpx.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": SP_CLIENT_ID,
            "client_secret": SP_CLIENT_SECRET,
            "scope": "all-apis",
        },
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return response.json()["access_token"]


@pytest.fixture(scope="module")
def sp_headers(sp_token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {sp_token}"}


def _get_available_model(base_url: str, headers: dict[str, str]) -> str:
    """Fetch the models list and return the first available alias or endpoint name."""
    resp = httpx.get(f"{base_url}/api/v1/models", headers=headers, timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    # Prefer aliases (friendlier names), fall back to raw endpoint names
    if data.get("aliases"):
        return next(iter(data["aliases"]))
    if data.get("endpoints"):
        return data["endpoints"][0]["name"]
    pytest.fail("No models available on the gateway")


@pytest.fixture(scope="module")
def available_model_sp(app_url: str, sp_headers: dict[str, str]) -> str:
    return _get_available_model(app_url, sp_headers)


# ===========================================================================
# PAT Auth Tests — Databricks Apps proxy rejects PATs (OAuth only)
# ===========================================================================


@pytest.mark.integration
@skip_pat
def test_pat_rejected_by_app_proxy(app_url: str, pat_headers: dict[str, str]):
    """Databricks Apps proxy only accepts OAuth tokens, not PATs.
    Verify the expected 401 so we document this constraint."""
    resp = httpx.get(f"{app_url}/api/v1/healthz", headers=pat_headers, timeout=TIMEOUT)
    assert resp.status_code == 401, (
        f"Expected 401 (PATs rejected by Apps proxy), got {resp.status_code}"
    )


# ===========================================================================
# Service Principal OAuth Tests
# ===========================================================================


@pytest.mark.integration
@skip_sp
def test_sp_health_check(app_url: str, sp_headers: dict[str, str]):
    resp = httpx.get(f"{app_url}/api/v1/healthz", headers=sp_headers, timeout=TIMEOUT)
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


@pytest.mark.integration
@skip_sp
def test_sp_list_models(app_url: str, sp_headers: dict[str, str]):
    resp = httpx.get(f"{app_url}/api/v1/models", headers=sp_headers, timeout=TIMEOUT)
    assert resp.status_code == 200
    data = resp.json()
    assert "aliases" in data
    assert "endpoints" in data


@pytest.mark.integration
@skip_sp
def test_sp_chat_completion(
    app_url: str,
    sp_headers: dict[str, str],
    available_model_sp: str,
):
    payload = {
        "model": available_model_sp,
        "messages": [{"role": "user", "content": "Say hello in one word."}],
    }
    resp = httpx.post(
        f"{app_url}/api/v1/chat/completions",
        headers=sp_headers,
        json=payload,
        timeout=TIMEOUT,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "choices" in data
    assert len(data["choices"]) > 0
