# Better Agent Gateway Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Databricks App that proxies AI Gateway serving endpoints with scoped OAuth (users authenticate as themselves but limited to LLM access) and automatic `-latest` model alias resolution across Claude, GPT, Gemini, and GPT-Codex families.

**Architecture:** FastAPI backend proxies chat completions requests to Databricks Model Serving endpoints using the user's OBO (on-behalf-of) token from `X-Forwarded-Access-Token`. The proxy layer enforces scope — it only forwards requests to `/serving-endpoints/*/invocations`, so even though the OBO token carries the user's full identity, the app restricts what actions are possible. An alias registry maps `*-latest` aliases to actual serving endpoint names by querying the workspace's available endpoints at startup and on a configurable refresh interval. React frontend provides a dashboard for viewing available models, alias mappings, usage stats, and health.

**Tech Stack:** Python 3.11+ / FastAPI / uvicorn / Databricks SDK / httpx (async HTTP client for proxying) / React 19 / TypeScript / Vite / Vitest

**Existing code:** The `llm-app-scoped-oauth/` directory has a working skeleton with session management, alias resolution (2 hardcoded aliases), policy engine, audit log, and 4 passing tests. This plan renames it to `better-agent-gateway/` and evolves each component.

---

## File Structure

### Renamed directory: `better-agent-gateway/`

```
better-agent-gateway/
  app.py                          # FastAPI entry point, CORS, SPA fallback
  app.yaml                        # Databricks Apps manifest with OAuth scopes
  pyproject.toml                  # Python deps (add databricks-sdk, httpx, python-dotenv)
  requirements.txt                # Runtime deps for Databricks Apps
  template.env                    # Documented env vars
  databricks.yml                  # DABs bundle config
  bundle/
    resources/
      app.yml                     # DABs app resource definition
  server/
    __init__.py
    config.py                     # Settings dataclass, workspace client factory
    auth.py                       # OBO token extraction from headers, RequestContext
    alias_registry.py             # Dynamic -latest resolution via serving endpoint discovery
    proxy.py                      # httpx-based async proxy to serving endpoints
    audit.py                      # Audit logging (keep existing, minor enhancements)
    policy.py                     # Allowlist policy (keep existing)
    routes/
      __init__.py                 # APIRouter aggregator
      chat.py                     # /v1/chat/completions proxy endpoint
      models.py                   # /v1/models listing endpoint
      health.py                   # /v1/healthz
      audit.py                    # /v1/audit/* endpoints
  tests/
    conftest.py                   # Shared fixtures
    test_alias_registry.py        # Alias resolution unit tests
    test_auth.py                  # Auth extraction tests
    test_proxy.py                 # Proxy routing tests (mocked httpx)
    test_chat_endpoint.py         # Chat completions API contract tests
    test_models_endpoint.py       # Models listing tests
  frontend/
    package.json
    tsconfig.json
    tsconfig.app.json
    tsconfig.node.json
    vite.config.ts
    eslint.config.js
    index.html
    src/
      main.tsx
      App.tsx
      App.css
      index.css
      components/
        ModelTable.tsx             # Available models + alias mappings
        UsageStats.tsx             # Request counts, latency
        HealthBadge.tsx            # Backend health indicator
      test/
        setup.ts
```

---

## Chunk 1: Rename, Config, and Auth Foundation

### Task 1: Rename directory and update all references

**Files:**
- Rename: `llm-app-scoped-oauth/` -> `better-agent-gateway/`
- Modify: `better-agent-gateway/pyproject.toml`
- Modify: `better-agent-gateway/bundle/resources/app.yml`
- Modify: `better-agent-gateway/app.yaml`

- [ ] **Step 1: Rename the directory**

```bash
cd /Users/randy.pitcher/projects/shareables/.worktrees/llm-app-scoped-oauth-latest-endpoints
git mv llm-app-scoped-oauth better-agent-gateway
```

- [ ] **Step 2: Update pyproject.toml project name and add new deps**

```toml
[project]
name = "better-agent-gateway"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "databricks-sdk>=0.55.0",
    "fastapi>=0.135.1",
    "httpx>=0.28.1",
    "pydantic>=2.12.5",
    "python-dotenv>=1.1.0",
    "uvicorn[standard]>=0.41.0",
]

[dependency-groups]
dev = [
    "pytest>=9.0.2",
    "pytest-asyncio>=1.0.0",
    "httpx>=0.28.1",
]

[tool.pytest.ini_options]
asyncio_mode = "auto"
```

- [ ] **Step 3: Update requirements.txt**

```
databricks-sdk>=0.55.0
fastapi>=0.135.1
httpx>=0.28.1
pydantic>=2.12.5
python-dotenv>=1.1.0
uvicorn[standard]>=0.41.0
```

- [ ] **Step 4: Update bundle/resources/app.yml**

```yaml
resources:
  apps:
    better-agent-gateway:
      name: better-agent-gateway
      description: "Scoped OAuth LLM gateway with automatic -latest model resolution"
      source_code_path: .
```

- [ ] **Step 5: Update app.yaml with OAuth scopes**

```yaml
command:
  - "python"
  - "-m"
  - "uvicorn"
  - "app:app"
  - "--host"
  - "0.0.0.0"
  - "--port"
  - "8000"

env:
  - name: ALIAS_REFRESH_INTERVAL_SECONDS
    value: "300"
```

Note: Databricks Apps `app.yaml` does not support a `permissions` block for scoping OAuth. The OBO token carries the user's full workspace identity. **Scope enforcement happens at the proxy layer** — the gateway only forwards requests to `/serving-endpoints/*/invocations`, so even admin users can only perform LLM inference through this app. This is the key security architecture: identity from Databricks SSO, scope restriction from the proxy.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "rename llm-app-scoped-oauth to better-agent-gateway, add deps"
```

---

### Task 2: Create config.py — Settings and workspace client

**Files:**
- Create: `better-agent-gateway/server/config.py`
- Create: `better-agent-gateway/template.env`
- Test: `better-agent-gateway/tests/test_config.py` (deferred — config is simple dataclass)

- [ ] **Step 1: Create template.env**

```bash
# -- Databricks Connection --
# Set DATABRICKS_PROFILE for local dev, omit when running as Databricks App
# DATABRICKS_PROFILE=DEFAULT

# -- App Settings --
# APP_ENV=dev
# ALIAS_REFRESH_INTERVAL_SECONDS=300
# DEFAULT_MODEL_ALIAS=claude-sonnet-latest

# -- Serving Endpoint Prefix --
# Model families to auto-discover. Comma-separated prefixes.
# MODEL_FAMILY_PREFIXES=databricks-claude,databricks-gpt,databricks-gemini,databricks-codex
```

- [ ] **Step 2: Create server/config.py**

```python
from __future__ import annotations

import os
from dataclasses import dataclass, field

from databricks.sdk import WorkspaceClient


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _list_env(name: str, default: list[str] | None = None) -> list[str]:
    value = os.getenv(name)
    if not value:
        return default or []
    return [v.strip() for v in value.split(",") if v.strip()]


@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv("DATABRICKS_APP_NAME", "better-agent-gateway")
    is_databricks_app: bool = bool(os.getenv("DATABRICKS_APP_NAME"))
    default_model_alias: str = os.getenv("DEFAULT_MODEL_ALIAS", "claude-sonnet-latest")
    alias_refresh_interval_seconds: int = int(
        os.getenv("ALIAS_REFRESH_INTERVAL_SECONDS", "300")
    )
    model_family_prefixes: list[str] = field(
        default_factory=lambda: _list_env(
            "MODEL_FAMILY_PREFIXES",
            ["databricks-claude", "databricks-gpt", "databricks-gemini", "databricks-codex"],
        )
    )


settings = Settings()


def get_workspace_client() -> WorkspaceClient:
    profile = os.getenv("DATABRICKS_PROFILE")
    if profile and not settings.is_databricks_app:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient()
```

- [ ] **Step 3: Commit**

```bash
git add server/config.py template.env
git commit -m "feat: add config module with settings and workspace client factory"
```

---

### Task 3: Create auth.py — OBO token extraction

**Files:**
- Create: `better-agent-gateway/server/auth.py`
- Create: `better-agent-gateway/tests/test_auth.py`

- [ ] **Step 1: Write failing tests for auth**

```python
# tests/test_auth.py
import pytest
from server.auth import extract_request_context, RequestContext


def test_obo_token_from_forwarded_header():
    ctx = extract_request_context(
        x_forwarded_access_token="obo-token-123",
        x_forwarded_user_id="user@company.com",
        x_forwarded_email="user@company.com",
        authorization=None,
    )
    assert ctx.access_token == "obo-token-123"
    assert ctx.user_id == "user@company.com"
    assert ctx.auth_mode == "on-behalf-of-user"


def test_bearer_token_fallback():
    ctx = extract_request_context(
        x_forwarded_access_token=None,
        x_forwarded_user_id=None,
        x_forwarded_email=None,
        authorization="Bearer my-bearer-token",
    )
    assert ctx.access_token == "my-bearer-token"
    assert ctx.auth_mode == "bearer"


def test_missing_auth_raises():
    with pytest.raises(ValueError, match="No authentication"):
        extract_request_context(
            x_forwarded_access_token=None,
            x_forwarded_user_id=None,
            x_forwarded_email=None,
            authorization=None,
        )


def test_user_id_from_email_fallback():
    ctx = extract_request_context(
        x_forwarded_access_token="token",
        x_forwarded_user_id=None,
        x_forwarded_email="fallback@company.com",
        authorization=None,
    )
    assert ctx.user_id == "fallback@company.com"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd better-agent-gateway && uv run pytest tests/test_auth.py -v
```

Expected: FAIL — `server.auth` module doesn't exist yet.

- [ ] **Step 3: Implement auth.py**

```python
# server/auth.py
from __future__ import annotations

from dataclasses import dataclass

from fastapi import Header, HTTPException, status


@dataclass(frozen=True)
class RequestContext:
    access_token: str
    user_id: str
    auth_mode: str  # "on-behalf-of-user" | "bearer"


def extract_request_context(
    *,
    x_forwarded_access_token: str | None,
    x_forwarded_user_id: str | None,
    x_forwarded_email: str | None,
    authorization: str | None,
) -> RequestContext:
    """Extract auth context from Databricks App proxy headers.

    Priority:
    1. X-Forwarded-Access-Token (OBO token set by Databricks Apps proxy)
    2. Authorization: Bearer <token> (direct API calls / local dev)

    Raises ValueError if no authentication is present.
    """
    # OBO path — Databricks Apps sets these headers
    if x_forwarded_access_token:
        user_id = x_forwarded_user_id or x_forwarded_email or "unknown-user"
        return RequestContext(
            access_token=x_forwarded_access_token,
            user_id=user_id,
            auth_mode="on-behalf-of-user",
        )

    # Bearer token fallback for local dev / direct calls
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization[7:]
        if token:
            return RequestContext(
                access_token=token,
                user_id=x_forwarded_user_id or x_forwarded_email or "bearer-user",
                auth_mode="bearer",
            )

    raise ValueError("No authentication provided")


def get_request_context(
    x_forwarded_access_token: str | None = Header(default=None),
    x_forwarded_user_id: str | None = Header(default=None),
    x_forwarded_email: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
) -> RequestContext:
    """FastAPI dependency that extracts auth context from request headers."""
    try:
        return extract_request_context(
            x_forwarded_access_token=x_forwarded_access_token,
            x_forwarded_user_id=x_forwarded_user_id,
            x_forwarded_email=x_forwarded_email,
            authorization=authorization,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
        ) from exc
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_auth.py -v
```

Expected: 4 PASS

- [ ] **Step 5: Commit**

```bash
git add server/auth.py tests/test_auth.py
git commit -m "feat: add auth module with OBO token extraction and tests"
```

---

## Chunk 2: Dynamic Alias Registry

### Task 4: Rewrite alias_registry.py with dynamic endpoint discovery

**Files:**
- Modify: `better-agent-gateway/server/alias_registry.py`
- Create: `better-agent-gateway/tests/test_alias_registry.py`

The registry queries Databricks serving endpoints on startup (and periodically) to discover available models and build `-latest` aliases automatically.

Model family mapping logic:
- Scan all serving endpoints matching configured prefixes
- Parse endpoint names like `databricks-claude-sonnet-4-5` into family `claude-sonnet` + version `4.5`
- For each family, track the highest version as the `-latest` target
- Produce aliases: `claude-sonnet-latest` -> `databricks-claude-sonnet-4-5`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_alias_registry.py
from __future__ import annotations

import pytest
from unittest.mock import MagicMock
from server.alias_registry import AliasRegistry, parse_model_family


class TestParseModelFamily:
    def test_claude_sonnet(self):
        family, version = parse_model_family("databricks-claude-sonnet-4-5")
        assert family == "claude-sonnet"
        assert version == (4, 5)

    def test_claude_opus(self):
        family, version = parse_model_family("databricks-claude-opus-4-6")
        assert family == "claude-opus"
        assert version == (4, 6)

    def test_gpt_4o(self):
        family, version = parse_model_family("databricks-gpt-4o")
        assert family == "gpt-4o"

    def test_gpt_4o_mini(self):
        family, version = parse_model_family("databricks-gpt-4o-mini")
        assert family == "gpt-4o-mini"

    def test_gpt_4o_and_mini_are_separate_families(self):
        """GPT-4o and GPT-4o-mini should resolve to separate -latest aliases."""
        f1, _ = parse_model_family("databricks-gpt-4o")
        f2, _ = parse_model_family("databricks-gpt-4o-mini")
        assert f1 != f2

    def test_gemini_pro(self):
        family, version = parse_model_family("databricks-gemini-2-0-flash")
        assert family == "gemini"
        assert version == (2, 0)

    def test_codex(self):
        family, version = parse_model_family("databricks-codex-mini-latest")
        family2, version2 = parse_model_family("databricks-codex-mini-2025-01-24")
        assert family == "codex"
        assert family2 == "codex"

    def test_no_match(self):
        result = parse_model_family("my-custom-endpoint")
        assert result is None

    def test_with_date_suffix(self):
        """Endpoints with date suffixes like -20260215 should still parse."""
        family, version = parse_model_family("databricks-claude-sonnet-4-5-20260215")
        assert family == "claude-sonnet"
        assert version == (4, 5)


class TestAliasRegistry:
    def _mock_endpoints(self, names: list[str]) -> list:
        endpoints = []
        for name in names:
            ep = MagicMock()
            ep.name = name
            ep.state = MagicMock()
            ep.state.ready = "READY"
            endpoints.append(ep)
        return endpoints

    def test_resolve_latest_picks_highest_version(self):
        registry = AliasRegistry(prefixes=["databricks-claude", "databricks-gpt"])
        endpoints = self._mock_endpoints([
            "databricks-claude-sonnet-4-5",
            "databricks-claude-sonnet-4-6",
            "databricks-claude-opus-4-6",
            "databricks-gpt-4o",
        ])
        registry.refresh_from_endpoints(endpoints)

        assert registry.resolve("claude-sonnet-latest") == "databricks-claude-sonnet-4-6"
        assert registry.resolve("claude-opus-latest") == "databricks-claude-opus-4-6"
        assert registry.resolve("gpt-latest") == "databricks-gpt-4o"

    def test_resolve_passthrough(self):
        registry = AliasRegistry(prefixes=["databricks-claude"])
        registry.refresh_from_endpoints([])
        assert registry.resolve("my-custom-model") == "my-custom-model"

    def test_resolve_explicit_endpoint_name(self):
        """If user passes an actual endpoint name, pass it through."""
        registry = AliasRegistry(prefixes=["databricks-claude"])
        endpoints = self._mock_endpoints(["databricks-claude-sonnet-4-5"])
        registry.refresh_from_endpoints(endpoints)
        assert registry.resolve("databricks-claude-sonnet-4-5") == "databricks-claude-sonnet-4-5"

    def test_list_aliases(self):
        registry = AliasRegistry(prefixes=["databricks-claude"])
        endpoints = self._mock_endpoints([
            "databricks-claude-sonnet-4-5",
            "databricks-claude-opus-4-6",
        ])
        registry.refresh_from_endpoints(endpoints)
        aliases = registry.list_aliases()
        assert "claude-sonnet-latest" in aliases
        assert "claude-opus-latest" in aliases

    def test_list_available_endpoints(self):
        registry = AliasRegistry(prefixes=["databricks-claude"])
        endpoints = self._mock_endpoints(["databricks-claude-sonnet-4-5"])
        registry.refresh_from_endpoints(endpoints)
        available = registry.list_endpoints()
        assert "databricks-claude-sonnet-4-5" in available
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_alias_registry.py -v
```

Expected: FAIL — `parse_model_family` doesn't exist.

- [ ] **Step 3: Implement the new alias_registry.py**

```python
# server/alias_registry.py
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Known model family patterns.
# Each tuple: (prefix_to_strip, family_name, version_regex_after_family)
# Order matters — first match wins.
_FAMILY_PATTERNS: list[tuple[str, str, re.Pattern[str]]] = [
    ("databricks-claude-sonnet-", "claude-sonnet", re.compile(r"^(\d+)[-.](\d+)(?:-\d{8})?$")),
    ("databricks-claude-opus-", "claude-opus", re.compile(r"^(\d+)[-.](\d+)(?:-\d{8})?$")),
    ("databricks-claude-haiku-", "claude-haiku", re.compile(r"^(\d+)[-.](\d+)(?:-\d{8})?$")),
    ("databricks-gemini-", "gemini", re.compile(r"^(\d+)[-.](\d+)")),
    ("databricks-codex-", "codex", re.compile(r"^(.+)$")),
    ("databricks-gpt-4o-mini", "gpt-4o-mini", re.compile(r"^(?:-(\d{4}-\d{2}-\d{2}))?$")),
    ("databricks-gpt-4o", "gpt-4o", re.compile(r"^(?:-(\d{4}-\d{2}-\d{2}))?$")),
    ("databricks-gpt-", "gpt", re.compile(r"^(\d+)")),
]


def parse_model_family(endpoint_name: str) -> tuple[str, tuple[int, ...]] | None:
    """Parse a serving endpoint name into (family, version_tuple) or None."""
    for prefix, family, version_re in _FAMILY_PATTERNS:
        if not endpoint_name.startswith(prefix):
            continue
        remainder = endpoint_name[len(prefix):]
        match = version_re.match(remainder)
        if match:
            # Extract numeric groups as version tuple
            version_parts = []
            for g in match.groups():
                try:
                    version_parts.append(int(g))
                except ValueError:
                    pass  # Non-numeric group (e.g., codex variant name)
            return family, tuple(version_parts)
        # Prefix matched but version didn't — still assign to family with empty version
        return family, ()
    return None


@dataclass
class AliasMapping:
    alias: str
    target_endpoint: str
    family: str
    version: tuple[int, ...]


class AliasRegistry:
    def __init__(self, prefixes: list[str] | None = None) -> None:
        self._prefixes = prefixes or [
            "databricks-claude",
            "databricks-gpt",
            "databricks-gemini",
            "databricks-codex",
        ]
        self._aliases: dict[str, AliasMapping] = {}
        self._endpoints: set[str] = set()

    def refresh_from_endpoints(self, endpoints: list) -> None:
        """Rebuild alias mappings from a list of serving endpoint objects."""
        family_best: dict[str, tuple[tuple[int, ...], str]] = {}
        ready_endpoints: set[str] = set()

        for ep in endpoints:
            name = ep.name
            # Check if endpoint matches any configured prefix
            if not any(name.startswith(p) for p in self._prefixes):
                continue

            # Only include ready endpoints
            state = getattr(ep, "state", None)
            ready = getattr(state, "ready", None) if state else None
            if ready and str(ready).upper() != "READY":
                continue

            ready_endpoints.add(name)
            parsed = parse_model_family(name)
            if not parsed:
                continue

            family, version = parsed
            current_best = family_best.get(family)
            if current_best is None or version > current_best[0]:
                family_best[family] = (version, name)

        # Build alias mappings
        new_aliases: dict[str, AliasMapping] = {}
        for family, (version, endpoint_name) in family_best.items():
            alias = f"{family}-latest"
            new_aliases[alias] = AliasMapping(
                alias=alias,
                target_endpoint=endpoint_name,
                family=family,
                version=version,
            )
            logger.info("Alias %s -> %s", alias, endpoint_name)

        self._aliases = new_aliases
        self._endpoints = ready_endpoints
        logger.info(
            "Refreshed aliases: %d aliases from %d endpoints",
            len(new_aliases),
            len(ready_endpoints),
        )

    def resolve(self, alias_or_model: str) -> str:
        """Resolve an alias to an endpoint name, or pass through unchanged."""
        mapping = self._aliases.get(alias_or_model)
        if mapping:
            return mapping.target_endpoint
        return alias_or_model

    def list_aliases(self) -> dict[str, str]:
        """Return {alias: target_endpoint} for all current aliases."""
        return {a: m.target_endpoint for a, m in self._aliases.items()}

    def list_endpoints(self) -> list[str]:
        """Return sorted list of discovered endpoint names."""
        return sorted(self._endpoints)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_alias_registry.py -v
```

Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add server/alias_registry.py tests/test_alias_registry.py
git commit -m "feat: dynamic alias registry with endpoint discovery and -latest resolution"
```

---

## Chunk 3: LLM Proxy and Chat Completions Endpoint

### Task 5: Create proxy.py — async HTTP proxy to serving endpoints

**Files:**
- Create: `better-agent-gateway/server/proxy.py`
- Create: `better-agent-gateway/tests/test_proxy.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_proxy.py
from __future__ import annotations

import json
import pytest
import httpx
from unittest.mock import AsyncMock, patch, MagicMock

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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_proxy.py -v
```

Expected: FAIL — module doesn't exist.

- [ ] **Step 3: Implement proxy.py**

```python
# server/proxy.py
from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = httpx.Timeout(60.0, connect=10.0)


class ServingEndpointProxy:
    """Async proxy that forwards chat completion requests to Databricks serving endpoints."""

    def __init__(self, workspace_host: str, timeout: httpx.Timeout | None = None) -> None:
        self._workspace_host = workspace_host.rstrip("/")
        self._client = httpx.AsyncClient(timeout=timeout or _DEFAULT_TIMEOUT)

    def _build_url(self, endpoint_name: str) -> str:
        return f"{self._workspace_host}/serving-endpoints/{endpoint_name}/invocations"

    async def chat_completion(
        self,
        *,
        endpoint_name: str,
        access_token: str,
        messages: list[dict[str, Any]],
        max_tokens: int | None = None,
        temperature: float | None = None,
        stream: bool = False,
        extra_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Proxy a chat completion request to the resolved serving endpoint.

        Uses the caller's access_token (OBO) so the request runs with their identity.
        """
        url = self._build_url(endpoint_name)
        payload: dict[str, Any] = {"messages": messages}
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        if temperature is not None:
            payload["temperature"] = temperature
        if stream:
            payload["stream"] = True
        if extra_params:
            payload.update(extra_params)

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        logger.info("Proxying to %s for endpoint %s", url, endpoint_name)
        response = await self._client.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()

    async def close(self) -> None:
        await self._client.aclose()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_proxy.py -v
```

Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add server/proxy.py tests/test_proxy.py
git commit -m "feat: add async serving endpoint proxy with OBO token forwarding"
```

---

### Task 6: Create routes — chat completions, models listing, health

**Files:**
- Create: `better-agent-gateway/server/routes/__init__.py`
- Create: `better-agent-gateway/server/routes/chat.py`
- Create: `better-agent-gateway/server/routes/models.py`
- Create: `better-agent-gateway/server/routes/health.py`
- Create: `better-agent-gateway/server/routes/audit_routes.py`
- Remove: `better-agent-gateway/server/routes.py` (old monolithic file)
- Create: `better-agent-gateway/tests/test_chat_endpoint.py`
- Create: `better-agent-gateway/tests/test_models_endpoint.py`
- Modify: `better-agent-gateway/tests/conftest.py`

- [ ] **Step 1: Update conftest.py with singleton reset fixtures**

```python
# tests/conftest.py
import sys
from pathlib import Path

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))


def pytest_runtest_setup(item):
    """Reset module-level singletons before each test to prevent pollution."""
    import server.routes.chat as chat_mod
    chat_mod._proxy = None

    import server.alias_registry as alias_mod
    alias_mod._registry = None

    import server.audit as audit_mod
    audit_mod._store = None
```

- [ ] **Step 2: Write failing tests for chat completions endpoint**

```python
# tests/test_chat_endpoint.py
from __future__ import annotations

from unittest.mock import AsyncMock, patch, MagicMock

import pytest
from fastapi.testclient import TestClient

from app import app

client = TestClient(app)


def test_chat_completions_requires_auth():
    response = client.post("/api/v1/chat/completions", json={
        "model": "claude-sonnet-latest",
        "messages": [{"role": "user", "content": "Hello"}],
    })
    assert response.status_code == 401


def test_chat_completions_resolves_alias_and_proxies():
    mock_result = {
        "id": "chatcmpl-123",
        "choices": [{"message": {"role": "assistant", "content": "Hi!"}}],
        "model": "databricks-claude-sonnet-4-6",
    }
    with patch("server.routes.chat._get_proxy") as mock_proxy_fn:
        mock_proxy = AsyncMock()
        mock_proxy.chat_completion = AsyncMock(return_value=mock_result)
        mock_proxy_fn.return_value = mock_proxy

        response = client.post(
            "/api/v1/chat/completions",
            headers={
                "x-forwarded-access-token": "user-token",
                "x-forwarded-user-id": "user@co.com",
            },
            json={
                "model": "claude-sonnet-latest",
                "messages": [{"role": "user", "content": "Hello"}],
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["choices"][0]["message"]["content"] == "Hi!"


def test_chat_completions_passes_optional_params():
    mock_result = {"id": "chatcmpl-123", "choices": []}
    with patch("server.routes.chat._get_proxy") as mock_proxy_fn:
        mock_proxy = AsyncMock()
        mock_proxy.chat_completion = AsyncMock(return_value=mock_result)
        mock_proxy_fn.return_value = mock_proxy

        response = client.post(
            "/api/v1/chat/completions",
            headers={
                "x-forwarded-access-token": "user-token",
                "x-forwarded-user-id": "user@co.com",
            },
            json={
                "model": "claude-sonnet-latest",
                "messages": [{"role": "user", "content": "Hello"}],
                "max_tokens": 500,
                "temperature": 0.7,
            },
        )

    assert response.status_code == 200
    call_kwargs = mock_proxy.chat_completion.call_args.kwargs
    assert call_kwargs["max_tokens"] == 500
    assert call_kwargs["temperature"] == 0.7
```

- [ ] **Step 3: Write failing tests for models endpoint**

```python
# tests/test_models_endpoint.py
from fastapi.testclient import TestClient

from app import app

client = TestClient(app)


def test_models_list_returns_aliases_and_endpoints():
    response = client.get("/api/v1/models")
    assert response.status_code == 200
    body = response.json()
    assert "aliases" in body
    assert "endpoints" in body


def test_health_check():
    response = client.get("/api/v1/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
```

- [ ] **Step 4: Run tests to verify they fail**

```bash
uv run pytest tests/test_chat_endpoint.py tests/test_models_endpoint.py -v
```

Expected: FAIL

- [ ] **Step 5: Create server/routes/__init__.py**

```python
# server/routes/__init__.py
from fastapi import APIRouter

from .chat import router as chat_router
from .models import router as models_router
from .health import router as health_router
from .audit_routes import router as audit_router

api_router = APIRouter(prefix="/api")
api_router.include_router(chat_router, tags=["chat"])
api_router.include_router(models_router, tags=["models"])
api_router.include_router(health_router, tags=["health"])
api_router.include_router(audit_router, tags=["audit"])
```

- [ ] **Step 6: Create server/routes/chat.py**

```python
# server/routes/chat.py
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from ..auth import RequestContext, get_request_context
from ..audit import get_audit_store
from ..proxy import ServingEndpointProxy
from ..config import get_workspace_client

# Module-level proxy singleton (initialized lazily)
_proxy: ServingEndpointProxy | None = None

router = APIRouter()


def _get_proxy() -> ServingEndpointProxy:
    global _proxy
    if _proxy is None:
        from ..config import get_workspace_client
        host = get_workspace_client().config.host or ""
        if not host.startswith("http"):
            host = f"https://{host}"
        _proxy = ServingEndpointProxy(workspace_host=host)
    return _proxy


def _get_alias_registry():
    from ..alias_registry import get_registry
    return get_registry()


class ChatCompletionRequest(BaseModel):
    model: str = Field(description="Model alias or endpoint name")
    messages: list[dict[str, Any]]
    max_tokens: int | None = None
    temperature: float | None = None
    stream: bool = False


@router.post("/v1/chat/completions")
async def chat_completions(
    payload: ChatCompletionRequest,
    context: RequestContext = Depends(get_request_context),
):
    registry = _get_alias_registry()
    resolved_endpoint = registry.resolve(payload.model)

    get_audit_store().add(
        user_id=context.user_id,
        action="chat_completion",
        decision="allowed",
        reason="allowed",
        metadata={
            "requested_model": payload.model,
            "resolved_endpoint": resolved_endpoint,
            "auth_mode": context.auth_mode,
        },
    )

    try:
        proxy = _get_proxy()
        result = await proxy.chat_completion(
            endpoint_name=resolved_endpoint,
            access_token=context.access_token,
            messages=payload.messages,
            max_tokens=payload.max_tokens,
            temperature=payload.temperature,
            stream=payload.stream,
        )
        return result
    except Exception as exc:
        get_audit_store().add(
            user_id=context.user_id,
            action="chat_completion_error",
            decision="error",
            reason=str(exc)[:200],
            metadata={"endpoint": resolved_endpoint},
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Serving endpoint error: {exc}",
        ) from exc
```

- [ ] **Step 7: Create server/routes/models.py**

```python
# server/routes/models.py
from fastapi import APIRouter

router = APIRouter()


def _get_alias_registry():
    from ..alias_registry import get_registry
    return get_registry()


@router.get("/v1/models")
def list_models():
    registry = _get_alias_registry()
    return {
        "aliases": registry.list_aliases(),
        "endpoints": registry.list_endpoints(),
    }
```

- [ ] **Step 8: Create server/routes/health.py**

```python
# server/routes/health.py
from fastapi import APIRouter

router = APIRouter()


@router.get("/v1/healthz")
def healthz():
    return {"status": "ok"}
```

- [ ] **Step 9: Create server/routes/audit_routes.py**

```python
# server/routes/audit_routes.py
from fastapi import APIRouter, HTTPException, status

from ..audit import get_audit_store

router = APIRouter()


@router.get("/v1/audit/requests/{request_id}")
def get_audit_request(request_id: str):
    record = get_audit_store().get(request_id)
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Request audit not found",
        )
    return record
```

- [ ] **Step 10: Add singleton accessor to audit.py**

Add to the bottom of `server/audit.py`:

```python
# Module-level singleton
_store: AuditStore | None = None


def get_audit_store() -> AuditStore:
    global _store
    if _store is None:
        _store = AuditStore()
    return _store
```

- [ ] **Step 11: Add module-level registry accessor to alias_registry.py**

Add to the bottom of `server/alias_registry.py`:

```python
# Module-level singleton
_registry: AliasRegistry | None = None


def get_registry() -> AliasRegistry:
    global _registry
    if _registry is None:
        from .config import settings
        _registry = AliasRegistry(prefixes=settings.model_family_prefixes)
    return _registry
```

- [ ] **Step 12: Rewrite app.py as the new entry point**

```python
# app.py
import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv

# Load env file early
runtime_env = os.getenv("APP_ENV", "dev").lower()
env_file = Path(__file__).parent / f"{runtime_env}.env"
if env_file.exists():
    load_dotenv(env_file)

from server.routes import api_router

app = FastAPI(
    title="Better Agent Gateway",
    description="Scoped OAuth LLM gateway with automatic -latest model resolution",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

# SPA frontend serving
frontend_dist = Path(__file__).parent / "frontend" / "dist"
assets_dir = frontend_dist / "assets"

if assets_dir.exists():
    app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")


@app.get("/", response_model=None)
def root():
    index_file = frontend_dist / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return {"status": "ok", "message": "Frontend not built. Run: cd frontend && npm run build"}
```

- [ ] **Step 13: Remove old server/routes.py**

```bash
git rm server/routes.py
```

- [ ] **Step 14: Run tests**

```bash
uv run pytest tests/ -v
```

Expected: New tests pass. Old `test_api_contract.py` will fail (routes changed). Remove or update it in next step.

- [ ] **Step 15: Update test_api_contract.py for new route structure**

Replace contents of `tests/test_api_contract.py`:

```python
# tests/test_api_contract.py
"""Smoke tests for basic API contract."""
from fastapi.testclient import TestClient
from app import app

client = TestClient(app)


def test_health_endpoint():
    response = client.get("/api/v1/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_models_endpoint():
    response = client.get("/api/v1/models")
    assert response.status_code == 200
    body = response.json()
    assert "aliases" in body
    assert "endpoints" in body


def test_chat_completions_requires_auth():
    response = client.post("/api/v1/chat/completions", json={
        "model": "claude-sonnet-latest",
        "messages": [{"role": "user", "content": "Hi"}],
    })
    assert response.status_code == 401
```

- [ ] **Step 16: Run all tests**

```bash
uv run pytest tests/ -v
```

Expected: All PASS

- [ ] **Step 17: Commit**

```bash
git add -A
git commit -m "feat: add chat completions proxy, models listing, and route structure"
```

---

## Chunk 4: DABs Bundle and Startup Refresh

### Task 7: Create databricks.yml bundle config

**Files:**
- Create: `better-agent-gateway/databricks.yml`
- Modify: `better-agent-gateway/bundle/resources/app.yml`

- [ ] **Step 1: Create databricks.yml**

```yaml
bundle:
  name: better-agent-gateway

include:
  - bundle/resources/*.yml

targets:
  dev:
    mode: development
    default: true
    workspace:
      root_path: /Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
  prod:
    mode: production
    workspace:
      root_path: /Shared/.bundle/${bundle.name}/${bundle.target}
```

- [ ] **Step 2: Update bundle/resources/app.yml with proper resource references**

```yaml
resources:
  apps:
    better-agent-gateway:
      name: better-agent-gateway-${bundle.target}
      description: "Scoped OAuth LLM gateway with automatic -latest model resolution"
      source_code_path: ../..
      config:
        command:
          - "python"
          - "-m"
          - "uvicorn"
          - "app:app"
          - "--host"
          - "0.0.0.0"
          - "--port"
          - "8000"
        env:
          - name: ALIAS_REFRESH_INTERVAL_SECONDS
            value: "300"
```

- [ ] **Step 3: Commit**

```bash
git add databricks.yml bundle/resources/app.yml
git commit -m "feat: add DABs bundle config for deployment"
```

---

### Task 8: Add background alias refresh on app startup

**Files:**
- Modify: `better-agent-gateway/app.py`
- Modify: `better-agent-gateway/server/alias_registry.py`

- [ ] **Step 1: Add async refresh method to alias_registry.py**

Add to the `AliasRegistry` class:

```python
async def refresh_from_workspace(self, workspace_client) -> None:
    """Fetch serving endpoints from workspace and rebuild aliases."""
    import asyncio

    endpoints = await asyncio.to_thread(
        lambda: list(workspace_client.serving_endpoints.list()),
    )
    self.refresh_from_endpoints(endpoints)
```

Add a standalone refresh function below `get_registry()`:

```python
async def refresh_registry_from_workspace() -> None:
    """Refresh the module-level registry from the workspace."""
    from .config import get_workspace_client, settings
    registry = get_registry()
    try:
        client = get_workspace_client()
        await registry.refresh_from_workspace(client)
        logger.info("Registry refreshed successfully")
    except Exception:
        logger.exception("Failed to refresh alias registry from workspace")
```

- [ ] **Step 2: Add lifespan context manager to app.py**

Replace the `app = FastAPI(...)` block and add the lifespan before it:

```python
import asyncio
import logging
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: refresh alias registry. Background: periodic refresh."""
    from server.alias_registry import refresh_registry_from_workspace
    from server.config import settings

    await refresh_registry_from_workspace()

    async def _periodic_refresh():
        while True:
            await asyncio.sleep(settings.alias_refresh_interval_seconds)
            await refresh_registry_from_workspace()

    task = asyncio.create_task(_periodic_refresh())
    logger.info("Alias refresh scheduled every %ds", settings.alias_refresh_interval_seconds)
    yield
    task.cancel()


app = FastAPI(
    title="Better Agent Gateway",
    description="Scoped OAuth LLM gateway with automatic -latest model resolution",
    version="0.1.0",
    lifespan=lifespan,
)
```

Note: This replaces the deprecated `@app.on_event("startup")` pattern.

- [ ] **Step 3: Commit**

```bash
git add app.py server/alias_registry.py
git commit -m "feat: add background alias refresh on startup with periodic interval"
```

---

## Chunk 5: React Frontend

### Task 9: Scaffold React frontend

**Files:**
- Create: `better-agent-gateway/frontend/` (full scaffold)

This follows the same pattern as the uc-metadata app: Vite + React 19 + TypeScript.

- [ ] **Step 1: Initialize frontend with npm**

```bash
cd better-agent-gateway
npm create vite@latest frontend -- --template react-ts
cd frontend
npm install
npm install --save-dev @testing-library/react @testing-library/jest-dom jsdom vitest @types/node
```

- [ ] **Step 2: Configure vite.config.ts**

Replace `frontend/vite.config.ts`:

```typescript
import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
      },
    },
  },
  test: {
    environment: 'jsdom',
    setupFiles: './src/test/setup.ts',
    globals: true,
  },
})
```

- [ ] **Step 3: Create test setup**

```typescript
// frontend/src/test/setup.ts
import '@testing-library/jest-dom'
```

- [ ] **Step 4: Commit scaffold**

```bash
git add frontend/
git commit -m "feat: scaffold React frontend with Vite and testing config"
```

---

### Task 10: Build frontend dashboard components

**Files:**
- Create: `better-agent-gateway/frontend/src/App.tsx`
- Create: `better-agent-gateway/frontend/src/App.css`
- Create: `better-agent-gateway/frontend/src/components/ModelTable.tsx`
- Create: `better-agent-gateway/frontend/src/components/HealthBadge.tsx`

- [ ] **Step 1: Create ModelTable component**

```typescript
// frontend/src/components/ModelTable.tsx
import { useEffect, useState } from 'react'

interface ModelsResponse {
  aliases: Record<string, string>
  endpoints: string[]
}

export function ModelTable() {
  const [data, setData] = useState<ModelsResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetch('/api/v1/models')
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then(setData)
      .catch((e) => setError(e.message))
  }, [])

  if (error) return <div className="error">Failed to load models: {error}</div>
  if (!data) return <div>Loading models...</div>

  const aliasEntries = Object.entries(data.aliases)

  return (
    <div className="model-table">
      <h2>Model Aliases (-latest)</h2>
      {aliasEntries.length === 0 ? (
        <p>No aliases configured. Serving endpoints may not be available yet.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Alias</th>
              <th>Resolves To</th>
            </tr>
          </thead>
          <tbody>
            {aliasEntries.map(([alias, target]) => (
              <tr key={alias}>
                <td><code>{alias}</code></td>
                <td><code>{target}</code></td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <h2>Available Endpoints ({data.endpoints.length})</h2>
      <ul className="endpoint-list">
        {data.endpoints.map((ep) => (
          <li key={ep}><code>{ep}</code></li>
        ))}
      </ul>
    </div>
  )
}
```

- [ ] **Step 2: Create HealthBadge component**

```typescript
// frontend/src/components/HealthBadge.tsx
import { useEffect, useState } from 'react'

export function HealthBadge() {
  const [healthy, setHealthy] = useState<boolean | null>(null)

  useEffect(() => {
    const check = () =>
      fetch('/api/v1/healthz')
        .then((r) => setHealthy(r.ok))
        .catch(() => setHealthy(false))

    check()
    const interval = setInterval(check, 30_000)
    return () => clearInterval(interval)
  }, [])

  if (healthy === null) return <span className="health-badge loading">Checking...</span>
  return (
    <span className={`health-badge ${healthy ? 'healthy' : 'unhealthy'}`}>
      {healthy ? 'Healthy' : 'Unhealthy'}
    </span>
  )
}
```

- [ ] **Step 3: Create App.tsx**

```typescript
// frontend/src/App.tsx
import { ModelTable } from './components/ModelTable'
import { HealthBadge } from './components/HealthBadge'
import './App.css'

function App() {
  return (
    <div className="app">
      <header>
        <h1>Better Agent Gateway</h1>
        <HealthBadge />
      </header>
      <main>
        <section className="info-banner">
          <p>
            This gateway proxies LLM requests through Databricks Model Serving
            with <strong>scoped OAuth</strong> — your identity, limited permissions.
            Use <code>*-latest</code> aliases to always hit the newest model version.
          </p>
        </section>
        <ModelTable />
      </main>
    </div>
  )
}

export default App
```

- [ ] **Step 4: Create App.css**

```css
/* frontend/src/App.css */
.app {
  max-width: 960px;
  margin: 0 auto;
  padding: 2rem;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
}

header {
  display: flex;
  align-items: center;
  gap: 1rem;
  margin-bottom: 2rem;
  border-bottom: 1px solid #e0e0e0;
  padding-bottom: 1rem;
}

header h1 {
  margin: 0;
  font-size: 1.5rem;
}

.info-banner {
  background: #f0f4ff;
  border: 1px solid #c0d0ff;
  border-radius: 8px;
  padding: 1rem 1.5rem;
  margin-bottom: 2rem;
}

.info-banner code {
  background: #e0e8ff;
  padding: 0.15em 0.4em;
  border-radius: 3px;
  font-size: 0.9em;
}

.health-badge {
  padding: 0.25rem 0.75rem;
  border-radius: 12px;
  font-size: 0.85rem;
  font-weight: 600;
}

.health-badge.healthy {
  background: #d4edda;
  color: #155724;
}

.health-badge.unhealthy {
  background: #f8d7da;
  color: #721c24;
}

.health-badge.loading {
  background: #fff3cd;
  color: #856404;
}

.model-table h2 {
  font-size: 1.2rem;
  margin-top: 1.5rem;
}

.model-table table {
  width: 100%;
  border-collapse: collapse;
  margin: 0.5rem 0 1.5rem;
}

.model-table th,
.model-table td {
  padding: 0.5rem 1rem;
  text-align: left;
  border-bottom: 1px solid #e0e0e0;
}

.model-table th {
  background: #f5f5f5;
  font-weight: 600;
}

.model-table code {
  background: #f0f0f0;
  padding: 0.15em 0.4em;
  border-radius: 3px;
  font-size: 0.9em;
}

.endpoint-list {
  list-style: none;
  padding: 0;
}

.endpoint-list li {
  padding: 0.3rem 0;
}

.error {
  color: #721c24;
  background: #f8d7da;
  padding: 0.75rem 1rem;
  border-radius: 6px;
}
```

- [ ] **Step 5: Build frontend**

```bash
cd frontend && npm run build
```

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: add React frontend with model table, health badge, and dashboard"
```

---

## Chunk 6: Update README and .gitignore

### Task 11: Update README and cleanup

**Files:**
- Modify: `better-agent-gateway/README.md`
- Modify: `better-agent-gateway/.gitignore`

- [ ] **Step 1: Write README.md**

```markdown
# Better Agent Gateway

A Databricks App that proxies AI Gateway serving endpoints with:

- **Scoped OAuth** — users authenticate as themselves (SSO), but the app limits their token scope to only LLM serving endpoint access
- **Automatic `-latest` aliases** — `claude-sonnet-latest`, `gpt-latest`, `gemini-latest`, etc. auto-resolve to the newest deployed model version

## Architecture

```
User -> Databricks App (OAuth scoped) -> Better Agent Gateway -> Serving Endpoints
                                              |
                                    OBO token forwarded
                                    (user's identity, limited scope)
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/chat/completions` | Proxy chat completion to resolved serving endpoint |
| GET | `/api/v1/models` | List available aliases and endpoints |
| GET | `/api/v1/healthz` | Health check |
| GET | `/api/v1/audit/requests/{id}` | Audit trail lookup |

## Model Families

| Alias | Example Resolution |
|-------|-------------------|
| `claude-sonnet-latest` | `databricks-claude-sonnet-4-6` |
| `claude-opus-latest` | `databricks-claude-opus-4-6` |
| `claude-haiku-latest` | `databricks-claude-haiku-4-5` |
| `gpt-latest` | `databricks-gpt-4o` |
| `gemini-latest` | `databricks-gemini-2-0-flash` |
| `codex-latest` | `databricks-codex-mini-latest` |

## Local Development

```bash
# Backend
cd better-agent-gateway
uv sync
APP_ENV=dev uv run uvicorn app:app --reload --port 8000

# Frontend
cd frontend
npm install
npm run dev
```

## Deploy with DABs

```bash
cd better-agent-gateway
databricks bundle deploy --target dev
```

## Run Tests

```bash
uv run pytest tests/ -v
cd frontend && npm test
```
```

- [ ] **Step 2: Update .gitignore**

```
__pycache__/
*.pyc
.venv/
.env
*.env
!template.env
.pytest_cache/
.databricks/
frontend/node_modules/
frontend/dist/
```

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs: update README and gitignore for better-agent-gateway"
```

---

## Chunk 7: Final Verification

### Task 12: End-to-end verification

- [ ] **Step 1: Run full test suite**

```bash
cd better-agent-gateway
uv sync
uv run pytest tests/ -v
```

Expected: All tests pass.

- [ ] **Step 2: Run frontend build**

```bash
cd frontend && npm run build
```

Expected: Clean build with no errors.

- [ ] **Step 3: Verify app starts locally**

```bash
cd better-agent-gateway
APP_ENV=dev uv run uvicorn app:app --port 8000 &
sleep 2
curl -s http://localhost:8000/api/v1/healthz | python -m json.tool
curl -s http://localhost:8000/api/v1/models | python -m json.tool
kill %1
```

Expected: Health returns `{"status": "ok"}`, models returns `{"aliases": {}, "endpoints": []}` (empty because no workspace connection in local dev without profile).

- [ ] **Step 4: Final commit if any cleanup needed**

```bash
git status
# If clean, skip. Otherwise:
git add -A
git commit -m "chore: final cleanup for better-agent-gateway"
```
