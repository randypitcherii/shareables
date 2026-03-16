# Connecting an LLM Client to the Better Agent Gateway

The gateway exposes an OpenAI-compatible API. Any tool or SDK that accepts a custom base URL and API key can point at it.

**Base URL:** `https://better-agent-gateway-<target>-<id>.aws.databricksapps.com/api`

---

## 1. Quick Start

Set two things in your client:

| Setting | Value |
|---|---|
| `base_url` | `https://<your-gateway-host>/api/v1` |
| `api_key` | Any non-empty string (see Authentication below) |

Authentication is handled by your browser session cookie — the `api_key` value is ignored by the gateway but required by most SDK clients.

---

## 2. Authentication

The gateway uses **Databricks App scoped OAuth** with On-Behalf-Of (OBO) token exchange. There are two ways to authenticate:

### Option A: Browser Session (Interactive)

1. Navigate to `https://<your-gateway-host>/` in a browser.
2. Databricks redirects you through your SSO provider (first visit only).
3. You are prompted to grant the app the `serving.serving-endpoints` scope.
4. After consent, your browser holds a session cookie that authenticates all subsequent API requests.

### Option B: Bearer Token (Programmatic) — Recommended for SDKs

Any valid Databricks OAuth token works as a bearer token on all routes (not just `/api/`). No browser needed.

```bash
# Get a token via Databricks CLI
databricks auth login --host https://<workspace-url>
TOKEN=$(databricks auth token | jq -r '.access_token')

# Use it directly
curl -H "Authorization: Bearer $TOKEN" \
  https://<your-gateway-host>/api/v1/healthz
```

Or in Python:
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(host="https://<workspace-url>")
headers = w.config.authenticate()  # {"Authorization": "Bearer <token>"}
```

**Key facts:**
- Standard `all-apis` scoped tokens work — no special scope needed
- PATs (Personal Access Tokens) do NOT work — always returns 401
- Token lifetime is ~1 hour; use `w.config.authenticate()` for auto-refresh
- Every request runs as your identity (never the app's service principal)

---

## 3. Available Models

The gateway auto-discovers READY serving endpoints prefixed with `databricks-` and builds **`-latest` aliases** that always point to the highest available version in each family.

| Alias | Resolves to |
|---|---|
| `claude-haiku-latest` | e.g. `databricks-claude-haiku-3-5` |
| `claude-sonnet-latest` | e.g. `databricks-claude-sonnet-3-7` |
| `claude-opus-latest` | e.g. `databricks-claude-opus-4` |
| `gpt-latest` | e.g. `databricks-gpt-4o` |
| `gemini-latest` | e.g. `databricks-gemini-2-0` |

Aliases refresh every 5 minutes (configurable). You can also use the raw endpoint name directly (e.g. `databricks-claude-sonnet-3-7`).

**Discover all available models:**

```bash
curl -b cookies.txt https://<your-gateway-host>/api/v1/models
```

Response shape:

```json
{
  "aliases": {
    "claude-sonnet-latest": "databricks-claude-sonnet-3-7",
    "claude-haiku-latest": "databricks-claude-haiku-3-5"
  },
  "endpoints": [
    "databricks-claude-haiku-3-5",
    "databricks-claude-sonnet-3-7"
  ]
}
```

---

## 4. Example: Python (openai SDK)

Use the Databricks SDK for automatic token management:

```python
from openai import OpenAI
from databricks.sdk import WorkspaceClient

GATEWAY_URL = "https://<your-gateway-host>/api/v1"

# Auto-refreshing token via Databricks SDK
w = WorkspaceClient(host="https://<workspace-url>")

client = OpenAI(
    base_url=GATEWAY_URL,
    api_key=w.config.authenticate()["Authorization"].replace("Bearer ", ""),
)

response = client.chat.completions.create(
    model="claude-sonnet-latest",
    messages=[{"role": "user", "content": "Hello!"}],
    max_tokens=256,
)

print(response.choices[0].message.content)
```

For long-running scripts, use a callable `api_key` for automatic refresh:

```python
client = OpenAI(
    base_url=GATEWAY_URL,
    api_key=lambda: w.config.authenticate()["Authorization"].replace("Bearer ", ""),
)
```

---

## 5. Example: curl

```bash
# Get a token (opens browser for SSO on first run)
databricks auth login --host https://<workspace-url>
TOKEN=$(databricks auth token | jq -r '.access_token')

# Chat completion
curl -s \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -X POST https://<your-gateway-host>/api/v1/chat/completions \
  -d '{
    "model": "claude-sonnet-latest",
    "messages": [{"role": "user", "content": "What is 2+2?"}],
    "max_tokens": 100
  }'
```

---

## 6. Example: Claude Code and Other OpenAI-Compatible Tools

Any tool that accepts a custom OpenAI base URL can use this gateway.

**Claude Code (`claude` CLI):**

```bash
# Set via environment variable
export ANTHROPIC_BASE_URL=https://<your-gateway-host>/api/v1
export ANTHROPIC_API_KEY=not-used
```

**Continue.dev / Cursor / VS Code extensions:**

In your tool's model config, set:
- Provider: `OpenAI-compatible` (or `openai`)
- Base URL: `https://<your-gateway-host>/api/v1`
- API Key: any non-empty string
- Model: `claude-sonnet-latest` (or any alias/endpoint name)

**LangChain:**

```python
from langchain_openai import ChatOpenAI

llm = ChatOpenAI(
    base_url="https://<your-gateway-host>/api/v1",
    api_key="not-used",
    model="claude-sonnet-latest",
)
```

**Note for all tools:** The gateway accepts either a browser session cookie or a `Authorization: Bearer <token>` header. For programmatic access, use the bearer token approach (see Section 2).

---

## 7. Troubleshooting

**`401 Unauthorized`**
- Your token has expired — run `databricks auth login` to refresh.
- PATs (Personal Access Tokens) do not work — only OAuth tokens are accepted.
- If using browser access, visit `https://<your-gateway-host>/` to re-authenticate.

**`404` on `/api/v1/models` or `/api/v1/chat/completions`**
- Confirm your base URL ends with `/api/v1` (not just the host root).
- The gateway mounts routes at `/api/v1/...`.

**`"model not found"` or alias resolves to wrong endpoint**
- The alias registry refreshes every 5 minutes from workspace serving endpoints.
- A model may not be READY yet — check `GET /api/v1/models` to see what is currently resolved.
- Use the raw endpoint name (e.g. `databricks-claude-sonnet-3-7`) to bypass alias resolution.

**`502 Bad Gateway`**
- The upstream Databricks serving endpoint returned an error.
- The endpoint may be scaled down (cold start) — retry after a few seconds.
- Your OBO token may not have access to that specific endpoint — check workspace permissions.

**Stale `-latest` alias**
- If a new model version was deployed but the alias hasn't updated, wait up to 5 minutes for the next refresh cycle.
- The gateway only tracks endpoints in `READY` state with the `databricks-` prefix.

---

## 8. Validation

Run the end-to-end validation script to verify the gateway is working correctly. It checks health, authentication, model discovery, chat completion, scope enforcement, and identity attribution.

```bash
# Default: uses GATEWAY_URL env var or the dev deployment URL
uv run python scripts/validate.py

# Point at a specific gateway
uv run python scripts/validate.py --app-url https://your-gateway.aws.databricksapps.com

# Verbose output (shows full request/response details)
uv run python scripts/validate.py --verbose

# Override both app and workspace URLs
uv run python scripts/validate.py \
  --app-url https://your-gateway.aws.databricksapps.com \
  --workspace-url https://your-workspace.cloud.databricks.com
```

The script requires a valid Databricks OAuth token. If you haven't authenticated recently, run:

```bash
databricks auth login --host https://<workspace-url>
```

All 8 checks should pass on a correctly deployed gateway. Checks 2-3 (auth rejection, PAT rejection) are enforced by the Databricks App proxy and may not pass against a local dev server.
