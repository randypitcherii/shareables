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

The gateway uses **Databricks App scoped OAuth** with On-Behalf-Of (OBO) token exchange.

**How it works:**

1. Navigate to `https://<your-gateway-host>/` in a browser.
2. Databricks redirects you through your SSO provider (first visit only).
3. You are prompted to grant the app the `serving.serving-endpoints` scope — this allows it to call model serving endpoints on your behalf.
4. After consent, your browser holds a session cookie that authenticates all subsequent API requests.

**Important:** API requests must include the session cookie. This means:

- Browser-based tools (fetch, curl with cookie jar) work out of the box after you've logged in.
- SDK clients running in the same machine/browser context need the cookie forwarded (see examples below).
- The gateway never uses a shared service principal — every request runs as your identity.

There is no persistent token or API key to manage. Sessions are tied to your Databricks SSO session.

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

First, log in via browser to establish the session cookie, then save it:

```bash
# After logging in via browser, export cookies from your browser
# or use a headless login flow. For scripted use, capture the cookie manually.
```

```python
import httpx
from openai import OpenAI

GATEWAY_URL = "https://<your-gateway-host>/api/v1"

# Pass session cookie via a custom httpx client
session_cookie = "<value of your_session_cookie from browser>"

client = OpenAI(
    base_url=GATEWAY_URL,
    api_key="not-used",  # required by the SDK, ignored by the gateway
    http_client=httpx.Client(
        cookies={"your_session_cookie": session_cookie}
    ),
)

response = client.chat.completions.create(
    model="claude-sonnet-latest",
    messages=[{"role": "user", "content": "Hello!"}],
    max_tokens=256,
)

print(response.choices[0].message.content)
```

**Tip:** If you're running this from a Databricks notebook or app that is already authenticated, the cookie is handled automatically by the runtime — just set `base_url` and `api_key`.

---

## 5. Example: curl

Save cookies after browser login, then use them in requests:

```bash
# Step 1: trigger login and save cookies (opens browser for SSO)
curl -c cookies.txt -b cookies.txt -L https://<your-gateway-host>/

# Step 2: chat completion
curl -s -b cookies.txt \
  -X POST https://<your-gateway-host>/api/v1/chat/completions \
  -H "Content-Type: application/json" \
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

**Note for all tools:** The gateway requires a valid session cookie. Tools that make server-side API calls (not from your browser) will need the cookie injected via a custom HTTP client or proxy.

---

## 7. Troubleshooting

**`401 Unauthorized`**
- Your session has expired or you haven't logged in.
- Visit `https://<your-gateway-host>/` in a browser and complete the SSO flow.
- Re-export your session cookie if using curl or an SDK.

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
